use std::collections::HashSet;
use std::convert::TryInto;

use anyhow::Error;
use rand::Rng;

use crypto::hash::BlockHash;

use criterion::{criterion_group, criterion_main, Criterion};

use storage::block_meta_storage::Meta;
use storage::tests_common::TmpStorage;
use storage::{BlockMetaStorage, BlockMetaStorageReader, StorageError};

/// Old naive implementation of find_block_at_distance kept only for benchmarking resons
fn find_block_at_distance_old(
    storage: &BlockMetaStorage,
    block_hash: BlockHash,
    requested_distance: i32,
) -> Result<Option<BlockHash>, StorageError> {
    if requested_distance == 0 {
        return Ok(Some(block_hash));
    }
    if requested_distance < 0 {
        unimplemented!("TODO: TE-238 - Not yet implemented block header parsing for '+' - means we need to go forwards throught successors, this could be tricky and should be related to current head branch - reorg");
    }

    let result = {
        let mut current_block = block_hash;
        let mut current_distance = 0;

        let predecessor_at_distance = loop {
            let (predecessor, predecesor_distance) = match storage.get(&current_block)? {
                Some(meta) => {
                    match meta.predecessor().to_owned() {
                        Some(predecessor) => {
                            if meta.level() > Meta::GENESIS_LEVEL {
                                (predecessor, current_distance + 1)
                            } else {
                                // if we found genesis level, we return None, because genesis does not have predecessor
                                break None;
                            }
                        }
                        None => break None,
                    }
                }
                None => break None,
            };

            // we finish, if we found distance or if it is a genesis
            if predecesor_distance == requested_distance {
                break Some(predecessor);
            } else {
                // else we continue to predecessor's predecessor
                current_block = predecessor;
                current_distance = predecesor_distance;
            }
        };
        predecessor_at_distance
    };

    Ok(result)
}

/// NOTE: duplicate block_meta_storage tests module
/// Create and return a storage with [number_of_blocks] blocks and the last BlockHash in it
fn init_mocked_storage(number_of_blocks: usize) -> Result<(BlockMetaStorage, BlockHash), Error> {
    let tmp_storage = TmpStorage::create("__mocked_storage")?;
    let storage = BlockMetaStorage::new(tmp_storage.storage());
    let mut block_hash_set = HashSet::new();
    let mut rng = rand::thread_rng();

    let k: BlockHash = vec![0; 32].try_into()?;
    let v = Meta::new(
        false,
        Some(vec![0; 32].try_into()?),
        0,
        vec![44; 4].try_into()?,
    );

    block_hash_set.insert(k.clone());

    storage.put(&k, &v)?;
    assert!(storage.get(&k)?.is_some());

    // save for the iteration
    let mut predecessor = k;

    // generate random block hashes, watch out for colissions
    let block_hashes: Vec<BlockHash> = (1..number_of_blocks)
        .map(|_| {
            let mut random_hash: BlockHash = (0..32)
                .map(|_| rng.gen_range(0, 255))
                .collect::<Vec<_>>()
                .try_into()
                .unwrap();
            // regenerate on collision
            while block_hash_set.contains(&random_hash) {
                random_hash = (0..32)
                    .map(|_| rng.gen_range(0, 255))
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap();
            }
            block_hash_set.insert(random_hash.clone());
            random_hash
        })
        .collect();

    // add them to the mocked storage
    for (idx, block_hash) in block_hashes.iter().enumerate() {
        let v = Meta::new(
            true,
            Some(predecessor.clone()),
            idx.try_into()?,
            vec![44; 4].try_into()?,
        );
        storage.put(&block_hash, &v)?;
        storage.store_predecessors(&block_hash, &v)?;
        predecessor = block_hash.clone();
    }

    Ok((storage, predecessor))
}

fn find_block_at_distance_benchmark(c: &mut Criterion) {
    let (storage, last_block_hash) = init_mocked_storage(100_000).unwrap();

    // just, check if impl. is correct
    assert_eq!(
        find_block_at_distance_old(&storage, last_block_hash.clone(), 99_998).unwrap(),
        storage
            .find_block_at_distance(last_block_hash.clone(), 99_998)
            .unwrap()
    );

    // run bench
    c.bench_function("find_block_at_distance", |b| {
        b.iter(|| storage.find_block_at_distance(last_block_hash.clone(), 99_998))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = find_block_at_distance_benchmark
}

criterion_main!(benches);
