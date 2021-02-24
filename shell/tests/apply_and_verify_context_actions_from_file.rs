use std::{fs, path::PathBuf, sync::Arc};

use crypto::hash::{BlockHash, ContextHash, OperationListListHash};
use failure::Error;
use rocksdb::Cache;
use shell::context_listener::get_new_tree_hash;
use shell::context_listener::perform_context_action;
use slog::{debug, info, warn, Drain, Level, Logger};
use std::convert::TryFrom;
use std::env;
use std::fs::OpenOptions;
use std::io::{BufReader, Read, Seek, SeekFrom};
use storage::action_file::ActionsFileReader;
use storage::BlockHeaderWithHash;
use storage::{
    context::{ContextApi, TezedgeContext},
    persistent::{CommitLogSchema, CommitLogs, KeyValueSchema, PersistentStorage},
    BlockStorage,
};
use tezos_context::channel::ContextAction;
use tezos_messages::p2p::encoding::prelude::BlockHeaderBuilder;

pub fn get_tree_action(action: &ContextAction) -> String {
    match action {
        ContextAction::Get { .. } => "ContextAction::Get".to_string(),
        ContextAction::Mem { .. } => "ContextAction::Mem".to_string(),
        ContextAction::DirMem { .. } => "ContextAction::DirMem".to_string(),
        ContextAction::Set { .. } => "ContextAction::Set".to_string(),
        ContextAction::Copy { .. } => "ContextAction::Copy".to_string(),
        ContextAction::Delete { .. } => "ContextAction::Delete".to_string(),
        ContextAction::RemoveRecursively { .. } => "ContextAction::RemoveRecursively".to_string(),
        ContextAction::Commit { .. } => "ContextAction::Commit".to_string(),
        ContextAction::Fold { .. } => "ContextAction::Fold".to_string(),
        ContextAction::Checkout { .. } => "ContextAction::Checkout".to_string(),
        ContextAction::Shutdown { .. } => "ContextAction::Shutdown".to_string(),
    }
}

fn create_logger() -> Logger {
    let drain = slog_async::Async::new(
        slog_term::FullFormat::new(slog_term::TermDecorator::new().build())
            .build()
            .fuse(),
    )
    .chan_size(32768)
    .overflow_strategy(slog_async::OverflowStrategy::Block)
    .build()
    .filter_level(Level::Debug)
    .fuse();

    Logger::root(drain, slog::o!())
}

fn create_commit_log(path: &PathBuf) -> Arc<CommitLogs> {
    storage::persistent::open_cl(&path, vec![BlockStorage::descriptor()])
        .map(Arc::new)
        .unwrap()
}

// process actionfile without deselializing blocks
// in order to get count of blocks
fn get_blocks_count(log: &Logger, path: PathBuf) -> u32 {
    let mut counter = 0;
    let file = OpenOptions::new()
        .write(false)
        .create(false)
        .read(true)
        .open(path)
        .unwrap();
    let mut reader = BufReader::new(file);
    let mut pos = 0_u64;

    let mut block_size = [0_u8; 4];

    loop {
        if reader.seek(SeekFrom::Start(pos)).is_err() {
            warn!(log, "missing block operations information");
            break;
        }
        if reader.read_exact(&mut block_size).is_err() {
            break;
        }
        // skips header
        pos += block_size.len() as u64;
        // skips block
        pos += u32::from_be_bytes(block_size) as u64;
        counter += 1;
    }
    counter
}

fn create_key_value_store(path: &PathBuf, cache: &Cache) -> Arc<rocksdb::DB> {
    let schemas = vec![
        storage::block_storage::BlockPrimaryIndex::descriptor(&cache),
        storage::block_storage::BlockByLevelIndex::descriptor(&cache),
        storage::block_storage::BlockByContextHashIndex::descriptor(&cache),
        storage::BlockMetaStorage::descriptor(&cache),
        storage::OperationsStorage::descriptor(&cache),
        storage::OperationsMetaStorage::descriptor(&cache),
        storage::context_action_storage::ContextActionByBlockHashIndex::descriptor(&cache),
        storage::context_action_storage::ContextActionByContractIndex::descriptor(&cache),
        storage::context_action_storage::ContextActionByTypeIndex::descriptor(&cache),
        storage::ContextActionStorage::descriptor(&cache),
        storage::merkle_storage::MerkleStorage::descriptor(&cache),
        storage::SystemStorage::descriptor(&cache),
        storage::persistent::sequence::Sequences::descriptor(&cache),
        storage::MempoolStorage::descriptor(&cache),
        storage::ChainMetaStorage::descriptor(&cache),
        storage::PredecessorStorage::descriptor(&cache),
    ];

    let db_config = storage::persistent::DbConfiguration::default();
    storage::persistent::open_kv(path, schemas, &db_config)
        .map(Arc::new)
        .unwrap()
}

#[test]
#[ignore]
fn feed_tezedge_context_with_actions() -> Result<(), Error> {
    let block_header_stub = BlockHeaderBuilder::default()
        .level(0)
        .proto(0)
        .predecessor(BlockHash::try_from(vec![0; 32])?)
        .timestamp(0)
        .validation_pass(0)
        .operations_hash(OperationListListHash::try_from(vec![0; 32])?)
        .fitness(vec![])
        .context(ContextHash::try_from(vec![0; 32])?)
        .protocol_data(vec![])
        .build()
        .unwrap();

    let header_stub = BlockHeaderWithHash::new(block_header_stub).unwrap();

    let cache = Cache::new_lru_cache(128 * 1024 * 1024).unwrap(); // 128 MB
    let commit_log_db_path = PathBuf::from("/tmp/commit_log/");
    let key_value_db_path = PathBuf::from("/tmp/key_value_store/");

    let input_file = env::var("INPUT").expect("test input not set");
    let actions_storage_path = PathBuf::from(input_file.as_str());

    let _ = fs::remove_dir_all(&commit_log_db_path);
    let _ = fs::remove_dir_all(&key_value_db_path);
    let logger = create_logger();

    let commit_log = create_commit_log(&commit_log_db_path);
    let kv = create_key_value_store(&key_value_db_path, &cache);
    let storage = PersistentStorage::new(
        kv.clone(),
        kv.clone(),
        kv,
        commit_log,
        storage::KeyValueStoreBackend::RocksDB, // TODO: test others too?
    );
    let mut context: Box<dyn ContextApi> = Box::new(TezedgeContext::new(
        BlockStorage::new(&storage),
        storage.merkle(),
    ));

    let block_storage = BlockStorage::new(&storage);

    info!(
        logger,
        "Reading info from file {}",
        actions_storage_path.to_str().unwrap()
    );

    let mut counter = 0;
    let blocks_count = get_blocks_count(&logger, actions_storage_path.clone());

    info!(logger, "{} blocks found", blocks_count);

    let actions_reader = ActionsFileReader::new(&actions_storage_path).unwrap();

    for messages in actions_reader {
        counter += 1;
        let progress = counter as f64 / blocks_count as f64 * 100.0;

        match messages.iter().last() {
            Some(action) => match action {
                ContextAction::Commit {
                    block_hash: Some(block_hash),
                    ..
                } => {
                    debug!(
                        logger,
                        "progress {:.7}% - processing block {} with {} messages",
                        progress,
                        hex::encode(&block_hash),
                        messages.len()
                    );
                }
                _ => {
                    panic!("missing commit action")
                }
            },
            None => {
                panic!("missing commit action")
            }
        };

        for action in messages.iter() {
            if let ContextAction::Commit {
                block_hash: Some(block_hash),
                ..
            } = action
            {
                // there is extra validation in ContextApi::commit that verifies that
                // applied action comes from known block - for testing purposes
                // block_storage needs to be fed with stub value in order to pass validation
                let mut b = header_stub.clone();
                b.hash = BlockHash::try_from(block_hash.clone())?;
                block_storage.put_block_header(&b).unwrap();
            }

            match action {
                // actions that does not mutate staging area can be ommited here
                ContextAction::Set { .. }
                | ContextAction::Copy { .. }
                | ContextAction::Delete { .. }
                | ContextAction::RemoveRecursively { .. }
                | ContextAction::Commit { .. }
                | ContextAction::Checkout { .. } => {
                    if let Err(e) = perform_context_action(&action, &mut context) {
                        panic!("cannot perform action error: '{}'", e);
                    }
                }
                ContextAction::Get { .. }
                | ContextAction::Mem { .. }
                | ContextAction::DirMem { .. }
                | ContextAction::Fold { .. }
                | ContextAction::Shutdown { .. } => {}
            };

            // verify context hashes after each block
            if let ContextAction::Checkout { context_hash, .. } = action {
                assert_eq!(
                    context_hash.as_slice(),
                    context.get_last_commit_hash().unwrap().as_slice()
                );
            }

            if let Some(expected_hash) = get_new_tree_hash(&action) {
                assert_eq!(context.get_merkle_root(), expected_hash);
            }
        }
    }
    println!("{:#?}", storage.merkle().read().unwrap().get_merkle_stats());
    Ok(())
}
