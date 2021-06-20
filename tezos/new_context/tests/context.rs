// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crypto::hash::ContextHash;
use storage::tests_common::TmpStorage;
use storage::{BlockHeaderWithHash, BlockStorage};
use tezos_api::ffi::TezosContextTezEdgeStorageConfiguration;
use tezos_messages::p2p::encoding::prelude::BlockHeaderBuilder;
use tezos_new_context::context_key;
use tezos_new_context::initializer::initialize_tezedge_context;
use tezos_new_context::{IndexApi, ProtocolContextApi, ShellContextApi};

#[test]
pub fn test_context_set_get_commit() -> Result<(), anyhow::Error> {
    // prepare temp storage
    let tmp_storage = TmpStorage::create_to_out_dir("__context:test_context_set_get_commit")
        .expect("Storage error");
    let persistent_storage = tmp_storage.storage();

    // init block storage (because of commit)
    let block = dummy_block("BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe", 0)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // context
    let mut context = initialize_tezedge_context(&TezosContextTezEdgeStorageConfiguration {
        backend: tezos_new_context::initializer::ContextKvStoreConfiguration::InMemGC,
        ipc_socket_path: None,
    })
    .unwrap();

    // add to context
    context = context.add(
        &context_key!("data/rolls/owner/current/index/123"),
        vec![1, 2, 3, 4, 5, 6],
    )?;

    // commit
    let new_context_hash =
        ContextHash::try_from("CoVf53zSDGcSWS74Mxe2i2RJnVfCaMrAjxK2Xq7tgiFMtkNwUdPv")?;

    let hash = context.commit("Tezos".to_string(), "Genesis".to_string(), 0)?;
    assert_eq!(hash, new_context_hash);

    // get key from new commit
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        new_context_hash,
        vec![1, 2, 3, 4, 5, 6]
    );

    Ok(())
}

#[test]
pub fn test_context_delete_and_remove() -> Result<(), anyhow::Error> {
    // prepare temp storage
    let tmp_storage = TmpStorage::create_to_out_dir("__context:test_context_delete_and_remove")
        .expect("Storage error");
    let persistent_storage = tmp_storage.storage();

    // init block with level 0 (because of commit)
    let block = dummy_block("BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe", 0)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // context
    let mut context = initialize_tezedge_context(&TezosContextTezEdgeStorageConfiguration {
        backend: tezos_new_context::initializer::ContextKvStoreConfiguration::InMemGC,
        ipc_socket_path: None,
    })
    .unwrap();

    // add to context
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/0"),
        vec![1, 2, 3, 4],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/1/a"),
        vec![1, 2, 3, 4, 5],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/1/b"),
        vec![1, 2, 3, 4, 5],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/2/a"),
        vec![1, 2, 3, 4, 5, 61],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/2/b"),
        vec![1, 2, 3, 4, 5, 62],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/index/123"),
        vec![1, 2, 3, 4, 5, 6, 7],
    )?;

    // commit
    let context_hash_1: ContextHash =
        "CoUyfscSjC3XYECq1aFYQQLrVZuNSW17B7SbFDV9W1REfhJpxZwB".try_into()?;

    let hash = context.commit("Tezos".to_string(), "Genesis".to_string(), 0)?;
    assert_eq!(hash, context_hash_1);

    // get key from new commit
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/0"),
        context_hash_1,
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1/a"),
        context_hash_1,
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1/b"),
        context_hash_1,
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/a"),
        context_hash_1,
        vec![1, 2, 3, 4, 5, 61]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/b"),
        context_hash_1,
        vec![1, 2, 3, 4, 5, 62]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        context_hash_1,
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    // insert another block with level 1
    let block = dummy_block("BKyQ9EofHrgaZKENioHyP4FZNsTmiSEcVmcghgzCC9cGhE7oCET", 1)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // checkout last commit to be modified
    context = context.index.checkout(&context_hash_1)?.expect(&format!(
        "Commit not found: {}",
        context_hash_1.to_base58_check()
    ));

    // 1. remove rec
    context = context.delete(&context_key!("data/rolls/owner/current/cpu/2"))?;
    context = context.delete(&context_key!("data/rolls/owner/current/cpu/1/b"))?;

    // commit
    let context_hash_2: ContextHash =
        "CoVGom58bpVjHWVsKuc8k7JC7QyzZ7n4ntGZiPpw2CwM43sxC4XF".try_into()?;

    let hash = context.commit("Tezos".to_string(), "Genesis".to_string(), 0)?;
    assert_eq!(hash, context_hash_2);

    // get key from new commit
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/0"),
        context_hash_2,
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1/a"),
        context_hash_2,
        vec![1, 2, 3, 4, 5]
    );
    assert_data_deleted!(
        context,
        context_key!("data/rolls/owner/current/cpu/1/b"),
        context_hash_2
    );
    assert_data_deleted!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/a"),
        context_hash_2
    );
    assert_data_deleted!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/b"),
        context_hash_2
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        context_hash_2,
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    Ok(())
}

#[test]
pub fn test_context_copy() -> Result<(), anyhow::Error> {
    // prepare temp storage
    let tmp_storage =
        TmpStorage::create_to_out_dir("__context:context_copy").expect("Storage error");
    let persistent_storage = tmp_storage.storage();

    // init block with level 0 (because of commit)
    let block = dummy_block("BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe", 0)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // context
    let mut context = initialize_tezedge_context(&TezosContextTezEdgeStorageConfiguration {
        backend: tezos_new_context::initializer::ContextKvStoreConfiguration::InMemGC,
        ipc_socket_path: None,
    })
    .unwrap();

    // add to context
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/0"),
        vec![1, 2, 3, 4],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/1"),
        vec![1, 2, 3, 4, 5],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/2/a"),
        vec![1, 2, 3, 4, 5, 61],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/cpu/2/b"),
        vec![1, 2, 3, 4, 5, 62],
    )?;
    context = context.add(
        &context_key!("data/rolls/owner/current/index/123"),
        vec![1, 2, 3, 4, 5, 6, 7],
    )?;

    // commit
    let context_hash_1: ContextHash =
        "CoVu1KaQQd2SFPqJh7go1t9q11upv1BewzShtTrNK7ZF6uCAcUQR".try_into()?;

    let hash = context.commit("Tezos".to_string(), "Genesis".to_string(), 0)?;
    assert_eq!(hash, context_hash_1);

    // get key from new commit
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/0"),
        context_hash_1,
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1"),
        context_hash_1,
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/a"),
        context_hash_1,
        vec![1, 2, 3, 4, 5, 61]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/b"),
        context_hash_1,
        vec![1, 2, 3, 4, 5, 62]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        context_hash_1,
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    // insert another block with level 1
    let block = dummy_block("BKyQ9EofHrgaZKENioHyP4FZNsTmiSEcVmcghgzCC9cGhE7oCET", 1)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // checkout last commit to be modified
    context = context.index.checkout(&context_hash_1)?.expect(&format!(
        "Commit not found: {}",
        context_hash_1.to_base58_check()
    ));

    // 1. copy
    if let Some(new_context) = context.copy(
        &context_key!("data/rolls/owner/current"),
        &context_key!("data/rolls/owner/snapshot/01/02"),
    )? {
        context = new_context;
    }

    // commit
    let context_hash_2: ContextHash =
        "CoVX1ptKigdesVSqaREXTTHKGegLGM4x1bSSFvPgX5V8qj85r98G".try_into()?;

    let hash = context.commit("Tezos".to_string(), "Genesis".to_string(), 0)?;
    assert_eq!(hash, context_hash_2);

    // get key from new commit - original stays unchanged
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/0"),
        context_hash_2,
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1"),
        context_hash_2,
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/a"),
        context_hash_2,
        vec![1, 2, 3, 4, 5, 61]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/b"),
        context_hash_2,
        vec![1, 2, 3, 4, 5, 62]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        context_hash_2,
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    // get key from new commit - original stays unchanged
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/cpu/0"),
        context_hash_2,
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/cpu/1"),
        context_hash_2,
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/cpu/2/a"),
        context_hash_2,
        vec![1, 2, 3, 4, 5, 61]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/cpu/2/b"),
        context_hash_2,
        vec![1, 2, 3, 4, 5, 62]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/index/123"),
        context_hash_2,
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    Ok(())
}

fn dummy_block(block_hash: &str, level: i32) -> Result<BlockHeaderWithHash, anyhow::Error> {
    Ok(BlockHeaderWithHash {
        hash: block_hash.try_into()?,
        header: Arc::new(
            BlockHeaderBuilder::default()
                .level(level)
                .proto(0)
                .predecessor("BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe".try_into()?)
                .timestamp(5_635_634)
                .validation_pass(0)
                .operations_hash(
                    "LLoaGLRPRx3Zf8kB4ACtgku8F4feeBiskeb41J1ciwfcXB3KzHKXc".try_into()?,
                )
                .fitness(vec![])
                .context("CoVmAcMV64uAQo8XvfLr9VDuz7HVZLT4cgK1w1qYmTjQNbGwQwDd".try_into()?)
                .protocol_data(vec![])
                .build()
                .unwrap(),
        ),
    })
}

#[macro_export]
macro_rules! assert_data_eq {
    ($ctx:expr, $key:expr, $context_hash:expr, $data:expr) => {{
        let data = $ctx.index.get_key_from_history(&$context_hash, &$key)?;
        assert!(data.is_some());
        assert_eq!(data.unwrap(), $data);
    }};
}

#[macro_export]
macro_rules! assert_data_deleted {
    ($ctx:expr, $key:expr, $context_hash:expr) => {{
        let data = $ctx.index.get_key_from_history(&$context_hash, &$key);
        assert!(data.is_ok());
        assert!(data.unwrap().is_none());
    }};
}
