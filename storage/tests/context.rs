// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{convert::{TryFrom, TryInto}, env};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crypto::hash::ContextHash;
use storage::context::{ContextApi, TezedgeContext};
use storage::tests_common::TmpStorage;
use storage::{context_key, BlockHeaderWithHash, BlockStorage};
use tezos_messages::p2p::encoding::prelude::BlockHeaderBuilder;

#[test]
pub fn test_context_set_get_commit() -> Result<(), failure::Error> {
    // prepare temp storage
    let tmp_storage = TmpStorage::create(test_storage_dir_path(
        "__context:test_context_set_get_commit",
    ))
    .expect("Storage error");
    let persistent_storage = tmp_storage.storage();

    // init block storage (because of commit)
    let block = dummy_block("BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe", 0)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // context
    let mut context = TezedgeContext::new(
        BlockStorage::new(&persistent_storage),
        persistent_storage.merkle(),
    );

    // add to context
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/index/123"),
        &vec![1, 2, 3, 4, 5, 6],
    )?;

    // commit
    let new_context_hash =
        ContextHash::try_from("CoVf53zSDGcSWS74Mxe2i2RJnVfCaMrAjxK2Xq7tgiFMtkNwUdPv")?;

    let hash = context.commit(
        &block.hash,
        &None,
        "Tezos".to_string(),
        "Genesis".to_string(),
        0,
    )?;
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
pub fn test_context_delete_and_remove() -> Result<(), failure::Error> {
    // prepare temp storage
    let tmp_storage = TmpStorage::create(test_storage_dir_path(
        "__context:test_context_delete_and_remove",
    ))
    .expect("Storage error");
    let persistent_storage = tmp_storage.storage();

    // init block with level 0 (because of commit)
    let block = dummy_block("BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe", 0)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // context
    let mut context = TezedgeContext::new(
        BlockStorage::new(&persistent_storage),
        persistent_storage.merkle(),
    );

    // add to context
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/0"),
        &vec![1, 2, 3, 4],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/1/a"),
        &vec![1, 2, 3, 4, 5],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/1/b"),
        &vec![1, 2, 3, 4, 5],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/2/a"),
        &vec![1, 2, 3, 4, 5, 61],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/2/b"),
        &vec![1, 2, 3, 4, 5, 62],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/index/123"),
        &vec![1, 2, 3, 4, 5, 6, 7],
    )?;

    // commit
    let context_hash_1: ContextHash = "CoUyfscSjC3XYECq1aFYQQLrVZuNSW17B7SbFDV9W1REfhJpxZwB".try_into()?;

    let hash = context.commit(
        &block.hash,
        &None,
        "Tezos".to_string(),
        "Genesis".to_string(),
        0,
    )?;
    assert_eq!(hash, context_hash_1);

    // get key from new commit
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/0"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1/a"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1/b"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/a"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5, 61]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/b"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5, 62]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    // insert another block with level 1
    let block = dummy_block("BKyQ9EofHrgaZKENioHyP4FZNsTmiSEcVmcghgzCC9cGhE7oCET", 1)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // checkout last commit to be modified
    context.checkout(&context_hash_1)?;

    // 1. remove rec
    context.remove_recursively_to_diff(
        &Some(context_hash_1.clone()),
        &context_key!("data/rolls/owner/current/cpu/2"),
    )?;
    context.delete_to_diff(
        &Some(context_hash_1.clone()),
        &context_key!("data/rolls/owner/current/cpu/1/b"),
    )?;

    // commit
    let context_hash_2: ContextHash = "CoVGom58bpVjHWVsKuc8k7JC7QyzZ7n4ntGZiPpw2CwM43sxC4XF".try_into()?;

    let hash = context.commit(
        &block.hash,
        &Some(context_hash_1),
        "Tezos".to_string(),
        "Genesis".to_string(),
        0,
    )?;
    assert_eq!(hash, context_hash_2);

    // get key from new commit
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/0"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1/a"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4, 5]
    );
    assert_data_deleted!(
        context,
        context_key!("data/rolls/owner/current/cpu/1/b"),
        context_hash_2.clone()
    );
    assert_data_deleted!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/a"),
        context_hash_2.clone()
    );
    assert_data_deleted!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/b"),
        context_hash_2.clone()
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    Ok(())
}

#[test]
pub fn test_context_copy() -> Result<(), failure::Error> {
    // prepare temp storage
    let tmp_storage =
        TmpStorage::create(test_storage_dir_path("__context:context_copy")).expect("Storage error");
    let persistent_storage = tmp_storage.storage();

    // init block with level 0 (because of commit)
    let block = dummy_block("BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe", 0)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // context
    let mut context = TezedgeContext::new(
        BlockStorage::new(&persistent_storage),
        persistent_storage.merkle(),
    );

    // add to context
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/0"),
        &vec![1, 2, 3, 4],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/1"),
        &vec![1, 2, 3, 4, 5],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/2/a"),
        &vec![1, 2, 3, 4, 5, 61],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/cpu/2/b"),
        &vec![1, 2, 3, 4, 5, 62],
    )?;
    context.set(
        &None,
        &context_key!("data/rolls/owner/current/index/123"),
        &vec![1, 2, 3, 4, 5, 6, 7],
    )?;

    // commit
    let context_hash_1: ContextHash = "CoVu1KaQQd2SFPqJh7go1t9q11upv1BewzShtTrNK7ZF6uCAcUQR".try_into()?;

    let hash = context.commit(
        &block.hash,
        &None,
        "Tezos".to_string(),
        "Genesis".to_string(),
        0,
    )?;
    assert_eq!(hash, context_hash_1);

    // get key from new commit
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/0"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/a"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5, 61]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/b"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5, 62]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        context_hash_1.clone(),
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    // insert another block with level 1
    let block = dummy_block("BKyQ9EofHrgaZKENioHyP4FZNsTmiSEcVmcghgzCC9cGhE7oCET", 1)?;
    let block_storage = BlockStorage::new(&persistent_storage);
    block_storage.put_block_header(&block)?;

    // checkout last commit to be modified
    context.checkout(&context_hash_1)?;

    // 1. copy
    context.copy_to_diff(
        &Some(context_hash_1.clone()),
        &context_key!("data/rolls/owner/current"),
        &context_key!("data/rolls/owner/snapshot/01/02"),
    )?;

    // commit
    let context_hash_2: ContextHash = "CoVX1ptKigdesVSqaREXTTHKGegLGM4x1bSSFvPgX5V8qj85r98G".try_into()?;

    let hash = context.commit(
        &block.hash,
        &Some(context_hash_1),
        "Tezos".to_string(),
        "Genesis".to_string(),
        0,
    )?;
    assert_eq!(hash, context_hash_2);

    // get key from new commit - original stays unchanged
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/0"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/1"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/a"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4, 5, 61]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/cpu/2/b"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4, 5, 62]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/current/index/123"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    // get key from new commit - original stays unchanged
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/cpu/0"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/cpu/1"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4, 5]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/cpu/2/a"),
        context_hash_2.clone(),
        vec![1, 2, 3, 4, 5, 61]
    );
    assert_data_eq!(
        context,
        context_key!("data/rolls/owner/snapshot/01/02/cpu/2/b"),
        context_hash_2.clone(),
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

fn dummy_block(block_hash: &str, level: i32) -> Result<BlockHeaderWithHash, failure::Error> {
    Ok(BlockHeaderWithHash {
        hash: block_hash.try_into()?,
        header: Arc::new(
            BlockHeaderBuilder::default()
                .level(level)
                .proto(0)
                .predecessor(
                    "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe".try_into()?,
                )
                .timestamp(5_635_634)
                .validation_pass(0)
                .operations_hash("LLoaGLRPRx3Zf8kB4ACtgku8F4feeBiskeb41J1ciwfcXB3KzHKXc".try_into()?)
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
        let data = $ctx.get_key_from_history(&$context_hash, &$key)?;
        assert!(data.is_some());
        assert_eq!(data.unwrap(), $data);
    }};
}

#[macro_export]
macro_rules! assert_data_deleted {
    ($ctx:expr, $key:expr, $context_hash:expr) => {{
        let data = $ctx.get_key_from_history(&$context_hash, &$key);
        assert!(data.is_ok());
        assert!(data.unwrap().is_none());
    }};
}

pub fn test_storage_dir_path(dir_name: &str) -> PathBuf {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR is not defined");
    let path = Path::new(out_dir.as_str())
        .join(Path::new(dir_name))
        .to_path_buf();
    path
}
