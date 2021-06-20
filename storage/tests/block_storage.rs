// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryInto;

use anyhow::Error;

use crypto::hash::HashType;
use storage::tests_common::TmpStorage;
use storage::*;
use tezos_messages::p2p::binary_message::BinaryRead;
use tezos_messages::p2p::encoding::prelude::*;

#[test]
fn block_storage_read_write() -> Result<(), Error> {
    let tmp_storage = TmpStorage::create_to_out_dir("__block_basictest")?;
    let storage = BlockStorage::new(tmp_storage.storage());

    let block_header = make_test_block_header()?;

    storage.put_block_header(&block_header)?;
    let block_header_res = storage.get(&block_header.hash)?.unwrap();
    assert_eq!(block_header_res, block_header);

    Ok(())
}

#[test]
fn test_put_block_header_twice() -> Result<(), Error> {
    let tmp_storage = TmpStorage::create_to_out_dir("__block_storage_write_twice")?;
    let storage = BlockStorage::new(tmp_storage.storage());

    // test block
    let block_header = make_test_block_header()?;

    // write block to storage FIRST time
    storage.put_block_header(&block_header)?;

    // check primary_index and commit_log
    assert_eq!(block_header, storage.get(&block_header.hash)?.unwrap());

    // stored in commit_log
    let stored_location1 = storage.get_location(&block_header.hash)?.unwrap();
    assert_eq!(0, stored_location1.block_header.0);

    // write the same block to storage SECOND time
    storage.put_block_header(&block_header)?;
    assert_eq!(block_header, storage.get(&block_header.hash)?.unwrap());

    // location shold not be updated, means, header is not rewritten or writen twice in commit_log
    let stored_location2 = storage.get_location(&block_header.hash)?.unwrap();
    assert_eq!(
        stored_location1.block_header.0,
        stored_location2.block_header.0
    );

    Ok(())
}

#[test]
fn block_storage_assign_context() -> Result<(), Error> {
    let tmp_storage = TmpStorage::create_to_out_dir("__block_assign_to_context")?;
    let storage = BlockStorage::new(tmp_storage.storage());

    let block_header = make_test_block_header()?;
    let context_hash = vec![1; HashType::ContextHash.size()].try_into()?;

    storage.put_block_header(&block_header)?;
    storage.assign_to_context(&block_header.hash, &context_hash)?;
    let block_header_res = storage.get_by_context_hash(&context_hash)?.unwrap();
    assert_eq!(block_header_res, block_header);

    Ok(())
}

fn make_test_block_header() -> Result<BlockHeaderWithHash, Error> {
    let message_bytes = hex::decode("00006d6e0102dd00defaf70c53e180ea148b349a6feb4795610b2abc7b07fe91ce50a90814000000005c1276780432bc1d3a28df9a67b363aa1638f807214bb8987e5f9c0abcbd69531facffd1c80000001100000001000000000800000000000c15ef15a6f54021cb353780e2847fb9c546f1d72c1dc17c3db510f45553ce501ce1de000000000003c762c7df00a856b8bfcaf0676f069f825ca75f37f2bee9fe55ba109cec3d1d041d8c03519626c0c0faa557e778cb09d2e0c729e8556ed6a7a518c84982d1f2682bc6aa753f")?;
    let block_header = BlockHeaderWithHash::new(BlockHeader::from_bytes(message_bytes)?)?;
    Ok(block_header)
}
