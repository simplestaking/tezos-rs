// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;
use crypto::hash::OperationHash;

use storage::mempool_storage::MempoolOperationType;
use storage::tests_common::TmpStorage;
use storage::MempoolStorage;
use tezos_messages::p2p::binary_message::BinaryRead;
use tezos_messages::p2p::binary_message::MessageHash;
use tezos_messages::p2p::encoding::prelude::*;

#[test]
fn mempool_storage_read_write() -> Result<(), Error> {
    let tmp_storage = TmpStorage::create("__mempool_storage_read_write")?;
    let mut storage = MempoolStorage::new(tmp_storage.storage());

    let operation = make_test_operation_message()?;
    let operation_hash = operation.message_typed_hash::<OperationHash>()?;

    storage.put_known_valid(operation.clone())?;
    let block_header_res = storage
        .get(MempoolOperationType::KnownValid, operation_hash.clone())?
        .unwrap();
    assert_eq!(block_header_res, operation);

    assert!(storage.find(&operation_hash)?.is_some());
    storage.delete(&operation_hash)?;
    assert!(storage.find(&operation_hash)?.is_none());

    Ok(())
}

fn make_test_operation_message() -> Result<OperationMessage, Error> {
    let message_bytes = hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08")?;
    let operation = Operation::from_bytes(message_bytes)?;
    Ok(operation.into())
}
