// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryFrom;

use anyhow::Error;
use crypto::hash::BlockHash;

use storage::tests_common::TmpStorage;
use storage::*;
use tezos_messages::p2p::encoding::prelude::*;

#[test]
fn test_get_operations() -> Result<(), Error> {
    let tmp_storage = TmpStorage::create("__op_storage_get_operations")?;

    let block_hash_1 = BlockHash::try_from("BKyQ9EofHrgaZKENioHyP4FZNsTmiSEcVmcghgzCC9cGhE7oCET")?;
    let block_hash_2 = BlockHash::try_from("BLaf78njreWdt2WigJjM9e3ecEdVKm5ehahUfYBKvcWvZ8vfTcJ")?;
    let block_hash_3 = BlockHash::try_from("BKzyxvaMgoY5M3BUD7UaUCPivAku2NRiYRA1z1LQUzB7CX6e8yy")?;

    let storage = OperationsStorage::new(tmp_storage.storage());
    let message = OperationsForBlocksMessage::new(
        OperationsForBlock::new(block_hash_1.clone(), 3),
        Path::op(),
        vec![],
    );
    storage.put_operations(&message)?;
    let message = OperationsForBlocksMessage::new(
        OperationsForBlock::new(block_hash_1.clone(), 1),
        Path::op(),
        vec![],
    );
    storage.put_operations(&message)?;
    let message = OperationsForBlocksMessage::new(
        OperationsForBlock::new(block_hash_1.clone(), 0),
        Path::op(),
        vec![],
    );
    storage.put_operations(&message)?;
    let message = OperationsForBlocksMessage::new(
        OperationsForBlock::new(block_hash_2.clone(), 1),
        Path::op(),
        vec![],
    );
    storage.put_operations(&message)?;
    let message = OperationsForBlocksMessage::new(
        OperationsForBlock::new(block_hash_1.clone(), 2),
        Path::op(),
        vec![],
    );
    storage.put_operations(&message)?;
    let message = OperationsForBlocksMessage::new(
        OperationsForBlock::new(block_hash_3.clone(), 3),
        Path::op(),
        vec![],
    );
    storage.put_operations(&message)?;

    let operations = storage.get_operations(&block_hash_1)?;
    assert_eq!(
        4,
        operations.len(),
        "Was expecting vector of {} elements but instead found {}",
        4,
        operations.len()
    );
    for i in 0..4 {
        assert_eq!(
            i as i8,
            operations[i].operations_for_block().validation_pass(),
            "Was expecting operation pass {} but found {}",
            i,
            operations[i].operations_for_block().validation_pass()
        );
        assert_eq!(
            &block_hash_1,
            operations[i].operations_for_block().hash(),
            "Block hash mismatch"
        );
    }
    let operations = storage.get_operations(&block_hash_2)?;
    assert_eq!(
        1,
        operations.len(),
        "Was expecting vector of {} elements but instead found {}",
        1,
        operations.len()
    );
    let operations = storage.get_operations(&block_hash_3)?;
    assert_eq!(
        1,
        operations.len(),
        "Was expecting vector of {} elements but instead found {}",
        1,
        operations.len()
    );

    Ok(())
}
