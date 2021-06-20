// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryInto;

use anyhow::Error;
use crypto::hash::HashType;
use tezos_messages::p2p::encoding::operations_for_blocks::PathItem;
use tezos_messages::p2p::{
    binary_message::{BinaryRead, BinaryWrite},
    encoding::prelude::*,
};

#[test]
fn can_deserialize_get_operations_for_blocks() -> Result<(), Error> {
    let message_bytes = hex::decode("0000008a006000000084ed4197d381a4d4f56be30bf7157426671276aa187bbe0bb9484974af59e069aa01ed4197d381a4d4f56be30bf7157426671276aa187bbe0bb9484974af59e069aa02ed4197d381a4d4f56be30bf7157426671276aa187bbe0bb9484974af59e069aa00ed4197d381a4d4f56be30bf7157426671276aa187bbe0bb9484974af59e069aa03")?;
    let message = PeerMessageResponse::from_bytes(message_bytes)?;

    let message = message.message();
    match message {
        PeerMessage::GetOperationsForBlocks(message) => {
            let operations = message.get_operations_for_blocks();
            assert_eq!(4, operations.len());
            assert_eq!(
                "BMWmj9CTojf7AnA8ZQFWGkh1cXB6FkST8Ey5coaeHX6cVNAZqA6",
                operations[0].hash().to_base58_check()
            );
            Ok(assert_eq!(1, operations[0].validation_pass()))
        }
        _ => panic!("Unsupported encoding: {:?}", message),
    }
}

#[test]
fn can_deserialize_operations_for_blocks_right() -> Result<(), Error> {
    let message_bytes = hex::decode("000000660061b12238a7c3577d725939970800ade6b82d94a231e855b46af46c37850dd02452030ffe7601035ca2892f983c10203656479cfd2f8a4ea656f300cd9d68f74aa625870f7c09f7c4d76ace86e1a7e1c7dc0a0c7edcaa8b284949320081131976a87760c300")?;
    let message = PeerMessageResponse::from_bytes(message_bytes)?;

    let message = message.message();
    match message {
        PeerMessage::OperationsForBlocks(message) => {
            assert_eq!(
                "BM4Hyf4ay3u2PcUBmumTEPcWW8Z7t45HXGZAjLNnenSC2f8bLte",
                message.operations_for_block().hash().to_base58_check()
            );

            let Path(path_items) = message.operation_hashes_path();
            assert_eq!(path_items.len(), 2);
            match path_items[0] {
                PathItem::Right(ref path) => {
                    assert_eq!(
                        "LLobFmsoFEGPP3q9ZxpE84rH1vPC1uKqEV8L1x8zUjGwanEYuHBVB",
                        HashType::OperationListListHash
                            .hash_to_b58check(path.left())
                            .unwrap()
                    );
                }
                _ => panic!(
                    "Unexpected path: {:?}. Was expecting Path::Left.",
                    message.operation_hashes_path()
                ),
            }
            match path_items[1] {
                PathItem::Right(ref path) => {
                    assert_eq!(
                        "LLoaGLRPRx3Zf8kB4ACtgku8F4feeBiskeb41J1ciwfcXB3KzHKXc",
                        HashType::OperationListListHash
                            .hash_to_b58check(path.left())
                            .unwrap()
                    );
                }
                _ => panic!(
                    "Unexpected path: {:?}. Was expecting Path::Left.",
                    message.operation_hashes_path()
                ),
            }
            Ok(())
        }
        _ => panic!("Unsupported encoding: {:?}", message),
    }
}

#[test]
fn can_deserialize_operations_for_blocks_left() -> Result<(), Error> {
    let message_bytes = hex::decode("0000027300613158c8503e7cd436d09a8a6320cd57014870a96f178915be25551e435d0830ab00f0f0007c09f7c4d76ace86e1a7e1c7dc0a0c7edcaa8b284949320081131976a87760c30a37f18e2562ae14388716247be0d4e451d72ce38d1d4a30f92d2f6ef95b4919000000658a7912f9de23a446748861d2667ffa3b4463ed236689492c74703cef598e6f3f0000002eb6d1852a1f397619b16f08121fb01d43a9bf4ded283ab0d96fd114028251690506a7ec514f0b297b6cdc8ff54a658f27f7635d201c61479cd48007c0096752fb0c000000658a7912f9de23a446748861d2667ffa3b4463ed236689492c74703cef598e6f3f0000002eb62b8768820e6b7343c32382544d0fa0f044289fd1b86ee5c66e36396bc9bc2492314543667770959449943d222ffd7f7cd8e3ad8eda9d21a8a5e9e34c73c0c9e3000000658a7912f9de23a446748861d2667ffa3b4463ed236689492c74703cef598e6f3f0000002eb6c5d4ac0ba67f6509fec4ae196d1cb7ccf8ee7a35bc06d362d69291631a5a07b511252c70d59ff94dc4071525dd6c22354349702c9821d80c748a15913f11b1d1000000658a7912f9de23a446748861d2667ffa3b4463ed236689492c74703cef598e6f3f0000002eb63d61de83c6f71ca631903f29be9040f63dbf5d00d7994a8420210270aa2c37e245ce70e8f4d7d384f342f7e6b6797c5f237ae1846a8b8652838663d1d0df91a0000000658a7912f9de23a446748861d2667ffa3b4463ed236689492c74703cef598e6f3f0000002eb6c69c651e14357c3a895cd6465fc1e3b1fd19b0d805efae484f2632e006101b9c80c28c92dcfbf58b99392b2108b286fd28039ddd72294929c2fbf9dda65acf01")?;
    let message = PeerMessageResponse::from_bytes(message_bytes)?;

    let message = message.message();
    match message {
        PeerMessage::OperationsForBlocks(message) => {
            assert_eq!(
                "BL61qJKRdXg6i628H62DyDqBNotK7f6CZrHGv4k7jEe8a86B7n8",
                message.operations_for_block().hash().to_base58_check()
            );
            assert_eq!(
                5,
                message.operations().len(),
                "Was expecting 5 operations but found {}",
                message.operations().len()
            );
            let Path(path_items) = message.operation_hashes_path();
            assert_eq!(path_items.len(), 2);
            match path_items[0] {
                PathItem::Left(ref path) => {
                    assert_eq!(
                        "LLoZQD2o1hNgoUhg6ha9dCVyRUY25GX1KN2TttXW2PZsyS8itbfpK",
                        HashType::OperationListListHash
                            .hash_to_b58check(path.right())
                            .unwrap()
                    );
                }
                _ => panic!(
                    "Unexpected path: {:?}. Was expecting Path::Left.",
                    message.operation_hashes_path()
                ),
            }
            match path_items[1] {
                PathItem::Left(ref path) => {
                    assert_eq!(
                        "LLoaGLRPRx3Zf8kB4ACtgku8F4feeBiskeb41J1ciwfcXB3KzHKXc",
                        HashType::OperationListListHash
                            .hash_to_b58check(path.right())
                            .unwrap()
                    );
                }
                _ => panic!(
                    "Unexpected path: {:?}. Was expecting Path::Left.",
                    message.operation_hashes_path()
                ),
            }
            Ok(())
        }
        _ => panic!("Unsupported encoding: {:?}", message),
    }
}

const LEFT_BOUNDARY: u8 = 0xB0;
const RIGHT_BOUNDARY: u8 = 0x0E;

// Returns a byte vector of specified size, with BE encoding of the num
// as its last 8 bytes
fn get_hash(num: u64, size: usize) -> Vec<u8> {
    use std::mem::size_of;
    assert!(size >= size_of::<u64>() + 2);
    let mut vec = Vec::new();
    vec.push(LEFT_BOUNDARY);
    vec.resize(size - size_of::<u64>() - 1, 0x11);
    vec.push(RIGHT_BOUNDARY);
    vec.append(&mut num.to_be_bytes().to_vec());
    vec
}

fn create_operations_for_blocks_encoded(depth: usize) -> Vec<u8> {
    let mut vec = Vec::new();

    // size is
    // 2 bytes for struct tag
    // 32 bytes for operations for block hash
    // 1 byte for operations for block validation
    // depth bytes for Left tags
    // 1 byte for Op tag
    // depth * 32 for right hashes
    let size = (2 + 32 + 1 + depth + 1 + (32 * depth)) as u32;

    // operation message tag
    vec.extend_from_slice(&size.to_be_bytes());

    // struct tag
    vec.extend_from_slice(&0x0061_u16.to_be_bytes());

    // operations for block hash
    vec.extend_from_slice(&get_hash(0xffffffff_u64, 32));

    // operations for block validation
    vec.push(0x01);

    // Left tags
    vec.append(&mut vec![0xf0].repeat(depth as usize));

    // Op tag
    vec.push(0x00);

    // right hashes
    for i in 0..depth {
        vec.append(&mut get_hash((depth - i - 1) as u64, 32));
    }

    vec
}

fn create_operations_for_blocks(depth: usize) -> PeerMessageResponse {
    let path = (0..depth)
        .map(|i| get_hash(i as u64, 32))
        .map(|h| PathItem::left(h))
        .collect();
    let message = PeerMessage::OperationsForBlocks(OperationsForBlocksMessage::new(
        OperationsForBlock::new(get_hash(0xffffffff_u64, 32).try_into().unwrap(), 0x01),
        Path(path),
        Vec::new(),
    ));

    message.into()
}

#[test]
fn can_deserialize_operations_for_blocks_left_deep() -> Result<(), Error> {
    let depth = MAX_PASS_MERKLE_DEPTH.expect("Bounded encoding expected");
    let message_bytes = create_operations_for_blocks_encoded(depth as usize);
    let message = PeerMessageResponse::from_bytes(message_bytes)?;

    let message = message.message();
    if let PeerMessage::OperationsForBlocks(message) = message {
        let Path(path) = message.operation_hashes_path();
        for item in path {
            if let PathItem::Left(_) = item {
            } else {
                panic!("Unexpected path kind");
            }
        }
        Ok(())
    } else {
        panic!("Invalid message");
    }
}

#[test]
fn can_deserialize_operations_for_blocks_left_too_deep() -> Result<(), Error> {
    let message_bytes = create_operations_for_blocks_encoded(
        MAX_PASS_MERKLE_DEPTH.expect("Bounded encoding expected") as usize + 1,
    );
    let result = PeerMessageResponse::from_bytes(message_bytes);
    assert!(result.is_err());

    Ok(())
}

#[test]
fn can_deserialize_operations_for_blocks_no_stack_overflow() -> Result<(), Error> {
    // encoding for 512 step path, causing stack overflow previously
    let message_bytes = create_operations_for_blocks_encoded(512);
    let result = PeerMessageResponse::from_bytes(message_bytes);
    assert!(result.is_err());

    Ok(())
}

#[test]
fn can_serialize_operations_for_blocks_left_deep() -> Result<(), Error> {
    let depth = MAX_PASS_MERKLE_DEPTH.expect("Bounded encoding expected");
    let message = create_operations_for_blocks(depth);
    let encoded = PeerMessageResponse::from(message).as_bytes()?;
    let expected = create_operations_for_blocks_encoded(depth);
    assert_eq!(encoded, expected);

    Ok(())
}

#[test]
fn can_serialize_operations_for_blocks_left_too_deep() -> Result<(), Error> {
    let message =
        create_operations_for_blocks(MAX_PASS_MERKLE_DEPTH.expect("Bounded encoding expected") + 1);
    let result = PeerMessageResponse::from(message).as_bytes();
    assert!(result.is_err());

    Ok(())
}
