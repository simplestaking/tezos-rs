// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;
use crypto::hash::HashType;
use tezos_messages::p2p::binary_message::{BinaryRead, MessageHash};
use tezos_messages::p2p::encoding::prelude::*;

#[test]
fn can_deserialize_block_header() -> Result<(), Error> {
    let message_bytes = hex::decode("00006d6e0102dd00defaf70c53e180ea148b349a6feb4795610b2abc7b07fe91ce50a90814000000005c1276780432bc1d3a28df9a67b363aa1638f807214bb8987e5f9c0abcbd69531facffd1c80000001100000001000000000800000000000c15ef15a6f54021cb353780e2847fb9c546f1d72c1dc17c3db510f45553ce501ce1de000000000003c762c7df00a856b8bfcaf0676f069f825ca75f37f2bee9fe55ba109cec3d1d041d8c03519626c0c0faa557e778cb09d2e0c729e8556ed6a7a518c84982d1f2682bc6aa753f")?;
    let block_header = BlockHeader::from_bytes(message_bytes)?;
    assert_eq!(28014, block_header.level());
    assert_eq!(1, block_header.proto());
    assert_eq!(4, block_header.validation_pass());
    assert_eq!(2, block_header.fitness().len());
    assert_eq!(1544713848, block_header.timestamp());
    assert_eq!("000000000003c762c7df00a856b8bfcaf0676f069f825ca75f37f2bee9fe55ba109cec3d1d041d8c03519626c0c0faa557e778cb09d2e0c729e8556ed6a7a518c84982d1f2682bc6aa753f", &hex::encode(block_header.protocol_data()));
    assert_eq!(
        "BKoBK7Qa8J4Wvz85MDRWmpAntd5UhPhCh3p6Ga6woJywF8cZkeJ",
        HashType::BlockHash.hash_to_b58check(&block_header.message_hash()?)?
    );
    assert_eq!(
        "BKjYUUtYXtXjEuL49jB8ZbFwVdg4hU6U7oKKSC5vp6stYsfFDVN",
        block_header.predecessor().to_base58_check()
    );
    assert_eq!(
        "LLoZi3xywrX9swZQgC82m7vj5hmuz6LGAatNq2Muh34oNn71JruZs",
        block_header.operations_hash().to_base58_check()
    );
    Ok(assert_eq!(
        "CoUoqw1cVKUUNWyAviph5cdsjDpgeNhH2DGkMtgy7N6kfwnbewvS",
        block_header.context().to_base58_check()
    ))
}

#[test]
fn can_deserialize_get_block_headers() -> Result<(), Error> {
    let message_bytes = hex::decode(
        "000000260020000000202253698f0c94788689fb95ca35eb1535ec3a8b7c613a97e6683f8007d7959e4b",
    )?;
    let messages = PeerMessageResponse::from_bytes(message_bytes)?;

    let message = messages.message();
    match message {
        PeerMessage::GetBlockHeaders(message) => {
            assert_eq!(1, message.get_block_headers().len());
            Ok(assert_eq!(
                "BKyQ9EofHrgaZKENioHyP4FZNsTmiSEcVmcghgzCC9cGhE7oCET",
                message
                    .get_block_headers()
                    .get(0)
                    .unwrap()
                    .to_base58_check()
            ))
        }
        _ => panic!("Unsupported encoding: {:?}", message),
    }
}
