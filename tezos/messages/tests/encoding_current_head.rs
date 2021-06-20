// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryFrom;

use anyhow::Error;
use crypto::hash::ChainId;
use tezos_messages::p2p::{
    binary_message::{BinaryRead, BinaryWrite},
    encoding::prelude::*,
};

#[test]
fn can_serialize_get_current_head_message() -> Result<(), Error> {
    let response: PeerMessageResponse =
        GetCurrentHeadMessage::new(ChainId::try_from(hex::decode("8eceda2f")?)?).into();
    let message_bytes = response.as_bytes().unwrap();
    let expected_writer_result = hex::decode("0000000600138eceda2f").expect("Failed to decode");
    Ok(assert_eq!(expected_writer_result, message_bytes))
}

#[test]
fn can_deserialize_get_current_head_message_known_valid() -> Result<(), Error> {
    let message_bytes = hex::decode("0000010400148eceda2f000000ce0003be930116caa5bebae6c1997498bd90b2d2d6dcb14e2cc3a83b38067c784a0b485a4763000000005c8f572e049518937f78bbc2e2d460e7d26daa73c93763362c64c2059f5b7ecaba6e6f580d000000110000000100000000080000000000714aa08e289a17ee0bbd90ef57b80c52318829029fc9e17e4a782248755cdeaafd0dac000000000003e35a661200a75ebed94c886ce8c2700cc2fb38e301e7573f481eff49aea6892068cef7c9290947567e9df3a2cfc99ed9b0666f9c0291f586f65eb9e42cf4cdbef1ef8424d000000020c533d1d8a515b35fac67eb9926a6c983397208511ce69808d57177415654bf090000000400000000")?;
    let message = PeerMessageResponse::from_bytes(message_bytes)?;
    let message = message.message();

    match message {
        PeerMessage::CurrentHead(current_head_message) => {
            assert_eq!(
                &hex::decode("8eceda2f")?,
                current_head_message.chain_id().as_ref()
            );

            let block_header = current_head_message.current_block_header();
            assert_eq!(245_395, block_header.level());
            assert_eq!(1, block_header.proto());

            let expected_protocol_data = hex::decode("000000000003e35a661200a75ebed94c886ce8c2700cc2fb38e301e7573f481eff49aea6892068cef7c9290947567e9df3a2cfc99ed9b0666f9c0291f586f65eb9e42cf4cdbef1ef8424d0")?;
            assert_eq!(&expected_protocol_data, block_header.protocol_data());

            let mempool = current_head_message.current_mempool();
            assert_eq!(1, mempool.known_valid().len());
            let expected_known_valid =
                hex::decode("c533d1d8a515b35fac67eb9926a6c983397208511ce69808d57177415654bf09")?;
            assert_eq!(
                &expected_known_valid,
                mempool.known_valid().get(0).unwrap().as_ref()
            );
            Ok(assert_eq!(0, mempool.pending().len()))
        }
        _ => panic!("Unsupported encoding: {:?}", message),
    }
}

#[test]
fn can_deserialize_get_current_head_message_pending() -> Result<(), Error> {
    let message_bytes = hex::decode("0000012400148eceda2f000000ce0003cad5019b6feff784b018e609632b3bb66248a13c9efcaaaef34ddb805f07b4b2191760000000005c917b62045a8e2646ea6a2cf1ed392de0d4e2a45696a9662c0af212de73b72544357d757f00000011000000010000000008000000000072a3ce162dfbadfbdc34b00d694ba656c165b660643f6af5216a2462c52f0c41d98ee8000000000003d671d0520032846bbd8b2d6e10daa9cb6d6f82e4070d9c8047b081ea80cd6a473a3868135229c7650b7a6401d7c83c1b8896662faeb361a7bbb2f881ede725630e1ce344830000000000000044000000403cebec53e6ff9207dd669c3777cec4e74feadcd5f0131c819d261cdb0d9b5d9470669010ec4053d96d750daefbcdc1f51ed79f9e29fb16931515eccb84cb6a55")?;
    let message = PeerMessageResponse::from_bytes(message_bytes)?;
    let message = message.message();

    match message {
        PeerMessage::CurrentHead(current_head_message) => {
            assert_eq!(
                &hex::decode("8eceda2f")?,
                current_head_message.chain_id().as_ref()
            );

            let block_header = current_head_message.current_block_header();
            assert_eq!(248_533, block_header.level());
            assert_eq!(1, block_header.proto());

            let expected_protocol_data = hex::decode("000000000003d671d0520032846bbd8b2d6e10daa9cb6d6f82e4070d9c8047b081ea80cd6a473a3868135229c7650b7a6401d7c83c1b8896662faeb361a7bbb2f881ede725630e1ce34483")?;
            assert_eq!(&expected_protocol_data, block_header.protocol_data());

            let mempool = current_head_message.current_mempool();
            assert_eq!(0, mempool.known_valid().len());
            assert_eq!(2, mempool.pending().len());
            let expected_pending =
                hex::decode("3cebec53e6ff9207dd669c3777cec4e74feadcd5f0131c819d261cdb0d9b5d94")?;
            assert_eq!(
                &expected_pending,
                mempool.pending().get(0).unwrap().as_ref()
            );
            let expected_pending =
                hex::decode("70669010ec4053d96d750daefbcdc1f51ed79f9e29fb16931515eccb84cb6a55")?;
            Ok(assert_eq!(
                &expected_pending,
                mempool.pending().get(1).unwrap().as_ref()
            ))
        }
        _ => panic!("Unsupported encoding: {:?}", message),
    }
}
