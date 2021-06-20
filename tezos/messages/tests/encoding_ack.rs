// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;

use tezos_messages::p2p::encoding::prelude::*;
use tezos_messages::p2p::{
    binary_message::{BinaryRead, BinaryWrite},
    encoding::ack::{NackInfo, NackMotive},
};

#[test]
fn can_serialize_ack() -> Result<(), Error> {
    let message = AckMessage::Ack;
    let serialized = hex::encode(message.as_bytes()?);
    let expected = "00";
    Ok(assert_eq!(expected, &serialized))
}

#[test]
fn can_deserialize_ack() -> Result<(), Error> {
    let message_bytes = hex::decode("00")?;
    let message = AckMessage::from_bytes(message_bytes)?;
    Ok(assert_eq!(AckMessage::Ack, message))
}

#[test]
fn can_serialize_nack() -> Result<(), Error> {
    let message = AckMessage::NackV0;
    let serialized = hex::encode(message.as_bytes()?);
    let expected = "ff";
    Ok(assert_eq!(expected, &serialized))
}

#[test]
fn can_deserialize_nack() -> Result<(), Error> {
    let message_bytes = hex::decode("ff")?;
    let message = AckMessage::from_bytes(message_bytes)?;
    Ok(assert_eq!(AckMessage::NackV0, message))
}

#[test]
fn can_serialize_nack_with_list() -> Result<(), Error> {
    let message = AckMessage::Nack(NackInfo::new(
        NackMotive::DeprecatedP2pVersion,
        &vec![String::from("127.0.0.1:9832")],
    ));
    let serialized = hex::encode(message.as_bytes()?);
    let expected = "010003000000120000000e3132372e302e302e313a39383332";
    Ok(assert_eq!(expected, &serialized))
}

#[test]
fn can_deserialize_nack_with_list() -> Result<(), Error> {
    let message_bytes = hex::decode("010002000000120000000e3132372e302e302e313a39383332")?;
    let message = AckMessage::from_bytes(message_bytes)?;
    Ok(assert_eq!(
        AckMessage::Nack(NackInfo::new(
            NackMotive::UnknownChainName,
            &vec![String::from("127.0.0.1:9832")]
        )),
        message,
    ))
}
