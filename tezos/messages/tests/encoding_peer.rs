// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;
use tezos_messages::p2p::{
    binary_message::{BinaryRead, BinaryWrite},
    encoding::prelude::*,
};

#[test]
fn can_deserialize_bootstrap() -> Result<(), Error> {
    let message_bytes = hex::decode("000000020002")?;
    let messages = PeerMessageResponse::from_bytes(message_bytes).unwrap();

    let message = messages.message();
    match message {
        PeerMessage::Bootstrap => Ok(()),
        _ => panic!("Unsupported encoding: {:?}", message),
    }
}

#[test]
fn can_serialize_bootstrap() -> Result<(), Error> {
    let message = PeerMessageResponse::from(PeerMessage::Bootstrap);
    let serialized = hex::encode(message.as_bytes()?);
    let expected = "000000020002";
    Ok(assert_eq!(expected, &serialized))
}
