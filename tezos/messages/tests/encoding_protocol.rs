// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;
use tezos_messages::p2p::binary_message::BinaryRead;
use tezos_messages::p2p::encoding::prelude::*;

#[test]
fn can_deserialize_protocol() -> Result<(), Error> {
    let message_bytes = hex::decode(include_str!("resources/encoding_protocol.bytes"))?;
    let message = Protocol::from_bytes(message_bytes)?;
    assert_eq!(68, message.components().len());
    Ok(assert_eq!(0, message.expected_env_version()))
}
