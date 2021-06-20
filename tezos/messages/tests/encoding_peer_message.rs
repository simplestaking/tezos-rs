// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;
use tezos_messages::p2p::{binary_message::BinaryRead, encoding::prelude::*};

#[test]
fn can_t_deserialize_empty_message() -> Result<(), Error> {
    // dynamic block of size 0
    let bytes = hex::decode("00000000")?;
    let _err = PeerMessageResponse::from_bytes(bytes).expect_err("Error is expected");
    Ok(())
}

#[test]
fn can_t_deserialize_message_list() -> Result<(), Error> {
    // dynamic block of size 2, bootstrap + disconnect
    let bytes = hex::decode("0000000400020001")?;
    let _err = PeerMessageResponse::from_bytes(bytes).expect_err("Error is expected");
    Ok(())
}
