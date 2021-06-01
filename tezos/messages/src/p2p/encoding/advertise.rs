// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::net::SocketAddr;

use getset::Getters;
use serde::{Deserialize, Serialize};

use tezos_encoding::encoding::{Encoding, Field, HasEncoding};
use tezos_encoding::has_encoding;

use super::limits::{ADVERTISE_ID_LIST_MAX_LENGTH, P2P_POINT_MAX_SIZE};
use crate::cached_data;
use crate::p2p::binary_message::cache::BinaryDataCache;

#[cfg(feature = "arbitrary-fuzz")]
use arbitrary::Arbitrary;

#[cfg_attr(feature = "arbitrary-fuzz", derive(Arbitrary))]
#[derive(Serialize, Deserialize, Debug, Getters, Clone)]
pub struct AdvertiseMessage {
    #[get = "pub"]
    id: Vec<String>,

    #[serde(skip_serializing)]
    body: BinaryDataCache,
}

impl AdvertiseMessage {
    pub fn new(addresses: &[SocketAddr]) -> Self {
        Self {
            id: addresses
                .iter()
                .map(|address| format!("{}", address))
                .collect(),
            body: Default::default(),
        }
    }
}

cached_data!(AdvertiseMessage, body);
has_encoding!(AdvertiseMessage, ADVERTISE_MESSAGE_ENCODING, {
    Encoding::Obj(
        "AdvertiseMessage",
        vec![Field::new(
            "id",
            Encoding::bounded_list(
                ADVERTISE_ID_LIST_MAX_LENGTH,
                Encoding::bounded(P2P_POINT_MAX_SIZE, Encoding::String),
            ),
        )],
    )
});
