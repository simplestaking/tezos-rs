// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crypto::hash::CryptoboxPublicKeyHash;
use getset::Getters;
use serde::{Deserialize, Serialize};

use tezos_encoding::encoding::HasEncoding;
use tezos_encoding::nom::NomReader;

use super::limits::P2P_POINT_MAX_SIZE;

#[derive(Serialize, Deserialize, Debug, Getters, Clone, HasEncoding, NomReader)]
pub struct SwapMessage {
    #[get = "pub"]
    #[encoding(bounded = "P2P_POINT_MAX_SIZE")]
    point: String,
    #[get = "pub"]
    peer_id: CryptoboxPublicKeyHash,
}

impl SwapMessage {
    pub fn new(point: String, peer_id: CryptoboxPublicKeyHash) -> Self {
        Self { point, peer_id }
    }
}
