// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use getset::Getters;
use serde::{Deserialize, Serialize};

use crypto::hash::OperationHash;
use tezos_encoding::encoding::HasEncoding;
use tezos_encoding::nom::NomReader;

use super::limits::MEMPOOL_MAX_SIZE;

#[derive(
    Clone, Serialize, Deserialize, Debug, Default, Getters, HasEncoding, NomReader, PartialEq,
)]
#[encoding(bounded = "MEMPOOL_MAX_SIZE")]
pub struct Mempool {
    #[get = "pub"]
    #[encoding(dynamic, list)]
    known_valid: Vec<OperationHash>,
    #[get = "pub"]
    #[encoding(dynamic, dynamic, list)]
    pending: Vec<OperationHash>,
}

impl Mempool {
    pub fn new(known_valid: Vec<OperationHash>, pending: Vec<OperationHash>) -> Self {
        Mempool {
            known_valid,
            pending,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.known_valid.is_empty() && self.pending.is_empty()
    }
}
