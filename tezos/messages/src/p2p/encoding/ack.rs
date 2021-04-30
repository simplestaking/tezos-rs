// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::fmt;

use getset::Getters;
use nom::{
    branch::alt,
    bytes::complete::{tag, take},
    combinator::{map, success},
    sequence::preceded,
};
use serde::{Deserialize, Serialize};
use quickcheck_derive::Arbitrary;
use quickcheck::Arbitrary;

use tezos_encoding::{
    encoding::HasEncoding,
    nom::{size, NomReader},
};

use crate::p2p::binary_message::{complete_input, SizeFromChunk};

use super::limits::{NACK_PEERS_MAX_LENGTH, P2P_POINT_MAX_SIZE};

#[derive(Serialize, Deserialize, PartialEq, Debug, HasEncoding, NomReader, Arbitrary, Clone)]
pub enum AckMessage {
    #[encoding(tag = 0x00)]
    Ack,
    #[encoding(tag = 0xff)]
    NackV0,
    #[encoding(tag = 0x01)]
    Nack(NackInfo),
}

impl SizeFromChunk for AckMessage {
    fn size_from_chunk(
        bytes: impl AsRef<[u8]>,
    ) -> Result<usize, tezos_encoding::binary_reader::BinaryReaderError> {
        let bytes = bytes.as_ref();
        let size = complete_input(
            alt((
                preceded(tag(0x00u8.to_be_bytes()), success(1)),
                preceded(tag(0xffu8.to_be_bytes()), success(1)),
                preceded(
                    tag(0x01u8.to_be_bytes()),
                    map(preceded(take(2usize), size), |s| (s as usize) + 3),
                ),
            )),
            bytes,
        )?;
        Ok(size as usize)
    }
}

#[derive(Serialize, Deserialize, Getters, PartialEq, HasEncoding, NomReader, Clone)]
pub struct NackInfo {
    #[get = "pub"]
    motive: NackMotive,
    #[get = "pub"]
    #[encoding(
        dynamic,
        list = "NACK_PEERS_MAX_LENGTH",
        bounded = "P2P_POINT_MAX_SIZE"
    )]
    potential_peers_to_connect: Vec<String>,
}

impl Arbitrary for NackInfo {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        NackInfo {
            motive: NackMotive::arbitrary(g),
            potential_peers_to_connect: vec![],
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, HasEncoding, NomReader, Arbitrary, Clone)]
#[encoding(tags = "u16")]
pub enum NackMotive {
    NoMotive,
    TooManyConnections,
    UnknownChainName,
    DeprecatedP2pVersion,
    DeprecatedDistributedDbVersion,
    AlreadyConnected,
}

impl NackInfo {
    pub fn new(motive: NackMotive, potential_peers_to_connect: &[String]) -> Self {
        Self {
            motive,
            potential_peers_to_connect: potential_peers_to_connect.to_vec(),
        }
    }
}

impl fmt::Debug for NackInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let potential_peers_to_connect = self.potential_peers_to_connect.join(", ");
        write!(
            f,
            "motive: {}, potential_peers_to_connect: {:?}",
            &self.motive, potential_peers_to_connect
        )
    }
}

impl fmt::Display for NackMotive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let motive = match &self {
            NackMotive::NoMotive => "No_motive",
            NackMotive::TooManyConnections => "Too_many_connections ",
            NackMotive::UnknownChainName => "Unknown_chain_name",
            NackMotive::DeprecatedP2pVersion => "Deprecated_p2p_version",
            NackMotive::DeprecatedDistributedDbVersion => "Deprecated_distributed_db_version",
            NackMotive::AlreadyConnected => "Already_connected",
        };
        write!(f, "{}", motive)
    }
}

impl fmt::Debug for NackMotive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self)
    }
}
