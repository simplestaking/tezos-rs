// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use lazy_static::lazy_static;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use thiserror::Error;

use crypto::hash::ProtocolHash;
use tezos_encoding::binary_reader::BinaryReaderError;

use crate::{
    base::rpc_support::{RpcJsonMap, ToRpcJsonMap},
    p2p::binary_message::BinaryRead,
};

pub mod proto_001;
pub mod proto_002;
pub mod proto_003;
pub mod proto_004;
pub mod proto_005;
pub mod proto_005_2;
pub mod proto_006;
pub mod proto_007;
pub mod proto_008;
pub mod proto_008_2;

lazy_static! {
    pub static ref SUPPORTED_PROTOCOLS: HashMap<String, SupportedProtocol> = init();
}

fn init() -> HashMap<String, SupportedProtocol> {
    let mut protos: HashMap<String, SupportedProtocol> = HashMap::new();
    for sp in SupportedProtocol::iter() {
        protos.insert(sp.protocol_hash(), sp);
    }
    protos
}

#[derive(EnumIter, Clone, Debug, PartialEq)]
pub enum SupportedProtocol {
    Proto001,
    Proto002,
    Proto003,
    Proto004,
    Proto005,
    Proto005_2,
    Proto006,
    Proto007,
    Proto008,
    Proto008_2,
}

impl SupportedProtocol {
    pub fn protocol_hash(&self) -> String {
        match self {
            SupportedProtocol::Proto001 => proto_001::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto002 => proto_002::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto003 => proto_003::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto004 => proto_004::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto005 => proto_005::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto005_2 => proto_005_2::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto006 => proto_006::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto007 => proto_007::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto008 => proto_008::PROTOCOL_HASH.to_string(),
            SupportedProtocol::Proto008_2 => proto_008_2::PROTOCOL_HASH.to_string(),
        }
    }
}

#[derive(Debug, Error)]
#[error("Protocol {protocol} is not yet supported!")]
pub struct UnsupportedProtocolError {
    pub protocol: String,
}

impl TryFrom<&ProtocolHash> for SupportedProtocol {
    type Error = UnsupportedProtocolError;

    fn try_from(protocol_hash: &ProtocolHash) -> Result<Self, Self::Error> {
        let protocol = protocol_hash.to_base58_check();
        match SUPPORTED_PROTOCOLS.get(&protocol) {
            Some(proto) => Ok(proto.clone()),
            None => Err(UnsupportedProtocolError { protocol }),
        }
    }
}

impl TryFrom<ProtocolHash> for SupportedProtocol {
    type Error = UnsupportedProtocolError;

    fn try_from(protocol_hash: ProtocolHash) -> Result<Self, Self::Error> {
        (&protocol_hash).try_into()
    }
}

#[derive(Debug, Error)]
#[error("Decode protocol constants error, reason: {reason}")]
pub struct ContextConstantsDecodeError {
    reason: BinaryReaderError,
}

impl From<BinaryReaderError> for ContextConstantsDecodeError {
    fn from(error: BinaryReaderError) -> Self {
        ContextConstantsDecodeError { reason: error }
    }
}

pub fn get_constants_for_rpc(
    bytes: &[u8],
    protocol: &SupportedProtocol,
) -> Result<Option<RpcJsonMap>, ContextConstantsDecodeError> {
    match protocol {
        SupportedProtocol::Proto001 => {
            use crate::protocol::proto_001::constants::{ParametricConstants, FIXED};
            let context_param = ParametricConstants::from_bytes(bytes)?;

            let param = ParametricConstants::create_with_default_and_merge(context_param);

            let mut param_map = param.as_map();
            param_map.extend(FIXED.clone().as_map());
            Ok(Some(param_map))
        }
        SupportedProtocol::Proto002 => {
            use crate::protocol::proto_002::constants::{ParametricConstants, FIXED};
            let context_param = ParametricConstants::from_bytes(bytes)?;

            let param = ParametricConstants::create_with_default_and_merge(context_param);

            let mut param_map = param.as_map();
            param_map.extend(FIXED.clone().as_map());
            Ok(Some(param_map))
        }
        SupportedProtocol::Proto003 => {
            use crate::protocol::proto_003::constants::{ParametricConstants, FIXED};
            let context_param = ParametricConstants::from_bytes(bytes)?;

            let param = ParametricConstants::create_with_default_and_merge(context_param);

            let mut param_map = param.as_map();
            param_map.extend(FIXED.clone().as_map());
            Ok(Some(param_map))
        }
        SupportedProtocol::Proto004 => {
            use crate::protocol::proto_004::constants::{ParametricConstants, FIXED};
            let context_param = ParametricConstants::from_bytes(bytes)?;

            let param = ParametricConstants::create_with_default_and_merge(context_param);

            let mut param_map = param.as_map();
            param_map.extend(FIXED.clone().as_map());
            Ok(Some(param_map))
        }
        SupportedProtocol::Proto005 => {
            use crate::protocol::proto_005::constants::{ParametricConstants, FIXED};
            let mut param = ParametricConstants::from_bytes(bytes)?.as_map();
            param.extend(FIXED.clone().as_map());
            Ok(Some(param))
        }
        SupportedProtocol::Proto005_2 => {
            use crate::protocol::proto_005_2::constants::{ParametricConstants, FIXED};
            let mut param = ParametricConstants::from_bytes(bytes)?.as_map();
            param.extend(FIXED.clone().as_map());
            Ok(Some(param))
        }
        SupportedProtocol::Proto006 => {
            use crate::protocol::proto_006::constants::{ParametricConstants, FIXED};
            let mut param = ParametricConstants::from_bytes(bytes)?.as_map();
            param.extend(FIXED.clone().as_map());
            Ok(Some(param))
        }
        SupportedProtocol::Proto007 => {
            use crate::protocol::proto_007::constants::{ParametricConstants, FIXED};
            let mut param = ParametricConstants::from_bytes(bytes)?.as_map();
            param.extend(FIXED.clone().as_map());
            Ok(Some(param))
        }
        SupportedProtocol::Proto008 => {
            use crate::protocol::proto_008::constants::{ParametricConstants, FIXED};
            let mut param = ParametricConstants::from_bytes(bytes)?.as_map();
            param.extend(FIXED.clone().as_map());
            Ok(Some(param))
        }
        SupportedProtocol::Proto008_2 => {
            use crate::protocol::proto_008_2::constants::{ParametricConstants, FIXED};
            let mut param = ParametricConstants::from_bytes(bytes)?.as_map();
            param.extend(FIXED.clone().as_map());
            Ok(Some(param))
        }
    }
}
