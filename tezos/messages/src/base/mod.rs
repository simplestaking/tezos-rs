// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use hex::FromHexError;
use thiserror::Error;

use crypto::base58::FromBase58CheckError;
use crypto::hash::FromBytesError;

pub mod rpc_support;
pub mod signature_public_key;
pub mod signature_public_key_hash;

#[derive(Debug, Error, PartialEq)]
pub enum ConversionError {
    #[error("Conversion from invalid public key")]
    InvalidPublicKey,

    #[error("Invalid hash: {hash}")]
    InvalidHash { hash: String },

    #[error("Invalid curve tag: {curve_tag}")]
    InvalidCurveTag { curve_tag: String },
}

impl From<hex::FromHexError> for ConversionError {
    fn from(error: FromHexError) -> Self {
        ConversionError::InvalidHash {
            hash: error.to_string(),
        }
    }
}

impl From<FromBase58CheckError> for ConversionError {
    fn from(error: FromBase58CheckError) -> Self {
        ConversionError::InvalidHash {
            hash: error.to_string(),
        }
    }
}

impl From<FromBytesError> for ConversionError {
    fn from(error: FromBytesError) -> Self {
        ConversionError::InvalidHash {
            hash: error.to_string(),
        }
    }
}
