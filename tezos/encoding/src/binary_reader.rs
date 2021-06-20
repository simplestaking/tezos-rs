// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Tezos binary data reader.

use std::fmt;

use thiserror::Error;

/// Error produced by a [BinaryReader].
#[derive(Debug, Clone, Error)]
pub enum BinaryReaderError {
    Error(String),
    UnknownTag(String),
}

impl fmt::Display for BinaryReaderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinaryReaderError::Error(error) => write!(f, "{}", error),
            BinaryReaderError::UnknownTag(tag) => write!(f, "Unknown tag: {}", tag),
        }
    }
}
