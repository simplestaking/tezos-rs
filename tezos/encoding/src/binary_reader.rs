// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Tezos binary data reader.

use std::fmt;

use failure::Fail;

/// Error produced by a [BinaryReader].
#[derive(Debug, Clone, Fail)]
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
