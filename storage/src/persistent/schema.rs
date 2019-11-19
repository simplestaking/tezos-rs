// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use failure::Fail;
use rocksdb::{ColumnFamilyDescriptor, Options};

use tezos_encoding::hash::Hash;

/// Possible errors for schema
#[derive(Debug, Fail)]
pub enum SchemaError {
    #[fail(display = "Failed to encode value")]
    EncodeError,
    #[fail(display = "Failed to decode value")]
    DecodeError,
}

/// This trait specifies arbitrary binary encoding and decoding methods for types requiring storing in database
pub trait Codec: Sized {
    /// Try to decode message from its binary format
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError>;

    /// Try to encode instance into its binary format
    fn encode(&self) -> Result<Vec<u8>, SchemaError>;
}

/// This trait extends basic column family by introducing Codec types safety and enforcement
pub trait Schema {
    const COLUMN_FAMILY_NAME: &'static str;

    type Key: Codec;

    type Value: Codec;

    fn cf_descriptor() -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(Self::COLUMN_FAMILY_NAME, Options::default())
    }
}

impl Codec for Hash {
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        Ok(bytes.to_vec())
    }

    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        Ok(self.clone())
    }
}
