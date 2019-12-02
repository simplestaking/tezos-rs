// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use failure::Fail;
use serde::{Deserialize, Serialize};

use tezos_encoding::hash::Hash;

/// Possible errors for schema
#[derive(Debug, Fail)]
pub enum SchemaError {
    #[fail(display = "Failed to encode value")]
    EncodeError,
    #[fail(display = "Failed to decode value")]
    DecodeError,
}

/// Encode input value to binary format.
pub trait Encoder: Sized {
    /// Try to encode instance into its binary format
    fn encode(&self) -> Result<Vec<u8>, SchemaError>;
}

/// Decode value from binary format.
pub trait Decoder: Sized {
    /// Try to decode message from its binary format
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError>;
}

/// This trait specifies arbitrary binary encoding and decoding methods for types requiring storing in database
pub trait Codec: Encoder + Decoder {}

impl<T> Codec for T where T: Encoder + Decoder {}

impl Encoder for Hash {
    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        Ok(self.clone())
    }
}

impl Decoder for Hash {
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        Ok(bytes.to_vec())
    }
}

impl Encoder for String {
    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        Ok(self.as_bytes().to_vec())
    }
}

impl Decoder for String {
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        String::from_utf8(bytes.to_vec()).map_err(|_| SchemaError::DecodeError)
    }
}

impl Decoder for i32 {
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        if bytes.len() == std::mem::size_of::<i32>() {
            let mut i32_bytes: [u8; std::mem::size_of::<i32>()] = Default::default();
            i32_bytes.copy_from_slice(&bytes[..]);
            Ok(i32::from_le_bytes(i32_bytes))
        } else {
            Err(SchemaError::DecodeError)
        }
    }
}

impl Encoder for i32 {
    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        let mut value = Vec::with_capacity(std::mem::size_of::<i32>());
        value.extend(&self.to_le_bytes());
        Ok(value)
    }
}

pub trait BincodeEncoded: Sized + Serialize + for<'a> Deserialize<'a> {
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        bincode::deserialize(bytes)
            .map_err(|_| SchemaError::DecodeError)
    }

    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        bincode::serialize::<Self>(self)
            .map_err(|_| SchemaError::EncodeError)
    }
}

impl<T> Encoder for T where T: BincodeEncoded {
    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        T::encode(self)
    }
}

impl<T> Decoder for T where T: BincodeEncoded {
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        T::decode(bytes)
    }
}
