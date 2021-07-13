// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use failure::Fail;

use crypto::hash::*;

/// Possible errors for schema
#[derive(Debug, Fail)]
pub enum SchemaError {
    #[fail(display = "Failed to encode value")]
    EncodeError,
    #[fail(display = "Failed to decode value")]
    DecodeError,
    #[fail(display = "Failed to decode value: {}", 0)]
    DecodeValidationError(String),
}

impl From<crypto::hash::FromBytesError> for SchemaError {
    fn from(_: crypto::hash::FromBytesError) -> Self {
        SchemaError::DecodeError
    }
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

macro_rules! hash_codec {
    ($hash:ident) => {
        impl Encoder for $hash {
            fn encode(&self) -> Result<Vec<u8>, SchemaError> {
                Ok(self.as_ref().clone())
            }
        }
        impl Decoder for $hash {
            fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
                use std::convert::TryFrom;
                Self::try_from(bytes).map_err(|_| SchemaError::DecodeError)
            }
        }
    };
}

hash_codec!(ChainId);
hash_codec!(BlockHash);
hash_codec!(BlockMetadataHash);
hash_codec!(OperationHash);
hash_codec!(OperationListListHash);
hash_codec!(OperationMetadataHash);
hash_codec!(OperationMetadataListListHash);
hash_codec!(ContextHash);
hash_codec!(ProtocolHash);
hash_codec!(ContractKt1Hash);
hash_codec!(ContractTz1Hash);
hash_codec!(ContractTz2Hash);
hash_codec!(ContractTz3Hash);
hash_codec!(CryptoboxPublicKeyHash);
hash_codec!(PublicKeyEd25519);
hash_codec!(PublicKeySecp256k1);
hash_codec!(PublicKeyP256);

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

/// Generate codec (encoder + decoder) for a numeric type
macro_rules! num_codec {
    ($num:ident) => {
        #[allow(dead_code)]
        impl Decoder for $num {
            fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
                if bytes.len() == std::mem::size_of::<$num>() {
                    let mut num_bytes: [u8; std::mem::size_of::<$num>()] = Default::default();
                    num_bytes.copy_from_slice(bytes);
                    Ok($num::from_be_bytes(num_bytes))
                } else {
                    Err(SchemaError::DecodeError)
                }
            }
        }
        #[allow(dead_code)]
        impl Encoder for $num {
            fn encode(&self) -> Result<Vec<u8>, SchemaError> {
                let mut value = Vec::with_capacity(std::mem::size_of::<$num>());
                value.extend(&self.to_be_bytes());
                Ok(value)
            }
        }
    };
}

num_codec!(u8);
num_codec!(i16);
num_codec!(u16);
num_codec!(i64);
num_codec!(u64);
num_codec!(i32);
num_codec!(u32);
num_codec!(usize);

/// Create number from a bytes
///
/// ```rust,no_run
/// use storage::num_from_slice;
///
/// let mut bytes = vec![];
/// let num: u64 = 45_165_489;
/// bytes.extend(&num.to_be_bytes());
/// assert_eq!(num, num_from_slice!(bytes, 0, u64));
///
/// ```
#[macro_export(local_inner_macros)]
macro_rules! num_from_slice {
    ($buf:expr, $from_idx:expr, $num:ident) => {{
        let mut bytes: [u8; std::mem::size_of::<$num>()] = Default::default();
        bytes.copy_from_slice(&$buf[$from_idx..$from_idx + std::mem::size_of::<$num>()]);
        $num::from_be_bytes(bytes)
    }};
}
