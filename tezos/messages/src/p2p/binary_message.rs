// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use bytes::{Buf, BufMut};
use failure::Fail;
use failure::_core::convert::TryFrom;
use nom::{
    combinator::{all_consuming, complete},
    Finish,
};
use serde::Serialize;

use crypto::blake2b::{self, Blake2bError};
use crypto::hash::Hash;
use tezos_encoding::nom::{error::convert_error, NomError, NomInput, NomResult};
use tezos_encoding::{
    binary_reader::BinaryReaderError,
    binary_writer::{self, BinaryWriterError},
};

use crate::p2p::binary_message::MessageHashError::SerializationError;

/// Size in bytes of the content length field
pub const CONTENT_LENGTH_FIELD_BYTES: usize = 2;
/// Max allowed message length in bytes
pub const CONTENT_LENGTH_MAX: usize = u16::max_value() as usize;

/// Trait for reading a binary message.
pub trait BinaryRead: Sized {
    /// Create new struct from bytes.
    fn from_bytes<B: AsRef<[u8]>>(buf: B) -> Result<Self, BinaryReaderError>;
}

/// Trait for writing a binary message.
pub trait BinaryWrite {
    /// Produce bytes from the struct.
    fn as_bytes(&self) -> Result<Vec<u8>, BinaryWriterError>;
}

/// Message that can be both encoded and decoded into binary format.
pub trait BinaryMessage: BinaryRead + BinaryWrite {}

impl<T: BinaryRead + BinaryWrite> BinaryMessage for T {}

impl<T> BinaryWrite for T
where
    T: tezos_encoding::encoding::HasEncoding + Serialize,
{
    #[inline]
    fn as_bytes(&self) -> Result<Vec<u8>, BinaryWriterError> {
        binary_writer::write(self, &Self::encoding())
    }
}

impl<T> BinaryRead for T
where
    T: tezos_encoding::nom::NomReader + Sized,
{
    #[inline]
    fn from_bytes<B: AsRef<[u8]>>(buf: B) -> Result<Self, BinaryReaderError> {
        all_consuming_complete_input(T::nom_read, buf.as_ref())
    }
}

/// This trait is able to predict the exact size of the message from the first bytes of the message.
pub trait SizeFromChunk {
    /// Returns the size of the message.
    fn size_from_chunk(bytes: impl AsRef<[u8]>) -> Result<usize, BinaryReaderError>;
}

/// Applies nom parser `parser` to the input, assuming that input is complete.
pub fn complete_input<'a, T>(
    parser: impl FnMut(NomInput<'a>) -> NomResult<'a, T>,
    input: NomInput<'a>,
) -> Result<T, BinaryReaderError> {
    // - `complete` combinator assumes that underlying parsing has complete input,
    //   converting ``Incomplete` into `Error`.
    // - `finish` flattens `nom::Err` into underlying error for easier processing.
    complete(parser)(input)
        .finish()
        .map(|(_, output)| output)
        .map_err(|error| map_nom_error(input, error))
}

/// Applies nom parser `parser` to the input, assuming that input is complete and
/// ensuring that it is fully consumed.
pub fn all_consuming_complete_input<T>(
    parser: impl FnMut(NomInput) -> NomResult<T>,
    input: NomInput,
) -> Result<T, BinaryReaderError> {
    // - `all_consuming` combinator ensures that all input is consumed,
    //   reporting error otherwise.
    // - `complete` combinator assumes that underlying parsing has complete input,
    //   converting ``Incomplete` into `Error`.
    // - `finish` flattens `nom::Err` into underlying error for easier processing.
    all_consuming(complete(parser))(input)
        .finish()
        .map(|(bytes, output)| {
            debug_assert!(
                bytes.is_empty(),
                "Successful parsing should consume all bytes"
            );
            output
        })
        .map_err(|error| map_nom_error(input, error))
}

/// Maps input and nom error into printable version.
pub(crate) fn map_nom_error(input: NomInput, error: NomError) -> BinaryReaderError {
    if let Some(unknown_tag) = error.get_unknown_tag() {
        BinaryReaderError::UnknownTag(unknown_tag.clone())
    } else {
        BinaryReaderError::Error(convert_error(input, error))
    }
}

/// Represents binary raw encoding received from peer node.
///
/// Difference from [`BinaryMessage`] is that it also contains [`CONTENT_LENGTH_FIELD_BYTES`] bytes
/// of information about how many bytes is the actual encoding.
#[derive(Debug, Clone)]
pub struct BinaryChunk(Vec<u8>);

impl BinaryChunk {
    pub fn from_raw(raw: Vec<u8>) -> Result<BinaryChunk, BinaryChunkError> {
        if raw.len() < CONTENT_LENGTH_FIELD_BYTES {
            return Err(BinaryChunkError::MissingSizeInformation);
        }

        let expected_len = (&raw[..]).get_u16() as usize;
        let found_len = raw.len() - CONTENT_LENGTH_FIELD_BYTES;

        if expected_len != found_len {
            Err(BinaryChunkError::IncorrectSizeInformation {
                expected: expected_len,
                actual: found_len,
            })
        } else if raw.len() > CONTENT_LENGTH_MAX + CONTENT_LENGTH_FIELD_BYTES {
            Err(BinaryChunkError::OverflowError)
        } else {
            Ok(BinaryChunk(raw))
        }
    }

    /// Create new `BinaryChunk` from input content.
    pub fn from_content(content: &[u8]) -> Result<BinaryChunk, BinaryChunkError> {
        if content.len() <= CONTENT_LENGTH_MAX {
            // add length
            let mut bytes = Vec::with_capacity(CONTENT_LENGTH_FIELD_BYTES + content.len());
            // adds MESSAGE_LENGTH_FIELD_SIZE -- 2 bytes with length of the content
            bytes.put_u16(content.len() as u16);
            // append data
            bytes.extend(content);

            Ok(BinaryChunk(bytes))
        } else {
            Err(BinaryChunkError::OverflowError)
        }
    }

    /// Gets raw data (including encoded content size)
    #[inline]
    pub fn raw(&self) -> &Vec<u8> {
        &self.0
    }

    /// Get content of the message
    #[inline]
    pub fn content(&self) -> &[u8] {
        &self.0[CONTENT_LENGTH_FIELD_BYTES..]
    }
}

/// `BinaryChunk` error
#[derive(Debug, Fail)]
pub enum BinaryChunkError {
    #[fail(display = "Overflow error")]
    OverflowError,
    #[fail(display = "Missing size information")]
    MissingSizeInformation,
    #[fail(
        display = "Incorrect content size information. expected={}, actual={}",
        expected, actual
    )]
    IncorrectSizeInformation { expected: usize, actual: usize },
}

/// Convert `Vec<u8>` into `BinaryChunk`. It is required that input `Vec<u8>`
/// contains also information about the content length in its first 2 bytes.
impl TryFrom<Vec<u8>> for BinaryChunk {
    type Error = BinaryChunkError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.len() < CONTENT_LENGTH_FIELD_BYTES {
            Err(BinaryChunkError::MissingSizeInformation)
        } else if value.len() <= (CONTENT_LENGTH_MAX + CONTENT_LENGTH_FIELD_BYTES) {
            let expected_content_length =
                (&value[0..CONTENT_LENGTH_FIELD_BYTES]).get_u16() as usize;
            if (expected_content_length + CONTENT_LENGTH_FIELD_BYTES) == value.len() {
                Ok(BinaryChunk(value))
            } else {
                Err(BinaryChunkError::IncorrectSizeInformation {
                    expected: expected_content_length,
                    actual: value.len(),
                })
            }
        } else {
            Err(BinaryChunkError::OverflowError)
        }
    }
}

/// Message hash error
#[derive(Debug, Fail)]
pub enum MessageHashError {
    #[fail(display = "Message serialization error: {}", error)]
    SerializationError { error: BinaryWriterError },
    #[fail(display = "Error constructing hash")]
    FromBytesError { error: crypto::hash::FromBytesError },
    #[fail(display = "Blake2b digest error")]
    Blake2bError,
}

impl From<BinaryWriterError> for MessageHashError {
    fn from(error: BinaryWriterError) -> Self {
        SerializationError { error }
    }
}

impl From<crypto::hash::FromBytesError> for MessageHashError {
    fn from(error: crypto::hash::FromBytesError) -> Self {
        MessageHashError::FromBytesError { error }
    }
}

impl From<Blake2bError> for MessageHashError {
    fn from(_: Blake2bError) -> Self {
        MessageHashError::Blake2bError
    }
}

/// Trait for getting hash of the message.
pub trait MessageHash {
    fn message_hash(&self) -> Result<Hash, MessageHashError>;
    fn message_typed_hash<H>(&self) -> Result<H, MessageHashError>
    where
        H: crypto::hash::HashTrait;
}

impl<T: BinaryMessage> MessageHash for T {
    #[inline]
    fn message_hash(&self) -> Result<Hash, MessageHashError> {
        let bytes = self.as_bytes()?;
        Ok(blake2b::digest_256(&bytes)?)
    }

    #[inline]
    fn message_typed_hash<H>(&self) -> Result<H, MessageHashError>
    where
        H: crypto::hash::HashTrait,
    {
        let bytes = self.as_bytes()?;
        let digest = blake2b::digest_256(&bytes)?;
        H::try_from_bytes(&digest).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_binary_from_content() -> Result<(), failure::Error> {
        let chunk = BinaryChunk::from_content(&[])?.0;
        assert_eq!(CONTENT_LENGTH_FIELD_BYTES, chunk.len());
        assert_eq!(CONTENT_LENGTH_FIELD_BYTES, chunk.capacity());

        let chunk = BinaryChunk::from_content(&[1])?.0;
        assert_eq!(CONTENT_LENGTH_FIELD_BYTES + 1, chunk.len());
        assert_eq!(CONTENT_LENGTH_FIELD_BYTES + 1, chunk.capacity());

        let chunk =
            BinaryChunk::from_content(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])?.0;
        assert_eq!(CONTENT_LENGTH_FIELD_BYTES + 15, chunk.len());
        assert_eq!(CONTENT_LENGTH_FIELD_BYTES + 15, chunk.capacity());

        let chunk = BinaryChunk::from_content(&[1; CONTENT_LENGTH_MAX])?.0;
        assert_eq!(CONTENT_LENGTH_FIELD_BYTES + CONTENT_LENGTH_MAX, chunk.len());
        assert_eq!(
            CONTENT_LENGTH_FIELD_BYTES + CONTENT_LENGTH_MAX,
            chunk.capacity()
        );
        Ok(())
    }
}
