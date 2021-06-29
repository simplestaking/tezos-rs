use std::io;

use crypto::CryptoError;
use tezos_encoding::{binary_reader::BinaryReaderError, binary_writer::BinaryWriterError};
use tezos_messages::p2p::binary_message::BinaryChunkError;

mod chunk_read_buffer;
pub use chunk_read_buffer::*;

mod message_read_buffer;
pub use message_read_buffer::*;

mod chunk_writer;
pub use chunk_writer::*;

mod encrypted_message_writer;
pub use encrypted_message_writer::*;

#[derive(Debug)]
pub enum ReadMessageError {
    Pending,
    IO(io::Error),
    Crypto(CryptoError),
    Decode(BinaryReaderError),
}

impl From<io::Error> for ReadMessageError {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::WouldBlock {
            Self::Pending
        } else {
            Self::IO(err)
        }
    }
}

impl From<BinaryReaderError> for ReadMessageError {
    fn from(err: BinaryReaderError) -> Self {
        Self::Decode(err)
    }
}

impl From<CryptoError> for ReadMessageError {
    fn from(err: CryptoError) -> Self {
        Self::Crypto(err)
    }
}

#[derive(Debug)]
pub enum WriteMessageError {
    Empty,
    Pending,
    IO(io::Error),
    Encode(BinaryWriterError),
    Crypto(CryptoError),
    BinaryChunk(BinaryChunkError)
}

impl From<io::Error> for WriteMessageError {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::WouldBlock {
            Self::Pending
        } else {
            Self::IO(err)
        }
    }
}

impl From<BinaryWriterError> for WriteMessageError {
    fn from(err: BinaryWriterError) -> Self {
        Self::Encode(err)
    }
}

impl From<CryptoError> for WriteMessageError {
    fn from(err: CryptoError) -> Self {
        Self::Crypto(err)
    }
}

impl From<BinaryChunkError> for WriteMessageError {
    fn from(err: BinaryChunkError) -> Self {
        Self::BinaryChunk(err)
    }
}
