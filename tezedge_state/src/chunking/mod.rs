use std::io;

use crypto::CryptoError;
use tezos_encoding::{binary_reader::BinaryReaderError, binary_writer::BinaryWriterError};

mod chunk_read_buffer;
pub use chunk_read_buffer::*;

mod message_read_buffer;
pub use message_read_buffer::*;

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
