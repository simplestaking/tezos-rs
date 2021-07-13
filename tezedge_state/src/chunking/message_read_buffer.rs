use std::io::{self, Read};

use bytes::Buf;
use tezos_messages::p2p::{
    binary_message::{BinaryChunk, BinaryRead, SizeFromChunk, CONTENT_LENGTH_FIELD_BYTES},
    encoding::peer::{PeerMessage, PeerMessageResponse},
};

use super::{ChunkReadBuffer, ReadMessageError};
use crate::PeerCrypto;

/// Read buffer for connected peer(`PeerMessage`).
///
/// This message can be split into multiple chunks. Encrypted part of
/// the first chunk includes information (`4 bytes`) about whole message's
/// size. We need to process chunks until decrypted accumulated message
/// isn't of that size.
#[derive(Debug, Clone)]
pub struct MessageReadBuffer {
    message_buf: Vec<u8>,
    message_len: usize,
    chunk_reader: ChunkReadBuffer,
}

impl MessageReadBuffer {
    pub fn new() -> Self {
        Self {
            message_buf: vec![],
            message_len: 0,
            chunk_reader: ChunkReadBuffer::new(),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.message_len > 0 && self.message_buf.len() >= self.message_len
    }

    /// Returns if more might be available to be read.
    pub fn read_from<R: Read>(
        &mut self,
        reader: &mut R,
        crypto: &mut PeerCrypto,
    ) -> Result<PeerMessage, ReadMessageError> {
        loop {
            self.chunk_reader.read_from(reader)?;
            // TODO: stop reading if message_len < message_buf.len() + chunk_expected_len

            if let Some(chunk) = self.chunk_reader.take_ref_if_ready() {
                let mut decrypted = crypto.decrypt(&chunk.content())?;

                if self.message_len == 0 {
                    self.message_len = PeerMessageResponse::size_from_chunk(&decrypted)?;
                }
                self.message_buf.append(&mut decrypted);

                if self.is_finished() {
                    return self.take_and_decode();
                }
            }
        }
    }

    fn take_and_decode(&mut self) -> Result<PeerMessage, ReadMessageError> {
        let result = PeerMessageResponse::from_bytes(&self.message_buf).map(|resp| resp.message);

        self.clear();
        Ok(result?)
    }

    pub fn clear(&mut self) {
        self.message_buf.clear();
        self.message_len = 0;
        self.chunk_reader.clear();
    }
}
