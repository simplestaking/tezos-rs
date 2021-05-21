use crypto::CryptoError;
use tezos_encoding::binary_reader::BinaryReaderError;
use tezos_messages::p2p::{binary_message::{BinaryChunk, BinaryMessage}, encoding::{ack::AckMessage, connection::ConnectionMessage, prelude::MetadataMessage}};
use super::{crypto::Crypto, raw_acceptor::{RawMessage, RawMessageError}};

impl From<BinaryReaderError> for RawMessageError {
    fn from(_: BinaryReaderError) -> Self {
        Self::InvalidMessage
    }
}

impl From<CryptoError> for RawMessageError {
    fn from(_: CryptoError) -> Self {
        Self::InvalidMessage
    }
}

#[derive(Debug, Clone)]
pub struct RawBinaryMessage {
    bytes: BinaryChunk,
    decrypted: Option<Vec<u8>>,
}

impl RawMessage for RawBinaryMessage {
    fn take_binary_chunk(self) -> BinaryChunk {
        self.bytes
    }

    fn binary_chunk(&self) -> &BinaryChunk {
        &self.bytes
    }

    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, RawMessageError> {
        Ok(ConnectionMessage::from_bytes(self.bytes.content())?)
    }

    fn as_metadata_msg(&mut self, crypto: &mut Crypto) -> Result<MetadataMessage, RawMessageError> {
        if let Some(decrypted) = self.decrypted.as_ref() {
            Ok(MetadataMessage::from_bytes(decrypted)?)
        } else {
            self.decrypted = Some(crypto.decrypt(&self.bytes.content())?);
            self.as_metadata_msg(crypto)
        }
    }

    fn as_ack_msg(&mut self, crypto: &mut Crypto) -> Result<AckMessage, RawMessageError> {
        if let Some(decrypted) = self.decrypted.as_ref() {
            Ok(AckMessage::from_bytes(decrypted)?)
        } else {
            self.decrypted = Some(crypto.decrypt(&self.bytes.content())?);
            self.as_ack_msg(crypto)
        }
    }
}

impl RawBinaryMessage {
    pub fn new(bytes: BinaryChunk) -> Self {
        Self { bytes, decrypted: None }
    }
}
