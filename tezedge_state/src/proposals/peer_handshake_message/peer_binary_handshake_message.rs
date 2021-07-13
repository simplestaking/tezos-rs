use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryRead};
use tezos_messages::p2p::encoding::prelude::{AckMessage, ConnectionMessage, MetadataMessage};

use super::{PeerHandshakeMessage, PeerHandshakeMessageError};
use crate::PeerCrypto;

#[derive(Debug, Clone)]
pub struct PeerBinaryHandshakeMessage {
    bytes: BinaryChunk,
    decrypted: Option<Vec<u8>>,
}

impl PeerHandshakeMessage for PeerBinaryHandshakeMessage {
    fn take_binary_chunk(self) -> BinaryChunk {
        self.bytes
    }

    fn binary_chunk(&self) -> &BinaryChunk {
        &self.bytes
    }

    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, PeerHandshakeMessageError> {
        Ok(ConnectionMessage::from_bytes(self.bytes.content())?)
    }

    fn as_metadata_msg(
        &mut self,
        crypto: &mut PeerCrypto,
    ) -> Result<MetadataMessage, PeerHandshakeMessageError> {
        if let Some(decrypted) = self.decrypted.as_ref() {
            Ok(MetadataMessage::from_bytes(decrypted)?)
        } else {
            self.decrypted = Some(crypto.decrypt(&self.bytes.content())?);
            self.as_metadata_msg(crypto)
        }
    }

    fn as_ack_msg(
        &mut self,
        crypto: &mut PeerCrypto,
    ) -> Result<AckMessage, PeerHandshakeMessageError> {
        if let Some(decrypted) = self.decrypted.as_ref() {
            Ok(AckMessage::from_bytes(decrypted)?)
        } else {
            self.decrypted = Some(crypto.decrypt(&self.bytes.content())?);
            self.as_ack_msg(crypto)
        }
    }
}

impl PeerBinaryHandshakeMessage {
    pub fn new(bytes: BinaryChunk) -> Self {
        Self {
            bytes,
            decrypted: None,
        }
    }
}
