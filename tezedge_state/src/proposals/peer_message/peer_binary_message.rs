use crypto::CryptoError;
use tezos_encoding::binary_reader::BinaryReaderError;
use tezos_messages::p2p::binary_message::{BinaryRead, BinaryChunk};
use tezos_messages::p2p::encoding::peer::PeerMessageResponse;
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, MetadataMessage, AckMessage};

use crate::PeerCrypto;
use super::{PeerMessage, PeerMessageError};

#[derive(Debug, Clone)]
pub struct PeerBinaryMessage {
    bytes: BinaryChunk,
    decrypted: Option<Vec<u8>>,
}

impl PeerMessage for PeerBinaryMessage {
    fn take_binary_chunk(self) -> BinaryChunk {
        self.bytes
    }

    fn binary_chunk(&self) -> &BinaryChunk {
        &self.bytes
    }

    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, PeerMessageError> {
        Ok(ConnectionMessage::from_bytes(self.bytes.content())?)
    }

    fn as_metadata_msg(&mut self, crypto: &mut PeerCrypto) -> Result<MetadataMessage, PeerMessageError> {
        if let Some(decrypted) = self.decrypted.as_ref() {
            Ok(MetadataMessage::from_bytes(decrypted)?)
        } else {
            self.decrypted = Some(crypto.decrypt(&self.bytes.content())?);
            self.as_metadata_msg(crypto)
        }
    }

    fn as_ack_msg(&mut self, crypto: &mut PeerCrypto) -> Result<AckMessage, PeerMessageError> {
        if let Some(decrypted) = self.decrypted.as_ref() {
            Ok(AckMessage::from_bytes(decrypted)?)
        } else {
            self.decrypted = Some(crypto.decrypt(&self.bytes.content())?);
            self.as_ack_msg(crypto)
        }
    }

    fn as_peer_msg(&mut self, crypto: &mut PeerCrypto) -> Result<PeerMessageResponse, PeerMessageError> {
        if let Some(decrypted) = self.decrypted.as_ref() {
            Ok(PeerMessageResponse::from_bytes(decrypted)?)
        } else {
            self.decrypted = Some(crypto.decrypt(&self.bytes.content())?);
            self.as_peer_msg(crypto)
        }
    }
}

impl PeerBinaryMessage {
    pub fn new(bytes: BinaryChunk) -> Self {
        Self { bytes, decrypted: None }
    }
}
