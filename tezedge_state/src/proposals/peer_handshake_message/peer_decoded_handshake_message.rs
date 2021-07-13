use tezos_messages::p2p::binary_message::BinaryChunk;
use tezos_messages::p2p::encoding::prelude::{AckMessage, ConnectionMessage, MetadataMessage};

use super::{PeerHandshakeMessage, PeerHandshakeMessageError};
use crate::PeerCrypto;

#[derive(Debug, Clone)]
pub enum PeerDecodedHanshakeMessageType {
    Connection(ConnectionMessage),
    Metadata(MetadataMessage),
    Ack(AckMessage),
}

impl From<ConnectionMessage> for PeerDecodedHanshakeMessageType {
    fn from(msg: ConnectionMessage) -> Self {
        Self::Connection(msg)
    }
}

impl From<MetadataMessage> for PeerDecodedHanshakeMessageType {
    fn from(msg: MetadataMessage) -> Self {
        Self::Metadata(msg)
    }
}

impl From<AckMessage> for PeerDecodedHanshakeMessageType {
    fn from(msg: AckMessage) -> Self {
        Self::Ack(msg)
    }
}

#[derive(Debug, Clone)]
pub struct PeerDecodedHanshakeMessage {
    bytes: BinaryChunk,
    decoded: PeerDecodedHanshakeMessageType,
}

impl PeerHandshakeMessage for PeerDecodedHanshakeMessage {
    fn take_binary_chunk(self) -> BinaryChunk {
        self.bytes
    }

    fn binary_chunk(&self) -> &BinaryChunk {
        &self.bytes
    }

    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, PeerHandshakeMessageError> {
        match &self.decoded {
            PeerDecodedHanshakeMessageType::Connection(msg) => Ok(msg.clone()),
            PeerDecodedHanshakeMessageType::Metadata(_) => {
                Err(PeerHandshakeMessageError::InvalidMessage)
            }
            PeerDecodedHanshakeMessageType::Ack(_) => {
                Err(PeerHandshakeMessageError::InvalidMessage)
            }
        }
    }

    fn as_metadata_msg(
        &mut self,
        _: &mut PeerCrypto,
    ) -> Result<MetadataMessage, PeerHandshakeMessageError> {
        match &self.decoded {
            PeerDecodedHanshakeMessageType::Metadata(msg) => Ok(msg.clone()),
            PeerDecodedHanshakeMessageType::Connection(_) => {
                Err(PeerHandshakeMessageError::InvalidMessage)
            }
            PeerDecodedHanshakeMessageType::Ack(_) => {
                Err(PeerHandshakeMessageError::InvalidMessage)
            }
        }
    }

    fn as_ack_msg(&mut self, _: &mut PeerCrypto) -> Result<AckMessage, PeerHandshakeMessageError> {
        match &self.decoded {
            PeerDecodedHanshakeMessageType::Ack(msg) => Ok(msg.clone()),
            PeerDecodedHanshakeMessageType::Connection(_) => {
                Err(PeerHandshakeMessageError::InvalidMessage)
            }
            PeerDecodedHanshakeMessageType::Metadata(_) => {
                Err(PeerHandshakeMessageError::InvalidMessage)
            }
        }
    }
}

impl PeerDecodedHanshakeMessage {
    pub fn new(bytes: BinaryChunk, decoded: PeerDecodedHanshakeMessageType) -> Self {
        Self { bytes, decoded }
    }

    pub fn message_type(&self) -> &PeerDecodedHanshakeMessageType {
        &self.decoded
    }
}
