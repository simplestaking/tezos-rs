use tezos_messages::p2p::binary_message::BinaryChunk;
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, MetadataMessage, AckMessage};

use crate::PeerCrypto;
use super::{PeerMessage, PeerMessageError};

#[derive(Debug, Clone)]
pub enum PeerDecodedMessageType {
    Connection(ConnectionMessage),
    Metadata(MetadataMessage),
    Ack(AckMessage),
}

impl From<ConnectionMessage> for PeerDecodedMessageType {
    fn from(msg: ConnectionMessage) -> Self {
        Self::Connection(msg)
    }
}

impl From<MetadataMessage> for PeerDecodedMessageType {
    fn from(msg: MetadataMessage) -> Self {
        Self::Metadata(msg)
    }
}

impl From<AckMessage> for PeerDecodedMessageType {
    fn from(msg: AckMessage) -> Self {
        Self::Ack(msg)
    }
}

#[derive(Debug, Clone)]
pub struct PeerDecodedMessage {
    bytes: BinaryChunk,
    decoded: PeerDecodedMessageType,
}

impl PeerMessage for PeerDecodedMessage {
    fn take_binary_chunk(self) -> BinaryChunk {
        self.bytes
    }

    fn binary_chunk(&self) -> &BinaryChunk {
        &self.bytes
    }

    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, PeerMessageError> {
        match &self.decoded {
            PeerDecodedMessageType::Connection(msg) => Ok(msg.clone()),
            PeerDecodedMessageType::Metadata(_) => Err(PeerMessageError::InvalidMessage),
            PeerDecodedMessageType::Ack(_) => Err(PeerMessageError::InvalidMessage),
        }
    }

    fn as_metadata_msg(&mut self, _: &mut PeerCrypto) -> Result<MetadataMessage, PeerMessageError> {
        match &self.decoded {
            PeerDecodedMessageType::Metadata(msg) => Ok(msg.clone()),
            PeerDecodedMessageType::Connection(_) => Err(PeerMessageError::InvalidMessage),
            PeerDecodedMessageType::Ack(_) => Err(PeerMessageError::InvalidMessage),
        }
    }

    fn as_ack_msg(&mut self, _: &mut PeerCrypto) -> Result<AckMessage, PeerMessageError> {
        match &self.decoded {
            PeerDecodedMessageType::Ack(msg) => Ok(msg.clone()),
            PeerDecodedMessageType::Connection(_) => Err(PeerMessageError::InvalidMessage),
            PeerDecodedMessageType::Metadata(_) => Err(PeerMessageError::InvalidMessage),
        }
    }
}

impl PeerDecodedMessage {
    pub fn new(bytes: BinaryChunk, decoded: PeerDecodedMessageType) -> Self {
        Self { bytes, decoded }
    }

    pub fn message_type(&self) -> &PeerDecodedMessageType {
        &self.decoded
    }
}
