use tezos_messages::p2p::binary_message::BinaryChunk;
use tezos_messages::p2p::encoding::peer::PeerMessage;
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, MetadataMessage, AckMessage};

use crate::PeerCrypto;
use super::{PeerAbstractMessage, PeerAbstractMessageError};

#[derive(Debug, Clone)]
pub enum PeerAbstractDecodedMessageType {
    Connection(ConnectionMessage),
    Metadata(MetadataMessage),
    Ack(AckMessage),
    Message(PeerMessage),
}

impl From<ConnectionMessage> for PeerAbstractDecodedMessageType {
    fn from(msg: ConnectionMessage) -> Self {
        Self::Connection(msg)
    }
}

impl From<MetadataMessage> for PeerAbstractDecodedMessageType {
    fn from(msg: MetadataMessage) -> Self {
        Self::Metadata(msg)
    }
}

impl From<AckMessage> for PeerAbstractDecodedMessageType {
    fn from(msg: AckMessage) -> Self {
        Self::Ack(msg)
    }
}

#[derive(Debug, Clone)]
pub struct PeerAbstractDecodedMessage {
    bytes: BinaryChunk,
    decoded: PeerAbstractDecodedMessageType,
}

impl PeerAbstractMessage for PeerAbstractDecodedMessage {
    fn take_binary_chunk(self) -> BinaryChunk {
        self.bytes
    }

    fn binary_chunk(&self) -> &BinaryChunk {
        &self.bytes
    }

    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, PeerAbstractMessageError> {
        match &self.decoded {
            PeerAbstractDecodedMessageType::Connection(msg) => Ok(msg.clone()),
            PeerAbstractDecodedMessageType::Metadata(_) => Err(PeerAbstractMessageError::InvalidMessage),
            PeerAbstractDecodedMessageType::Ack(_) => Err(PeerAbstractMessageError::InvalidMessage),
            PeerAbstractDecodedMessageType::Message(_) => Err(PeerAbstractMessageError::InvalidMessage),
        }
    }

    fn as_metadata_msg(&mut self, _: &mut PeerCrypto) -> Result<MetadataMessage, PeerAbstractMessageError> {
        match &self.decoded {
            PeerAbstractDecodedMessageType::Metadata(msg) => Ok(msg.clone()),
            PeerAbstractDecodedMessageType::Connection(_) => Err(PeerAbstractMessageError::InvalidMessage),
            PeerAbstractDecodedMessageType::Ack(_) => Err(PeerAbstractMessageError::InvalidMessage),
            PeerAbstractDecodedMessageType::Message(_) => Err(PeerAbstractMessageError::InvalidMessage),
        }
    }

    fn as_ack_msg(&mut self, _: &mut PeerCrypto) -> Result<AckMessage, PeerAbstractMessageError> {
        match &self.decoded {
            PeerAbstractDecodedMessageType::Ack(msg) => Ok(msg.clone()),
            PeerAbstractDecodedMessageType::Connection(_) => Err(PeerAbstractMessageError::InvalidMessage),
            PeerAbstractDecodedMessageType::Metadata(_) => Err(PeerAbstractMessageError::InvalidMessage),
            PeerAbstractDecodedMessageType::Message(_) => Err(PeerAbstractMessageError::InvalidMessage),
        }
    }

    fn as_peer_msg(&mut self, crypto: &mut PeerCrypto) -> Result<PeerMessage, PeerAbstractMessageError> {
        match &self.decoded {
            PeerAbstractDecodedMessageType::Message(msg) => Ok(msg.clone()),
            PeerAbstractDecodedMessageType::Connection(_) => Err(PeerAbstractMessageError::InvalidMessage),
            PeerAbstractDecodedMessageType::Metadata(_) => Err(PeerAbstractMessageError::InvalidMessage),
            PeerAbstractDecodedMessageType::Ack(_) => Err(PeerAbstractMessageError::InvalidMessage),
        }
    }
}

impl PeerAbstractDecodedMessage {
    pub fn new(bytes: BinaryChunk, decoded: PeerAbstractDecodedMessageType) -> Self {
        Self { bytes, decoded }
    }

    pub fn message_type(&self) -> &PeerAbstractDecodedMessageType {
        &self.decoded
    }
}
