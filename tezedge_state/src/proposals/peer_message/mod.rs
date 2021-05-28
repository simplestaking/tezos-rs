use tezos_messages::p2p::binary_message::BinaryChunk;
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, MetadataMessage, AckMessage};

use crate::PeerCrypto;

mod peer_binary_message;
pub use peer_binary_message::*;

mod peer_decoded_message;
pub use peer_decoded_message::*;

pub enum PeerMessageError {
    InvalidMessage,
}

pub trait PeerMessage {
    fn take_binary_chunk(self) -> BinaryChunk;
    fn binary_chunk(&self) -> &BinaryChunk;
    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, PeerMessageError>;
    fn as_metadata_msg(&mut self, crypto: &mut PeerCrypto) -> Result<MetadataMessage, PeerMessageError>;
    fn as_ack_msg(&mut self, crypto: &mut PeerCrypto) -> Result<AckMessage, PeerMessageError>;
}
