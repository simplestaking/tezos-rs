use crypto::CryptoError;
use tezos_encoding::binary_reader::BinaryReaderError;
use tezos_messages::p2p::binary_message::BinaryChunk;
use tezos_messages::p2p::encoding::prelude::{AckMessage, ConnectionMessage, MetadataMessage};

use crate::PeerCrypto;

mod peer_binary_handshake_message;
pub use peer_binary_handshake_message::*;

mod peer_decoded_handshake_message;
pub use peer_decoded_handshake_message::*;

#[derive(Debug)]
pub enum PeerHandshakeMessageError {
    InvalidMessage,
    Crypto(CryptoError),
    Decode(BinaryReaderError),
}

impl From<BinaryReaderError> for PeerHandshakeMessageError {
    fn from(err: BinaryReaderError) -> Self {
        Self::Decode(err)
    }
}

impl From<CryptoError> for PeerHandshakeMessageError {
    fn from(err: CryptoError) -> Self {
        Self::Crypto(err)
    }
}

pub trait PeerHandshakeMessage {
    fn take_binary_chunk(self) -> BinaryChunk;
    fn binary_chunk(&self) -> &BinaryChunk;
    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, PeerHandshakeMessageError>;
    fn as_metadata_msg(
        &mut self,
        crypto: &mut PeerCrypto,
    ) -> Result<MetadataMessage, PeerHandshakeMessageError>;
    fn as_ack_msg(
        &mut self,
        crypto: &mut PeerCrypto,
    ) -> Result<AckMessage, PeerHandshakeMessageError>;
}
