use std::io::{self, Read, Write};
use std::fmt::{self, Debug};
use std::time::{Instant, Duration};

use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryWrite};
pub use tla_sm::{Proposal, GetRequests};
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};

use crate::peer_address::PeerListenerAddress;
use crate::{PeerCrypto, PeerAddress, Port};
use crate::state::{NotMatchingAddress, RequestState};
use crate::chunking::{HandshakeReadBuffer, ChunkWriter, EncryptedMessageWriter, WriteMessageError};

#[derive(Debug, Clone, Copy)]
pub enum HandshakeMessageType {
    Connection,
    Metadata,
    Ack,
}

#[derive(Debug, Clone)]
pub enum Handshake {
    Incoming(HandshakeStep),
    Outgoing(HandshakeStep),
}

pub struct HandshakeResult {
    pub conn_msg: ConnectionMessage,
    pub meta_msg: MetadataMessage,
    pub crypto: PeerCrypto,
}

impl Handshake {
    pub fn is_finished(&self) -> bool {
        match self {
            Self::Incoming(step) => step.is_finished(),
            Self::Outgoing(step) => step.is_finished(),
        }
    }

    pub fn crypto(&mut self) -> Option<&mut PeerCrypto> {
        match self {
            Self::Incoming(step) => step.crypto(),
            Self::Outgoing(step) => step.crypto(),
        }
    }

    pub fn to_result(self) -> Option<HandshakeResult> {
        match self {
            Self::Incoming(step) => step.to_result(),
            Self::Outgoing(step) => step.to_result(),
        }
    }
}

#[derive(Clone)]
pub enum HandshakeStep {
    Initiated { at: Instant },
    Connect {
        sent: Option<RequestState>,
        received: Option<ConnectionMessage>,
        sent_conn_msg: ConnectionMessage,
    },
    Metadata {
        conn_msg: ConnectionMessage,
        crypto: PeerCrypto,
        sent: Option<RequestState>,
        received: Option<MetadataMessage>,
    },
    Ack {
        conn_msg: ConnectionMessage,
        meta_msg: MetadataMessage,
        crypto: PeerCrypto,
        sent: Option<RequestState>,
        received: bool,
    },
}

impl Debug for HandshakeStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initiated { at } => {
                f.debug_struct("HandshakeStep::Initiated")
                    .field("at", at)
                    .finish()
            }
            Self::Connect { sent, received, .. } => {
                f.debug_struct("HandshakeStep::Connect")
                    .field("sent", sent)
                    .field("received", &received.is_some())
                    .finish()
            }
            Self::Metadata { sent, received, .. } => {
                f.debug_struct("HandshakeStep::Metadata")
                    .field("sent", sent)
                    .field("received", &received.is_some())
                    .finish()
            }
            Self::Ack { sent, received, .. } => {
                f.debug_struct("HandshakeStep::Ack")
                    .field("sent", sent)
                    .field("received", received)
                    .finish()
            }
        }
    }
}

impl HandshakeStep {
    pub fn is_finished(&self) -> bool {
        use RequestState::*;
        matches!(
            self,
            Self::Ack { sent: Some(Success { .. }), received: true, .. }
        )
    }

    pub fn crypto(&mut self) -> Option<&mut PeerCrypto> {
        match self {
            Self::Connect { .. }
            | Self::Initiated { .. } => None,
            Self::Metadata { crypto, .. }
            | Self::Ack { crypto, .. } => Some(crypto),
        }
    }

    pub fn to_result(self) -> Option<HandshakeResult> {
        match self {
            Self::Ack { conn_msg, meta_msg, crypto, .. } =>
            {
                Some(HandshakeResult { conn_msg, meta_msg, crypto })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingPeer {
    pub address: PeerAddress,
    pub handshake: Handshake,
    pub read_buf: HandshakeReadBuffer,
    conn_msg_writer: Option<ChunkWriter>,
    msg_writer: Option<(HandshakeMessageType, EncryptedMessageWriter)>,
}

impl PendingPeer {
    pub(crate) fn new(address: PeerAddress, handshake: Handshake) -> Self {
        Self {
            address,
            handshake,
            read_buf: HandshakeReadBuffer::new(),
            conn_msg_writer: None,
            msg_writer: None,
        }
    }

    /// Port on which peer is listening.
    pub fn listener_port(&self) -> Option<Port> {
        use Handshake::*;
        use HandshakeStep::*;

        match &self.handshake {
            // if it's outgoing connection, then the port is correct.
            Outgoing(_) => Some(self.address.port()),
            Incoming(Initiated { .. }) => None,
            Incoming(Connect { received, .. }) => received.as_ref().map(|x| x.port),
            Incoming(Metadata { conn_msg, .. }) => Some(conn_msg.port),
            Incoming(Ack { conn_msg, .. }) => Some(conn_msg.port),
        }
    }

    pub fn listener_address(&self) -> Option<PeerListenerAddress> {
        self.listener_port()
            .map(|port| PeerListenerAddress::new(self.address.ip(), port))
    }

    #[inline]
    pub fn read_message_from<R: Read>(&mut self, reader: &mut R) -> Result<(), io::Error> {
        self.read_buf.read_from(reader)
    }

    /// Enqueues send connection message and updates `RequestState` with
    /// `Pending` state, if we should be sending connection message based
    /// on current handshake state with the peer.
    ///
    /// Returns:
    ///
    /// - `Ok(was_message_queued)`: if `false`, means we shouldn't be
    ///   sending this concrete message at a current stage(state).
    ///
    /// - `Err(error)`: if error ocurred when encoding the message.
    pub fn enqueue_send_conn_msg(&mut self, at: Instant) -> Result<bool, WriteMessageError> {
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.handshake {
            Incoming(Connect { sent: Some(req_state @ Idle { .. }), received: Some(_), sent_conn_msg })
            | Outgoing(Connect { sent: Some(req_state @ Idle { .. }), sent_conn_msg, .. }) => {
                self.conn_msg_writer = Some(ChunkWriter::new(
                    BinaryChunk::from_content(
                        &sent_conn_msg.as_bytes()?,
                    )?,
                ));
                *req_state = Pending { at };
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Enqueues send metadata message and updates `RequestState` with
    /// `Pending` state, if we should be sending connection message based
    /// on current handshake state with the peer.
    ///
    /// Returns:
    ///
    /// - `Ok(was_message_queued)`: if `false`, means we shouldn't be
    ///   sending this concrete message at a current stage(state).
    ///
    /// - `Err(error)`: if error ocurred when encoding the message.
    pub fn enqueue_send_meta_msg(&mut self, at: Instant, meta_msg: MetadataMessage) -> Result<bool, WriteMessageError> {
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.handshake {
            Incoming(Metadata { sent: Some(req_state @ Idle { .. }), received: Some(_), .. })
            | Outgoing(Metadata { sent: Some(req_state @ Idle { .. }), .. }) => {
                self.msg_writer = Some((
                    HandshakeMessageType::Metadata,
                    EncryptedMessageWriter::try_new(&meta_msg)?,
                ));
                *req_state = Pending { at };
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Enqueues send ack message and updates `RequestState` with
    /// `Pending` state, if we should be sending connection message based
    /// on current handshake state with the peer.
    ///
    /// Returns:
    ///
    /// - `Ok(was_message_queued)`: if `false`, means we shouldn't be
    ///   sending this concrete message at a current stage(state).
    ///
    /// - `Err(error)`: if error ocurred when encoding the message.
    pub fn enqueue_send_ack_msg(&mut self, at: Instant, ack_msg: AckMessage) -> Result<bool, WriteMessageError> {
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.handshake {
            Incoming(Ack { sent: Some(req_state @ Idle { .. }), received: true, .. })
            | Outgoing(Ack { sent: Some(req_state @ Idle { .. }), .. }) => {
                self.msg_writer = Some((
                    HandshakeMessageType::Ack,
                    EncryptedMessageWriter::try_new(&ack_msg)?,
                ));
                *req_state = Pending { at };
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub fn send_conn_msg_successful(&mut self, at: Instant) -> bool {
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.handshake {
            Incoming(Connect { sent: Some(req_state @ Pending { .. }), received: Some(_), sent_conn_msg })
            | Outgoing(Connect { sent: Some(req_state @ Pending { .. }), sent_conn_msg, .. }) => {
                *req_state = Success { at };
                self.conn_msg_writer = None;
                true
            }
            _ => false
        }
    }

    pub fn send_meta_msg_successful(&mut self, at: Instant) -> bool {
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.handshake {
            Incoming(Metadata { sent: Some(req_state @ Pending { .. }), received: Some(_), .. })
            | Outgoing(Metadata { sent: Some(req_state @ Pending { .. }), .. }) => {
                *req_state = Success { at };
                self.msg_writer = None;
                true
            }
            _ => false,
        }
    }

    pub fn send_ack_msg_successful(&mut self, at: Instant) -> bool {
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.handshake {
            Incoming(Ack { sent: Some(req_state @ Pending { .. }), received: true, .. })
            | Outgoing(Ack { sent: Some(req_state @ Pending { .. }), .. }) => {
                *req_state = Success { at };
                self.msg_writer = None;
                true
            }
            _ => false,
        }
    }

    pub fn write_to<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> Result<HandshakeMessageType, WriteMessageError>
    {
        if let Some(chunk_writer) = self.conn_msg_writer.as_mut() {
            chunk_writer.write_to(writer)?;
            self.conn_msg_writer = None;
            Ok(HandshakeMessageType::Connection)
        } else if let Some((msg_type, msg_writer)) = self.msg_writer.as_mut() {
            let msg_type = *msg_type;

            let crypto = match self.handshake.crypto() {
                Some(crypto) => crypto,
                None => {
                    #[cfg(test)]
                    unreachable!("this shouldn't be reachable, as encryption is needed by metadata and ack message, and we shouldn't be sending that if we haven't exchange connection messages.");

                    self.msg_writer = None;
                    return Err(WriteMessageError::Empty);
                }
            };

            msg_writer.write_to(writer, crypto)?;
            self.msg_writer = None;
            Ok(msg_type)
        } else {
            Err(WriteMessageError::Empty)
        }
    }

    pub fn to_result(self) -> Option<HandshakeResult> {
        self.handshake.to_result()
    }
}

#[derive(Debug, Clone)]
pub struct PendingPeers {
    peers: slab::Slab<PendingPeer>,
}

impl PendingPeers {
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            peers: slab::Slab::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    fn find_index(&self, address: &PeerAddress) -> Option<usize> {
        self.peers.iter()
            .find(|(_, x)| &x.address == address)
            .map(|(index, _)| index)
    }

    #[inline]
    pub fn contains_address(&self, address: &PeerAddress) -> bool {
        self.find_index(address).is_some()
    }

    #[inline]
    pub fn get(&self, id: &PeerAddress) -> Option<&PendingPeer> {
        if let Some(index) = self.find_index(id) {
            self.peers.get(index)
        } else {
            None
        }
    }

    #[inline]
    pub fn get_mut(&mut self, id: &PeerAddress) -> Option<&mut PendingPeer> {
        if let Some(index) = self.find_index(id) {
            self.peers.get_mut(index)
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn insert(&mut self, peer: PendingPeer) -> usize {
        self.peers.insert(peer)
    }

    #[inline]
    pub(crate) fn remove(&mut self, id: &PeerAddress) -> Option<PendingPeer> {
        self.find_index(id)
            .map(|index| self.peers.remove(index))
    }

    #[inline]
    pub fn iter(&self) -> slab::Iter<PendingPeer> {
        self.peers.iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> slab::IterMut<PendingPeer> {
        self.peers.iter_mut()
    }

    #[inline]
    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, Self::new())
    }
}

impl IntoIterator for PendingPeers {
    type Item = (usize, PendingPeer);
    type IntoIter = slab::IntoIter<PendingPeer>;

    fn into_iter(self) -> Self::IntoIter {
        self.peers.into_iter()
    }
}
