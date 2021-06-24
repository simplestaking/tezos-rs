use std::io::{self, Read};
use std::fmt::{self, Debug};
use std::time::{Instant, Duration};

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
use crate::chunking::HandshakeReadBuffer;

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
    pub fn to_result(self) -> Option<HandshakeResult> {
        match self {
            Self::Incoming(step) => step.to_result(),
            Self::Outgoing(step) => step.to_result(),
        }
    }
}

#[derive(Debug, Clone)]
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

impl HandshakeStep {
    pub fn to_result(self) -> Option<HandshakeResult> {
        match self {
            Self::Ack { conn_msg, meta_msg, crypto, .. } => {
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
}

impl PendingPeer {
    pub(crate) fn new(address: PeerAddress, handshake: Handshake) -> Self {
        Self {
            address,
            handshake,
            read_buf: HandshakeReadBuffer::new(),
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
