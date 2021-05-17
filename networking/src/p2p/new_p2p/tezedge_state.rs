use std::time::{Duration, Instant};
use std::sync::Arc;
use std::collections::BTreeMap;
use getset::Getters;

use crypto::nonce::Nonce;
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};
use super::NewestTimeSeen;

#[derive(Debug)]
pub enum Error {
    ProposalOutdated,
    MaximumPeersReached,
    PeerBlacklisted {
        till: Instant,
    },
    InvalidMsg,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum RequestState {
    Idle {
        at: Instant,
    },
    Pending {
        at: Instant,
    },
    Success {
        at: Instant,
    },
    // Error {
    //     at: Instant,
    // },
}

#[derive(Debug, Clone)]
pub struct ConnectedPeer {
    pub connected_since: Instant,
}

#[derive(Debug, Clone)]
pub enum Handshake {
    Incoming(HandshakeStep),
    Outgoing(HandshakeStep),
}

#[derive(Debug, Clone)]
pub enum HandshakeStep {
    Connect {
        sent: Option<RequestState>,
        received: Option<ConnectionMessage>,
    },
    Metadata {
        conn_msg: ConnectionMessage,
        sent: Option<RequestState>,
        received: Option<MetadataMessage>,
    },
    Ack {
        conn_msg: ConnectionMessage,
        meta_msg: MetadataMessage,
        sent: Option<RequestState>,
        received: bool,
    },
}

impl HandshakeStep {
    pub(crate) fn set_sent(&mut self, state: RequestState) {
        match self {
            Self::Connect { sent, .. }
            | Self::Metadata { sent, .. }
            | Self::Ack { sent, .. } => {
                *sent = Some(state);
            }
        }
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(id: String) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone)]
pub struct TezedgeConfig {
    pub port: u16,
    pub disable_mempool: bool,
    pub private_node: bool,
    pub min_connected_peers: u8,
    pub max_connected_peers: u8,
    pub max_pending_peers: u8,
    pub peer_blacklist_duration: Duration,
    pub peer_timeout: Duration,
}

#[derive(Debug, Clone)]
pub enum P2pState {
    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Pending {
        pending_peers: BTreeMap<PeerId, Handshake>,
    },

    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections reached.
    PendingFull {
        pending_peers: BTreeMap<PeerId, Handshake>,
    },

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Ready {
        pending_peers: BTreeMap<PeerId, Handshake>,
    },

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending peers reached.
    ReadyFull {
        pending_peers: BTreeMap<PeerId, Handshake>,
    },

    /// Maximum number of connected peers reached.
    ReadyMaxed,
}

impl P2pState {
    pub fn is_full(&self) -> bool {
        matches!(self,
            Self::PendingFull { .. }
            | Self::ReadyFull { .. }
            | Self::ReadyMaxed)
    }
}

#[derive(Debug, Clone)]
pub struct TezedgeState {
    pub config: TezedgeConfig,
    pub identity: Identity,
    pub network_version: NetworkVersion,
    pub available_peers: BTreeMap<PeerId, ()>,
    pub connected_peers: BTreeMap<PeerId, ConnectedPeer>,
    pub newest_time_seen: Instant,
    pub p2p_state: P2pState,
}

impl NewestTimeSeen for TezedgeState {
    fn newest_time_seen(&self) -> Instant {
        self.newest_time_seen
    }

    fn newest_time_seen_mut(&mut self) -> &mut Instant {
       &mut self.newest_time_seen
    }
}

impl TezedgeState {
    pub fn new(
        config: TezedgeConfig,
        identity: Identity,
        network_version: NetworkVersion,
        initial_time: Instant,
    ) -> Self
    {
        Self {
            config,
            identity,
            network_version,
            available_peers: BTreeMap::new(),
            connected_peers: BTreeMap::new(),
            newest_time_seen: initial_time,
            p2p_state: P2pState::Pending {
                pending_peers: BTreeMap::new(),
            },
        }
    }

    pub fn connection_msg(&self) -> ConnectionMessage {
        ConnectionMessage::try_new(
            self.config.port,
            &self.identity.public_key,
            &self.identity.proof_of_work_stamp,
            Nonce::random(),
            self.network_version.clone(),
        ).unwrap()
    }

    pub fn meta_msg(&self) -> MetadataMessage {
        MetadataMessage::new(
            self.config.disable_mempool,
            self.config.private_node,
        )
    }
}
