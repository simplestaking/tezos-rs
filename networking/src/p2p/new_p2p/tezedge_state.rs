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

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum PeerState {
    Disconnected {
        at: Instant,
    },
    IncomingHandshake(HandshakeStep),
    OutgoingHandshake(HandshakeStep),
    Connected {
        at: Instant,
    },
    Blacklisted {
        at: Instant,
    },
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum HandshakeStep {
    ConnectionSent(RequestState),
    MetaSent(RequestState),
    AckSent(RequestState),
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(id: String) -> Self {
        Self(id)
    }
}

pub enum P2pManagerAction {
    SendPeerConnect((PeerId, ConnectionMessage)),
    SendPeerMeta((PeerId, MetadataMessage)),
    SendPeerAck((PeerId, AckMessage)),
}

#[derive(Debug, Clone)]
pub struct TezedgeConfig {
    pub port: u16,
    pub disable_mempool: bool,
    pub private_node: bool,
    pub min_connected_peers: u8,
    pub max_connected_peers: u8,
    pub peer_blacklist_duration: Duration,
    pub peer_timeout: Duration,
}

pub enum P2pState {
    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Pending,

    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections reached.
    PendingFull,

    /// Minimum number of connected peers reached.
    /// Maximum number of pending connections **not** reached.
    Ready,

    /// Minimum number of connected peers reached.
    /// Maximum number of pending connections reached.
    ReadyFull,
}

pub struct TezedgeState {
    pub config: TezedgeConfig,
    pub identity: Identity,
    pub network_version: NetworkVersion,
    pub peers: BTreeMap<PeerId, PeerState>,
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
            peers: BTreeMap::new(),
            newest_time_seen: initial_time,
            p2p_state: P2pState::Pending,
        }
    }

    pub fn peer_state(&self, peer_id: &PeerId) -> Option<&PeerState> {
        self.peers.get(peer_id)
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

    fn update_newest_time_seen(&mut self, time: Instant) {
        self.newest_time_seen = time;
    }
}
