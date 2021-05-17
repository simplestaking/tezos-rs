use std::mem;
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
use super::{GetRequests, NewestTimeSeen, React};

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

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct PeerAddress(String);

impl PeerAddress {
    pub fn new(addr: String) -> Self {
        Self(addr)
    }
}

#[derive(Debug, Clone)]
pub struct ConnectedPeer {
    pub connected_since: Instant,
}

#[derive(Debug, Clone)]
pub struct TezedgeConfig {
    pub port: u16,
    pub disable_mempool: bool,
    pub private_node: bool,
    pub min_connected_peers: u8,
    pub max_connected_peers: u8,
    pub max_pending_peers: u8,
    pub max_potential_peers: usize,
    pub peer_blacklist_duration: Duration,
    pub peer_timeout: Duration,
}

#[derive(Debug, Clone)]
pub enum P2pState {
    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Pending {
        pending_peers: BTreeMap<PeerAddress, Handshake>,
    },

    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections reached.
    PendingFull {
        pending_peers: BTreeMap<PeerAddress, Handshake>,
    },

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Ready {
        pending_peers: BTreeMap<PeerAddress, Handshake>,
    },

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending peers reached.
    ReadyFull {
        pending_peers: BTreeMap<PeerAddress, Handshake>,
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
    pub potential_peers: Vec<PeerAddress>,
    pub connected_peers: BTreeMap<PeerAddress, ConnectedPeer>,
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
            potential_peers: Vec::new(),
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

impl React for TezedgeState {
    fn react(&mut self) {
        use P2pState::*;

        let min_connected = self.config.min_connected_peers as usize;
        let max_connected = self.config.max_connected_peers as usize;
        let max_pending = self.config.max_pending_peers as usize;

        if self.connected_peers.len() == max_connected {
            // TODO: write handling pending_peers, e.g. sending them nack.
            self.p2p_state = ReadyMaxed;
        } else if self.connected_peers.len() < min_connected {
            self.p2p_state = match &mut self.p2p_state {
                ReadyMaxed => {
                    Pending { pending_peers: BTreeMap::new() }
                }
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = mem::replace(pending_peers, BTreeMap::new());
                    if pending_peers.len() == max_pending {
                        PendingFull { pending_peers }
                    } else {
                        Pending { pending_peers }
                    }
                }
            };
        } else {
            self.p2p_state = match &mut self.p2p_state {
                ReadyMaxed => {
                    Ready { pending_peers: BTreeMap::new() }
                }
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = mem::replace(pending_peers, BTreeMap::new());
                    if pending_peers.len() == max_pending {
                        ReadyFull { pending_peers }
                    } else {
                        Ready { pending_peers }
                    }
                }
            };
        }
    }
}

/// Requests which may be made after accepting handshake proposal.
#[derive(Debug, Clone)]
pub enum TezedgeRequest {
    SendPeerConnect((PeerAddress, ConnectionMessage)),
    SendPeerMeta((PeerAddress, MetadataMessage)),
    SendPeerAck((PeerAddress, AckMessage)),
}

impl GetRequests for TezedgeState {
    type Request = TezedgeRequest;

    fn get_requests(&self) -> Vec<TezedgeRequest> {
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        let mut requests = vec![];

        match &self.p2p_state {
            P2pState::ReadyMaxed => {}
            P2pState::Ready { pending_peers }
            | P2pState::ReadyFull { pending_peers }
            | P2pState::Pending { pending_peers }
            | P2pState::PendingFull { pending_peers } => {
                for (peer_address, handshake_step) in pending_peers.iter() {
                    match handshake_step {
                        Incoming(Connect { sent: Some(Idle { .. }), .. })
                        | Outgoing(Connect { sent: Some(Idle { .. }), .. }) => {
                            requests.push(
                                TezedgeRequest::SendPeerConnect((
                                    peer_address.clone(),
                                    self.connection_msg(),
                                ))
                            );
                        }
                        Incoming(Connect { .. }) | Outgoing(Connect { .. }) => {}

                        Incoming(Metadata { sent: Some(Idle { .. }), .. })
                        | Outgoing(Metadata { sent: Some(Idle { .. }), .. }) => {
                            requests.push(
                                TezedgeRequest::SendPeerMeta((
                                    peer_address.clone(),
                                    self.meta_msg(),
                                ))
                            )
                        }
                        Incoming(Metadata { .. }) | Outgoing(Metadata { .. }) => {}

                        Incoming(Ack { sent: Some(Idle { .. }), .. })
                        | Outgoing(Ack { sent: Some(Idle { .. }), .. }) => {
                            requests.push(
                                TezedgeRequest::SendPeerAck((
                                    peer_address.clone(),
                                    AckMessage::Ack,
                                ))
                            )
                        }
                        Incoming(Ack { .. }) | Outgoing(Ack { .. }) => {}
                    }
                }
            }
        }

        requests
    }
}
