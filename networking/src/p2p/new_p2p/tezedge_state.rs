use std::mem;
use std::fmt::{self, Debug};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use getset::{Getters, CopyGetters};


use crypto::{crypto_box::{CryptoKey, PrecomputedKey, PublicKey}, nonce::{Nonce, NoncePair, generate_nonces}};
use tezos_identity::Identity;
use tezos_messages::p2p::{binary_message::BinaryMessage, encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
}};

use super::{GetRequests, NewestTimeSeen, React, crypto::Crypto};

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

pub struct HandshakeResult {
    pub conn_msg: ConnectionMessage,
    pub meta_msg: MetadataMessage,
    pub crypto: Crypto,
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
    Connect {
        sent: Option<RequestState>,
        received: Option<ConnectionMessage>,
        sent_conn_msg: ConnectionMessage,
    },
    Metadata {
        conn_msg: ConnectionMessage,
        crypto: Crypto,
        sent: Option<RequestState>,
        received: Option<MetadataMessage>,
    },
    Ack {
        conn_msg: ConnectionMessage,
        meta_msg: MetadataMessage,
        crypto: Crypto,
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

#[derive(Debug, Hash, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct PeerAddress(pub String);

impl PeerAddress {
    pub fn new(addr: String) -> Self {
        Self(addr)
    }
}

#[derive(Getters, CopyGetters, Debug, Clone)]
pub struct ConnectedPeer {
    // #[get_copy = "pub"]
    pub port: u16,

    // #[get = "pub"]
    pub version: NetworkVersion,

    // #[get = "pub"]
    pub public_key: Vec<u8>,

    pub proof_of_work_stamp: Vec<u8>,

    // #[get = "pub"]
    pub crypto: Crypto,

    // #[get_copy = "pub"]
    pub disable_mempool: bool,

    // #[get_copy = "pub"]
    pub private_node: bool,

    // #[get_copy = "pub"]
    pub connected_since: Instant,
}

#[derive(Debug, Clone)]
pub struct BlacklistedPeer {
    since: Instant,
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
        pending_peers: HashMap<PeerAddress, Handshake>,
    },

    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections reached.
    PendingFull {
        pending_peers: HashMap<PeerAddress, Handshake>,
    },

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Ready {
        pending_peers: HashMap<PeerAddress, Handshake>,
    },

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending peers reached.
    ReadyFull {
        pending_peers: HashMap<PeerAddress, Handshake>,
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
    pub connected_peers: HashMap<PeerAddress, ConnectedPeer>,
    pub blacklisted_peers: HashMap<PeerAddress, BlacklistedPeer>,
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
            connected_peers: HashMap::new(),
            blacklisted_peers: HashMap::new(),
            newest_time_seen: initial_time,
            p2p_state: P2pState::Pending {
                pending_peers: HashMap::new(),
            },
        }
    }

    pub fn connection_msg(&self) -> ConnectionMessage {
        ConnectionMessage::try_new(
            self.config.port,
            &self.identity.public_key,
            &self.identity.proof_of_work_stamp,
            // TODO: this introduces non-determinism
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

    pub fn extend_potential_peers<I>(&mut self, peers: I)
        where I: IntoIterator<Item = PeerAddress>,
    {
        // Return if maximum number of potential peers is already reached.
        if self.potential_peers.len() >= self.config.max_potential_peers {
            return;
        }

        let limit = self.config.max_potential_peers - self.potential_peers.len();

        self.potential_peers.extend(peers.into_iter().take(limit));
    }

    pub fn get_peer_crypto(&mut self, peer_address: &PeerAddress) -> Option<&mut Crypto> {
        if let Some(peer) = self.connected_peers.get_mut(peer_address) {
            return Some(&mut peer.crypto);
        }
        use Handshake::*;
        use HandshakeStep::*;

        match &mut self.p2p_state {
            P2pState::ReadyMaxed => None,
            P2pState::Pending { pending_peers }
            | P2pState::PendingFull { pending_peers }
            | P2pState::Ready { pending_peers }
            | P2pState::ReadyFull { pending_peers } => {
                match pending_peers.get_mut(peer_address) {
                    Some(Incoming(Metadata { crypto, .. }))
                    | Some(Outgoing(Metadata { crypto, .. }))
                    | Some(Incoming(Ack { crypto, .. }))
                    | Some(Outgoing(Ack { crypto, .. })) => {
                        Some(crypto)
                    }
                    _ => None,
                }
            }

        }
    }

    pub(crate) fn set_peer_connected(
        &mut self,
        at: Instant,
        peer_address: PeerAddress,
        result: HandshakeResult,
    ) {
        self.connected_peers.insert(peer_address, ConnectedPeer {
            connected_since: at,
            port: result.conn_msg.port,
            version: result.conn_msg.version,
            public_key: result.conn_msg.public_key,
            proof_of_work_stamp: result.conn_msg.proof_of_work_stamp,
            crypto: result.crypto,
            disable_mempool: result.meta_msg.disable_mempool(),
            private_node: result.meta_msg.private_node(),
        });
    }

}

impl React for TezedgeState {
    fn react(&mut self, at: Instant) {
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
                    Pending { pending_peers: HashMap::new() }
                }
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = mem::replace(pending_peers, HashMap::new());
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
                    Ready { pending_peers: HashMap::new() }
                }
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = mem::replace(pending_peers, HashMap::new());
                    if pending_peers.len() == max_pending {
                        ReadyFull { pending_peers }
                    } else {
                        Ready { pending_peers }
                    }
                }
            };
        }

        let whitelist_peers = self.blacklisted_peers.iter()
            .filter(|(_, blacklisted)| {
                at.duration_since(blacklisted.since) >= self.config.peer_blacklist_duration
            })
            .map(|(address, _)| address.clone())
            .collect::<Vec<_>>();

        for address in whitelist_peers {
            self.blacklisted_peers.remove(&address);
            self.extend_potential_peers(
                std::iter::once(address.clone()),
            );
        }
        let potential_peers = &mut self.potential_peers;

        match &mut self.p2p_state {
            ReadyMaxed | ReadyFull { .. } | PendingFull { .. } => {}
            Pending { pending_peers }
            | Ready { pending_peers } => {
                let len = potential_peers.len().min(
                    max_pending - pending_peers.len(),
                );
                let end = potential_peers.len();
                let start = end - len;

                for peer in potential_peers.drain(start..end) {
                    pending_peers.insert(peer, Handshake::Outgoing(
                        HandshakeStep::Connect {
                            // sent_conn_msg: self.connection_msg(),
                            sent_conn_msg: ConnectionMessage::try_new(
                                self.config.port,
                                &self.identity.public_key,
                                &self.identity.proof_of_work_stamp,
                                // TODO: this introduces non-determinism
                                Nonce::random(),
                                self.network_version.clone(),
                            ).unwrap(),
                            sent: Some(RequestState::Idle { at }),
                            received: None,
                        }
                    ));
                }

                if len > 0 {
                    return self.react(at);
                }
            }
        }

    }
}

/// Requests which may be made after accepting handshake proposal.
#[derive(Debug, Clone)]
pub enum TezedgeRequest {
    SendPeerConnect {
        peer: PeerAddress,
        message: ConnectionMessage,
    },
    SendPeerMeta {
        peer: PeerAddress,

        message: MetadataMessage,
    },
    SendPeerAck {
        peer: PeerAddress,
        message: AckMessage,
    },
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
                        Incoming(Connect { sent: Some(Idle { .. }), sent_conn_msg, .. })
                        | Outgoing(Connect { sent: Some(Idle { .. }), sent_conn_msg, .. }) => {
                            requests.push(
                                TezedgeRequest::SendPeerConnect {
                                    peer: peer_address.clone(),
                                    message: sent_conn_msg.clone(),
                                }
                            );
                        }
                        Incoming(Connect { .. }) | Outgoing(Connect { .. }) => {}

                        Incoming(Metadata { sent: Some(Idle { .. }), conn_msg, .. })
                        | Outgoing(Metadata { sent: Some(Idle { .. }), conn_msg, .. }) => {
                            requests.push(
                                TezedgeRequest::SendPeerMeta {
                                    peer: peer_address.clone(),
                                    message: self.meta_msg(),
                                }
                            )
                        }
                        Incoming(Metadata { .. }) | Outgoing(Metadata { .. }) => {}

                        Incoming(Ack { sent: Some(Idle { .. }), .. })
                        | Outgoing(Ack { sent: Some(Idle { .. }), .. }) => {
                            requests.push(
                                TezedgeRequest::SendPeerAck {
                                    peer: peer_address.clone(),
                                    message: AckMessage::Ack,
                                }
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
