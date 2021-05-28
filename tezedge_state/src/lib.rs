use std::mem;
use std::fmt::{self, Debug};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use getset::{Getters, CopyGetters};

pub use tla_sm::{Proposal, GetRequests};
use crypto::nonce::Nonce;
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::ack::{NackInfo, NackMotive};
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};

mod peer_crypto;
pub use peer_crypto::PeerCrypto;

pub mod proposals;
pub mod acceptors;

#[derive(Debug)]
pub enum InvalidProposalError {
    ProposalOutdated,
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
    pub crypto: PeerCrypto,

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
    pub min_connected_peers: u32,
    pub max_connected_peers: u32,
    pub max_pending_peers: u32,
    pub max_potential_peers: usize,
    pub periodic_react_interval: Duration,
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
    pub(crate) newest_time_seen: Instant,
    pub(crate) last_periodic_react: Instant,
    pub(crate) config: TezedgeConfig,
    pub(crate) identity: Identity,
    pub(crate) network_version: NetworkVersion,
    pub(crate) potential_peers: Vec<PeerAddress>,
    pub(crate) connected_peers: HashMap<PeerAddress, ConnectedPeer>,
    pub(crate) blacklisted_peers: HashMap<PeerAddress, BlacklistedPeer>,
    pub(crate) p2p_state: P2pState,
    pub(crate) requests: slab::Slab<PendingRequestState>,
}

impl TezedgeState {
    pub fn new(
        config: TezedgeConfig,
        identity: Identity,
        network_version: NetworkVersion,
        initial_time: Instant,
    ) -> Self
    {
        let periodic_react_interval = config.periodic_react_interval;

        Self {
            config,
            identity,
            network_version,
            potential_peers: Vec::new(),
            connected_peers: HashMap::new(),
            blacklisted_peers: HashMap::new(),
            p2p_state: P2pState::Pending {
                pending_peers: HashMap::new(),
            },
            requests: slab::Slab::new(),
            newest_time_seen: initial_time,
            last_periodic_react: initial_time - periodic_react_interval,
        }
    }

    pub(crate) fn check_and_update_time<P: Proposal>(
        &mut self,
        proposal: &P,
    ) -> Result<(), InvalidProposalError>
    {
        if proposal.time() >= self.newest_time_seen {
            self.newest_time_seen = proposal.time();
            Ok(())
        } else {
            Err(InvalidProposalError::ProposalOutdated)
        }
    }

    fn validate_proposal<P: Proposal>(
        &mut self,
        proposal: &P,
    ) -> Result<(), InvalidProposalError>
    {
        self.check_and_update_time(proposal)?;

        Ok(())
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

    pub fn get_peer_crypto(&mut self, peer_address: &PeerAddress) -> Option<&mut PeerCrypto> {
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

    pub(crate) fn adjust_p2p_state(&mut self, at: Instant) {
        use P2pState::*;
        let min_connected = self.config.min_connected_peers as usize;
        let max_connected = self.config.max_connected_peers as usize;
        let max_pending = self.config.max_pending_peers as usize;

        if self.connected_peers.len() == max_connected {
            // TODO: write handling pending_peers, e.g. sending them nack.
            self.p2p_state = ReadyMaxed;
        } else if self.connected_peers.len() < min_connected {
            match &mut self.p2p_state {
                ReadyMaxed => {
                    self.p2p_state = Pending { pending_peers: HashMap::new() };
                    self.initiate_handshakes(at);
                }
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = mem::replace(pending_peers, HashMap::new());
                    if pending_peers.len() == max_pending {
                        self.p2p_state = PendingFull { pending_peers };
                    } else {
                        self.p2p_state = Pending { pending_peers };
                        self.initiate_handshakes(at);
                    }
                }
            };
        } else {
            match &mut self.p2p_state {
                ReadyMaxed => {
                    self.p2p_state = Ready { pending_peers: HashMap::new() };
                    self.initiate_handshakes(at);
                }
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = mem::replace(pending_peers, HashMap::new());
                    if pending_peers.len() == max_pending {
                        self.p2p_state = ReadyFull { pending_peers };
                    } else {
                        self.p2p_state = Ready { pending_peers };
                        self.initiate_handshakes(at);
                    }
                }
            };
        }

    }

    pub(crate) fn check_timeouts(&mut self, at: Instant) {
        use P2pState::*;
        let peer_timeout = self.config.peer_timeout;

        match &mut self.p2p_state {
            ReadyMaxed => {}
            Ready { pending_peers }
            | ReadyFull { pending_peers }
            | Pending { pending_peers }
            | PendingFull { pending_peers } => {
                use Handshake::*;
                use HandshakeStep::*;
                use RequestState::*;

                let now = at;

                let end_handshakes = pending_peers.iter_mut()
                    .filter_map(|(peer_address, handshake)| {
                        match handshake {
                            Incoming(Connect { sent: Some(Pending { at, .. }), .. })
                            | Incoming(Metadata { sent: Some(Pending { at, .. }), .. })
                            | Incoming(Ack { sent: Some(Pending { at, .. }), .. })
                            | Outgoing(Connect { sent: Some(Pending { at, .. }), .. })
                            | Outgoing(Metadata { sent: Some(Pending { at, .. }), .. })
                            | Outgoing(Ack { sent: Some(Pending { at, .. }), .. })
                            => {
                                if now.duration_since(*at) >= peer_timeout {
                                    // sending timed out
                                    Some(peer_address.clone())
                                } else {
                                    None
                                }
                            }
                            Incoming(Connect { sent: Some(Success { at, .. }), .. })
                            | Incoming(Metadata { sent: Some(Success { at, .. }), .. })
                            | Outgoing(Connect { received: None, sent: Some(Success { at, .. }), .. })
                            | Outgoing(Metadata { received: None, sent: Some(Success { at, .. }), .. })
                            | Outgoing(Ack { received: false, sent: Some(Success { at, .. }), .. })
                            => {
                                if now.duration_since(*at) >= peer_timeout {
                                    // receiving timed out
                                    Some(peer_address.clone())
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>();

                if end_handshakes.len() == 0 {
                    return;
                }

                for peer in end_handshakes.iter() {
                    pending_peers.remove(peer);
                }

                for peer in end_handshakes.into_iter() {
                    self.blacklisted_peers.insert(peer.clone(), BlacklistedPeer {
                        since: now,
                    });
                    let entry = self.requests.vacant_entry();
                    self.requests.insert(PendingRequestState {
                        request: PendingRequest::BlacklistPeer { peer },
                        status: RequestState::Idle { at: now },
                    });
                }
                self.initiate_handshakes(now);
            }
        }
    }

    pub(crate) fn check_blacklisted_peers(&mut self, at: Instant) {
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
    }

    pub(crate) fn initiate_handshakes(&mut self, at: Instant) {
        use P2pState::*;
        let potential_peers = &mut self.potential_peers;
        let max_pending = self.config.max_pending_peers as usize;

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
                    self.adjust_p2p_state(at);
                }
            }
        }
    }

    pub(crate) fn periodic_react(&mut self, at: Instant) {
        let dur_since_last_periodic_react = at.duration_since(self.last_periodic_react);
        if  dur_since_last_periodic_react > self.config.periodic_react_interval {
            self.last_periodic_react = at;
            self.check_timeouts(at);
            self.check_blacklisted_peers(at);
            self.initiate_handshakes(at);
        }
    }

    pub(crate) fn nack_peer_handshake(
        &mut self,
        at: Instant,
        peer: PeerAddress,
        motive: NackMotive
    ) {
        use P2pState::*;

        match &mut self.p2p_state {
            ReadyMaxed => {}
            Pending { pending_peers }
            | PendingFull { pending_peers }
            | Ready { pending_peers }
            | ReadyFull { pending_peers } => {
                if let Some((peer_address, _)) = pending_peers.remove_entry(&peer) {
                    self.potential_peers.push(peer_address);
                    let entry = self.requests.vacant_entry();
                    entry.insert(PendingRequestState {
                        request: PendingRequest::NackAndDisconnectPeer {
                            peer,
                            // TODO: include potential peers.
                            nack_info: NackInfo::new(motive, &[]),
                        },
                        status: RequestState::Idle { at },
                    });
                }
            }
        }
        // TODO: depending on motive, blacklist them, but if we have
        // to much connections, but only till we need more connections.
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
    DisconnectPeer {
        req_id: usize,
        peer: PeerAddress,
    },
    BlacklistPeer {
        req_id: usize,
        peer: PeerAddress,
    },
}

#[derive(Debug, Clone)]
pub enum PendingRequest {
    NackAndDisconnectPeer {
        peer: PeerAddress,
        nack_info: NackInfo,
    },
    DisconnectPeer {
        peer: PeerAddress,
    },
    BlacklistPeer {
        peer: PeerAddress,
    },
}

#[derive(Debug, Clone)]
pub struct PendingRequestState {
    pub request: PendingRequest,
    pub status: RequestState,
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

                        Incoming(Metadata { sent: Some(Idle { .. }), .. })
                        | Outgoing(Metadata { sent: Some(Idle { .. }), .. }) => {
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

        for (req_id, req) in self.requests.iter() {
            if let RequestState::Idle { .. } = req.status {
                requests.push(match &req.request {
                    PendingRequest::NackAndDisconnectPeer { peer, nack_info } => {
                        TezedgeRequest::SendPeerAck {
                            peer: peer.clone(),
                            message: AckMessage::Nack(nack_info.clone()),
                        }
                    }
                    PendingRequest::DisconnectPeer { peer } => {
                        let peer = peer.clone();
                        TezedgeRequest::DisconnectPeer { req_id, peer }
                    }
                    PendingRequest::BlacklistPeer { peer } => {
                        let peer = peer.clone();
                        TezedgeRequest::BlacklistPeer { req_id, peer }
                    }
                })
            }
        }

        requests
    }
}
