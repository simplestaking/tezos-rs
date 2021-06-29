use std::fmt::Debug;
use std::time::{Instant, Duration};
use std::collections::HashSet;
use crypto::crypto_box::{CryptoKey, PublicKey};

pub use tla_sm::{Proposal, GetRequests};
use crypto::nonce::Nonce;
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::ack::{NackInfo, NackMotive};
use tezos_messages::p2p::encoding::prelude::{
    ConnectionMessage,
    MetadataMessage,
};

use crate::{InvalidProposalError, PeerCrypto, PeerAddress, Port, ShellCompatibilityVersion};
use crate::peer_address::PeerListenerAddress;

// mod peer_token;
// pub use peer_token::*;

mod requests;
pub(crate) use requests::*;

mod pending_peers;
pub(crate) use pending_peers::*;

mod connected_peers;
pub(crate) use connected_peers::*;

mod blacklisted_peers;
pub(crate) use blacklisted_peers::*;

#[derive(Debug)]
pub struct NotMatchingAddress;

#[derive(Debug, Clone)]
pub struct TezedgeConfig {
    pub port: Port,
    pub disable_mempool: bool,
    pub private_node: bool,
    pub min_connected_peers: usize,
    pub max_connected_peers: usize,
    pub max_pending_peers: usize,
    pub max_potential_peers: usize,
    pub periodic_react_interval: Duration,
    pub peer_blacklist_duration: Duration,
    pub peer_timeout: Duration,
    pub pow_target: f64,
}

#[derive(Debug, Clone)]
pub enum P2pState {
    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Pending {
        pending_peers: PendingPeers,
    },

    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections reached.
    PendingFull {
        pending_peers: PendingPeers,
    },

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Ready {
        pending_peers: PendingPeers,
    },

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending peers reached.
    ReadyFull {
        pending_peers: PendingPeers,
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
    pub(crate) listening_for_connection_requests: bool,
    pub(crate) newest_time_seen: Instant,
    pub(crate) last_periodic_react: Instant,
    pub(crate) config: TezedgeConfig,
    pub(crate) identity: Identity,
    pub(crate) shell_compatibility_version: ShellCompatibilityVersion,
    pub(crate) potential_peers: HashSet<PeerListenerAddress>,
    pub(crate) connected_peers: ConnectedPeers,
    pub(crate) blacklisted_peers: BlacklistedPeers,
    // TODO: blacklist identities as well.
    pub(crate) p2p_state: P2pState,
    pub(crate) requests: slab::Slab<PendingRequestState>,
}

impl TezedgeState {
    pub fn new(
        config: TezedgeConfig,
        identity: Identity,
        shell_compatibility_version: ShellCompatibilityVersion,
        initial_time: Instant,
    ) -> Self
    {
        let periodic_react_interval = config.periodic_react_interval;
        let max_connected_peers = config.max_connected_peers;
        let max_pending_peers = config.max_pending_peers;

        Self {
            config,
            identity,
            shell_compatibility_version,
            listening_for_connection_requests: false,
            potential_peers: HashSet::new(),
            connected_peers: ConnectedPeers::with_capacity(max_connected_peers),
            blacklisted_peers: BlacklistedPeers::new(),
            p2p_state: P2pState::Pending {
                pending_peers: PendingPeers::with_capacity(max_pending_peers),
            },
            requests: slab::Slab::new(),
            newest_time_seen: initial_time,
            last_periodic_react: initial_time - periodic_react_interval,
        }
    }

    pub fn newest_time_seen(&self) -> Instant {
        self.newest_time_seen
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

    pub fn validate_proposal<P: Proposal>(
        &mut self,
        proposal: &P,
    ) -> Result<(), InvalidProposalError>
    {
        self.check_and_update_time(proposal)?;

        Ok(())
    }

    pub fn pending_peers(&self) -> Option<&PendingPeers> {
        use P2pState::*;

        match &self.p2p_state {
            Pending { pending_peers }
            | PendingFull { pending_peers }
            | Ready { pending_peers }
            | ReadyFull { pending_peers } => Some(pending_peers),
            ReadyMaxed => None,
        }
    }

    pub(crate) fn pending_peers_mut(&mut self) -> Option<&mut PendingPeers> {
        use P2pState::*;

        match &mut self.p2p_state {
            Pending { pending_peers }
            | PendingFull { pending_peers }
            | Ready { pending_peers }
            | ReadyFull { pending_peers } => Some(pending_peers),
            ReadyMaxed => None,
        }
    }

    pub fn blacklisted_peers(&self) -> &BlacklistedPeers {
        &self.blacklisted_peers
    }

    pub fn request_states(&self) -> &slab::Slab<PendingRequestState> {
        &self.requests
    }

    #[inline(always)]
    pub fn pending_peers_len(&self) -> usize {
        self.pending_peers().map(|x| x.len()).unwrap_or(0)
    }

    pub fn meta_msg(&self) -> MetadataMessage {
        MetadataMessage::new(
            self.config.disable_mempool,
            self.config.private_node,
        )
    }

    pub(crate) fn extend_potential_peers<I>(&mut self, peers: I)
        where I: IntoIterator<Item = PeerListenerAddress>,
    {
        // Return if maximum number of potential peers is already reached.
        if self.potential_peers.len() >= self.config.max_potential_peers {
            return;
        }

        let limit = self.config.max_potential_peers - self.potential_peers.len();

        let connected_peers = &self.connected_peers;
        let blacklisted_peers = &self.blacklisted_peers;

        self.potential_peers.extend(
            peers
                .into_iter()
                .take(limit)
                .filter(|addr| !connected_peers.contains_address(&addr.into())
                            && !blacklisted_peers.is_address_blacklisted(&addr.into()))
        );
    }

    pub fn is_peer_connected(&self, peer: &PeerAddress) -> bool {
        self.connected_peers.contains_address(peer)
    }

    pub(crate) fn set_peer_connected(
        &mut self,
        at: Instant,
        peer_address: PeerAddress,
        result: HandshakeResult,
    ) {
        // TODO: put public key in state instead of having unwrap here.
        let public_key_hash = PublicKey::from_bytes(&result.conn_msg.public_key).unwrap().public_key_hash().unwrap();

        let connected_peer = self.connected_peers
            .set_peer_connected(at, peer_address, result);

        self.requests.insert(PendingRequestState {
            request: PendingRequest::NotifyHandshakeSuccessful {
                peer_address: peer_address.clone(),
                peer_public_key_hash: public_key_hash,
                metadata: MetadataMessage::new(
                    connected_peer.disable_mempool,
                    connected_peer.private_node,
                ),
                network_version: connected_peer.version.clone(),
            },
            status: RequestState::Idle { at },
        });
    }

    pub(crate) fn adjust_p2p_state(&mut self, at: Instant) {
        use P2pState::*;
        let min_connected = self.config.min_connected_peers as usize;
        let max_connected = self.config.max_connected_peers as usize;
        let max_pending = self.config.max_pending_peers as usize;

        if self.connected_peers.len() == max_connected {
            // TODO: write handling pending_peers, e.g. sending them nack.
            match &mut self.p2p_state {
                ReadyMaxed => {}
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = pending_peers.take();
                    self.requests.insert(PendingRequestState {
                        request: PendingRequest::StopListeningForNewPeers,
                        status: RequestState::Idle { at },
                    });

                    for (_, peer) in pending_peers.into_iter() {
                        // TODO: send them nack
                        self.requests.insert(PendingRequestState {
                            request: PendingRequest::DisconnectPeer { peer: peer.address },
                            status: RequestState::Idle { at },
                        });
                    }
                }
            };

            self.p2p_state = ReadyMaxed;
        } else if self.connected_peers.len() < min_connected {
            match &mut self.p2p_state {
                ReadyMaxed => {
                    self.requests.insert(PendingRequestState {
                        request: PendingRequest::StartListeningForNewPeers,
                        status: RequestState::Idle { at },
                    });
                    self.p2p_state = Pending { pending_peers: PendingPeers::new() };
                    self.initiate_handshakes(at);
                }
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = pending_peers.take();
                    if pending_peers.len() == max_pending {
                        self.p2p_state = PendingFull { pending_peers };
                        if self.listening_for_connection_requests {
                            self.requests.insert(PendingRequestState {
                                request: PendingRequest::StopListeningForNewPeers,
                                status: RequestState::Idle { at },
                            });
                            self.listening_for_connection_requests = false;
                        }
                    } else {
                        self.p2p_state = Pending { pending_peers };
                        if !self.listening_for_connection_requests {
                            self.requests.insert(PendingRequestState {
                                request: PendingRequest::StartListeningForNewPeers,
                                status: RequestState::Idle { at },
                            });
                            self.listening_for_connection_requests = true;
                        }
                        self.initiate_handshakes(at);
                    }
                }
            };
        } else {
            match &mut self.p2p_state {
                ReadyMaxed => {
                    self.p2p_state = Ready { pending_peers: PendingPeers::new() };
                    self.initiate_handshakes(at);
                }
                Ready { pending_peers }
                | ReadyFull { pending_peers }
                | Pending { pending_peers }
                | PendingFull { pending_peers } => {
                    let pending_peers = pending_peers.take();
                    if pending_peers.len() == max_pending {
                        self.p2p_state = ReadyFull { pending_peers };
                        if self.listening_for_connection_requests {
                            self.requests.insert(PendingRequestState {
                                request: PendingRequest::StopListeningForNewPeers,
                                status: RequestState::Idle { at },
                            });
                            self.listening_for_connection_requests = false;
                        }
                    } else {
                        self.p2p_state = Ready { pending_peers };
                        if !self.listening_for_connection_requests {
                            self.requests.insert(PendingRequestState {
                                request: PendingRequest::StartListeningForNewPeers,
                                status: RequestState::Idle { at },
                            });
                            self.listening_for_connection_requests = true;
                        }
                        self.initiate_handshakes(at);
                    }
                }
            };
        }

    }

    pub(crate) fn check_timeouts(&mut self, at: Instant) {
        use P2pState::*;
        let now = at;
        let peer_timeout = self.config.peer_timeout;

        self.requests.retain(|req_id, req| {
            match &req.request {
                PendingRequest::ConnectPeer { .. } => {
                    match &req.status {
                        RequestState::Idle { at }
                        | RequestState::Pending { at } => {
                            if now.duration_since(*at) >= peer_timeout {
                                false
                            } else {
                                true
                            }
                        }
                        _ => false,
                    }
                }
                _ => true
            }
        });

        match &mut self.p2p_state {
            ReadyMaxed => {}
            Ready { pending_peers }
            | ReadyFull { pending_peers }
            | Pending { pending_peers }
            | PendingFull { pending_peers } => {
                use Handshake::*;
                use HandshakeStep::*;
                use RequestState::*;

                let end_handshakes = pending_peers.iter_mut()
                    .filter_map(|(_, peer)| {

                        match &mut peer.handshake {
                            Outgoing(Initiated { at })
                            | Incoming(Connect { sent: Some(Pending { at, .. }), .. })
                            | Incoming(Metadata { sent: Some(Pending { at, .. }), .. })
                            | Incoming(Ack { sent: Some(Pending { at, .. }), .. })
                            | Outgoing(Connect { sent: Some(Pending { at, .. }), .. })
                            | Outgoing(Metadata { sent: Some(Pending { at, .. }), .. })
                            | Outgoing(Ack { sent: Some(Pending { at, .. }), .. })
                            => {
                                if now.duration_since(*at) >= peer_timeout {
                                    // sending timed out
                                    Some(peer.address.clone())
                                } else {
                                    None
                                }
                            }
                            Incoming(Initiated { at })
                            | Incoming(Connect { sent: Some(Success { at, .. }), .. })
                            | Incoming(Metadata { sent: Some(Success { at, .. }), .. })
                            | Outgoing(Connect { received: None, sent: Some(Success { at, .. }), .. })
                            | Outgoing(Metadata { received: None, sent: Some(Success { at, .. }), .. })
                            | Outgoing(Ack { received: false, sent: Some(Success { at, .. }), .. })
                            => {
                                if now.duration_since(*at) >= peer_timeout {
                                    // receiving timed out
                                    Some(peer.address.clone())
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

                for peer in end_handshakes.into_iter() {
                    self.blacklist_peer(now, peer);
                }
                self.initiate_handshakes(now);
            }
        }
    }

    pub(crate) fn check_blacklisted_peers(&mut self, at: Instant) {
        let whitelist_peers = self.blacklisted_peers
            .take_expired_blacklisted_peers(at, self.config.peer_blacklist_duration);

        self.extend_potential_peers(
            whitelist_peers.into_iter()
                .filter_map(|(ip, port)| {
                    port.map(|port| PeerListenerAddress::new(ip, port))
                })
        )
    }

    pub(crate) fn initiate_handshakes(&mut self, at: Instant) {
        use P2pState::*;
        let requests = &mut self.requests;
        let potential_peers = &mut self.potential_peers;
        let max_pending = self.config.max_pending_peers as usize;

        match &mut self.p2p_state {
            ReadyMaxed | ReadyFull { .. } | PendingFull { .. } => {}
            Pending { pending_peers }
            | Ready { pending_peers } => {
                let len = potential_peers.len().min(
                    max_pending - pending_peers.len(),
                );

                let peers = potential_peers.iter()
                    .take(len)
                    .cloned()
                    .collect::<Vec<_>>();

                for peer in peers {
                    potential_peers.remove(&peer);
                    pending_peers.insert(PendingPeer::new(peer.into(), Handshake::Outgoing(
                        HandshakeStep::Initiated { at },
                    )));
                    requests.insert(PendingRequestState {
                        status: RequestState::Idle { at },
                        request: PendingRequest::ConnectPeer { peer: peer.into() },
                    });
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

    pub(crate) fn disconnect_peer(
        &mut self,
        at: Instant,
        address: PeerAddress,
    ) {
        let pending_peer_addr = self.pending_peers_mut()
            .and_then(|pending_peers| pending_peers.remove(&address))
            .and_then(|peer| peer.listener_address());

        let listener_addr = self.connected_peers.remove(&address)
            .map(|peer| peer.listener_address())
            .or(pending_peer_addr);

        if let Some(addr) = listener_addr {
            self.extend_potential_peers(std::iter::once(addr));
        }

        self.requests.insert(PendingRequestState {
            request: PendingRequest::DisconnectPeer { peer: address },
            status: RequestState::Idle { at },
        });
    }

    pub(crate) fn blacklist_peer(
        &mut self,
        at: Instant,
        address: PeerAddress,
    ) {
        let pending_peer_port = self.pending_peers_mut()
            .and_then(|pending_peers| pending_peers.remove(&address))
            .and_then(|peer| peer.listener_port());

        let listener_port = self.connected_peers.remove(&address)
            .map(|peer| peer.listener_port())
            .or(pending_peer_port);

        self.blacklisted_peers.insert_ip(address.ip(), BlacklistedPeer {
            since: at,
            port: listener_port,
        });
        // TODO: blacklist identity as well.

        self.requests.insert(PendingRequestState {
            request: PendingRequest::BlacklistPeer { peer: address },
            status: RequestState::Idle { at },
        });
    }

    pub(crate) fn nack_peer_handshake(
        &mut self,
        at: Instant,
        peer: PeerAddress,
        motive: NackMotive
    ) {
        if let Some(pending_peers) = self.pending_peers_mut() {
            let entry = self.requests.vacant_entry();
            // TODO: send nack
            self.disconnect_peer(at, peer);
        }
    }

    pub fn stats(&self) -> TezedgeStats {
        TezedgeStats {
            newest_time_seen: self.newest_time_seen,
            last_periodic_react: self.last_periodic_react,
            potential_peers_len: self.potential_peers.len(),
            connected_peers_len: self.connected_peers.len(),
            blacklisted_peers_len: self.blacklisted_peers.len(),
            pending_peers_len: self.pending_peers_len(),
            requests_len: self.requests.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TezedgeStats {
    pub newest_time_seen: Instant,
    pub last_periodic_react: Instant,
    pub potential_peers_len: usize,
    pub connected_peers_len: usize,
    pub blacklisted_peers_len: usize,
    pub pending_peers_len: usize,
    pub requests_len: usize,
}

