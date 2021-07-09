use std::fmt::Debug;
use std::time::{Instant, Duration};
use std::collections::HashSet;
use slog::{Logger, warn};

use crypto::crypto_box::{CryptoKey, PublicKey};
pub use tla_sm::{Proposal, GetRequests};
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::prelude::MetadataMessage;

use crate::{DefaultEffects, Effects, InvalidProposalError, PeerAddress, Port, ShellCompatibilityVersion};
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

#[derive(Debug, Clone, Copy)]
pub enum P2pState {
    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Pending,

    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections reached.
    PendingFull,

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Ready,

    /// Minimum number of connected peers reached.
    /// Maximum number of connected peers **not** reached.
    /// Maximum number of pending peers reached.
    ReadyFull,

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

#[derive(Debug)]
pub struct TezedgeState<E = DefaultEffects> {
    pub(crate) log: Logger,
    pub(crate) listening_for_connection_requests: bool,
    pub(crate) newest_time_seen: Instant,
    pub(crate) last_periodic_react: Instant,
    pub(crate) config: TezedgeConfig,
    pub(crate) identity: Identity,
    pub(crate) shell_compatibility_version: ShellCompatibilityVersion,
    pub(crate) effects: E,
    pub(crate) potential_peers: HashSet<PeerListenerAddress>,
    pub(crate) pending_peers: PendingPeers,
    pub(crate) connected_peers: ConnectedPeers,
    pub(crate) blacklisted_peers: BlacklistedPeers,
    // TODO: blacklist identities as well.
    pub(crate) p2p_state: P2pState,
    pub(crate) requests: slab::Slab<PendingRequestState>,
}

impl<E> TezedgeState<E> {
    pub fn new(
        log: Logger,
        config: TezedgeConfig,
        identity: Identity,
        shell_compatibility_version: ShellCompatibilityVersion,
        effects: E,
        initial_time: Instant,
    ) -> Self
    {
        let periodic_react_interval = config.periodic_react_interval;
        let max_connected_peers = config.max_connected_peers;
        let max_pending_peers = config.max_pending_peers;

        Self {
            log,
            config,
            identity,
            shell_compatibility_version,
            effects,
            listening_for_connection_requests: false,
            potential_peers: HashSet::new(),
            pending_peers: PendingPeers::with_capacity(max_pending_peers),
            connected_peers: ConnectedPeers::with_capacity(max_connected_peers),
            blacklisted_peers: BlacklistedPeers::new(),
            p2p_state: P2pState::Pending,
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

    pub fn blacklisted_peers(&self) -> &BlacklistedPeers {
        &self.blacklisted_peers
    }

    pub fn request_states(&self) -> &slab::Slab<PendingRequestState> {
        &self.requests
    }

    #[inline(always)]
    pub fn pending_peers_len(&self) -> usize {
        self.pending_peers.len()
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

    pub(crate) fn disconnect_peer(
        &mut self,
        at: Instant,
        address: PeerAddress,
    ) {
        let pending_peer_addr = self.pending_peers
            .remove(&address)
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
        let pending_peer_port = self.pending_peers
            .remove(&address)
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

    #[inline]
    fn missing_connected_peers(&self) -> usize {
        self.config.max_connected_peers
            .checked_sub(self.connected_peers.len()).unwrap_or(0)
    }

    #[inline]
    fn missing_pending_peers(&self) -> usize {
        self.config.max_pending_peers
            .min(self.missing_connected_peers())
            .checked_sub(self.pending_peers.len())
            .unwrap_or(0)
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

impl<E: Effects> TezedgeState<E> {
    pub(crate) fn set_peer_connected(
        &mut self,
        at: Instant,
        peer_address: PeerAddress,
        result: HandshakeResult,
    ) {
        // double check to make sure we don't go over the limit.
        use P2pState::*;
        self.adjust_p2p_state(at);
        match &self.p2p_state {
            Pending | PendingFull | Ready | ReadyFull => {}
            ReadyMaxed => {
                warn!(&self.log, "Blacklisting Peer"; "reason" => "Tried to connect to peer while we are maxed out on connected peers.");
                return self.blacklist_peer(at, peer_address);
            }
        }

        let public_key_hash = result.public_key.public_key_hash().unwrap();

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
        let missing_connected  = self.missing_connected_peers();
        let missing_pending = self.missing_pending_peers();

        let mut should_listen_for_connections = false;
        let mut should_initiate_connections = false;

        if missing_connected == 0 {
            self.p2p_state = ReadyMaxed;
        } else if self.connected_peers.len() < min_connected {
            if missing_pending == 0 {
                self.p2p_state = PendingFull;
            } else {
                should_listen_for_connections = true;
                should_initiate_connections = true;
                self.p2p_state = Pending;
                self.initiate_handshakes(at);
            }
        } else {
            if missing_pending == 0 {
                self.p2p_state = ReadyFull;
            } else {
                self.p2p_state = Ready;
                should_listen_for_connections = true;
                should_initiate_connections = true;
                self.initiate_handshakes(at);
            }
        }

        if should_listen_for_connections != self.listening_for_connection_requests {
            self.listening_for_connection_requests = should_listen_for_connections;

            if !should_listen_for_connections {
                self.requests.insert(PendingRequestState {
                    request: PendingRequest::StopListeningForNewPeers,
                    status: RequestState::Idle { at },
                });
            } else {
                self.requests.insert(PendingRequestState {
                    request: PendingRequest::StartListeningForNewPeers,
                    status: RequestState::Idle { at },
                });
            }
        }

        if should_initiate_connections {
            return self.initiate_handshakes(at);
        }
    }

    pub(crate) fn check_timeouts(&mut self, at: Instant) {
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

        use HandshakeStep::*;
        use RequestState::*;

        let end_handshakes = self.pending_peers.iter_mut()
            .filter_map(|(_, peer)| {
                let incoming = peer.incoming;

                match &mut peer.step {
                    // send or receive timed out based on `peer.incoming`
                    Initiated { at }

                    // send timed out
                    | Connect { sent: Idle { at, .. }, .. }
                    | Connect { sent: Pending { at, .. }, .. }
                    | Metadata { sent: Idle { at, .. }, .. }
                    | Metadata { sent: Pending { at, .. }, .. }
                    | Ack { sent: Idle { at, .. }, .. }
                    | Ack { sent: Pending { at, .. }, .. }

                    // receive timed out
                    | Connect { sent: Success { at, .. }, .. }
                    | Metadata { sent: Success { at, .. }, .. }
                    | Ack { sent: Success { at, .. }, .. }
                    => {
                        if now.duration_since(*at) >= peer_timeout {
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

    pub(crate) fn initiate_handshakes(&mut self, at: Instant) {
        use P2pState::*;
        let max_pending = self.config.max_pending_peers as usize;

        match self.p2p_state {
            ReadyMaxed | ReadyFull | PendingFull => return,
            Pending | Ready => {}
        }
        let len = self.potential_peers.len()
            .min(self.missing_pending_peers());

        if len == 0 {
            return
        }
        slog::info!(&self.log, "Initiating handshakes";
                     "connected_peers" => self.connected_peers.len(),
                     "pending_peers" => self.pending_peers.len(),
                     "potential_peers" => self.potential_peers.len(),
                     "initiated_handshakes" => len);

        let peers = self.effects.choose_peers_to_connect_to(
            &self.potential_peers,
            len,
        );

        for peer in peers {
            self.potential_peers.remove(&peer);
            self.pending_peers.insert(PendingPeer::new(
                peer.into(),
                false,
                HandshakeStep::Initiated { at },
            ));
            self.requests.insert(PendingRequestState {
                status: RequestState::Idle { at },
                request: PendingRequest::ConnectPeer { peer: peer.into() },
            });
        }

        self.adjust_p2p_state(at);
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
}

impl<E: Clone> Clone for TezedgeState<E> {
    fn clone(&self) -> Self {
        Self {
            log: self.log.clone(),
            config: self.config.clone(),
            identity: self.identity.clone(),
            shell_compatibility_version: self.shell_compatibility_version.clone(),
            effects: self.effects.clone(),
            listening_for_connection_requests: self.listening_for_connection_requests,
            newest_time_seen: self.newest_time_seen,
            last_periodic_react: self.last_periodic_react,

            p2p_state: self.p2p_state.clone(),
            potential_peers: self.potential_peers.clone(),
            pending_peers: self.pending_peers.clone(),
            connected_peers: self.connected_peers.clone(),
            blacklisted_peers: self.blacklisted_peers.clone(),
            requests: self.requests.clone(),
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

