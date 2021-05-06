use std::time::{Duration, Instant};
use std::sync::Arc;
use std::collections::BTreeMap;
use getset::Getters;

use tezos_identity::Identity;
use tezos_messages::p2p::encoding::prelude::{
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};

pub enum Error {
    Poisoned,
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
    IncomingConnection(ConnectionStep),
    OutgoingConnection(ConnectionStep),
    Connected {
        at: Instant,
    },
    Blacklisted {
        at: Instant,
    },
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ConnectionStep {
    ConnectionSent(RequestState),
    ConnectionReceived {
        at: Instant,
    },
    MetaSent(RequestState),
    MetaReceived {
        at: Instant,
    },
    AckSent(RequestState),
    AckReceived {
        at: Instant,
    },
}

#[derive(Getters, Ord, Eq, Clone)]
pub struct Peer {
    #[getset(get = "pub")]
    name: String,
}

impl Peer {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl PartialOrd for Peer {
    fn partial_cmp(&self, other: &Peer) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl PartialEq for Peer {
    fn eq(&self, other: &Peer) -> bool {
        self.name == other.name
    }
}

pub enum HandshakeMsg {
    SendConnectPending,
    SendConnectSuccess,
    SendConnectError,

    SendMetaPending,
    SendMetaSuccess,
    SendMetaError,

    SendAckPending,
    SendAckSuccess,
    SendAckError,

    ReceivedConnect(ConnectionMessage),
    ReceivedMeta(MetadataMessage),
    ReceivedAck(AckMessage),
}

pub struct HandshakeProposal {
    at: Instant,
    peer: Peer,
    message: HandshakeMsg,
}

pub type PeerManagerResult = Result<(), Error>;

pub enum PeerManagerAction {
    SendPeerConnect((Peer, ConnectionMessage)),
    SendPeerMeta((Peer, MetadataMessage)),
    SendPeerAck((Peer, AckMessage)),
}

pub struct PeerManagerConfig {
    disable_mempool: bool,
    private_node: bool,
    min_connected_peers: u8,
    max_connected_peers: u8,
    peer_blacklist_duration: Duration,
    peer_timeout: Duration,
}

pub struct PeerManagerInner {
    config: PeerManagerConfig,
    identity: Identity,
    peers: BTreeMap<Peer, PeerState>,
    newest_time_seen: Instant,
}

pub enum PeerManager {
    /// Poisoned state.
    ///
    /// Can only happen if panic occures during transition.
    Poisoned,

    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections **not** reached.
    Pending(PeerManagerInner),

    /// Minimum number of connected peers **not** reached.
    /// Maximum number of pending connections reached.
    PendingFull(PeerManagerInner),

    /// Minimum number of connected peers reached.
    /// Maximum number of pending connections **not** reached.
    Ready(PeerManagerInner),

    /// Minimum number of connected peers reached.
    /// Maximum number of pending connections reached.
    ReadyFull(PeerManagerInner),
}

impl PeerManager {
    fn assert_proposal_not_outdated(&self, proposal: &HandshakeProposal) -> PeerManagerResult {
        let newest_time_seen = match self {
            Self::Poisoned => { return Err(Error::Poisoned); }
            Self::Pending(inner)
                | Self::PendingFull(inner)
                | Self::Ready(inner)
                | Self::ReadyFull(inner)
                => {
                    inner.newest_time_seen
                }
        };

        if newest_time_seen > proposal.at {
            Ok(())
        } else {
            Err(Error::ProposalOutdated)
        }
    }

    pub fn accept_handshake(&mut self, proposal: &HandshakeProposal) -> PeerManagerResult {
        self.assert_proposal_not_outdated(proposal)?;

        let this = match self {
            Self::Poisoned => {
                return Err(Error::Poisoned);
            }
            Self::PendingFull(_) | Self::ReadyFull(_) => {
                return Err(Error::MaximumPeersReached);
            }
            Self::Pending(this) | Self::Ready(this) => {
                this
            }
        };

        let peer_state = this.peers.get_mut(&proposal.peer);

        match (&proposal.message, peer_state) {
            (_, Some(PeerState::Blacklisted { at })) =>{
                return Err(Error::PeerBlacklisted {
                    till: *at + this.config.peer_blacklist_duration,
                });
            }

            // ReceivedConnect
            (HandshakeMsg::ReceivedConnect(conn_msg),
                Some(PeerState::OutgoingConnection(step @ ConnectionStep::ConnectionSent(RequestState::Success { .. })))
            ) => {
                *step = ConnectionStep::ConnectionReceived { at: proposal.at };
            }
            (HandshakeMsg::ReceivedConnect(conn_msg),
                peer_state @ Some(PeerState::Disconnected { .. })
                | peer_state @ None
            ) => {
                let new_state = PeerState::IncomingConnection(ConnectionStep::ConnectionReceived { at: proposal.at });
                if let Some(peer_state) = peer_state {
                    *peer_state = new_state;
                } else {
                    this.peers.insert(proposal.peer.clone(), new_state);
                }
            }
            (HandshakeMsg::ReceivedConnect(_), _) => {
                return Err(Error::InvalidMsg)
            },

            // ReceivedMeta
            (HandshakeMsg::ReceivedMeta(meta_msg),
                Some(PeerState::OutgoingConnection(step @ ConnectionStep::MetaSent(RequestState::Success { .. })))
                | Some(PeerState::IncomingConnection(step @ ConnectionStep::ConnectionSent(RequestState::Success { .. })))
            ) => {
                *step = ConnectionStep::MetaReceived { at: proposal.at };
            }
            (HandshakeMsg::ReceivedMeta(_), _) => {
                return Err(Error::InvalidMsg)
            },

            // ReceivedAck
            (HandshakeMsg::ReceivedAck(meta_msg),
                Some(PeerState::OutgoingConnection(step @ ConnectionStep::AckSent(RequestState::Success { .. })))
                | Some(PeerState::IncomingConnection(step @ ConnectionStep::MetaSent(RequestState::Success { .. })))
            ) => {
                *step = ConnectionStep::MetaReceived { at: proposal.at };
            }
            (HandshakeMsg::ReceivedAck(_), _) => {
                return Err(Error::InvalidMsg)
            },

            // SendConnect
            (HandshakeMsg::SendConnectPending,
                Some(PeerState::OutgoingConnection(ConnectionStep::ConnectionSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingConnection(ConnectionStep::ConnectionSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendConnectPending, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendConnectSuccess,
                Some(PeerState::OutgoingConnection(ConnectionStep::ConnectionSent(req_state @ RequestState::Pending { .. })))
                | Some(PeerState::IncomingConnection(ConnectionStep::ConnectionSent(req_state @ RequestState::Pending { .. })))
            ) => {
                *req_state = RequestState::Success { at: proposal.at };
            }
            (HandshakeMsg::SendConnectSuccess, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendConnectError,
                Some(peer_state @ PeerState::OutgoingConnection(ConnectionStep::ConnectionSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingConnection(ConnectionStep::ConnectionSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Blacklisted { at: proposal.at };
            }
            (HandshakeMsg::SendConnectError, _) => {
                return Err(Error::InvalidMsg)
            },

            // SendMeta
            (HandshakeMsg::SendMetaPending,
                Some(PeerState::OutgoingConnection(ConnectionStep::MetaSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingConnection(ConnectionStep::MetaSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendMetaPending, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendMetaSuccess,
                Some(PeerState::OutgoingConnection(ConnectionStep::MetaSent(req_state @ RequestState::Pending { .. })))
                | Some(PeerState::IncomingConnection(ConnectionStep::MetaSent(req_state @ RequestState::Pending { .. })))
            ) => {
                *req_state = RequestState::Success { at: proposal.at };
            }
            (HandshakeMsg::SendMetaSuccess, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendMetaError,
                Some(peer_state @ PeerState::OutgoingConnection(ConnectionStep::MetaSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingConnection(ConnectionStep::MetaSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Blacklisted { at: proposal.at };
            }
            (HandshakeMsg::SendMetaError, _) => {
                return Err(Error::InvalidMsg)
            },

            // SendAck
            (HandshakeMsg::SendAckPending,
                Some(PeerState::OutgoingConnection(ConnectionStep::AckSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingConnection(ConnectionStep::AckSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendAckPending, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendAckSuccess,
                Some(PeerState::OutgoingConnection(ConnectionStep::AckSent(req_state @ RequestState::Pending { .. })))
            ) => {
            }
            (HandshakeMsg::SendAckSuccess,
                Some(peer_state @ PeerState::IncomingConnection(ConnectionStep::AckSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Connected { at: proposal.at }
            }
            (HandshakeMsg::SendAckSuccess, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendAckError,
                Some(peer_state @ PeerState::OutgoingConnection(ConnectionStep::AckSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingConnection(ConnectionStep::AckSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Blacklisted { at: proposal.at };
            }
            (HandshakeMsg::SendAckError, _) => {
                return Err(Error::InvalidMsg)
            },
        }

        Ok(())
    }

    pub fn react(&mut self, at: Instant) -> Vec<PeerManagerAction> {
        let mut actions = vec![];

        actions
    }
}
