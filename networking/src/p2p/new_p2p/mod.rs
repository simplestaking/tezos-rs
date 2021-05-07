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

#[derive(Debug)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct HandshakeProposal {
    pub at: Instant,
    pub peer: PeerId,
    pub message: HandshakeMsg,
}

pub type PeerManagerResult = Result<(), Error>;

pub enum PeerManagerAction {
    SendPeerConnect((PeerId, ConnectionMessage)),
    SendPeerMeta((PeerId, MetadataMessage)),
    SendPeerAck((PeerId, AckMessage)),
}

#[derive(Debug, Clone)]
pub struct PeerManagerConfig {
    pub disable_mempool: bool,
    pub private_node: bool,
    pub min_connected_peers: u8,
    pub max_connected_peers: u8,
    pub peer_blacklist_duration: Duration,
    pub peer_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct PeerManagerInner {
    config: PeerManagerConfig,
    identity: Identity,
    peers: BTreeMap<PeerId, PeerState>,
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
    pub fn new(
        config: PeerManagerConfig,
        identity: Identity,
        initial_time: Instant,
    ) -> Self
    {
        Self::Pending(PeerManagerInner {
            config,
            identity,
            peers: BTreeMap::new(),
            newest_time_seen: initial_time,
        })
    }

    fn inner(&self) -> &PeerManagerInner {
        match self {
            Self::Poisoned => panic!(),
            Self::Pending(inner)
                | Self::PendingFull(inner)
                | Self::Ready(inner)
                | Self::ReadyFull(inner)
                => {
                    inner
                }
        }
    }

    fn inner_mut(&mut self) -> &mut PeerManagerInner {
        match self {
            Self::Poisoned => panic!(),
            Self::Pending(inner)
                | Self::PendingFull(inner)
                | Self::Ready(inner)
                | Self::ReadyFull(inner)
                => {
                    inner
                }
        }
    }

    pub fn peer_state(&self, peer_id: &PeerId) -> Option<&PeerState> {
        self.inner().peers.get(peer_id)
    }

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
            Err(Error::ProposalOutdated)
        } else {
            Ok(())
        }
    }

    fn update_newest_time_seen(&mut self, time: Instant) {
        self.inner_mut().newest_time_seen = time;

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
                Some(PeerState::OutgoingHandshake(step @ HandshakeStep::ConnectionSent(RequestState::Success { .. })))
            ) => {
                *step = HandshakeStep::MetaSent(RequestState::Idle { at: proposal.at });
            }
            (HandshakeMsg::ReceivedConnect(conn_msg),
                peer_state @ Some(PeerState::Disconnected { .. })
                | peer_state @ None
            ) => {
                let step = HandshakeStep::ConnectionSent(RequestState::Idle { at: proposal.at });
                let new_state = PeerState::IncomingHandshake(step);
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
                Some(PeerState::OutgoingHandshake(step @ HandshakeStep::MetaSent(RequestState::Success { .. })))
            ) => {
                *step = HandshakeStep::AckSent(RequestState::Idle { at: proposal.at });
            }
            (HandshakeMsg::ReceivedMeta(meta_msg),
                Some(PeerState::IncomingHandshake(step @ HandshakeStep::ConnectionSent(RequestState::Success { .. })))
            ) => {
                *step = HandshakeStep::MetaSent(RequestState::Idle { at: proposal.at });
            }
            (HandshakeMsg::ReceivedMeta(_), _) => {
                return Err(Error::InvalidMsg)
            },

            // ReceivedAck
            (HandshakeMsg::ReceivedAck(AckMessage::Ack),
                Some(peer_state @ PeerState::OutgoingHandshake(HandshakeStep::AckSent(RequestState::Success { .. })))
            ) => {
                *peer_state = PeerState::Connected { at: proposal.at };
            }
            (HandshakeMsg::ReceivedAck(AckMessage::Ack),
                Some(PeerState::IncomingHandshake(step @ HandshakeStep::MetaSent(RequestState::Success { .. })))
            ) => {
                *step = HandshakeStep::AckSent(RequestState::Idle { at: proposal.at });
            }
            (HandshakeMsg::ReceivedAck(_), _) => {
                return Err(Error::InvalidMsg)
            },

            // SendConnect
            (HandshakeMsg::SendConnectPending,
                Some(PeerState::OutgoingHandshake(HandshakeStep::ConnectionSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::ConnectionSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendConnectPending, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendConnectSuccess,
                Some(PeerState::OutgoingHandshake(HandshakeStep::ConnectionSent(req_state @ RequestState::Pending { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::ConnectionSent(req_state @ RequestState::Pending { .. })))
            ) => {
                *req_state = RequestState::Success { at: proposal.at };
            }
            (HandshakeMsg::SendConnectSuccess, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendConnectError,
                Some(peer_state @ PeerState::OutgoingHandshake(HandshakeStep::ConnectionSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingHandshake(HandshakeStep::ConnectionSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Blacklisted { at: proposal.at };
            }
            (HandshakeMsg::SendConnectError, _) => {
                return Err(Error::InvalidMsg)
            },

            // SendMeta
            (HandshakeMsg::SendMetaPending,
                Some(PeerState::OutgoingHandshake(HandshakeStep::MetaSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::MetaSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendMetaPending, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendMetaSuccess,
                Some(PeerState::OutgoingHandshake(HandshakeStep::MetaSent(req_state @ RequestState::Pending { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::MetaSent(req_state @ RequestState::Pending { .. })))
            ) => {
                *req_state = RequestState::Success { at: proposal.at };
            }
            (HandshakeMsg::SendMetaSuccess, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendMetaError,
                Some(peer_state @ PeerState::OutgoingHandshake(HandshakeStep::MetaSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingHandshake(HandshakeStep::MetaSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Blacklisted { at: proposal.at };
            }
            (HandshakeMsg::SendMetaError, _) => {
                return Err(Error::InvalidMsg)
            },

            // SendAck
            (HandshakeMsg::SendAckPending,
                Some(PeerState::OutgoingHandshake(HandshakeStep::AckSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::AckSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendAckPending, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendAckSuccess,
                Some(PeerState::OutgoingHandshake(HandshakeStep::AckSent(req_state @ RequestState::Pending { .. })))
            ) => {
                *req_state = RequestState::Success { at: proposal.at };
            }
            (HandshakeMsg::SendAckSuccess,
                Some(peer_state @ PeerState::IncomingHandshake(HandshakeStep::AckSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Connected { at: proposal.at }
            }
            (HandshakeMsg::SendAckSuccess, _) => {
                return Err(Error::InvalidMsg)
            },

            (HandshakeMsg::SendAckError,
                Some(peer_state @ PeerState::OutgoingHandshake(HandshakeStep::AckSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingHandshake(HandshakeStep::AckSent(RequestState::Pending { .. })))
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
