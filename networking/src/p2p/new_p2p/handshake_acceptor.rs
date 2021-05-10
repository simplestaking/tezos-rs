use std::time::Instant;

use crypto::nonce::Nonce;
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};
use super::{Acceptor, AcceptorError, HandshakeStep, P2pState, PeerId, PeerState, Proposal, RequestState, TezedgeState};

pub enum HandshakeError {
    ProposalOutdated,
    MaximumPeersReached,
    PeerBlacklisted {
        till: Instant,
    },
    InvalidMsg,
}

pub type HandshakeAcceptorError = AcceptorError<HandshakeError>;

impl From<HandshakeError> for HandshakeAcceptorError {
    fn from(error: HandshakeError) -> Self {
        Self::Custom(error)
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

impl Proposal for HandshakeProposal {
    fn time(&self) -> Instant {
        self.at
    }
}

impl Acceptor<HandshakeProposal> for TezedgeState {
    type Error = HandshakeError;

    fn accept(&mut self, proposal: HandshakeProposal) -> Result<(), HandshakeAcceptorError> {
        self.validate_proposal(&proposal)?;

        match self.p2p_state {
            P2pState::PendingFull | P2pState::ReadyFull => {
                return Err(HandshakeError::MaximumPeersReached.into());
            }
            P2pState::Pending | P2pState::Ready => {}
        };

        let peer_state = self.peers.get_mut(&proposal.peer);

        match (&proposal.message, peer_state) {
            (_, Some(PeerState::Blacklisted { at })) =>{
                return Err(HandshakeError::PeerBlacklisted {
                    till: *at + self.config.peer_blacklist_duration,
                }.into());
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
                    self.peers.insert(proposal.peer.clone(), new_state);
                }
            }
            (HandshakeMsg::ReceivedConnect(_), _) => {
                return Err(HandshakeError::InvalidMsg.into())
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
                return Err(HandshakeError::InvalidMsg.into())
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
                return Err(HandshakeError::InvalidMsg.into())
            },

            // SendConnect
            (HandshakeMsg::SendConnectPending,
                Some(PeerState::OutgoingHandshake(HandshakeStep::ConnectionSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::ConnectionSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendConnectPending, _) => {
                return Err(HandshakeError::InvalidMsg.into())
            },

            (HandshakeMsg::SendConnectSuccess,
                Some(PeerState::OutgoingHandshake(HandshakeStep::ConnectionSent(req_state @ RequestState::Pending { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::ConnectionSent(req_state @ RequestState::Pending { .. })))
            ) => {
                *req_state = RequestState::Success { at: proposal.at };
            }
            (HandshakeMsg::SendConnectSuccess, _) => {
                return Err(HandshakeError::InvalidMsg.into())
            },

            (HandshakeMsg::SendConnectError,
                Some(peer_state @ PeerState::OutgoingHandshake(HandshakeStep::ConnectionSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingHandshake(HandshakeStep::ConnectionSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Blacklisted { at: proposal.at };
            }
            (HandshakeMsg::SendConnectError, _) => {
                return Err(HandshakeError::InvalidMsg.into())
            },

            // SendMeta
            (HandshakeMsg::SendMetaPending,
                Some(PeerState::OutgoingHandshake(HandshakeStep::MetaSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::MetaSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendMetaPending, _) => {
                return Err(HandshakeError::InvalidMsg.into())
            },

            (HandshakeMsg::SendMetaSuccess,
                Some(PeerState::OutgoingHandshake(HandshakeStep::MetaSent(req_state @ RequestState::Pending { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::MetaSent(req_state @ RequestState::Pending { .. })))
            ) => {
                *req_state = RequestState::Success { at: proposal.at };
            }
            (HandshakeMsg::SendMetaSuccess, _) => {
                return Err(HandshakeError::InvalidMsg.into())
            },

            (HandshakeMsg::SendMetaError,
                Some(peer_state @ PeerState::OutgoingHandshake(HandshakeStep::MetaSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingHandshake(HandshakeStep::MetaSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Blacklisted { at: proposal.at };
            }
            (HandshakeMsg::SendMetaError, _) => {
                return Err(HandshakeError::InvalidMsg.into())
            },

            // SendAck
            (HandshakeMsg::SendAckPending,
                Some(PeerState::OutgoingHandshake(HandshakeStep::AckSent(req_state @ RequestState::Idle { .. })))
                | Some(PeerState::IncomingHandshake(HandshakeStep::AckSent(req_state @ RequestState::Idle { .. })))
            ) => {
                *req_state = RequestState::Pending { at: proposal.at };
            }
            (HandshakeMsg::SendAckPending, _) => {
                return Err(HandshakeError::InvalidMsg.into())
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
                return Err(HandshakeError::InvalidMsg.into())
            },

            (HandshakeMsg::SendAckError,
                Some(peer_state @ PeerState::OutgoingHandshake(HandshakeStep::AckSent(RequestState::Pending { .. })))
                | Some(peer_state @ PeerState::IncomingHandshake(HandshakeStep::AckSent(RequestState::Pending { .. })))
            ) => {
                *peer_state = PeerState::Blacklisted { at: proposal.at };
            }
            (HandshakeMsg::SendAckError, _) => {
                return Err(HandshakeError::InvalidMsg.into())
            },
        }

        Ok(())
    }
}
