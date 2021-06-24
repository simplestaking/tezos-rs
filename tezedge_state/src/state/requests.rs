use std::fmt::{self, Debug};
use std::time::{Instant, Duration};
use crypto::hash::CryptoboxPublicKeyHash;

use tezos_messages::p2p::encoding::peer::PeerMessageResponse;
pub use tla_sm::{Proposal, GetRequests};
use tezos_messages::p2p::encoding::ack::{NackInfo, NackMotive};
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};

use crate::state::{TezedgeState, P2pState};
use crate::state::pending_peers::*;
use crate::{InvalidProposalError, PeerCrypto, PeerAddress, Port};

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

/// Requests which may be made after accepting handshake proposal.
#[derive(Debug, Clone)]
pub enum TezedgeRequest {
    StartListeningForNewPeers {
        req_id: usize,
    },
    StopListeningForNewPeers {
        req_id: usize,
    },
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
    PeerMessageReceived {
        req_id: usize,
        peer: PeerAddress,
        message: PeerMessageResponse,
    },
    NotifyHandshakeSuccessful {
        req_id: usize,
        peer_address: PeerAddress,
        peer_public_key_hash: CryptoboxPublicKeyHash,
        metadata: MetadataMessage,
        network_version: NetworkVersion,
    },
}

#[derive(Debug, Clone)]
pub enum PendingRequest {
    StartListeningForNewPeers,
    StopListeningForNewPeers,
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

    // -- TODO: temporary until everything is handled inside TezedgeState.
    PeerMessageReceived {
        peer: PeerAddress,
        message: PeerMessageResponse,
    },
    NotifyHandshakeSuccessful {
        peer_address: PeerAddress,
        peer_public_key_hash: CryptoboxPublicKeyHash,
        metadata: MetadataMessage,
        network_version: NetworkVersion,
    }
}

#[derive(Debug, Clone)]
pub struct PendingRequestState {
    pub request: PendingRequest,
    pub status: RequestState,
}

impl GetRequests for TezedgeState {
    type Request = TezedgeRequest;

    fn get_requests(&self, buf: &mut Vec<TezedgeRequest>) -> usize {
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        let buf_initial_len = buf.len();

        match &self.p2p_state {
            P2pState::ReadyMaxed => {}
            P2pState::Ready { pending_peers }
            | P2pState::ReadyFull { pending_peers }
            | P2pState::Pending { pending_peers }
            | P2pState::PendingFull { pending_peers } => {
                for (id, pending_peer) in pending_peers.iter() {
                    match &pending_peer.handshake {
                        Incoming(Initiated { .. }) | Outgoing(Initiated { .. }) => {}

                        Incoming(Connect { sent: Some(Idle { .. }), sent_conn_msg, .. })
                        | Outgoing(Connect { sent: Some(Idle { .. }), sent_conn_msg, .. }) => {
                            buf.push(
                                TezedgeRequest::SendPeerConnect {
                                    peer: pending_peer.address.clone(),
                                    message: sent_conn_msg.clone(),
                                }
                            );
                        }
                        Incoming(Connect { .. }) | Outgoing(Connect { .. }) => {}

                        Incoming(Metadata { sent: Some(Idle { .. }), .. })
                        | Outgoing(Metadata { sent: Some(Idle { .. }), .. }) => {
                            buf.push(
                                TezedgeRequest::SendPeerMeta {
                                    peer: pending_peer.address.clone(),
                                    message: self.meta_msg(),
                                }
                            )
                        }
                        Incoming(Metadata { .. }) | Outgoing(Metadata { .. }) => {}

                        Incoming(Ack { sent: Some(Idle { .. }), .. })
                        | Outgoing(Ack { sent: Some(Idle { .. }), .. }) => {
                            buf.push(
                                TezedgeRequest::SendPeerAck {
                                    peer: pending_peer.address.clone(),
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
                buf.push(match &req.request {
                    PendingRequest::StartListeningForNewPeers => {
                        TezedgeRequest::StartListeningForNewPeers { req_id }
                    }
                    PendingRequest::StopListeningForNewPeers => {
                        TezedgeRequest::StopListeningForNewPeers { req_id }
                    }
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
                    PendingRequest::PeerMessageReceived { peer, message } => {
                        TezedgeRequest::PeerMessageReceived {
                            req_id,
                            peer: peer.clone(),
                            // TODO: find a way to get rid of cloning.
                            message: message.clone(),
                        }
                    }
                    PendingRequest::NotifyHandshakeSuccessful {
                        peer_address, peer_public_key_hash, metadata, network_version
                    } => {
                        TezedgeRequest::NotifyHandshakeSuccessful {
                            req_id,
                            peer_address: peer_address.clone(),
                            peer_public_key_hash: peer_public_key_hash.clone(),
                            metadata: metadata.clone(),
                            network_version: network_version.clone(),
                        }
                    }
                })
            }
        }

        buf.len() - buf_initial_len
    }
}
