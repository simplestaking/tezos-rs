use std::time::Instant;
use crypto::hash::CryptoboxPublicKeyHash;

use tezos_messages::p2p::encoding::peer::PeerMessageResponse;
pub use tla_sm::{Proposal, GetRequests};
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    MetadataMessage,
};

use crate::state::TezedgeState;
use crate::PeerAddress;

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
    ConnectPeer {
        req_id: usize,
        peer: PeerAddress,
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
    ConnectPeer {
        peer: PeerAddress,
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

impl<E> GetRequests for TezedgeState<E> {
    type Request = TezedgeRequest;

    fn get_requests(&self, buf: &mut Vec<TezedgeRequest>) -> usize {
        let buf_initial_len = buf.len();

        for (req_id, req) in self.requests.iter() {
            if let RequestState::Idle { .. } = req.status {
                buf.push(match &req.request {
                    PendingRequest::StartListeningForNewPeers => {
                        TezedgeRequest::StartListeningForNewPeers { req_id }
                    }
                    PendingRequest::StopListeningForNewPeers => {
                        TezedgeRequest::StopListeningForNewPeers { req_id }
                    }
                    PendingRequest::ConnectPeer { peer } => {
                        let peer = peer.clone();
                        TezedgeRequest::ConnectPeer { req_id, peer }
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
