use std::mem;
use std::time::Instant;
use std::collections::BTreeMap;

use crypto::nonce::Nonce;
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};
use super::{GetRequests, acceptor::{Acceptor, React, Proposal, NewestTimeSeen}};
use super::{ConnectedPeer, Handshake, HandshakeStep, P2pState, PeerAddress, RequestState, TezedgeState, TezedgeRequest};

#[derive(Debug, Clone)]
pub enum PendingRequestMsg {
    DisconnectPeerPending,
    DisconnectPeerSuccess,

    BlacklistPeerPending,
    BlacklistPeerSuccess,
}

#[derive(Debug, Clone)]
pub struct PendingRequestProposal {
    pub at: Instant,
    pub req_id: usize,
    pub message: PendingRequestMsg,
}

impl Proposal for PendingRequestProposal {
    fn time(&self) -> Instant {
        self.at
    }
}

impl Acceptor<PendingRequestProposal> for TezedgeState {
    fn accept(&mut self, proposal: PendingRequestProposal) {
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }

        if let Some(req) = self.requests.get_mut(proposal.req_id) {
            match &req.request {
                TezedgeRequest::DisconnectPeer { peer, .. } => {
                    match proposal.message {
                        PendingRequestMsg::DisconnectPeerPending => {
                            req.status = RequestState::Pending { at: proposal.at };
                        }
                        PendingRequestMsg::DisconnectPeerSuccess => {
                            if let Some((peer_address, _)) = self.connected_peers.remove_entry(peer) {
                                self.potential_peers.push(peer_address);
                            }
                            self.requests.remove(proposal.req_id);
                        }
                        _ => eprintln!("unexpected request type"),
                    }
                }
                TezedgeRequest::BlacklistPeer { .. } => {
                    match proposal.message {
                        PendingRequestMsg::BlacklistPeerPending => {
                            req.status = RequestState::Pending { at: proposal.at };
                        }
                        PendingRequestMsg::BlacklistPeerSuccess => {
                            self.requests.remove(proposal.req_id);
                        }
                        _ => eprintln!("unexpected request type"),
                    }
                }
                _ => {
                    eprintln!("unexpected request type");
                }
            }
        } else {
            eprintln!("req not found");
        }

        self.react(proposal.at);
    }
}
