use tla_sm::{Proposal, Acceptor};

use crate::{RequestState, TezedgeState, PendingRequest, PendingRequestState};
use crate::proposals::{PendingRequestProposal, PendingRequestMsg};

impl Acceptor<PendingRequestProposal> for TezedgeState {
    fn accept(&mut self, proposal: PendingRequestProposal) {
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }

        if let Some(req) = self.requests.get_mut(proposal.req_id) {
            match &req.request {
                PendingRequest::StartListeningForNewPeers => {
                    match proposal.message {
                        PendingRequestMsg::StartListeningForNewPeersPending => {
                            req.status = RequestState::Pending { at: proposal.at };
                        }
                        PendingRequestMsg::StartListeningForNewPeersSuccess => {
                            self.requests.remove(proposal.req_id);
                        }
                        _ => eprintln!("unexpected request type"),
                    }
                }
                PendingRequest::StopListeningForNewPeers => {
                    match proposal.message {
                        PendingRequestMsg::StopListeningForNewPeersPending => {
                            req.status = RequestState::Pending { at: proposal.at };
                        }
                        PendingRequestMsg::StopListeningForNewPeersSuccess => {
                            self.requests.remove(proposal.req_id);
                        }
                        _ => eprintln!("unexpected request type"),
                    }
                }
                PendingRequest::NackAndDisconnectPeer { peer, .. } => {
                    match proposal.message {
                        PendingRequestMsg::SendPeerAckPending => {
                            req.status = RequestState::Pending { at: proposal.at };
                        }
                        PendingRequestMsg::SendPeerAckSuccess => {
                            *req = PendingRequestState {
                                request: PendingRequest::DisconnectPeer {
                                    peer: peer.clone(),
                                },
                                status: RequestState::Idle { at: proposal.at },
                            };
                        }
                        _ => eprintln!("unexpected request type"),
                    }
                }
                PendingRequest::DisconnectPeer { peer, .. } => {
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
                PendingRequest::BlacklistPeer { .. } => {
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
            }
        } else {
            eprintln!("req not found");
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
