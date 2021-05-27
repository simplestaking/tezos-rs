use tla_sm::{Proposal, Acceptor};

use crate::{RequestState, TezedgeState, TezedgeRequest};
use crate::proposals::{PendingRequestProposal, PendingRequestMsg};

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

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
