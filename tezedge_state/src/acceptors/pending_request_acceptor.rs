use tezos_messages::p2p::encoding::connection::ConnectionMessage;
use tla_sm::Acceptor;

use crate::{Effects, HandshakeStep, PendingRequest, RequestState, TezedgeState};
use crate::proposals::{PendingRequestProposal, PendingRequestMsg};

impl<E> Acceptor<PendingRequestProposal> for TezedgeState<E>
    where E: Effects
{
    fn accept(&mut self, proposal: PendingRequestProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
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
                PendingRequest::ConnectPeer { peer, .. } => {
                    match proposal.message {
                        PendingRequestMsg::ConnectPeerPending => {
                            req.status = RequestState::Pending { at: proposal.at };
                        }
                        PendingRequestMsg::ConnectPeerSuccess => {
                            let peer_address = *peer;
                            let sent_conn_msg = ConnectionMessage::try_new(
                                self.config.port,
                                &self.identity.public_key,
                                &self.identity.proof_of_work_stamp,
                                self.effects.get_nonce(&peer_address),
                                self.shell_compatibility_version.to_network_version(),
                            ).unwrap();
                            let peer = self.pending_peers_mut()
                                .and_then(|peers| peers.get_mut(&peer_address));

                            if let Some(peer) = peer {
                                peer.step = HandshakeStep::Connect {
                                    sent_conn_msg,
                                    sent: RequestState::Idle { at: proposal.at },
                                    received: None,
                                };
                            }
                            self.requests.remove(proposal.req_id);
                        }
                        PendingRequestMsg::ConnectPeerError => {
                            let peer = *peer;
                            self.blacklist_peer(proposal.at, peer);
                            self.requests.remove(proposal.req_id);
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
                            if let Some(peer) = self.connected_peers.remove(peer) {
                                self.extend_potential_peers(std::iter::once(peer.listener_address()));
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
                PendingRequest::PeerMessageReceived { .. } => {
                    match proposal.message {
                        PendingRequestMsg::PeerMessageReceivedNotified => {
                            self.requests.remove(proposal.req_id);
                        }
                        _ => eprintln!("unexpected request type"),
                    }
                }
                PendingRequest::NotifyHandshakeSuccessful { .. } => {
                    match proposal.message {
                        PendingRequestMsg::HandshakeSuccessfulNotified => {
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
