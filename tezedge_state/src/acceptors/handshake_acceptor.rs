use std::time::Instant;
use tla_sm::Acceptor;

use crate::{Handshake, HandshakeStep, P2pState, PeerAddress, PendingPeer, RequestState, TezedgeState};
use crate::proposals::{HandshakeProposal, HandshakeMsg};

fn handle_send_connect_pending(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {

            match pending_peers.get_mut(&peer_address).map(|x| &mut x.handshake) {
                Some(Outgoing(Connect { sent: Some(status @ Idle { .. }), .. }))
                | Some(Incoming(Connect { sent: Some(status @ Idle { .. }), .. })) => {
                    *status = Pending { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                }
            }
        }
    }
}

fn handle_send_meta_pending(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address).map(|x| &mut x.handshake) {
                Some(Outgoing(Metadata { sent: Some(status @ Idle { .. }), .. }))
                | Some(Incoming(Metadata { sent: Some(status @ Idle { .. }), .. })) => {
                    *status = Pending { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                }
            }
        }
    }
}

fn handle_send_ack_pending(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address).map(|x| &mut x.handshake) {
                Some(Outgoing(Ack { sent: Some(status @ Idle { .. }), .. }))
                | Some(Incoming(Ack { sent: Some(status @ Idle { .. }), .. })) => {
                    *status = Pending { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                }
            }
        }
    }
}

fn handle_send_connect_success(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address).map(|x| &mut x.handshake) {
                Some(Outgoing(Connect { sent: Some(status @ Pending { .. }), .. }))
                | Some(Incoming(Connect { sent: Some(status @ Pending { .. }), .. })) => {
                    *status = Success { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                }
            }
        }
    }
}

fn handle_send_meta_success(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address).map(|x| &mut x.handshake) {
                Some(Outgoing(Metadata { sent: Some(status @ Pending { .. }), .. }))
                | Some(Incoming(Metadata { sent: Some(status @ Pending { .. }), .. })) => {
                    *status = Success { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                }
            }
        }
    }
}

fn handle_send_ack_success(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.remove(&peer_address).map(|p| p.handshake) {
                Some(Outgoing(mut step @ Ack { sent: Some(Pending { .. }), .. })) => {
                    match &mut step {
                        Ack { sent, .. } => *sent = Some(Success { at }),
                        _ => unreachable!(),
                    };
                    pending_peers.insert(PendingPeer::new(peer_address, Outgoing(step)));
                }
                Some(handshake @ Incoming(Ack { sent: Some(Pending { .. }), .. })) => {
                    let result = handshake.to_result().unwrap();
                    state.set_peer_connected(at, peer_address, result);
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {}
            }
        }
    }
}

fn handle_send_connect_error(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    // TODO: retry
    state.blacklist_peer(at, peer_address);
}

fn handle_send_meta_error(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    // TODO: retry
    state.blacklist_peer(at, peer_address);
}

fn handle_send_ack_error(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    // TODO: retry
    state.blacklist_peer(at, peer_address);
}

impl Acceptor<HandshakeProposal> for TezedgeState {
    fn accept(&mut self, proposal: HandshakeProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        // Return if maximum number of connections is already reached.
        if let P2pState::ReadyMaxed = self.p2p_state {
            return;
        }

        match proposal.message {
            HandshakeMsg::SendConnectPending => {
                handle_send_connect_pending(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendMetaPending => {
                handle_send_meta_pending(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendAckPending => {
                handle_send_ack_pending(self, proposal.at, proposal.peer)
            }

            HandshakeMsg::SendConnectSuccess => {
                handle_send_connect_success(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendMetaSuccess => {
                handle_send_meta_success(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendAckSuccess => {
                handle_send_ack_success(self, proposal.at, proposal.peer)
            }

            HandshakeMsg::SendConnectError => {
                handle_send_connect_error(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendMetaError => {
                handle_send_meta_error(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendAckError => {
                handle_send_ack_error(self, proposal.at, proposal.peer)
            }
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
