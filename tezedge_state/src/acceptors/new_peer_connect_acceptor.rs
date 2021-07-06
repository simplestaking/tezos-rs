use tla_sm::Acceptor;
use crate::{Effects, HandshakeStep, P2pState, PendingPeer, TezedgeState};
use crate::proposals::NewPeerConnectProposal;

impl<E: Effects> Acceptor<NewPeerConnectProposal> for TezedgeState<E> {
    fn accept(&mut self, proposal: NewPeerConnectProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        match self.p2p_state {
            P2pState::Pending | P2pState::Ready => {
                self.pending_peers.insert(PendingPeer::new(
                    proposal.peer.clone(),
                    true,
                    HandshakeStep::Initiated { at: proposal.at },
                ));
            }
            P2pState::PendingFull | P2pState::ReadyFull | P2pState::ReadyMaxed => {
                slog::warn!(&self.log, "Disconnecting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Requested by the Proposer");
                self.disconnect_peer(proposal.at, proposal.peer);
            }
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
