use tla_sm::Acceptor;
use crate::{Handshake, HandshakeStep, P2pState, PendingPeer, TezedgeState};
use crate::proposals::NewPeerConnectProposal;

impl<E> Acceptor<NewPeerConnectProposal> for TezedgeState<E> {
    fn accept(&mut self, proposal: NewPeerConnectProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        match &mut self.p2p_state {
            P2pState::Pending { pending_peers }
            | P2pState::Ready { pending_peers } => {
                pending_peers.insert(PendingPeer::new(
                    proposal.peer.clone(),
                    Handshake::Incoming(HandshakeStep::Initiated { at: proposal.at }),
                ));
            }
            _ => {
                self.disconnect_peer(proposal.at, proposal.peer);
            }
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
