use tla_sm::Acceptor;
use crate::{P2pState, TezedgeState, Handshake, HandshakeStep};
use crate::proposals::NewPeerConnectProposal;

impl Acceptor<NewPeerConnectProposal> for TezedgeState {
    fn accept(&mut self, proposal: NewPeerConnectProposal) {
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }

        match &mut self.p2p_state {
            P2pState::Pending { pending_peers }
            | P2pState::Ready { pending_peers } => {
                pending_peers.insert(
                    proposal.peer.clone(),
                    Handshake::Incoming(HandshakeStep::Initiated { at: proposal.at }),
                );
            }
            _ => {
                self.disconnect_peer(proposal.at, proposal.peer);
            }
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
