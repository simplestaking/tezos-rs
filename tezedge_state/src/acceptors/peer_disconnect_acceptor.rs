use tla_sm::Acceptor;
use crate::TezedgeState;
use crate::proposals::PeerDisconnectProposal;

impl Acceptor<PeerDisconnectProposal> for TezedgeState {
    fn accept(&mut self, proposal: PeerDisconnectProposal) {
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }

        self.blacklist_peer(proposal.at, proposal.peer);

        self.periodic_react(proposal.at);
    }
}
