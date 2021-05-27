use tla_sm::Acceptor;
use crate::{TezedgeState, PeerAddress};
use crate::proposals::ExtendPotentialPeersProposal;

impl<P> Acceptor<ExtendPotentialPeersProposal<P>> for TezedgeState
    where P: IntoIterator<Item = PeerAddress>,
{
    fn accept(&mut self, proposal: ExtendPotentialPeersProposal<P>) {
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }
        self.extend_potential_peers(proposal.peers);
        self.periodic_react(proposal.at);
    }
}
