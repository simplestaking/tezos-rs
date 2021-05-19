use std::time::Instant;

use super::acceptor::{Proposal, Acceptor};
use super::{TezedgeState, PeerAddress, React};

#[derive(Debug, Clone)]
pub struct ExtendPotentialPeersProposal<P> {
    pub at: Instant,
    pub peers: P,
}

impl<I> Proposal for ExtendPotentialPeersProposal<I> {
    fn time(&self) -> Instant {
        self.at
    }
}

// TODO: detect and handle timeouts
impl<P> Acceptor<ExtendPotentialPeersProposal<P>> for TezedgeState
    where P: IntoIterator<Item = PeerAddress>,
{
    fn accept(&mut self, proposal: ExtendPotentialPeersProposal<P>) {
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }
        self.extend_potential_peers(proposal.peers);

        self.react(proposal.at);
    }
}
