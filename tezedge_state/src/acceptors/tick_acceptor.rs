use tla_sm::{Proposal, Acceptor};
use crate::TezedgeState;
use crate::proposals::TickProposal;

impl Acceptor<TickProposal> for TezedgeState {
    fn accept(&mut self, proposal: TickProposal) {
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }

        self.periodic_react(proposal.at);
    }
}
