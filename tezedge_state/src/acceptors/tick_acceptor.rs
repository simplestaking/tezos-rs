use tla_sm::Acceptor;
use crate::{TezedgeState, Effects};
use crate::proposals::TickProposal;

impl<E: Effects> Acceptor<TickProposal> for TezedgeState<E> {
    fn accept(&mut self, proposal: TickProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        self.periodic_react(proposal.at);
    }
}
