use tla_sm::Acceptor;
use crate::TezedgeState;
use crate::proposals::PeerBlacklistProposal;

impl Acceptor<PeerBlacklistProposal> for TezedgeState {
    fn accept(&mut self, proposal: PeerBlacklistProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        self.blacklist_peer(proposal.at, proposal.peer);

        self.periodic_react(proposal.at);
    }
}
