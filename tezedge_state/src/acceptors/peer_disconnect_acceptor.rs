use tla_sm::Acceptor;
use crate::TezedgeState;
use crate::proposals::PeerDisconnectProposal;

impl<E> Acceptor<PeerDisconnectProposal> for TezedgeState<E> {
    fn accept(&mut self, proposal: PeerDisconnectProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        self.disconnect_peer(proposal.at, proposal.peer);

        self.periodic_react(proposal.at);
    }
}
