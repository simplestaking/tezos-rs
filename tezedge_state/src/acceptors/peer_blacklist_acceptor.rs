use tla_sm::Acceptor;
use crate::{TezedgeState, Effects};
use crate::proposals::PeerBlacklistProposal;

impl<E: Effects> Acceptor<PeerBlacklistProposal> for TezedgeState<E> {
    fn accept(&mut self, proposal: PeerBlacklistProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        slog::warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Requested by the Proposer");
        self.blacklist_peer(proposal.at, proposal.peer);

        self.periodic_react(proposal.at);
    }
}
