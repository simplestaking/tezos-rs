use tla_sm::Acceptor;

use crate::{TezedgeState, Effects};
use crate::proposals::SendPeerMessageProposal;

impl<E: Effects> Acceptor<SendPeerMessageProposal> for TezedgeState<E> {
    fn accept(&mut self, proposal: SendPeerMessageProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        if let Some(peer) = self.connected_peers.get_mut(&proposal.peer) {
            // handle connected peer messages.
            peer.enqueue_send_message(proposal.message);
        } else {
            slog::warn!(&self.log, "Disconnecting peer!"; "reason" => "Received request from Proposer to send a message for non-existant peer.");
            self.disconnect_peer(proposal.at, proposal.peer);
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
