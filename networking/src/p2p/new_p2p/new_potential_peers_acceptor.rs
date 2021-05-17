use std::mem;
use std::time::Instant;
use std::collections::BTreeMap;

use crypto::nonce::Nonce;
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};
use super::{GetRequests, acceptor::{Acceptor, AcceptorError, Proposal, NewestTimeSeen}};
use super::{ConnectedPeer, Handshake, HandshakeStep, P2pState, PeerId, RequestState, TezedgeState};

pub type NewPotentialPeersAcceptorError = AcceptorError<()>;

#[derive(Debug, Clone)]
pub struct NewPotentialPeersProposal {
    pub at: Instant,
    pub peers: Vec<PeerAddress>,
}

impl Proposal for NewPotentialPeersProposal {
    fn time(&self) -> Instant {
        self.at
    }
}

// TODO: detect and handle timeouts
impl Acceptor<NewPotentialPeersProposal> for TezedgeState {
    type Error = NewPotentialPeersAcceptorError;

    fn accept(&mut self, proposal: HandshakeProposal) -> Result<(), NewPotentialPeersAcceptorError> {
        self.validate_proposal(&proposal)?;

        // Return if maximum number of connections is already reached.
        if self.potential_peers.len() >= self.config.max_potential_peers {
            return Ok(());
        }

        let max_new_peers_len = self.config.max_potential_peers - self.potential_peers.len();

        self.potential_peers.extend(
            proposal.peers.into_iter().take(max_new_peers_len),
        );

        self.react();
        Ok(())
    }

    #[inline]
    fn react(&mut self) {
        HandshakeAcceptor::react(self)
    }
}
