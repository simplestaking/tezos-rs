use std::time::Instant;

use crate::PeerAddress;
use tezos_messages::p2p::encoding::peer::PeerMessage;
use tla_sm::Proposal;

#[derive(Debug, Clone)]
pub struct PeerMessageProposal {
    pub at: Instant,
    pub peer: PeerAddress,
    pub message: PeerMessage,
}

impl Proposal for PeerMessageProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
