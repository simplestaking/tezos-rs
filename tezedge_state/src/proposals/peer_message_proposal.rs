use std::time::Instant;

use tla_sm::Proposal;
use tezos_messages::p2p::encoding::peer::PeerMessage;
use crate::PeerAddress;

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
