use std::time::Instant;
use tla_sm::Proposal;

use crate::PeerAddress;

#[derive(Debug, Clone)]
pub struct PeerDisconnectProposal {
    pub at: Instant,
    pub peer: PeerAddress,
}

impl Proposal for PeerDisconnectProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
