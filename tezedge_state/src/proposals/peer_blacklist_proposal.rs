use std::time::Instant;
use tla_sm::Proposal;

use crate::PeerAddress;

#[derive(Debug, Clone)]
pub struct PeerBlacklistProposal {
    pub at: Instant,
    pub peer: PeerAddress,
}

impl Proposal for PeerBlacklistProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
