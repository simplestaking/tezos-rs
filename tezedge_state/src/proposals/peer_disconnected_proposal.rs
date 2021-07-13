use std::time::Instant;
use tla_sm::Proposal;

use crate::PeerAddress;

/// Peer has disconnected.
#[derive(Debug, Clone)]
pub struct PeerDisconnectedProposal {
    pub at: Instant,
    pub peer: PeerAddress,
}

impl Proposal for PeerDisconnectedProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
