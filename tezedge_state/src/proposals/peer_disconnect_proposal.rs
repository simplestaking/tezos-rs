use std::time::Instant;
use tla_sm::Proposal;

use crate::PeerAddress;

/// Disconnect the peer.
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
