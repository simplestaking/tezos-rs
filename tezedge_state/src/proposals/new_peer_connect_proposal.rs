use std::time::Instant;
use tla_sm::Proposal;

use crate::PeerAddress;

#[derive(Debug, Clone)]
pub struct NewPeerConnectProposal {
    pub at: Instant,
    pub peer: PeerAddress,
}

impl Proposal for NewPeerConnectProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
