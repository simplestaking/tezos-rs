use std::time::Instant;
use tla_sm::Proposal;
use crate::PeerAddress;

#[derive(Debug, Clone)]
pub struct PeerProposal<M> {
    pub at: Instant,
    pub peer: PeerAddress,
    pub message: M,
}

impl<M> Proposal for PeerProposal<M> {
    fn time(&self) -> Instant {
        self.at
    }
}
