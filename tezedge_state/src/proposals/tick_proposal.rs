use std::time::Instant;
use tla_sm::Proposal;

#[derive(Debug, Clone)]
pub struct TickProposal {
    pub at: Instant,
}

impl Proposal for TickProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
