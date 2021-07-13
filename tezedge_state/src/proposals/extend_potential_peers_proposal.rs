use std::time::Instant;
use tla_sm::Proposal;

#[derive(Debug, Clone)]
pub struct ExtendPotentialPeersProposal<P> {
    pub at: Instant,
    pub peers: P,
}

impl<I> Proposal for ExtendPotentialPeersProposal<I> {
    fn time(&self) -> Instant {
        self.at
    }
}
