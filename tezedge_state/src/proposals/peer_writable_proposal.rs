use std::fmt::{self, Debug};
use std::time::Instant;
use tla_sm::Proposal;

use crate::PeerAddress;

// TODO: there should be difference between 2 kinds of proposals. 1 is
// normal one, which is meant to be recorded and if replayed will give
// us exact same state. and 2nd is like this one, which is purely just
// for performance reasons. Since we invoke read on reader which makes a
// syscall to read certain bytes means that those bytes aren't available
// and are consumed on read, hence hard to debug. So these types of proposals
// should simply invoke another proposal like in this case that the chunk
// is ready so that proposal can be recorded and replayed.
pub struct PeerWritableProposal<'a, W> {
    pub at: Instant,
    pub peer: PeerAddress,
    pub stream: &'a mut W,
}

impl<'a, W> Debug for PeerWritableProposal<'a, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerWritableProposal")
            .field("at", &self.at)
            .field("peer", &self.peer)
            .finish()
    }
}

impl<'a, W> Proposal for PeerWritableProposal<'a, W> {
    fn time(&self) -> Instant {
        self.at
    }
}
