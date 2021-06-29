use std::fmt::{self, Debug};
use std::time::Instant;
use tla_sm::Proposal;
use tezos_messages::p2p::encoding::peer::PeerMessage;

use crate::PeerAddress;

pub struct SendPeerMessageProposal {
    pub at: Instant,
    pub peer: PeerAddress,
    pub message: PeerMessage,
}

impl Debug for SendPeerMessageProposal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerSendMessageProposal")
            .field("at", &self.at)
            .field("peer", &self.peer)
            .finish()
    }
}

impl Proposal for SendPeerMessageProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
