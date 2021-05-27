use std::time::Instant;
use tla_sm::Proposal;

use crate::PeerAddress;

#[derive(Debug, Clone)]
pub enum HandshakeMsg {
    SendConnectPending,
    SendConnectSuccess,
    SendConnectError,

    SendMetaPending,
    SendMetaSuccess,
    SendMetaError,

    SendAckPending,
    SendAckSuccess,
    SendAckError,
}

#[derive(Debug, Clone)]
pub struct HandshakeProposal {
    pub at: Instant,
    pub peer: PeerAddress,
    pub message: HandshakeMsg,
}

impl Proposal for HandshakeProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
