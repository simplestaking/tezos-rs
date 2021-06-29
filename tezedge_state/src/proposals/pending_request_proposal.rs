use std::time::Instant;
use tla_sm::Proposal;

#[derive(Debug, Clone)]
pub enum PendingRequestMsg {
    StartListeningForNewPeersPending,
    StartListeningForNewPeersSuccess,

    StopListeningForNewPeersPending,
    StopListeningForNewPeersSuccess,

    SendPeerAckPending,
    SendPeerAckSuccess,

    ConnectPeerPending,
    ConnectPeerSuccess,
    ConnectPeerError,

    DisconnectPeerPending,
    DisconnectPeerSuccess,

    BlacklistPeerPending,
    BlacklistPeerSuccess,

    PeerMessageReceivedNotified,

    /// Handshake which was successful was notified.
    HandshakeSuccessfulNotified,
}

#[derive(Debug, Clone)]
pub struct PendingRequestProposal {
    pub at: Instant,
    pub req_id: usize,
    pub message: PendingRequestMsg,
}

impl Proposal for PendingRequestProposal {
    fn time(&self) -> Instant {
        self.at
    }
}
