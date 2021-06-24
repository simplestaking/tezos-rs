use std::fmt::Debug;
use std::time::Instant;

use tla_sm::GetRequests;
use tla_sm::{Acceptor, Proposal};

use crate::TezedgeRequest;
use crate::{PeerAddress, PeerCrypto, TezedgeState};

#[derive(Debug, Clone)]
pub struct TezedgeStateWrapper(TezedgeState);

impl TezedgeStateWrapper {
    #[inline]
    pub fn newest_time_seen(&self) -> Instant {
        self.0.newest_time_seen()
    }

    #[inline]
    pub fn get_peer_crypto(&mut self, peer_address: &PeerAddress) -> Option<&mut PeerCrypto> {
        self.0.get_peer_crypto(peer_address)
    }

    #[inline]
    pub fn is_peer_connected(&mut self, peer: &PeerAddress) -> bool {
        self.0.is_peer_connected(peer)
    }
}

impl<P> Acceptor<P> for TezedgeStateWrapper
    where P: Proposal + Debug,
          TezedgeState: Acceptor<P>,
{
    #[inline]
    fn accept(&mut self, proposal: P) {
        // self.0.accept(dbg!(proposal))
        self.0.accept(proposal)
    }
}

impl GetRequests for TezedgeStateWrapper {
    type Request = TezedgeRequest;

    #[inline]
    fn get_requests(&self, buf: &mut Vec<Self::Request>) -> usize {
        self.0.get_requests(buf)
    }
}

impl From<TezedgeState> for TezedgeStateWrapper {
    #[inline]
    fn from(state: TezedgeState) -> Self {
        TezedgeStateWrapper(state)
    }
}
