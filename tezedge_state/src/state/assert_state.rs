use crate::TezedgeState;

impl<E> TezedgeState<E> {
    pub fn assert_state(&self) {
        assert!(self.potential_peers.len() <= self.config.max_potential_peers);
        assert!(self.pending_peers.len() <= self.config.max_pending_peers);
        assert!(self.connected_peers.len() <= self.config.max_connected_peers);
    }
}
