use std::io::{self, Read, Write};

use crate::chunking::ReadMessageError;
use crate::proposals::peer_handshake_message::PeerBinaryHandshakeMessage;
use crate::proposals::{
    PeerHandshakeMessageProposal, PeerMessageProposal, PeerReadableProposal, PeerWritableProposal,
};
use crate::{Effects, TezedgeState};
use tla_sm::Acceptor;

impl<'a, E, S> Acceptor<PeerReadableProposal<'a, S>> for TezedgeState<E>
where
    E: Effects,
    S: Read + Write,
{
    fn accept(&mut self, proposal: PeerReadableProposal<S>) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }
        let time = proposal.at;

        if let Some(peer) = self.connected_peers.get_mut(&proposal.peer) {
            match peer.read_message_from(proposal.stream) {
                Ok(message) => {
                    self.accept(PeerMessageProposal {
                        at: proposal.at,
                        peer: proposal.peer,
                        message,
                    });
                    self.accept(proposal);
                }
                Err(ReadMessageError::Pending) => {}
                Err(err) => {
                    slog::warn!(&self.log, "Read failed!"; "description" => "error while trying to read from connected peer stream.", "error" => format!("{:?}", err));
                    self.blacklist_peer(proposal.at, proposal.peer);
                }
            };
        } else {
            if let Some(peer) = self.pending_peers.get_mut(&proposal.peer) {
                if !peer.should_read() {
                    return;
                }
                if let Err(err) = peer.read_message_from(proposal.stream) {
                    match err.kind() {
                        io::ErrorKind::WouldBlock => {}
                        _ => {
                            slog::warn!(&self.log, "Read failed!"; "description" => "error while trying to read from peer stream during handshake.", "error" => format!("{:?}", err));
                            self.blacklist_peer(proposal.at, proposal.peer);
                        }
                    }
                } else if let Some(message) = peer.read_buf.take_if_ready() {
                    self.accept(PeerHandshakeMessageProposal {
                        at: proposal.at,
                        peer: proposal.peer,
                        message: PeerBinaryHandshakeMessage::new(message),
                    });
                    // try writing to peer after succesfully reading a message.
                    return self.accept(PeerWritableProposal::from(proposal));
                }
            } else {
                // we received event for a non existant peer, probably
                // mio's view about connected peers is out of sync.
                slog::warn!(&self.log, "Disconnecting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Received readable proposal for a non-existant peer. MIO out of sync!");
                self.disconnect_peer(proposal.at, proposal.peer);
            }
        }

        self.adjust_p2p_state(time);
        self.periodic_react(time);
    }
}
