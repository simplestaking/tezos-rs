use std::io::{self, Read};

use tla_sm::Acceptor;
use crate::Effects;
use crate::proposals::peer_handshake_message::PeerBinaryHandshakeMessage;
use crate::TezedgeState;
use crate::proposals::{PeerReadableProposal, PeerMessageProposal, PeerHandshakeMessageProposal};
use crate::chunking::ReadMessageError;

impl<'a, E, R> Acceptor<PeerReadableProposal<'a, R>> for TezedgeState<E>
    where E: Effects,
          R: Read,
{
    fn accept(&mut self, proposal: PeerReadableProposal<R>) {
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
                    eprintln!("error while trying to read/decrypt/decode from peer stream: {:?}", err);
                    self.blacklist_peer(proposal.at, proposal.peer);
                }
            };
        } else {
            let peer = self.pending_peers_mut().and_then(|peers| peers.get_mut(&proposal.peer));
            if let Some(peer) = peer {
                if let Err(err) = peer.read_message_from(proposal.stream) {
                    match err.kind() {
                        io::ErrorKind::WouldBlock => {}
                        _ => {
                            eprintln!("error while trying to read from peer stream: {:?}", err);
                            self.blacklist_peer(proposal.at, proposal.peer);
                        }
                    }
                } else if let Some(message) = peer.read_buf.take_if_ready() {
                    self.accept(PeerHandshakeMessageProposal {
                        at: proposal.at,
                        peer: proposal.peer,
                        message: PeerBinaryHandshakeMessage::new(message),
                    });
                    self.accept(proposal);
                }
            } else {
                // we received event for a non existant peer, probably
                // mio's view about connected peers is out of sync.
                self.disconnect_peer(proposal.at, proposal.peer);
            }
        }

        self.adjust_p2p_state(time);
        self.periodic_react(time);
    }
}
