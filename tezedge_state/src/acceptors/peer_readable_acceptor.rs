use std::io::{self, Read};

use tla_sm::{Proposal, Acceptor};
use crypto::crypto_box::{CryptoKey, PrecomputedKey, PublicKey};
use crypto::nonce::{Nonce, generate_nonces};
use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryWrite};
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, AckMessage, PeerMessage};
use tezos_messages::p2p::encoding::ack::NackMotive;

use crate::proposals::peer_handshake_message::PeerBinaryHandshakeMessage;
use crate::{Handshake, HandshakeStep, P2pState, PeerCrypto, PendingRequest, PendingRequestState, RequestState, TezedgeState};
use crate::proposals::{PeerReadableProposal, PeerMessageProposal, PeerHandshakeMessageProposal};
use crate::chunking::ReadMessageError;

impl<'a, R> Acceptor<PeerReadableProposal<'a, R>> for TezedgeState
    where R: Read,
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
            }
        }

        self.adjust_p2p_state(time);
        self.periodic_react(time);
    }
}
