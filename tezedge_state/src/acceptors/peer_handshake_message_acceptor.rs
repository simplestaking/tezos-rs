use slog::{crit, debug, info, trace, warn, error, Logger};

use tla_sm::{Proposal, Acceptor};
use crypto::crypto_box::{CryptoKey, PrecomputedKey, PublicKey};
use crypto::nonce::generate_nonces;
use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryWrite};
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, AckMessage, PeerMessage};
use tezos_messages::p2p::encoding::ack::NackMotive;

use crate::{Effects, HandleReceivedConnMessageError, Handshake, HandshakeStep, P2pState, PeerCrypto, RequestState, TezedgeState};
use crate::proposals::{PeerHandshakeMessage, ExtendPotentialPeersProposal, PeerHandshakeMessageProposal};

impl<E, M> Acceptor<PeerHandshakeMessageProposal<M>> for TezedgeState<E>
    where E: Effects,
          M: PeerHandshakeMessage,
{
    fn accept(&mut self, mut proposal: PeerHandshakeMessageProposal<M>) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        // handle handshake messages.
        use Handshake::*;
        use HandshakeStep::*;
        use RequestState::*;

        let pending_peers = match &mut self.p2p_state {
            P2pState::ReadyMaxed => {
                self.nack_peer_handshake(proposal.at, proposal.peer, NackMotive::TooManyConnections);
                return;
            }
            P2pState::Pending { pending_peers }
            | P2pState::PendingFull { pending_peers }
            | P2pState::Ready { pending_peers }
            | P2pState::ReadyFull { pending_peers } => pending_peers,
        };

        let pending_peer = match pending_peers.get_mut(&proposal.peer) {
            Some(peer) => peer,
            None => {
                // Receiving any message from a peer that is not in pending peers
                // is impossible. When new peer opens connection with us,
                // we add new pending peer with `Initiated` state. So this
                // can only happen if outside world is out of sync with TezedgeState.
                eprintln!("WARNING: received message proposal from an unknown peer. Should be impossible!");
                self.disconnect_peer(proposal.at, proposal.peer);
                return self.periodic_react(proposal.at);
            }
        };

        match &mut pending_peer.handshake {
            Incoming(Initiated { .. })
            | Outgoing(Connect { sent: Some(Success { .. }), .. }) => {
                let result = pending_peer.handle_received_conn_message(
                    &self.config,
                    &self.identity,
                    &self.shell_compatibility_version,
                    &mut self.effects,
                    proposal.at,
                    proposal.message,
                );
                match result {
                    Err(HandleReceivedConnMessageError::BadPow) => {
                        warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Bad Proof of work");
                        self.blacklist_peer(proposal.at, proposal.peer);
                    }
                    Err(HandleReceivedConnMessageError::BadHandshakeMessage(error)) => {
                        warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Unexpected handshake message", "error" => format!("{:?}", error));
                        self.blacklist_peer(proposal.at, proposal.peer);
                    }
                    Err(HandleReceivedConnMessageError::Nack(motive)) => {
                        warn!(&self.log, "Nacking peer handshake"; "peer_address" => proposal.peer.to_string(), "motive" => motive.to_string());
                        self.nack_peer_handshake(proposal.at, proposal.peer, motive);
                    }
                    Err(HandleReceivedConnMessageError::UnexpectedState) => {
                        warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Unexpected state!");
                        trace!(&self.log, "Trace"; "peer_state" => format!("{:?}", &pending_peer));
                        self.blacklist_peer(proposal.at, proposal.peer);
                    }
                    Ok(pub_key) => {
                        let proposal_peer = proposal.peer;
                        // check if peer with such identity is already connected.
                        let already_connected = self.pending_peers().unwrap()
                            .iter()
                            .any(|(_, peer)| {
                                let existing_pub_key = match peer.handshake.step() {
                                    Connect { received: Some(conn_msg), .. }
                                    | Metadata { conn_msg, .. }
                                    | Ack { conn_msg, .. } => {
                                        conn_msg.public_key()
                                    }
                                    _ => return false,
                                };
                                existing_pub_key == pub_key.as_ref().as_ref()
                                    && peer.address != proposal_peer
                            }) || {
                                self.connected_peers.iter().any(|peer| {
                                    peer.public_key == pub_key.as_ref().as_ref()
                                })
                            };

                        if already_connected {
                            self.nack_peer_handshake(proposal.at, proposal.peer, NackMotive::AlreadyConnected);
                        }
                    }
                }
            }
            Outgoing(step @ Metadata { sent: Some(Success { .. }), .. }) => {
                let crypto = match step {
                    Metadata { crypto, .. } => Some(crypto),
                    _ => None,
                }.unwrap();

                if let Ok(meta_msg) = proposal.message.as_metadata_msg(crypto) {
                    let (conn_msg, crypto) = match step {
                        Metadata { conn_msg, crypto, .. } => {
                            Some((conn_msg.clone(), crypto.clone()))
                        }
                        _ => None,
                    }.unwrap();
                    *step = Ack {
                        conn_msg,
                        meta_msg,
                        crypto,
                        sent: Some(Idle { at: proposal.at }),
                        received: false,
                    };
                } else {
                    self.blacklist_peer(proposal.at, proposal.peer);
                }
            }
            Outgoing(Ack { sent: Some(Success { .. }), crypto, .. }) => {
                match proposal.message.as_ack_msg(crypto) {
                    Ok(AckMessage::Ack) => {
                        let result = pending_peers
                            .remove(&proposal.peer).unwrap()
                            .to_result().unwrap();

                        self.set_peer_connected(
                            proposal.at,
                            proposal.peer,
                            result,
                        );
                    }
                    Ok(AckMessage::NackV0) => self.blacklist_peer(proposal.at, proposal.peer),
                    Ok(AckMessage::Nack(_)) => self.blacklist_peer(proposal.at, proposal.peer),
                    Err(_) => self.blacklist_peer(proposal.at, proposal.peer),
                }
            }
            Incoming(step @ Connect { sent: Some(Success { .. }), .. }) => {
                let (conn_msg, sent_conn_msg) = match step {
                    Connect { sent_conn_msg, received, .. } => {
                        if let None = received {
                            unreachable!();
                        }
                        received.take().map(|x| (x, sent_conn_msg))
                    }
                    _ => None,
                }.unwrap();
                let nonce_pair = generate_nonces(
                    &sent_conn_msg.as_bytes().unwrap(),
                    &conn_msg.as_bytes().unwrap(),
                    false,
                    ).unwrap();
                let precomputed_key = PrecomputedKey::precompute(
                    &PublicKey::from_bytes(conn_msg.public_key()).unwrap(),
                    &self.identity.secret_key,
                    );
                let mut crypto = PeerCrypto::new(precomputed_key, nonce_pair);

                if let Ok(meta_msg) = proposal.message.as_metadata_msg(&mut crypto) {
                    *step = Metadata {
                        conn_msg,
                        crypto,
                        sent: Some(Idle { at: proposal.at }),
                        received: Some(meta_msg),
                    };
                } else {
                    self.blacklist_peer(proposal.at, proposal.peer);
                }
            }
            Incoming(step @ Metadata { sent: Some(Success { .. }), .. }) => {
                let crypto = match step {
                    Metadata { crypto, .. } => Some(crypto),
                    _ => None,
                }.unwrap();

                match proposal.message.as_ack_msg(crypto) {
                    Ok(AckMessage::Ack) => {
                        let (conn_msg, meta_msg, crypto) = match step {
                            Metadata { conn_msg, received, crypto, .. } => {
                                received.take().map(|meta_msg| {
                                    (conn_msg.clone(), meta_msg, crypto.clone())
                                })
                            }
                            _ => None,
                        }.unwrap();
                        *step = Ack {
                            conn_msg,
                            meta_msg,
                            crypto,
                            sent: Some(Idle { at: proposal.at }),
                            received: true,
                        }
                    }
                    Ok(AckMessage::NackV0) => self.blacklist_peer(proposal.at, proposal.peer),
                    Ok(AckMessage::Nack(_)) => self.blacklist_peer(proposal.at, proposal.peer),
                    Err(_) => self.blacklist_peer(proposal.at, proposal.peer),
                }
            }
            _ => {
                self.blacklist_peer(proposal.at, proposal.peer);
            }
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
