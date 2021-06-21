use std::fmt::Debug;

use tla_sm::{Proposal, Acceptor};
use crypto::crypto_box::{CryptoKey, PrecomputedKey, PublicKey};
use crypto::nonce::{Nonce, generate_nonces};
use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryWrite};
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, AckMessage, PeerMessage};
use tezos_messages::p2p::encoding::ack::NackMotive;

use crate::{Handshake, HandshakeStep, P2pState, PeerCrypto, PendingRequest, PendingRequestState, RequestState, TezedgeState};
use crate::proposals::{PeerAbstractMessage, ExtendPotentialPeersProposal, PeerProposal};

impl<M> Acceptor<PeerProposal<M>> for TezedgeState
    where M: Debug + PeerAbstractMessage,
{
    fn accept(&mut self, mut proposal: PeerProposal<M>) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        if let Some(peer) = self.connected_peers.get_mut(&proposal.peer) {
            // handle connected peer messages.
            match proposal.message.as_peer_msg(&mut peer.crypto) {
                Ok(message) => {
                    match message{
                        PeerMessage::Advertise(message) => {
                            self.accept(ExtendPotentialPeersProposal {
                                at: proposal.at,
                                peers: message
                                    .id()
                                    .iter()
                                    .filter_map(|str_ip_port| str_ip_port.parse().ok()),
                            });
                        }
                        // messages not handled in state machine for now.
                        message => {
                            self.requests.insert(PendingRequestState {
                                request: PendingRequest::PeerMessageReceived {
                                    peer: proposal.peer,
                                    message: message.into(),
                                },
                                status: RequestState::Idle { at: proposal.at },
                            });
                        }
                    }
                }
                Err(err) => {
                    eprintln!("ERROR while decoding/decrypting peer message {:?}, {:?}", proposal.message, err);
                    self.blacklist_peer(proposal.at, proposal.peer);
                }
            }
        } else {
            // handle handshake messages.
            use Handshake::*;
            use HandshakeStep::*;
            use RequestState::*;

            let (pending_peers, allow_new_peers) = match &mut self.p2p_state {
                P2pState::ReadyMaxed => {
                    self.nack_peer_handshake(proposal.at, proposal.peer, NackMotive::TooManyConnections);
                    return;
                }
                P2pState::PendingFull { pending_peers }
                | P2pState::ReadyFull { pending_peers } => {
                    (pending_peers, false)
                }
                P2pState::Pending { pending_peers }
                | P2pState::Ready { pending_peers } => {
                    (pending_peers, true)
                }
            };

            match pending_peers.get_mut(&proposal.peer) {
                Some(Outgoing(step @ Connect { sent: Some(Success { .. }), .. })) => {
                    if let Ok(conn_msg) = proposal.message.as_connection_msg() {
                        let step = if conn_msg.port != proposal.peer.port() {
                            // TODO: if we accept multiple peers with the same
                            // ip, we will need to make sure to notify mio part
                            // about the new port.
                            let handshake_step = pending_peers.remove(&proposal.peer).unwrap();
                            let mut peer = proposal.peer;
                            peer.set_port(conn_msg.port);
                            pending_peers.insert(peer, handshake_step);
                            match pending_peers.get_mut(&peer).unwrap() {
                                Outgoing(s) => s,
                                _ => unreachable!(),
                            }
                        } else {
                            step
                        };
                        let sent_conn_msg = match step {
                            Connect { sent_conn_msg, .. } => Some(sent_conn_msg.clone()),
                            _ => None
                        }.unwrap();
                        let nonce_pair = generate_nonces(
                            &BinaryChunk::from_content(&sent_conn_msg.as_bytes().unwrap()).unwrap().raw(),
                            proposal.message.take_binary_chunk().raw(),
                            false,
                        ).unwrap();
                        let precomputed_key = PrecomputedKey::precompute(
                            &PublicKey::from_bytes(conn_msg.public_key()).unwrap(),
                            &self.identity.secret_key,
                        );
                        let crypto = PeerCrypto::new(precomputed_key, nonce_pair);
                        *step = Metadata {
                            conn_msg,
                            crypto,
                            sent: Some(Idle { at: proposal.at }),
                            received: None,
                        };
                    } else {
                        self.blacklist_peer(proposal.at, proposal.peer);
                    }
                }
                Some(Outgoing(step @ Metadata { sent: Some(Success { .. }), .. })) => {
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
                Some(Outgoing(Ack { sent: Some(Success { .. }), crypto, .. })) => {
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
                Some(Incoming(Initiated { .. })) => {
                    if let Ok(conn_msg) = proposal.message.as_connection_msg() {
                        // TODO: if we accept multiple peers with the same
                        // ip, we will need to make sure to notify mio part
                        // about the new port.
                        pending_peers.remove(&proposal.peer);
                        let mut peer = proposal.peer;
                        peer.set_port(conn_msg.port);

                        pending_peers.insert(peer, Incoming(Connect {
                            sent: Some(Idle { at: proposal.at }),
                            received: Some(conn_msg),
                            sent_conn_msg: ConnectionMessage::try_new(
                                self.config.port,
                                &self.identity.public_key,
                                &self.identity.proof_of_work_stamp,
                                // TODO: this introduces non-determinism
                                Nonce::random(),
                                self.network_version.clone(),
                            ).unwrap(),
                        }));
                    } else {
                        self.blacklist_peer(proposal.at, proposal.peer);
                    }
                }
                Some(Incoming(step @ Connect { sent: Some(Success { .. }), .. })) => {
                    let (conn_msg, sent_conn_msg) = match step {
                        Connect { sent_conn_msg, received, .. } => {
                            if let None = received {
                                dbg!(&self);
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
                Some(Incoming(step @ Metadata { sent: Some(Success { .. }), .. })) => {
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
                None => {
                    if !allow_new_peers {
                        self.nack_peer_handshake(proposal.at, proposal.peer, NackMotive::TooManyConnections);
                        return;
                    }

                    if let Ok(conn_msg) = proposal.message.as_connection_msg() {
                        pending_peers.insert(
                            proposal.peer.clone(),
                            Incoming(Connect {
                                sent_conn_msg: ConnectionMessage::try_new(
                                    self.config.port,
                                    &self.identity.public_key,
                                    &self.identity.proof_of_work_stamp,
                                    // TODO: this introduces non-determinism
                                    Nonce::random(),
                                    self.network_version.clone(),
                                ).unwrap(),
                                sent: Some(Idle { at: proposal.at }),
                                received: Some(conn_msg),
                            }),
                        );
                    }
                }
                _ => {
                    self.blacklist_peer(proposal.at, proposal.peer);
                }
            }
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
