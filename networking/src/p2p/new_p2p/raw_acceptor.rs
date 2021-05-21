use std::mem;
use std::fmt::Debug;
use std::time::Instant;
use std::collections::BTreeMap;

use crypto::{crypto_box::{CryptoKey, PrecomputedKey, PublicKey}, nonce::{Nonce, generate_nonces}};
use tezos_encoding::binary_reader::BinaryReaderError;
use tezos_identity::Identity;
use tezos_messages::p2p::{binary_message::{BinaryChunk, BinaryMessage}, encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
}};

use super::{GetRequests, acceptor::{Acceptor, React, Proposal}};
use super::crypto::Crypto;
use super::{ConnectedPeer, Handshake, HandshakeStep, P2pState, PeerAddress, RequestState, TezedgeState};

#[derive(Debug)]
pub enum RawMessageError {
    InvalidMessage,
}

pub trait RawMessage: Debug {
    fn take_binary_chunk(self) -> BinaryChunk;
    fn binary_chunk(&self) -> &BinaryChunk;
    fn as_connection_msg(&mut self) -> Result<ConnectionMessage, RawMessageError>;
    fn as_metadata_msg(&mut self, crypto: &mut Crypto) -> Result<MetadataMessage, RawMessageError>;
    fn as_ack_msg(&mut self, crypto: &mut Crypto) -> Result<AckMessage, RawMessageError>;
}

#[derive(Debug, Clone)]
pub struct RawProposal<M> {
    pub at: Instant,
    pub peer: PeerAddress,
    pub message: M,
}

impl<M> Proposal for RawProposal<M> {
    fn time(&self) -> Instant {
        self.at
    }
}

// TODO: detect and handle timeouts
impl<M> Acceptor<RawProposal<M>> for TezedgeState
    where M: RawMessage,
{
    fn accept(&mut self, mut proposal: RawProposal<M>) {
        dbg!(&proposal);
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }

        if let Some(peer) = self.connected_peers.get(&proposal.peer) {
            // handle connected peer messages.
            dbg!("message from connected peer");
        } else {
            // handle handshake messages.
            use Handshake::*;
            use HandshakeStep::*;
            use RequestState::*;

            let (pending_peers, allow_new_peers) = match &mut self.p2p_state {
                P2pState::ReadyMaxed => {
                    // TODO: if message is connection message
                    // nack and send potential peers.
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
                        let crypto = Crypto::new(precomputed_key, nonce_pair);
                        *step = Metadata {
                            conn_msg,
                            crypto,
                            sent: Some(Idle { at: proposal.at }),
                            received: None,
                        };
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
                        Ok(AckMessage::NackV0) => {}
                        Ok(AckMessage::Nack(_)) => {}
                        Err(_) => {}
                    }
                }
                Some(Incoming(step @ Connect { sent: Some(Success { .. }), .. })) => {
                    let crypto = match step {
                        Ack { crypto, .. } => Some(crypto),
                        _ => None,
                    }.unwrap();

                    if let Ok(meta_msg) = proposal.message.as_metadata_msg(crypto) {
                        let (conn_msg, sent_conn_msg) = match step {
                            Connect { sent_conn_msg, received, .. } => {
                                received.take().map(|x| (x, sent_conn_msg))
                            }
                            _ => None,
                        }.unwrap();
                        let nonce_pair = generate_nonces(
                            &sent_conn_msg.as_bytes().unwrap(),
                            proposal.message.take_binary_chunk().raw(),
                            false,
                        ).unwrap();
                        let precomputed_key = PrecomputedKey::precompute(
                            &PublicKey::from_bytes(conn_msg.public_key()).unwrap(),
                            &self.identity.secret_key,
                        );
                        let crypto = Crypto::new(precomputed_key, nonce_pair);

                        *step = Metadata {
                            conn_msg,
                            crypto,
                            sent: Some(Idle { at: proposal.at }),
                            received: Some(meta_msg),
                        };
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
                        Ok(AckMessage::NackV0) => {}
                        Ok(AckMessage::Nack(_)) => {}
                        Err(_) => {}
                    }
                }
                None => {
                    if !allow_new_peers {
                        // TODO: if message is connection message
                        // nack and send potential peers.
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
                _ => {}
            }
        }

        self.react(proposal.at);
    }
}
