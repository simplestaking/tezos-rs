use std::mem;
use std::time::Instant;
use std::collections::BTreeMap;

use crypto::nonce::Nonce;
use tezos_encoding::binary_reader::BinaryReaderError;
use tezos_identity::Identity;
use tezos_messages::p2p::{binary_message::BinaryMessage, encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
}};
use super::{GetRequests, acceptor::{Acceptor, React, Proposal}};
use super::{ConnectedPeer, Handshake, HandshakeStep, P2pState, PeerAddress, RequestState, TezedgeState};

#[derive(Debug, Clone)]
pub struct RawMessage<'a> {
    bytes: &'a [u8],
}

impl<'a> RawMessage<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn as_connection_msg(&self) -> Result<ConnectionMessage, BinaryReaderError> {
        ConnectionMessage::from_bytes(self.bytes)
    }

    pub fn as_metadata_msg(&self) -> Result<MetadataMessage, BinaryReaderError> {
        MetadataMessage::from_bytes(self.bytes)
    }

    pub fn as_ack_msg(&self) -> Result<AckMessage, BinaryReaderError> {
        AckMessage::from_bytes(self.bytes)
    }
}

#[derive(Debug, Clone)]
pub struct RawProposal<'a> {
    pub at: Instant,
    pub peer: PeerAddress,
    pub message: RawMessage<'a>,
}

impl<'a> Proposal for RawProposal<'a> {
    fn time(&self) -> Instant {
        self.at
    }
}

// TODO: detect and handle timeouts
impl<'a> Acceptor<RawProposal<'a>> for TezedgeState {
    fn accept(&mut self, proposal: RawProposal<'a>) {
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }

        if let Some(peer) = self.connected_peers.get(&proposal.peer) {
            // handle connected peer messages.
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
                        *step = Metadata {
                            conn_msg,
                            sent: Some(Idle { at: proposal.at }),
                            received: None,
                        };
                    }
                }
                Some(Outgoing(step @ Metadata { sent: Some(Success { .. }), .. })) => {
                    if let Ok(meta_msg) = proposal.message.as_metadata_msg() {
                        let conn_msg = match step {
                            Metadata { conn_msg, .. } => Some(conn_msg.clone()),
                            _ => None,
                        }.unwrap();
                        *step = Ack {
                            conn_msg,
                            meta_msg,
                            sent: Some(Idle { at: proposal.at }),
                            received: false,
                        };
                    }
                }
                Some(Outgoing(Ack { sent: Some(Success { .. }), .. })) => {
                    match proposal.message.as_ack_msg() {
                        Ok(AckMessage::Ack) => {
                            let result = pending_peers
                                .remove(&proposal.peer).unwrap()
                                .to_result().unwrap();

                            self.set_peer_connected(
                                proposal.at,
                                proposal.peer,
                                result.conn_msg,
                                result.meta_msg,
                            );
                        }
                        Ok(AckMessage::NackV0) => {}
                        Ok(AckMessage::Nack(_)) => {}
                        Err(_) => {}
                    }
                }
                Some(Incoming(step @ Connect { sent: Some(Success { .. }), .. })) => {
                    if let Ok(meta_msg) = proposal.message.as_metadata_msg() {
                        let conn_msg = match step {
                            Connect { received, .. } => received.take(),
                            _ => None,
                        }.unwrap();

                        *step = Metadata {
                            conn_msg,
                            sent: Some(Idle { at: proposal.at }),
                            received: Some(meta_msg),
                        };
                    }
                }
                Some(Incoming(step @ Metadata { sent: Some(Success { .. }), .. })) => {
                    match proposal.message.as_ack_msg() {
                        Ok(AckMessage::Ack) => {
                            let (conn_msg, meta_msg) = match step {
                                Metadata { received, conn_msg, .. } => {
                                    received.take().map(|meta_msg| (conn_msg.clone(), meta_msg))
                                },
                                _ => None,
                            }.unwrap();
                            *step = Ack {
                                sent: Some(Idle { at: proposal.at }),
                                received: true,
                                conn_msg: conn_msg.clone(),
                                meta_msg: meta_msg.clone(),
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
