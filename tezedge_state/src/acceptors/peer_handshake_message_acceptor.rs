use std::fmt::Debug;

use crypto::proof_of_work::{PowError, ProofOfWork, check_proof_of_work};
use tla_sm::{Proposal, Acceptor};
use crypto::crypto_box::{CryptoKey, PrecomputedKey, PublicKey};
use crypto::nonce::{Nonce, generate_nonces};
use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryWrite};
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, AckMessage, PeerMessage};
use tezos_messages::p2p::encoding::ack::NackMotive;

use crate::{Handshake, HandshakeStep, P2pState, PeerCrypto, RequestState, TezedgeState, ShellCompatibilityVersion};
use crate::proposals::{PeerHandshakeMessage, ExtendPotentialPeersProposal, PeerHandshakeMessageProposal};

impl<M> Acceptor<PeerHandshakeMessageProposal<M>> for TezedgeState
    where M: Debug + PeerHandshakeMessage,
{
    fn accept(&mut self, mut proposal: PeerHandshakeMessageProposal<M>) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        let pow_target = self.config.pow_target;
        let shell_compatibility_version = &self.shell_compatibility_version;

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

        let pending_peer = pending_peers.get_mut(&proposal.peer);

        match pending_peer.map(|x| &mut x.handshake) {
            Some(Outgoing(step @ Connect { sent: Some(Success { .. }), .. })) => {
                let pow_check_result = if proposal.message.binary_chunk().raw().len() < 60 {
                    Err(PowError::CheckFailed)
                } else {
                    Ok(&proposal.message.binary_chunk().raw()[4..60])
                }.map(|pow_bytes| check_proof_of_work(pow_bytes, pow_target));

                if let Err(e) = pow_check_result {
                    // TODO: check maybe this message is nack.
                    eprintln!("blacklisting peer as identity check failed");
                    self.blacklist_peer(proposal.at, proposal.peer);
                } else if let Ok(conn_msg) = proposal.message.as_connection_msg() {
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
            Some(Incoming(step @ Initiated { .. })) => {
                let pow_check_result = if proposal.message.binary_chunk().raw().len() < 60 {
                    Err(PowError::CheckFailed)
                } else {
                    Ok(&proposal.message.binary_chunk().raw()[4..60])
                }.map(|pow_bytes| check_proof_of_work(pow_bytes, pow_target));

                if let Err(e) = pow_check_result {
                    // TODO: check maybe this message is nack.
                    eprintln!("blacklisting peer as identity check failed");
                    self.blacklist_peer(proposal.at, proposal.peer);
                } else if let Ok(conn_msg) = proposal.message.as_connection_msg() {
                    let chosen_version_result = shell_compatibility_version
                        .choose_compatible_version(conn_msg.version());
                    match chosen_version_result{
                        Ok(compatible_network_version) => {
                            *step = Connect {
                                sent: Some(Idle { at: proposal.at }),
                                received: Some(conn_msg),
                                sent_conn_msg: ConnectionMessage::try_new(
                                    self.config.port,
                                    &self.identity.public_key,
                                    &self.identity.proof_of_work_stamp,
                                    // TODO: this introduces non-determinism
                                    Nonce::random(),
                                    compatible_network_version,
                                ).unwrap(),
                            };
                        }
                        Err(motive) => {
                            self.nack_peer_handshake(proposal.at, proposal.peer, motive);
                        }
                    }
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
                // Receiving any message from a peer that is not in pending peers
                // is impossible. When new peer opens connection with us,
                // we add new pending peer with `Initiated` state. So this
                // can only happen if outside world is out of sync with TezedgeState.
                eprintln!("WARNING: received message proposal from an unknown peer. Should be impossible!");
            }
            _ => {
                self.blacklist_peer(proposal.at, proposal.peer);
            }
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
