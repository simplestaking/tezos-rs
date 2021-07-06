use std::io::{Read, Write};

use tezos_messages::p2p::encoding::ack::{AckMessage, NackMotive};
use tla_sm::Acceptor;
use crate::{TezedgeState, P2pState, Effects, HandshakeMessageType};
use crate::proposals::{PeerReadableProposal, PeerWritableProposal};
use crate::chunking::WriteMessageError;

impl<'a, E, S> Acceptor<PeerWritableProposal<'a, S>> for TezedgeState<E>
    where E: Effects,
          S: Read + Write,
{
    fn accept(&mut self, proposal: PeerWritableProposal<S>) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }
        let time = proposal.at;

        if let Some(peer) = self.connected_peers.get_mut(&proposal.peer) {
            loop {
                match peer.write_to(proposal.stream) {
                    Ok(()) => {}
                    Err(WriteMessageError::Empty)
                    | Err(WriteMessageError::Pending) => break,
                    Err(err) => {
                        slog::error!(&self.log, "Write failed!"; "description" => "error while trying to write to connected peer stream.", "error" => format!("{:?}", err));
                        self.blacklist_peer(proposal.at, proposal.peer);
                        break;
                    }
                };
            }
        } else {
            let meta_msg = self.meta_msg();
            if let Some(peer) = self.pending_peers.get_mut(&proposal.peer) {
                loop {
                    match peer.write_to(proposal.stream) {
                        Ok(msg_type) => {
                            match msg_type {
                                HandshakeMessageType::Connection => {
                                    peer.send_conn_msg_successful(proposal.at, &self.identity);
                                }
                                HandshakeMessageType::Metadata => {
                                    peer.send_meta_msg_successful(proposal.at);
                                }
                                HandshakeMessageType::Ack => {
                                    peer.send_ack_msg_successful(proposal.at);
                                    if peer.is_handshake_finished() {
                                        let peer = self.pending_peers
                                            .remove(&proposal.peer)
                                            .unwrap();
                                        if let Some(result) = peer.to_handshake_result() {
                                            self.set_peer_connected(proposal.at, proposal.peer, result);
                                            self.adjust_p2p_state(time);
                                            // try to write and read from peer
                                            // after successful handshake.
                                            self.accept(PeerReadableProposal {
                                                at: proposal.at,
                                                peer: proposal.peer,
                                                stream: proposal.stream,
                                            });
                                            return self.accept(proposal);
                                        } else {
                                            slog::warn!(&self.log, "Blacklisting peer!"; "reason" => "Handshake failed!");
                                            self.blacklist_peer(proposal.at, proposal.peer);
                                            self.adjust_p2p_state(time);
                                            return self.periodic_react(time);
                                        }
                                    }
                                }
                            }
                            // try reading from peer after succesfully sending a message.
                            return self.accept(PeerReadableProposal::from(proposal));
                        }
                        Err(WriteMessageError::Empty) => {
                            let p2p_state = self.p2p_state;
                            let effects = &mut self.effects;
                            let potential_peers = &self.potential_peers;

                            let result = peer.enqueue_send_conn_msg(proposal.at)
                                .and_then(|enqueued| {
                                    if !enqueued {
                                        peer.enqueue_send_meta_msg(proposal.at, meta_msg.clone())
                                    } else {
                                        Ok(enqueued)
                                    }
                                })
                                .and_then(|enqueued| {
                                    if !enqueued {
                                        match p2p_state {
                                            P2pState::Pending
                                            | P2pState::PendingFull
                                            | P2pState::Ready
                                            | P2pState::ReadyFull
                                            => {}
                                            P2pState::ReadyMaxed => {
                                                peer.nack_peer(NackMotive::TooManyConnections);
                                            }
                                        }

                                        peer.enqueue_send_ack_msg(proposal.at, || {
                                            effects.choose_potential_peers_for_nack(potential_peers)
                                                .into_iter()
                                                .map(|x| x.to_string())
                                                .collect()
                                        })
                                    } else {
                                        Ok(enqueued)
                                    }
                                });
                            match result {
                                Ok(true) => {}
                                Ok(false) => break,
                                Err(err) =>  {
                                    slog::error!(&self.log, "Failed to enqueue send handshake message!"; "error" => format!("{:?}", err));
                                    #[cfg(test)]
                                    unreachable!("enqueueing handshake messages should always succeed");
                                    break;
                                }
                            }
                        }
                        Err(WriteMessageError::Pending) => {}
                        Err(err) => {
                            eprintln!("error sending handshake message to peer({}): {:?}", proposal.peer, err);
                            self.blacklist_peer(proposal.at, proposal.peer);
                            break;
                        }
                    };
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
