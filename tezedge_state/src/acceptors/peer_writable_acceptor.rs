use std::io::Write;

use tezos_messages::p2p::encoding::ack::{AckMessage, NackMotive};
use tla_sm::Acceptor;
use crate::{TezedgeState, P2pState, HandshakeMessageType};
use crate::proposals::PeerWritableProposal;
use crate::chunking::WriteMessageError;

impl<'a, E, W> Acceptor<PeerWritableProposal<'a, W>> for TezedgeState<E>
    where W: Write,
{
    fn accept(&mut self, proposal: PeerWritableProposal<W>) {
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
                        eprintln!("error while trying to write to peer's stream: {:?}", err);
                        self.blacklist_peer(proposal.at, proposal.peer);
                        break;
                    }
                };
            }
        } else {
            let meta_msg = self.meta_msg();
            let pending_peers = match &mut self.p2p_state {
                P2pState::ReadyMaxed => {
                    self.nack_peer_handshake(proposal.at, proposal.peer, NackMotive::TooManyConnections);
                    return self.periodic_react(time);
                }
                P2pState::Pending { pending_peers }
                | P2pState::PendingFull { pending_peers }
                | P2pState::Ready { pending_peers }
                | P2pState::ReadyFull { pending_peers } => pending_peers,
            };
            let peer = pending_peers.get_mut(&proposal.peer);
            if let Some(peer) = peer {
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
                                        let peer = self.pending_peers_mut().unwrap()
                                            .remove(&proposal.peer)
                                            .unwrap();
                                        let result = peer.to_handshake_result().unwrap();
                                        self.set_peer_connected(proposal.at, proposal.peer, result);
                                        return self.accept(proposal);
                                    }
                                }
                            }
                        }
                        Err(WriteMessageError::Empty) => {
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
                                        peer.enqueue_send_ack_msg(proposal.at, AckMessage::Ack)
                                    } else {
                                        Ok(enqueued)
                                    }
                                });
                            match result {
                                Ok(true) => {}
                                Ok(false) => break,
                                Err(err) =>  {
                                    eprintln!("failed to enqueue sending connection message for peer({}): {:?}", proposal.peer, err);
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
                self.disconnect_peer(proposal.at, proposal.peer);
            }
        }

        self.adjust_p2p_state(time);
        self.periodic_react(time);
    }
}
