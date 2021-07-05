use slog::{crit, debug, info, trace, warn, error, Logger};

use tla_sm::Acceptor;
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, AckMessage, PeerMessage};
use tezos_messages::p2p::encoding::ack::NackMotive;

use crate::{Effects, HandleReceivedMessageError, HandshakeStep, P2pState, RequestState, TezedgeState};
use crate::proposals::{PeerHandshakeMessage, PeerHandshakeMessageProposal};

impl<E, M> Acceptor<PeerHandshakeMessageProposal<M>> for TezedgeState<E>
    where E: Effects,
          M: PeerHandshakeMessage,
{
    fn accept(&mut self, proposal: PeerHandshakeMessageProposal<M>) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        // handle handshake messages.
        use HandshakeStep::*;

        let pending_peers = match &mut self.p2p_state {
            P2pState::ReadyMaxed => {
                self.nack_peer_handshake(proposal.at, proposal.peer, NackMotive::TooManyConnections);
                return self.periodic_react(proposal.at);
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

        let result = match &mut pending_peer.step {
            Initiated { .. }
            | Connect { received: None, .. }
            => {
                let result = pending_peer.handle_received_conn_message(
                    &self.config,
                    &self.identity,
                    &self.shell_compatibility_version,
                    &mut self.effects,
                    proposal.at,
                    proposal.message,
                );

                match result {
                    Ok(pub_key) => {
                        let proposal_peer = proposal.peer;
                        // check if peer with such identity is already connected.
                        let already_connected = pending_peers
                            .iter()
                            .any(|(_, peer)| {
                                peer.step.public_key().map(|existing_pub_key| {
                                    existing_pub_key == pub_key.as_ref().as_ref()
                                        && peer.address != proposal_peer
                                }).unwrap_or(false)
                            }) || {
                                self.connected_peers.iter().any(|peer| {
                                    peer.public_key == pub_key.as_ref().as_ref()
                                })
                            };

                        if already_connected {
                            self.nack_peer_handshake(proposal.at, proposal.peer, NackMotive::AlreadyConnected);
                        }
                        Ok(())
                    }
                    Err(err) => Err(err),
                }

            }
            Metadata { received: None, .. } => {
                pending_peer.handle_received_meta_message(proposal.at, proposal.message)
            }
            Ack { received: false, .. } => {
                let result = pending_peer.handle_received_ack_message(proposal.at, proposal.message);

                match result {
                    Ok(AckMessage::Ack) => {
                        if pending_peer.is_handshake_finished() {
                            let result = self.pending_peers_mut().unwrap()
                                .remove(&proposal.peer).unwrap()
                                .to_handshake_result().unwrap();
                            self.set_peer_connected(
                                proposal.at,
                                proposal.peer,
                                result,
                            );
                        }
                        Ok(())
                    }
                    Ok(AckMessage::NackV0) => {
                        warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Received NackV0");
                        self.blacklist_peer(proposal.at, proposal.peer);
                        Ok(())
                    }
                    Ok(AckMessage::Nack(info)) => {
                        self.extend_potential_peers(
                            info.potential_peers_to_connect()
                                .into_iter()
                                .filter_map(|x| x.parse::<>().ok()),
                        );
                        warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Received Nack", "motive" => info.motive().to_string());
                        self.blacklist_peer(proposal.at, proposal.peer);
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
            _ => {
                warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Unexpected message!");
                debug!(&self.log, ""; "peer_state" => format!("{:?}", &pending_peer));
                self.blacklist_peer(proposal.at, proposal.peer);
                Ok(())
            }
        };

        match result {
            Err(HandleReceivedMessageError::BadPow) => {
                warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Bad Proof of work");
                self.blacklist_peer(proposal.at, proposal.peer);
            }
            Err(HandleReceivedMessageError::BadHandshakeMessage(error)) => {
                warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Unexpected handshake message", "error" => format!("{:?}", error));
                self.blacklist_peer(proposal.at, proposal.peer);
            }
            Err(HandleReceivedMessageError::Nack(motive)) => {
                warn!(&self.log, "Nacking peer handshake"; "peer_address" => proposal.peer.to_string(), "motive" => motive.to_string());
                self.nack_peer_handshake(proposal.at, proposal.peer, motive);
            }
            Err(HandleReceivedMessageError::UnexpectedState) => {
                warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Unexpected state!");
                self.blacklist_peer(proposal.at, proposal.peer);
            }
            Ok(()) => {}
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
