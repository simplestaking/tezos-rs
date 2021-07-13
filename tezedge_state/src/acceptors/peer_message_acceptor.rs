use std::net::SocketAddr;

use tezos_messages::p2p::encoding::prelude::PeerMessage;
use tla_sm::Acceptor;

use crate::proposals::{ExtendPotentialPeersProposal, PeerMessageProposal};
use crate::{Effects, PendingRequest, PendingRequestState, RequestState, TezedgeState};

impl<E: Effects> Acceptor<PeerMessageProposal> for TezedgeState<E> {
    fn accept(&mut self, proposal: PeerMessageProposal) {
        if let Err(_err) = self.validate_proposal(&proposal) {
            #[cfg(test)]
            assert_ne!(_err, crate::InvalidProposalError::ProposalOutdated);
            return;
        }

        if let Some(peer) = self.connected_peers.get_mut(&proposal.peer) {
            // handle connected peer messages.
            match proposal.message {
                PeerMessage::Advertise(message) => {
                    self.accept(ExtendPotentialPeersProposal {
                        at: proposal.at,
                        peers: message
                            .id()
                            .iter()
                            .filter_map(|str_ip_port| str_ip_port.parse::<SocketAddr>().ok()),
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
        } else {
            slog::warn!(&self.log, "Blacklisting peer"; "peer_address" => proposal.peer.to_string(), "reason" => "Received PeerMessage from not connected(handshake not done) or non-existant peer");
            self.blacklist_peer(proposal.at, proposal.peer);
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
