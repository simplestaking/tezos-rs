use std::net::SocketAddr;

use tla_sm::Acceptor;
use tezos_messages::p2p::encoding::prelude::PeerMessage;

use crate::{PendingRequest, PendingRequestState, RequestState, TezedgeState};
use crate::proposals::{ExtendPotentialPeersProposal, PeerMessageProposal};

impl<E> Acceptor<PeerMessageProposal> for TezedgeState<E> {
    fn accept(&mut self, mut proposal: PeerMessageProposal) {
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
            self.blacklist_peer(proposal.at, proposal.peer);
        }

        self.adjust_p2p_state(proposal.at);
        self.periodic_react(proposal.at);
    }
}
