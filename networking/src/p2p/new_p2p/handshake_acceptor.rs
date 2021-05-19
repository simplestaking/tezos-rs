use std::mem;
use std::time::Instant;
use std::collections::BTreeMap;

use crypto::nonce::Nonce;
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};
use super::{GetRequests, acceptor::{Acceptor, React, Proposal, NewestTimeSeen}};
use super::{ConnectedPeer, Handshake, HandshakeStep, P2pState, PeerAddress, RequestState, TezedgeState};

#[derive(Debug, Clone)]
pub enum HandshakeMsg {
    SendConnectPending,
    SendConnectSuccess,
    SendConnectError,

    SendMetaPending,
    SendMetaSuccess,
    SendMetaError,

    SendAckPending,
    SendAckSuccess,
    SendAckError,
}

#[derive(Debug, Clone)]
pub struct HandshakeProposal {
    pub at: Instant,
    pub peer: PeerAddress,
    pub message: HandshakeMsg,
}

impl Proposal for HandshakeProposal {
    fn time(&self) -> Instant {
        self.at
    }
}

fn connect_to_peer(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
    conn_msg: ConnectionMessage,
    meta_msg: MetadataMessage,
) {
    state.connected_peers.insert(peer_address, ConnectedPeer {
        connected_since: at,
        port: conn_msg.port,
        version: conn_msg.version,
        public_key: conn_msg.public_key,
        proof_of_work_stamp: conn_msg.proof_of_work_stamp,
        message_nonce: conn_msg.message_nonce,
        disable_mempool: meta_msg.disable_mempool(),
        private_node: meta_msg.private_node(),
    });
}

fn handle_send_connect_pending(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address) {
                Some(Outgoing(Connect { sent: Some(status @ Idle { .. }), .. }))
                | Some(Incoming(Connect { sent: Some(status @ Idle { .. }), .. })) => {
                    *status = Pending { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                    // maybe remove from pending and ban here?
                }
            }
        }
    }
}

fn handle_send_meta_pending(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address) {
                Some(Outgoing(Metadata { sent: Some(status @ Idle { .. }), .. }))
                | Some(Incoming(Metadata { sent: Some(status @ Idle { .. }), .. })) => {
                    *status = Pending { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                    // maybe remove from pending and ban here?
                }
            }
        }
    }
}

fn handle_send_ack_pending(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address) {
                Some(Outgoing(Ack { sent: Some(status @ Idle { .. }), .. }))
                | Some(Incoming(Ack { sent: Some(status @ Idle { .. }), .. })) => {
                    *status = Pending { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                    // maybe remove from pending and ban here?
                }
            }
        }
    }
}

fn handle_send_connect_success(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address) {
                Some(Outgoing(Connect { sent: Some(status @ Pending { .. }), .. }))
                | Some(Incoming(Connect { sent: Some(status @ Pending { .. }), .. })) => {
                    *status = Success { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                    // maybe remove from pending and ban here?
                }
            }
        }
    }
}

fn handle_send_meta_success(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.get_mut(&peer_address) {
                Some(Outgoing(Metadata { sent: Some(status @ Pending { .. }), .. }))
                | Some(Incoming(Metadata { sent: Some(status @ Pending { .. }), .. })) => {
                    *status = Success { at };
                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                    // maybe remove from pending and ban here?
                }
            }
        }
    }
}

fn handle_send_ack_success(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    use Handshake::*;
    use HandshakeStep::*;
    use RequestState::*;

    match &mut state.p2p_state {
        P2pState::ReadyMaxed => {
        }
        P2pState::Pending { pending_peers }
        | P2pState::PendingFull { pending_peers }
        | P2pState::Ready { pending_peers }
        | P2pState::ReadyFull { pending_peers } => {
            match pending_peers.remove(&peer_address) {
                Some(Outgoing(mut step @ Ack { sent: Some(Pending { .. }), .. })) => {
                    step.set_sent(Success { at });
                    pending_peers.insert(peer_address, Outgoing(step));
                }
                Some(Incoming(Ack { sent: Some(Pending { .. }), conn_msg, meta_msg, .. })) => {
                    connect_to_peer(state, at, peer_address, conn_msg, meta_msg);

                }
                Some(Outgoing(_)) | Some(Incoming(_)) | None => {
                    // maybe remove from pending and ban here?
                }
            }
        }
    }
}

fn handle_send_connect_error(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    // TODO: retry
}

fn handle_send_meta_error(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    // TODO: retry
}

fn handle_send_ack_error(
    state: &mut TezedgeState,
    at: Instant,
    peer_address: PeerAddress,
) {
    // TODO: retry
}

// TODO: detect and handle timeouts
impl Acceptor<HandshakeProposal> for TezedgeState {
    fn accept(&mut self, proposal: HandshakeProposal) {
        dbg!(&proposal);
        if let Err(_) = self.validate_proposal(&proposal) {
            return;
        }

        // Return if maximum number of connections is already reached.
        if let P2pState::ReadyMaxed = self.p2p_state {
            return;
        }

        match proposal.message {
            HandshakeMsg::SendConnectPending => {
                handle_send_connect_pending(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendMetaPending => {
                handle_send_meta_pending(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendAckPending => {
                handle_send_ack_pending(self, proposal.at, proposal.peer)
            }

            HandshakeMsg::SendConnectSuccess => {
                handle_send_connect_success(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendMetaSuccess => {
                handle_send_meta_success(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendAckSuccess => {
                handle_send_ack_success(self, proposal.at, proposal.peer)
            }

            HandshakeMsg::SendConnectError => {
                handle_send_connect_error(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendMetaError => {
                handle_send_meta_error(self, proposal.at, proposal.peer)
            }
            HandshakeMsg::SendAckError => {
                handle_send_ack_error(self, proposal.at, proposal.peer)
            }
        }

        self.react(proposal.at);
    }
}

// #[cfg(test)]
// mod tests {
//     use std::time::{Instant, Duration};
//     use std::fmt::{self, Display};
//     use std::collections::VecDeque;
//     use hex::FromHex;
//     use quickcheck::{Arbitrary, Gen};
//     use itertools::Itertools;

//     use crypto::{crypto_box::{CryptoKey, PublicKey, SecretKey}, hash::{CryptoboxPublicKeyHash, HashTrait}, nonce::Nonce, proof_of_work::ProofOfWork};
//     use tezos_messages::p2p::encoding::{ack::{AckMessage, NackInfo, NackMotive}, connection::ConnectionMessage, prelude::{MetadataMessage, NetworkVersion}};
//     use tezos_messages::p2p::encoding::Message;
//     use tezos_identity::Identity;
//     use crate::p2p::new_p2p::TezedgeRequest;

//     use super::super::TezedgeConfig;
//     use super::*;

//     fn network_version() -> NetworkVersion {
//         NetworkVersion::new("EDONET".to_string(), 0, 0)
//     }

//     fn identity(pkh: &[u8], pk: &[u8], sk: &[u8], pow: &[u8]) -> Identity {
//         Identity {
//             peer_id: CryptoboxPublicKeyHash::try_from_bytes(pkh).unwrap(),
//             public_key: PublicKey::from_bytes(pk).unwrap(),
//             secret_key: SecretKey::from_bytes(sk).unwrap(),
//             proof_of_work_stamp: ProofOfWork::from_hex(hex::encode(pow)).unwrap(),
//         }
//     }

//     fn identity_1() -> Identity {
//         identity(
//             &[205, 116, 184, 13, 186, 153, 102, 224, 225, 49, 230, 89, 113, 152, 215, 86],
//             &[241, 188, 163, 23, 147, 64, 39, 19, 183, 151, 213, 22, 200, 248, 165, 227, 156, 11, 10, 224, 2, 153, 152, 103, 40, 58, 166, 66, 184, 65, 115, 6],
//             &[15, 205, 34, 77, 63, 120, 181, 118, 223, 194, 12, 230, 192, 122, 159, 6, 165, 115, 17, 188, 188, 251, 104, 65, 161, 92, 139, 56, 136, 173, 30, 72],
//             &[118, 200, 143, 194, 232, 89, 158, 106, 142, 226, 250, 131, 220, 145, 166, 128, 97, 237, 225, 124, 248, 41, 90, 151],
//         )
//     }

//     fn identity_2() -> Identity {
//         identity(
//             &[91, 27, 63, 222, 61, 69, 235, 1, 240, 22, 218, 246, 65, 183, 177, 67],
//             &[39, 190, 254, 15, 130, 101, 178, 96, 131, 22, 67, 149, 147, 234, 69, 131, 157, 240, 31, 59, 21, 6, 169, 55, 74, 178, 133, 29, 166, 128, 141, 7],
//             &[72, 34, 113, 70, 55, 59, 151, 190, 231, 67, 22, 32, 38, 143, 122, 234, 24, 92, 150, 28, 221, 165, 241, 119, 71, 97, 191, 2, 236, 187, 49, 119],
//             &[62, 190, 249, 96, 102, 138, 92, 8, 13, 125, 100, 245, 176, 22, 172, 243, 23, 75, 86, 67, 162, 206, 198, 60],
//         )
//     }

//     #[test]
//     fn correct_sequence_results_in_successful_peer_connection() {
//         let client_peer_address = PeerAddress::new("peer1".to_string());
//         let client_identity = identity_1();
//         let node_identity = identity_2();

//         let build_proposal = |msg: HandshakeMsg| -> HandshakeProposal {
//             HandshakeProposal {
//                 at: Instant::now(),
//                 peer: client_peer_address.clone(),
//                 message: msg,
//             }
//         };

//         let mut tezedge_state = TezedgeState::new(
//             TezedgeConfig {
//                 port: 0,
//                 disable_mempool: false,
//                 private_node: false,
//                 min_connected_peers: 10,
//                 max_connected_peers: 20,
//                 max_pending_peers: 20,
//                 max_potential_peers: 100,
//                 peer_blacklist_duration: Duration::from_secs(30 * 60),
//                 peer_timeout: Duration::from_secs(8),
//             },
//             node_identity,
//             network_version(),
//             Instant::now(),
//         );

//         let conn_msg = ConnectionMessage::try_new(
//                 0,
//                 &client_identity.public_key,
//                 &client_identity.proof_of_work_stamp,
//                 Nonce::random(),
//                 network_version(),
//         ).unwrap();

//         tezedge_state.accept(dbg!(build_proposal(
//             HandshakeMsg::ReceivedConnect(conn_msg),
//         ))).unwrap();

//         assert!(dbg!(tezedge_state.get_requests()).iter().any(|req| {
//             match req {
//                 TezedgeRequest::SendPeerConnect((peer_address, _)) => {
//                     peer_address == &client_peer_address
//                 }
//                 _ => false
//             }
//         }));

//         tezedge_state.accept(dbg!(build_proposal(HandshakeMsg::SendConnectPending))).unwrap();
//         assert_eq!(tezedge_state.get_requests().len(), 0);

//         tezedge_state.accept(dbg!(build_proposal(HandshakeMsg::SendConnectSuccess))).unwrap();
//         assert_eq!(tezedge_state.get_requests().len(), 0);

//         let meta_msg = MetadataMessage::new(false, true);

//         tezedge_state.accept(dbg!(build_proposal(
//             HandshakeMsg::ReceivedMeta(meta_msg),
//         ))).unwrap();

//         assert!(dbg!(tezedge_state.get_requests()).iter().any(|req| {
//             match req {
//                 TezedgeRequest::SendPeerMeta((peer_address, _)) => {
//                     peer_address == &client_peer_address
//                 }
//                 _ => false
//             }
//         }));

//         tezedge_state.accept(dbg!(build_proposal(HandshakeMsg::SendMetaPending))).unwrap();
//         assert_eq!(tezedge_state.get_requests().len(), 0);

//         tezedge_state.accept(dbg!(build_proposal(HandshakeMsg::SendMetaSuccess))).unwrap();
//         assert_eq!(tezedge_state.get_requests().len(), 0);

//         tezedge_state.accept(dbg!(build_proposal(
//             HandshakeMsg::ReceivedAck(AckMessage::Ack),
//         ))).unwrap();

//         assert!(dbg!(tezedge_state.get_requests()).iter().any(|req| {
//             match req {
//                 TezedgeRequest::SendPeerAck((peer_address, _)) => {
//                     peer_address == &client_peer_address
//                 }
//                 _ => false
//             }
//         }));

//         tezedge_state.accept(dbg!(build_proposal(HandshakeMsg::SendAckPending))).unwrap();
//         assert_eq!(tezedge_state.get_requests().len(), 0);

//         tezedge_state.accept(dbg!(build_proposal(HandshakeMsg::SendAckSuccess))).unwrap();
//         assert_eq!(tezedge_state.get_requests().len(), 0);

//         // verify that peer got connected.
//         assert_eq!(tezedge_state.connected_peers.len(), 1);
//         assert!(tezedge_state.connected_peers.contains_key(&client_peer_address));
//     }
// }
