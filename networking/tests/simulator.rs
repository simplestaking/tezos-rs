use std::time::{Instant, Duration};
use std::fmt::{self, Display};
use std::collections::VecDeque;
use hex::FromHex;
use quickcheck::{Arbitrary, Gen};
use itertools::Itertools;

use crypto::{crypto_box::{CryptoKey, PublicKey}, nonce::Nonce, proof_of_work::ProofOfWork};
use tezos_messages::p2p::encoding::{ack::{AckMessage, NackInfo, NackMotive}, connection::ConnectionMessage, prelude::{MetadataMessage, NetworkVersion}};
use tezos_messages::p2p::encoding::Message;
use tezos_identity::Identity;
use networking::p2p::new_p2p::*;


fn message_iter(g: &mut Gen, identity: &Identity) -> impl Iterator<Item = HandshakeMsg> {
    vec![
        HandshakeMsg::SendConnectPending,
        HandshakeMsg::SendConnectSuccess,
        HandshakeMsg::SendConnectError,

        HandshakeMsg::SendMetaPending,
        HandshakeMsg::SendMetaSuccess,
        HandshakeMsg::SendMetaError,

        HandshakeMsg::SendAckPending,
        HandshakeMsg::SendAckSuccess,
        HandshakeMsg::SendAckError,

        HandshakeMsg::ReceivedConnect(
            ConnectionMessage::try_new(
                    0,
                    &identity.public_key,
                    &identity.proof_of_work_stamp,
                    Nonce::random(),
                    NetworkVersion::new("EDONET".to_string(), 0, 0)
            ).unwrap(),
        ),

        HandshakeMsg::ReceivedMeta(
            MetadataMessage::new(false, true),
        ),

        HandshakeMsg::ReceivedAck(AckMessage::Ack),
        HandshakeMsg::ReceivedAck(AckMessage::NackV0),
        HandshakeMsg::ReceivedAck(AckMessage::Nack(NackInfo::arbitrary(g))),
    ].into_iter()
}

fn try_sequence(pm: &mut PeerManager, sequence: &Vec<HandshakeMsg>, counter: &mut u64) -> bool {
    let peer_id = PeerId::new("peer1".to_string());

    for msg in sequence {
        *counter += 1;

        let result = pm.accept_handshake(&HandshakeProposal {
            at: Instant::now() + Duration::from_millis(*counter),
            peer: peer_id.clone(),
            message: msg.clone(),
        });

        if let Err(_) = result {
            return false;
        }
    }

    matches!(pm.peer_state(&peer_id), Some(&PeerState::Connected { .. }))
}

fn sequence_to_str(seq: &Vec<HandshakeMsg>) -> String {
    let seq_str = seq.iter()
        .map(|msg| match msg {
            HandshakeMsg::SendConnectPending => "send_connect_pending",
            HandshakeMsg::SendConnectSuccess => "send_connect_success",
            HandshakeMsg::SendConnectError => "send_connect_error",

            HandshakeMsg::SendMetaPending => "send_meta_pending",
            HandshakeMsg::SendMetaSuccess => "send_meta_success",
            HandshakeMsg::SendMetaError => "send_meta_error",

            HandshakeMsg::SendAckPending => "send_ack_pending",
            HandshakeMsg::SendAckSuccess => "send_ack_success",
            HandshakeMsg::SendAckError => "send_ack_error",

            HandshakeMsg::ReceivedConnect(_) => "receive_connect",

            HandshakeMsg::ReceivedMeta(_) => "receive_meta",

            HandshakeMsg::ReceivedAck(AckMessage::Ack) => "receive_ack",
            HandshakeMsg::ReceivedAck(AckMessage::NackV0) => "receive_nackv0",
            HandshakeMsg::ReceivedAck(AckMessage::Nack(_)) => "receive_nack",
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{}]", seq_str)
}

#[test]
fn random_simulator() {
    let mut g = Gen::new(10);
    let identity = Identity::generate(ProofOfWork::DEFAULT_TARGET).unwrap();
    let node_identity = Identity::generate(ProofOfWork::DEFAULT_TARGET).unwrap();

    let mut successful_sequences = vec![];

    let msgs = message_iter(&mut g, &identity).collect::<Vec<_>>();
    let mut counter = 0;

    for seq_len in 1..=9 {
        for seq in msgs.clone().into_iter().permutations(seq_len) {
            let mut pm = PeerManager::new(
                PeerManagerConfig {
                    disable_mempool: false,
                    private_node: false,
                    min_connected_peers: 10,
                    max_connected_peers: 20,
                    peer_blacklist_duration: Duration::from_secs(30 * 60),
                    peer_timeout: Duration::from_secs(8),
                },
                node_identity.clone(),
                Instant::now(),
            );
            if try_sequence(&mut pm, &seq, &mut counter) {
                println!("successful sequence: {}", sequence_to_str(&seq));
                successful_sequences.push(seq.clone());
            }
        }
    }
}
