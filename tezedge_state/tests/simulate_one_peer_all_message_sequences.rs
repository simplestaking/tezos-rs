use std::time::{Instant, Duration};
use quickcheck::{Arbitrary, Gen};
use itertools::Itertools;

use crypto::nonce::Nonce;
use crypto::proof_of_work::ProofOfWork;
use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryWrite, BinaryRead};
use tezos_messages::p2p::encoding::prelude::{ConnectionMessage, MetadataMessage, NetworkVersion};
use tezos_messages::p2p::encoding::ack::{AckMessage, NackInfo};
use tezos_identity::Identity;
use tezedge_state::*;
use tezedge_state::proposals::*;
use tezedge_state::proposals::peer_message::*;
use tezedge_state::acceptors::*;

pub mod common;
use common::identities::identity_1;

fn network_version() -> NetworkVersion {
    NetworkVersion::new("EDONET".to_string(), 0, 0)
}

#[derive(Debug, Clone)]
enum Messages {
    Handshake(HandshakeMsg),
    PeerMessage(PeerAbstractDecodedMessage),
}

impl From<HandshakeMsg> for Messages {
    fn from(msg: HandshakeMsg) -> Self {
        Messages::Handshake(msg)
    }
}

impl From<PeerAbstractDecodedMessage> for Messages {
    fn from(msg: PeerAbstractDecodedMessage) -> Self {
        Messages::PeerMessage(msg)
    }
}

fn to_binary_chunk<M: BinaryMessage>(msg: &M) -> BinaryChunk {
    BinaryChunk::from_content(&msg.as_bytes().unwrap()).unwrap()
}

fn message_iter(g: &mut Gen, identity: &Identity) -> impl Iterator<Item = Messages> {
    let conn_msg = ConnectionMessage::try_new(
            0,
            &identity.public_key,
            &identity.proof_of_work_stamp,
            Nonce::random(),
            network_version(),
    ).unwrap();
    let meta_msg = MetadataMessage::new(false, true);
    let ack_msg = AckMessage::Ack;
    let nack_v0_msg = AckMessage::NackV0;
    let nack_msg = AckMessage::Nack(NackInfo::arbitrary(g));

    vec![
        HandshakeMsg::SendConnectPending.into(),
        HandshakeMsg::SendConnectSuccess.into(),
        HandshakeMsg::SendConnectError.into(),

        HandshakeMsg::SendMetaPending.into(),
        HandshakeMsg::SendMetaSuccess.into(),
        HandshakeMsg::SendMetaError.into(),

        HandshakeMsg::SendAckPending.into(),
        HandshakeMsg::SendAckSuccess.into(),
        HandshakeMsg::SendAckError.into(),


        PeerAbstractDecodedMessage::new(to_binary_chunk(&conn_msg), conn_msg.into()).into(),
        PeerAbstractDecodedMessage::new(to_binary_chunk(&meta_msg), meta_msg.into()).into(),
        PeerAbstractDecodedMessage::new(to_binary_chunk(&ack_msg), ack_msg.into()).into(),
        PeerAbstractDecodedMessage::new(to_binary_chunk(&nack_v0_msg), nack_v0_msg.into()).into(),
        PeerAbstractDecodedMessage::new(to_binary_chunk(&nack_msg), nack_msg.into()).into(),
    ].into_iter()
}

fn try_sequence(state: &mut TezedgeState, sequence: Vec<Messages>) -> bool {
    let peer = PeerAddress::ipv4_from_index(1);

    for msg in sequence {
        match msg {
            Messages::Handshake(msg) => {
                state.accept(HandshakeProposal {
                    at: Instant::now(),
                    peer: peer.clone(),
                    message: msg.clone(),
                })
            }
            Messages::PeerMessage(msg) => {
                state.accept(PeerProposal {
                    at: Instant::now(),
                    peer: peer.clone(),
                    message: msg.clone(),
                })
            }
        };
    }

    state.is_peer_connected(&peer)
}

fn sequence_to_str(seq: &Vec<Messages>) -> String {
    let seq_str = seq.iter()
        .map(|msg| match msg {
            Messages::Handshake(msg) => match msg {
                HandshakeMsg::SendConnectPending => "send_connect_pending",
                HandshakeMsg::SendConnectSuccess => "send_connect_success",
                HandshakeMsg::SendConnectError => "send_connect_error",

                HandshakeMsg::SendMetaPending => "send_meta_pending",
                HandshakeMsg::SendMetaSuccess => "send_meta_success",
                HandshakeMsg::SendMetaError => "send_meta_error",

                HandshakeMsg::SendAckPending => "send_ack_pending",
                HandshakeMsg::SendAckSuccess => "send_ack_success",
                HandshakeMsg::SendAckError => "send_ack_error",
            },
            Messages::PeerMessage(msg) => match msg.message_type() {
                PeerAbstractDecodedMessageType::Connection(_) => "receive_connect",
                PeerAbstractDecodedMessageType::Metadata(_) => "receive_metadata",
                PeerAbstractDecodedMessageType::Ack(AckMessage::Ack) => "receive_ack",
                PeerAbstractDecodedMessageType::Ack(AckMessage::NackV0) => "receive_nack_v0",
                PeerAbstractDecodedMessageType::Ack(AckMessage::Nack(_)) => "receive_nack",
                PeerAbstractDecodedMessageType::Message(_) => "message",
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{}]", seq_str)
}

#[test]
fn simulate_one_peer_all_message_sequences() {
    let mut g = Gen::new(10);
    println!("generating identity for client...");
    let identity = identity_1();
    println!("generating identity for p2p manager...");
    let node_identity = Identity::generate(ProofOfWork::DEFAULT_TARGET).unwrap();

    let mut successful_sequences = vec![];

    let msgs = message_iter(&mut g, &identity).collect::<Vec<_>>();

    let tezedge_state = TezedgeState::new(
        TezedgeConfig {
            port: 0,
            disable_mempool: false,
            private_node: false,
            min_connected_peers: 10,
            max_connected_peers: 20,
            max_potential_peers: 100,
            max_pending_peers: 20,
            periodic_react_interval: Duration::from_millis(1),
            peer_blacklist_duration: Duration::from_secs(30 * 60),
            peer_timeout: Duration::from_secs(8),
        },
        node_identity.clone(),
        network_version(),
        Default::default(),
        Instant::now(),
    );

    for seq_len in 1..=9 {
        println!("trying sequences with length: {}", seq_len);
        let mut count = 0;
        for seq in msgs.clone().into_iter().permutations(seq_len) {
            count += 1;
            let mut pm = tezedge_state.clone();
            if try_sequence(&mut pm, seq.clone()) {
                println!("successful sequence: {}", sequence_to_str(&seq));
                successful_sequences.push(seq.clone());
            }
        }
        println!("tried permutations: {}", count);
    }
}
