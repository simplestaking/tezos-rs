use std::fmt::{self, Display};
use std::collections::VecDeque;
use hex::FromHex;
use quickcheck::{Arbitrary, Gen};

use crypto::{crypto_box::{CryptoKey, PublicKey}, nonce::Nonce, proof_of_work::ProofOfWork};
use tezos_messages::p2p::encoding::{ack::{AckMessage, NackInfo, NackMotive}, connection::ConnectionMessage, prelude::{MetadataMessage, NetworkVersion}};
use tezos_messages::p2p::encoding::Message;
use tezos_identity::Identity;


fn message_iter(g: &mut Gen, identity: &Identity) -> impl Iterator<Item = Message> {
    vec![
        Message::Ack(AckMessage::Ack),
        Message::Ack(AckMessage::NackV0),
        Message::Ack(AckMessage::Nack(NackInfo::arbitrary(g))),

        Message::Connection(ConnectionMessage::try_new(
                0,
                &identity.public_key,
                &identity.proof_of_work_stamp,
                Nonce::random(),
                NetworkVersion::new("EDONET".to_string(), 0, 0)
        ).unwrap()),

        Message::Metadata(MetadataMessage::new(false, true)),
    ].into_iter()
}

fn try_sequence(sequence: &Vec<Message>) -> bool {
    if sequence.len() == 1 {
        matches!(&sequence[..], &[Message::Connection(_)])
    } else if sequence.len() == 2 {
        matches!(&sequence[..], &[
            Message::Connection(_),
            Message::Ack(AckMessage::Ack),
        ])
    } else {
        matches!(&sequence[(sequence.len() - 3)..], &[
                 Message::Connection(_),
                 Message::Ack(AckMessage::Ack),
                 Message::Metadata(_),
        ])
    }
}

fn sequence_to_str(seq: &Vec<Message>) -> String {
    let seq_str = seq.iter()
        .map(|msg| match msg {
            Message::Ack(AckMessage::Ack) => "ack",
            Message::Ack(AckMessage::NackV0) => "nackv0",
            Message::Ack(AckMessage::Nack(_)) => "nack",
            Message::Connection(_) => "connection",
            Message::Metadata(_) => "meta",
            _ => unimplemented!()
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{}]", seq_str)
}

#[test]
fn random_simulator() {
    let mut g = Gen::new(10);
    let identity = Identity::generate(ProofOfWork::DEFAULT_TARGET).unwrap();

    let mut successful_sequences = vec![];
    let mut sequences = VecDeque::<Vec<Message>>::new();
    sequences.push_back(vec![]);

    while let Some(sequence) = sequences.pop_front() {
        if sequence.len() > 4 {
            break;
        }

        for message in message_iter(&mut g, &identity) {
            let mut new_seq = sequence.clone();
            new_seq.push(message);

            if try_sequence(&new_seq) {
                println!("successful sequence: {}", sequence_to_str(&new_seq));
                successful_sequences.push(new_seq.clone());
            }

            sequences.push_back(new_seq);
        }
    }
}
