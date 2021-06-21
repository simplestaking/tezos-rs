use crypto::crypto_box::CryptoKey;
use crypto::crypto_box::PrecomputedKey;
use crypto::crypto_box::PublicKey;
use crypto::nonce::Nonce;
use crypto::nonce::generate_nonces;
use tezedge_state::*;
use tezos_identity::Identity;
use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryWrite, BinaryRead};
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};

use super::identities::identity_1;

fn network_version() -> NetworkVersion {
    NetworkVersion::new("EDONET".to_string(), 0, 0)
}

struct PeerInfo {
    crypto: PeerCrypto,
}

#[derive(Debug, Clone)]
pub struct GeneratedPeer {
    identity: Identity,
    sent_conn_msg: BinaryChunk,
    crypto: Option<PeerCrypto>,
}

impl GeneratedPeer {
    pub fn generate() -> Self {
        let identity = identity_1();
        let sent_conn_msg = BinaryChunk::from_content(
            &ConnectionMessage::try_new(
                100,
                &identity.public_key,
                &identity.proof_of_work_stamp,
                Nonce::random(),
                network_version(),
            ).unwrap().as_bytes().unwrap(),
        ).unwrap();

        Self {
            identity,
            sent_conn_msg,
            crypto: None,
        }
    }

    pub fn set_received_connection_msg(&mut self, message: BinaryChunk, incoming: bool) {
        let conn_msg = ConnectionMessage::from_bytes(message.content()).unwrap();
        let nonce_pair = generate_nonces(
            self.connection_msg().content(),
            message.content(),
            incoming,
        ).unwrap();

        let precomputed_key = PrecomputedKey::precompute(
            &PublicKey::from_bytes(conn_msg.public_key()).unwrap(),
            &self.identity.secret_key
        );

        self.crypto = Some(PeerCrypto::new(precomputed_key, nonce_pair));

    }

    pub fn connection_msg(&self) -> BinaryChunk {
        self.sent_conn_msg.clone()
    }

    pub fn encrypted_metadata_msg(&mut self) -> BinaryChunk {
        let crypto = self.crypto.as_mut().unwrap();

        BinaryChunk::from_content(
            &crypto.encrypt(
                &MetadataMessage::new(false, false).as_bytes().unwrap(),
            ).unwrap(),
        ).unwrap()
    }

    pub fn encrypted_ack_msg(&mut self, message: AckMessage) -> BinaryChunk {
        let crypto = self.crypto.as_mut().unwrap();

        BinaryChunk::from_content(
            &crypto.encrypt(
                &message.as_bytes().unwrap(),
            ).unwrap(),
        ).unwrap()
    }
}
