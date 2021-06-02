use std::time::Instant;
use tezos_messages::p2p::encoding::prelude::NetworkVersion;

use crypto::{crypto_box::{CryptoKey, PublicKey, SecretKey}, hash::{CryptoboxPublicKeyHash, HashTrait}, proof_of_work::ProofOfWork};
use hex::FromHex;
use tezos_identity::Identity;
use crate::*;

fn network_version() -> NetworkVersion {
    NetworkVersion::new("TEZOS_MAINNET".to_string(), 0, 1)
}

fn identity(pkh: &[u8], pk: &[u8], sk: &[u8], pow: &[u8]) -> Identity {
    Identity {
        peer_id: CryptoboxPublicKeyHash::try_from_bytes(pkh).unwrap(),
        public_key: PublicKey::from_bytes(pk).unwrap(),
        secret_key: SecretKey::from_bytes(sk).unwrap(),
        proof_of_work_stamp: ProofOfWork::from_hex(hex::encode(pow)).unwrap(),
    }
}

fn identity_1() -> Identity {
    identity(
        &[86, 205, 231, 178, 152, 146, 2, 157, 213, 131, 90, 117, 83, 132, 177, 84],
        &[148, 73, 141, 148, 22, 20, 15, 188, 69, 132, 149, 51, 61, 170, 193, 180, 200, 126, 65, 159, 87, 38, 113, 122, 84, 249, 182, 198, 116, 118, 174, 28],
        &[172, 122, 207, 58, 254, 215, 99, 123, 225, 15, 143, 199, 106, 46, 182, 179, 53, 156, 120, 173, 177, 216, 19, 180, 28, 186, 179, 250, 233, 84, 244, 177],
        &[187, 194, 48, 1, 73, 36, 158, 28, 204, 132, 165, 67, 98, 35, 108, 60, 187, 194, 204, 47, 251, 211, 182, 234],
    )
}

pub fn build(config: TezedgeConfig) -> TezedgeState {
    let node_identity = identity_1();

    let tezedge_state = TezedgeState::new(
        config,
        node_identity.clone(),
        network_version(),
        Instant::now(),
    );

    tezedge_state
}
