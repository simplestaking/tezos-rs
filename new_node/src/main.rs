use std::iter::FromIterator;
use std::time::{Duration, Instant};
use std::collections::HashSet;
use tezos_messages::p2p::encoding::prelude::NetworkVersion;
use tla_sm::Acceptor;

use crypto::{crypto_box::{CryptoKey, PublicKey, SecretKey}, hash::{CryptoboxPublicKeyHash, HashTrait}, proof_of_work::ProofOfWork};
use hex::FromHex;
use tezos_identity::Identity;
use tezedge_state::{TezedgeState, TezedgeConfig, PeerAddress};
use tezedge_state::proposals::ExtendPotentialPeersProposal;

use tezedge_state::proposer::{TezedgeProposer, TezedgeProposerConfig};
use tezedge_state::proposer::net_p2p::{NetP2pManager, MioEvents};

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

const SERVER_PORT: u16 = 13632;

fn build_tezedge_state() -> TezedgeState {
    // println!("generating identity...");
    // let node_identity = Identity::generate(ProofOfWork::DEFAULT_TARGET).unwrap();
    // dbg!(&node_identity);
    // dbg!(node_identity.secret_key.as_ref().0);

    let node_identity = identity_1();

    // println!("identity generated!");
    let mut tezedge_state = TezedgeState::new(
        TezedgeConfig {
            port: SERVER_PORT,
            disable_mempool: true,
            private_node: true,
            min_connected_peers: 500,
            max_connected_peers: 1000,
            max_pending_peers: 1000,
            max_potential_peers: 100000,
            periodic_react_interval: Duration::from_millis(250),
            peer_blacklist_duration: Duration::from_secs(30 * 60),
            peer_timeout: Duration::from_secs(8),
        },
        node_identity.clone(),
        network_version(),
        Instant::now(),
    );


    let raw_peer_addresses = HashSet::<_>::from_iter(
        [
            vec!["159.65.98.117:9732", "34.245.171.88:9732", "18.182.168.120:9732", "13.115.2.66:9732", "18.179.219.134:9732", "45.77.35.193:9732", "73.96.221.90:9732", "62.149.16.61:9732", "18.182.169.115:9732", "143.110.185.25:9732", "45.32.203.167:9732", "66.70.178.32:9732", "64.225.6.118:9732", "104.236.125.54:9732", "84.201.132.206:9732", "46.245.179.162:9733", "18.158.218.189:9732", "138.201.9.113:9735", "95.217.154.147:9732", "62.109.18.93:9732", "24.134.10.217:9732", "135.181.49.110:9732", "95.217.46.253:9732", "46.245.179.163:9732", "18.185.162.213:9732", "34.107.95.94:9732", "162.55.1.145:9732", "34.208.149.159:9732", "13.251.146.136:9732", "143.110.209.198:9732", "34.255.45.216:9732", "107.191.62.113:9732", "15.236.199.66:9732", "[::ffff:95.216.45.62]:9732", "157.90.35.112:9732", "144.76.200.188:9732", "[::ffff:18.185.162.213]:9732", "[::ffff:18.184.136.151]:9732", "[::ffff:18.195.59.36]:9732", "[::ffff:18.185.162.144]:9732", "[::ffff:18.185.78.112]:9732", "[::ffff:116.202.172.21]:9732"]
                .into_iter()
                .map(|x| x.to_owned())
                .collect::<Vec<_>>(),
            // fake peers, just for testing.
            // (0..10000).map(|x| format!(
            //         "{}.{}.{}.{}:12345",
            //         (x / 256 / 256 / 256) % 256,
            //         (x / 256 / 256) % 256,
            //         (x / 256) % 256,
            //         x % 256,
            //     )).collect::<Vec<_>>(),
        ].concat().into_iter()
    );

    let _ = tezedge_state.accept(ExtendPotentialPeersProposal {
        at: Instant::now(),
        peers: raw_peer_addresses.into_iter()
            .map(|x| PeerAddress::new(x)),
    });

    tezedge_state
}

fn main() {
    let mut p2p_manager = NetP2pManager::new(SERVER_PORT);
    p2p_manager.listen_on(SERVER_PORT);

    let mut proposer = TezedgeProposer::new(
        TezedgeProposerConfig {
            wait_for_events_timeout: Some(Duration::from_millis(250)),
            events_limit: 1024,
        },
        build_tezedge_state(),
        // capacity is changed by events_limit.
        MioEvents::with_capacity(0),
        p2p_manager,
    );

    println!("starting loop");
    loop {
        proposer.make_progress();
    }
}
