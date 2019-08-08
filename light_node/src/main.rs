#![feature(async_await)]
#[macro_use]
extern crate lazy_static;

use std::path::{Path, PathBuf};

use log::{debug, error, info};

use networking::p2p::node::P2pLayer;
use networking::rpc::message::{BootstrapMessage, PeerAddress};
use storage::db::Db;
use tokio;
use networking::rpc::server::RpcLayer;

mod configuration;

const LOG_FILE: &str = "log4rs.yml";
pub const MPSC_BUFFER_SIZE: usize = 50;

/// Function configures default console logger.
fn configure_default_logger() {
    use log::LevelFilter;
    use log4rs::append::console::ConsoleAppender;
    use log4rs::encode::pattern::PatternEncoder;
    use log4rs::config::{Appender, Config, Root};

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} {h({l})} {t} - {h({m})} {n}")))
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))
        .unwrap();

    log4rs::init_config(config).unwrap();
}

#[tokio::main]
async fn main() {
    use networking::p2p::client::P2pClient;

    match log4rs::init_file(LOG_FILE, Default::default()) {
        Ok(_) => debug!("Logger configured from file: {}", LOG_FILE),
        Err(m) => {
            println!("Logger configuration file {} not loaded: {}", LOG_FILE, m);
            println!("Using default logger configuration");
            configure_default_logger()
        }
    }

    let initial_peers: Vec<PeerAddress> = configuration::ENV.initial_peers.clone()
        .into_iter()
        .map(|(ip, port)| {
            PeerAddress {
                host: ip.clone(),
                port,
            }
        })
        .collect();

    let identity_json_file_path: PathBuf = configuration::ENV.identity_json_file_path.clone()
        .unwrap_or_else(|| {
            let tezos_default_identity: PathBuf = configuration::tezos_node::get_default_tezos_identity_json_file_path().unwrap();
            if tezos_default_identity.exists() {
                // if exists tezos default location, then use it
                tezos_default_identity
            } else {
                // or just use our config/identity.json
                Path::new("./config/identity.json").to_path_buf()
            }
        });

    info!("Starting Iron p2p");

    let init_chain_id = hex::decode(configuration::tezos_node::genesis_chain_id());
    if let Err(e) = init_chain_id {
        error!("Failed to load initial chain id. Reason: {:?}", e);
        return;
    }
    let identity = configuration::tezos_node::load_identity(identity_json_file_path);
    if let Err(e) = identity {
        error!("Failed to load identity. Reason: {:?}", e);
        return;
    }

    let p2p_client = P2pClient::new(
        init_chain_id.unwrap(),
        identity.unwrap(),
        configuration::tezos_node::versions(),
        Db::new(),
        configuration::ENV.p2p.listener_port,
        configuration::ENV.log_message_contents
    );

    let p2p = P2pLayer::new(p2p_client);
    // init node bootstrap
    if !initial_peers.is_empty() {
        p2p.bootstrap_with_peers(BootstrapMessage { initial_peers }).await.expect("Failed to transmit bootstrap encoding to p2p layer")
    } else {
        p2p.bootstrap_with_lookup(&configuration::ENV.p2p.bootstrap_lookup_addresses).await.expect("Failed to transmit bootstrap encoding to p2p layer")
    }

    let rpc = RpcLayer::new(&configuration::ENV.p2p.bootstrap_lookup_addresses, configuration::ENV.rpc.listener_port);
    // ------------------
    // Lines after the following block will be executed only after accept_connections() task will complete
    // ------------------
    let res = networking::rpc::server::accept_connections(rpc, p2p).await;
    if let Err(e) = res {
        error!("Failed to start accepting RPC connections. Reason: {:?}", e);
        return;
    }

    info!("Iron p2p stopped")
}
