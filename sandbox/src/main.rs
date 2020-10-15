// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![forbid(unsafe_code)]

use std::{env, iter};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use rand::distributions::Alphanumeric;
use rand::Rng;
use slog::{Drain, info, Level, Logger};

mod configuration;
mod filters;
mod handlers;
mod node_runner;
mod tezos_client_runner;

#[tokio::main]
async fn main() {
    // parse and validate program arguments
    let env = configuration::LauncherEnvironment::from_args();

    // create an slog logger
    let log = create_logger(env.log_level);

    // sandbox peers map
    let peers = Arc::new(Mutex::new(HashSet::new()));

    // create a thread safe reference to the runner struct
    let runner = Arc::new(RwLock::new(node_runner::LightNodeRunner::new(
        "light-node-0",
        env.light_node_path,
    )));

    // create a thread safe reference to the client runner struct
    let client_runner = Arc::new(RwLock::new(tezos_client_runner::TezosClientRunner::new(
        "tezos-client",
        env.tezos_client_path,
    )));

    // the port to open the rpc server on
    let rpc_port = env.sandbox_rpc_port;

    // combined warp filter
    let api = filters::sandbox(log.clone(), runner, client_runner, peers);

    info!(log, "Start to serving Sandbox RPCs");

    // start serving the api
    warp::serve(api).run(([0, 0, 0, 0], rpc_port)).await;
}

/// Creates a slog Logger
fn create_logger(level: Level) -> Logger {
    let drain = slog_async::Async::new(
        slog_term::FullFormat::new(slog_term::TermDecorator::new().build())
            .build()
            .fuse(),
    )
        .chan_size(32768)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .filter_level(level)
        .fuse();
    Logger::root(drain, slog::o!())
}

/// Crate new randomly named temp directory.
pub fn temp_dir(prefix: &str) -> PathBuf {
    let mut rng = rand::thread_rng();
    let suffix = iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .take(7)
        .collect::<String>();

    let temp_dir = env::temp_dir();
    temp_dir.join(format!("{}-{}", prefix, suffix))
}