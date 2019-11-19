// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

/// Separate Tezos protocol runner, as we used OCaml protocol more and more, we noticed increasing
/// problems, from panics to high memory usage, for better stability, we separated protocol into
/// self-contained process communicating through Unix Socket.
use std::thread;
use std::time::Duration;

use clap::{App, Arg};
use slog::*;

use tezos_context::channel;

fn create_logger() -> Logger {
    let drain = slog_async::Async::new(slog_term::FullFormat::new(slog_term::TermDecorator::new().build()).build().fuse()).build().filter_level(Level::Info).fuse();

    Logger::root(drain, slog::o!())
}

fn main() {
    let log = create_logger();

    let matches = App::new("Protocol Runner")
        .version("1.0")
        .author("Tomas Sedlak <tomas.sedlak@simplestaking.com>")
        .about("Tezos Protocol Runner")
        .arg(Arg::with_name("sock-cmd")
            .short("c")
            .long("sock-cmd")
            .value_name("path")
            .help("Path to a command socket")
            .takes_value(true)
            .empty_values(false)
            .required(true))
        .arg(Arg::with_name("sock-evt")
            .short("e")
            .long("sock-evt")
            .value_name("path")
            .help("Path to an event socket")
            .takes_value(true)
            .empty_values(false)
            .required(true))
        .get_matches();

    let cmd_socket_path = matches.value_of("sock-cmd").expect("Missing sock-cmd value");
    let evt_socket_path = matches.value_of("sock-evt").expect("Missing sock-evt value").to_string();

    ctrlc::set_handler(move || {
        // do nothing and wait for parent process to send termination command
    }).expect("Error setting Ctrl-C handler");

    let event_thread = {
        let log = log.clone();
        channel::enable_context_channel();
        thread::spawn(move || {
            for _ in 0..5 {
                match tezos_wrapper::service::process_protocol_events(&evt_socket_path) {
                    Ok(()) => break,
                    Err(err) => {
                        warn!(log, "Error while processing protocol events"; "reason" => format!("{:?}", err));
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        })
    };

    if let Err(err) = tezos_wrapper::service::process_protocol_commands::<crate::tezos::NativeTezosLib, _>(cmd_socket_path) {
        error!(log, "Error while processing protocol commands"; "reason" => format!("{:?}", err));
    }

    event_thread.join().expect("Failed to join event thread");
}

mod tezos {
    use tezos_api::client::TezosStorageInitInfo;
    use tezos_api::environment::TezosEnvironment;
    use tezos_api::ffi::{ApplyBlockError, ApplyBlockResult, TezosGenerateIdentityError, TezosRuntimeConfiguration, TezosRuntimeConfigurationError, TezosStorageInitError};
    use tezos_api::identity::Identity;
    use tezos_client::client::{apply_block, change_runtime_configuration, generate_identity, init_storage};
    use tezos_encoding::hash::ChainId;
    use tezos_messages::p2p::encoding::prelude::*;
    use tezos_wrapper::protocol::ProtocolApi;

    pub struct NativeTezosLib;

    impl ProtocolApi for NativeTezosLib {
        fn apply_block(chain_id: &ChainId, block_header: &BlockHeader, operations: &Vec<Option<OperationsForBlocksMessage>>) -> Result<ApplyBlockResult, ApplyBlockError> {
            apply_block(chain_id, block_header, operations)
        }

        fn change_runtime_configuration(settings: TezosRuntimeConfiguration) -> Result<(), TezosRuntimeConfigurationError> {
            change_runtime_configuration(settings)
        }

        fn init_storage(storage_data_dir: String, tezos_environment: TezosEnvironment) -> Result<TezosStorageInitInfo, TezosStorageInitError> {
            init_storage(storage_data_dir, tezos_environment)
        }

        fn generate_identity(expected_pow: f64) -> Result<Identity, TezosGenerateIdentityError> {
            generate_identity(expected_pow)
        }
    }
}