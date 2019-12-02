// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use riker::actors::*;
use slog::{crit, debug, Drain, error, info, Logger};
use tokio::runtime::Runtime;

use logging::detailed_json;
use logging::file::FileAppenderBuilder;
use monitoring::{listener::{
    EventPayloadStorage,
    EventStorage, NetworkChannelListener,
}, Monitor, WebsocketHandler};
use networking::p2p::network_channel::NetworkChannel;
use rpc::rpc_actor::RpcServer;
use shell::chain_feeder::ChainFeeder;
use shell::chain_manager::ChainManager;
use shell::context_listener::ContextListener;
use shell::peer_manager::PeerManager;
use shell::shell_channel::{ShellChannel, ShellChannelTopic, ShuttingDown};
use storage::{block_storage, BlockMetaStorage, BlockStorage, ContextStorage, initialize_storage_with_genesis_block, OperationsMetaStorage, OperationsStorage, StorageError, SystemStorage};
use storage::persistent::{CommitLogs, CommitLogSchema, KeyValueSchema, open_db};
use tezos_api::client::TezosStorageInitInfo;
use tezos_api::environment;
use tezos_api::ffi::TezosRuntimeConfiguration;
use tezos_api::identity::Identity;
use tezos_wrapper::service::{IpcCmdServer, IpcEvtServer, ProtocolEndpointConfiguration, ProtocolRunner, ProtocolRunnerEndpoint};

use crate::configuration::LogFormat;

mod configuration;
mod identity;

const EXPECTED_POW: f64 = 26.0;
const DATABASE_VERSION: i64 = 3;

macro_rules! shutdown_and_exit {
    ($err:expr, $sys:ident) => {{
        $err;
        futures::executor::block_on($sys.shutdown()).unwrap();
        return;
    }}
}

macro_rules! create_terminal_logger {
    ($type:expr) => {{
        match $type {
            LogFormat::Simple => slog_async::Async::new(slog_term::FullFormat::new(slog_term::TermDecorator::new().build()).build().fuse()).chan_size(32768).overflow_strategy(slog_async::OverflowStrategy::Block).build(),
            LogFormat::Json => slog_async::Async::new(detailed_json::default(std::io::stdout()).fuse()).chan_size(32768).overflow_strategy(slog_async::OverflowStrategy::Block).build(),
        }
    }}
}

macro_rules! create_file_logger {
    ($type:expr, $path:expr) => {{
        let appender = FileAppenderBuilder::new($path)
            .rotate_size(10_485_760) // 10 MB
            .rotate_keep(4)
            .rotate_compress(true)
            .build();

        match $type {
            LogFormat::Simple => slog_async::Async::new(slog_term::FullFormat::new(slog_term::PlainDecorator::new(appender)).build().fuse()).chan_size(32768).overflow_strategy(slog_async::OverflowStrategy::Block).build(),
            LogFormat::Json => slog_async::Async::new(detailed_json::default(appender).fuse()).chan_size(32768).overflow_strategy(slog_async::OverflowStrategy::Block).build(),
        }
    }}
}

fn create_logger(env: &crate::configuration::Environment) -> Logger {
    let drain = match &env.logging.file {
        Some(log_file) => create_file_logger!(env.logging.format, log_file),
        None => create_terminal_logger!(env.logging.format),
    }.filter_level(env.logging.level).fuse();

    Logger::root(drain, slog::o!())
}

fn block_on_actors(env: &crate::configuration::Environment, identity: Identity, actor_system: ActorSystem, init_info: TezosStorageInitInfo, rocks_db: Arc<rocksdb::DB>, commit_logs: Arc<CommitLogs>, protocol_commands: IpcCmdServer, protocol_events: IpcEvtServer, protocol_runner_run: Arc<AtomicBool>, log: Logger) {
    let tokio_runtime = Runtime::new().expect("Failed to create tokio runtime");

    let network_channel = NetworkChannel::actor(&actor_system)
        .expect("Failed to create network channel");
    let shell_channel = ShellChannel::actor(&actor_system)
        .expect("Failed to create shell channel");
    let _ = PeerManager::actor(
        &actor_system,
        network_channel.clone(),
        shell_channel.clone(),
        tokio_runtime.executor(),
        &env.p2p.bootstrap_lookup_addresses,
        &env.p2p.initial_peers,
        env.p2p.peer_threshold,
        env.p2p.listener_port,
        identity,
        environment::TEZOS_ENV
            .get(&env.tezos_network)
            .map(|cfg| cfg.version.clone())
            .expect(&format!("No tezos environment version configured for: {:?}", env.tezos_network)))
        .expect("Failed to create peer manager");
    let _ = ChainManager::actor(&actor_system, network_channel.clone(), shell_channel.clone(), rocks_db.clone(), commit_logs.clone(), &init_info)
        .expect("Failed to create chain manager");
    let _ = ChainFeeder::actor(&actor_system, shell_channel.clone(), rocks_db.clone(), commit_logs.clone(), &init_info, protocol_commands, log.clone())
        .expect("Failed to create chain feeder");
    let _ = ContextListener::actor(&actor_system, rocks_db.clone(), protocol_events, log.clone())
        .expect("Failed to create context event listener");
    let websocket_handler = WebsocketHandler::actor(&actor_system, env.rpc.websocket_address, log.clone())
        .expect("Failed to start websocket actor");
    let _ = Monitor::actor(&actor_system, network_channel.clone(), websocket_handler, shell_channel.clone(), rocks_db.clone())
        .expect("Failed to create monitor actor");
    let _ = RpcServer::actor(&actor_system, shell_channel.clone(), ([127, 0, 0, 1], env.rpc.listener_port).into(), &tokio_runtime, rocks_db.clone(), commit_logs.clone(), &init_info)
        .expect("Failed to create RPC server");
    if env.record {
        info!(log, "Running in record mode");
        let _ = NetworkChannelListener::actor(&actor_system, rocks_db.clone(), network_channel.clone());
    }

    tokio_runtime.block_on(async move {
        use std::thread;
        use std::time::Duration;

        use futures::future;
        use futures::stream::StreamExt;
        use tokio::net::signal;

        let ctrl_c = signal::ctrl_c().unwrap();
        let prog = ctrl_c.take(1).for_each(|_| {
            info!(log, "ctrl-c received!");

            protocol_runner_run.store(false, Ordering::Release);

            info!(log, "Sending shutdown notification to actors");
            shell_channel.tell(
                Publish {
                    msg: ShuttingDown.into(),
                    topic: ShellChannelTopic::ShellCommands.into(),
                }, None
            );

            // give actors some time to shut down
            thread::sleep(Duration::from_secs(1));
            // resolve future
            future::ready(())
        });
        prog.await;
        info!(log, "Shutting down actor runtime");
        let _ = actor_system.shutdown().await;
    });
}

fn check_database_compatibility(db: Arc<rocksdb::DB>, init_info: &TezosStorageInitInfo, log: Logger) -> Result<bool, StorageError> {
    let mut system_info = SystemStorage::new(db.clone());
    let db_version_ok = match system_info.get_db_version()? {
        Some(db_version) => db_version == DATABASE_VERSION,
        None => {
            system_info.set_db_version(DATABASE_VERSION)?;
            true
        }
    };
    if !db_version_ok {
        error!(log, "Incompatible database version found. Please re-sync your node.");
    }

    let chain_id_ok = match system_info.get_chain_id()? {
        Some(chain_id) => chain_id == init_info.chain_id,
        None => {
            system_info.set_chain_id(&init_info.chain_id)?;
            true
        }
    };
    if !chain_id_ok {
        error!(log, "Current database was created for another chain. Please re-sync your node.");
    }

    Ok(db_version_ok && chain_id_ok)
}

fn main() {
    // Parses config + cli args
    let env = crate::configuration::Environment::from_args();

    // Creates default logger
    let log = create_logger(&env);

    let actor_system = SystemBuilder::new().name("light-node").log(log.clone()).create().expect("Failed to create actor system");

    // tezos protocol runner endpoint
    let mut protocol_runner_endpoint = ProtocolRunnerEndpoint::new(ProtocolEndpointConfiguration::new(
        TezosRuntimeConfiguration {
            log_enabled: env.logging.ocaml_log_enabled,
            no_of_ffi_calls_treshold_for_gc: env.no_of_ffi_calls_treshold_for_gc,
        },
        env.tezos_network,
        &env.storage.tezos_data_dir,
        &env.protocol_runner,
    ));
    let mut protocol_runner_process = match protocol_runner_endpoint.runner.spawn() {
        Ok(process) => process,
        Err(e) => shutdown_and_exit!(error!(log, "Failed to spawn protocol runner process"; "reason" => e), actor_system),
    };

    // setup tezos ocaml runtime
    let protocol_controller = match protocol_runner_endpoint.commands.accept() {
        Ok(controller) => controller,
        Err(e) => shutdown_and_exit!(error!(log, "Failed to create protocol controller. Reason: {:?}", e), actor_system),
    };
    let tezos_storage_init_info = match protocol_controller.init_protocol() {
        Ok(res) => res,
        Err(err) => shutdown_and_exit!(error!(log, "Failed to initialize Tezos OCaml storage"; "reason" => err), actor_system)
    };
    debug!(log, "Loaded Tezos constants: {:?}", &tezos_storage_init_info);


    // Loads tezos identity based on provided identity-file argument. In case it does not exist, it will try to automatically generate it
    let tezos_identity =
        if env.identity_json_file_path.exists() {
            match identity::load_identity(&env.identity_json_file_path) {
                Ok(identity) => {
                    info!(log, "Identity loaded from file"; "file" => env.identity_json_file_path.clone().into_os_string().into_string().unwrap());
                    identity
                },
                Err(e) => shutdown_and_exit!(error!(log, "Failed to load identity"; "reason" => e, "file" => env.identity_json_file_path.into_os_string().into_string().unwrap()), actor_system),
            }
        } else {
            info!(log, "Generating new tezos identity. This will take a while"; "expected_pow" => EXPECTED_POW);
            match protocol_controller.generate_identity(EXPECTED_POW) {
                Ok(identity) => {
                    info!(log, "Identity successfully generated");
                    match identity::store_identity(&env.identity_json_file_path, &identity) {
                        Ok(()) => {
                            info!(log, "Generated identity stored to file"; "file" => env.identity_json_file_path.clone().into_os_string().into_string().unwrap());
                            identity
                        },
                        Err(e) => shutdown_and_exit!(error!(log, "Failed to store generated identity"; "reason" => e), actor_system),
                    }
                },
                Err(e) => shutdown_and_exit!(error!(log, "Failed to generate identity"; "reason" => e), actor_system),
        }
    };
    drop(protocol_controller);

    let schemas = vec![
        block_storage::BlockIndex::descriptor(),
        block_storage::BlockByLevelIndex::descriptor(),
        BlockMetaStorage::descriptor(),
        OperationsStorage::descriptor(),
        OperationsMetaStorage::descriptor(),
        EventPayloadStorage::descriptor(),
        EventStorage::descriptor(),
        ContextStorage::descriptor(),
        SystemStorage::descriptor(),
    ];
    let rocks_db = match open_db(&env.storage.bootstrap_db_path, schemas) {
        Ok(db) => Arc::new(db),
        Err(_) => shutdown_and_exit!(error!(log, "Failed to create RocksDB database at '{:?}'", &env.storage.bootstrap_db_path), actor_system)
    };
    debug!(log, "Loaded RocksDB database");

    match check_database_compatibility(rocks_db.clone(), &tezos_storage_init_info, log.clone()) {
        Ok(false) => shutdown_and_exit!(crit!(log, "Database incompatibility detected"), actor_system),
        Err(e) => shutdown_and_exit!(error!(log, "Failed to verify database compatibility"; "reason" => e), actor_system),
        _ => ()
    }


    let ProtocolRunnerEndpoint {
        runner: protocol_runner,
        commands: protocol_commands,
        events: protocol_events,
    } = protocol_runner_endpoint;

    let protocol_runner_run = Arc::new(AtomicBool::new(true));
    {
        use std::thread;
        use std::time::Duration;

        let log = log.clone();
        let run = protocol_runner_run.clone();

        let _ = thread::spawn(move || {
            while run.load(Ordering::Acquire) {
                if !ProtocolRunner::is_running(&mut protocol_runner_process) {
                    info!(log, "Starting protocol runner process");
                    protocol_runner_process = match protocol_runner.spawn() {
                        Ok(process) => {
                            info!(log, "Protocol runner started successfully");
                            process
                        },
                        Err(e) => {
                            crit!(log, "Failed to spawn protocol runner process"; "reason" => e);
                            break
                        },
                    };
                }
                thread::sleep(Duration::from_secs(1));
            }

            if ProtocolRunner::is_running(&mut protocol_runner_process) {
                ProtocolRunner::terminate(protocol_runner_process);
            }
        });
    }

    let schemas = vec![
        BlockStorage::descriptor(),
    ];
    let commit_logs = match CommitLogs::new(&env.storage.bootstrap_db_path, &schemas) {
        Ok(commit_logs) => Arc::new(commit_logs),
        Err(e) => shutdown_and_exit!(error!(log, "Failed to open commit logs"; "reason" => e), actor_system)
    };

    match initialize_storage_with_genesis_block(&tezos_storage_init_info.genesis_block_header_hash, &tezos_storage_init_info.genesis_block_header, &tezos_storage_init_info.chain_id, rocks_db.clone(), commit_logs.clone(), log.clone()) {
        Ok(_) => block_on_actors(&env, tezos_identity, actor_system, tezos_storage_init_info, rocks_db.clone(), commit_logs.clone(), protocol_commands, protocol_events, protocol_runner_run, log),
        Err(e) => shutdown_and_exit!(error!(log, "Failed to initialize storage with genesis block. Reason: {}", e), actor_system),
    }

    commit_logs.flush().expect("Failed to flush commit logs");
    rocks_db.flush().expect("Failed to flush database");
}


