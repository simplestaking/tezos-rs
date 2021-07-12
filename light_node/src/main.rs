// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT
// NOTE: unsafe cannot be forbidden right now because of code in systems.rs
// #![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use riker::actors::*;
use slog::{debug, error, info, warn, Logger};

use crypto::hash::BlockHash;
use monitoring::{Monitor, WebsocketHandler};
use networking::p2p::network_channel::NetworkChannel;
use networking::ShellCompatibilityVersion;
use rpc::rpc_actor::RpcServer;
use shell::mempool::{init_mempool_state_storage, MempoolPrevalidatorFactory};
use shell::peer_manager::PeerManager;
use shell::shell_channel::ShellChannelRef;
use shell::shell_channel::{ShellChannel, ShellChannelTopic, ShuttingDown};
use shell::state::head_state::init_current_head_state;
use shell::state::synchronization_state::init_synchronization_bootstrap_state_storage;
use shell::{chain_current_head_manager::ChainCurrentHeadManager, chain_feeder::ChainFeederRef};
use shell::{chain_feeder::ApplyBlock, chain_manager::ChainManager};
use shell::{chain_feeder::ChainFeeder, state::ApplyBlockBatch};
use storage::persistent::sequence::Sequences;
use storage::persistent::{open_cl, CommitLogSchema};
use storage::{
    initializer::{initialize_rocksdb, GlobalRocksDbCacheHolder, MainChain, RocksDbCache},
    BlockMetaStorage, Replay,
};
use storage::{resolve_storage_init_chain_data, BlockStorage, PersistentStorage, StorageInitInfo};
use tezos_api::environment;
use tezos_api::environment::TezosEnvironmentConfiguration;
use tezos_api::ffi::TezosRuntimeConfiguration;
use tezos_identity::Identity;
use tezos_wrapper::ProtocolEndpointConfiguration;
use tezos_wrapper::TezosApiConnectionPoolError;
use tezos_wrapper::{TezosApiConnectionPool, TezosApiConnectionPoolConfiguration};

use crate::configuration::Environment;
use storage::database::tezedge_database::TezedgeDatabaseBackendConfiguration;
use storage::initializer::initialize_maindb;

mod configuration;
mod identity;
mod system;

extern crate jemallocator;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn create_tokio_runtime(
    env: &crate::configuration::Environment,
) -> std::io::Result<tokio::runtime::Runtime> {
    // use threaded work staling scheduler
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // set number of threads in a thread pool
    if env.tokio_threads > 0 {
        builder.worker_threads(env.tokio_threads);
    }
    // build runtime
    builder.build()
}

/// Create pool for ffi protocol runner connections (used just for readonly context)
/// Connections are created on demand, but depends on [TezosApiConnectionPoolConfiguration][min_connections]
fn create_tezos_readonly_api_pool(
    pool_name: &str,
    pool_cfg: TezosApiConnectionPoolConfiguration,
    env: &crate::configuration::Environment,
    tezos_env: TezosEnvironmentConfiguration,
    log: Logger,
) -> Result<TezosApiConnectionPool, TezosApiConnectionPoolError> {
    TezosApiConnectionPool::new_with_readonly_context(
        String::from(pool_name),
        pool_cfg,
        ProtocolEndpointConfiguration::new(
            TezosRuntimeConfiguration {
                log_enabled: env.logging.ocaml_log_enabled,
                debug_mode: false,
                compute_context_action_tree_hashes: false,
            },
            tezos_env,
            env.enable_testchain,
            env.storage.context_storage_configuration.readonly(),
            &env.ffi.protocol_runner,
            env.logging.level,
        ),
        log,
    )
}

/// Create pool for ffi protocol runner connections (used just for ffi calls which does not need context)
/// Connections are created on demand, but depends on [TezosApiConnectionPoolConfiguration][min_connections]
fn create_tezos_without_context_api_pool(
    pool_name: &str,
    pool_cfg: TezosApiConnectionPoolConfiguration,
    env: &crate::configuration::Environment,
    tezos_env: TezosEnvironmentConfiguration,
    log: Logger,
) -> Result<TezosApiConnectionPool, TezosApiConnectionPoolError> {
    TezosApiConnectionPool::new_without_context(
        String::from(pool_name),
        pool_cfg,
        ProtocolEndpointConfiguration::new(
            TezosRuntimeConfiguration {
                log_enabled: env.logging.ocaml_log_enabled,
                debug_mode: false,
                compute_context_action_tree_hashes: false,
            },
            tezos_env,
            env.enable_testchain,
            env.storage.context_storage_configuration.clone(),
            &env.ffi.protocol_runner,
            env.logging.level,
        ),
        log,
    )
}

/// Create pool for ffi protocol runner connection (used for write to context)
/// There is limitation, that only one write connection to context can be open, so we limit this pool to 1.
fn create_tezos_writeable_api_pool(
    env: &crate::configuration::Environment,
    tezos_env: TezosEnvironmentConfiguration,
    log: Logger,
) -> Result<TezosApiConnectionPool, TezosApiConnectionPoolError> {
    TezosApiConnectionPool::new_without_context(
        String::from("tezos_writeable_api_pool"),
        TezosApiConnectionPoolConfiguration {
            // TODO: hard-coded, not used, make as Optional
            idle_timeout: Duration::from_secs(1800),
            // TODO: hard-coded, not used, make as Optional
            max_lifetime: Duration::from_secs(21600),
            connection_timeout: Duration::from_secs(30),
            min_connections: 0,
            max_connections: 1,
        },
        ProtocolEndpointConfiguration::new(
            TezosRuntimeConfiguration {
                log_enabled: env.logging.ocaml_log_enabled,
                compute_context_action_tree_hashes: env.storage.compute_context_action_tree_hashes,
                debug_mode: false,
            },
            tezos_env,
            env.enable_testchain,
            env.storage.context_storage_configuration.clone(),
            &env.ffi.protocol_runner,
            env.logging.level,
        ),
        log,
    )
}

fn block_on_actors(
    env: crate::configuration::Environment,
    tezos_env: &TezosEnvironmentConfiguration,
    init_storage_data: StorageInitInfo,
    identity: Arc<Identity>,
    persistent_storage: PersistentStorage,
    mut blocks_replay: Option<Vec<Arc<BlockHash>>>,
    log: Logger,
) {
    // if feeding is started, than run chain manager
    let is_sandbox = env.tezos_network == environment::TezosEnvironment::Sandbox;
    // version
    let shell_compatibility_version = Arc::new(ShellCompatibilityVersion::new(
        tezos_env.version.clone(),
        shell::SUPPORTED_DISTRIBUTED_DB_VERSION.to_vec(),
        shell::SUPPORTED_P2P_VERSION.to_vec(),
    ));

    info!(log, "Initializing protocol runners... (4/5)");

    // pool and event server dedicated for applying blocks to chain
    let tezos_writeable_api_pool = Arc::new(
        create_tezos_writeable_api_pool(&env, tezos_env.clone(), log.clone())
            .expect("Failed to initialize writable API pool"),
    );

    // create pool for ffi protocol runner connections (used just for readonly context)
    let tezos_readonly_api_pool = Arc::new(
        create_tezos_readonly_api_pool(
            "tezos_readonly_api_pool",
            env.ffi.tezos_readonly_api_pool.clone(),
            &env,
            tezos_env.clone(),
            log.clone(),
        )
        .expect("Failed to initialize read-only API pool"),
    );
    let tezos_readonly_prevalidation_api_pool = Arc::new(
        create_tezos_readonly_api_pool(
            "tezos_readonly_prevalidation_api",
            env.ffi.tezos_readonly_prevalidation_api_pool.clone(),
            &env,
            tezos_env.clone(),
            log.clone(),
        )
        .expect("Failed to initialize read-only prevalidation API pool"),
    );
    let tezos_without_context_api_pool = Arc::new(
        create_tezos_without_context_api_pool(
            "tezos_without_context_api_pool",
            env.ffi.tezos_without_context_api_pool.clone(),
            &env,
            tezos_env.clone(),
            log.clone(),
        )
        .expect("Failed to initialize API pool without context"),
    );

    info!(log, "Protocol runners initialized");

    info!(log, "Initializing actors... (5/5)");

    // create partial (global) states for sharing between threads/actors
    let local_current_head_state = init_current_head_state();
    let remote_current_head_state = init_current_head_state();
    let current_mempool_state_storage = init_mempool_state_storage();
    let bootstrap_state = init_synchronization_bootstrap_state_storage(
        env.p2p
            .peer_threshold
            .num_of_peers_for_bootstrap_threshold(),
    );

    // create tokio runtime
    let tokio_runtime = create_tokio_runtime(&env).expect("Failed to create tokio runtime");

    // create riker's actor system
    let actor_system = SystemBuilder::new()
        .name("light-node")
        .log(log.clone())
        .create()
        .expect("Failed to create actor system");

    let network_channel =
        NetworkChannel::actor(&actor_system).expect("Failed to create network channel");
    let shell_channel = ShellChannel::actor(&actor_system).expect("Failed to create shell channel");
    let mempool_prevalidator_factory = Arc::new(MempoolPrevalidatorFactory::new(
        shell_channel.clone(),
        persistent_storage.clone(),
        current_mempool_state_storage.clone(),
        tezos_readonly_api_pool.clone(),
        env.p2p.disable_mempool,
    ));

    let chain_current_head_manager = ChainCurrentHeadManager::actor(
        &actor_system,
        shell_channel.clone(),
        persistent_storage.clone(),
        init_storage_data.clone(),
        local_current_head_state.clone(),
        remote_current_head_state.clone(),
        current_mempool_state_storage.clone(),
        bootstrap_state.clone(),
        mempool_prevalidator_factory.clone(),
    )
    .expect("Failed to create chain current head manager");
    let block_applier = ChainFeeder::actor(
        &actor_system,
        chain_current_head_manager,
        shell_channel.clone(),
        persistent_storage.clone(),
        tezos_writeable_api_pool.clone(),
        init_storage_data.clone(),
        tezos_env.clone(),
        log.clone(),
    )
    .expect("Failed to create chain feeder");
    let _ = ChainManager::actor(
        &actor_system,
        block_applier.clone(),
        network_channel.clone(),
        shell_channel.clone(),
        persistent_storage.clone(),
        tezos_readonly_prevalidation_api_pool.clone(),
        init_storage_data.clone(),
        is_sandbox,
        local_current_head_state,
        remote_current_head_state,
        current_mempool_state_storage.clone(),
        bootstrap_state,
        mempool_prevalidator_factory,
        identity.clone(),
    )
    .expect("Failed to create chain manager");

    // Only start Monitoring when websocket is set
    if let Some(websocket_address) = env.rpc.websocket_address {
        let websocket_handler = WebsocketHandler::actor(
            &actor_system,
            tokio_runtime.handle().clone(),
            websocket_address,
            log.clone(),
        ).expect("Failed to start websocket actor");

        let _ = Monitor::actor(
            &actor_system,
            network_channel.clone(),
            websocket_handler,
            shell_channel.clone(),
            persistent_storage.clone(),
            init_storage_data.chain_id.clone(),
        ).expect("Failed to create monitor actor");
    }

    let _ = RpcServer::actor(
        &actor_system,
        shell_channel.clone(),
        ([0, 0, 0, 0], env.rpc.listener_port).into(),
        tokio_runtime.handle().clone(),
        &persistent_storage,
        current_mempool_state_storage,
        tezos_readonly_api_pool.clone(),
        tezos_readonly_prevalidation_api_pool.clone(),
        tezos_without_context_api_pool.clone(),
        tezos_env.clone(),
        Arc::new(shell_compatibility_version.to_network_version()),
        &init_storage_data,
        env.storage
            .context_storage_configuration
            .tezedge_is_enabled(),
    )
    .expect("Failed to create RPC server");

    if let Some(blocks) = blocks_replay.take() {
        return schedule_replay_blocks(
            blocks,
            &init_storage_data,
            block_applier,
            shell_channel.clone(),
            log.clone(),
        );
    } else {
        // TODO: TE-386 - controlled startup
        std::thread::sleep(std::time::Duration::from_secs(2));

        // and than open p2p and others
        let _ = PeerManager::actor(
            &actor_system,
            network_channel,
            shell_channel.clone(),
            tokio_runtime.handle().clone(),
            identity,
            shell_compatibility_version,
            env.p2p,
            env.identity.expected_pow,
        )
        .expect("Failed to create peer manager");
    }

    info!(log, "Actors initialized");

    tokio_runtime.block_on(async move {
        use tokio::signal;
        use tokio::time::timeout;

        signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c event");
        info!(log, "Ctrl-c or SIGINT received!");

        info!(log, "Sending shutdown notification to actors (1/5)");
        shell_channel.tell(
            Publish {
                msg: ShuttingDown.into(),
                topic: ShellChannelTopic::ShellShutdown.into(),
            },
            None,
        );

        // give actors some time to shut down
        tokio::time::sleep(Duration::from_secs(2)).await;

        info!(log, "Shutting down actors (2/5)");
        match timeout(Duration::from_secs(10), actor_system.shutdown()).await {
            Ok(_) => info!(log, "Shutdown actors complete"),
            Err(_) => info!(log, "Shutdown actors did not finish to timeout (10s)"),
        };

        info!(log, "Shutting down protocol runner pools (3/5)");
        drop(tezos_readonly_api_pool);
        drop(tezos_readonly_prevalidation_api_pool);
        drop(tezos_without_context_api_pool);
        drop(tezos_writeable_api_pool);
        debug!(log, "Protocol runners completed");

        info!(log, "Flushing databases (4/5)");
        drop(persistent_storage);
        info!(log, "Databases flushed");

        info!(log, "Shutdown complete (5/5)");
    });
}

fn check_deprecated_network(env: &Environment, log: &Logger) {
    if let Some(deprecation_notice) = env.tezos_network.check_deprecated_network() {
        warn!(log, "Deprecated network: {}", deprecation_notice);
    }
}

fn schedule_replay_blocks(
    blocks: Vec<Arc<BlockHash>>,
    init_storage_data: &StorageInitInfo,
    block_applier: ChainFeederRef,
    shell_channel: ShellChannelRef,
    log: Logger,
) {
    let chain_id = Arc::new(init_storage_data.chain_id.clone());
    let (result_callback_sender, result_callback_receiver) = {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        (Arc::new(sender), receiver)
    };
    let fail_above = init_storage_data.replay.as_ref().unwrap().fail_above;
    let nblocks = blocks.len();
    let now = std::time::Instant::now();

    for (index, block) in blocks.into_iter().enumerate() {
        let batch = ApplyBlockBatch::one((&*block).clone());
        let result_callback = Some(result_callback_sender.clone());

        let now = std::time::Instant::now();
        block_applier.tell(
            ApplyBlock::new(Arc::clone(&chain_id), batch, result_callback, None, None),
            None,
        );

        let result = result_callback_receiver
            .recv()
            .expect("Failed to wait for block aplication");
        let time = now.elapsed();

        let hash = block.to_base58_check();
        let percent = (index as f64 / nblocks as f64) * 100.0;

        if let Err(_) = result.as_ref() {
            replay_shutdown(shell_channel);
            panic!(
                "{:08} {:.5}% Block {} failed in {:?}. Result={:?}",
                index, percent, hash, time, result
            );
        } else if time > fail_above && index > 0 {
            replay_shutdown(shell_channel);
            panic!(
                "{:08} {:.5}% Block {} processed in {:?} (more than {:?}). Result={:?}",
                index, percent, hash, time, fail_above, result
            );
        } else {
            info!(
                log,
                "{:08} {:.5}% Block {} applied in {:?}. Result={:?}",
                index,
                percent,
                hash,
                time,
                result
            );
        }
    }

    info!(
        log,
        "Replayer successfully applied {} blocks in {:?}",
        nblocks,
        now.elapsed()
    );

    replay_shutdown(shell_channel);
}

fn replay_shutdown(shell_channel: ShellChannelRef) {
    shell_channel.tell(
        Publish {
            msg: ShuttingDown.into(),
            topic: ShellChannelTopic::ShellShutdown.into(),
        },
        None,
    );

    // give actors some time to shut down
    std::thread::sleep(Duration::from_secs(2));
}

fn collect_replayed_blocks(
    persistent_storage: &PersistentStorage,
    replay: &Replay,
    log: &Logger,
) -> Vec<Arc<BlockHash>> {
    let block_meta_storage = BlockMetaStorage::new(&persistent_storage);
    let from_block = replay.from_block.as_ref();
    let mut block_hash = replay.to_block.clone();
    let mut blocks = Vec::new();
    let mut first_block_reached = false;

    info!(log, "Replayer is collecting blocks");

    if block_meta_storage
        .get(&block_hash)
        .map(|h| h.is_none())
        .unwrap_or(true)
    {
        panic!("Unable to find block {:?}", block_hash.to_base58_check());
    }

    while let Ok(Some(block_meta)) = block_meta_storage.get(&block_hash) {
        blocks.push(Arc::new(block_hash.clone()));

        first_block_reached = block_meta.level() == 1;
        if from_block == Some(&block_hash) || first_block_reached {
            break;
        }

        block_hash = match block_meta.predecessor() {
            Some(block) => block.clone(),
            None => {
                panic!(
                    "Unable to find predecessor of level={:?} block_hash={:?}",
                    block_meta.level(),
                    block_hash.to_base58_check()
                );
            }
        }
    }

    blocks.reverse();
    match (from_block, blocks.get(0)) {
        (Some(block), first) if blocks.is_empty() || block != &**first.unwrap() => {
            panic!("Unable to find block {:?}", block);
        }
        (None, _) if !first_block_reached => {
            panic!("Unable to find first block");
        }
        _ => {}
    }

    info!(log, "Replayer will apply {} blocks", blocks.len());

    blocks
}

fn main() {
    // Parses config + cli args
    let env = crate::configuration::Environment::from_args();
    let tezos_env = environment::TEZOS_ENV
        .get(&env.tezos_network)
        .unwrap_or_else(|| {
            panic!(
                "No tezos environment version configured for: {:?}",
                env.tezos_network
            )
        });

    // Creates loggers
    let log = match env.create_logger() {
        Ok(log) => log,
        Err(e) => panic!(
            "Error while creating loggers, check '--log' argument, reason: {:?}",
            e
        ),
    };

    // check deprecated networks
    info!(
        log,
        "Configured network {:?} -> {}",
        env.tezos_network.supported_values(),
        tezos_env.version
    );
    check_deprecated_network(&env, &log);

    // Validate zcash-params
    info!(log, "Checking zcash-params for sapling... (1/5)");
    if let Err(e) = env.ffi.zcash_param.assert_zcash_params(&log) {
        let description = env.ffi.zcash_param.description("'--init-sapling-spend-params-file=<spend-file-path>' / '--init-sapling-output-params-file=<output-file-path'");
        error!(log, "Failed to validate zcash-params required for sapling support"; "description" => description.clone(), "reason" => format!("{}", e));
        panic!(
            "Failed to validate zcash-params required for sapling support, reason: {}, description: {}",
            e, description
        );
    }

    // Loads tezos identity based on provided identity-file argument. In case it does not exist, it will try to automatically generate it
    info!(log, "Loading identity... (2/5)");
    let tezos_identity = match identity::ensure_identity(&env.identity, &log) {
        Ok(identity) => {
            info!(log, "Identity loaded from file"; "file" => env.identity.identity_json_file_path.as_path().display().to_string());
            if env.validate_cfg_identity_and_stop {
                info!(log, "Configuration and identity is ok!");
                return;
            }
            identity
        }
        Err(e) => {
            error!(log, "Failed to load identity"; "reason" => format!("{}", e), "file" => env.identity.identity_json_file_path.as_path().display().to_string());
            panic!(
                "Failed to load identity: {}",
                env.identity
                    .identity_json_file_path
                    .as_path()
                    .display()
                    .to_string()
            );
        }
    };

    // Enable core dumps and increase open files limit
    system::init_limits(&log);

    // create/initialize databases
    info!(log, "Loading databases... (3/5)");

    // create common RocksDB block cache to be shared among column families
    // IMPORTANT: Cache object must live at least as long as DB (returned by open_kv)
    let mut caches = GlobalRocksDbCacheHolder::with_capacity(3);
    let main_chain = MainChain::new(
        tezos_env.main_chain_id().expect("Failed to decode chainId"),
        tezos_env.version.clone(),
    );

    // initialize dbs
    let kv_cache = RocksDbCache::new_lru_cache(env.storage.db.cache_size)
        .expect("Failed to initialize RocksDB cache (db)");

    let maindb = match env.storage.main_db {
        TezedgeDatabaseBackendConfiguration::Sled => initialize_maindb(
            &log,
            None,
            &env.storage.db,
            env.storage.db.expected_db_version,
            &main_chain,
            env.storage.main_db,
        )
        .expect("Failed to create/initialize MainDB database (db)"),
        TezedgeDatabaseBackendConfiguration::RocksDB => {
            let kv = initialize_rocksdb(&log, &kv_cache, &env.storage.db, &main_chain)
                .expect("Failed to create/initialize RocksDB database (db)");
            caches.push(kv_cache);
            initialize_maindb(
                &log,
                Some(kv),
                &env.storage.db,
                env.storage.db.expected_db_version,
                &main_chain,
                env.storage.main_db,
            )
            .expect("Failed to create/initialize MainDB database (db)")
        }
    };

    let commit_logs = Arc::new(
        open_cl(&env.storage.db_path, vec![BlockStorage::descriptor()])
            .expect("Failed to open plain block_header storage"),
    );
    let sequences = Arc::new(Sequences::new(maindb.clone(), 1000));

    {
        let persistent_storage = PersistentStorage::new(maindb, commit_logs, sequences);

        match resolve_storage_init_chain_data(
            &tezos_env,
            &env.storage.db_path,
            &env.storage.context_storage_configuration,
            &env.storage.patch_context,
            &env.storage.context_stats_db_path,
            &env.replay,
            &log,
        ) {
            Ok(init_data) => {
                let blocks_replay = env
                    .replay
                    .as_ref()
                    .map(|replay| collect_replayed_blocks(&persistent_storage, replay, &log));

                info!(log, "Databases loaded successfully");
                block_on_actors(
                    env,
                    tezos_env,
                    init_data,
                    Arc::new(tezos_identity),
                    persistent_storage,
                    blocks_replay,
                    log,
                )
            }
            Err(e) => panic!("Failed to resolve init storage chain data, reason: {}", e),
        }
    }
}
