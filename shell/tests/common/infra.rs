// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Module which runs actor's very similar than real node runs

#[cfg(test)]
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use riker::actors::*;
use riker::system::SystemBuilder;
use slog::{info, warn, Level, Logger};
use tezos_api::ffi::TezosContextTezEdgeStorageConfiguration;
use tokio::runtime::Runtime;

use common::contains_all_keys;
use crypto::hash::{BlockHash, OperationHash};
use networking::p2p::network_channel::{NetworkChannel, NetworkChannelRef};
use networking::ShellCompatibilityVersion;
use shell::chain_current_head_manager::ChainCurrentHeadManager;
use shell::chain_feeder::{ChainFeeder, ChainFeederRef};
use shell::chain_manager::{ChainManager, ChainManagerRef};
use shell::mempool::mempool_channel::MempoolChannel;
use shell::mempool::mempool_prevalidator::MempoolPrevalidator;
use shell::mempool::{init_mempool_state_storage, CurrentMempoolStateStorageRef};
use shell::peer_manager::{P2p, PeerManager, PeerManagerRef, WhitelistAllIpAddresses};
use shell::shell_channel::{ShellChannel, ShellChannelRef, ShellChannelTopic, ShuttingDown};
use shell::state::head_state::init_current_head_state;
use shell::state::synchronization_state::{
    init_synchronization_bootstrap_state_storage, SynchronizationBootstrapStateRef,
};
use shell::PeerConnectionThreshold;
use storage::chain_meta_storage::ChainMetaStorageReader;
use storage::tests_common::TmpStorage;
use storage::{resolve_storage_init_chain_data, ChainMetaStorage};
use tezos_api::environment::TezosEnvironmentConfiguration;
use tezos_api::ffi::{
    PatchContext, TezosContextIrminStorageConfiguration, TezosContextStorageConfiguration,
    TezosRuntimeConfiguration,
};
use tezos_identity::Identity;
use tezos_wrapper::ProtocolEndpointConfiguration;
use tezos_wrapper::{TezosApiConnectionPool, TezosApiConnectionPoolConfiguration};

use crate::common;

pub struct NodeInfrastructure {
    name: String,
    pub log: Logger,
    pub peer_manager: Option<PeerManagerRef>,
    pub block_applier: ChainFeederRef,
    pub chain_manager: ChainManagerRef,
    pub shell_channel: ShellChannelRef,
    pub network_channel: NetworkChannelRef,
    pub actor_system: ActorSystem,
    pub tmp_storage: TmpStorage,
    pub current_mempool_state_storage: CurrentMempoolStateStorageRef,
    pub bootstrap_state: SynchronizationBootstrapStateRef,
    pub tezos_env: TezosEnvironmentConfiguration,
    pub tokio_runtime: Runtime,
}

impl NodeInfrastructure {
    pub fn start(
        tmp_storage: TmpStorage,
        context_db_path: &str,
        name: &str,
        tezos_env: &TezosEnvironmentConfiguration,
        patch_context: Option<PatchContext>,
        p2p: Option<(P2p, ShellCompatibilityVersion)>,
        identity: Identity,
        pow_target: f64,
        (log, log_level): (Logger, Level),
        (record_also_readonly_context_action, compute_context_action_tree_hashes): (bool, bool),
    ) -> Result<Self, anyhow::Error> {
        warn!(log, "[NODE] Starting node infrastructure"; "name" => name);

        // environement
        let is_sandbox = false;
        let p2p_threshold = match p2p.as_ref() {
            Some((p2p, _)) => p2p.peer_threshold.clone(),
            None => PeerConnectionThreshold::try_new(1, 1, Some(0))?,
        };
        let identity = Arc::new(identity);

        // storage
        let persistent_storage = tmp_storage.storage();
        let context_db_path = if !PathBuf::from(context_db_path).exists() {
            common::prepare_empty_dir(context_db_path)
        } else {
            context_db_path.to_string()
        };

        let ipc_socket_path = Some(ipc::temp_sock().to_string_lossy().as_ref().to_owned());

        let context_storage_configuration = TezosContextStorageConfiguration::Both(
            TezosContextIrminStorageConfiguration {
                data_dir: context_db_path,
            },
            TezosContextTezEdgeStorageConfiguration {
                backend: tezos_api::ffi::ContextKvStoreConfiguration::InMemGC,
                ipc_socket_path,
            },
        );

        let init_storage_data = resolve_storage_init_chain_data(
            &tezos_env,
            &tmp_storage.path(),
            &context_storage_configuration,
            &patch_context,
            &None,
            &None,
            &log,
        )
        .expect("Failed to resolve init storage chain data");

        // create pool for ffi protocol runner connections (used just for readonly context)
        let tezos_readonly_api = Arc::new(TezosApiConnectionPool::new_with_readonly_context(
            String::from(&format!("{}_readonly_runner_pool", name)),
            TezosApiConnectionPoolConfiguration {
                min_connections: 0,
                max_connections: 2,
                connection_timeout: Duration::from_secs(3),
                max_lifetime: Duration::from_secs(60),
                idle_timeout: Duration::from_secs(60),
            },
            ProtocolEndpointConfiguration::new(
                TezosRuntimeConfiguration {
                    log_enabled: common::is_ocaml_log_enabled(),
                    debug_mode: false,
                    compute_context_action_tree_hashes: false,
                },
                tezos_env.clone(),
                false,
                context_storage_configuration.readonly(),
                &common::protocol_runner_executable_path(),
                log_level,
            ),
            log.clone(),
        )?);

        // create pool for ffi protocol runner connections (used just for readonly context)
        let tezos_writeable_api = Arc::new(TezosApiConnectionPool::new_without_context(
            String::from(&format!("{}_writeable_runner_pool", name)),
            TezosApiConnectionPoolConfiguration {
                min_connections: 0,
                max_connections: 1,
                connection_timeout: Duration::from_secs(3),
                max_lifetime: Duration::from_secs(60),
                idle_timeout: Duration::from_secs(60),
            },
            ProtocolEndpointConfiguration::new(
                TezosRuntimeConfiguration {
                    log_enabled: common::is_ocaml_log_enabled(),
                    debug_mode: record_also_readonly_context_action,
                    compute_context_action_tree_hashes,
                },
                tezos_env.clone(),
                false,
                context_storage_configuration,
                &common::protocol_runner_executable_path(),
                log_level,
            ),
            log.clone(),
        )?);

        let local_current_head_state = init_current_head_state();
        let remote_current_head_state = init_current_head_state();
        let current_mempool_state_storage = init_mempool_state_storage();
        let bootstrap_state = init_synchronization_bootstrap_state_storage(
            p2p_threshold.num_of_peers_for_bootstrap_threshold(),
        );
        let tokio_runtime = create_tokio_runtime();

        // run actor's
        let actor_system = SystemBuilder::new()
            .name(name)
            .log(log.clone())
            .create()
            .expect("Failed to create actor system");
        let shell_channel =
            ShellChannel::actor(&actor_system).expect("Failed to create shell channel");
        let network_channel =
            NetworkChannel::actor(&actor_system).expect("Failed to create network channel");
        let mempool_channel =
            MempoolChannel::actor(&actor_system).expect("Failed to create mempool channel");

        let chain_current_head_manager = ChainCurrentHeadManager::actor(
            &actor_system,
            shell_channel.clone(),
            mempool_channel.clone(),
            persistent_storage.clone(),
            init_storage_data.clone(),
            local_current_head_state.clone(),
            remote_current_head_state.clone(),
            current_mempool_state_storage.clone(),
            bootstrap_state.clone(),
            false,
        )
        .expect("Failed to create chain current head manager");
        let block_applier = ChainFeeder::actor(
            &actor_system,
            chain_current_head_manager,
            shell_channel.clone(),
            persistent_storage.clone(),
            tezos_writeable_api,
            init_storage_data.clone(),
            tezos_env.clone(),
            log.clone(),
        )
        .expect("Failed to create chain feeder");
        let chain_manager = ChainManager::actor(
            &actor_system,
            block_applier.clone(),
            network_channel.clone(),
            shell_channel.clone(),
            mempool_channel.clone(),
            persistent_storage.clone(),
            tezos_readonly_api.clone(),
            init_storage_data.clone(),
            is_sandbox,
            local_current_head_state,
            remote_current_head_state,
            current_mempool_state_storage.clone(),
            bootstrap_state.clone(),
            false,
            identity.clone(),
        )
        .expect("Failed to create chain manager");
        let _ = MempoolPrevalidator::actor(
            &actor_system,
            shell_channel.clone(),
            mempool_channel,
            &persistent_storage,
            current_mempool_state_storage.clone(),
            init_storage_data.chain_id,
            tezos_readonly_api,
            log.clone(),
        )
        .expect("Failed to create chain feeder");

        // and than open p2p and others - if configured
        let peer_manager = if let Some((p2p_config, shell_compatibility_version)) = p2p {
            let peer_manager = PeerManager::actor(
                &actor_system,
                network_channel.clone(),
                shell_channel.clone(),
                tokio_runtime.handle().clone(),
                identity,
                Arc::new(shell_compatibility_version),
                p2p_config,
                pow_target,
            )
            .expect("Failed to create peer manager");
            Some(peer_manager)
        } else {
            None
        };

        Ok(NodeInfrastructure {
            name: String::from(name),
            log,
            peer_manager,
            chain_manager,
            block_applier,
            shell_channel,
            network_channel,
            tokio_runtime,
            actor_system,
            tmp_storage,
            current_mempool_state_storage,
            bootstrap_state,
            tezos_env: tezos_env.clone(),
        })
    }

    fn stop(&mut self) {
        let NodeInfrastructure {
            log,
            shell_channel,
            actor_system,
            tokio_runtime,
            ..
        } = self;
        warn!(log, "[NODE] Stopping node infrastructure"; "name" => self.name.clone());

        // clean up + shutdown events listening
        shell_channel.tell(
            Publish {
                msg: ShuttingDown.into(),
                topic: ShellChannelTopic::ShellShutdown.into(),
            },
            None,
        );

        let _ = tokio_runtime.block_on(async move {
            tokio::time::timeout(Duration::from_secs(10), actor_system.shutdown()).await
        });

        warn!(log, "[NODE] Node infrastructure stopped"; "name" => self.name.clone());
    }

    // TODO: refactor with async/condvar, not to block main thread
    pub fn wait_for_new_current_head(
        &self,
        marker: &str,
        tested_head: BlockHash,
        (timeout, delay): (Duration, Duration),
    ) -> Result<(), anyhow::Error> {
        let start = Instant::now();
        let tested_head = Some(tested_head).map(|th| th.to_base58_check());

        let chain_meta_data = ChainMetaStorage::new(self.tmp_storage.storage());
        let result = loop {
            let current_head = chain_meta_data
                .get_current_head(&self.tezos_env.main_chain_id()?)?
                .map(|ch| ch.block_hash().to_base58_check());

            if current_head.eq(&tested_head) {
                info!(self.log, "[NODE] Expected current head detected"; "head" => tested_head, "marker" => marker);
                break Ok(());
            }

            // kind of simple retry policy
            if start.elapsed().le(&timeout) {
                thread::sleep(delay);
            } else {
                break Err(anyhow::format_err!("wait_for_new_current_head({:?}) - timeout (timeout: {:?}, delay: {:?}) exceeded! marker: {}", tested_head, timeout, delay, marker));
            }
        };
        result
    }

    // TODO: refactor with async/condvar, not to block main thread
    pub fn wait_for_mempool_on_head(
        &self,
        marker: &str,
        tested_head: BlockHash,
        (timeout, delay): (Duration, Duration),
    ) -> Result<(), anyhow::Error> {
        let start = Instant::now();
        let tested_head = Some(tested_head).map(|th| th.to_base58_check());

        let result = loop {
            let mempool_state = self
                .current_mempool_state_storage
                .read()
                .expect("Failed to obtain lock");
            let current_head = mempool_state.head().map(|ch| ch.to_base58_check());

            if current_head.eq(&tested_head) {
                info!(self.log, "[NODE] Expected mempool head detected"; "head" => tested_head, "marker" => marker);
                break Ok(());
            }

            // kind of simple retry policy
            if start.elapsed().le(&timeout) {
                thread::sleep(delay);
            } else {
                break Err(anyhow::format_err!("wait_for_mempool_on_head({:?}) - timeout (timeout: {:?}, delay: {:?}) exceeded! marker: {}", tested_head, timeout, delay, marker));
            }
        };
        result
    }

    // TODO: refactor with async/condvar, not to block main thread
    pub fn wait_for_mempool_contains_operations(
        &self,
        marker: &str,
        expected_operations: &HashSet<OperationHash>,
        (timeout, delay): (Duration, Duration),
    ) -> Result<(), anyhow::Error> {
        let start = Instant::now();

        let result = loop {
            let mempool_state = self
                .current_mempool_state_storage
                .read()
                .expect("Failed to obtain lock");
            if contains_all_keys(mempool_state.operations(), expected_operations) {
                info!(self.log, "[NODE] All expected operations found in mempool"; "marker" => marker);
                break Ok(());
            }
            drop(mempool_state);

            // kind of simple retry policy
            if start.elapsed().le(&timeout) {
                thread::sleep(delay);
            } else {
                break Err(anyhow::format_err!("wait_for_mempool_contains_operations() - timeout (timeout: {:?}, delay: {:?}) exceeded! marker: {}", timeout, delay, marker));
            }
        };
        result
    }

    pub fn whitelist_all(&self) {
        if let Some(peer_manager) = &self.peer_manager {
            peer_manager.tell(WhitelistAllIpAddresses, None);
        }
    }
}

impl Drop for NodeInfrastructure {
    fn drop(&mut self) {
        warn!(self.log, "[NODE] Dropping node infrastructure"; "name" => self.name.clone());
        self.stop();
    }
}

fn create_tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
}

pub mod test_actor {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant};

    use riker::actors::*;

    use crypto::hash::CryptoboxPublicKeyHash;
    use networking::p2p::network_channel::{NetworkChannelMsg, NetworkChannelRef};
    use shell::subscription::subscribe_to_network_events;

    use crate::common::test_node_peer::TestNodePeer;

    #[derive(PartialEq, Eq, Debug)]
    pub enum PeerConnectionStatus {
        Connected,
        Blacklisted,
        Stalled,
    }

    #[actor(NetworkChannelMsg)]
    pub struct NetworkChannelListener {
        peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, PeerConnectionStatus>>>,
        network_channel: NetworkChannelRef,
    }

    pub type NetworkChannelListenerRef = ActorRef<NetworkChannelListenerMsg>;

    impl Actor for NetworkChannelListener {
        type Msg = NetworkChannelListenerMsg;

        fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
            subscribe_to_network_events(&self.network_channel, ctx.myself());
        }

        fn recv(
            &mut self,
            ctx: &Context<Self::Msg>,
            msg: Self::Msg,
            sender: Option<BasicActorRef>,
        ) {
            self.receive(ctx, msg, sender);
        }
    }

    impl
        ActorFactoryArgs<(
            NetworkChannelRef,
            Arc<RwLock<HashMap<CryptoboxPublicKeyHash, PeerConnectionStatus>>>,
        )> for NetworkChannelListener
    {
        fn create_args(
            (network_channel, peers_mirror): (
                NetworkChannelRef,
                Arc<RwLock<HashMap<CryptoboxPublicKeyHash, PeerConnectionStatus>>>,
            ),
        ) -> Self {
            Self {
                network_channel,
                peers_mirror,
            }
        }
    }

    impl Receive<NetworkChannelMsg> for NetworkChannelListener {
        type Msg = NetworkChannelListenerMsg;

        fn receive(&mut self, ctx: &Context<Self::Msg>, msg: NetworkChannelMsg, _sender: Sender) {
            self.process_network_channel_message(ctx, msg)
        }
    }

    impl NetworkChannelListener {
        pub fn name() -> &'static str {
            "network-channel-listener-actor"
        }

        pub fn actor(
            sys: &ActorSystem,
            network_channel: NetworkChannelRef,
            peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, PeerConnectionStatus>>>,
        ) -> Result<NetworkChannelListenerRef, CreateError> {
            Ok(sys.actor_of_props::<NetworkChannelListener>(
                Self::name(),
                Props::new_args((network_channel, peers_mirror)),
            )?)
        }

        fn process_network_channel_message(
            &mut self,
            _: &Context<NetworkChannelListenerMsg>,
            msg: NetworkChannelMsg,
        ) {
            match msg {
                NetworkChannelMsg::PeerMessageReceived(_) => {}
                NetworkChannelMsg::PeerBootstrapped(peer_id, _, _) => {
                    self.peers_mirror.write().unwrap().insert(
                        peer_id.peer_public_key_hash.clone(),
                        PeerConnectionStatus::Connected,
                    );
                }
                NetworkChannelMsg::BlacklistPeer(..) => {}
                NetworkChannelMsg::PeerBlacklisted(peer_id) => {
                    self.peers_mirror.write().unwrap().insert(
                        peer_id.peer_public_key_hash.clone(),
                        PeerConnectionStatus::Blacklisted,
                    );
                }
                _ => (),
            }
        }

        pub fn verify_connected(
            peer: &TestNodePeer,
            peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, PeerConnectionStatus>>>,
        ) -> Result<(), anyhow::Error> {
            Self::verify_state(
                PeerConnectionStatus::Connected,
                peer,
                peers_mirror,
                (Duration::from_secs(5), Duration::from_millis(250)),
            )
        }

        pub fn verify_blacklisted(
            peer: &TestNodePeer,
            peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, PeerConnectionStatus>>>,
        ) -> Result<(), anyhow::Error> {
            Self::verify_state(
                PeerConnectionStatus::Blacklisted,
                peer,
                peers_mirror,
                (Duration::from_secs(5), Duration::from_millis(250)),
            )
        }

        // TODO: refactor with async/condvar, not to block main thread
        fn verify_state(
            expected_state: PeerConnectionStatus,
            peer: &TestNodePeer,
            peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, PeerConnectionStatus>>>,
            (timeout, delay): (Duration, Duration),
        ) -> Result<(), anyhow::Error> {
            let start = Instant::now();
            let peer_public_key_hash = &peer.identity.public_key.public_key_hash()?;

            let result = loop {
                let peers_mirror = peers_mirror.read().unwrap();
                if let Some(peer_state) = peers_mirror.get(peer_public_key_hash) {
                    if peer_state == &expected_state {
                        break Ok(());
                    }
                }

                // kind of simple retry policy
                if start.elapsed().le(&timeout) {
                    std::thread::sleep(delay);
                } else {
                    break Err(
                        anyhow::format_err!(
                            "[{}] verify_state - peer_public_key({}) - (expected_state: {:?}) - timeout (timeout: {:?}, delay: {:?}) exceeded!",
                            peer.name, peer_public_key_hash.to_base58_check(), expected_state, timeout, delay
                        )
                    );
                }
            };
            result
        }
    }
}
