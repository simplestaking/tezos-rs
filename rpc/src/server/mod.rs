// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use getset::Getters;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response};
use riker::actors::ActorSystem;
use slog::{error, Logger};

use crypto::hash::{BlockHash, ChainId};
use shell::mempool::mempool_channel::MempoolChannelRef;
use shell::mempool::CurrentMempoolStateStorageRef;
use shell::shell_channel::ShellChannelRef;
use storage::PersistentStorage;
use tezos_api::environment::TezosEnvironmentConfiguration;
use tezos_messages::p2p::encoding::version::NetworkVersion;
use tezos_wrapper::TezedgeContextClient;
use tezos_wrapper::TezosApiConnectionPool;

use crate::rpc_actor::{RpcCollectedStateRef, RpcServerRef};
use crate::{error_with_message, not_found, options};

mod dev_handler;
mod protocol_handler;
mod router;
mod shell_handler;

/// Server environment parameters
#[derive(Getters, Clone)]
pub struct RpcServiceEnvironment {
    #[get = "pub(crate)"]
    sys: ActorSystem,
    #[get = "pub(crate)"]
    actor: RpcServerRef,
    #[get = "pub(crate)"]
    persistent_storage: PersistentStorage,
    #[get = "pub(crate)"]
    current_mempool_state_storage: CurrentMempoolStateStorageRef,
    #[get = "pub(crate)"]
    state: RpcCollectedStateRef,
    #[get = "pub(crate)"]
    shell_channel: ShellChannelRef,
    #[get = "pub(crate)"]
    mempool_channel: MempoolChannelRef,
    #[get = "pub(crate)"]
    tezos_environment: TezosEnvironmentConfiguration,
    #[get = "pub(crate)"]
    network_version: Arc<NetworkVersion>,
    #[get = "pub(crate)"]
    log: Logger,

    #[get = "pub(crate)"]
    main_chain_genesis_hash: BlockHash,
    #[get = "pub(crate)"]
    main_chain_id: ChainId,

    #[get = "pub(crate)"]
    tezedge_context: TezedgeContextClient,
    #[get = "pub(crate)"]
    tezos_readonly_api: Arc<TezosApiConnectionPool>,
    #[get = "pub(crate)"]
    tezos_readonly_prevalidation_api: Arc<TezosApiConnectionPool>,
    #[get = "pub(crate)"]
    tezos_without_context_api: Arc<TezosApiConnectionPool>,
    #[get = "pub(crate)"]
    context_stats_db_path: Option<PathBuf>,
    pub tezedge_is_enabled: bool,
}

impl RpcServiceEnvironment {
    pub fn new(
        sys: ActorSystem,
        actor: RpcServerRef,
        shell_channel: ShellChannelRef,
        mempool_channel: MempoolChannelRef,
        tezos_environment: TezosEnvironmentConfiguration,
        network_version: Arc<NetworkVersion>,
        persistent_storage: &PersistentStorage,
        current_mempool_state_storage: CurrentMempoolStateStorageRef,
        tezos_readonly_api: Arc<TezosApiConnectionPool>,
        tezos_readonly_prevalidation_api: Arc<TezosApiConnectionPool>,
        tezos_without_context_api: Arc<TezosApiConnectionPool>,
        main_chain_id: ChainId,
        main_chain_genesis_hash: BlockHash,
        state: RpcCollectedStateRef,
        context_stats_db_path: Option<PathBuf>,
        tezedge_is_enabled: bool,
        log: &Logger,
    ) -> Self {
        let tezedge_context = TezedgeContextClient::new(Arc::clone(&tezos_readonly_api));
        Self {
            sys,
            actor,
            shell_channel,
            mempool_channel,
            tezos_environment,
            network_version,
            persistent_storage: persistent_storage.clone(),
            current_mempool_state_storage,
            main_chain_id,
            main_chain_genesis_hash,
            state,
            log: log.clone(),
            tezedge_context,
            tezos_readonly_api,
            tezos_readonly_prevalidation_api,
            tezos_without_context_api,
            context_stats_db_path,
            tezedge_is_enabled,
        }
    }
}

pub type Params = Vec<(String, String)>;

pub type Query = HashMap<String, Vec<String>>;

pub type HResult = Result<Response<Body>, Box<dyn std::error::Error + Sync + Send>>;

pub type Handler = Arc<
    dyn Fn(
            Request<Body>,
            Params,
            Query,
            RpcServiceEnvironment,
        ) -> Box<dyn Future<Output = HResult> + Send>
        + Send
        + Sync,
>;

pub struct MethodHandler {
    allowed_methods: Arc<HashSet<Method>>,
    handler: Handler,
}

impl MethodHandler {
    pub fn new(allowed_methods: Arc<HashSet<Method>>, handler: Handler) -> Self {
        Self {
            allowed_methods,
            handler,
        }
    }
}

/// Spawn new HTTP server on given address interacting with specific actor system
pub fn spawn_server(
    bind_address: &SocketAddr,
    env: RpcServiceEnvironment,
) -> impl Future<Output = Result<(), hyper::Error>> {
    let routes = Arc::new(router::create_routes(
        env.state().read().unwrap().is_sandbox(),
        env.tezedge_is_enabled,
    ));

    hyper::Server::bind(bind_address)
        .serve(make_service_fn(move |_| {
            let env = env.clone();
            let routes = routes.clone();

            async move {
                let env = env.clone();
                let routes = routes.clone();
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let env = env.clone();
                    let routes = routes.clone();
                    async move {
                        if let Some((method_and_handler, params)) = routes.find(req.uri().path().to_string().trim_end_matches('/')) {
                            let MethodHandler {
                                allowed_methods,
                                handler,
                            } = method_and_handler;

                            let request_method = req.method();
                            let log = env.log.clone();

                            match *request_method {
                                Method::OPTIONS => {
                                    // lets globaly handle options
                                    options()
                                }
                                _ => {
                                    if allowed_methods.contains(request_method) {
                                        let params: Params = params.into_iter().map(|(param, value)| (param.to_string(), value.to_string())).collect();
                                        let query: Query = req.uri().query().map(parse_query_string).unwrap_or_else(HashMap::new);

                                        let handler = handler.clone();
                                        let fut = handler(req, params, query, env);
                                        match Pin::from(fut).await {
                                            Ok(response) => Ok(response),
                                            Err(e) => {
                                                error!(log, "Failed to execute RPC function - unhandled error"; "reason" => format!("{:?}", &e));
                                                error_with_message(format!("{:?}", e))
                                            }
                                        }
                                    } else {
                                        let error_message = format!("Failed to execute RPC function - Method {} not registered for this RPC function", request_method);
                                        error!(log, "{}", error_message);
                                        error_with_message(format!("{:?}", error_message))
                                    }
                                }
                            }
                        } else {
                            not_found()
                        }
                    }
                }))
            }
        }))
}

/// Helper for parsing URI queries.
/// Functions takes URI query in format `key1=val1&key1=val2&key2=val3`
/// and produces map `{ key1: [val1, val2], key2: [val3] }`
fn parse_query_string(query: &str) -> HashMap<String, Vec<String>> {
    let mut ret: HashMap<String, Vec<String>> = HashMap::new();
    for (key, value) in query.split('&').map(|x| {
        let mut parts = x.split('=');
        (parts.next().unwrap(), parts.next().unwrap_or(""))
    }) {
        if let Some(vals) = ret.get_mut(key) {
            // append value to existing vector
            vals.push(value.to_string());
        } else {
            // create new vector with a single value
            ret.insert(key.to_string(), vec![value.to_string()]);
        }
    }
    ret
}

pub trait HasSingleValue {
    fn get_str(&self, key: &str) -> Option<&str>;

    fn get_u64(&self, key: &str) -> Option<u64> {
        self.get_str(key)
            .and_then(|value| value.parse::<u64>().ok())
    }

    fn get_usize(&self, key: &str) -> Option<usize> {
        self.get_str(key)
            .and_then(|value| value.parse::<usize>().ok())
    }

    fn contains_key(&self, key: &str) -> bool {
        self.get_str(key).is_some()
    }
}

impl HasSingleValue for Params {
    fn get_str(&self, key: &str) -> Option<&str> {
        self.iter()
            .find_map(|(k, v)| if k == key { Some(v.as_str()) } else { None })
    }
}

impl HasSingleValue for Query {
    fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key)
            .map(|values| values.iter().next().map(String::as_str))
            .flatten()
    }
}
