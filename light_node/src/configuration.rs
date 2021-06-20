// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{self, BufRead};
use std::net::SocketAddr;
use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, collections::HashSet, fmt::Debug};

use clap::{App, Arg};
use slog::{Drain, Duplicate, Logger, Never, SendSyncRefUnwindSafeDrain};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use thiserror::Error;

use crypto::hash::BlockHash;
use logging::detailed_json;
use logging::file::FileAppenderBuilder;
use shell::peer_manager::P2p;
use shell::PeerConnectionThreshold;
use storage::initializer::{DbsRocksDbTableInitializer, RocksDbConfig};
use storage::Replay;
use tezos_api::environment;
use tezos_api::environment::{TezosEnvironment, ZcashParams};
use tezos_api::ffi::TezosContextTezEdgeStorageConfiguration;
use tezos_api::ffi::{
    PatchContext, TezosContextIrminStorageConfiguration, TezosContextStorageConfiguration,
};
use tezos_new_context::actions::ContextActionStoreBackend;
use tezos_new_context::initializer::ContextKvStoreConfiguration;
use tezos_new_context::kv_store::SupportedContextKeyValueStore;
use tezos_wrapper::TezosApiConnectionPoolConfiguration;

macro_rules! create_terminal_logger {
    ($type:expr) => {{
        match $type {
            LogFormat::Simple => slog_async::Async::new(
                slog_term::FullFormat::new(slog_term::TermDecorator::new().build())
                    .build()
                    .fuse(),
            )
            .chan_size(32768)
            .overflow_strategy(slog_async::OverflowStrategy::Block)
            .build(),
            LogFormat::Json => {
                slog_async::Async::new(detailed_json::default(std::io::stdout()).fuse())
                    .chan_size(32768)
                    .overflow_strategy(slog_async::OverflowStrategy::Block)
                    .build()
            }
        }
    }};
}

macro_rules! create_file_logger {
    ($type:expr, $path:expr) => {{
        let appender = FileAppenderBuilder::new($path)
            .rotate_size(10_485_760 * 10) // 100 MB
            .rotate_keep(2)
            .rotate_compress(true)
            .build();

        match $type {
            LogFormat::Simple => slog_async::Async::new(
                slog_term::FullFormat::new(slog_term::PlainDecorator::new(appender))
                    .build()
                    .fuse(),
            )
            .chan_size(32768)
            .overflow_strategy(slog_async::OverflowStrategy::Block)
            .build(),
            LogFormat::Json => slog_async::Async::new(detailed_json::default(appender).fuse())
                .chan_size(32768)
                .overflow_strategy(slog_async::OverflowStrategy::Block)
                .build(),
        }
    }};
}

#[derive(Debug, Clone)]
pub struct Rpc {
    pub listener_port: u16,
    pub websocket_address: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct Logging {
    pub log: Vec<LoggerType>,
    pub ocaml_log_enabled: bool,
    pub level: slog::Level,
    pub format: LogFormat,
    pub file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ParseLoggerTypeError(String);

impl FromStr for LoggerType {
    type Err = ParseLoggerTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        for sp in LoggerType::iter() {
            if sp.supported_values().contains(&s.as_str()) {
                return Ok(sp);
            }
        }

        Err(ParseLoggerTypeError(format!("Invalid variant name: {}", s)))
    }
}

#[derive(PartialEq, Debug, Clone, EnumIter)]
pub enum LoggerType {
    TerminalLogger,
    FileLogger,
}

impl MultipleValueArg for LoggerType {
    fn supported_values(&self) -> Vec<&'static str> {
        match self {
            LoggerType::TerminalLogger => vec!["terminal"],
            LoggerType::FileLogger => vec!["file"],
        }
    }
}

#[derive(Debug, Error)]
#[error("No logger target was provided")]
pub struct NoDrainError;

#[derive(Debug, Clone)]
pub struct ParseTezosContextStorageChoiceError(String);

enum TezosContextStorageChoice {
    Irmin,
    TezEdge,
    Both,
}

impl FromStr for TezosContextStorageChoice {
    type Err = ParseTezosContextStorageChoiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_ref() {
            "both" => Ok(TezosContextStorageChoice::Both),
            "irmin" => Ok(TezosContextStorageChoice::Irmin),
            "tezedge" => Ok(TezosContextStorageChoice::TezEdge),
            _ => Err(ParseTezosContextStorageChoiceError(format!(
                "Invalid context storage name: {}",
                s
            ))),
        }
    }
}

pub trait MultipleValueArg: IntoEnumIterator {
    fn possible_values() -> Vec<&'static str> {
        let mut possible_values = Vec::new();
        for sp in Self::iter() {
            possible_values.extend(sp.supported_values());
        }
        possible_values
    }
    fn supported_values(&self) -> Vec<&'static str>;
}

#[derive(Debug, Clone)]
pub struct Storage {
    pub db: RocksDbConfig<DbsRocksDbTableInitializer>,
    pub db_path: PathBuf,
    pub context_stats_db_path: Option<PathBuf>,
    pub context_storage_configuration: TezosContextStorageConfiguration,
    pub compute_context_action_tree_hashes: bool,
    pub patch_context: Option<PatchContext>,
}

impl Storage {
    const STORAGES_COUNT: usize = 3;
    const MINIMAL_THREAD_COUNT: usize = 1;

    const DB_STORAGE_VERSION: i64 = 19;

    const LRU_CACHE_SIZE_96MB: usize = 96 * 1024 * 1024;

    const DEFAULT_CONTEXT_KV_STORE_BACKEND: &'static str = tezos_new_context::kv_store::INMEMGC;
}

#[derive(Debug, Clone)]
pub struct Identity {
    pub identity_json_file_path: PathBuf,
    pub expected_pow: f64,
}

#[derive(Debug, Clone)]
pub struct Ffi {
    pub protocol_runner: PathBuf,
    pub tezos_readonly_api_pool: TezosApiConnectionPoolConfiguration,
    pub tezos_readonly_prevalidation_api_pool: TezosApiConnectionPoolConfiguration,
    pub tezos_without_context_api_pool: TezosApiConnectionPoolConfiguration,
    pub zcash_param: ZcashParams,
}

impl Ffi {
    const TEZOS_READONLY_API_POOL_DISCRIMINATOR: &'static str = "";
    const TEZOS_READONLY_PREVALIDATION_API_POOL_DISCRIMINATOR: &'static str = "trpap";
    const TEZOS_WITHOUT_CONTEXT_API_POOL_DISCRIMINATOR: &'static str = "twcap";

    pub const DEFAULT_ZCASH_PARAM_SAPLING_SPEND_FILE_PATH: &'static str =
        "tezos/sys/lib_tezos/artifacts/sapling-spend.params";
    pub const DEFAULT_ZCASH_PARAM_SAPLING_OUTPUT_FILE_PATH: &'static str =
        "tezos/sys/lib_tezos/artifacts/sapling-output.params";
}

#[derive(Debug, Clone)]
pub enum LogFormat {
    Json,
    Simple,
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "simple" => Ok(LogFormat::Simple),
            "json" => Ok(LogFormat::Json),
            _ => Err(format!("Unsupported variant: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Environment {
    pub p2p: P2p,
    pub rpc: Rpc,
    pub logging: Logging,
    pub storage: Storage,
    pub identity: Identity,
    pub ffi: Ffi,
    pub replay: Option<Replay>,

    pub tezos_network: TezosEnvironment,
    pub enable_testchain: bool,
    pub tokio_threads: usize,

    /// This flag is used, just for to stop node immediatelly after generate identity,
    /// to prevent and initialize actors and create data (except identity)
    pub validate_cfg_identity_and_stop: bool,
}

macro_rules! parse_validator_fn {
    ($t:ident, $err:expr) => {
        |v| {
            if v.parse::<$t>().is_ok() {
                Ok(())
            } else {
                Err($err.to_string())
            }
        }
    };
}

// Creates tezos app
pub fn tezos_app() -> App<'static, 'static> {
    // Default values for arguments are specidied in default configuration file
    //
    // Flag Required=true must be handled separately as we parse args twice,
    // once to see only if confi-file arg is present and second time to parse all args
    //
    // In case some args are required=true and user provides only config-file,
    // first round of parsing would always fail then
    let app = App::new("TezEdge Light Node")
        .version(env!("CARGO_PKG_VERSION"))
        .author("TezEdge and the project contributors")
        .about("Rust implementation of the Tezos node")
        .setting(clap::AppSettings::AllArgsOverrideSelf)
        .arg(Arg::with_name("validate-cfg-identity-and-stop")
            .long("validate-cfg-identity-and-stop")
            .global(true)
            .takes_value(false)
            .help("Validate configuration and generated identity, than just stops application"))
        .arg(Arg::with_name("config-file")
            .long("config-file")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("Configuration file with start-up arguments (same format as cli arguments)")
            .validator(|v| if Path::new(&v).exists() { Ok(()) } else { Err(format!("Configuration file not found at '{}'", v)) }))
        .arg(Arg::with_name("tezos-context-storage")
            .long("tezos-context-storage")
            .global(true)
            .takes_value(true)
            .value_name("NAME")
            .help("Context storage to use (irmin/tezedge/both)"))
        .arg(Arg::with_name("tezos-data-dir")
            .long("tezos-data-dir")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("A directory for Tezos OCaml runtime storage (context/store)")
            .validator(|v| {
                let dir = Path::new(&v);
                if dir.exists() {
                    if dir.is_dir() {
                        Ok(())
                    } else {
                        Err(format!("Required tezos data dir '{}' exists, but is not a directory!", v))
                    }
                } else {
                    // Tezos data dir does not exists, try to create it
                    if let Err(e) = fs::create_dir_all(dir) {
                        Err(format!("Unable to create required tezos data dir '{}': {} ", v, e))
                    } else {
                        Ok(())
                    }
                }
            }))
        .arg(Arg::with_name("identity-file")
            .long("identity-file")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("Path to the json identity file with peer-id, public-key, secret-key and pow-stamp.
                       In case it starts with ./ or ../, it is relative path to the current dir, otherwise to the --tezos-data-dir"))
        .arg(Arg::with_name("identity-expected-pow")
            .long("identity-expected-pow")
            .global(true)
            .takes_value(true)
            .value_name("NUM")
            .help("Expected power of identity for node. It is used to generate new identity. Default: 26.0")
            .validator(parse_validator_fn!(f64, "Value must be a valid f64 number for expected_pow")))
        .arg(Arg::with_name("bootstrap-db-path")
            .long("bootstrap-db-path")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("Path to bootstrap database directory.
                       In case it starts with ./ or ../, it is relative path to the current dir, otherwise to the --tezos-data-dir"))
        .arg(Arg::with_name("context-stats-db-path")
            .long("context-stats-db-path")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("Path to context-stats database directory.
                       In case it starts with ./ or ../, it is relative path to the current dir, otherwise to the --tezos-data-dir"))
        .arg(Arg::with_name("db-cfg-max-threads")
            .long("db-cfg-max-threads")
            .global(true)
            .takes_value(true)
            .value_name("NUM")
            .help("Max number of threads used by database configuration. If not specified, then number of threads equal to CPU cores.")
            .validator(parse_validator_fn!(usize, "Value must be a valid number")))
        .arg(Arg::with_name("db-context-cfg-max-threads")
            .long("db-context-cfg-max-threads")
            .global(true)
            .takes_value(true)
            .value_name("NUM")
            .help("Max number of threads used by database configuration. If not specified, then number of threads equal to CPU cores.")
            .validator(parse_validator_fn!(usize, "Value must be a valid number")))
        .arg(Arg::with_name("db-context-actions-cfg-max-threads")
            .long("db-context-actions-cfg-max-threads")
            .global(true)
            .takes_value(true)
            .value_name("NUM")
            .help("Max number of threads used by database configuration. If not specified, then number of threads equal to CPU cores.")
            .validator(parse_validator_fn!(usize, "Value must be a valid number")))
        .arg(Arg::with_name("bootstrap-lookup-address")
            .long("bootstrap-lookup-address")
            .global(true)
            .takes_value(true)
            .conflicts_with("peers")
            .conflicts_with("private-node")
            .help("A peers for dns lookup to get the peers to bootstrap the network from. Peers are delimited by a colon. Default: used according to --network parameter see TezosEnvironment"))
        .arg(Arg::with_name("disable-bootstrap-lookup")
            .long("disable-bootstrap-lookup")
            .global(true)
            .takes_value(false)
            .conflicts_with("bootstrap-lookup-address")
            .help("Disables dns lookup to get the peers to bootstrap the network from. Default: false"))
        .arg(Arg::with_name("log")
            .long("log")
            .global(true)
            .takes_value(true)
            .multiple(true)
            .value_name("STRING")
            .possible_values(&LoggerType::possible_values())
            .help("Set the logger target. Default: terminal"))
        .arg(Arg::with_name("log-file")
            .long("log-file")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("Path to the log file. If provided, logs are displayed the log file, otherwise in terminal.
                       In case it starts with ./ or ../, it is relative path to the current dir, otherwise to the --tezos-data-dir"))
        .arg(Arg::with_name("log-format")
            .long("log-format")
            .global(true)
            .takes_value(true)
            .possible_values(&["json", "simple"])
            .help("Set output format of the log"))
        .arg(Arg::with_name("log-level")
            .long("log-level")
            .global(true)
            .takes_value(true)
            .value_name("LEVEL")
            .possible_values(&["critical", "error", "warn", "info", "debug", "trace"])
            .help("Set log level"))
        .arg(Arg::with_name("ocaml-log-enabled")
            .long("ocaml-log-enabled")
            .global(true)
            .takes_value(true)
            .value_name("BOOL")
            .help("Flag for turn on/off logging in Tezos OCaml runtime"))
        .arg(Arg::with_name("disable-mempool")
            .long("disable-mempool")
            .global(true)
            .help("Enable or disable mempool"))
        .arg(Arg::with_name("private-node")
            .long("private-node")
            .global(true)
            .takes_value(true)
            .value_name("BOOL")
            .requires("peers")
            .conflicts_with("bootstrap-lookup-address")
            .help("Enable or disable private node. Use peers to set IP addresses of the peers you want to connect to"))
        .arg(Arg::with_name("network")
            .long("network")
            .global(true)
            .takes_value(true)
            .possible_values(&TezosEnvironment::possible_values())
            .help("Choose the Tezos environment")
        )
        .arg(Arg::with_name("p2p-port")
            .long("p2p-port")
            .global(true)
            .takes_value(true)
            .value_name("PORT")
            .help("Socket listening port for p2p for communication with tezos world")
            .validator(parse_validator_fn!(u16, "Value must be a valid port number")))
        .arg(Arg::with_name("rpc-port")
            .long("rpc-port")
            .global(true)
            .takes_value(true)
            .value_name("PORT")
            .help("Rust server RPC port for communication with rust node")
            .validator(parse_validator_fn!(u16, "Value must be a valid port number")))
        .arg(Arg::with_name("enable-testchain")
            .long("enable-testchain")
            .global(true)
            .takes_value(true)
            .value_name("BOOL")
            .help("Flag for enable/disable test chain switching for block applying. Default: false"))
        .arg(Arg::with_name("websocket-address")
            .long("websocket-address")
            .global(true)
            .takes_value(true)
            .value_name("IP:PORT")
            .help("Websocket address where various node metrics and statistics are available")
            .validator(parse_validator_fn!(SocketAddr, "Value must be a valid IP:PORT")))
        .arg(Arg::with_name("peers")
            .long("peers")
            .global(true)
            .takes_value(true)
            .value_name("IP:PORT")
            .help("A peer to bootstrap the network from. Peers are delimited by a colon. Format: IP1:PORT1,IP2:PORT2,IP3:PORT3")
            .validator(|v| {
                let err_count = v.split(',')
                    .map(|ip_port| ip_port.parse::<SocketAddr>())
                    .filter(|v| v.is_err())
                    .count();
                if err_count == 0 {
                    Ok(())
                } else {
                    Err(format!("Value '{}' is not valid. Expected format is: IP1:PORT1,IP2:PORT2,IP3:PORT3", v))
                }
            }))
        .arg(Arg::with_name("peer-thresh-low")
            .long("peer-thresh-low")
            .global(true)
            .takes_value(true)
            .value_name("NUM")
            .help("Minimal number of peers to connect to")
            .validator(parse_validator_fn!(usize, "Value must be a valid number")))
        .arg(Arg::with_name("peer-thresh-high")
            .long("peer-thresh-high")
            .global(true)
            .takes_value(true)
            .value_name("NUM")
            .help("Maximal number of peers to connect to")
            .validator(parse_validator_fn!(usize, "Value must be a valid number")))
        .arg(Arg::with_name("synchronization-thresh")
            .long("synchronization-thresh")
            .global(true)
            .takes_value(true)
            .value_name("NUM")
            .help("Maximal number of peers to connect to")
            .validator(parse_validator_fn!(usize, "Value must be a valid number")))
        .arg(Arg::with_name("protocol-runner")
            .long("protocol-runner")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("Path to a tezos protocol runner executable")
            .validator(|v| if Path::new(&v).exists() { Ok(()) } else { Err(format!("Tezos protocol runner executable not found at '{}'", v)) }))
        .args(
            &[
                Arg::with_name("ffi-pool-max-connections")
                    .long("ffi-pool-max-connections")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of max ffi pool connections, default: 10")
                    .validator(parse_validator_fn!(u8, "Value must be a valid number")),
                Arg::with_name("ffi-pool-connection-timeout-in-secs")
                    .long("ffi-pool-connection-timeout-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to wait for connection, default: 60")
                    .validator(parse_validator_fn!(u16, "Value must be a valid number")),
                Arg::with_name("ffi-pool-max-lifetime-in-secs")
                    .long("ffi-pool-max-lifetime-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to remove protocol_runner from pool, default: 21600 means 6 hours")
                    .validator(parse_validator_fn!(u64, "Value must be a valid number")),
                Arg::with_name("ffi-pool-idle-timeout-in-secs")
                    .long("ffi-pool-idle-timeout-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to remove unused protocol_runner from pool, default: 1800 means 30 minutes")
                    .validator(parse_validator_fn!(u64, "Value must be a valid number"))
            ])
        .args(
            &[
                Arg::with_name("ffi-trpap-pool-max-connections")
                    .long("ffi-trpap-pool-max-connections")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of max ffi pool connections, default: 10")
                    .validator(parse_validator_fn!(u8, "Value must be a valid number")),
                Arg::with_name("ffi-trpap-pool-connection-timeout-in-secs")
                    .long("ffi-trpap-pool-connection-timeout-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to wait for connection, default: 60")
                    .validator(parse_validator_fn!(u16, "Value must be a valid number")),
                Arg::with_name("ffi-trpap-pool-max-lifetime-in-secs")
                    .long("ffi-trpap-pool-max-lifetime-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to remove protocol_runner from pool, default: 21600 means 6 hours")
                    .validator(parse_validator_fn!(u64, "Value must be a valid number")),
                Arg::with_name("ffi-trpap-pool-idle-timeout-in-secs")
                    .long("ffi-trpap-pool-idle-timeout-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to remove unused protocol_runner from pool, default: 1800 means 30 minutes")
                    .validator(parse_validator_fn!(u64, "Value must be a valid number"))
            ])
        .args(
            &[
                Arg::with_name("ffi-twcap-pool-max-connections")
                    .long("ffi-twcap-pool-max-connections")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of max ffi pool connections, default: 10")
                    .validator(parse_validator_fn!(u8, "Value must be a valid number")),
                Arg::with_name("ffi-twcap-pool-connection-timeout-in-secs")
                    .long("ffi-twcap-pool-connection-timeout-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to wait for connection, default: 60")
                    .validator(parse_validator_fn!(u16, "Value must be a valid number")),
                Arg::with_name("ffi-twcap-pool-max-lifetime-in-secs")
                    .long("ffi-twcap-pool-max-lifetime-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to remove protocol_runner from pool, default: 21600 means 6 hours")
                    .validator(parse_validator_fn!(u64, "Value must be a valid number")),
                Arg::with_name("ffi-twcap-pool-idle-timeout-in-secs")
                    .long("ffi-twcap-pool-idle-timeout-in-secs")
                    .global(true)
                    .takes_value(true)
                    .value_name("NUM")
                    .help("Number of seconds to remove unused protocol_runner from pool, default: 1800 means 30 minutes")
                    .validator(parse_validator_fn!(u64, "Value must be a valid number"))
            ])
        .arg(Arg::with_name("init-sapling-spend-params-file")
            .long("init-sapling-spend-params-file")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("Path to a init file for sapling-spend.params")
        )
        .arg(Arg::with_name("init-sapling-output-params-file")
            .long("init-sapling-output-params-file")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .help("Path to a init file for sapling-output.params")
        )
        .arg(Arg::with_name("tokio-threads")
            .long("tokio-threads")
            .global(true)
            .takes_value(true)
            .value_name("NUM")
            .help("Number of threads spawned by a tokio thread pool. If value is zero, then number of threads equal to CPU cores is spawned.")
            .validator(parse_validator_fn!(usize, "Value must be a valid number")))
        .arg(Arg::with_name("actions-store-backend")
            .long("actions-store-backend")
            .global(true)
            .takes_value(true)
            // TODO: hard to override, do as single atribute commanseparated
            // .multiple(true)
            .value_name("STRING")
            .possible_values(&ContextActionStoreBackend::possible_values())
            .help("Activate recording of context storage actions"))
        .arg(Arg::with_name("context-kv-store")
            .long("context-kv-store")
            .global(true)
            .takes_value(true)
            .value_name("STRING")
            .possible_values(&SupportedContextKeyValueStore::possible_values())
            .help("Choose the merkle storege backend - supported backends: 'inmem', 'inmem-gc', 'btree'"))
        .arg(Arg::with_name("compute-context-action-tree-hashes")
            .long("compute-context-action-tree-hashes")
            .global(true)
            .takes_value(true)
            .value_name("BOOL")
            .help("Activate the computation of tree hashes when applying context actions"))
        .arg(Arg::with_name("sandbox-patch-context-json-file")
            .long("sandbox-patch-context-json-file")
            .global(true)
            .takes_value(true)
            .value_name("PATH")
            .required(false)
            .help("Path to the json file with key-values, which will be added to empty context on startup and commit genesis.")
            .validator(|v| if Path::new(&v).exists() { Ok(()) } else { Err(format!("Sandbox patch-context json file not found at '{}'", v)) }))
        .subcommand(
            clap::SubCommand::with_name("replay")
                .arg(Arg::with_name("from-block")
                     .long("from-block")
                     .takes_value(true)
                     .value_name("HASH")
                     .display_order(0)
                     .help("Block from which we start the replay")
                     .validator(|value| {
                         value.parse::<BlockHash>().map(|_| ()).map_err(|_| format!("Block hash not valid"))
                     })
                )
                .arg(Arg::with_name("to-block")
                     .long("to-block")
                     .takes_value(true)
                     .value_name("HASH")
                     .display_order(0)
                     .required(true)
                     .help("Replay until this block")
                     .validator(|value| {
                         value.parse::<BlockHash>().map(|_| ()).map_err(|_| format!("Block hash not valid"))
                     })
                )
                .arg(Arg::with_name("target-path")
                     .long("target-path")
                     .takes_value(true)
                     .value_name("PATH")
                     .display_order(1)
                     .required(true)
                     .help("A directory for the replay")
                     .validator(|v| {
                         let dir = Path::new(&v);
                         if dir.exists() {
                             if dir.is_dir() {
                                 Ok(())
                             } else {
                                 Err(format!("Required replay data dir '{}' exists, but is not a directory!", v))
                             }
                         } else {
                             // Tezos data dir does not exists, try to create it
                             if let Err(e) = fs::create_dir_all(dir) {
                                 Err(format!("Unable to create required replay data dir '{}': {} ", v, e))
                             } else {
                                 Ok(())
                             }
                         }
                     }))
                .arg(Arg::with_name("fail-above")
                     .long("fail-above")
                     .takes_value(true)
                     .value_name("NUM")
                     .display_order(1)
                     .required(false)
                     .help("Panic if the block application took longer than this number of milliseconds")
                     .validator(parse_validator_fn!(u64, "Value must be a valid number"))
                )
        );
    app
}

fn pool_cfg(
    args: &clap::ArgMatches,
    pool_name_discriminator: &str,
) -> TezosApiConnectionPoolConfiguration {
    TezosApiConnectionPoolConfiguration {
        min_connections: 0,
        /* 0 means that connections are created on-demand, because of AT_LEAST_ONE_WRITE_PROTOCOL_CONTEXT_WAS_SUCCESS_AT_FIRST_LOCK */
        max_connections: args
            .value_of(&format!(
                "ffi-{}-pool-max-connections",
                pool_name_discriminator
            ))
            .unwrap_or("10")
            .parse::<u8>()
            .expect("Provided value cannot be converted to number"),
        connection_timeout: args
            .value_of(&format!(
                "ffi-{}-pool-connection-timeout-in-secs",
                pool_name_discriminator
            ))
            .unwrap_or("60")
            .parse::<u16>()
            .map(|seconds| Duration::from_secs(seconds as u64))
            .expect("Provided value cannot be converted to number"),
        max_lifetime: args
            .value_of(&format!(
                "ffi-{}-pool-max-lifetime-in-secs",
                pool_name_discriminator
            ))
            .unwrap_or("21600")
            .parse::<u16>()
            .map(|seconds| Duration::from_secs(seconds as u64))
            .expect("Provided value cannot be converted to number"),
        idle_timeout: args
            .value_of(&format!(
                "ffi-{}-pool-idle-timeout-in-secs",
                pool_name_discriminator
            ))
            .unwrap_or("1800")
            .parse::<u16>()
            .map(|seconds| Duration::from_secs(seconds as u64))
            .expect("Provided value cannot be converted to number"),
    }
}

// Explicitly validates all required parameters
// Flag Required=true must be handled separately as we parse args twice,
// once to see only if config-file arg is present and second time to parse all args
// In case some args are required=true and user provides only config-file,
// first round of parsing would always fail then
fn validate_required_args(args: &clap::ArgMatches) {
    validate_required_arg(args, "tezos-data-dir", None);
    validate_required_arg(
        args,
        "network",
        Some(format!(
            "possible_values: {:?}",
            TezosEnvironment::possible_values()
        )),
    );
    validate_required_arg(args, "bootstrap-db-path", None);
    validate_required_arg(args, "p2p-port", None);
    validate_required_arg(args, "protocol-runner", None);
    validate_required_arg(args, "rpc-port", None);
    validate_required_arg(args, "websocket-address", None);
    validate_required_arg(args, "peer-thresh-low", None);
    validate_required_arg(args, "peer-thresh-high", None);
    validate_required_arg(args, "tokio-threads", None);
    validate_required_arg(args, "identity-file", None);
    validate_required_arg(args, "identity-expected-pow", None);
}

// Validates single required arg. If missing, exit whole process
pub fn validate_required_arg(args: &clap::ArgMatches, arg_name: &str, help: Option<String>) {
    if !args.is_present(arg_name) {
        match help {
            Some(help) => panic!("Required \"{}\" arg is missing, {} !!!", arg_name, help),
            None => panic!("Required \"{}\" arg is missing !!!", arg_name),
        }
    }
}

// Returns final path. In case:
//      1. path is relative -> final_path = tezos_data_dir / path
//      2. path is absolute -> final_path = path
pub fn get_final_path(tezos_data_dir: &Path, path: PathBuf) -> PathBuf {
    let mut final_path: PathBuf;

    // path is absolute or relative to the current dir -> start with ./ or ../
    if path.is_absolute() || path.starts_with(".") {
        final_path = path
    }
    // otherwise path is relative to the tezos-data-dir
    else {
        final_path = tezos_data_dir.to_path_buf();
        final_path.push(path);
    }

    // Tries to create final_path parent dir, if non-existing
    if let Some(parent_dir) = final_path.parent() {
        if !parent_dir.exists() {
            if let Err(e) = fs::create_dir_all(parent_dir) {
                panic!("Unable to create required dir '{:?}': {} ", parent_dir, e);
            }
        }
    }

    final_path
}

// Parses config file and returns vector of OsString representing all argument strings from file
// All lines that are empty or begin with "#" or "//" are ignored
pub fn parse_config(config_path: PathBuf) -> Vec<OsString> {
    let file = fs::File::open(&config_path)
        .unwrap_or_else(|_| panic!("Unable to open config file at: {:?}", config_path));
    let reader = io::BufReader::new(file);

    let mut args: Vec<OsString> = vec![];

    let mut line_num = 0;
    for line_result in reader.lines() {
        let mut line = line_result.unwrap_or_else(|_| {
            panic!(
                "Unable to read line: {:?} from config file at: {:?}",
                line_num, config_path
            )
        });
        line = line.trim().to_string();

        if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
            continue;
        }

        args.push(OsString::from(line));
        line_num += 1;
    }

    args
}

impl Environment {
    pub fn from_args() -> Self {
        let app = tezos_app();
        let args: clap::ArgMatches;

        // First, get cli arguments and find out only if config-file arg is provided
        // If config-file argument is present, read all parameters from config-file and merge it with cli arguments
        let temp_args = app.clone().get_matches();
        if temp_args.is_present("config-file") {
            let config_path = temp_args
                .value_of("config-file")
                .unwrap()
                .parse::<PathBuf>()
                .expect("Provided config-file cannot be converted to path");

            let mut merged_args = parse_config(config_path);

            let mut cli_args = env::args_os();
            if let Some(bin) = cli_args.next() {
                merged_args.insert(0, bin);
            }
            merged_args.extend(cli_args);

            args = app.get_matches_from(merged_args);
        }
        // Otherwise use only cli arguments that are already parsed
        else {
            args = temp_args;
        }

        // Validates required flags of args
        validate_required_args(&args);

        let tezos_network: TezosEnvironment = args
            .value_of("network")
            .expect("Network is required")
            .parse::<TezosEnvironment>()
            .expect("Was expecting one value from TezosEnvironment");

        let context_storage: TezosContextStorageChoice = args
            .value_of("tezos-context-storage")
            .unwrap_or("both")
            .parse::<TezosContextStorageChoice>()
            .expect("Provided value cannot be converted to a context storage option");
        let mut tezos_data_dir: PathBuf = args
            .value_of("tezos-data-dir")
            .unwrap_or("")
            .parse::<PathBuf>()
            .expect("Provided value cannot be converted to path");

        let replay = args.subcommand_matches("replay").map(|args| {
            let target_path = args
                .value_of("target-path")
                .unwrap()
                .parse::<PathBuf>()
                .expect("Provided value cannot be converted to path");

            let to_block = args
                .value_of("to-block")
                .unwrap()
                .parse::<BlockHash>()
                .expect("Provided value cannot be converted to BlockHash");

            let from_block = args.value_of("from-block").map(|b| {
                b.parse::<BlockHash>()
                    .expect("Provided value cannot be converted to BlockHash")
            });

            let fail_above = std::time::Duration::from_millis(
                args.value_of("fail-above")
                    .unwrap_or(&format!("{}", u64::MAX))
                    .parse::<u64>()
                    .expect("Provided value cannot be converted to number"),
            );

            let mut options = fs_extra::dir::CopyOptions::default();
            options.content_only = true;
            options.overwrite = true;

            fs_extra::dir::copy(tezos_data_dir.as_path(), target_path.as_path(), &options).unwrap();

            tezos_data_dir = target_path;

            Replay {
                from_block,
                to_block,
                fail_above,
            }
        });

        let log_targets: HashSet<String> = match args.values_of("log") {
            Some(v) => v.map(String::from).collect(),
            None => std::iter::once("terminal".to_string()).collect(),
        };

        let log = log_targets
            .iter()
            .map(|name| {
                LoggerType::from_str(name).unwrap_or_else(|_| {
                    panic!(
                        "Unknown log target {} - supported are: {:?}",
                        &name,
                        LoggerType::possible_values()
                    )
                })
            })
            .collect();

        let listener_port = args
            .value_of("p2p-port")
            .unwrap_or("")
            .parse::<u16>()
            .expect("Was expecting value of p2p-port");

        Environment {
            p2p: crate::configuration::P2p {
                listener_port,
                listener_address: format!("0.0.0.0:{}", listener_port)
                    .parse::<SocketAddr>()
                    .expect("Failed to parse listener address"),
                disable_bootstrap_lookup: args.is_present("disable-bootstrap-lookup"),
                bootstrap_lookup_addresses: args
                    .value_of("bootstrap-lookup-address")
                    .map(|addresses_str| {
                        addresses_str
                            .split(',')
                            .map(|address| address.to_string())
                            .collect()
                    })
                    .unwrap_or_else(|| {
                        if !args.is_present("peers") && !args.is_present("private-node") {
                            match environment::TEZOS_ENV.get(&tezos_network) {
                                None => panic!(
                                    "No tezos environment configured for: {:?}",
                                    tezos_network
                                ),
                                Some(cfg) => cfg.bootstrap_lookup_addresses.clone(),
                            }
                        } else {
                            Vec::with_capacity(0)
                        }
                    })
                    .iter()
                    .map(|addr| {
                        environment::parse_bootstrap_addr_port(
                            addr,
                            crate::configuration::P2p::DEFAULT_P2P_PORT_FOR_LOOKUP,
                        )
                        .unwrap_or_else(|_| {
                            panic!(
                                "Was expecting 'ADDR' or 'ADDR:PORT', invalid value: {}",
                                addr
                            )
                        })
                    })
                    .collect(),
                bootstrap_peers: args
                    .value_of("peers")
                    .map(|peers_str| {
                        peers_str
                            .split(',')
                            .map(|ip_port| ip_port.parse().expect("Was expecting IP:PORT"))
                            .collect()
                    })
                    .unwrap_or_default(),
                peer_threshold: PeerConnectionThreshold::try_new(
                    args.value_of("peer-thresh-low")
                        .unwrap_or("")
                        .parse::<usize>()
                        .expect("Provided value cannot be converted to number"),
                    args.value_of("peer-thresh-high")
                        .unwrap_or("")
                        .parse::<usize>()
                        .expect("Provided value cannot be converted to number"),
                    args.value_of("synchronization-thresh").map(|v| {
                        v.parse::<usize>()
                            .expect("Provided value cannot be converted to number")
                    }),
                )
                .expect("Invalid threashold range"),
                private_node: args
                    .value_of("private-node")
                    .unwrap_or("false")
                    .parse::<bool>()
                    .expect("Provided value cannot be converted to bool"),
                disable_mempool: args.is_present("disable-mempool"),
            },
            rpc: crate::configuration::Rpc {
                listener_port: args
                    .value_of("rpc-port")
                    .unwrap_or("")
                    .parse::<u16>()
                    .expect("Was expecting value of rpc-port"),
                websocket_address: args
                    .value_of("websocket-address")
                    .unwrap_or("")
                    .parse()
                    .expect("Provided value cannot be converted into valid uri"),
            },
            logging: crate::configuration::Logging {
                log,
                ocaml_log_enabled: args
                    .value_of("ocaml-log-enabled")
                    .unwrap_or("")
                    .parse::<bool>()
                    .expect("Provided value cannot be converted to bool"),
                level: args
                    .value_of("log-level")
                    .unwrap_or("")
                    .parse::<slog::Level>()
                    .expect("Was expecting one value from slog::Level"),
                format: args
                    .value_of("log-format")
                    .unwrap_or("")
                    .parse::<LogFormat>()
                    .expect("Was expecting 'simple' or 'json'"),
                file: {
                    let log_file_path = args.value_of("log-file").map(|v| {
                        v.parse::<PathBuf>()
                            .expect("Provided value cannot be converted to path")
                    });

                    if let Some(path) = log_file_path {
                        Some(get_final_path(&tezos_data_dir, path))
                    } else {
                        log_file_path
                    }
                },
            },
            storage: {
                let path = args
                    .value_of("bootstrap-db-path")
                    .unwrap_or("")
                    .parse::<PathBuf>()
                    .expect("Provided value cannot be converted to path");
                let db_path = get_final_path(&tezos_data_dir, path.clone());

                let context_stats_db_path = args.value_of("context-stats-db-path").map(|value| {
                    let path = value
                        .parse::<PathBuf>()
                        .expect("Provided value cannot be converted to path");
                    get_final_path(&tezos_data_dir, path)
                });

                let db_threads_count = args.value_of("db-cfg-max-threads").map(|value| {
                    value
                        .parse::<usize>()
                        .map(|val| {
                            std::cmp::min(
                                Storage::MINIMAL_THREAD_COUNT,
                                val / Storage::STORAGES_COUNT,
                            )
                        })
                        .expect("Provided value cannot be converted to number")
                });

                let db = RocksDbConfig {
                    cache_size: Storage::LRU_CACHE_SIZE_96MB,
                    expected_db_version: Storage::DB_STORAGE_VERSION,
                    db_path: db_path.join("db"),
                    columns: DbsRocksDbTableInitializer,
                    threads: db_threads_count,
                };

                let context_kv_store = args
                    .value_of("context-kv-store")
                    .unwrap_or(Storage::DEFAULT_CONTEXT_KV_STORE_BACKEND)
                    .parse::<SupportedContextKeyValueStore>()
                    .map(|v| match v {
                        SupportedContextKeyValueStore::InMem => ContextKvStoreConfiguration::InMem,
                        SupportedContextKeyValueStore::BTreeMap => {
                            ContextKvStoreConfiguration::BTreeMap
                        }
                        SupportedContextKeyValueStore::InMemGC => {
                            ContextKvStoreConfiguration::InMemGC
                        }
                    })
                    .unwrap_or_else(|e| {
                        panic!(
                            "Expecting one value from {:?}, error: {:?}",
                            SupportedContextKeyValueStore::possible_values(),
                            e
                        )
                    });

                let compute_context_action_tree_hashes = args
                    .value_of("compute-context-action-tree-hashes")
                    .unwrap_or("false")
                    .parse::<bool>()
                    .expect("Provided value cannot be converted to bool");

                // TODO - TE-261: can this conversion be made prettier without `to_string_lossy`?
                // Path for the socket that will be used for IPC access to the context
                let context_ipc_socket_path =
                    ipc::temp_sock().to_string_lossy().as_ref().to_owned();

                let context_storage_configuration = match context_storage {
                    TezosContextStorageChoice::TezEdge => {
                        TezosContextStorageConfiguration::TezEdgeOnly(
                            TezosContextTezEdgeStorageConfiguration {
                                backend: context_kv_store,
                                ipc_socket_path: Some(context_ipc_socket_path),
                            },
                        )
                    }
                    TezosContextStorageChoice::Irmin => {
                        TezosContextStorageConfiguration::IrminOnly(
                            TezosContextIrminStorageConfiguration {
                                data_dir: tezos_data_dir
                                    .to_str()
                                    .expect("Invalid tezos_data_dir value")
                                    .to_string(),
                            },
                        )
                    }
                    TezosContextStorageChoice::Both => TezosContextStorageConfiguration::Both(
                        TezosContextIrminStorageConfiguration {
                            data_dir: tezos_data_dir
                                .to_str()
                                .expect("Invalid tezos_data_dir value")
                                .to_string(),
                        },
                        TezosContextTezEdgeStorageConfiguration {
                            backend: context_kv_store,
                            ipc_socket_path: Some(context_ipc_socket_path),
                        },
                    ),
                };

                crate::configuration::Storage {
                    context_storage_configuration,
                    db,
                    db_path,
                    context_stats_db_path,
                    compute_context_action_tree_hashes,
                    patch_context: {
                        match args.value_of("sandbox-patch-context-json-file") {
                            Some(path) => {
                                let path = path
                                    .parse::<PathBuf>()
                                    .expect("Provided value cannot be converted to path");
                                let path = get_final_path(&tezos_data_dir, path);
                                match fs::read_to_string(&path) {
                                    Ok(content) => {
                                        // validate valid json
                                        if let Err(e) = serde_json::from_str::<
                                            HashMap<String, serde_json::Value>,
                                        >(
                                            &content
                                        ) {
                                            panic!(
                                                "Invalid json file: {}, reason: {}",
                                                path.as_path().display().to_string(),
                                                e
                                            );
                                        }
                                        Some(PatchContext {
                                            key: "sandbox_parameter".to_string(),
                                            json: content,
                                        })
                                    }
                                    Err(e) => panic!("Cannot read file, reason: {}", e),
                                }
                            }
                            None => {
                                // check default configuration, if any
                                match environment::TEZOS_ENV.get(&tezos_network) {
                                    None => panic!(
                                        "No tezos environment configured for: {:?}",
                                        tezos_network
                                    ),
                                    Some(cfg) => cfg.patch_context_genesis_parameters.clone(),
                                }
                            }
                        }
                    },
                }
            },
            identity: crate::configuration::Identity {
                identity_json_file_path: {
                    let identity_path = args
                        .value_of("identity-file")
                        .unwrap_or("")
                        .parse::<PathBuf>()
                        .expect("Provided value cannot be converted to path");
                    get_final_path(&tezos_data_dir, identity_path)
                },
                expected_pow: args
                    .value_of("identity-expected-pow")
                    .unwrap_or("26.0")
                    .parse::<f64>()
                    .expect("Provided value cannot be converted to number"),
            },
            ffi: Ffi {
                protocol_runner: args
                    .value_of("protocol-runner")
                    .unwrap_or("")
                    .parse::<PathBuf>()
                    .expect("Provided value cannot be converted to path"),
                tezos_readonly_api_pool: pool_cfg(
                    &args,
                    Ffi::TEZOS_READONLY_API_POOL_DISCRIMINATOR,
                ),
                tezos_readonly_prevalidation_api_pool: pool_cfg(
                    &args,
                    Ffi::TEZOS_READONLY_PREVALIDATION_API_POOL_DISCRIMINATOR,
                ),
                tezos_without_context_api_pool: pool_cfg(
                    &args,
                    Ffi::TEZOS_WITHOUT_CONTEXT_API_POOL_DISCRIMINATOR,
                ),
                zcash_param: ZcashParams {
                    init_sapling_spend_params_file: args
                        .value_of("init-sapling-spend-params-file")
                        .unwrap_or(Ffi::DEFAULT_ZCASH_PARAM_SAPLING_SPEND_FILE_PATH)
                        .parse::<PathBuf>()
                        .expect("Provided value cannot be converted to path"),
                    init_sapling_output_params_file: args
                        .value_of("init-sapling-output-params-file")
                        .unwrap_or(Ffi::DEFAULT_ZCASH_PARAM_SAPLING_OUTPUT_FILE_PATH)
                        .parse::<PathBuf>()
                        .expect("Provided value cannot be converted to path"),
                },
            },
            replay,
            tokio_threads: args
                .value_of("tokio-threads")
                .unwrap_or("0")
                .parse::<usize>()
                .expect("Provided value cannot be converted to number"),
            tezos_network,
            enable_testchain: args
                .value_of("enable-testchain")
                .unwrap_or("false")
                .parse::<bool>()
                .expect("Provided value cannot be converted to bool"),
            validate_cfg_identity_and_stop: args.is_present("validate-cfg-identity-and-stop"),
        }
    }

    pub fn create_logger(&self) -> Result<Logger, NoDrainError> {
        let Environment { logging, .. } = self;
        let drains: Vec<Arc<slog_async::Async>> = logging
            .log
            .iter()
            .map(|log_target| match log_target {
                LoggerType::TerminalLogger => Arc::new(create_terminal_logger!(logging.format)),
                LoggerType::FileLogger => {
                    let log_file = if let Some(path) = &logging.file {
                        path.clone()
                    } else {
                        PathBuf::from("./tezedge.log")
                    };
                    Arc::new(create_file_logger!(logging.format, log_file))
                }
            })
            .collect();

        if drains.is_empty() {
            Err(NoDrainError)
        } else if drains.len() == 1 {
            // if there is only one drain, return the logger
            Ok(Logger::root(
                drains[0].clone().filter_level(logging.level).fuse(),
                slog::o!(),
            ))
        } else {
            // combine 2 or more drains into Duplicates

            // need an initial value for fold, create it from the first two drains in the vector
            let initial_value =
                Box::new(Duplicate::new(drains[0].clone(), drains[1].clone()).fuse());

            // collect the leftover drains
            let leftover_drains: Vec<Arc<slog_async::Async>> = drains.into_iter().skip(2).collect();

            // fold the drains into one Duplicate struct
            let merged_drains: Box<
                dyn SendSyncRefUnwindSafeDrain<Ok = (), Err = Never> + UnwindSafe,
            > = leftover_drains.into_iter().fold(initial_value, |acc, new| {
                Box::new(Duplicate::new(Arc::new(acc), new).fuse())
            });

            Ok(Logger::root(
                merged_drains.filter_level(logging.level).fuse(),
                slog::o!(),
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct InvalidRecorderConfigurationError(String);
