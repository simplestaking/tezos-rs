// // Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// // SPDX-License-Identifier: MIT

// use std::collections::{HashMap, HashSet};
// use std::env;
// use std::fs;
// use std::path::{Path, PathBuf};

// use serde::{Deserialize, Serialize};
// use slog::{Drain, Level, Logger};

// use crypto::hash::OperationHash;
// use networking::ShellCompatibilityVersion;
// use shell::peer_manager::P2p;
// use shell::PeerConnectionThreshold;
// use tezos_messages::p2p::encoding::prelude::Operation;

// pub mod infra;
// pub mod samples;
// pub mod test_cases_data;
// pub mod test_data;
// pub mod test_node_peer;

// pub fn prepare_empty_dir(dir_name: &str) -> String {
//     let path = test_storage_dir_path(dir_name);
//     if path.exists() {
//         fs::remove_dir_all(&path)
//             .unwrap_or_else(|_| panic!("Failed to delete directory: {:?}", &path));
//     }
//     fs::create_dir_all(&path).unwrap_or_else(|_| panic!("Failed to create directory: {:?}", &path));
//     String::from(path.to_str().unwrap())
// }

// pub fn test_storage_dir_path(dir_name: &str) -> PathBuf {
//     let out_dir = env::var("OUT_DIR").expect("OUT_DIR is not defined");
//     Path::new(out_dir.as_str()).join(Path::new(dir_name))
// }

// pub fn create_logger(level: Level) -> Logger {
//     let drain = slog_async::Async::new(
//         slog_term::FullFormat::new(slog_term::TermDecorator::new().build())
//             .build()
//             .fuse(),
//     )
//     .build()
//     .filter_level(level)
//     .fuse();

//     Logger::root(drain, slog::o!())
// }

// pub fn is_ocaml_log_enabled() -> bool {
//     env::var("OCAML_LOG_ENABLED")
//         .unwrap_or_else(|_| "false".to_string())
//         .parse::<bool>()
//         .unwrap()
// }

// pub fn log_level() -> Level {
//     env::var("LOG_LEVEL")
//         .unwrap_or_else(|_| "info".to_string())
//         .parse::<Level>()
//         .unwrap()
// }

// pub fn protocol_runner_executable_path() -> PathBuf {
//     let executable = env::var("PROTOCOL_RUNNER")
//         .unwrap_or_else(|_| panic!("This test requires environment parameter: 'PROTOCOL_RUNNER' to point to protocol_runner executable"));
//     PathBuf::from(executable)
// }

// fn contains_all_keys(
//     map: &HashMap<OperationHash, Operation>,
//     keys: &HashSet<OperationHash>,
// ) -> bool {
//     let mut contains_counter = 0;
//     for key in keys {
//         if map.contains_key(key) {
//             contains_counter += 1;
//         }
//     }
//     contains_counter == keys.len()
// }

// pub fn p2p_cfg_with_threshold(
//     mut cfg: (P2p, ShellCompatibilityVersion),
//     low: usize,
//     high: usize,
//     peers_for_bootstrap_threshold: usize,
// ) -> (P2p, ShellCompatibilityVersion) {
//     cfg.0.peer_threshold =
//         PeerConnectionThreshold::try_new(low, high, Some(peers_for_bootstrap_threshold))
//             .expect("Invalid range");
//     cfg
// }

// /// Empty message
// #[derive(Serialize, Deserialize, Debug)]
// pub struct NoopMessage;
