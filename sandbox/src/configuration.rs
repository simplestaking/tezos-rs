// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::path::{Path, PathBuf};

use clap::{App, Arg};

pub struct LauncherEnvironment {
    pub light_node_path: PathBuf,
    pub log_level: slog::Level,
    pub sandbox_rpc_port: u16,
    pub tezos_client_path: PathBuf,
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

fn sandbox_app() -> App<'static, 'static> {
    let app = App::new("Tezos Light Node Launcher")
        .version("0.3.1")
        .author("SimpleStaking and the project contributors")
        .setting(clap::AppSettings::AllArgsOverrideSelf)
        .arg(
            Arg::with_name("light-node-path")
                .long("light-node-path")
                .takes_value(true)
                .value_name("PATH")
                .help("Path to the light-node binary")
                .validator(|v| {
                    if Path::new(&v).exists() {
                        Ok(())
                    } else {
                        Err(format!("Light-node binary not found at '{}'", v))
                    }
                }),
        )
        .arg(
            Arg::with_name("tezos-client-path")
                .long("tezos-client-path")
                .takes_value(true)
                .value_name("PATH")
                .help("Path to the tezos-client binary")
                .validator(|v| {
                    if Path::new(&v).exists() {
                        Ok(())
                    } else {
                        Err(format!("Tezos-client binary not found at '{}'", v))
                    }
                }),
        )
        .arg(
            Arg::with_name("log-level")
                .long("log-level")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&["critical", "error", "warn", "info", "debug", "trace"])
                .help("Set log level"),
        )
        .arg(
            Arg::with_name("sandbox-rpc-port")
                .long("sandbox-rpc-port")
                .takes_value(true)
                .value_name("PORT")
                .help("Rust server RPC port for communication with rust node")
                .validator(parse_validator_fn!(
                    u16,
                    "Value must be a valid port number"
                )),
        );
    app
}

impl LauncherEnvironment {
    pub fn from_args() -> Self {
        let app = sandbox_app();
        let args = app.clone().get_matches();

        LauncherEnvironment {
            light_node_path: args
                .value_of("light-node-path")
                .unwrap_or("")
                .parse::<PathBuf>()
                .expect("Provided value cannot be converted to path"),
            log_level: args
                .value_of("log-level")
                .unwrap_or("")
                .parse::<slog::Level>()
                .expect("Was expecting one value from slog::Level"),
            sandbox_rpc_port: args
                .value_of("sandbox-rpc-port")
                .unwrap_or("")
                .parse::<u16>()
                .expect("Was expecting value of sandbox-rpc-port"),
            tezos_client_path: args
                .value_of("tezos-client-path")
                .unwrap_or("")
                .parse::<PathBuf>()
                .expect("Provided value cannot be converted to path"),
        }
    }
}
