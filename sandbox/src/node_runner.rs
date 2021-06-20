// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::fmt;
use std::fs;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use itertools::Itertools;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use slog::{error, info, warn, Logger};
use thiserror::Error;
use wait_timeout::ChildExt;
use warp::reject;

use crate::{create_temp_dir, rand_chars};

#[derive(Debug, Error)]
pub enum LightNodeRunnerError {
    /// Already running error.
    #[error("Sandbox light-node is already running")]
    NodeAlreadyRunning,

    /// Already running error.
    #[error("Sandbox light-node is not running, node_ref: {node_ref}")]
    NodeNotRunning { node_ref: NodeRpcIpPort },

    /// IO Error.
    #[error("IOError - {message}, reason: {reason}")]
    IOError {
        message: String,
        reason: std::io::Error,
    },

    /// Json argument parsing error.
    #[error("Json argument parsing error, json: {json}")]
    JsonParsingError { json: serde_json::Value },

    /// Startup Error
    #[error("Error after light-node process spawned, reason: {reason}")]
    NodeStartupError { reason: String },

    /// Rpc port is missing in cfg
    #[error("Failed to start light-node - missing or invalid u16 `rpc_port` in configuration, value: {value:?}")]
    ConfigurationMissingValidRpcPort { value: Option<String> },
}

impl reject::Reject for LightNodeRunnerError {}

/// Thread safe reference to a shared Runner
pub type LightNodeRunnerRef = Arc<RwLock<LightNodeRunner>>;

const SANDBOX_NODE_IP: &str = "localhost";
const NODE_CONFIG_RPC_PORT: &str = "rpc_port";
const NODE_CONFIG_TEZOS_DATA_DIR: &str = "tezos_data_dir";
const NODE_CONFIG_TEZEDGE_DATA_DIR: &str = "bootstrap_db_path";
const NODE_CONFIG_IDENTITY_FILE: &str = "identity_file";
const NODE_CONFIG_PATCH_CONTEXT_JSON_FILE_PATH: &str = "sandbox_patch_context_json_file";
const NODE_CONFIG_PROTOCOL_RUNNER: &str = "protocol_runner";

/// RPC ip/port, where is light node listening for rpc requests
#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct NodeRpcIpPort {
    pub ip: String,
    pub port: u16,
}

impl NodeRpcIpPort {
    pub fn new(cfg: &serde_json::Value) -> Result<Self, LightNodeRunnerError> {
        let port = match cfg.get(NODE_CONFIG_RPC_PORT) {
            Some(value) => match value.as_u64() {
                Some(port) => {
                    if port <= u16::MAX as u64 {
                        port as u16
                    } else {
                        return Err(LightNodeRunnerError::ConfigurationMissingValidRpcPort {
                            value: Some(value.to_string()),
                        });
                    }
                }
                None => {
                    return Err(LightNodeRunnerError::ConfigurationMissingValidRpcPort {
                        value: Some(value.to_string()),
                    })
                }
            },
            None => {
                return Err(LightNodeRunnerError::ConfigurationMissingValidRpcPort { value: None })
            }
        };
        Ok(Self {
            ip: SANDBOX_NODE_IP.to_string(),
            port,
        })
    }
}

impl fmt::Display for NodeRpcIpPort {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_fmt(format_args!("SandboxNode({},{})", &self.ip, self.port))
    }
}

/// Struct that holds info about the running child process
pub struct LightNodeRunner {
    executable_path: PathBuf,
    protocol_runner_executable_path: PathBuf,
    _name: String,

    // TODO: TE-213 Launch multiple nodes + wrap process with rpc ip/port
    // TODO: in main we have peers
    process: Option<Child>,
}

impl LightNodeRunner {
    const PROCESS_WAIT_TIMEOUT: Duration = Duration::from_secs(4);

    pub fn new(
        name: &str,
        executable_path: PathBuf,
        protocol_runner_executable_path: PathBuf,
    ) -> Self {
        Self {
            executable_path,
            protocol_runner_executable_path,
            _name: name.to_string(),
            process: None,
        }
    }

    /// Spawn a light-node child process with health check
    ///
    /// Returns:
    /// `node_ref` - port/ip where is running sandbox node
    /// `data_dir` - one node will have its own temp folder for data (identity, dbs, temp files,...)
    ///            - so we need to filter and modify input cfg properties: [NODE_CONFIG_TEZOS_DATA_DIR][NODE_CONFIG_TEZEDGE_DATA_DIR][NODE_CONFIG_IDENTITY_FILE]
    pub fn spawn(
        &mut self,
        cfg: serde_json::Value,
        log: &Logger,
    ) -> Result<(NodeRpcIpPort, PathBuf), LightNodeRunnerError> {
        if self.is_running() {
            Err(LightNodeRunnerError::NodeAlreadyRunning)
        } else {
            // parse rpc settings
            let node = NodeRpcIpPort::new(&cfg)?;

            // one node will have its own temp folder for data (identity, dbs, ...)
            let data_dir =
                create_temp_dir("sandbox-node").map_err(|err| LightNodeRunnerError::IOError {
                    message: "Failed to create temp data file for sandbox node".to_string(),
                    reason: err,
                })?;

            // TODO: remove (temporary fix for debugger), which is waiting for identity in dedicated folder
            let original_identity_path = cfg.get(NODE_CONFIG_IDENTITY_FILE).cloned();

            // fix setting for sandbox temp dir
            let cfg = self.ensure_sandbox_cfg(cfg, &data_dir, log)?;

            // spawn the process for a "health check" with piped stderr
            // adds [validate_cfg_identity_and_stop=true], which just validate configuration and generates identity
            // and stops immediatelly
            let mut process = Command::new(&self.executable_path)
                .args(Self::construct_args(cfg.clone(), true)?)
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|err| LightNodeRunnerError::IOError {
                    message: "Failed to start light-node (setup)".to_string(),
                    reason: err,
                })?;

            // give some time for the process to spawn, a second is sufficient, we only care if it fails to start up
            // and if it does, it fails almost instatnly
            // TODO: this depends on identity pow, maybe we should generate identity here before run subprocess
            thread::sleep(Duration::from_secs(1));

            // check for live process
            match process.try_wait() {
                // process exited, we need to handle and show the exact error
                Ok(Some(exit_status)) => {
                    if !exit_status.success() {
                        let error_msg = handle_stderr(&mut process);
                        error!(log, "Failed to start light-node (validate_args)"; "reason" => error_msg.clone());
                        return Err(LightNodeRunnerError::NodeStartupError {
                            reason: error_msg
                                .split("USAGE:")
                                .take(1)
                                .join("")
                                .replace("error:", "")
                                .trim()
                                .into(),
                        });
                    } else {
                        info!(
                            log,
                            "Light-node configuration and identity validation finished!"
                        );
                    }
                }
                _ => {
                    // somehow process was not finished, so we need to kill him
                    match Self::send_sigint(process.id()) {
                        Ok(()) => {
                            self.process = None;
                        }
                        // if for some reason, the SIGINT fails to end the process, kill it with SIGKILL
                        Err(e) => {
                            let error_msg = handle_stderr(&mut process);
                            warn!(log, "Light-node configration and identity validation not finished, so we need to kill him!"; "reason" => format!("{}", e), "error_msg" => error_msg);
                            Self::terminate_ref(&mut process);
                        }
                    }
                    // enable a to shut down gracefully
                    thread::sleep(Duration::from_secs(5));
                }
            }

            // TODO: remove (temporary fix for debugger), now we just copy real identity to original path
            if let Some(original_identity_path) = original_identity_path {
                if let Some(original_identity_path) = original_identity_path.as_str() {
                    if let Some(Some(real_identity_path)) =
                        cfg.get(NODE_CONFIG_IDENTITY_FILE).map(|path| path.as_str())
                    {
                        let original_identity_path = PathBuf::from(original_identity_path);
                        let real_identity_path = PathBuf::from(real_identity_path);

                        // ensure parent target dir
                        if let Some(parent) = original_identity_path.parent() {
                            if !parent.exists() {
                                if let Err(e) = fs::create_dir_all(parent) {
                                    error!(log, "Failed to create parent directory for identity";
                                            "parent" => parent.display().to_string(),
                                            "reason" => format!("{}", e));
                                }
                            }
                        }

                        // copy identity
                        match fs::copy(&real_identity_path, &original_identity_path) {
                            Ok(_) => info!(log, "Identity was copied successfully";
                                            "to_original_identity_path" => original_identity_path.as_path().display().to_string(),
                                            "from_real_identity_path" => real_identity_path.as_path().display().to_string()),
                            Err(e) => error!(log, "Identity was not copied successfully";
                                            "to_original_identity_path" => original_identity_path.as_path().display().to_string(),
                                            "from_real_identity_path" => real_identity_path.as_path().display().to_string(),
                                            "reason" => format!("{}", e)),
                        }
                    }
                }
            }

            // the process started up OK, but we restart it to enable normal logging (stderr won't be piped)
            // start the process again without piped stdout/stderr, e.g. to have ability log to syslog in docker to tezos-debugger
            let process = Command::new(&self.executable_path)
                .args(Self::construct_args(cfg, false)?)
                .spawn()
                .map_err(|err| LightNodeRunnerError::IOError {
                    message: "Failed to start light-node".to_string(),
                    reason: err,
                })?;

            self.process = Some(process);
            Ok((node, data_dir))
        }
    }

    /// Shut down the light-node
    pub fn shutdown(&mut self, node_ref: &NodeRpcIpPort) -> Result<(), LightNodeRunnerError> {
        if self.is_running() {
            let process = self.process.as_mut().unwrap();
            // kill with SIGINT (ctr-c)
            match Self::send_sigint(process.id()) {
                Ok(()) => {
                    self.process = None;
                    Ok(())
                }
                // if for some reason, the SIGINT fails to end the process, kill it with SIGKILL
                Err(_) => {
                    Self::terminate_ref(process);
                    Ok(())
                }
            }
        } else {
            Err(LightNodeRunnerError::NodeNotRunning {
                node_ref: node_ref.clone(),
            })
        }
    }

    fn terminate_ref(process: &mut Child) {
        match process.wait_timeout(Self::PROCESS_WAIT_TIMEOUT).unwrap() {
            Some(_) => (),
            None => {
                // child hasn't exited yet
                let _ = process.kill();
            }
        };
    }

    fn is_running(&mut self) -> bool {
        if let Some(process) = &mut self.process {
            match process.try_wait() {
                Ok(None) => true,
                _ => false,
            }
        } else {
            false
        }
    }

    /// function to construct a vector with all the passed (via RPC) arguments
    fn construct_args(
        cfg: serde_json::Value,
        add_validate_cfg_identity_and_stop: bool,
    ) -> Result<Vec<String>, LightNodeRunnerError> {
        let mut args: Vec<String> = Vec::new();

        if let Some(arg_map) = cfg.as_object() {
            for (key, value) in arg_map {
                args.push(format!("--{}", key.replace("_", "-").replace("\"", "")));
                let str_val = value.to_string().replace("\"", "");

                // options are defined as a key with an empty string
                if !str_val.is_empty() {
                    args.push(str_val);
                }
            }
            if add_validate_cfg_identity_and_stop {
                args.push("--validate-cfg-identity-and-stop".to_string());
            }
            Ok(args)
        } else {
            Err(LightNodeRunnerError::JsonParsingError { json: cfg })
        }
    }

    /// This function will filter and modify input cfg properties
    /// 1. replaces [NODE_CONFIG_TEZOS_DATA_DIR][NODE_CONFIG_TEZEDGE_DATA_DIR][NODE_CONFIG_IDENTITY_FILE] with custom names prefixed [sandbox_data_dir]
    /// 2. replaces [NODE_CONFIG_PROTOCOL_RUNNER] with own settings
    /// 3. stores [sandbox_patch_context_json] to tempfile and sets it as [NODE_CONFIG_PATCH_CONTEXT_JSON_FILE_PATH] (if present)
    fn ensure_sandbox_cfg(
        &self,
        mut cfg: serde_json::Value,
        sandbox_data_dir: &PathBuf,
        log: &Logger,
    ) -> Result<serde_json::Value, LightNodeRunnerError> {
        if let Some(map) = cfg.as_object_mut() {
            // 1.
            let sandboxed = sandbox_data_dir
                .join("sandbox_tezos_db")
                .as_path()
                .display()
                .to_string();
            if let Some(old_value) = map.insert(
                NODE_CONFIG_TEZOS_DATA_DIR.to_string(),
                serde_json::Value::String(sandboxed.clone()),
            ) {
                info!(log, "Changing sandbox node configuration"; "property" => NODE_CONFIG_TEZOS_DATA_DIR.to_string(), "old_value" => old_value.to_string(), "new_value" => sandboxed);
            }
            let sandboxed = sandbox_data_dir
                .join("sandbox_tezedge_db")
                .as_path()
                .display()
                .to_string();
            if let Some(old_value) = map.insert(
                NODE_CONFIG_TEZEDGE_DATA_DIR.to_string(),
                serde_json::Value::String(sandboxed.clone()),
            ) {
                info!(log, "Changing sandbox node configuration"; "property" => NODE_CONFIG_TEZEDGE_DATA_DIR.to_string(), "old_value" => old_value.to_string(), "new_value" => sandboxed);
            }
            let sandboxed = sandbox_data_dir
                .join("sandbox_identity.json")
                .as_path()
                .display()
                .to_string();
            if let Some(old_value) = map.insert(
                NODE_CONFIG_IDENTITY_FILE.to_string(),
                serde_json::Value::String(sandboxed.clone()),
            ) {
                info!(log, "Changing sandbox node configuration"; "property" => NODE_CONFIG_IDENTITY_FILE.to_string(), "old_value" => old_value.to_string(), "new_value" => sandboxed);
            }

            // 2.
            let protocol_runner = self
                .protocol_runner_executable_path
                .as_path()
                .display()
                .to_string();
            if let Some(old_value) = map.insert(
                NODE_CONFIG_PROTOCOL_RUNNER.to_string(),
                serde_json::Value::String(protocol_runner.clone()),
            ) {
                info!(log, "Changing sandbox node configuration"; "property" => NODE_CONFIG_PROTOCOL_RUNNER.to_string(), "old_value" => old_value.to_string(), "new_value" => protocol_runner);
            }

            // 3.
            if let Some(sandbox_patch_context_json) = map.remove("sandbox_patch_context_json") {
                let sandbox_patch_context_json_file =
                    sandbox_data_dir.join(format!("sandbox-patch-context-{}.json", rand_chars(5)));

                // create temp file
                fs::write(
                    &sandbox_patch_context_json_file,
                    &sandbox_patch_context_json.to_string(),
                )
                .map_err(|err| LightNodeRunnerError::IOError {
                    message: sandbox_patch_context_json_file
                        .as_path()
                        .display()
                        .to_string(),
                    reason: err,
                })?;

                // insert to cfg
                if let Some(old_value) = map.insert(
                    NODE_CONFIG_PATCH_CONTEXT_JSON_FILE_PATH.to_string(),
                    serde_json::Value::String(
                        sandbox_patch_context_json_file
                            .as_path()
                            .display()
                            .to_string(),
                    ),
                ) {
                    info!(log, "Changing sandbox node configuration";
                               "property" => NODE_CONFIG_PATCH_CONTEXT_JSON_FILE_PATH.to_string(),
                               "old_value" => old_value.to_string(),
                               "new_value" => sandbox_patch_context_json_file.as_path().display().to_string());
                }
            }
        }
        Ok(cfg)
    }

    /// Send SIGINT signal to the process with PID, light-node is cheking for this signal and shuts down
    /// gracefully if recieved
    fn send_sigint(pid: u32) -> Result<(), nix::Error> {
        signal::kill(Pid::from_raw(pid as i32), Signal::SIGINT)
    }
}

/// extract data as String form the piped stderr
fn handle_stderr(process: &mut Child) -> String {
    if let Some(stderr) = process.stderr.take() {
        let mut extract = String::new();
        let _ = BufReader::new(stderr).read_to_string(&mut extract);
        extract
    } else {
        "-empty stderr-".to_string()
    }
}
