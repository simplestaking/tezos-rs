// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, RwLock};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use slog::{error, info, warn, Logger};
use thiserror::Error;
use warp::http::StatusCode;
use warp::reject;

use crate::handlers::ErrorMessage;
use crate::node_runner::NodeRpcIpPort;
use crate::rand_chars;

#[derive(Debug, Error)]
pub enum TezosClientRunnerError {
    /// IO Error.
    #[error("IO error during process creation, reason: {reason}")]
    IOError { reason: std::io::Error },

    /// Protocol parameters json error
    #[error("Error while deserializing protocol parameters, json: {json}")]
    ProtocolParameterError { json: serde_json::Value },

    /// Wallet does not exists error
    #[error("Alias ({alias}) does not exists among the known wallets")]
    NonexistantWallet { alias: String },

    /// Serde Error.
    #[error("Error in serde, reason: {reason}")]
    SerdeError { reason: serde_json::Error },

    /// Call Error.
    #[error("Tezos-client call error, message: {message}")]
    CallError { message: ErrorMessage },

    /// Sandbox node is not running.
    #[error("Sandbox node is not running/reachable!")]
    UnavailableSandboxNodeError,

    #[error("System error - sandbox data dir was not initialized for node_ref: {node_ref}")]
    SandboxDataDirNotInitialized { node_ref: NodeRpcIpPort },
}

impl From<std::io::Error> for TezosClientRunnerError {
    fn from(err: std::io::Error) -> TezosClientRunnerError {
        TezosClientRunnerError::IOError { reason: err }
    }
}

impl From<serde_json::Error> for TezosClientRunnerError {
    fn from(err: serde_json::Error) -> TezosClientRunnerError {
        TezosClientRunnerError::SerdeError { reason: err }
    }
}

impl reject::Reject for TezosClientRunnerError {}

/// Type alias for a vecotr of Wallets
pub type SandboxWallets = Vec<Wallet>;

/// Structure holding data used by tezos client
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Wallet {
    alias: String,
    public_key_hash: String,
    public_key: String,
    secret_key: String,
    initial_balance: String,
}

/// The json body incoming with the bake request containing the alias for the wallet to bake with
#[derive(Clone, Debug, Deserialize)]
pub struct BakeRequest {
    alias: String,
}

#[derive(Serialize)]
pub struct TezosClientErrorReply {
    pub message: String,
    pub field_name: String,
}

/// Like wallets we need to store per node, because, if we run multiple nodes, we can have different wallet setting per node
#[derive(Clone)]
pub struct SandboxData {
    pub data_dir_path: PathBuf,
    pub wallets: HashMap<String, Wallet>,
}

/// Thread-safe reference to the client runner
pub type TezosClientRunnerRef = Arc<RwLock<TezosClientRunner>>;

/// Structure holding data to use a tezos-client binary
#[derive(Clone)]
pub struct TezosClientRunner {
    pub name: String,
    pub executable_path: PathBuf,

    /// Temporary data per node
    sandbox_data: HashMap<NodeRpcIpPort, SandboxData>,
}

/// A structure holding all the required parameters to activate an economic protocol
#[derive(Clone, Debug, Deserialize)]
pub struct TezosProtcolActivationParameters {
    timestamp: String,
    protocol_hash: String,
    protocol_parameters: serde_json::Value,
}

#[derive(Serialize, Clone, Debug)]
pub struct TezosClientReply {
    pub output: String,
    pub error: String,
}

impl TezosClientReply {
    pub fn new(output: String, error: String) -> Self {
        Self { output, error }
    }
}

impl Default for TezosClientReply {
    fn default() -> TezosClientReply {
        TezosClientReply::new(String::new(), String::new())
    }
}

impl TezosClientRunner {
    pub fn new(name: &str, executable_path: PathBuf) -> Self {
        Self {
            name: name.to_string(),
            executable_path,
            sandbox_data: HashMap::default(),
        }
    }

    pub fn init_sandbox_data(&mut self, node_ref: NodeRpcIpPort, data_dir_path: PathBuf) {
        self.sandbox_data.insert(
            node_ref,
            SandboxData {
                data_dir_path,
                wallets: HashMap::default(),
            },
        );
    }

    pub fn wallets(
        &self,
        node_ref: &NodeRpcIpPort,
    ) -> Result<&HashMap<String, Wallet>, TezosClientRunnerError> {
        match self.sandbox_data.get(node_ref) {
            Some(data) => Ok(&data.wallets),
            None => Err(TezosClientRunnerError::SandboxDataDirNotInitialized {
                node_ref: node_ref.clone(),
            }),
        }
    }

    fn insert_wallet(
        &mut self,
        node_ref: &NodeRpcIpPort,
        wallet: Wallet,
    ) -> Result<(), TezosClientRunnerError> {
        if let Some(data) = self.sandbox_data.get_mut(node_ref) {
            data.wallets.insert(wallet.alias.clone(), wallet);
            Ok(())
        } else {
            Err(TezosClientRunnerError::SandboxDataDirNotInitialized {
                node_ref: node_ref.clone(),
            })
        }
    }

    /// Activate a protocol with the provided parameters
    pub fn activate_protocol(
        &self,
        mut activation_parameters: TezosProtcolActivationParameters,
        node_ref: &NodeRpcIpPort,
        log: &Logger,
    ) -> Result<TezosClientReply, TezosClientRunnerError> {
        let data_dir = match self.sandbox_data.get(node_ref) {
            Some(data) => &data.data_dir_path,
            None => {
                return Err(TezosClientRunnerError::SandboxDataDirNotInitialized {
                    node_ref: node_ref.clone(),
                })
            }
        };

        // get as mutable object, so we can insert the hardcoded bootstrap accounts
        let params = if let Some(params) = activation_parameters.protocol_parameters.as_object_mut()
        {
            params
        } else {
            return Err(TezosClientRunnerError::ProtocolParameterError {
                json: activation_parameters.protocol_parameters,
            });
        };

        let wallet_activation: Vec<[String; 2]> = self
            .wallets(node_ref)?
            .clone()
            .into_iter()
            .map(|(_, w)| [w.public_key, w.initial_balance])
            .collect();

        // serialize the harcoded accounts as json array and include it in protocol_parameters
        let sandbox_accounts = serde_json::json!(wallet_activation);
        params.insert("bootstrap_accounts".to_string(), sandbox_accounts);

        // create a temporary file, the tezos-client requires the parameters to be passed in a .json file
        let protocol_parameters_json_file =
            data_dir.join(format!("protocol_parameters-{}.json", rand_chars(5)));
        fs::write(
            &protocol_parameters_json_file,
            activation_parameters.protocol_parameters.to_string(),
        )
        .map_err(|err| TezosClientRunnerError::IOError { reason: err })?;

        let mut client_output: TezosClientReply = Default::default();
        self.run_client(
            node_ref,
            [
                "--block",
                "genesis",
                "activate",
                "protocol",
                &activation_parameters.protocol_hash,
                "with",
                "fitness",
                "1",
                "and",
                "key",
                "activator",
                "and",
                "parameters",
                &protocol_parameters_json_file
                    .as_path()
                    .display()
                    .to_string(),
                "--timestamp",
                &activation_parameters.timestamp,
            ]
            .to_vec(),
            &mut client_output,
            log,
        )?;

        Ok(client_output)
    }

    /// Bake a block with the bootstrap1 account
    pub fn bake_block(
        &self,
        request: Option<BakeRequest>,
        node_ref: &NodeRpcIpPort,
        log: &Logger,
    ) -> Result<TezosClientReply, TezosClientRunnerError> {
        let mut client_output: TezosClientReply = Default::default();

        let alias = if let Some(request) = request {
            if let Some(wallet) = self.wallets(node_ref)?.get(&request.alias) {
                &wallet.alias
            } else {
                return Err(TezosClientRunnerError::NonexistantWallet {
                    alias: request.alias,
                });
            }
        } else {
            // if there is no wallet provided in the request (GET) set the alias to be an arbitrary wallet
            if let Some(wallet) = self.wallets(node_ref)?.values().next() {
                &wallet.alias
            } else {
                return Err(TezosClientRunnerError::NonexistantWallet {
                    alias: "-none-".to_string(),
                });
            }
        };

        self.run_client(
            node_ref,
            ["bake", "for", &alias].to_vec(),
            &mut client_output,
            log,
        )?;

        Ok(client_output)
    }

    /// Initialize the accounts in the tezos-client
    pub fn init_client_data(
        &mut self,
        requested_wallets: SandboxWallets,
        node_ref: &NodeRpcIpPort,
        log: &Logger,
    ) -> Result<TezosClientReply, TezosClientRunnerError> {
        let mut client_output: TezosClientReply = Default::default();

        self.run_client(
            node_ref,
            [
                "import",
                "secret",
                "key",
                "activator",
                "unencrypted:edsk31vznjHSSpGExDMHYASz45VZqXN4DPxvsa4hAyY8dHM28cZzp6",
            ]
            .to_vec(),
            &mut client_output,
            log,
        )?;

        for wallet in requested_wallets {
            self.run_client(
                node_ref,
                [
                    "import",
                    "secret",
                    "key",
                    &wallet.alias,
                    &format!("unencrypted:{}", &wallet.secret_key),
                ]
                .to_vec(),
                &mut client_output,
                log,
            )?;
            self.insert_wallet(node_ref, wallet)?;
        }

        Ok(client_output)
    }

    /// Cleanup the tezos-client directory
    pub fn cleanup(&mut self, node_ref: &NodeRpcIpPort) -> Result<(), anyhow::Error> {
        // clear node sandbox data
        if let Some(data) = self.sandbox_data.remove(node_ref) {
            // remove work dir
            fs::remove_dir_all(&data.data_dir_path)?;
        }

        Ok(())
    }

    /// Private method to run the tezos-client as a subprocess and wait for its completion
    ///
    /// args - should contains just command args
    ///
    /// --base-dir | -A | -P are added automatically
    fn run_client(
        &self,
        node_ref: &NodeRpcIpPort,
        command_args: Vec<&str>,
        client_output: &mut TezosClientReply,
        log: &Logger,
    ) -> Result<(), TezosClientRunnerError> {
        let data_dir = match self.sandbox_data.get(node_ref) {
            Some(data) => data.data_dir_path.as_path().display().to_string(),
            None => {
                return Err(TezosClientRunnerError::SandboxDataDirNotInitialized {
                    node_ref: node_ref.clone(),
                })
            }
        };
        let endpoint = format!("http://{}:{}", &node_ref.ip, &node_ref.port);

        let mut args = [
            // add base-dir
            "--base-dir",
            &data_dir,
            // add client ip/port
            "--endpoint",
            &endpoint,
        ]
        .to_vec();
        args.extend(command_args);

        info!(log, "Calling tezos-client ({})", self.executable_path.as_path().display().to_string(); "command" => args.join(" "));

        // call tezos-client
        let output = Command::new(&self.executable_path)
            .args(args)
            .output()
            .map_err(|err| TezosClientRunnerError::IOError { reason: err })?;

        let _ = BufReader::new(output.stdout.as_slice()).read_to_string(&mut client_output.output);
        let _ = BufReader::new(output.stderr.as_slice()).read_to_string(&mut client_output.error);

        Ok(())
    }
}

/// Construct a reply using the output from the tezos-client
pub fn reply_with_client_output(
    reply: TezosClientReply,
    log: &Logger,
) -> Result<impl warp::Reply, TezosClientRunnerError> {
    if reply.error.is_empty() {
        // no error, means success
        info!(log, "Tezos-client call successfull finished"; "replay" => format!("{:?}", &reply));
        Ok(warp::reply::with_status(
            warp::reply::json(&reply),
            StatusCode::OK,
        ))
    } else {
        // no output and error, means error
        if reply.output.is_empty() {
            // whole error
            let error = reply.error;

            // parse error if contains field/message
            if let Some((field_name, message)) = extract_field_name_and_message_ocaml(&error) {
                error!(log, "Tezos-client call finished with validation error"; "error" => error.clone(), "field_name" => field_name.clone(), "message" => message.clone());
                Err(TezosClientRunnerError::CallError {
                    message: ErrorMessage::validation(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &message,
                        field_name,
                        error,
                    ),
                })
            } else {
                error!(log, "Tezos-client call finished with error"; "error" => error.clone());
                Err(TezosClientRunnerError::CallError {
                    message: ErrorMessage::generic(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Tezos-client call finished with error",
                        error,
                    ),
                })
            }
        } else {
            // output and error, means warning
            warn!(log, "Tezos-client call successfull finished with warning"; "replay" => format!("{:?}", &reply));
            Ok(warp::reply::with_status(
                warp::reply::json(&reply),
                StatusCode::OK,
            ))
        }
    }
}

/// Parse the returned error string from the tezos client
pub fn extract_field_name_and_message_ocaml(error: &str) -> Option<(String, String)> {
    let parsed_message = error
        .replace("\\", "")
        .split("\"")
        .filter(|s| s.contains("Invalid protocol_parameters"))
        .join("")
        .replace(" n{ ", "")
        .replace("{", "");

    // extract the field name depending on the parsed error
    let field_name = if parsed_message.contains("Missing object field") {
        Some(
            parsed_message
                .split_whitespace()
                .last()
                .unwrap_or("")
                .to_string(),
        )
    } else if parsed_message.contains("/") {
        Some(
            parsed_message
                .split_whitespace()
                .filter(|s| s.contains("/"))
                .join("")
                .replace("/", "")
                .replace(",", ""),
        )
    } else {
        None
    };

    if let Some(field_name) = field_name {
        // simply remove the field name from the error message
        let message = parsed_message
            .replace(&field_name, "")
            .replace("At /, ", "")
            .trim()
            .to_string();
        Some((field_name, message))
    } else {
        None
    }
}
