// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{collections::HashSet, convert::TryFrom};

use strum::IntoEnumIterator;

use crypto::hash::{ContextHash, ProtocolHash};
use tezos_api::environment::{TezosEnvironment, TezosEnvironmentConfiguration, TEZOS_ENV};
use tezos_api::ffi::{
    PatchContext, TezosContextConfiguration, TezosContextIrminStorageConfiguration,
    TezosContextStorageConfiguration, TezosContextTezEdgeStorageConfiguration,
    TezosRuntimeConfiguration,
};
use tezos_client::client;

mod common;

#[test]
fn test_init_empty_context_for_all_enviroment_nets() {
    // init runtime and turn on/off ocaml logging
    client::change_runtime_configuration(TezosRuntimeConfiguration {
        debug_mode: false,
        compute_context_action_tree_hashes: false,
        log_enabled: common::is_ocaml_log_enabled(),
    })
    .unwrap();

    // prepare data
    let storage_data_dir = "init_storage_tests_01";

    let mut genesis_commit_hashes: Vec<ContextHash> = Vec::new();
    let mut protocol_hashes: HashSet<ProtocolHash> = HashSet::new();

    // run init storage for all nets
    let mut environment_counter = 0;
    TezosEnvironment::iter().for_each(|net| {
        environment_counter += 1;

        let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
            .get(&net)
            .unwrap_or_else(|| panic!("no tezos environment configured for: {:?}", &net));

        let storage = TezosContextStorageConfiguration::Both(
            TezosContextIrminStorageConfiguration {
                data_dir: common::prepare_empty_dir(storage_data_dir),
            },
            TezosContextTezEdgeStorageConfiguration {
                backend: tezos_api::ffi::ContextKvStoreConfiguration::InMemGC,
                ipc_socket_path: None,
            },
        );
        let context_config = TezosContextConfiguration {
            storage,
            genesis: tezos_env.genesis.clone(),
            protocol_overrides: tezos_env.protocol_overrides.clone(),
            commit_genesis: true,
            enable_testchain: false,
            readonly: false,
            sandbox_json_patch_context: None,
            context_stats_db_path: None,
        };

        match client::init_protocol_context(context_config) {
            Err(e) => panic!(
                "Failed to initialize storage for: {:?}, Reason: {:?}",
                net, e
            ),
            Ok(init_info) => {
                if let Some(commit_hash) = &init_info.genesis_commit_hash {
                    genesis_commit_hashes.push(commit_hash.clone());
                }
                init_info
                    .supported_protocol_hashes
                    .iter()
                    .for_each(|protocol_hash| {
                        protocol_hashes.insert(protocol_hash.clone());
                    });
            }
        }
    });

    // check result - we should have
    assert_eq!(environment_counter, genesis_commit_hashes.len());
    assert!(protocol_hashes.len() > 1);
}

#[test]
fn test_init_empty_context_for_sandbox_with_patch_json() -> Result<(), anyhow::Error> {
    // init runtime and turn on/off ocaml logging
    client::change_runtime_configuration(TezosRuntimeConfiguration {
        debug_mode: false,
        compute_context_action_tree_hashes: false,
        log_enabled: common::is_ocaml_log_enabled(),
    })
    .unwrap();

    // prepare data
    let storage_data_dir = "init_storage_tests_02";

    // run init storage for all nets
    let net = TezosEnvironment::Sandbox;
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&net)
        .unwrap_or_else(|| panic!("no tezos environment configured for: {:?}", &net));

    let patch_context = PatchContext {
        key: String::from("sandbox_parameter"),
        json: String::from(
            r#" { "genesis_pubkey": "edpkuSLWfVU1Vq7Jg9FucPyKmma6otcMHac9zG4oU1KMHSTBpJuGQ2"} "#,
        ),
    };

    let storage = TezosContextStorageConfiguration::Both(
        TezosContextIrminStorageConfiguration {
            data_dir: common::prepare_empty_dir(storage_data_dir),
        },
        TezosContextTezEdgeStorageConfiguration {
            backend: tezos_api::ffi::ContextKvStoreConfiguration::InMemGC,
            ipc_socket_path: None,
        },
    );
    let context_config = TezosContextConfiguration {
        storage,
        genesis: tezos_env.genesis.clone(),
        protocol_overrides: tezos_env.protocol_overrides.clone(),
        commit_genesis: true,
        enable_testchain: false,
        readonly: false,
        sandbox_json_patch_context: Some(patch_context),
        context_stats_db_path: None,
    };

    match client::init_protocol_context(context_config) {
        Err(e) => panic!(
            "Failed to initialize storage for: {:?}, Reason: {:?}",
            net, e
        ),
        Ok(init_info) => {
            if let Some(commit_hash) = &init_info.genesis_commit_hash {
                assert_eq!(
                    *commit_hash,
                    ContextHash::try_from("CoVBYdAGWBoDTkiVXJEGX6FQvDN1oGCPJu8STMvaTYdeh7N3KGTz")?
                )
            } else {
                panic!("Expected some context hash")
            }
        }
    }

    Ok(())
}

#[test]
fn test_init_empty_context_for_sandbox_without_patch_json() -> Result<(), anyhow::Error> {
    // init runtime and turn on/off ocaml logging
    client::change_runtime_configuration(TezosRuntimeConfiguration {
        debug_mode: false,
        compute_context_action_tree_hashes: false,
        log_enabled: common::is_ocaml_log_enabled(),
    })
    .unwrap();

    // prepare data
    let storage_data_dir = "init_storage_tests_03";

    // run init storage for all nets
    let net = TezosEnvironment::Sandbox;
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&net)
        .unwrap_or_else(|| panic!("no tezos environment configured for: {:?}", &net));

    let storage = TezosContextStorageConfiguration::Both(
        TezosContextIrminStorageConfiguration {
            data_dir: common::prepare_empty_dir(storage_data_dir),
        },
        TezosContextTezEdgeStorageConfiguration {
            backend: tezos_api::ffi::ContextKvStoreConfiguration::InMemGC,
            ipc_socket_path: None,
        },
    );
    let context_config = TezosContextConfiguration {
        storage,
        genesis: tezos_env.genesis.clone(),
        protocol_overrides: tezos_env.protocol_overrides.clone(),
        commit_genesis: true,
        enable_testchain: false,
        readonly: false,
        sandbox_json_patch_context: None,
        context_stats_db_path: None,
    };

    match client::init_protocol_context(context_config) {
        Err(e) => panic!(
            "Failed to initialize storage for: {:?}, Reason: {:?}",
            net, e
        ),
        Ok(init_info) => {
            if let Some(commit_hash) = &init_info.genesis_commit_hash {
                assert_eq!(
                    *commit_hash,
                    ContextHash::try_from("CoVewPVcrKctWXSbrRgoGD6NmkdbDhmTFk5oi1FZpEcRT3bmKxdQ")?
                )
            } else {
                panic!("Expected some context hash")
            }
        }
    }

    Ok(())
}
