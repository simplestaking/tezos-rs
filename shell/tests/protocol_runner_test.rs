// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]
extern crate test;

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::format_err;
use serial_test::serial;
use slog::{error, info, o, warn, Level, Logger};

use tezos_api::environment::{TezosEnvironmentConfiguration, TEZOS_ENV};
use tezos_api::ffi::TezosContextTezEdgeStorageConfiguration;
use tezos_api::ffi::{
    InitProtocolContextResult, TezosContextIrminStorageConfiguration,
    TezosContextStorageConfiguration, TezosRuntimeConfiguration,
};
use tezos_wrapper::runner::{ExecutableProtocolRunner, ProtocolRunner};
use tezos_wrapper::service::{IpcCmdServer, ProtocolRunnerEndpoint};
use tezos_wrapper::ProtocolEndpointConfiguration;
use tezos_wrapper::{TezosApiConnectionPool, TezosApiConnectionPoolConfiguration};

pub mod common;

#[ignore]
#[test]
#[serial]
fn test_mutliple_protocol_runners_with_one_write_multiple_read_init_context(
) -> Result<(), anyhow::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);
    let number_of_endpoints = 10;

    // We must have just one write and others can be readonly
    // we need to ensure, that first is write context created
    let mut flags_readonly = test_data::init_flags_readonly(number_of_endpoints);

    let context_db_path = PathBuf::from(common::prepare_empty_dir(
        "__shell_test_mutliple_protocol_runners",
    ));

    // spawn thread for init_protocol_context for every endpoint
    let mut handles = Vec::new();
    for i in 0..number_of_endpoints {
        // create endpoint
        let (mut protocol, child, endpoint_name) = create_endpoint::<ExecutableProtocolRunner>(
            log.clone(),
            log_level,
            format!("test_multiple_endpoint_{}", i),
            context_db_path.clone(),
        )?;

        // choose flag readonly
        let flag_readonly = flags_readonly
            .pop_front()
            .expect("Every thread should have defined flag!");

        // spawn thread for every endpoint and try to initialize protocol context
        let handle = thread::spawn(
            move || -> Result<InitProtocolContextResult, anyhow::Error> {
                // init protocol read or write
                match protocol.try_accept(Duration::from_secs(3)) {
                    Ok(proto) => Ok({
                        if flag_readonly {
                            proto.init_protocol_for_read()?
                        } else {
                            proto.init_protocol_for_write(false, &None, None)?
                        }
                    }),
                    Err(e) => Err(format_err!("{:?}", e)),
                }
            },
        );
        handles.push((handle, child, endpoint_name, flag_readonly));
    }

    // check result
    assert_eq!(number_of_endpoints as usize, handles.len());
    let mut success_counter = 0;
    for (handle, mut child, endpoint_name, flag_readonly) in handles {
        match handle.join().unwrap() {
            Ok(result) => {
                info!(log, "Init protocol context success"; "endpoint_name" => endpoint_name.clone(), "flag_readonly" => flag_readonly);
                match child.kill() {
                    Ok(_) => (),
                    Err(e) => {
                        warn!(log, "Failed to kill child process"; "endpoint_name" => endpoint_name.clone(), "flag_readonly" => flag_readonly, "error" => format!("{:?}", &e))
                    }
                };
                if !result.supported_protocol_hashes.is_empty() {
                    success_counter += 1;
                }
            }
            Err(e) => {
                error!(log, "Init protocol context error"; "endpoint_name" => endpoint_name.clone(), "flag_readonly" => flag_readonly, "error" => format!("{:?}", &e));
                match child.kill() {
                    Ok(_) => (),
                    Err(e) => {
                        warn!(log, "Failed to kill child process"; "endpoint_name" => endpoint_name.clone(), "flag_readonly" => flag_readonly, "error" => format!("{:?}", &e))
                    }
                };
            }
        }
    }

    assert_eq!(number_of_endpoints, success_counter);

    Ok(())
}

fn create_endpoint<Runner: ProtocolRunner + 'static>(
    log: Logger,
    log_level: Level,
    endpoint_name: String,
    context_db_path: PathBuf,
) -> Result<(IpcCmdServer, Runner::Subprocess, String), anyhow::Error> {
    // environement
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&test_data::TEZOS_NETWORK)
        .expect("no environment configuration");

    let storage = TezosContextStorageConfiguration::Both(
        TezosContextIrminStorageConfiguration {
            data_dir: context_db_path
                .to_str()
                .expect("Invalid context_db_path value")
                .to_string(),
        },
        TezosContextTezEdgeStorageConfiguration {
            backend: tezos_api::ffi::ContextKvStoreConfiguration::InMemGC,
            ipc_socket_path: None,
        },
    );

    // init protocol runner endpoint
    let protocol_runner = common::protocol_runner_executable_path();
    let protocol_runner_endpoint = ProtocolRunnerEndpoint::<Runner>::try_new(
        &endpoint_name,
        ProtocolEndpointConfiguration::new(
            TezosRuntimeConfiguration {
                log_enabled: common::is_ocaml_log_enabled(),
                debug_mode: false,
                compute_context_action_tree_hashes: false,
            },
            tezos_env.clone(),
            false,
            storage,
            &protocol_runner,
            log_level,
        ),
        log.new(o!("endpoint" => endpoint_name.clone())),
    )?;

    // start subprocess
    let (subprocess, protocol_commands) = match protocol_runner_endpoint.start() {
        Ok(subprocess) => {
            let ProtocolRunnerEndpoint { commands, .. } = protocol_runner_endpoint;
            (subprocess, commands)
        }
        Err(e) => {
            return Err(format_err!(
                "Error to start test_protocol_runner_endpoint: {} - error: {:?}",
                protocol_runner.as_os_str().to_str().unwrap_or("-none-"),
                e
            ));
        }
    };

    Ok((protocol_commands, subprocess, endpoint_name))
}

#[ignore]
#[test]
#[serial]
fn test_readonly_protocol_runner_connection_pool() -> Result<(), anyhow::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);
    let number_of_endpoints = 3;

    // environement
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&test_data::TEZOS_NETWORK)
        .expect("no environment configuration");

    // storage
    let context_db_path = PathBuf::from(common::prepare_empty_dir(
        "__shell_test_readonly_protocol_runner_pool",
    ));

    // init protocol runner endpoint
    let protocol_runner = common::protocol_runner_executable_path();

    // at first we need to create one writerable context, because of creating new one - see feature AT_LEAST_ONE_WRITE_PROTOCOL_CONTEXT_WAS_SUCCESS_AT_FIRST_LOCK
    let (mut write_context_commands, mut subprocess, ..) =
        create_endpoint::<ExecutableProtocolRunner>(
            log.clone(),
            log_level,
            "test_one_writeable_endpoint".to_string(),
            context_db_path.clone(),
        )?;

    let mut write_api = write_context_commands.try_accept(Duration::from_secs(3))?;
    let genesis_context_hash = write_api
        .init_protocol_for_write(true, &None, None)?
        .genesis_commit_hash
        .expect("Genesis context_hash should be commited!");
    write_api.shutdown()?;
    let _ =
        ExecutableProtocolRunner::wait_and_terminate_ref(&mut subprocess, Duration::from_secs(5));

    // cfg for pool
    let pool_cfg = TezosApiConnectionPoolConfiguration {
        min_connections: 0,
        max_connections: number_of_endpoints,
        connection_timeout: Duration::from_secs(1),
        max_lifetime: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(1),
    };

    let storage = TezosContextStorageConfiguration::Both(
        TezosContextIrminStorageConfiguration {
            data_dir: context_db_path
                .to_str()
                .expect("Invalid context_db_path value")
                .to_string(),
        },
        TezosContextTezEdgeStorageConfiguration {
            backend: tezos_api::ffi::ContextKvStoreConfiguration::InMemGC,
            ipc_socket_path: None,
        },
    );

    // cfg for protocol runner
    let endpoint_cfg = ProtocolEndpointConfiguration::new(
        TezosRuntimeConfiguration {
            log_enabled: common::is_ocaml_log_enabled(),
            debug_mode: false,
            compute_context_action_tree_hashes: false,
        },
        tezos_env.clone(),
        false,
        storage,
        &protocol_runner,
        log_level,
    );

    // create pool
    let pool_name = "test_pool_with_readonly_context";
    let pool_wrapper = Arc::new(TezosApiConnectionPool::new_with_readonly_context(
        pool_name.to_string(),
        pool_cfg,
        endpoint_cfg,
        log,
    )?);

    // create readonly pool pool
    let pool = &pool_wrapper.pool;
    assert_eq!(0, pool.state().connections);

    // test pool
    {
        {
            // acquire one
            assert!(&pool
                .get()?
                .api
                .genesis_result_data(&genesis_context_hash)
                .is_ok());
            assert_eq!(1, pool.state().connections);
            // released1
        }
        assert_eq!(1, pool.state().connections);

        // acquire (reuse connection and not release - api is not out of the scope yet)
        let api1 = &pool.get()?.api;
        assert!(api1.genesis_result_data(&genesis_context_hash).is_ok());
        assert_eq!(1, pool.state().connections);

        // acquire (create new connection and not release - api is not out of the scope yet)
        let api2 = &pool.get()?.api;
        assert!(api2.genesis_result_data(&genesis_context_hash).is_ok());
        assert_eq!(2, pool.state().connections);

        // acquire (create new connection and not release - api is not out of the scope yet)
        let api3 = &pool.get()?.api;
        assert!(api3.genesis_result_data(&genesis_context_hash).is_ok());
        assert_eq!(3, pool.state().connections);

        // try get another one (reached max number of connections)
        assert!(pool.try_get().is_none());
        assert_eq!(3, pool.state().connections);

        // get another one (reached max number of connections) - default connection_timeout
        let result = pool.get();
        assert!(result.is_err())

        // release all api1/2/3
    }
    assert_eq!(3, pool.state().connections);
    assert_eq!(3, pool.state().idle_connections);

    {
        // acquire (reused and not release)
        let api = &pool.get()?.api;
        assert!(api.genesis_result_data(&genesis_context_hash).is_ok());
        assert_eq!(3, pool.state().connections);
        assert_eq!(2, pool.state().idle_connections);
    }

    // use all and check
    {
        let expected_connection_names = vec![
            format!("{}_{}", pool_name, 1),
            format!("{}_{}", pool_name, 2),
            format!("{}_{}", pool_name, 3),
        ];

        let con1 = pool.get()?;
        assert!(expected_connection_names.contains(&con1.name));
        let api1 = &con1.api;
        assert!(api1.genesis_result_data(&genesis_context_hash).is_ok());

        let con2 = pool.get()?;
        assert!(expected_connection_names.contains(&con2.name));
        let api2 = &con2.api;
        assert!(api2.genesis_result_data(&genesis_context_hash).is_ok());

        let con3 = pool.get()?;
        assert!(expected_connection_names.contains(&con3.name));
        let api3 = &con3.api;
        assert!(api3.genesis_result_data(&genesis_context_hash).is_ok());
    }

    // 3 managed and all idle
    assert_eq!(3, pool.state().connections);
    assert_eq!(3, pool.state().idle_connections);

    // wait 35 seconds: connections are recreated after: <max_lifetime + 30> seconds
    thread::sleep(Duration::from_secs(40));

    // use all (recreated)
    {
        let expected_connection_names = vec![
            format!("{}_{}", pool_name, 4),
            format!("{}_{}", pool_name, 5),
            format!("{}_{}", pool_name, 6),
        ];

        let con1 = pool.get()?;
        assert!(expected_connection_names.contains(&con1.name));
        let api1 = &con1.api;
        assert!(api1.genesis_result_data(&genesis_context_hash).is_ok());

        let con2 = pool.get()?;
        assert!(expected_connection_names.contains(&con2.name));
        let api2 = &con2.api;
        assert!(api2.genesis_result_data(&genesis_context_hash).is_ok());

        let con3 = pool.get()?;
        assert!(expected_connection_names.contains(&con3.name));
        let api3 = &con3.api;
        assert!(api3.genesis_result_data(&genesis_context_hash).is_ok());
    }

    Ok(())
}

mod test_data {
    use std::collections::VecDeque;

    use rand::Rng;

    use tezos_api::environment::TezosEnvironment;

    pub const TEZOS_NETWORK: TezosEnvironment = TezosEnvironment::Carthagenet;

    /// Initialize all to readonly true and one set randomly to as 'write/false'
    pub fn init_flags_readonly(count: i32) -> VecDeque<bool> {
        // initialize all to readonly true
        let mut flags_readonly = VecDeque::new();

        // and one set randomly to as 'write/false'
        let mut rng = rand::thread_rng();
        let rand_index = rng.gen_range(0, count);

        for i in 0..count {
            if rand_index == i {
                flags_readonly.push_back(false);
            } else {
                flags_readonly.push_back(true);
            }
        }

        assert_eq!(flags_readonly.len(), count as usize);
        let mut count_false = 0;
        for f in &flags_readonly {
            if !f {
                count_false += 1;
            }
        }
        assert_eq!(count_false, 1);
        flags_readonly
    }
}
