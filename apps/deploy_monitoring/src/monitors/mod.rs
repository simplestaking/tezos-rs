// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use shiplift::Docker;
use slog::{error, info, Logger};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::configuration::DeployMonitoringEnvironment;
use crate::constants::MEASUREMENTS_MAX_CAPACITY;
use crate::deploy_with_compose::{
    cleanup_docker, restart_sandbox, restart_stack, stop_with_compose,
};
use crate::monitors::alerts::Alerts;
use crate::monitors::deploy::DeployMonitor;
use crate::monitors::resource::{
    ResourceMonitor, ResourceUtilization, ResourceUtilizationStorageMap,
};
use crate::rpc;
use crate::slack::SlackServer;

pub mod alerts;
pub mod deploy;
pub mod resource;

pub fn start_deploy_monitoring(
    compose_file_path: PathBuf,
    slack: Option<SlackServer>,
    interval: u64,
    log: Logger,
    running: Arc<AtomicBool>,
    cleanup_data: bool,
    tezedge_only: bool,
) -> JoinHandle<()> {
    let docker = Docker::new();
    let deploy_monitor = DeployMonitor::new(
        compose_file_path,
        docker,
        slack,
        log.clone(),
        cleanup_data,
        tezedge_only,
    );
    tokio::spawn(async move {
        while running.load(Ordering::Acquire) {
            if let Err(e) = deploy_monitor.monitor_stack().await {
                error!(log, "Deploy monitoring error: {}", e);
            }
            sleep(Duration::from_secs(interval)).await;
        }
    })
}

pub fn start_sandbox_monitoring(
    compose_file_path: PathBuf,
    slack: Option<SlackServer>,
    interval: u64,
    log: Logger,
    running: Arc<AtomicBool>,
    cleanup_data: bool,
    tezedge_only: bool,
) -> JoinHandle<()> {
    let docker = Docker::new();
    let deploy_monitor = DeployMonitor::new(
        compose_file_path,
        docker,
        slack,
        log.clone(),
        cleanup_data,
        tezedge_only,
    );
    tokio::spawn(async move {
        while running.load(Ordering::Acquire) {
            if let Err(e) = deploy_monitor.monitor_sandbox_launcher().await {
                error!(log, "Sandbox launcher monitoring error: {}", e);
            }
            sleep(Duration::from_secs(interval)).await;
        }
    })
}

pub fn start_resource_monitoring(
    env: &DeployMonitoringEnvironment,
    log: Logger,
    running: Arc<AtomicBool>,
    resource_utilization: ResourceUtilizationStorageMap,
    slack: Option<SlackServer>,
) -> JoinHandle<()> {
    let DeployMonitoringEnvironment {
        tezedge_alert_thresholds,
        ocaml_alert_thresholds,
        resource_monitor_interval,
        ..
    } = env;

    let alerts = Alerts::new(*tezedge_alert_thresholds, *ocaml_alert_thresholds);
    let mut resource_monitor = ResourceMonitor::new(
        resource_utilization,
        HashMap::new(),
        alerts,
        log.clone(),
        slack,
    );

    let resource_monitor_interval = *resource_monitor_interval;
    tokio::spawn(async move {
        while running.load(Ordering::Acquire) {
            if let Err(e) = resource_monitor.take_measurement().await {
                error!(log, "Resource monitoring error: {}", e);
            }
            sleep(Duration::from_secs(resource_monitor_interval)).await;
        }
    })
}

pub async fn shutdown_and_cleanup(
    compose_file_path: &PathBuf,
    slack: Option<SlackServer>,
    log: &Logger,
    cleanup_data: bool,
) -> Result<(), anyhow::Error> {
    if let Some(slack_server) = slack {
        slack_server.send_message("Manual shuttdown ").await?;
    }
    info!(log, "Manual shutdown");

    stop_with_compose(compose_file_path);
    cleanup_docker(cleanup_data);

    Ok(())
}

pub async fn start_stack(
    env: &DeployMonitoringEnvironment,
    slack: Option<SlackServer>,
    log: &Logger,
) -> Result<(), anyhow::Error> {
    info!(log, "Starting tezedge stack");

    let DeployMonitoringEnvironment {
        cleanup_volumes,
        compose_file_path,
        tezedge_alert_thresholds,
        ocaml_alert_thresholds,
        tezedge_only,
        ..
    } = env;

    // cleanup possible dangling containers/volumes and start the stack
    restart_stack(&compose_file_path, log, *cleanup_volumes, *tezedge_only).await;
    if let Some(slack_server) = slack {
        slack_server.send_message("Tezedge stack started").await?;
        slack_server
            .send_message(&format!(
                "Alert thresholds for tezedge nodes set to: {}",
                tezedge_alert_thresholds
            ))
            .await?;
        slack_server
            .send_message(&format!(
                "Alert thresholds for ocaml nodes set to: {}",
                ocaml_alert_thresholds
            ))
            .await?;
    }
    Ok(())
}

pub async fn start_sandbox(
    compose_file_path: &PathBuf,
    slack: Option<SlackServer>,
    log: &Logger,
) -> Result<(), anyhow::Error> {
    info!(log, "Starting tezedge stack");

    // cleanup possible dangling containers/volumes and start the stack
    restart_sandbox(compose_file_path, log).await;
    if let Some(slack_server) = slack {
        slack_server
            .send_message("Tezedge sandbox launcher started")
            .await?;
    }
    Ok(())
}

pub async fn spawn_sandbox(
    env: DeployMonitoringEnvironment,
    slack_server: Option<SlackServer>,
    log: &Logger,
    running: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    start_sandbox(&env.compose_file_path, slack_server.clone(), &log)
        .await
        .expect("Sandbox failed to start");

    info!(log, "Creating docker image monitor");
    if let Some(image_monitor_interval) = env.image_monitor_interval {
        let deploy_handle = start_sandbox_monitoring(
            env.compose_file_path.clone(),
            slack_server.clone(),
            image_monitor_interval,
            log.clone(),
            running.clone(),
            env.cleanup_volumes,
            env.tezedge_only,
        );

        vec![deploy_handle]
    } else {
        vec![]
    }
}

pub async fn spawn_node_stack(
    env: DeployMonitoringEnvironment,
    slack_server: Option<SlackServer>,
    log: &Logger,
    running: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    start_stack(&env, slack_server.clone(), &log)
        .await
        .expect("Stack failed to start");

    let mut handles = Vec::new();

    if let Some(image_monitor_interval) = env.image_monitor_interval {
        let deploy_handle = start_deploy_monitoring(
            env.compose_file_path.clone(),
            slack_server.clone(),
            image_monitor_interval,
            log.clone(),
            running.clone(),
            env.cleanup_volumes,
            env.tezedge_only,
        );
        handles.push(deploy_handle);
    }

    // TODO: TE-499 - (multiple nodes) rework this to load from a config, where all the nodes all defined
    // create a thread safe VecDeque for each node's resource utilization data
    let mut storage_map = HashMap::new();
    storage_map.insert(
        "tezedge",
        Arc::new(RwLock::new(VecDeque::<ResourceUtilization>::with_capacity(
            MEASUREMENTS_MAX_CAPACITY,
        ))),
    );
    if !env.tezedge_only {
        storage_map.insert(
            "ocaml",
            Arc::new(RwLock::new(VecDeque::<ResourceUtilization>::with_capacity(
                MEASUREMENTS_MAX_CAPACITY,
            ))),
        );
    }

    info!(log, "Creating reosurces monitor");
    let resources_handle = start_resource_monitoring(
        &env,
        log.clone(),
        running.clone(),
        storage_map.clone(),
        slack_server.clone(),
    );

    handles.push(resources_handle);

    info!(log, "Starting rpc server on port {}", &env.rpc_port);
    let rpc_server_handle = rpc::spawn_rpc_server(env.rpc_port, log.clone(), storage_map.clone());
    handles.push(rpc_server_handle);

    handles
}
