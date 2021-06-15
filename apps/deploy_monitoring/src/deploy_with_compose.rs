// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::path::PathBuf;
use std::process::{Command, Output};

use slog::{info, Logger};
use tokio::time::{sleep, Duration};

use crate::constants::{DEBUGGER_PORT, EXPLORER_PORT, OCAML_PORT, TEZEDGE_PORT};
use crate::image::{DeployMonitoringContainer, Explorer, Sandbox, TezedgeDebugger, TezedgeMemprof};
use crate::node::{OcamlNode, TezedgeNode};

pub async fn launch_stack(
    compose_file_path: &PathBuf,
    log: &Logger,
    tezedge_only: bool,
    disable_debugger: bool,
) {
    info!(log, "Tezedge explorer is starting");
    start_with_compose(compose_file_path, Explorer::NAME, "explorer");
    wait_for_start(&format!("http://localhost:{}", EXPLORER_PORT)).await;

    info!(log, "Tezedge explorer is running");

    if !disable_debugger {
        info!(log, "Debugger is starting");
        start_with_compose(compose_file_path, TezedgeDebugger::NAME, "tezedge-debugger");
        wait_for_start(&format!("http://localhost:{}/v2/log", DEBUGGER_PORT)).await;
        info!(log, "Debugger is running");

        info!(log, "Memprof is starting");
        start_with_compose(compose_file_path, TezedgeMemprof::NAME, "tezedge-memprof");
        info!(log, "Memprof is running");
    }

    info!(log, "Tezedge node is starting");
    start_with_compose(compose_file_path, TezedgeNode::NAME, "tezedge-node");
    wait_for_start(&format!(
        "http://localhost:{}/chains/main/blocks/head/header",
        TEZEDGE_PORT
    ))
    .await;
    info!(log, "Tezedge node is running");

    if !tezedge_only {
        info!(log, "Ocaml node is starting");
        start_with_compose(compose_file_path, OcamlNode::NAME, "ocaml-node");
        wait_for_start(&format!(
            "http://localhost:{}/chains/main/blocks/head/header",
            OCAML_PORT
        ))
        .await;
        info!(log, "Ocaml node is running");
    }
}

pub async fn launch_sandbox(compose_file_path: &PathBuf, log: &Logger) {
    info!(log, "Debugger is running");
    start_with_compose(compose_file_path, TezedgeDebugger::NAME, "tezedge-debugger");
    wait_for_start(&format!("http://localhost:{}/v2/log", DEBUGGER_PORT)).await;
    info!(log, "Debugger is running");

    info!(log, "Memprof is starting");
    start_with_compose(compose_file_path, TezedgeMemprof::NAME, "tezedge-memprof");
    info!(log, "Memprof is running");

    info!(log, "Sandbox launcher starting");
    start_with_compose(compose_file_path, Sandbox::NAME, "tezedge-sandbox");
    wait_for_start("http://localhost:3030/list_nodes").await;
    info!(log, "Sandbox launcher running");
}

pub async fn restart_stack(
    compose_file_path: &PathBuf,
    log: &Logger,
    cleanup_data: bool,
    tezedge_only: bool,
    disable_debugger: bool,
) {
    stop_with_compose(compose_file_path);
    cleanup_docker(cleanup_data);
    launch_stack(compose_file_path, log, tezedge_only, disable_debugger).await;
}

pub async fn shutdown_and_update(
    compose_file_path: &PathBuf,
    log: &Logger,
    cleanup_data: bool,
    tezedge_only: bool,
    disable_debugger: bool,
) {
    stop_with_compose(compose_file_path);
    cleanup_docker_system();
    update_with_compose(compose_file_path);
    restart_stack(
        compose_file_path,
        log,
        cleanup_data,
        tezedge_only,
        disable_debugger,
    )
    .await;
}

pub async fn restart_sandbox(compose_file_path: &PathBuf, log: &Logger) {
    stop_with_compose(compose_file_path);
    cleanup_volumes();
    launch_sandbox(compose_file_path, log).await;
}

pub async fn shutdown_and_update_sandbox(compose_file_path: &PathBuf, log: &Logger, cleanup: bool) {
    stop_with_compose(compose_file_path);
    cleanup_docker(cleanup);
    update_with_compose(compose_file_path);
    restart_sandbox(compose_file_path, log).await;
}

pub fn cleanup_docker(cleanup_data: bool) {
    cleanup_docker_system();
    if cleanup_data {
        cleanup_volumes();
    }
}

pub fn start_with_compose(
    compose_file_path: &PathBuf,
    container_name: &str,
    service_ports_name: &str,
) -> Output {
    Command::new("docker-compose")
        .args(&[
            "-f",
            compose_file_path
                .to_str()
                .unwrap_or("apps/deploy_monitoring/docker-compose.deploy.latest.yml"),
            "run",
            "-d",
            "--name",
            container_name,
            "--service-ports",
            service_ports_name,
        ])
        .output()
        .expect("failed to execute docker-compose command")
}

pub fn stop_with_compose(compose_file_path: &PathBuf) -> Output {
    Command::new("docker-compose")
        .args(&[
            "-f",
            compose_file_path
                .to_str()
                .unwrap_or("apps/deploy_monitoring/docker-compose.deploy.latest.yml"),
            "down",
        ])
        .output()
        .expect("failed to execute docker-compose command")
}

pub fn update_with_compose(compose_file_path: &PathBuf) -> Output {
    Command::new("docker-compose")
        .args(&[
            "-f",
            compose_file_path
                .to_str()
                .unwrap_or("apps/deploy_monitoring/docker-compose.deploy.latest.yml"),
            "pull",
        ])
        .output()
        .expect("failed to execute docker-compose command")
}

pub fn cleanup_volumes() -> Output {
    Command::new("docker")
        .args(&["volume", "prune", "-f"])
        .output()
        .expect("failed to execute docker command")
}

pub fn cleanup_docker_system() -> Output {
    Command::new("docker")
        .args(&["system", "prune", "-a", "-f"])
        .output()
        .expect("failed to execute docker command")
}

async fn wait_for_start(url: &str) {
    while reqwest::get(url).await.is_err() {
        sleep(Duration::from_millis(1000)).await;
    }
}
