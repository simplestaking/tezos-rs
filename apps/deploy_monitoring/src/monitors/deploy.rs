// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::fs::File;
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::path::PathBuf;

use chrono::Utc;
use failure::bail;
use shiplift::Docker;
use slog::{info, warn, Logger};
use zip::write::ZipWriter;

use crate::deploy_with_compose::{
    restart_sandbox, restart_stack, shutdown_and_update, shutdown_and_update_sandbox,
};
use crate::image::{
    local_hash, remote_hash, DeployMonitoringContainer, Explorer, Sandbox, TezedgeDebugger,
    TezedgeMemprof,
};

use crate::constants::{DEBUGGER_PORT, TEZEDGE_NODE_P2P_PORT};
use crate::node::TezedgeNode;
use crate::slack::SlackServer;

pub struct DeployMonitor {
    compose_file_path: PathBuf,
    docker: Docker,
    slack: Option<SlackServer>,
    log: Logger,
    cleanup: bool,
    tezedge_only: bool,
    disable_debugger: bool,
    tezedge_volume_path: String,
}

impl DeployMonitor {
    pub fn new(
        compose_file_path: PathBuf,
        docker: Docker,
        slack: Option<SlackServer>,
        log: Logger,
        cleanup: bool,
        tezedge_only: bool,
        disable_debugger: bool,
        tezedge_volume_path: String,
    ) -> Self {
        Self {
            compose_file_path,
            docker,
            slack,
            log,
            cleanup,
            tezedge_only,
            disable_debugger,
            tezedge_volume_path,
        }
    }

    /// Collect node logs from the debugger with a step and stores them in a file
    /// format: [[<latest to latest - LIMIT>], ..., [<LIMIT to 0>]]
    async fn collect_node_logs(&self) -> Result<String, failure::Error> {
        const LIMIT: usize = 2000;
        if !Path::new("/tmp/tezedge-monitoring-logs").exists() {
            std::fs::create_dir("/tmp/tezedge-monitoring-logs")?;
        }
        let timestamp = Utc::now().time().to_string();
        let file_name = format!("{}_crash.log", &timestamp);
        let zip_name = format!("{}_crash.zip", &timestamp);
        let file_path = format!("/tmp/tezedge-monitoring-logs/{}", &zip_name);

        let mut zip_file = ZipWriter::new(File::create(&file_path)?);
        let zip_options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);

        zip_file.start_file(&file_name, zip_options)?;

        // enclose the arrays in an another array to make the file a valid json
        zip_file.write_all("[".as_bytes())?;

        // last log, get its cursor_id
        let mut cursor_id: Option<usize> = match reqwest::get(&format!(
            "http://localhost:{}/v2/log?node_name={}limit=1",
            DEBUGGER_PORT, TEZEDGE_NODE_P2P_PORT
        ))
        .await
        {
            Ok(result) => {
                let res_json = result.json::<serde_json::Value>().await?;
                res_json[0]["id"].as_u64().map(|val| val as usize)
            }
            Err(e) => {
                bail!("Error collecting last: {:?}", e);
            }
        };

        // "move" trough all the possible cursor_ids with a LIMIT step
        while let Some(cursor) = cursor_id {
            match reqwest::get(&format!(
                "http://localhost:{}/v2/log?node_name={}&limit={}&cursor_id={}",
                DEBUGGER_PORT, TEZEDGE_NODE_P2P_PORT, LIMIT, cursor
            ))
            .await
            {
                Ok(result) => {
                    zip_file.write_all(&result.bytes().await?)?;
                    zip_file.write_all(",".as_bytes())?;
                }
                Err(e) => bail!("Error collecting node logs from debugger: {:?}", e),
            }
            cursor_id = cursor.checked_sub(LIMIT);
        }

        zip_file.write_all("]".as_bytes())?;

        let log_file_name = format!("{}/tezedge.log", self.tezedge_volume_path);
        let log_file_path = Path::new(&log_file_name);
        if log_file_path.exists() {
            zip_file.start_file("tezedge.log", zip_options)?;
            let file = File::open(log_file_path)?;
            let reader = BufReader::new(file);

            for line in reader.lines() {
                zip_file.write_all(line?.as_bytes())?;
                zip_file.write_all(&[b'\n'])?;
            }
        }

        zip_file.finish()?;
        Ok(file_path)
    }

    pub async fn monitor_stack(&self) -> Result<(), failure::Error> {
        let DeployMonitor {
            slack,
            log,
            compose_file_path,
            ..
        } = self;

        if self.is_node_container_running().await {
            let node_updated = self.changed::<TezedgeNode>().await?;
            let explorer_updated = self.changed::<Explorer>().await?;
            // TODO: TE-499 here restart individually,
            // if debugger updated not need to restart explorer
            // if explorer updated, only need to restart explorer and so on...

            if self.disable_debugger {
                if node_updated || explorer_updated {
                    shutdown_and_update(
                        &compose_file_path,
                        log,
                        self.cleanup,
                        self.tezedge_only,
                        self.disable_debugger,
                    )
                    .await;
                } else {
                    // Do nothing, No update occurred
                    info!(self.log, "No image change detected");
                }
            } else {
                let debugger_updated = self.changed::<TezedgeDebugger>().await?;
                let memprof_updated = self.changed::<TezedgeMemprof>().await?;
                if node_updated || debugger_updated || explorer_updated || memprof_updated {
                    shutdown_and_update(
                        &compose_file_path,
                        log,
                        self.cleanup,
                        self.tezedge_only,
                        self.disable_debugger,
                    )
                    .await;
                } else {
                    // Do nothing, No update occurred
                    info!(self.log, "No image change detected");
                }
            }
        } else {
            warn!(self.log, "Node not running. Restarting stack");
            if let Some(slack_server) = slack {
                slack_server
                    .send_message("Node not running. Restarting stack")
                    .await?;
            }

            self.send_log_dump().await?;
            restart_stack(
                &compose_file_path,
                log,
                self.cleanup,
                self.tezedge_only,
                self.disable_debugger,
            )
            .await;
        };

        Ok(())
    }

    pub async fn monitor_sandbox_launcher(&self) -> Result<(), failure::Error> {
        let DeployMonitor {
            slack,
            log,
            compose_file_path,
            ..
        } = self;

        if self.is_sandbox_container_running().await {
            if self.changed::<Sandbox>().await? {
                shutdown_and_update_sandbox(&compose_file_path, log, self.cleanup).await;
            } else {
                // Do nothing, No update occurred
                info!(self.log, "No image change detected");
            }
        } else {
            warn!(self.log, "Sandbox launcher not running. Restarting");
            if let Some(slack_server) = slack {
                slack_server
                    .send_message("Sandbox launcher not running. Restarting")
                    .await?;
            }
            restart_sandbox(&compose_file_path, log).await;
        }

        Ok(())
    }

    async fn is_node_container_running(&self) -> bool {
        let DeployMonitor { docker, .. } = self;

        match docker.containers().get(TezedgeNode::NAME).inspect().await {
            Ok(container_data) => container_data.state.running,
            _ => false,
        }
    }

    async fn is_sandbox_container_running(&self) -> bool {
        let DeployMonitor { docker, .. } = self;

        match docker.containers().get(Sandbox::NAME).inspect().await {
            Ok(container_data) => container_data.state.running,
            _ => false,
        }
    }

    async fn send_log_dump(&self) -> Result<(), failure::Error> {
        let logs = serde_json::to_string(&self.collect_node_logs().await?)?;

        if let Some(slack_server) = &self.slack {
            slack_server
                .upload_file(":warning: Logs from debugger :warning:", &logs)
                .await?;
        }
        Ok(())
    }

    async fn changed<T: DeployMonitoringContainer + Sync + Send>(
        &self,
    ) -> Result<bool, failure::Error> {
        let DeployMonitor {
            docker, slack, log, ..
        } = self;

        let image = T::image().await?;
        let local_image_hash = local_hash::<T>(&docker).await?;
        let remote_image_hash = remote_hash::<T>().await?;

        if local_image_hash != remote_image_hash {
            slog::info!(
                log,
                "Image changed, local: {} != remote: {}",
                local_image_hash,
                remote_image_hash
            );
            if let Some(slack_server) = slack {
                slack_server
                    .send_message(&format!(
                        "{} image changing {} -> {}",
                        image, local_image_hash, remote_image_hash
                    ))
                    .await?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
