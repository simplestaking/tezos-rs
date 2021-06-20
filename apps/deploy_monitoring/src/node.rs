// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryInto;

use anyhow::{bail, format_err};
use async_trait::async_trait;
use fs_extra::dir;
use itertools::Itertools;
use merge::Merge;

use sysinfo::{ProcessExt, System, SystemExt};

use shell::stats::memory::{MemoryData, ProcessMemoryStats};

use crate::constants::{DEBUGGER_VOLUME_PATH, OCAML_VOLUME_PATH, TEZEDGE_VOLUME_PATH};
use crate::display_info::NodeInfo;
use crate::display_info::{OcamlDiskData, TezedgeDiskData};
use crate::image::DeployMonitoringContainer;

pub struct TezedgeNode;

#[async_trait]
impl Node for TezedgeNode {}

impl DeployMonitoringContainer for TezedgeNode {
    const NAME: &'static str = "deploy-monitoring-tezedge-node";
}

impl TezedgeNode {
    pub fn collect_disk_data() -> Result<TezedgeDiskData, anyhow::Error> {
        // context actions DB is optional
        let context_actions = dir::get_size(&format!(
            "{}/{}",
            TEZEDGE_VOLUME_PATH, "bootstrap_db/context_actions"
        ))
        .unwrap_or(0);

        let disk_data = TezedgeDiskData::new(
            dir::get_size(&format!("{}/{}", DEBUGGER_VOLUME_PATH, "tezedge")).unwrap_or(0),
            dir::get_size(&format!("{}/{}", TEZEDGE_VOLUME_PATH, "context")).unwrap_or(0),
            dir::get_size(&format!(
                "{}/{}",
                TEZEDGE_VOLUME_PATH, "bootstrap_db/context"
            ))
            .unwrap_or(0),
            dir::get_size(&format!(
                "{}/{}",
                TEZEDGE_VOLUME_PATH, "bootstrap_db/block_storage"
            ))
            .unwrap_or(0),
            context_actions,
            dir::get_size(&format!("{}/{}", TEZEDGE_VOLUME_PATH, "bootstrap_db/db")).unwrap_or(0),
        );

        Ok(disk_data)
    }

    pub async fn collect_protocol_runners_memory_stats(
        port: u16,
    ) -> Result<ProcessMemoryStats, anyhow::Error> {
        let protocol_runners: Vec<MemoryData> = match reqwest::get(&format!(
            "http://localhost:{}/stats/memory/protocol_runners",
            port
        ))
        .await
        {
            Ok(result) => result.json().await?,
            Err(e) => bail!("GET memory error: {}", e),
        };

        let memory_stats: ProcessMemoryStats = protocol_runners
            .into_iter()
            .map(|v| v.try_into().unwrap())
            .fold1(|mut m1: ProcessMemoryStats, m2| {
                m1.merge(m2);
                m1
            })
            .unwrap_or_default();

        Ok(memory_stats)
    }
}

pub struct OcamlNode;

#[async_trait]
impl Node for OcamlNode {}

impl DeployMonitoringContainer for OcamlNode {
    const NAME: &'static str = "deploy-monitoring-ocaml-node";
}

impl OcamlNode {
    pub fn collect_disk_data() -> Result<OcamlDiskData, anyhow::Error> {
        Ok(OcamlDiskData::new(
            dir::get_size(&format!("{}/{}", DEBUGGER_VOLUME_PATH, "tezos")).unwrap_or(0),
            dir::get_size(&format!("{}/{}", OCAML_VOLUME_PATH, "data/store")).unwrap_or(0),
            dir::get_size(&format!("{}/{}", OCAML_VOLUME_PATH, "data/context")).unwrap_or(0),
        ))
    }

    pub fn collect_validator_memory_stats() -> Result<ProcessMemoryStats, anyhow::Error> {
        let mut system = System::new_all();
        system.refresh_all();

        // collect all processes from the system
        let system_processes = system.get_processes();

        // collect all PIDs from process called tezos-node (ocaml node)
        let tezos_ocaml_processes: Vec<Option<i32>> = system_processes
            .iter()
            .filter(|(_, process)| process.name().contains("tezos-node"))
            .map(|(pid, _)| Some(*pid))
            .collect();

        // collect all processes that is the child of the main process and sum up the memory usage
        let valaidators: ProcessMemoryStats = system_processes
            .iter()
            .filter(|(_, process)| tezos_ocaml_processes.contains(&process.parent()))
            .map(|(_, process)| {
                ProcessMemoryStats::new(
                    (process.virtual_memory() * 1024).try_into().unwrap(),
                    (process.memory() * 1024).try_into().unwrap(),
                )
            })
            .fold1(|mut m1, m2| {
                m1.merge(m2);
                m1
            })
            .unwrap_or_default();

        Ok(valaidators)
    }
}

#[async_trait]
pub trait Node {
    async fn collect_head_data(port: u16) -> Result<NodeInfo, anyhow::Error> {
        let head_data: serde_json::Value = match reqwest::get(&format!(
            "http://localhost:{}/chains/main/blocks/head/header",
            port
        ))
        .await
        {
            Ok(result) => result.json().await?,
            Err(e) => bail!("GET header error: {}", e),
        };

        let head_metadata: serde_json::Value = match reqwest::get(&format!(
            "http://localhost:{}/chains/main/blocks/head/metadata",
            port
        ))
        .await
        {
            Ok(result) => result.json().await?,
            Err(e) => bail!("GET header error: {}", e),
        };

        Ok(NodeInfo::new(
            head_data["level"]
                .as_u64()
                .ok_or_else(|| format_err!("Level is not u64"))?,
            head_data["hash"]
                .as_str()
                .ok_or_else(|| format_err!("hash is not str"))?
                .to_string(),
            head_data["timestamp"]
                .as_str()
                .ok_or_else(|| format_err!("timestamp is not str"))?
                .to_string(),
            head_data["proto"]
                .as_u64()
                .ok_or_else(|| format_err!("Protocol is not u64"))?,
            head_metadata["level"]["cycle_position"]
                .as_u64()
                .unwrap_or(0),
            head_metadata["level"]["voting_period_position"]
                .as_u64()
                .unwrap_or(0),
            head_metadata["voting_period_kind"]
                .as_str()
                .unwrap_or("")
                .to_string(),
        ))

        // Ok(head_data)
    }

    async fn collect_memory_data(port: u16) -> Result<ProcessMemoryStats, anyhow::Error> {
        let tezedge_raw_memory_info: MemoryData =
            match reqwest::get(&format!("http://localhost:{}/stats/memory", port)).await {
                Ok(result) => result.json().await?,
                Err(e) => bail!("GET memory error: {}", e),
            };
        let memory_stats: ProcessMemoryStats = tezedge_raw_memory_info.try_into()?;

        Ok(memory_stats)
    }

    async fn collect_commit_hash(port: u16) -> Result<String, anyhow::Error> {
        let commit_hash =
            match reqwest::get(&format!("http://localhost:{}/monitor/commit_hash", port)).await {
                Ok(result) => result.text().await?,
                Err(e) => bail!("GET commit_hash error: {}", e),
            };

        Ok(commit_hash.trim_matches('"').trim_matches('\n').to_string())
    }

    fn collect_cpu_data(system: &mut System, process_name: &str) -> Result<i32, anyhow::Error> {
        // get node process
        Ok(system
            .get_processes()
            .iter()
            .filter(|(_, process)| process.name().contains(process_name))
            .map(|(_, process)| process.cpu_usage())
            .sum::<f32>() as i32)
    }
}
