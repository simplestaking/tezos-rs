// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::path::PathBuf;
use std::convert::TryInto;

use failure::{bail, format_err};
use fs_extra::dir;
use getset::{CopyGetters, Getters, Setters};
use itertools::Itertools;
use merge::Merge;

use sysinfo::{ProcessExt, System, SystemExt};

use crate::monitors::resource::ProcessMemoryStats;

use crate::constants::DEBUGGER_VOLUME_PATH;
use crate::display_info::NodeInfo;
use crate::display_info::{DiskData, OcamlDiskData, TezedgeDiskData};

#[derive(Clone, Debug, PartialEq)]
pub enum NodeType {
    Ocaml,
    Tezedge,
}

#[derive(Clone, Debug, CopyGetters, Setters, Getters)]
pub struct Node {
    #[get_copy = "pub"]
    port: u16,

    #[get = "pub"]
    tag: String,

    #[set = "pub"]
    pid: Option<i32>,

    #[get = "pub"]
    volume_path: PathBuf,

    #[get = "pub"]
    node_type: NodeType,
}

impl Node {
    pub fn new(
        port: u16,
        tag: String,
        pid: Option<i32>,
        volume_path: PathBuf,
        node_type: NodeType,
    ) -> Self {
        Self {
            port,
            tag,
            pid,
            volume_path,
            node_type,
        }
    }

    pub fn collect_disk_data(&self) -> Result<DiskData, failure::Error> {
        let volume_path = self.volume_path.as_path().display();
        if self.node_type == NodeType::Tezedge {
            // context actions DB is optional
            let context_actions = dir::get_size(&format!(
                "{}/{}",
                volume_path, "bootstrap_db/context_actions"
            ))
            .unwrap_or(0);

            let disk_data = TezedgeDiskData::new(
                dir::get_size(&format!("{}/{}", DEBUGGER_VOLUME_PATH, "tezedge")).unwrap_or(0),
                dir::get_size(&format!("{}/{}", volume_path, "context")).unwrap_or(0),
                dir::get_size(&format!("{}/{}", volume_path, "bootstrap_db/context"))
                    .unwrap_or(0),
                dir::get_size(&format!(
                    "{}/{}",
                    volume_path, "bootstrap_db/block_storage"
                ))
                .unwrap_or(0),
                context_actions,
                dir::get_size(&format!("{}/{}", volume_path, "bootstrap_db/db")).unwrap_or(0),
            );

            Ok(DiskData::Tezedge(disk_data))
        } else {
            Ok(DiskData::Ocaml(OcamlDiskData::new(
                dir::get_size(&format!("{}/{}", DEBUGGER_VOLUME_PATH, "tezos")).unwrap_or(0),
                dir::get_size(&format!("{}/{}", volume_path, "data/store")).unwrap_or(0),
                dir::get_size(&format!("{}/{}", volume_path, "data/context")).unwrap_or(0),
            )))
        }
    }

    pub async fn collect_head_data(&self) -> Result<NodeInfo, failure::Error> {
        let head_data: serde_json::Value = match reqwest::get(&format!(
            "http://localhost:{}/chains/main/blocks/head/header",
            self.port
        ))
        .await
        {
            Ok(result) => result.json().await?,
            Err(e) => bail!("GET header error: {}", e),
        };

        let head_metadata: serde_json::Value = match reqwest::get(&format!(
            "http://localhost:{}/chains/main/blocks/head/metadata",
            self.port
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

    }

    pub fn collect_memory_stats(
        &self,
        system: &mut System,
    ) -> Result<ProcessMemoryStats, failure::Error> {
        if let Some(pid) = self.pid {
            if let Some(process) = system.process(pid) {
                Ok(ProcessMemoryStats::new(
                    (process.virtual_memory() * 1024).try_into().unwrap(),
                    (process.memory() * 1024).try_into().unwrap(),
                ))
            } else {
                Err(format_err!("Process with PID {} not found", pid))
            }
        } else {
            Err(format_err!(
                "Node was not registered with PID {:?}",
                self.pid
            ))
        }
    }

    pub fn collect_memory_stats_children(
        &self,
        system: &mut System,
        children_name: &str,
    ) -> Result<ProcessMemoryStats, failure::Error> {
        if let Some(pid) = self.pid {
            // collect all processes from the system
            let system_processes = system.processes();

            // collect all processes that is the child of the main process and sum up the memory usage
            let children: ProcessMemoryStats = system_processes
                .iter()
                .filter(|(_, process)| {
                    process.parent() == Some(pid) && process.name().eq(children_name)
                })
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

            Ok(children)
        } else {
            Err(format_err!(
                "Node was not registered with PID {:?}",
                self.pid
            ))
        }
    }

    // pub async fn collect_commit_hash(&self) -> Result<String, failure::Error> {
    //     let commit_hash =
    //         match reqwest::get(&format!("http://localhost:{}/monitor/commit_hash", self.port)).await {
    //             Ok(result) => result.text().await?,
    //             Err(e) => bail!("GET commit_hash error: {}", e),
    //         };

    //     Ok(commit_hash.trim_matches('"').trim_matches('\n').to_string())
    // }

    pub fn collect_cpu_data(&self, system: &mut System) -> Result<i32, failure::Error> {
        if let Some(pid) = self.pid {
            if let Some(process) = system.process(pid) {
                Ok(process.cpu_usage() as i32)
            } else {
                Err(format_err!("Process with PID {} not found", pid))
            }
        } else {
            Err(format_err!(
                "Node was not registered with PID {:?}",
                self.pid
            ))
        }
    }

    pub fn collect_cpu_data_children(
        &self,
        system: &mut System,
        children_name: &str,
    ) -> Result<i32, failure::Error> {
        if let Some(pid) = self.pid {
            // collect all processes from the system
            let system_processes = system.processes();

            // collect all processes that is the child of the main process and sum up the cpu usage
            Ok(system_processes
                .iter()
                .filter(|(_, process)| {
                    process.parent() == Some(pid) && process.name().eq(children_name)
                })
                .map(|(_, process)| process.cpu_usage())
                .sum::<f32>() as i32)
        } else {
            Err(format_err!(
                "Node was not registered with PID {:?}",
                self.pid
            ))
        }
    }
}
