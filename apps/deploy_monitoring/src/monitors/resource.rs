// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::cmp;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use chrono::Utc;
use failure::format_err;
use getset::Getters;
use serde::Serialize;
use slog::{error, Logger};
use sysinfo::{System, SystemExt};

use shell::stats::memory::ProcessMemoryStats;

use crate::constants::{MEASUREMENTS_MAX_CAPACITY, OCAML_PORT, TEZEDGE_PORT};
use crate::display_info::{NodeInfo, OcamlDiskData, TezedgeDiskData};
use crate::monitors::Alerts;
use crate::node::OcamlNode;
use crate::node::{Node, TezedgeNode};
use crate::slack::SlackServer;

pub type ResourceUtilizationStorage = Arc<RwLock<VecDeque<ResourceUtilization>>>;
pub type ResourceUtilizationStorageMap = HashMap<&'static str, ResourceUtilizationStorage>;

pub struct ResourceMonitor {
    resource_utilization: ResourceUtilizationStorageMap,
    last_checked_head_level: HashMap<String, u64>,
    alerts: Alerts,
    log: Logger,
    slack: Option<SlackServer>,
    system: System,
    tezedge_volume_path: String,
}

#[derive(Clone, Debug, Serialize, Getters, Default)]
pub struct MemoryStats {
    #[get = "pub(crate)"]
    node: ProcessMemoryStats,

    // TODO: TE-499 remove protocol_runners and use validators for ocaml and tezedge type
    #[get = "pub(crate)"]
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol_runners: Option<ProcessMemoryStats>,

    #[get = "pub(crate)"]
    #[serde(skip_serializing_if = "Option::is_none")]
    validators: Option<ProcessMemoryStats>,
}

#[derive(Clone, Debug, Serialize, Getters)]
pub struct ResourceUtilization {
    #[get = "pub(crate)"]
    timestamp: i64,

    #[get = "pub(crate)"]
    memory: MemoryStats,

    #[get = "pub(crate)"]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "disk")]
    ocaml_disk: Option<OcamlDiskData>,

    #[get = "pub(crate)"]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "disk")]
    tezedge_disk: Option<TezedgeDiskData>,

    #[get = "pub(crate)"]
    cpu: CpuStats,

    #[get = "pub(crate)"]
    #[serde(skip)]
    head_info: NodeInfo,
}

impl ResourceUtilization {
    pub fn merge(&self, other: Self) -> Self {
        let merged_ocaml_disk = if let (Some(ocaml_disk1), Some(ocaml_disk2)) =
            (self.ocaml_disk.as_ref(), other.ocaml_disk)
        {
            Some(OcamlDiskData::new(
                cmp::max(ocaml_disk1.debugger(), ocaml_disk2.debugger()),
                cmp::max(ocaml_disk1.block_storage(), ocaml_disk2.block_storage()),
                cmp::max(ocaml_disk1.context_irmin(), ocaml_disk2.context_irmin()),
            ))
        } else {
            None
        };

        let merged_tezedge_disk = if let (Some(tezedge_disk1), Some(tezedge_disk2)) =
            (self.tezedge_disk.as_ref(), other.tezedge_disk)
        {
            Some(TezedgeDiskData::new(
                cmp::max(tezedge_disk1.debugger(), tezedge_disk2.debugger()),
                cmp::max(tezedge_disk1.context_irmin(), tezedge_disk2.context_irmin()),
                cmp::max(
                    tezedge_disk1.context_merkle_rocksdb(),
                    tezedge_disk2.context_merkle_rocksdb(),
                ),
                cmp::max(tezedge_disk1.block_storage(), tezedge_disk2.block_storage()),
                cmp::max(
                    tezedge_disk1.context_actions(),
                    tezedge_disk2.context_actions(),
                ),
                cmp::max(tezedge_disk1.main_db(), tezedge_disk2.main_db()),
            ))
        } else {
            None
        };

        let merged_protocol_runner_memory =
            if let (Some(protocol_runner_mem1), Some(protocol_runner_mem2)) = (
                self.memory.protocol_runners.as_ref(),
                other.memory.protocol_runners,
            ) {
                Some(ProcessMemoryStats::new(
                    cmp::max(
                        protocol_runner_mem1.virtual_mem(),
                        protocol_runner_mem2.virtual_mem(),
                    ),
                    cmp::max(
                        protocol_runner_mem1.resident_mem(),
                        protocol_runner_mem2.resident_mem(),
                    ),
                ))
            } else {
                None
            };

        let merged_validators_memory = if let (Some(validators_mem1), Some(validators_mem2)) =
            (self.memory.validators.as_ref(), other.memory.validators)
        {
            Some(ProcessMemoryStats::new(
                cmp::max(validators_mem1.virtual_mem(), validators_mem2.virtual_mem()),
                cmp::max(
                    validators_mem1.resident_mem(),
                    validators_mem2.resident_mem(),
                ),
            ))
        } else {
            None
        };

        Self {
            timestamp: cmp::max(self.timestamp, other.timestamp),
            cpu: CpuStats {
                node: cmp::max(self.cpu.node, other.cpu.node),
                protocol_runners: cmp::max(self.cpu.protocol_runners, other.cpu.protocol_runners),
            },
            memory: MemoryStats {
                node: ProcessMemoryStats::new(
                    cmp::max(
                        self.memory.node.virtual_mem(),
                        other.memory.node.virtual_mem(),
                    ),
                    cmp::max(
                        self.memory.node.resident_mem(),
                        other.memory.node.resident_mem(),
                    ),
                ),
                protocol_runners: merged_protocol_runner_memory,
                validators: merged_validators_memory,
            },
            ocaml_disk: merged_ocaml_disk,
            tezedge_disk: merged_tezedge_disk,
            // this is not present in the FE data, do not need to merge with max strategy
            head_info: other.head_info,
        }
    }
}

#[derive(Clone, Debug, Serialize, Getters, Default)]
pub struct CpuStats {
    #[get = "pub(crate)"]
    node: i32,

    #[get = "pub(crate)"]
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol_runners: Option<i32>,
}

impl ResourceMonitor {
    pub fn new(
        resource_utilization: ResourceUtilizationStorageMap,
        last_checked_head_level: HashMap<String, u64>,
        alerts: Alerts,
        log: Logger,
        slack: Option<SlackServer>,
        tezedge_volume_path: String,
    ) -> Self {
        Self {
            resource_utilization,
            last_checked_head_level,
            alerts,
            log,
            slack,
            system: System::new_all(),
            tezedge_volume_path,
        }
    }

    pub async fn take_measurement(&mut self) -> Result<(), failure::Error> {
        let ResourceMonitor {
            system,
            resource_utilization,
            log,
            last_checked_head_level,
            alerts,
            slack,
            tezedge_volume_path,
            ..
        } = self;

        system.refresh_all();

        for (node_tag, resource_storage) in resource_utilization {
            let node_resource_measurement = if node_tag == &"tezedge" {
                let current_head_info = TezedgeNode::collect_head_data(TEZEDGE_PORT).await?;
                let tezedge_node = TezedgeNode::collect_memory_data(TEZEDGE_PORT).await?;
                let protocol_runners =
                    TezedgeNode::collect_protocol_runners_memory_stats(TEZEDGE_PORT).await?;
                let tezedge_disk = TezedgeNode::collect_disk_data(tezedge_volume_path.to_string())?;

                let tezedge_cpu = TezedgeNode::collect_cpu_data(system, "light-node")?;
                let protocol_runners_cpu =
                    TezedgeNode::collect_cpu_data(system, "protocol-runner")?;
                let resources = ResourceUtilization {
                    timestamp: chrono::Local::now().timestamp(),
                    memory: MemoryStats {
                        node: tezedge_node,
                        protocol_runners: Some(protocol_runners),
                        validators: None,
                    },
                    tezedge_disk: Some(tezedge_disk),
                    ocaml_disk: None,
                    cpu: CpuStats {
                        node: tezedge_cpu,
                        protocol_runners: Some(protocol_runners_cpu),
                    },
                    head_info: current_head_info,
                };
                handle_alerts(
                    node_tag,
                    resources.clone(),
                    last_checked_head_level,
                    slack.clone(),
                    alerts,
                    log,
                )
                .await?;
                resources
            } else {
                let current_head_info = OcamlNode::collect_head_data(OCAML_PORT).await?;
                let ocaml_node = OcamlNode::collect_memory_data(OCAML_PORT).await?;
                let tezos_validators = OcamlNode::collect_validator_memory_stats()?;
                let ocaml_disk = OcamlNode::collect_disk_data()?;
                let ocaml_cpu = OcamlNode::collect_cpu_data(system, "tezos-node")?;

                let resources = ResourceUtilization {
                    timestamp: chrono::Local::now().timestamp(),
                    memory: MemoryStats {
                        node: ocaml_node,
                        protocol_runners: None,
                        validators: Some(tezos_validators),
                    },
                    ocaml_disk: Some(ocaml_disk),
                    tezedge_disk: None,
                    cpu: CpuStats {
                        node: ocaml_cpu,
                        protocol_runners: None,
                    },
                    head_info: current_head_info,
                };
                handle_alerts(
                    node_tag,
                    resources.clone(),
                    last_checked_head_level,
                    slack.clone(),
                    alerts,
                    log,
                )
                .await?;
                resources
            };

            match &mut resource_storage.write() {
                Ok(resources_locked) => {
                    if resources_locked.len() == MEASUREMENTS_MAX_CAPACITY {
                        resources_locked.pop_back();
                    }

                    resources_locked.push_front(node_resource_measurement.clone());
                }
                Err(e) => error!(log, "Resource lock poisoned, reason => {}", e),
            }
        }
        Ok(())
    }
}

async fn handle_alerts(
    node_tag: &str,
    last_measurement: ResourceUtilization,
    last_checked_head_level: &mut HashMap<String, u64>,
    slack: Option<SlackServer>,
    alerts: &mut Alerts,
    log: &Logger,
) -> Result<(), failure::Error> {
    // TODO: TE-499 - (multinode) - fix for multinode support
    let thresholds = if node_tag == "tezedge" {
        *alerts.tezedge_thresholds()
    } else if node_tag == "ocaml" {
        *alerts.ocaml_thresholds()
    } else {
        return Err(format_err!("Node [{}] not defined", node_tag));
    };

    // current time timestamp
    let current_time = Utc::now().timestamp();

    let last_head = last_checked_head_level.get(node_tag).copied();
    let current_head_info = last_measurement.head_info.clone();

    alerts
        .check_disk_alert(
            node_tag,
            &thresholds,
            slack.as_ref(),
            current_time,
            current_head_info.clone(),
        )
        .await?;
    alerts
        .check_memory_alert(
            node_tag,
            &thresholds,
            slack.as_ref(),
            current_time,
            last_measurement.clone(),
        )
        .await?;
    alerts
        .check_node_stuck_alert(
            node_tag,
            &thresholds,
            last_head,
            current_time,
            slack.as_ref(),
            log,
            current_head_info.clone(),
        )
        .await?;

    if thresholds.cpu.is_some() {
        alerts
            .check_cpu_alert(
                node_tag,
                &thresholds,
                slack.as_ref(),
                current_time,
                last_measurement.clone(),
                current_head_info.clone(),
            )
            .await?;
    }

    last_checked_head_level.insert(node_tag.to_string(), *current_head_info.level());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display_info::TezedgeDiskData;
    use itertools::Itertools;

    #[test]
    fn test_mergable_resources() {
        let resources1 = ResourceUtilization {
            cpu: CpuStats {
                node: 150,
                protocol_runners: Some(10),
            },
            tezedge_disk: TezedgeDiskData::new(1, 1, 1, 1, 1, 1).into(),
            ocaml_disk: None,
            memory: MemoryStats {
                node: ProcessMemoryStats::new(1000, 100),
                protocol_runners: Some(ProcessMemoryStats::new(1000, 100)),
                validators: None,
            },
            timestamp: 1,
            head_info: NodeInfo::default(),
        };

        let resources2 = ResourceUtilization {
            cpu: CpuStats {
                node: 200,
                protocol_runners: Some(20),
            },
            tezedge_disk: TezedgeDiskData::new(6, 5, 4, 3, 2, 125).into(),
            ocaml_disk: None,
            memory: MemoryStats {
                node: ProcessMemoryStats::new(2000, 200),
                protocol_runners: Some(ProcessMemoryStats::new(3000, 300)),
                validators: None,
            },
            timestamp: 2,
            head_info: NodeInfo::default(),
        };

        let resources3 = ResourceUtilization {
            cpu: CpuStats {
                node: 90,
                protocol_runners: Some(258),
                // validators: None,
            },
            tezedge_disk: TezedgeDiskData::new(12, 11, 10, 9, 8, 7).into(),
            ocaml_disk: None,
            memory: MemoryStats {
                node: ProcessMemoryStats::new(1500, 45000),
                protocol_runners: Some(ProcessMemoryStats::new(2500, 250)),
                validators: None,
            },
            timestamp: 3,
            head_info: NodeInfo::default(),
        };

        let expected = ResourceUtilization {
            cpu: CpuStats {
                node: 200,
                protocol_runners: Some(258),
                // validators: None,
            },
            tezedge_disk: TezedgeDiskData::new(12, 11, 10, 9, 8, 125).into(),
            ocaml_disk: None,
            memory: MemoryStats {
                node: ProcessMemoryStats::new(2000, 45000),
                protocol_runners: Some(ProcessMemoryStats::new(3000, 300)),
                validators: None,
            },
            timestamp: 3,
            head_info: NodeInfo::default(),
        };

        let resources = vec![resources1, resources2, resources3];
        let merged_final = resources.into_iter().fold1(|m1, m2| m1.merge(m2)).unwrap();

        assert_eq!(merged_final.cpu.node, expected.cpu.node);
        assert_eq!(
            merged_final.cpu.protocol_runners,
            expected.cpu.protocol_runners
        );
        assert_eq!(merged_final.tezedge_disk, expected.tezedge_disk);
        assert_eq!(merged_final.memory.node, expected.memory.node);
        assert_eq!(
            merged_final.memory.protocol_runners,
            expected.memory.protocol_runners
        );
        assert_eq!(merged_final.timestamp, expected.timestamp);
    }
}
