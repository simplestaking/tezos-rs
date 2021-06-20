// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};

use getset::Getters;
use percentage::{Percentage, PercentageInteger};
use slog::{crit, Logger};

use shell::stats::memory::ProcessMemoryStats;

use crate::configuration::AlertThresholds;
use crate::constants::TEZEDGE_VOLUME_PATH;
use crate::display_info::NodeInfo;
use crate::slack::SlackServer;
use crate::ResourceUtilization;

#[derive(Debug, PartialEq)]
pub enum AlertResult {
    Incresed(MonitorAlert),
    Decreased(AlertLevel, MonitorAlert),
    Unchanged,
}

#[derive(Clone, Debug, Getters)]
pub struct Alerts {
    inner: HashSet<MonitorAlert>,

    #[get = "pub(crate)"]
    tezedge_thresholds: AlertThresholds,

    #[get = "pub(crate)"]
    ocaml_thresholds: AlertThresholds,
}

#[derive(Clone, Debug, Eq)]
pub struct MonitorAlert {
    node_tag: String,
    level: AlertLevel,
    kind: AlertKind,
    timestamp: Option<i64>,
    reported: bool,
    value: u64,
    head_info: NodeInfo,
}

impl PartialEq for MonitorAlert {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Hash for MonitorAlert {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.node_tag.hash(state);
    }
}

impl MonitorAlert {
    pub fn new(
        node_tag: &str,
        level: AlertLevel,
        kind: AlertKind,
        timestamp: Option<i64>,
        value: u64,
        head_info: NodeInfo,
    ) -> Self {
        Self {
            node_tag: node_tag.to_string(),
            level,
            kind,
            timestamp,
            reported: false,
            value,
            head_info,
        }
    }
}

impl Alerts {
    pub fn new(tezedge_thresholds: AlertThresholds, ocaml_thresholds: AlertThresholds) -> Self {
        Self {
            inner: HashSet::default(),
            tezedge_thresholds,
            ocaml_thresholds,
        }
    }

    pub fn assign_resource_alert(
        &mut self,
        node_tag: &str,
        kind: AlertKind,
        threshold: u64,
        value: u64,
        timestamp: Option<i64>,
        head_info: NodeInfo,
    ) -> AlertResult {
        let level = if value >= AlertLevel::Critical.value().apply_to(threshold) {
            AlertLevel::Critical
        } else {
            AlertLevel::NonAlert
        };

        let alert = MonitorAlert {
            node_tag: node_tag.to_string(),
            level,
            kind,
            timestamp,
            reported: false,
            value,
            head_info,
        };

        // decide if the alert has increased/decreased
        if self.inner.contains(&alert) {
            if let Some(previous_alert) = self.inner.get(&alert) {
                let previous_level = previous_alert.level.clone();
                if alert.level == AlertLevel::NonAlert {
                    self.inner.remove(&alert);
                    AlertResult::Decreased(previous_level, alert)
                } else if alert.level > previous_alert.level {
                    self.inner.replace(alert.clone());
                    AlertResult::Incresed(alert)
                } else if alert.level < previous_alert.level {
                    self.inner.replace(alert.clone());
                    AlertResult::Decreased(previous_level, alert)
                } else {
                    AlertResult::Unchanged
                }
            } else {
                AlertResult::Unchanged
            }
        } else if alert.level != AlertLevel::NonAlert {
            self.inner.insert(alert.clone());
            AlertResult::Incresed(alert)
        } else {
            AlertResult::Unchanged
        }
    }

    pub fn assign_node_stuck_alert(
        &mut self,
        node_tag: &str,
        thresholds: &AlertThresholds,
        last_checked_head_level: Option<u64>,
        // current_head_level: u64,
        current_time: i64,
        log: &Logger,
        head_info: NodeInfo,
    ) -> AlertResult {
        if let Some(last_checked_head_level) = last_checked_head_level {
            let current_head_level = *head_info.level();
            let head_alert = MonitorAlert::new(
                node_tag,
                AlertLevel::Critical,
                AlertKind::NodeStucked,
                Some(current_time),
                *head_info.level(),
                head_info,
            );

            if let Some(alert) = self.inner.get(&head_alert) {
                // The node is stuck when the level from the last measurement is the same as in the current measurement
                if last_checked_head_level == current_head_level {
                    // report the alert trough slack if it was not already reported
                    if alert.reported {
                        crit!(
                            log,
                            "[{}] Node still STUCK, already reported. LEVEL: {}",
                            node_tag,
                            current_head_level
                        )
                    } else {
                        // check for the threshold
                        // Note: When the node is synced, the blocks update in more or less fixed interval (1min on mainnet)
                        // so do not report the alert until it's time stuck exceeds a defined threshold
                        if current_time - alert.timestamp.unwrap_or(current_time)
                            > thresholds.synchronization
                        {
                            crit!(
                                log,
                                "[{}] Node STUCK! - LEVEL {}",
                                node_tag,
                                current_head_level
                            );

                            let mut modified = alert.clone();
                            modified.reported = true;

                            self.inner.replace(modified.clone());
                            return AlertResult::Incresed(modified);
                        } else {
                            crit!(
                                log,
                                "[{}] Node appears to be STUCK - LEVEL {}, time until alert: {}s",
                                node_tag,
                                current_head_level,
                                thresholds.synchronization
                                    - (current_time - alert.timestamp.unwrap_or(current_time))
                            );
                        }
                    }
                } else {
                    // When the node applies the next block, it becomes unstuck, report this
                    crit!(
                        log,
                        "[{}] Node unstuck. Level: {}",
                        node_tag,
                        current_head_level
                    );

                    let mut removed = self.inner.take(&head_alert).unwrap_or(head_alert);
                    removed.value = current_head_level;

                    return AlertResult::Decreased(removed.level.clone(), removed);
                }
            } else {
                // No alert was reported, node is stuck, insert alert, but do not notify trough slack, lets wait for the treshold
                if last_checked_head_level == current_head_level {
                    crit!(
                        log,
                        "[{}]Node appears to be stuck on level {}, time until alert: {}s",
                        node_tag,
                        current_head_level,
                        thresholds.synchronization
                    );
                    self.inner.insert(head_alert.clone());
                }
            }
        }
        AlertResult::Unchanged
    }

    pub async fn check_disk_alert(
        &mut self,
        node_tag: &str,
        thresholds: &AlertThresholds,
        slack: Option<&SlackServer>,
        time: i64,
        head_info: NodeInfo,
    ) -> Result<(), anyhow::Error> {
        // gets the total space on the filesystem of the specified path
        let free_disk_space = fs2::free_space(TEZEDGE_VOLUME_PATH)?;
        // let total_disk_space = fs2::total_space(TEZEDGE_VOLUME_PATH)?;
        let total_disk_space = fs2::total_space(TEZEDGE_VOLUME_PATH)?;

        // set it to a percentage of the max capacity
        let disk_threshold = 100 / thresholds.disk * total_disk_space;

        let res = self.assign_resource_alert(
            node_tag,
            AlertKind::Disk,
            disk_threshold,
            total_disk_space - free_disk_space,
            Some(time),
            head_info,
        );
        send_resource_alert(node_tag, slack, res).await?;
        Ok(())
    }

    pub async fn check_memory_alert(
        &mut self,
        node_tag: &str,
        thresholds: &AlertThresholds,
        slack: Option<&SlackServer>,
        time: i64,
        last_measurement: ResourceUtilization,
    ) -> Result<(), anyhow::Error> {
        let ram_total = if node_tag == "tezedge" {
            last_measurement.memory().node().resident_mem()
                + last_measurement
                    .memory()
                    .protocol_runners()
                    .as_ref()
                    .unwrap_or(&ProcessMemoryStats::default())
                    .resident_mem()
        } else {
            last_measurement.memory().node().resident_mem()
                + last_measurement
                    .memory()
                    .validators()
                    .as_ref()
                    .unwrap_or(&ProcessMemoryStats::default())
                    .resident_mem()
        };
        let res = self.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            thresholds.memory,
            ram_total as u64,
            Some(time),
            last_measurement.head_info().clone(),
        );

        send_resource_alert(node_tag, slack, res).await?;

        Ok(())
    }

    pub async fn check_cpu_alert(
        &mut self,
        node_tag: &str,
        thresholds: &AlertThresholds,
        slack: Option<&SlackServer>,
        time: i64,
        last_measurement: ResourceUtilization,
        head_info: NodeInfo,
    ) -> Result<(), anyhow::Error> {
        let cpu_total: i32 =
            last_measurement.cpu().node() + last_measurement.cpu().protocol_runners().unwrap_or(0);
        let res = self.assign_resource_alert(
            node_tag,
            AlertKind::Cpu,
            // TODO: TE-499 rework for multinode
            thresholds.cpu.unwrap(),
            cpu_total as u64,
            Some(time),
            head_info,
        );

        send_resource_alert(node_tag, slack, res).await?;

        Ok(())
    }

    pub async fn check_node_stuck_alert(
        &mut self,
        node_tag: &str,
        thresholds: &AlertThresholds,
        last_checked_head_level: Option<u64>,
        // current_head_level: u64,
        current_time: i64,
        slack: Option<&SlackServer>,
        log: &Logger,
        head_info: NodeInfo,
    ) -> Result<(), anyhow::Error> {
        let alert_result = self.assign_node_stuck_alert(
            node_tag,
            thresholds,
            last_checked_head_level,
            current_time,
            log,
            head_info.clone(),
        );
        if let Some(slack_server) = slack {
            match alert_result {
                AlertResult::Incresed(alert) => {
                    slack_server
                        .send_message(&format!(
                            ":warning: Node [{}] is stuck on level: {}\nHead Info: {}",
                            node_tag, alert.value, head_info
                        ))
                        .await?;
                }
                AlertResult::Decreased(_, alert) => {
                    if alert.reported {
                        slack_server
                            .send_message(&format!(
                                ":white_check_mark: Node [{}] is back to applying blocks on level: {}",
                                node_tag,
                                alert.value
                            ))
                            .await?;
                    }
                }
                AlertResult::Unchanged => (/* Do not alert on unchanged */),
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn contains(&mut self, kind: AlertKind, node_tag: &str) -> bool {
        // doesn't matter what level or value, just kind
        let alert = MonitorAlert::new(
            node_tag,
            AlertLevel::NonAlert,
            kind,
            None,
            0,
            NodeInfo::default(),
        );
        self.inner.contains(&alert)
    }

    #[cfg(test)]
    fn get(&mut self, kind: AlertKind, node_tag: &str) -> Option<&MonitorAlert> {
        // doesn't matter what level or value, just kind
        let alert = MonitorAlert::new(
            node_tag,
            AlertLevel::NonAlert,
            kind,
            None,
            0,
            NodeInfo::default(),
        );
        self.inner.get(&alert)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd)]
pub enum AlertLevel {
    NonAlert = 1,
    Critical = 2,
}

impl AlertLevel {
    pub fn value(&self) -> PercentageInteger {
        match *self {
            AlertLevel::NonAlert => Percentage::from(0),
            AlertLevel::Critical => Percentage::from(100),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AlertKind {
    Disk,
    Memory,
    Cpu,
    NodeStucked,
}

impl fmt::Display for AlertKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            AlertKind::Disk => write!(f, "TOTAL Disk space"),
            AlertKind::Memory => write!(f, "Memory"),
            AlertKind::Cpu => write!(f, "CPU"),
            AlertKind::NodeStucked => write!(f, "Synchronization"),
        }
    }
}

async fn send_resource_alert(
    node_tag: &str,
    slack: Option<&SlackServer>,
    alert_result: AlertResult,
) -> Result<(), anyhow::Error> {
    if let Some(slack_server) = slack {
        match alert_result {
            AlertResult::Incresed(alert) => {
                let current_value = match alert.kind {
                    AlertKind::Cpu => format!("{}%", alert.value),
                    AlertKind::Disk | AlertKind::Memory => {
                        format!("{}MB", alert.value / 1024 / 1024)
                    }
                    AlertKind::NodeStucked => format!("{} level", alert.value),
                };
                if alert.level == AlertLevel::Critical {
                    slack_server
                        .send_message(&format!(
                        ":warning: [{}] - {} surpassed the defined threshold! Current value: {}\nHead Info: {}",
                        node_tag,
                        alert.kind,
                        current_value,
                        alert.head_info,
                    ))
                        .await?;
                }
            }
            AlertResult::Decreased(previous_alert, alert) => {
                let current_value = match alert.kind {
                    AlertKind::Cpu => format!("{}%", alert.value),
                    AlertKind::Disk | AlertKind::Memory => {
                        format!("{}MB", alert.value / 1024 / 1024)
                    }
                    AlertKind::NodeStucked => format!("{} level", alert.value),
                };
                if previous_alert == AlertLevel::Critical {
                    slack_server
                    .send_message(&format!(
                        ":white_check_mark: [{}] - {} Decreased bellow of the defined threshold! Current value: {}",
                        node_tag,
                        alert.kind,
                        current_value
                    ))
                    .await?;
                }
            }
            AlertResult::Unchanged => (/* Do nothing */),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_alert() {
        const TOTAL_DISK_SPACE: u64 = 100_000_000_000;
        let threshold_percentage = 100;
        let node_tag = "tezedge";

        let threshold = threshold_percentage / 100 * TOTAL_DISK_SPACE;

        let mut alerts = Alerts::new(
            AlertThresholds {
                disk: threshold,
                memory: 0,
                synchronization: 0,
                cpu: Some(0),
            },
            AlertThresholds {
                disk: threshold,
                memory: 0,
                synchronization: 0,
                cpu: Some(0),
            },
        );

        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Disk,
            threshold,
            300_000_000_000,
            None,
            NodeInfo::default(),
        );

        let disk_alert = alerts.get(AlertKind::Disk, node_tag).unwrap();
        let expected = MonitorAlert {
            node_tag: node_tag.to_string(),
            kind: AlertKind::Disk,
            level: AlertLevel::Critical,
            timestamp: None,
            reported: false,
            value: 300_000_000_000,
            head_info: NodeInfo::default(),
        };

        assert_eq!(disk_alert.level, expected.level);

        // Critical
        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Disk,
            threshold,
            100_000_000_001,
            None,
            NodeInfo::default(),
        );
        let disk_alert = alerts.get(AlertKind::Disk, node_tag).unwrap();

        assert_eq!(disk_alert.level, expected.level);

        // NonAlert
        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Disk,
            threshold,
            39_999_999_999,
            None,
            NodeInfo::default(),
        );

        assert_eq!(alerts.inner.len(), 0);
    }

    #[test]
    fn test_alert_increase_decrease() {
        let node_tag = "tezedge";
        let memory = 1000;
        let threshold = 10000;

        let mut alerts = Alerts::new(
            AlertThresholds {
                memory: threshold,
                disk: 0,
                synchronization: 0,
                cpu: Some(0),
            },
            AlertThresholds {
                memory: threshold,
                disk: 0,
                synchronization: 0,
                cpu: Some(0),
            },
        );

        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            threshold,
            memory,
            None,
            NodeInfo::default(),
        );
        assert!(!alerts.contains(AlertKind::Memory, node_tag));

        // increase memory consuption, still NonAlert
        let memory = 2000;

        // this should replace the memory alert
        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            threshold,
            memory,
            None,
            NodeInfo::default(),
        );

        assert!(!alerts.contains(AlertKind::Memory, node_tag));
        assert_eq!(alerts.inner.len(), 0);

        // increased to Info alert
        let memory = 5000;

        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            threshold,
            memory,
            None,
            NodeInfo::default(),
        );

        // still 0 alert
        assert_eq!(alerts.inner.len(), 0);

        // increased to Critical alert
        let memory = 10001;

        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            threshold,
            memory,
            None,
            NodeInfo::default(),
        );

        // still only one alert, the memory alert, now with increased level
        assert!(alerts.contains(AlertKind::Memory, node_tag));
        assert_eq!(alerts.inner.len(), 1);
        assert_eq!(
            alerts.get(AlertKind::Memory, node_tag).unwrap().level,
            AlertLevel::Critical
        );

        let memory = 2000;

        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            threshold,
            memory,
            None,
            NodeInfo::default(),
        );

        // still only one alert now the level should decrease to NonAlert
        assert!(!alerts.contains(AlertKind::Memory, node_tag));
        assert_eq!(alerts.inner.len(), 0);
    }

    #[test]
    fn test_multiple_allerts() {
        let node_tag = "tezedge";
        let memory = 1000;
        let memory_threshold = 10000;

        const TOTAL_DISK_SPACE: u64 = 100_000_000_000;
        let disk_threshold_percentage = 100;

        let disk_threshold = disk_threshold_percentage / 100 * TOTAL_DISK_SPACE;

        let mut alerts = Alerts::new(
            AlertThresholds {
                memory: memory_threshold,
                disk: disk_threshold,
                synchronization: 0,
                cpu: Some(0),
            },
            AlertThresholds {
                memory: memory_threshold,
                disk: disk_threshold,
                synchronization: 0,
                cpu: Some(0),
            },
        );

        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            memory_threshold,
            memory,
            None,
            NodeInfo::default(),
        );
        assert!(!alerts.contains(AlertKind::Memory, node_tag));

        // increase memory consuption
        let memory = 1100;
        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            memory_threshold,
            memory,
            None,
            NodeInfo::default(),
        );

        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Disk,
            disk_threshold,
            300_000_000_000,
            None,
            NodeInfo::default(),
        );

        assert_eq!(
            alerts.get(AlertKind::Disk, node_tag).unwrap().level,
            AlertLevel::Critical
        );

        let memory = 1000;
        let disk = 100_100;

        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Disk,
            disk_threshold,
            disk,
            None,
            NodeInfo::default(),
        );
        alerts.assign_resource_alert(
            node_tag,
            AlertKind::Memory,
            memory_threshold,
            memory,
            None,
            NodeInfo::default(),
        );
        assert_eq!(alerts.inner.len(), 0);
    }

    #[test]
    fn test_node_stuck_alert() {
        let node_tag = "tezedge";
        let mut alerts = Alerts::new(
            AlertThresholds {
                disk: 0,
                memory: 0,
                synchronization: 300,
                cpu: Some(0),
            },
            AlertThresholds {
                disk: 0,
                memory: 0,
                synchronization: 300,
                cpu: Some(0),
            },
        );

        let thresholds = alerts.tezedge_thresholds().clone();

        // discard the logs
        let log = Logger::root(slog::Discard, slog::o!());
        let initial_time: i64 = 1617296614;

        let node_info = NodeInfo::new(
            126,
            String::default(),
            String::default(),
            1,
            125,
            125,
            String::default(),
        );

        // no alert should generate
        let res = alerts.assign_node_stuck_alert(
            node_tag,
            &thresholds,
            Some(125),
            initial_time,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 0);
        assert_eq!(res, AlertResult::Unchanged);

        // 5s in future - register the error, but do not report
        let res = alerts.assign_node_stuck_alert(
            node_tag,
            &thresholds,
            Some(126),
            initial_time + 5,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 1);
        assert_eq!(res, AlertResult::Unchanged);

        // 150s in future - error is still registered but the threshold is still not exceeded
        let res = alerts.assign_node_stuck_alert(
            node_tag,
            &thresholds,
            Some(126),
            initial_time + 150,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 1);
        assert_eq!(res, AlertResult::Unchanged);

        // 306s in future - error is still registered and now reported
        let mut expected = MonitorAlert::new(
            node_tag,
            AlertLevel::Critical,
            AlertKind::NodeStucked,
            Some(initial_time + 306),
            126,
            node_info.clone(),
        );
        expected.reported = true;
        let res = alerts.assign_node_stuck_alert(
            node_tag,
            &thresholds,
            Some(126),
            initial_time + 306,
            &log,
            node_info,
        );
        assert_eq!(alerts.inner.len(), 1);
        assert_eq!(res, AlertResult::Incresed(expected));
    }

    #[ignore]
    #[test]
    fn test_multiple_node_stuck_alert() {
        let node1_tag = "tezedge";
        let node2_tag = "ocaml";
        let mut alerts = Alerts::new(
            AlertThresholds {
                disk: 0,
                memory: 0,
                synchronization: 300,
                cpu: Some(0),
            },
            AlertThresholds {
                disk: 0,
                memory: 0,
                synchronization: 300,
                cpu: Some(0),
            },
        );

        let thresholds = alerts.tezedge_thresholds().clone();

        // discard the logs
        let log = Logger::root(slog::Discard, slog::o!());
        let initial_time: i64 = 1617296614;
        let node_info = NodeInfo::new(
            126,
            String::default(),
            String::default(),
            1,
            125,
            125,
            String::default(),
        );

        // no alert should generate
        let res = alerts.assign_node_stuck_alert(
            node1_tag,
            &thresholds,
            Some(125),
            initial_time,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 0);
        assert_eq!(res, AlertResult::Unchanged);

        let res = alerts.assign_node_stuck_alert(
            node2_tag,
            &thresholds,
            Some(125),
            initial_time,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 0);
        assert_eq!(res, AlertResult::Unchanged);

        // 5s in future - register the error, but do not report
        let res = alerts.assign_node_stuck_alert(
            node1_tag,
            &thresholds,
            Some(126),
            initial_time + 5,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 1);
        assert_eq!(res, AlertResult::Unchanged);

        let res = alerts.assign_node_stuck_alert(
            node2_tag,
            &thresholds,
            Some(126),
            initial_time + 5,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 2);
        assert_eq!(res, AlertResult::Unchanged);

        // 150s in future - error is still registered but the threshold is still not exceeded
        let res = alerts.assign_node_stuck_alert(
            node1_tag,
            &thresholds,
            Some(126),
            initial_time + 150,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 2);
        assert_eq!(res, AlertResult::Unchanged);

        let res = alerts.assign_node_stuck_alert(
            node2_tag,
            &thresholds,
            Some(126),
            initial_time + 150,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 2);
        assert_eq!(res, AlertResult::Unchanged);

        // 306s in future - error is still registered and now reported
        let mut expected1 = MonitorAlert::new(
            node1_tag,
            AlertLevel::Critical,
            AlertKind::NodeStucked,
            Some(initial_time + 306),
            126,
            node_info.clone(),
        );
        expected1.reported = true;
        let res = alerts.assign_node_stuck_alert(
            node1_tag,
            &thresholds,
            Some(126),
            initial_time + 306,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 2);
        assert_eq!(res, AlertResult::Incresed(expected1));

        let mut expected2 = MonitorAlert::new(
            node2_tag,
            AlertLevel::Critical,
            AlertKind::NodeStucked,
            Some(initial_time + 306),
            126,
            node_info.clone(),
        );
        expected2.reported = true;
        let res = alerts.assign_node_stuck_alert(
            node2_tag,
            &thresholds,
            Some(126),
            initial_time + 306,
            &log,
            node_info.clone(),
        );
        assert_eq!(alerts.inner.len(), 2);
        assert_eq!(res, AlertResult::Incresed(expected2));
    }
}
