// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use core::convert::TryFrom;
use std::fmt;

use failure::format_err;
use getset::{CopyGetters, Getters};
use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct DiskSpaceData {
    total_disk_space: u64,
    free_disk_space: u64,
    tezedge: DiskData,
    ocaml: DiskData,
}

impl fmt::Display for DiskSpaceData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "\nFile system: \n\t Total space: {} MB\n\t Free space: {} MB\n\n{}{}",
            self.total_disk_space, self.free_disk_space, self.tezedge, self.ocaml,
        )
    }
}

#[derive(Clone, Serialize, Debug, Getters, CopyGetters, Eq, PartialEq, Default)]
pub struct NodeInfo {
    #[get = "pub(crate)"]
    level: u64,

    block_hash: String,

    #[get = "pub(crate)"]
    timestamp: String,

    #[get = "pub(crate)"]
    protocol: u64,

    #[get = "pub(crate)"]
    cycle_position: u64,

    #[get = "pub(crate)"]
    voting_period_position: u64,

    #[get = "pub(crate)"]
    voting_period: String,
}

impl NodeInfo {
    pub fn new(
        level: u64,
        block_hash: String,
        timestamp: String,
        protocol: u64,
        cycle_position: u64,
        voting_period_position: u64,
        voting_period: String,
    ) -> Self {
        Self {
            level,
            block_hash,
            timestamp,
            protocol,
            cycle_position,
            voting_period,
            voting_period_position,
        }
    }
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "\n\tLevel: {}\n\tCycle position: {}\n\tProtocol: {}\n\tVoting period: {}\n\tVoting period position: {}",
            self.level, self.cycle_position, self.protocol, self.voting_period, self.voting_period_position
        )
    }
}

#[derive(Serialize, PartialEq, Clone, Debug, Default, CopyGetters)]
pub struct OcamlDiskData {
    #[get_copy = "pub(crate)"]
    debugger: u64,

    #[get_copy = "pub(crate)"]
    block_storage: u64,

    #[get_copy = "pub(crate)"]
    context_irmin: u64,
}

impl fmt::Display for OcamlDiskData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total = self.block_storage + self.context_irmin;
        write!(
            f,
            "{} MB (total)\n\tBlock storage: {} MB\n\tContext: {} MB",
            total, self.block_storage, self.context_irmin,
        )
    }
}

impl OcamlDiskData {
    pub fn new(debugger: u64, block_storage: u64, context_irmin: u64) -> Self {
        Self {
            block_storage,
            context_irmin,
            debugger,
        }
    }

    pub fn to_megabytes(&self) -> Self {
        Self {
            block_storage: self.block_storage / 1024 / 1024,
            context_irmin: self.context_irmin / 1024 / 1024,
            debugger: self.debugger / 1024 / 1024,
        }
    }
}

#[derive(Serialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum DiskData {
    Ocaml(OcamlDiskData),
    Tezedge(TezedgeDiskData),
}

impl From<OcamlDiskData> for DiskData {
    fn from(data: OcamlDiskData) -> Self {
        DiskData::Ocaml(data)
    }
}

impl From<TezedgeDiskData> for DiskData {
    fn from(data: TezedgeDiskData) -> Self {
        DiskData::Tezedge(data)
    }
}

impl TryFrom<DiskData> for TezedgeDiskData {
    type Error = failure::Error;

    fn try_from(data: DiskData) -> Result<Self, failure::Error> {
        if let DiskData::Tezedge(disk) = data {
            Ok(disk)
        } else {
            Err(format_err!("Cannot conver to TezedgeDiskData "))
        }
    }
}

impl TryFrom<DiskData> for OcamlDiskData {
    type Error = failure::Error;

    fn try_from(data: DiskData) -> Result<Self, failure::Error> {
        if let DiskData::Ocaml(disk) = data {
            Ok(disk)
        } else {
            Err(format_err!("Cannot conver to OcamlDiskData "))
        }
    }
}

impl fmt::Display for DiskData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiskData::Tezedge(tezedge) => {
                write!(f, "Tezedge node: {}", tezedge.to_megabytes(),)
            }
            DiskData::Ocaml(ocaml) => {
                write!(f, "\nOcaml node: {}", ocaml.to_megabytes(),)
            }
        }
    }
}

#[derive(Serialize, PartialEq, Clone, Debug, Default, CopyGetters)]
pub struct TezedgeDiskData {
    #[get_copy = "pub(crate)"]
    context_irmin: u64,

    #[get_copy = "pub(crate)"]
    context_merkle_rocksdb: u64,

    #[get_copy = "pub(crate)"]
    block_storage: u64,

    #[get_copy = "pub(crate)"]
    context_actions: u64,

    #[get_copy = "pub(crate)"]
    main_db: u64,

    #[get_copy = "pub(crate)"]
    debugger: u64,
}

impl fmt::Display for TezedgeDiskData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let TezedgeDiskData {
            context_actions,
            context_irmin,
            context_merkle_rocksdb,
            block_storage,
            main_db,
            debugger,
        } = self;

        let total =
            context_actions + context_irmin + context_merkle_rocksdb + block_storage + main_db;
        writeln!(
            f,
            "{} MB (total)\n\tMain database: {} MB\n\tContex - irmin: {} MB\n\tContext - rust_merkel_tree: {} MB\n\tContext actions: {} MB\n\tBlock storage (commit log): {} MB\n\tDebugger: {} MB",
            total,
            main_db,
            context_irmin,
            context_merkle_rocksdb,
            context_actions,
            block_storage,
            debugger
        )
    }
}

impl TezedgeDiskData {
    pub fn new(
        debugger: u64,
        context_irmin: u64,
        context_merkle_rocksdb: u64,
        block_storage: u64,
        context_actions: u64,
        main_db: u64,
    ) -> Self {
        Self {
            context_irmin,
            context_merkle_rocksdb,
            block_storage,
            context_actions,
            main_db,
            debugger,
        }
    }

    pub fn to_megabytes(&self) -> Self {
        Self {
            context_irmin: self.context_irmin / 1024 / 1024,
            context_merkle_rocksdb: self.context_merkle_rocksdb / 1024 / 1024,
            block_storage: self.block_storage / 1024 / 1024,
            context_actions: self.context_actions / 1024 / 1024,
            main_db: self.main_db / 1024 / 1024,
            debugger: self.debugger / 1024 / 1024,
        }
    }
}

#[derive(Serialize)]
pub struct CpuData {
    ocaml: i32,
    tezedge: i32,
    protocol_runners: i32,
}

impl fmt::Display for CpuData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "\nOcaml: {}%\nTezedge: \n\tNode: {}\n\tProtocol: {}",
            self.ocaml, self.tezedge, self.protocol_runners
        )
    }
}
