// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryFrom;
use std::fmt;
use std::fs::{read_dir, File};
use std::io::prelude::*;
use std::path::Path;
use std::process::Command;
use std::string::FromUtf8Error;

use getset::CopyGetters;
use merge::Merge;
use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use lazy_static::lazy_static;

pub type MemoryStatsResult<T> = std::result::Result<T, MemoryStatsError>;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, Default)]
pub struct LinuxData {
    page_size: usize, // unit of memory assignment/addressing used by the Linux kernel
    size: String,     // total program size
    resident: String, // resident set size
    shared: String,   // shared pages (i.e., backed by a file)
    text: String,     // text (code)
    lib: String,      // library (unused in Linux 2.6)
    data: String,     // data + stack
    dt: String,       // dirty pages (unused in Linux 2.6)
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct DarwinOsData {
    page_size: usize, // unit of memory assignment/addressing used by the Linux kernel
    mem: f64,         // percentage of real memory being used by the process in KB
    resident: String, // resident set size
}

#[derive(Serialize, Debug, Default, Merge, Clone, PartialEq, CopyGetters)]
pub struct ProcessMemoryStats {
    #[get_copy = "pub"]
    #[merge(strategy = merge::num::saturating_add)]
    virtual_mem: usize,

    #[get_copy = "pub"]
    #[merge(strategy = merge::num::saturating_add)]
    resident_mem: usize,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum MemoryData {
    Linux(LinuxData),
    DarwinOs(DarwinOsData),
}

impl From<LinuxData> for MemoryData {
    fn from(data: LinuxData) -> Self {
        MemoryData::Linux(data)
    }
}

impl From<DarwinOsData> for MemoryData {
    fn from(data: DarwinOsData) -> Self {
        MemoryData::DarwinOs(data)
    }
}

impl TryFrom<MemoryData> for ProcessMemoryStats {
    type Error = anyhow::Error;

    fn try_from(data: MemoryData) -> Result<Self, Self::Error> {
        match data {
            MemoryData::Linux(stats) => {
                let LinuxData {
                    size,
                    resident,
                    page_size,
                    ..
                } = stats;

                let size = size.parse::<usize>()?;
                let resident = resident.parse::<usize>()?;

                let virtual_mem = size * page_size;
                let resident_mem = resident * page_size;

                Ok(ProcessMemoryStats {
                    virtual_mem,
                    resident_mem,
                })
            }
            MemoryData::DarwinOs(stats) => {
                let DarwinOsData {
                    resident,
                    page_size,
                    ..
                } = stats;

                let resident = resident.parse::<usize>()?;

                // Note: we cannot get the virstual memory size from the data available
                let virtual_mem = 0;
                let resident_mem = resident * page_size;

                Ok(ProcessMemoryStats {
                    virtual_mem,
                    resident_mem,
                })
            }
        }
    }
}

impl fmt::Display for ProcessMemoryStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "\n\tVirtual memory: {} MB\n\tResident memory: {} MB",
            self.virtual_mem, self.resident_mem,
        )
    }
}

impl ProcessMemoryStats {
    pub fn new(virtual_mem: usize, resident_mem: usize) -> Self {
        Self {
            virtual_mem,
            resident_mem,
        }
    }

    pub fn to_megabytes(&self) -> Self {
        Self {
            resident_mem: self.resident_mem / 1024 / 1024,
            virtual_mem: self.virtual_mem / 1024 / 1024,
        }
    }
}

#[derive(PartialEq, Debug, Error)]
pub enum MemoryStatsError {
    #[error("IOError: {0}")]
    IOError(String),
    #[error("fail to parse data")]
    ParsingData,
    #[error("not supported OS")]
    NotSupportedOs,
    #[error("Utf8 error: {0}")]
    Uft8Error(FromUtf8Error),
}

impl From<FromUtf8Error> for MemoryStatsError {
    fn from(source: FromUtf8Error) -> Self {
        Self::Uft8Error(source)
    }
}

lazy_static! {
    static ref LINUX_DATA: Regex = Regex::new(r"^(?P<size>\d+)\s+(?P<resident>\d+)\s+(?P<shared>\d+)\s+(?P<text>\d+)\s+(?P<lib>\d+)\s+(?P<data>\d+)\s+(?P<dt>\d+).*").expect("Invalid regex");
    static ref MAC_DATA: Regex = Regex::new(r"(?P<mem>[0-9.]+)\s+(?P<resident>\d+)").expect("Invalid regex");
}

#[derive(Default)]
pub struct Memory {
    pub pid: i32,
    pub page_size: usize,
}

impl Memory {
    pub fn new() -> Self {
        Memory {
            pid: nix::unistd::getpid().as_raw(),
            page_size: page_size::get(),
        }
    }

    pub fn get_memory_stats(&self) -> MemoryStatsResult<MemoryData> {
        if cfg!(target_os = "linux") {
            //self.get_linux_data(format!("/proc/{}/statm", self.pid))
            self.parse_linux_statm(self.read_linux_file(format!("/proc/{}/statm", self.pid))?)
        } else if cfg!(target_os = "macos") || cfg!(target_os = "ios") {
            self.get_mac_data()
        } else {
            Err(MemoryStatsError::NotSupportedOs)
        }
    }

    pub fn get_memory_stats_protocol_runners(&self) -> MemoryStatsResult<Vec<MemoryData>> {
        if cfg!(target_os = "linux") {
            self.get_linux_protocol_runner_stats()
        } else {
            // TODO: TE-394 implement for macOS
            Err(MemoryStatsError::NotSupportedOs)
        }
    }

    /// private helpers

    fn get_mac_data(&self) -> MemoryStatsResult<MemoryData> {
        let output = Command::new("ps")
            .args(&["-o", "%mem,rss", "-p", &self.pid.to_string()])
            .output()
            .map_err(|e| {
                MemoryStatsError::IOError(format!("failed to execute ps command: {}", e))
            })?;
        let out_str = String::from_utf8(output.stdout)?;
        self.parse_mac_data(out_str)
    }

    fn parse_mac_data(&self, out: String) -> MemoryStatsResult<MemoryData> {
        if let Some(captures) = MAC_DATA.captures(&out) {
            let data = DarwinOsData {
                page_size: self.page_size,
                mem: captures["mem"]
                    .parse()
                    .map_err(|_| MemoryStatsError::ParsingData)?,
                resident: captures["resident"].to_string(),
            };
            Ok(MemoryData::from(data))
        } else {
            Err(MemoryStatsError::ParsingData)
        }
    }

    fn get_protocol_runner_pids(&self) -> MemoryStatsResult<Vec<i32>> {
        let paths = match read_dir(format!("/proc/{}/task/", self.pid)) {
            Ok(paths) => paths,
            Err(e) => return Err(MemoryStatsError::IOError(format!("{}", e))),
        };

        let mut ret = Vec::new();

        for path in paths {
            match path {
                Ok(path) => {
                    if let Some(path) = path.path().to_str() {
                        let file_str = self.read_linux_file(format!("{}/children", path))?;
                        if !file_str.is_empty() {
                            file_str
                                .split(' ')
                                .filter(|s| !s.is_empty())
                                .for_each(|s| ret.push(s.parse::<i32>().unwrap()))
                        }
                    }
                }
                Err(e) => return Err(MemoryStatsError::IOError(format!("{}", e))),
            }
        }
        Ok(ret)
    }

    fn get_linux_protocol_runner_stats(&self) -> MemoryStatsResult<Vec<MemoryData>> {
        if cfg!(target_os = "linux") {
            self.get_protocol_runner_pids()?
                .iter()
                .map(|pid| {
                    self.parse_linux_statm(
                        self.read_linux_file(format!("/proc/{}/statm", pid))
                            .unwrap(),
                    )
                })
                .collect()
        } else {
            // TODO: TE-394 implement for macOS
            Err(MemoryStatsError::NotSupportedOs)
        }
    }

    /// get LinuxData from statm String
    /// all exept page_size must be strings convertable to i64
    /// no need to check them because regular will match only numbers /(\d+)/
    fn parse_linux_statm(&self, statm: String) -> MemoryStatsResult<MemoryData> {
        if let Some(captures) = LINUX_DATA.captures(&statm) {
            let data = LinuxData {
                page_size: self.page_size,
                size: captures["size"].to_string(),
                resident: captures["resident"].to_string(),
                shared: captures["shared"].to_string(),
                text: captures["text"].to_string(),
                lib: captures["lib"].to_string(),
                data: captures["data"].to_string(),
                dt: captures["dt"].to_string(),
            };
            Ok(MemoryData::from(data))
        } else {
            Err(MemoryStatsError::ParsingData)
        }
    }

    /// read lines of given file path and parse linux memory data
    fn read_linux_file(&self, filepath: String) -> Result<String, MemoryStatsError> {
        let path = Path::new(&filepath);
        let display = path.display();

        let mut file = match File::open(&path) {
            // `io::Error`
            Err(e) => {
                return Err(MemoryStatsError::IOError(format!(
                    "couldn't open {}: {}",
                    display, e
                )))
            }
            Ok(file) => file,
        };

        let mut s = String::new();
        match file.read_to_string(&mut s) {
            // `io::Error`
            Err(e) => Err(MemoryStatsError::IOError(format!(
                "couldn't read {}: {}",
                display, e
            ))),
            Ok(_) => Ok(s),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn correct_parsing_linux() {
        let statm_to_parse = "218428 10272 6459 7780 0 22424 0".to_string();
        let memory = Memory::new();
        let parse_result = MemoryData::Linux(LinuxData {
            page_size: memory.page_size,
            size: "218428".to_string(),
            resident: "10272".to_string(),
            shared: "6459".to_string(),
            text: "7780".to_string(),
            lib: "0".to_string(),
            data: "22424".to_string(),
            dt: "0".to_string(),
        });
        assert_eq!(Ok(parse_result), memory.parse_linux_statm(statm_to_parse))
    }

    #[test]
    fn correct_parsing_mac() {
        let to_parse = "PID %MEM   RSS\n0.3  5336\n".to_string();
        let memory = Memory::new();
        let parse_result = MemoryData::DarwinOs(DarwinOsData {
            page_size: memory.page_size,
            mem: 0.3,
            resident: "5336".to_string(),
        });
        assert_eq!(Ok(parse_result), memory.parse_mac_data(to_parse))
    }
}
