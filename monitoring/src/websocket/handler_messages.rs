// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::iter::FromIterator;

use serde::Serialize;
use slog_derive::SerdeValue;

use crate::monitors::ChainMonitor;
use crate::monitors::PeerMonitor;

// -------------------------- GENERAL METRICS -------------------------- //
#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BlockMetrics {
    group: i32,
    numbers_of_blocks: i32,
    finished_blocks: i32,
    applied_blocks: i32,
    download_duration: Option<f32>,
}

impl BlockMetrics {
    pub fn new(
        group: i32,
        numbers_of_blocks: i32,
        finished_blocks: i32,
        applied_blocks: i32,
        download_duration: Option<f32>,
    ) -> Self {
        Self {
            group,
            numbers_of_blocks,
            finished_blocks,
            applied_blocks,
            download_duration,
        }
    }
}

// -------------------------- GENERAL METRICS -------------------------- //
#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IncomingTransferMetrics {
    eta: f32,
    current_block_count: usize,
    downloaded_blocks: usize,
    download_rate: f32,
    average_download_rate: f32,
    downloaded_headers: usize,
    header_download_rate: f32,
    header_average_download_rate: f32,
}

impl IncomingTransferMetrics {
    pub fn new(
        eta: f32,
        current_block_count: usize,
        downloaded_blocks: usize,
        download_rate: f32,
        average_download_rate: f32,
        downloaded_headers: usize,
        header_download_rate: f32,
        header_average_download_rate: f32,
    ) -> Self {
        Self {
            eta,
            current_block_count,
            downloaded_blocks,
            download_rate,
            average_download_rate,
            downloaded_headers,
            header_download_rate,
            header_average_download_rate,
        }
    }
}

// -------------------------- PEER TRANSFER STATS MESSAGE -------------------------- //
#[derive(Clone, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PeerMetrics {
    #[serde(rename = "id")]
    public_key_hash: String,
    ip_address: String,
    transferred_bytes: usize,
    average_transfer_speed: f32,
    current_transfer_speed: f32,
}

impl PeerMetrics {
    pub fn new(
        public_key_hash: String,
        ip_address: String,
        transferred_bytes: usize,
        average_transfer_speed: f32,
        current_transfer_speed: f32,
    ) -> Self {
        Self {
            public_key_hash,
            ip_address,
            transferred_bytes,
            average_transfer_speed,
            current_transfer_speed,
        }
    }
}

// -------------------------- PEER CONNECTING/DISCONNECTING MESSAGE -------------------------- //
#[derive(Clone, Serialize, Debug)]
#[serde(rename_all = "camelCase", tag = "status", content = "id")]
pub enum PeerConnectionStatus {
    Connected(String),
    Disconnected(String),
}

impl PeerConnectionStatus {
    pub fn connected(peer_address: String) -> Self {
        Self::Connected(peer_address)
    }

    pub fn disconnected(peer_address: String) -> Self {
        Self::Disconnected(peer_address)
    }
}

// -------------------------- MONITOR MESSAGE -------------------------- //
#[derive(SerdeValue, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum HandlerMessage {
    PeersMetrics { payload: Vec<PeerMetrics> },
    PeerStatus { payload: PeerConnectionStatus },
    IncomingTransfer { payload: IncomingTransferMetrics },
    BlockStatus { payload: Vec<BlockMetrics> },
    BlockApplicationStatus { payload: BlockApplicationMessage },
    ChainStatus { payload: ChainMonitor },
    NotImplemented(String),
}

impl<'a> FromIterator<&'a mut PeerMonitor> for HandlerMessage {
    fn from_iter<I: IntoIterator<Item = &'a mut PeerMonitor>>(monitors: I) -> Self {
        let mut payload = Vec::new();
        for monitor in monitors {
            payload.push(monitor.snapshot())
        }

        Self::PeersMetrics { payload }
    }
}

impl From<PeerConnectionStatus> for HandlerMessage {
    fn from(payload: PeerConnectionStatus) -> Self {
        Self::PeerStatus { payload }
    }
}

impl From<IncomingTransferMetrics> for HandlerMessage {
    fn from(payload: IncomingTransferMetrics) -> Self {
        Self::IncomingTransfer { payload }
    }
}

// -------------------------- BLOCK APPLICATION MESSAGE -------------------------- //
#[derive(Clone, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlockApplicationMessage {
    pub(crate) current_application_speed: f32,
    pub(crate) average_application_speed: f32,
    pub(crate) last_applied_block: Option<BlockInfo>,
}

#[derive(Clone, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlockInfo {
    pub(crate) hash: String,
    pub(crate) level: i32,
}
