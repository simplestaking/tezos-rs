// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{net::SocketAddr, time::Instant};

use crypto::hash::CryptoboxPublicKeyHash;

use crate::websocket::handler_messages::PeerMetrics;

/// Peer specific details about transfer *FROM* peer.
pub(crate) struct PeerMonitor {
    peer_address: SocketAddr,
    peer_public_key_hash: CryptoboxPublicKeyHash,

    total_transferred: usize,
    current_transferred: usize,
    last_update: Instant,
    first_update: Instant,
}

impl PeerMonitor {
    pub fn new(
        peer_address: SocketAddr,
        peer_public_key_hash: CryptoboxPublicKeyHash,
    ) -> Self {
        let now = Instant::now();
        Self {
            peer_address,
            peer_public_key_hash,
            total_transferred: 0,
            current_transferred: 0,
            last_update: now,
            first_update: now,
        }
    }

    pub fn avg_speed(&self) -> f32 {
        self.total_transferred as f32 / self.first_update.elapsed().as_secs_f32()
    }

    pub fn current_speed(&self) -> f32 {
        self.current_transferred as f32 / self.last_update.elapsed().as_secs_f32()
    }

    pub fn incoming_bytes(&mut self, incoming: usize) {
        self.total_transferred += incoming;
        self.current_transferred += incoming
    }

    pub fn snapshot(&mut self) -> PeerMetrics {
        let ret = PeerMetrics::new(
            self.peer_public_key_hash(),
            self.peer_address(),
            self.total_transferred,
            self.avg_speed(),
            self.current_speed(),
        );

        self.current_transferred = 0;
        self.last_update = Instant::now();
        ret
    }

    pub fn peer_address(&self) -> String {
        self.peer_address.to_string()
    }

    pub fn peer_public_key_hash(&self) -> String {
        self.peer_public_key_hash.to_base58_check()
    }
}
