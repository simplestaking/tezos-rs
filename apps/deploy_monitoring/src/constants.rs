// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

// Port constants
pub const DEBUGGER_PORT: u16 = 17732;
pub const EXPLORER_PORT: u16 = 80;
pub const TEZEDGE_PORT: u16 = 18732;
pub const TEZEDGE_NODE_P2P_PORT: u16 = 19732;
pub const OCAML_PORT: u16 = 18733;

// TODO: TE-499 get this info from docker (shiplift needs to implement docker volume inspect)
// Path constants to the volumes
// pub const TEZEDGE_VOLUME_PATH: &str =
//     "/var/lib/docker/volumes/deploy_monitoring_tezedge-shared-data/_data";
pub const OCAML_VOLUME_PATH: &str =
    "/var/lib/docker/volumes/deploy_monitoring_ocaml-shared-data/_data";
pub const DEBUGGER_VOLUME_PATH: &str =
    "/var/lib/docker/volumes/deploy_monitoring_debugger-data/_data";

/// The max capacity of the VecDeque holding the measurements
pub const MEASUREMENTS_MAX_CAPACITY: usize = 40320;
