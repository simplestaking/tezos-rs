// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

// TODO: TE-499 get this info from docker (shiplift needs to implement docker volume inspect)
// Path constants to the volumes
pub const OCAML_VOLUME_PATH: &str =
    "/var/lib/docker/volumes/deploy_monitoring_ocaml-shared-data/_data";
pub const DEBUGGER_VOLUME_PATH: &str =
    "/var/lib/docker/volumes/deploy_monitoring_debugger-data/_data";

/// The max capacity of the VecDeque holding the measurements
pub const MEASUREMENTS_MAX_CAPACITY: usize = 40320;
