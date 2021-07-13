// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use slog::Logger;
use tokio::task::JoinHandle;

use crate::monitors::resource::ResourceUtilizationStorage;

pub mod filters;
pub mod handlers;

pub fn spawn_rpc_server(
    rpc_port: u16,
    log: Logger,
    resource_utilization: Vec<ResourceUtilizationStorage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let api = filters::filters(log.clone(), resource_utilization.clone());

        warp::serve(api).run(([0, 0, 0, 0], rpc_port)).await;
    })
}
