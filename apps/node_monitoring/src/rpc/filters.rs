// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use slog::Logger;
use warp::Filter;

use crate::monitors::resource::{ResourceUtilizationStorage, ResourceUtilizationStorageMap};
use crate::rpc::handlers::{get_measurements, MeasurementOptions};

pub fn filters(
    log: Logger,
    resource_utilization_storage: ResourceUtilizationStorageMap,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    // Allow cors from any origin
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["content-type"])
        .allow_methods(vec!["GET"]);

    // TODO: TE-499 - (multiple nodes) rework this to load from a config, where all the nodes all defined
    let tezedge_resource_utilization_storage = resource_utilization_storage.get("tezedge").unwrap();
    if let Some(ocaml_resource_utilization_storage) = resource_utilization_storage.get("ocaml") {
        get_ocaml_measurements_filter(log.clone(), ocaml_resource_utilization_storage.clone())
            .or(get_tezedge_measurements_filter(
                log,
                tezedge_resource_utilization_storage.clone(),
            ))
            .with(cors)
    } else {
        // This is just a hack to enable only tezedge node
        get_ocaml_measurements_filter(log.clone(), ResourceUtilizationStorage::default())
            .or(get_tezedge_measurements_filter(
                log,
                tezedge_resource_utilization_storage.clone(),
            ))
            .with(cors)
    }
}

pub fn get_tezedge_measurements_filter(
    log: Logger,
    tezedge_resource_utilization: ResourceUtilizationStorage,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("resources" / "tezedge")
        .and(warp::get())
        .and(warp::query::<MeasurementOptions>())
        .and(with_log(log))
        .and(with_resource_utilization_storage(
            tezedge_resource_utilization,
        ))
        .and_then(get_measurements)
}

pub fn get_ocaml_measurements_filter(
    log: Logger,
    ocaml_resource_utilization: ResourceUtilizationStorage,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("resources" / "ocaml")
        .and(warp::get())
        .and(warp::query::<MeasurementOptions>())
        .and(with_log(log))
        .and(with_resource_utilization_storage(
            ocaml_resource_utilization,
        ))
        .and_then(get_measurements)
}

fn with_log(
    log: Logger,
) -> impl Filter<Extract = (Logger,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || log.clone())
}

fn with_resource_utilization_storage(
    storage: ResourceUtilizationStorage,
) -> impl Filter<Extract = (ResourceUtilizationStorage,), Error = std::convert::Infallible> + Clone
{
    warp::any().map(move || storage.clone())
}
