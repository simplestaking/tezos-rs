// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use itertools::Itertools;
use slog::Logger;
use warp::Filter;

use warp::filters::BoxedFilter;

use crate::monitors::resource::ResourceUtilizationStorage;
use crate::rpc::handlers::{get_measurements, MeasurementOptions};

pub fn filters(
    log: Logger,
    resource_utilization_storage: Vec<ResourceUtilizationStorage>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    // Allow cors from any origin
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["content-type"])
        .allow_methods(vec!["GET"]);

    // TODO: TE-499 - (multiple nodes) rework this to load from a config, where all the nodes all defined
    // let tezedge_resource_utilization_storage = resource_utilization_storage.get("tezedge").unwrap();

    // if let Some(ocaml_resource_utilization_storage) = resource_utilization_storage.get("ocaml") {
    //     get_ocaml_measurements_filter(log.clone(), ocaml_resource_utilization_storage.clone())
    //         .or(get_tezedge_measurements_filter(
    //             log,
    //             tezedge_resource_utilization_storage.clone(),
    //         ))
    //         .with(cors)
    // } else {
    //     // This is just a hack to enable only tezedge node
    //     get_ocaml_measurements_filter(log.clone(), ResourceUtilizationStorage::default())
    //         .or(get_tezedge_measurements_filter(
    //             log,
    //             tezedge_resource_utilization_storage.clone(),
    //         ))
    //         .with(cors)
    // }

    // TODO: this is a workaround, but nothing better is available atm in warp
    // let mut routes = get_measurements_filter(log.clone(), resource_utilization_storage[0].clone()).boxed();

    // for storage in resource_utilization_storage {
    //     let next_route = get_measurements_filter(log.clone(), storage.clone());
    //     routes = routes.or(next_route).unify().boxed();
    // }

    let filters = resource_utilization_storage
        .into_iter()
        .map(|storage| get_measurements_filter(log.clone(), storage.clone()))
        .fold1(|combined, filter| combined.or(filter).unify().boxed())
        .expect("Failed to create combined filters");

    filters.with(cors)
    // let routes = resource_utilization_storage.into_iter()
    //     .map(|rs| {
    //         get_measurements_filter(log.clone(), rs.clone())
    //     })
    //     .fold(warp::get().map(warp::reply).boxed(), |routes, route| routes.or(route).boxed());
}

pub fn get_measurements_filter(
    log: Logger,
    resource_utilization: ResourceUtilizationStorage,
) -> BoxedFilter<(impl warp::Reply,)> {
    let tag = resource_utilization.node().tag().clone();
    warp::path("resources")
        .and(warp::path(tag))
        .and(warp::get())
        .and(warp::query::<MeasurementOptions>())
        .and(with_log(log))
        .and(with_resource_utilization_storage(resource_utilization))
        .and_then(get_measurements)
        .boxed()
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
