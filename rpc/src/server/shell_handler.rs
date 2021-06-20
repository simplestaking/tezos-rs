// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::format_err;
use hyper::body::Buf;
use hyper::{Body, Method, Request};
use serde::Serialize;

use crypto::hash::ProtocolHash;
use tezos_api::ffi::ProtocolRpcError;
use tezos_messages::ts_to_rfc3339;
use tezos_wrapper::service::{ProtocolError, ProtocolServiceError};

use crate::helpers::{
    create_rpc_request, parse_async, parse_block_hash, parse_chain_id, MAIN_CHAIN_ID,
};
use crate::server::{HResult, HasSingleValue, Params, Query, RpcServiceEnvironment};
use crate::services::{base_services, stream_services};
use crate::{
    empty,
    encoding::{base_types::*, monitor::BootstrapInfo},
    helpers, make_json_response, make_json_stream_response, not_found, required_param,
    result_option_to_json_response, result_to_empty_json_response, result_to_json_response,
    services, ServiceResult,
};
use storage::BlockHeaderWithHash;

#[derive(Serialize)]
pub struct ErrorMessage {
    error_type: String,
    message: String,
}

pub async fn bootstrapped(
    _: Request<Body>,
    _: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> HResult {
    let state_read = env.state().read().unwrap();

    let bootstrap_info = match state_read.current_head().as_ref() {
        Some(current_head) => {
            let current_head: &BlockHeaderWithHash = &current_head;
            let timestamp = ts_to_rfc3339(current_head.header.timestamp());
            Ok(BootstrapInfo::new(
                &current_head.hash,
                TimeStamp::Rfc(timestamp),
            ))
        }
        None => Err(format_err!("No current head")),
    };

    result_to_json_response(bootstrap_info, env.log())
}

pub async fn commit_hash(
    _: Request<Body>,
    _: Params,
    _: Query,
    _: RpcServiceEnvironment,
) -> HResult {
    let resp = &UniString::from(env!("GIT_HASH"));
    make_json_response(&resp)
}

pub async fn active_chains(
    _: Request<Body>,
    _: Params,
    _: Query,
    _: RpcServiceEnvironment,
) -> HResult {
    empty()
}

pub async fn protocols(_: Request<Body>, _: Params, _: Query, _: RpcServiceEnvironment) -> HResult {
    empty()
}

pub async fn valid_blocks(
    _: Request<Body>,
    _: Params,
    _: Query,
    _: RpcServiceEnvironment,
) -> HResult {
    empty()
}

pub async fn head_chain(
    _: Request<Body>,
    params: Params,
    query: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let _chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let protocol = if let Some(protocol) = query.get_str("next_protocol") {
        ProtocolHash::from_base58_check(protocol).ok()
    } else {
        None
    };

    let RpcServiceEnvironment {
        state,
        persistent_storage,
        ..
    } = env;

    make_json_stream_response(stream_services::HeadMonitorStream::new(
        state,
        protocol,
        &persistent_storage,
    ))
}

pub async fn mempool_monitor_operations(
    _: Request<Body>,
    params: Params,
    query: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;

    let applied = query.get_str("applied");
    let branch_refused = query.get_str("branch_refused");
    let branch_delayed = query.get_str("branch_delayed");
    let refused = query.get_str("refused");

    let mempool_query = stream_services::MempoolOperationsQuery {
        applied: applied == Some("yes"),
        branch_refused: branch_refused == Some("yes"),
        branch_delayed: branch_delayed == Some("yes"),
        refused: refused == Some("yes"),
    };

    let RpcServiceEnvironment {
        state,
        log,
        current_mempool_state_storage,
        ..
    } = env;

    let last_checked_head = state
        .read()
        .unwrap()
        .current_head()
        .as_ref()
        .unwrap()
        .hash
        .clone();
    make_json_stream_response(stream_services::OperationMonitorStream::new(
        chain_id,
        current_mempool_state_storage,
        state,
        log,
        last_checked_head,
        mempool_query,
    ))
}

pub async fn blocks(
    _: Request<Body>,
    params: Params,
    query: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let length = query.get_str("length").unwrap_or("0");
    // TODO: mutliparameter
    let head = parse_block_hash(&chain_id, query.get_str("head").unwrap(), &env)?;
    // TODO: implement min_date query arg

    // TODO: This can be implemented in a more optimised and cleaner way
    // Note: Need to investigate the "more heads per level" variant

    make_json_response(&vec![base_services::get_block_hashes(
        chain_id,
        head,
        None,
        length.parse::<usize>()?,
        env.persistent_storage(),
    )?
    .iter()
    .map(|block| block.to_base58_check())
    .collect::<Vec<String>>()])
}

pub async fn chains_block_id(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_to_json_response(
        base_services::get_block(&chain_id, &block_hash, &env).await,
        env.log(),
    )
}

pub async fn chains_block_id_header(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_to_json_response(
        base_services::get_block_header(chain_id, block_hash, env.persistent_storage()).await,
        env.log(),
    )
}

pub async fn chains_block_id_header_shell(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_option_to_json_response(
        base_services::get_block_shell_header(chain_id, block_hash, env.persistent_storage()),
        env.log(),
    )
}

pub async fn chains_block_id_metadata(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_to_json_response(
        base_services::get_block_metadata(&chain_id, &block_hash, &env).await,
        env.log(),
    )
}

pub async fn context_raw_bytes(
    _: Request<Body>,
    params: Params,
    query: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;
    let prefix = params.get_str("any");
    let depth = query.get_usize("depth");

    result_to_json_response(
        base_services::get_context_raw_bytes(&block_hash, prefix, depth, &env),
        env.log(),
    )
}

pub async fn mempool_pending_operations(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let RpcServiceEnvironment {
        current_mempool_state_storage,
        log,
        ..
    } = env;
    let (pending_operations, _) = services::mempool_services::get_pending_operations(
        &chain_id,
        current_mempool_state_storage,
    )?;
    result_to_json_response(Ok(pending_operations), &log)
}

pub async fn inject_operation(
    req: Request<Body>,
    _: Params,
    query: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let operation_data_raw = hyper::body::aggregate(req).await?;
    let operation_data: String = serde_json::from_reader(&mut operation_data_raw.reader())?;

    let chain_id_query = query.get_str("chain_id").unwrap_or(MAIN_CHAIN_ID);
    let chain_id = parse_chain_id(chain_id_query, &env)?;
    let is_async = parse_async(&query, false);

    result_to_json_response(
        services::mempool_services::inject_operation(
            is_async,
            chain_id,
            &operation_data,
            &env,
            env.mempool_channel(),
        ),
        env.log(),
    )
}

pub async fn inject_block(
    req: Request<Body>,
    _: Params,
    query: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body = String::from_utf8(body.to_vec())?;

    let shell_channel = env.shell_channel();

    let chain_id_query = query.get_str("chain_id").unwrap_or(MAIN_CHAIN_ID);
    let chain_id = parse_chain_id(chain_id_query, &env)?;
    let is_async = parse_async(&query, false);

    result_to_json_response(
        services::mempool_services::inject_block(is_async, chain_id, &body, &env, shell_channel),
        env.log(),
    )
}

pub async fn mempool_request_operations(
    _: Request<Body>,
    _: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    result_to_empty_json_response(
        services::mempool_services::request_operations(env.shell_channel.clone()),
        env.log(),
    )
}

pub async fn get_block_protocols(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_to_json_response(
        base_services::get_block_protocols(&chain_id, &block_hash, env.persistent_storage()),
        env.log(),
    )
}

pub async fn get_block_hash(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_to_json_response(Ok(block_hash.to_base58_check()), env.log())
}

pub async fn get_chain_id(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;

    result_to_json_response(Ok(chain_id.to_base58_check()), env.log())
}

pub async fn get_metadata_hash(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    match base_services::get_additional_data(&chain_id, &block_hash, env.persistent_storage())? {
        Some(data) => match data.block_metadata_hash() {
            Some(hash) => result_to_json_response(Ok(hash.to_base58_check()), env.log()),
            None => not_found(),
        },
        None => not_found(),
    }
}

pub async fn get_operations_metadata_hash(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    match base_services::get_additional_data(&chain_id, &block_hash, env.persistent_storage())? {
        Some(data) => match data.ops_metadata_hash() {
            Some(hash) => result_to_json_response(Ok(hash.to_base58_check()), env.log()),
            None => not_found(),
        },
        None => not_found(),
    }
}

pub async fn get_operations_metadata_hash_operation_metadata_hashes(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    match base_services::get_additional_data(&chain_id, &block_hash, env.persistent_storage())? {
        Some(data) => match data.ops_metadata_hashes() {
            Some(hashes) => {
                let hashes = hashes
                    .iter()
                    .map(|ops| {
                        ops.iter()
                            .map(|hash| hash.to_base58_check())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                result_to_json_response(Ok(hashes), env.log())
            }
            None => not_found(),
        },
        None => not_found(),
    }
}

pub async fn get_operations_metadata_hash_operation_metadata_hashes_by_validation_pass(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;
    let validation_pass: usize = required_param!(params, "validation_pass_index")?.parse()?;

    match base_services::get_additional_data(&chain_id, &block_hash, env.persistent_storage())? {
        Some(data) => match data.ops_metadata_hashes() {
            Some(hashes) => {
                if let Some(validation_passes) = hashes.get(validation_pass) {
                    let validation_passes = validation_passes
                        .iter()
                        .map(|hash| hash.to_base58_check())
                        .collect::<Vec<_>>();
                    result_to_json_response(Ok(validation_passes), env.log())
                } else {
                    not_found()
                }
            }
            None => not_found(),
        },
        None => not_found(),
    }
}

pub async fn get_operations_metadata_hash_operation_metadata_hashes_by_validation_pass_by_operation_index(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;
    let validation_pass: usize = required_param!(params, "validation_pass_index")?.parse()?;
    let operation_index: usize = required_param!(params, "operation_index")?.parse()?;

    match base_services::get_additional_data(&chain_id, &block_hash, env.persistent_storage())? {
        Some(data) => match data.ops_metadata_hashes() {
            Some(hashes) => {
                if let Some(validation_passes) = hashes.get(validation_pass) {
                    if let Some(validation_pass) = validation_passes.get(operation_index) {
                        result_to_json_response(Ok(validation_pass.to_base58_check()), env.log())
                    } else {
                        not_found()
                    }
                } else {
                    not_found()
                }
            }
            None => not_found(),
        },
        None => not_found(),
    }
}

pub async fn get_block_operation_hashes(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_to_json_response(
        base_services::get_block_operation_hashes(chain_id, &block_hash, &env).await,
        env.log(),
    )
}

pub async fn get_block_operations(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_to_json_response(
        base_services::get_block_operations_metadata(chain_id, &block_hash, &env).await,
        env.log(),
    )
}

pub async fn get_block_operations_validation_pass(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    let validation_pass: usize = required_param!(params, "validation_pass_index")?.parse()?;

    result_to_json_response(
        base_services::get_block_operations_validation_pass(
            chain_id,
            &block_hash,
            &env,
            validation_pass,
        )
        .await,
        env.log(),
    )
}

pub async fn get_block_operation(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    let validation_pass: usize = required_param!(params, "validation_pass_index")?.parse()?;
    let operation_order: usize = required_param!(params, "operation_index")?.parse()?;

    result_to_json_response(
        base_services::get_block_operation(
            chain_id,
            &block_hash,
            &env,
            validation_pass,
            operation_order,
        )
        .await,
        env.log(),
    )
}

pub async fn live_blocks(
    _: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id = parse_chain_id(required_param!(params, "chain_id")?, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    result_to_json_response(
        services::base_services::live_blocks(chain_id, block_hash, &env),
        env.log(),
    )
}

pub async fn preapply_operations(
    req: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id_param = required_param!(params, "chain_id")?;
    let chain_id = parse_chain_id(chain_id_param, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    let rpc_request = create_rpc_request(req).await?;

    result_to_json_response(
        services::protocol::preapply_operations(
            chain_id_param,
            chain_id,
            block_hash,
            rpc_request,
            &env,
        ),
        env.log(),
    )
}

pub async fn preapply_block(
    req: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id_param = required_param!(params, "chain_id")?;
    let chain_id = parse_chain_id(chain_id_param, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    let rpc_request = create_rpc_request(req).await?;

    // launcher - we need the error from preapply
    match services::protocol::preapply_block(
        chain_id_param,
        chain_id,
        block_hash,
        rpc_request,
        &env,
    ) {
        Ok(resp) => result_to_json_response(Ok(resp), env.log()),
        Err(e) => {
            if let Some(ProtocolServiceError::ProtocolError {
                reason:
                    ProtocolError::ProtocolRpcError {
                        reason: ProtocolRpcError::FailedToCallProtocolRpc(message),
                    },
                // TODO - NEWERRORS: revise
                // Was: e.as_fail().downcast_ref::<ProtocolServiceError>()
            }) = e.downcast_ref::<ProtocolServiceError>()
            {
                return make_json_response(&ErrorMessage {
                    error_type: "ocaml".to_string(),
                    message: message.to_string(),
                });
            }
            empty()
        }
    }
}

pub async fn node_version(
    _: Request<Body>,
    _: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    result_to_json_response(
        Ok(base_services::get_node_version(env.network_version())),
        env.log(),
    )
}

pub async fn config_user_activated_upgrades(
    _: Request<Body>,
    _: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    result_to_json_response(
        Ok(env
            .tezos_environment()
            .protocol_overrides
            .user_activated_upgrades_to_rpc_json()),
        env.log(),
    )
}

pub async fn config_user_activated_protocol_overrides(
    _: Request<Body>,
    _: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    result_to_json_response(
        Ok(env
            .tezos_environment()
            .protocol_overrides
            .user_activated_protocol_overrides_to_rpc_json()),
        env.log(),
    )
}

// TODO: TE-275 - implement correctly - at least for protocol rpcs. This is a 'fake it till you make it' handler
/// Handler mockin the describe routes in ocaml to be compatible with tezoses python test framework
pub async fn describe(
    allowed_methods: Arc<HashSet<Method>>,
    req: Request<Body>,
    _: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let path: Vec<String> = req
        .uri()
        .path()
        .split('/')
        .skip(2)
        .map(|v| v.to_string())
        .collect();

    // TODO: dyanmically get the method from a protocol call, for now, get the first element...
    // NOTE: protocol rpcs are dynamically created and called trough the protocol,
    // as a next step, we should somehow get the registreds paths method from the protocol
    let method = if !allowed_methods.is_empty() {
        // TODO: same reasoning as above
        if path.contains(&"injection".to_string()) || path.contains(&"forge".to_string()) {
            &Method::POST
        } else if path.contains(&"storage".to_string()) {
            &Method::GET
        } else {
            allowed_methods.iter().next().unwrap()
        }
    } else {
        return empty();
    };

    let service_fields = serde_json::json!({
        "meth": method.as_str(),
        "path": path,
        "description": "Handler mockin the describe routes in ocaml to be compatible with tezoses python test framework.",
        "query": [],
        "output": {
            "json_schema": {},
            "binary_schema": {
                "toplevel": {
                    "fields": [],
                },
                "fields": [],
            },
        },
        "error": {
            "json_schema": {},
            "binary_schema": {
                "toplevel": {
                    "fields": [],
                },
                "fields": [],
            },
        },
    });

    let describe_json = match *method {
        Method::GET => serde_json::json!({
            "static": {
                "get_service": service_fields
            },
        }),
        Method::POST => serde_json::json!({
            "static": {
                "post_service": service_fields
            },
        }),
        Method::PUT => serde_json::json!({
            "static": {
                "put_service": service_fields
            },
        }),
        Method::DELETE => serde_json::json!({
            "static": {
                "delete_service": service_fields
            },
        }),
        _ => unimplemented!(),
    };

    result_to_json_response(Ok(describe_json), env.log())
}

pub async fn worker_prevalidators(
    _: Request<Body>,
    _: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    result_to_json_response(helpers::get_prevalidators(&env, None), env.log())
}
