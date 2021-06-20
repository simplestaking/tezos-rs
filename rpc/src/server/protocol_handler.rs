// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crate::not_found;
use hyper::{Body, Request};
use slog::warn;

use crate::helpers::{create_rpc_request, parse_block_hash, parse_chain_id};
use crate::server::{HasSingleValue, Params, Query, RpcServiceEnvironment};
use crate::services::protocol::{ContextParamsError, RightsError, VotesError};
use crate::{required_param, result_to_json_response, services, ServiceResult};

pub async fn context_constants(
    req: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id_param = required_param!(params, "chain_id")?;
    let chain_id = parse_chain_id(chain_id_param, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    // try to call our implementation
    let result = services::protocol::get_context_constants_just_for_rpc(&block_hash, &env);

    // fallback, if protocol is not supported, we trigger rpc protocol router
    if let Err(ContextParamsError::UnsupportedProtocolError { .. }) = result {
        result_to_json_response(
            services::protocol::call_protocol_rpc(
                chain_id_param,
                chain_id,
                block_hash,
                create_rpc_request(req).await?,
                &env,
            ),
            env.log(),
        )
    } else {
        result_to_json_response(result.map_err(|e| e.into()), env.log())
    }
}

pub async fn baking_rights(
    req: Request<Body>,
    params: Params,
    query: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id_param = required_param!(params, "chain_id")?;
    let chain_id = parse_chain_id(chain_id_param, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    let max_priority = query.get_str("max_priority");
    let level = query.get_str("level");
    let delegate = query.get_str("delegate");
    let cycle = query.get_str("cycle");
    let has_all = query.contains_key("all");

    match services::protocol::check_and_get_baking_rights(
        &block_hash,
        level,
        delegate,
        cycle,
        max_priority,
        has_all,
        &env,
    ) {
        Ok(Some(rights)) => result_to_json_response(Ok(Some(rights)), env.log()),
        Err(e) => {
            // fallback, if protocol is not supported, we trigger rpc protocol router
            if let RightsError::UnsupportedProtocolError { .. } = e {
                result_to_json_response(
                    services::protocol::call_protocol_rpc(
                        chain_id_param,
                        chain_id,
                        block_hash,
                        create_rpc_request(req).await?,
                        &env,
                    ),
                    env.log(),
                )
            } else {
                //pass error to response parser
                let res: Result<Option<String>, anyhow::Error> = Err(e.into());
                result_to_json_response(res, env.log())
            }
        }
        _ => {
            //ignore other options from enum
            warn!(env.log(), "Wrong RpcResponseData format");
            let res: Result<Option<String>, anyhow::Error> = Ok(None);
            result_to_json_response(res, env.log())
        }
    }
}

pub async fn endorsing_rights(
    req: Request<Body>,
    params: Params,
    query: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id_param = required_param!(params, "chain_id")?;
    let chain_id = parse_chain_id(chain_id_param, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    let level = query.get_str("level");
    let cycle = query.get_str("cycle");
    let delegate = query.get_str("delegate");
    let has_all = query.contains_key("all");

    // get RPC response and unpack it from RpcResponseData enum
    match services::protocol::check_and_get_endorsing_rights(
        &block_hash,
        level,
        delegate,
        cycle,
        has_all,
        &env,
    ) {
        Ok(Some(rights)) => result_to_json_response(Ok(Some(rights)), env.log()),
        Err(e) => {
            // fallback, if protocol is not supported, we trigger rpc protocol router
            if let RightsError::UnsupportedProtocolError { .. } = e {
                result_to_json_response(
                    services::protocol::call_protocol_rpc(
                        chain_id_param,
                        chain_id,
                        block_hash,
                        create_rpc_request(req).await?,
                        &env,
                    ),
                    env.log(),
                )
            } else {
                //pass error to response parser
                let res: Result<Option<String>, anyhow::Error> = Err(e.into());
                result_to_json_response(res, env.log())
            }
        }
        _ => {
            //ignore other options from enum
            warn!(env.log(), "Wrong RpcResponseData format");
            let res: Result<Option<String>, anyhow::Error> = Ok(None);
            result_to_json_response(res, env.log())
        }
    }
}

pub async fn votes_listings(
    req: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id_param = required_param!(params, "chain_id")?;
    let chain_id = parse_chain_id(chain_id_param, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    // try to call our implementation
    let result = services::protocol::get_votes_listings(&block_hash, &env);

    // if our implementation returns None, it means that the protocol does not support
    if let Err(VotesError::UnsupportedProtocolRpc { protocol }) = result {
        warn!(
            env.log(),
            "This rpc is not supported in protocol {}", protocol
        );
        return not_found();
    }

    // fallback, if protocol is not supported, we trigger rpc protocol router
    if let Err(VotesError::UnsupportedProtocolError { .. }) = result {
        result_to_json_response(
            services::protocol::call_protocol_rpc(
                chain_id_param,
                chain_id,
                block_hash,
                create_rpc_request(req).await?,
                &env,
            ),
            env.log(),
        )
    } else {
        result_to_json_response(result.map_err(|e| e.into()), env.log())
    }
}

pub async fn call_protocol_rpc(
    req: Request<Body>,
    params: Params,
    _: Query,
    env: RpcServiceEnvironment,
) -> ServiceResult {
    let chain_id_param = required_param!(params, "chain_id")?;
    let chain_id = parse_chain_id(chain_id_param, &env)?;
    let block_hash = parse_block_hash(&chain_id, required_param!(params, "block_id")?, &env)?;

    let json_request = create_rpc_request(req).await?;

    result_to_json_response(
        services::protocol::call_protocol_rpc(
            chain_id_param,
            chain_id,
            block_hash,
            json_request,
            &env,
        ),
        env.log(),
    )
}
