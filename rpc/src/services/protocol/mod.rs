// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This module exposes protocol rpc services.
//!
//! Rule 1:
//!     - if service has different implementation and uses different structs for various protocols, it have to be placed and implemented in correct subpackage proto_XYZ
//!     - and here will be just redirector to correct subpackage by protocol_hash
//! Rule 2:
//!     - if service has the same implementation for various protocol, can be place directly here
//!     - if in new version of protocol is changed behavior, we have to splitted it here aslo by protocol_hash

use std::convert::{TryFrom, TryInto};

use anyhow::{bail, format_err, Error};
use thiserror::Error;

use crypto::hash::{BlockHash, ChainId, FromBytesError, ProtocolHash};
use storage::{
    BlockHeaderWithHash, BlockMetaStorage, BlockMetaStorageReader, BlockStorage, BlockStorageReader,
};
use tezos_api::ffi::{
    HelpersPreapplyBlockRequest, ProtocolRpcRequest, ProtocolRpcResponse, RpcRequest,
};
use tezos_messages::base::rpc_support::RpcJsonMap;
use tezos_messages::base::signature_public_key_hash::ConversionError;
use tezos_messages::protocol::{SupportedProtocol, UnsupportedProtocolError};
use tezos_new_context::context_key_owned;

use crate::helpers::get_context_hash;
use crate::server::RpcServiceEnvironment;
use tezos_wrapper::TezedgeContextClientError;

mod proto_001;
mod proto_002;
mod proto_003;
mod proto_004;
mod proto_005_2;
mod proto_006;
mod proto_007;
mod proto_008;
mod proto_008_2;

#[derive(Debug, Error)]
pub enum RightsError {
    #[error("Rights error, reason: {reason}")]
    ServiceError { reason: Error },
    #[error("Unsupported protocol {protocol}")]
    UnsupportedProtocolError { protocol: String },
}

impl From<ContextParamsError> for RightsError {
    fn from(error: ContextParamsError) -> Self {
        match error {
            ContextParamsError::UnsupportedProtocolError { protocol } => {
                RightsError::UnsupportedProtocolError { protocol }
            }
            _ => RightsError::ServiceError {
                reason: error.into(),
            },
        }
    }
}

impl From<anyhow::Error> for RightsError {
    fn from(error: anyhow::Error) -> Self {
        RightsError::ServiceError { reason: error }
    }
}

/// Return generated baking rights.
///
/// # Arguments
///
/// * `chain_id` - Url path parameter 'chain_id'.
/// * `block_id` - Url path parameter 'block_id', it contains string "head", block level or block hash.
/// * `level` - Url query parameter 'level'.
/// * `delegate` - Url query parameter 'delegate'.
/// * `cycle` - Url query parameter 'cycle'.
/// * `max_priority` - Url query parameter 'max_priority'.
/// * `has_all` - Url query parameter 'all'.
/// * `list` - Context list handler.
/// * `persistent_storage` - Persistent storage handler.
/// * `state` - Current RPC collected state (head).
///
/// Prepare all data to generate baking rights and then use Tezos PRNG to generate them.
pub(crate) fn check_and_get_baking_rights(
    block_hash: &BlockHash,
    level: Option<&str>,
    delegate: Option<&str>,
    cycle: Option<&str>,
    max_priority: Option<&str>,
    has_all: bool,
    env: &RpcServiceEnvironment,
) -> Result<Option<Vec<RpcJsonMap>>, RightsError> {
    // get protocol and constants
    let context_proto_params = get_context_protocol_params(block_hash, env)?;

    // split impl by protocol
    match context_proto_params.protocol_hash {
        SupportedProtocol::Proto001 => proto_001::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto002 => proto_002::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto003 => proto_003::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto004 => proto_004::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto005 => panic!("not yet implemented!"),
        SupportedProtocol::Proto005_2 => proto_005_2::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto006 => proto_006::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto007 => proto_007::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto008 => proto_008::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto008_2 => proto_008_2::rights_service::check_and_get_baking_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            max_priority,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
    }
}

/// Return generated endorsing rights.
///
/// # Arguments
///
/// * `chain_id` - Url path parameter 'chain_id'.
/// * `block_id` - Url path parameter 'block_id', it contains string "head", block level or block hash.
/// * `level` - Url query parameter 'level'.
/// * `delegate` - Url query parameter 'delegate'.
/// * `cycle` - Url query parameter 'cycle'.
/// * `has_all` - Url query parameter 'all'.
/// * `list` - Context list handler.
/// * `persistent_storage` - Persistent storage handler.
/// * `state` - Current RPC collected state (head).
///
/// Prepare all data to generate endorsing rights and then use Tezos PRNG to generate them.
pub(crate) fn check_and_get_endorsing_rights(
    block_hash: &BlockHash,
    level: Option<&str>,
    delegate: Option<&str>,
    cycle: Option<&str>,
    has_all: bool,
    env: &RpcServiceEnvironment,
) -> Result<Option<Vec<RpcJsonMap>>, RightsError> {
    // get protocol and constants
    let context_proto_params = get_context_protocol_params(block_hash, env)?;

    // split impl by protocol
    match context_proto_params.protocol_hash {
        SupportedProtocol::Proto001 => proto_001::rights_service::check_and_get_endorsing_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto002 => proto_002::rights_service::check_and_get_endorsing_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto003 => proto_003::rights_service::check_and_get_endorsing_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto004 => proto_004::rights_service::check_and_get_endorsing_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto005 => panic!("not yet implemented!"),
        SupportedProtocol::Proto005_2 => {
            proto_005_2::rights_service::check_and_get_endorsing_rights(
                context_proto_params,
                level,
                delegate,
                cycle,
                has_all,
                env.tezedge_context(),
            )
            .map_err(RightsError::from)
        }
        SupportedProtocol::Proto006 => proto_006::rights_service::check_and_get_endorsing_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto007 => proto_007::rights_service::check_and_get_endorsing_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto008 => proto_008::rights_service::check_and_get_endorsing_rights(
            context_proto_params,
            level,
            delegate,
            cycle,
            has_all,
            env.tezedge_context(),
        )
        .map_err(RightsError::from),
        SupportedProtocol::Proto008_2 => {
            proto_008_2::rights_service::check_and_get_endorsing_rights(
                context_proto_params,
                level,
                delegate,
                cycle,
                has_all,
                env.tezedge_context(),
            )
            .map_err(RightsError::from)
        }
    }
}

#[derive(Debug, Error)]
pub enum VotesError {
    #[error("Votes error, reason: {reason}")]
    ServiceError { reason: Error },
    #[error("Unsupported protocol {protocol}")]
    UnsupportedProtocolError { protocol: String },
    #[error("This rpc is not suported in this protocol {protocol}")]
    UnsupportedProtocolRpc { protocol: String },
}

impl From<anyhow::Error> for VotesError {
    fn from(error: anyhow::Error) -> Self {
        VotesError::ServiceError { reason: error }
    }
}

impl From<TezedgeContextClientError> for VotesError {
    fn from(error: TezedgeContextClientError) -> Self {
        VotesError::ServiceError {
            reason: error.into(),
        }
    }
}

impl From<ConversionError> for VotesError {
    fn from(error: ConversionError) -> Self {
        VotesError::ServiceError {
            reason: error.into(),
        }
    }
}

impl From<UnsupportedProtocolError> for VotesError {
    fn from(error: UnsupportedProtocolError) -> Self {
        VotesError::UnsupportedProtocolError {
            protocol: error.protocol,
        }
    }
}

impl From<serde_json::Error> for VotesError {
    fn from(error: serde_json::Error) -> Self {
        VotesError::ServiceError {
            reason: error.into(),
        }
    }
}

impl From<FromBytesError> for VotesError {
    fn from(error: FromBytesError) -> Self {
        VotesError::ServiceError {
            reason: error.into(),
        }
    }
}

pub(crate) fn get_votes_listings(
    block_hash: &BlockHash,
    env: &RpcServiceEnvironment,
) -> Result<Option<serde_json::Value>, VotesError> {
    // TODO - TE-261: this will not work with Irmin right now, we should check that or
    // try to reimplement the missing parts on top of Irmin too.
    // if only_irmin {
    //     return Err(ContextParamsError::UnsupportedProtocolError {
    //         protocol: "only-supported-with-tezedge-context".to_string(),
    //     });
    // }
    let context_hash = get_context_hash(block_hash, env)?;

    // get protocol version
    let protocol_hash = if let Some(protocol_hash) = env
        .tezedge_context()
        .get_key_from_history(&context_hash, context_key_owned!("protocol"))?
    {
        ProtocolHash::try_from(protocol_hash)?
    } else {
        return Err(VotesError::ServiceError {
            reason: format_err!(
                "No protocol found in context for block_hash: {}",
                block_hash.to_base58_check()
            ),
        });
    };

    // check if we support impl for this protocol
    let supported_protocol = SupportedProtocol::try_from(protocol_hash)?;
    match supported_protocol {
        SupportedProtocol::Proto001 | SupportedProtocol::Proto002 => {
            Err(VotesError::UnsupportedProtocolRpc {
                protocol: supported_protocol.protocol_hash(),
            })
        }
        SupportedProtocol::Proto003 => {
            proto_003::votes_services::get_votes_listings(env, &context_hash)
        }
        SupportedProtocol::Proto004 => {
            proto_004::votes_services::get_votes_listings(env, &context_hash)
        }
        SupportedProtocol::Proto005 => Err(VotesError::UnsupportedProtocolError {
            protocol: supported_protocol.protocol_hash(),
        }),
        SupportedProtocol::Proto005_2 => {
            proto_005_2::votes_services::get_votes_listings(env, &context_hash)
        }
        SupportedProtocol::Proto006 => {
            proto_006::votes_services::get_votes_listings(env, &context_hash)
        }
        SupportedProtocol::Proto007 => {
            proto_007::votes_services::get_votes_listings(env, &context_hash)
        }
        SupportedProtocol::Proto008 => {
            proto_008::votes_service::get_votes_listings(env, &context_hash)
        }
        SupportedProtocol::Proto008_2 => {
            proto_008_2::votes_service::get_votes_listings(env, &context_hash)
        }
    }
}

/// Get protocol context constants from context list
/// (just for RPC render use-case, do not use in processing or algorithms)
///
/// # Arguments
///
/// * `block_id` - Url path parameter 'block_id', it contains string "head", block level or block hash.
/// * `opt_level` - Optionaly input block level from block_id if is already known to prevent double code execution.
/// * `list` - Context list handler.
/// * `persistent_storage` - Persistent storage handler.
/// * `state` - Current RPC collected state (head).
pub(crate) fn get_context_constants_just_for_rpc(
    block_hash: &BlockHash,
    env: &RpcServiceEnvironment,
) -> Result<Option<RpcJsonMap>, ContextParamsError> {
    let context_proto_params = get_context_protocol_params(block_hash, env)?;
    Ok(tezos_messages::protocol::get_constants_for_rpc(
        &context_proto_params.constants_data,
        &context_proto_params.protocol_hash,
    )?)
}

// TODO: TE-220, be more explicit about the kind of response from the RPC service
fn handle_rpc_response(
    response: &ProtocolRpcResponse,
    context_path: String,
) -> Result<serde_json::value::Value, anyhow::Error> {
    match response {
        ProtocolRpcResponse::RPCOk(body) => Ok(serde_json::from_str(&body)?),
        other => Err(anyhow::format_err!(
            "Got non-OK response from protocol-RPC service '{}', reason: {:?}",
            context_path,
            other
        )),
    }
}

pub(crate) fn call_protocol_rpc(
    chain_param: &str,
    chain_id: ChainId,
    block_hash: BlockHash,
    rpc_request: RpcRequest,
    env: &RpcServiceEnvironment,
) -> Result<serde_json::value::Value, anyhow::Error> {
    let context_path = rpc_request.context_path.clone();
    let request =
        create_protocol_rpc_request(chain_param, chain_id, block_hash, rpc_request, &env)?;

    // TODO: retry?
    let response = env
        .tezos_readonly_api()
        .pool
        .get()?
        .api
        .call_protocol_rpc(request)?;

    handle_rpc_response(&response, context_path)
}

pub(crate) fn preapply_operations(
    chain_param: &str,
    chain_id: ChainId,
    block_hash: BlockHash,
    rpc_request: RpcRequest,
    env: &RpcServiceEnvironment,
) -> Result<serde_json::value::Value, anyhow::Error> {
    let request =
        create_protocol_rpc_request(chain_param, chain_id, block_hash, rpc_request, &env)?;

    // TODO: retry?
    let response = env
        .tezos_readonly_api()
        .pool
        .get()?
        .api
        .helpers_preapply_operations(request)?;

    Ok(serde_json::from_str(&response.body)?)
}

pub(crate) fn preapply_block(
    chain_param: &str,
    chain_id: ChainId,
    block_hash: BlockHash,
    rpc_request: RpcRequest,
    env: &RpcServiceEnvironment,
) -> Result<serde_json::value::Value, anyhow::Error> {
    let block_storage = BlockStorage::new(env.persistent_storage());
    let block_meta_storage = BlockMetaStorage::new(env.persistent_storage());

    let (block_header, (predecessor_block_metadata_hash, predecessor_ops_metadata_hash)) =
        match block_storage.get(&block_hash)? {
            Some(block_header) => match block_meta_storage.get_additional_data(&block_hash)? {
                Some(block_header_additional_data) => {
                    (block_header, block_header_additional_data.into())
                }
                None => bail!(
                    "No block additioanl data found for hash: {}",
                    block_hash.to_base58_check()
                ),
            },
            None => bail!(
                "No block header found for hash: {}",
                block_hash.to_base58_check()
            ),
        };

    // create request to ffi
    let request = HelpersPreapplyBlockRequest {
        protocol_rpc_request: ProtocolRpcRequest {
            chain_arg: chain_param.to_string(),
            block_header: block_header.header.as_ref().clone(),
            request: rpc_request,
            chain_id,
        },
        predecessor_block_metadata_hash,
        predecessor_ops_metadata_hash,
    };

    // TODO: retry?
    let response = env
        .tezos_readonly_api()
        .pool
        .get()?
        .api
        .helpers_preapply_block(request)?;

    Ok(serde_json::from_str(&response.body)?)
}

fn create_protocol_rpc_request(
    chain_param: &str,
    chain_id: ChainId,
    block_hash: BlockHash,
    rpc_request: RpcRequest,
    env: &RpcServiceEnvironment,
) -> Result<ProtocolRpcRequest, anyhow::Error> {
    let block_storage = BlockStorage::new(env.persistent_storage());
    let block_header = match block_storage.get(&block_hash)? {
        Some(header) => header.header.as_ref().clone(),
        None => bail!(
            "No block header found for hash: {}",
            block_hash.to_base58_check()
        ),
    };

    // create request to ffi
    Ok(ProtocolRpcRequest {
        chain_arg: chain_param.to_string(),
        block_header,
        request: rpc_request,
        chain_id,
    })
}

pub(crate) struct ContextProtocolParam {
    pub protocol_hash: SupportedProtocol,
    pub constants_data: Vec<u8>,
    pub block_header: BlockHeaderWithHash,
}

#[derive(Debug, Error)]
pub enum ContextParamsError {
    #[error("Protocol not found in context for block: {0}")]
    NoProtocolForBlock(String),
    #[error("Protocol constants not found in context for block: {0}")]
    NoConstantsForBlock(String),
    #[error("Storage error occurred, reason: {reason}")]
    StorageError { reason: storage::StorageError },
    #[error("Context error occurred, reason: {reason}")]
    ContextError { reason: TezedgeContextClientError },
    #[error("Context constants, reason: {reason}")]
    ContextConstantsDecodeError {
        reason: tezos_messages::protocol::ContextConstantsDecodeError,
    },
    #[error("Unsupported protocol {protocol}")]
    UnsupportedProtocolError { protocol: String },
    #[error("Hash error {error}")]
    HashError { error: FromBytesError },
}

impl From<storage::StorageError> for ContextParamsError {
    fn from(error: storage::StorageError) -> Self {
        ContextParamsError::StorageError { reason: error }
    }
}

impl From<TezedgeContextClientError> for ContextParamsError {
    fn from(error: TezedgeContextClientError) -> Self {
        ContextParamsError::ContextError { reason: error }
    }
}

impl From<UnsupportedProtocolError> for ContextParamsError {
    fn from(error: UnsupportedProtocolError) -> Self {
        ContextParamsError::UnsupportedProtocolError {
            protocol: error.protocol,
        }
    }
}

impl From<tezos_messages::protocol::ContextConstantsDecodeError> for ContextParamsError {
    fn from(error: tezos_messages::protocol::ContextConstantsDecodeError) -> Self {
        ContextParamsError::ContextConstantsDecodeError { reason: error }
    }
}

impl From<FromBytesError> for ContextParamsError {
    fn from(error: FromBytesError) -> ContextParamsError {
        ContextParamsError::HashError { error }
    }
}

/// Get protocol and context constants as bytes from context list for desired block or level
///
/// # Arguments
///
/// * `block_hash` - [BlockHash]
/// * `opt_level` - Optionaly input block level from block_id if is already known to prevent double code execution.
/// * `list` - Context list handler.
/// * `persistent_storage` - Persistent storage handler.
/// * `state` - Current RPC collected state (head).
pub(crate) fn get_context_protocol_params(
    block_hash: &BlockHash,
    env: &RpcServiceEnvironment,
) -> Result<ContextProtocolParam, ContextParamsError> {
    // TODO - TE-261: this will not work with Irmin right now, we should check that or
    // try to reimplement the missing parts on top of Irmin too.
    // if only_irmin {
    //     return Err(ContextParamsError::UnsupportedProtocolError {
    //         protocol: "only-supported-with-tezedge-context".to_string(),
    //     });
    // }

    // get block header
    let block_header = match BlockStorage::new(env.persistent_storage()).get(block_hash)? {
        Some(block) => block,
        None => return Err(storage::StorageError::MissingKey.into()),
    };

    let protocol_hash: ProtocolHash;
    let constants: Vec<u8>;
    {
        let context = env.tezedge_context();
        let context_hash = block_header.header.context();

        if let Some(data) =
            context.get_key_from_history(&context_hash, context_key_owned!("protocol"))?
        {
            protocol_hash = ProtocolHash::try_from(data)?;
        } else {
            return Err(ContextParamsError::NoProtocolForBlock(
                block_hash.to_base58_check(),
            ));
        }

        if let Some(data) =
            context.get_key_from_history(&context_hash, context_key_owned!("data/v1/constants"))?
        {
            constants = data;
        } else {
            return Err(ContextParamsError::NoConstantsForBlock(
                block_hash.to_base58_check(),
            ));
        }
    };

    Ok(ContextProtocolParam {
        protocol_hash: protocol_hash.try_into()?,
        constants_data: constants,
        block_header,
    })
}
