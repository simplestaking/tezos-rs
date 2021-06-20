// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

// TODO - TE-261: many things commented out here because they don't work with the new
// context until we reintroduce something equivalent to the context actions storage.
// The timings database, along with the readonly IPC context access could be used
// to reproduce the same functionality.

use std::convert::TryFrom;

use anyhow::bail;
use crypto::hash::ContractKt1Hash;
use serde::Serialize;
use slog::Logger;

use crypto::hash::{BlockHash, ChainId, ContractTz1Hash, ContractTz2Hash, ContractTz3Hash};
use shell::stats::memory::{Memory, MemoryData, MemoryStatsResult};
//use tezos_new_context::actions::context_action_storage::{
//    contract_id_to_contract_address_for_index, ContextActionBlockDetails, ContextActionFilters,
//    ContextActionJson, ContextActionRecordValue, ContextActionStorageReader, ContextActionType,
//};
use storage::{
    BlockMetaStorage, BlockMetaStorageReader, BlockStorage, BlockStorageReader, PersistentStorage,
};
//use tezos_context::channel::ContextAction;
use tezos_messages::base::rpc_support::UniversalValue;
use tezos_messages::base::ConversionError;

use crate::helpers::{BlockMetadata, PagedResult};
use crate::server::RpcServiceEnvironment;
use crate::services::protocol::get_context_protocol_params;

pub type ContractAddress = Vec<u8>;

/// Get actions for a specific block in ascending order.
#[allow(dead_code)]
pub(crate) fn get_block_actions(
    block_hash: BlockHash,
    persistent_storage: &PersistentStorage,
) -> Result<Vec<() /*ContextAction*/>, anyhow::Error> {
    get_block_actions_by_hash(
        &ensure_context_action_storage(persistent_storage)?,
        &block_hash,
    )
}

#[allow(dead_code)]
pub(crate) fn get_block_actions_by_hash(
    _context_action_storage: &(), /*&ContextActionStorageReader,*/
    _block_hash: &BlockHash,
) -> Result<Vec<() /*ContextAction*/>, anyhow::Error> {
    //context_action_storage
    //    .get_by_block_hash(&block_hash)
    //    .map(|values| values.into_iter().map(|v| v.into_action()).collect())
    //    .map_err(|e| e.into())
    Err(anyhow::format_err!(
        "Persistent context actions storage is not implemented!"
    ))
}

pub(crate) fn get_block_actions_cursor(
    _block_hash: BlockHash,
    _cursor_id: Option<u64>,
    _limit: Option<usize>,
    _action_types: Option<&str>,
    _persistent_storage: &PersistentStorage,
) -> Result<Vec<() /*ContextActionJson*/>, anyhow::Error> {
    //let context_action_storage = ensure_context_action_storage(persistent_storage)?;
    //let mut filters = ContextActionFilters::with_block_hash(block_hash.into());
    //if let Some(action_types) = action_types {
    //    filters = filters.with_action_types(get_action_types(action_types));
    //}
    //let values = context_action_storage
    //    .load_cursor(cursor_id, limit, filters)?
    //    .into_iter()
    //    .map(ContextActionJson::from)
    //    .collect();
    //Ok(values)
    Err(anyhow::format_err!(
        "Persistent context actions storage is not implemented!"
    ))
}

pub(crate) fn get_block_action_details(
    _block_hash: BlockHash,
    _persistent_storage: &PersistentStorage,
) -> Result<() /*ContextActionBlockDetails*/, anyhow::Error> {
    //let context_action_storage = ensure_context_action_storage(persistent_storage)?;

    //let actions: Vec<ContextAction> = context_action_storage
    //    .get_by_block_hash(&block_hash)?
    //    .into_iter()
    //    .map(|action_record| action_record.action)
    //    .collect();

    //Ok(ContextActionBlockDetails::calculate_block_action_details(
    //    actions,
    //))
    Err(anyhow::format_err!(
        "Persistent context actions storage is not implemented!"
    ))
}

pub(crate) fn get_contract_actions_cursor(
    _contract_address: &str,
    _cursor_id: Option<u64>,
    _limit: Option<usize>,
    _action_types: Option<&str>,
    _persistent_storage: &PersistentStorage,
) -> Result<Vec<() /*ContextActionJson*/>, anyhow::Error> {
    //let context_action_storage = ensure_context_action_storage(persistent_storage)?;
    //let contract_address = contract_id_to_contract_address_for_index(contract_address)?;
    //let mut filters = ContextActionFilters::with_contract_id(contract_address);
    //if let Some(action_types) = action_types {
    //    filters = filters.with_action_types(get_action_types(action_types));
    //}
    //let values = context_action_storage
    //    .load_cursor(cursor_id, limit, filters)?
    //    .into_iter()
    //    .map(ContextActionJson::from)
    //    .collect();
    //Ok(values)
    Err(anyhow::format_err!(
        "Persistent context actions storage is not implemented!"
    ))
}

/// Get actions for a specific contract in ascending order.
#[allow(dead_code)]
pub(crate) fn get_contract_actions(
    _contract_id: &str,
    _from_id: Option<u64>,
    _limit: usize,
    _persistent_storage: &PersistentStorage,
) -> Result<PagedResult<Vec<() /*ContextActionRecordValue*/>>, anyhow::Error> {
    //let context_action_storage = ensure_context_action_storage(persistent_storage)?;
    //let contract_address = contract_id_to_contract_address_for_index(contract_id)?;
    //let mut context_records =
    //    context_action_storage.get_by_contract_address(&contract_address, from_id, limit + 1)?;
    //let next_id = if context_records.len() > limit {
    //    context_records.last().map(|rec| rec.id())
    //} else {
    //    None
    //};
    //context_records.truncate(std::cmp::min(context_records.len(), limit));
    //Ok(PagedResult::new(context_records, next_id, limit))
    Err(anyhow::format_err!(
        "Persistent context actions storage is not implemented!"
    ))
}

pub(crate) fn get_stats_memory() -> MemoryStatsResult<MemoryData> {
    let memory = Memory::new();
    memory.get_memory_stats()
}

pub(crate) fn get_stats_memory_protocol_runners() -> MemoryStatsResult<Vec<MemoryData>> {
    let memory = Memory::new();
    memory.get_memory_stats_protocol_runners()
}

pub(crate) fn get_cycle_length_for_block(
    block_hash: &BlockHash,
    env: &RpcServiceEnvironment,
    log: &Logger,
) -> Result<i32, anyhow::Error> {
    if let Ok(context_proto_params) = get_context_protocol_params(block_hash, env) {
        Ok(tezos_messages::protocol::get_constants_for_rpc(
            &context_proto_params.constants_data,
            &context_proto_params.protocol_hash,
        )?
            .map(|constants| constants.get("blocks_per_cycle")
                .map(|value| if let UniversalValue::Number(value) = value { *value } else {
                    slog::warn!(log, "Cycle length missing"; "block" => block_hash.to_base58_check());
                    4096
                })
            ).flatten().unwrap_or_else(|| {
            slog::warn!(log, "Cycle length missing"; "block" => block_hash.to_base58_check());
            4096
        }))
    } else {
        slog::warn!(log, "Cycle length missing"; "block" => block_hash.to_base58_check());
        Ok(4096)
    }
}

pub(crate) fn get_dev_version() -> String {
    let version_env: &'static str = env!("CARGO_PKG_VERSION");

    format!("v{}", version_env.to_string())
}

#[inline]
pub(crate) fn _get_action_types(_action_types: &str) -> Vec<() /*ContextActionType*/> {
    //action_types
    //    .split(',')
    //    .filter_map(|x: &str| x.parse().ok())
    //    .collect()
    vec![]
}

#[inline]
pub(crate) fn ensure_context_action_storage(
    _persistent_storage: &PersistentStorage,
) -> Result<() /*ContextActionStorageReader*/, anyhow::Error> {
    Err(anyhow::format_err!(
        "Persistent context actions storage is not implemented!"
    ))
}

/// Retrieve blocks from database.
pub(crate) fn get_blocks(
    _chain_id: ChainId,
    block_hash: BlockHash,
    every_nth_level: Option<i32>,
    limit: usize,
    env: &RpcServiceEnvironment,
) -> Result<Vec<SlimBlockData>, anyhow::Error> {
    let block_meta_storage = BlockMetaStorage::new(env.persistent_storage());

    let blocks = match every_nth_level {
        Some(every_nth_level) => BlockStorage::new(env.persistent_storage())
            .get_every_nth_with_json_data(every_nth_level, &block_hash, limit),
        None => BlockStorage::new(env.persistent_storage())
            .get_multiple_with_json_data(&block_hash, limit),
    }?
    .into_iter()
    .map(|(block_header, block_json_data)| {
        if let Some(block_additional_data) = block_meta_storage.get_additional_data(&block_hash)? {
            let response = env
                .tezos_readonly_api()
                .pool
                .get()?
                .api
                .apply_block_result_metadata(
                    block_header.header.context().clone(),
                    block_json_data.block_header_proto_metadata_bytes,
                    block_additional_data.max_operations_ttl().into(),
                    block_additional_data.protocol_hash,
                    block_additional_data.next_protocol_hash,
                )?;

            let metadata: BlockMetadata = serde_json::from_str(&response).unwrap_or_default();
            let cycle_position = if let Some(level) = metadata.get("level") {
                level["cycle_position"].as_i64()
            } else if let Some(level) = metadata.get("level_info") {
                level["cycle_position"].as_i64()
            } else {
                None
            };

            Ok(SlimBlockData {
                level: block_header.header.level(),
                block_hash: block_header.hash.to_base58_check(),
                timestamp: block_header.header.timestamp().to_string(),
                cycle_position,
            })
        } else {
            bail!(
                "No additional data found for block_hash: {}",
                block_hash.to_base58_check()
            )
        }
    })
    .filter_map(Result::ok)
    .collect::<Vec<SlimBlockData>>();
    Ok(blocks)
}

/// Struct to show in tezedge explorer to lower data flow
#[derive(Serialize, Debug, Clone)]
pub struct SlimBlockData {
    pub level: i32,
    pub block_hash: String,
    pub timestamp: String,
    // TODO: TE-199 Refactor FullBlockInfo (should be i32)
    // Note: serde's Value can be converted into Option<i64> without panicing, the original tezos value is an i32
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cycle_position: Option<i64>,
}

/// Dedicated function to convert contract id to contract address for indexing in storage action,
/// contract id index has specified length [LEN_TOTAL]
///
/// # Arguments
///
/// * `contract_id` - contract id (tz... or KT1...)
#[inline]
pub(crate) fn contract_id_to_contract_address_for_index(
    contract_id: &str,
) -> Result<ContractAddress, ConversionError> {
    let contract_address = {
        if contract_id.len() == 44 {
            hex::decode(contract_id)?
        } else if contract_id.len() > 3 {
            let mut contract_address = Vec::with_capacity(22);
            match &contract_id[0..3] {
                "tz1" => {
                    contract_address.extend(&[0, 0]);
                    contract_address.extend(ContractTz1Hash::try_from(contract_id)?.as_ref());
                }
                "tz2" => {
                    contract_address.extend(&[0, 1]);
                    contract_address.extend(ContractTz2Hash::try_from(contract_id)?.as_ref());
                }
                "tz3" => {
                    contract_address.extend(&[0, 2]);
                    contract_address.extend(ContractTz3Hash::try_from(contract_id)?.as_ref());
                }
                "KT1" => {
                    contract_address.push(1);
                    contract_address.extend(ContractKt1Hash::try_from(contract_id)?.as_ref());
                    contract_address.push(0);
                }
                _ => {
                    return Err(ConversionError::InvalidCurveTag {
                        curve_tag: contract_id.to_string(),
                    });
                }
            }
            contract_address
        } else {
            return Err(ConversionError::InvalidHash {
                hash: contract_id.to_string(),
            });
        }
    };

    Ok(contract_address)
}
