// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use anyhow::format_err;
use itertools::Itertools;

use crate::services::dev_services::contract_id_to_contract_address_for_index;
use tezos_messages::base::rpc_support::{RpcJsonMap, ToRpcJsonMap};
use tezos_messages::base::signature_public_key_hash::SignaturePublicKeyHash;
use tezos_messages::protocol::proto_007::rights::{BakingRights, EndorsingRight};
use tezos_wrapper::TezedgeContextClient;

use crate::services::protocol::proto_007::helpers::{
    get_prng_number, init_prng, EndorserSlots, RightsConstants, RightsContextData, RightsParams,
};
use crate::services::protocol::ContextProtocolParam;

/// Return generated baking rights.
///
/// # Arguments
///
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
    context_proto_params: ContextProtocolParam,
    level: Option<&str>,
    delegate: Option<&str>,
    cycle: Option<&str>,
    max_priority: Option<&str>,
    has_all: bool,
    context: &TezedgeContextClient,
) -> Result<Option<Vec<RpcJsonMap>>, anyhow::Error> {
    let constants: RightsConstants =
        RightsConstants::parse_rights_constants(&context_proto_params)?;

    let params: RightsParams = RightsParams::parse_rights_parameters(
        level,
        delegate,
        cycle,
        max_priority,
        has_all,
        &constants,
        &context_proto_params.block_header,
        true,
    )?;

    let context_data: RightsContextData = RightsContextData::prepare_context_data_for_rights(
        params.clone(),
        constants.clone(),
        (&context_proto_params.block_header.header.context(), context),
    )?;

    get_baking_rights(&context_data, &params, &constants)
}

/// Use prepared data to generate baking rights
///
/// # Arguments
///
/// * `context_data` - Data from context list used in baking and endorsing rights generation filled in [RightsContextData](RightsContextData::prepare_context_data_for_rights).
/// * `parameters` - Parameters created by [RightsParams](RightsParams::parse_rights_parameters).
/// * `constants` - Context constants used in baking and endorsing rights [RightsConstants](RightsConstants::parse_rights_constants).
#[inline]
pub(crate) fn get_baking_rights(
    context_data: &RightsContextData,
    parameters: &RightsParams,
    constants: &RightsConstants,
) -> Result<Option<Vec<RpcJsonMap>>, anyhow::Error> {
    let mut baking_rights = Vec::<BakingRights>::new();

    let blocks_per_cycle = *constants.blocks_per_cycle();
    let time_between_blocks = constants.time_between_blocks();

    let timestamp = parameters.block_timestamp();
    let block_level = parameters.block_level();

    // iterate through the whole cycle if necessery
    if let Some(cycle) = parameters.requested_cycle() {
        let first_block_level = cycle * (blocks_per_cycle as i64) + 1;
        let last_block_level = first_block_level + (blocks_per_cycle as i64);

        for level in first_block_level..last_block_level {
            let seconds_to_add = (level - block_level).abs() * time_between_blocks[0];
            let estimated_timestamp = timestamp + seconds_to_add;

            // assign rolls goes here
            baking_rights_assign_rolls(
                &parameters,
                &constants,
                &context_data,
                level.try_into()?,
                estimated_timestamp,
                true,
                &mut baking_rights,
            )?;

            // baking_rights = merge_slices!(&baking_rights, &level_baking_rights);
        }
    } else {
        let level = *parameters.requested_level();
        let seconds_to_add = (level - block_level).abs() * time_between_blocks[0];
        let estimated_timestamp = timestamp + seconds_to_add;
        // assign rolls goes here
        baking_rights_assign_rolls(
            &parameters,
            &constants,
            &context_data,
            level.try_into()?,
            estimated_timestamp,
            false,
            &mut baking_rights,
        )?;
    }

    // if there is some delegate specified, retrive his priorities
    if let Some(delegate) = parameters.requested_delegate() {
        let delegate = &SignaturePublicKeyHash::from_b58_hash(delegate)?;
        Ok(Some(
            baking_rights
                .into_iter()
                .filter(|val| val.delegate.eq(delegate))
                .map(|val| val.as_map())
                .collect::<Vec<RpcJsonMap>>(),
        ))
    } else {
        Ok(Some(
            baking_rights
                .into_iter()
                .map(|val| val.as_map())
                .collect::<Vec<RpcJsonMap>>(),
        ))
    }
}

/// Use prepared data to generate baking rights
///
/// # Arguments
///
/// * `parameters` - Parameters created by [RightsParams](RightsParams::parse_rights_parameters).
/// * `constants` - Context constants used in baking and endorsing rights [RightsConstants](RightsConstants::parse_rights_constants).
/// * `context_data` - Data from context list used in baking and endorsing rights generation filled in [RightsContextData](RightsContextData::prepare_context_data_for_rights).
/// * `level` - Level to feed Tezos PRNG.
/// * `estimated_head_timestamp` - Estimated time of baking, is set to None if in past relative to block_id.
///
/// Baking priorities are are assigned to Roles, the default behavior is to include only the top priority for the delegate
#[inline]
fn baking_rights_assign_rolls(
    parameters: &RightsParams,
    constants: &RightsConstants,
    context_data: &RightsContextData,
    level: i32,
    estimated_head_timestamp: i64,
    is_cycle: bool,
    baking_rights: &mut Vec<BakingRights>,
) -> Result<(), anyhow::Error> {
    const BAKING_USE_STRING: &[u8] = b"level baking:";

    // hashset is defined to keep track of the delegates with priorities already assigned
    let mut assigned = HashSet::new();

    let time_between_blocks = constants.time_between_blocks();

    let max_priority = *parameters.max_priority();
    let has_all = parameters.has_all();
    let block_level = *parameters.block_level();
    let last_roll = *context_data.last_roll();
    let rolls_map = context_data.rolls();
    let display_level: i32 = (*parameters.display_level()).try_into()?;

    for priority in 0..max_priority + 1 {
        // draw the rolls for the requested parameters
        let delegate_to_assign;
        // TODO: priority can overflow in the ocaml code, do a priority % i32::max_value()
        let mut state = init_prng(
            &context_data,
            &constants,
            BAKING_USE_STRING,
            level,
            priority.try_into()?,
        )?;

        loop {
            let (random_num, sequence) = get_prng_number(state, last_roll)?;

            if let Some(d) = rolls_map.get(&random_num) {
                delegate_to_assign = d;
                break;
            } else {
                state = sequence;
            }
        }

        // if the delegate was assgined and the the has_all flag is not set skip this priority
        if assigned.contains(&delegate_to_assign) && !has_all {
            continue;
        }

        // we omit the estimated_time field if the block on the requested level is already baked
        let priority_timestamp = if block_level < (level as i64) {
            let time = if time_between_blocks.len() == 1 {
                time_between_blocks[0]
            } else {
                time_between_blocks[1]
            };
            Some(estimated_head_timestamp + (priority as i64 * time))
        } else {
            None
        };

        // used to handle corner cases, where the requested level < 1, not necessary when handling whole cycles
        let rights_level = if is_cycle { level } else { display_level };

        baking_rights.push(BakingRights::new(
            rights_level,
            SignaturePublicKeyHash::from_b58_hash(&delegate_to_assign)?,
            priority as u16,
            priority_timestamp,
        ));
        assigned.insert(delegate_to_assign);
    }
    Ok(())
}

/// Return generated endorsing rights.
///
/// # Arguments
///
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
    context_proto_params: ContextProtocolParam,
    level: Option<&str>,
    delegate: Option<&str>,
    cycle: Option<&str>,
    has_all: bool,
    context: &TezedgeContextClient,
) -> Result<Option<Vec<RpcJsonMap>>, anyhow::Error> {
    let constants: RightsConstants =
        RightsConstants::parse_rights_constants(&context_proto_params)?;

    let params: RightsParams = RightsParams::parse_rights_parameters(
        level,
        delegate,
        cycle,
        None,
        has_all,
        &constants,
        &context_proto_params.block_header,
        false,
    )?;

    let context_data: RightsContextData = RightsContextData::prepare_context_data_for_rights(
        params.clone(),
        constants.clone(),
        (&context_proto_params.block_header.header.context(), context),
    )?;

    get_endorsing_rights(&context_data, &params, &constants)
}

/// Use prepared data to generate endosring rights
///
/// # Arguments
///
/// * `context_data` - Data from context list used in baking and endorsing rights generation filled in [RightsContextData](RightsContextData::prepare_context_data_for_rights).
/// * `parameters` - Parameters created by [RightsParams](RightsParams::parse_rights_parameters).
/// * `constants` - Context constants used in baking and endorsing rights [RightsConstants](RightsConstants::parse_rights_constants).
fn get_endorsing_rights(
    context_data: &RightsContextData,
    parameters: &RightsParams,
    constants: &RightsConstants,
) -> Result<Option<Vec<RpcJsonMap>>, anyhow::Error> {
    // define helper and output variables
    let mut endorsing_rights = Vec::<EndorsingRight>::new();

    // when query param cycle is specified then iterate over all cycle levels, else only given level
    if let Some(cycle) = parameters.requested_cycle() {
        let blocks_per_cycle = *constants.blocks_per_cycle();
        let first_cycle_level = cycle * (blocks_per_cycle as i64) + 1;
        let last_cycle_level = first_cycle_level + (blocks_per_cycle as i64);
        for level in first_cycle_level..last_cycle_level {
            // get estimated time first because is equal for all endorsers in given level
            // the base level for estimated time computation is level of previous block
            let estimated_time: Option<i64> =
                parameters.get_estimated_time(constants, Some(level - 1));

            complete_endorsing_rights_for_level(
                context_data,
                parameters,
                constants,
                level,
                level,
                estimated_time,
                &mut endorsing_rights,
            )?;
            // endorsing_rights = merge_slices!(&endorsing_rights, &level_endorsing_rights);
        }
    } else {
        // use level prepared during parameter parsing to compute estimated time
        let estimated_time: Option<i64> = parameters.get_estimated_time(constants, None);

        complete_endorsing_rights_for_level(
            context_data,
            parameters,
            constants,
            *parameters.requested_level(),
            *parameters.display_level(),
            estimated_time,
            &mut endorsing_rights,
        )?;
    };

    Ok(Some(
        endorsing_rights
            .into_iter()
            .map(|val| val.as_map())
            .collect::<Vec<RpcJsonMap>>(),
    ))
}

/// Use prepared data to generate endosring rights
///
/// # Arguments
///
/// * `context_data` - Data from context list used in baking and endorsing rights generation filled in [RightsContextData](RightsContextData::prepare_context_data_for_rights).
/// * `parameters` - Parameters created by [RightsParams](RightsParams::parse_rights_parameters).
/// * `constants` - Context constants used in baking and endorsing rights [RightsConstants](RightsConstants::parse_rights_constants).
/// * `level` - Level to feed Tezos PRNG.
/// * `display_level` - Level to be displayed in output.
/// * `estimated_time` - Estimated time of endorsement, is set to None if in past relative to block_id.
#[inline]
fn complete_endorsing_rights_for_level(
    context_data: &RightsContextData,
    parameters: &RightsParams,
    constants: &RightsConstants,
    level: i64,
    display_level: i64,
    estimated_time: Option<i64>,
    endorsing_rights: &mut Vec<EndorsingRight>,
) -> Result<(), anyhow::Error> {
    // endorsers_slots is needed to group all slots by delegate
    let endorsers_slots = get_endorsers_slots(constants, context_data, level)?;

    // convert contract id to hash contract address hex byte string (needed for ordering)
    let mut endorers_slots_keys_for_order: HashMap<String, String> = HashMap::new();
    for key in endorsers_slots.keys() {
        endorers_slots_keys_for_order.insert(
            hex::encode(contract_id_to_contract_address_for_index(key.as_str())?),
            key.clone(),
        );
    }

    // order descending by delegate public key hash address hex byte string
    for delegate in endorers_slots_keys_for_order.keys().sorted().rev() {
        let delegate_key = endorers_slots_keys_for_order
            .get(delegate)
            .ok_or_else(|| format_err!("missing delegate key"))?;
        let delegate_data = endorsers_slots.get(delegate_key).ok_or_else(|| {
            format_err!("missing EndorserSlots for delegate_key: {:?}", delegate_key)
        })?;

        // prepare delegate contract id
        let delegate_contract_id = delegate_data.contract_id();

        // filter delegates
        if let Some(d) = parameters.requested_delegate() {
            if delegate_contract_id != d {
                continue;
            }
        }

        endorsing_rights.push(EndorsingRight::new(
            display_level.try_into()?,
            SignaturePublicKeyHash::from_b58_hash(&delegate_contract_id)?,
            delegate_data.slots().clone(),
            estimated_time,
        ))
    }
    Ok(())
}

/// Use tezos PRNG to collect all slots for each endorser by public key hash (for later ordering of endorsers)
///
/// # Arguments
///
/// * `constants` - Context constants used in baking and endorsing rights [RightsConstants](RightsConstants::parse_rights_constants).
/// * `context_data` - Data from context list used in baking and endorsing rights generation filled in [RightsContextData](RightsContextData::prepare_context_data_for_rights).
/// * `level` - Level to feed Tezos PRNG.
#[inline]
fn get_endorsers_slots(
    constants: &RightsConstants,
    context_data: &RightsContextData,
    level: i64,
) -> Result<HashMap<String, EndorserSlots>, anyhow::Error> {
    // special byte string used in Tezos PRNG
    const ENDORSEMENT_USE_STRING: &[u8] = b"level endorsement:";
    // prepare helper variable
    let mut endorsers_slots: HashMap<String, EndorserSlots> = HashMap::new();

    for endorser_slot in (0..*constants.endorsers_per_block() as u8).rev() {
        // generate PRNG per endorsement slot and take delegates by roll number from context_rolls
        // if roll number is not found then reroll with new state till roll nuber is found in context_rolls
        let mut state = init_prng(
            &context_data,
            &constants,
            ENDORSEMENT_USE_STRING,
            level.try_into()?,
            endorser_slot.try_into()?,
        )?;
        loop {
            let (random_num, sequence) = get_prng_number(state, *context_data.last_roll())?;

            if let Some(delegate) = context_data.rolls().get(&random_num) {
                // collect all slots for each delegate
                let endorsers_slots_entry = endorsers_slots
                    .entry(delegate.clone())
                    .or_insert_with(|| EndorserSlots::new(delegate.clone(), Vec::new()));
                endorsers_slots_entry.push_to_slot(endorser_slot as u16);
                break;
            } else {
                state = sequence;
            }
        }
    }
    Ok(endorsers_slots)
}
