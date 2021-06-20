// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::convert::TryFrom;

use anyhow::Error;
use assert_json_diff::assert_json_eq;
use serde_json::{json, Value};

use crypto::hash::ProtocolHash;
use tezos_messages::base::rpc_support::{ToRpcJsonMap, UniversalValue};
use tezos_messages::protocol::{
    get_constants_for_rpc, proto_005_2, proto_006, proto_007, proto_008, proto_008_2,
    SupportedProtocol,
};

#[test]
fn can_deserialize_constants_005_2() -> Result<(), Error> {
    // 005 data
    let just_dynamic_constants_bytes = hex::decode("030000080000000020000001000000080000000010000000000000001e0000000000000028002080d46180c8d00700003fffffffffff80a0d9e61d03e8c8d00700000101808092f40180a0c21e80c8d00780897ae807a0a907000000000000a8c000000bb800001b58000001f400180000000000000002")?;
    let expected_dynamic_constants_json = json!({"preserved_cycles":3,"blocks_per_cycle":2048,"blocks_per_commitment":32,"blocks_per_roll_snapshot":256,"blocks_per_voting_period":2048,"time_between_blocks":["30","40"],"endorsers_per_block":32,"hard_gas_limit_per_operation":"800000","hard_gas_limit_per_block":"8000000","proof_of_work_threshold":"70368744177663","tokens_per_roll":"8000000000","michelson_maximum_type_size":1000,"seed_nonce_revelation_tip":"125000","origination_size":257,"block_security_deposit":"512000000","endorsement_security_deposit":"64000000","block_reward":"16000000","endorsement_reward":"2000000","cost_per_byte":"1000","hard_storage_limit_per_operation":"60000","test_chain_duration":"43200","quorum_min":3000,"quorum_max":7000,"min_proposal_quorum":500,"initial_endorsers":24,"delay_per_missing_endorsement":"2"});
    let protocol_hash = ProtocolHash::try_from(proto_005_2::PROTOCOL_HASH)?;
    let expected_fixed_constants = proto_005_2::constants::FIXED.as_map();

    assert_constants_eq(
        ProtocolHash::try_from("PsBabyM1eUXZseaJdmXFApDSBqj8YBfwELoxZHHW77EMcAbbwAS")?,
        expected_dynamic_constants_json,
        expected_fixed_constants,
        protocol_hash,
        &just_dynamic_constants_bytes,
    )
}

#[test]
fn can_deserialize_constants_006() -> Result<(), Error> {
    // 006 data
    let just_dynamic_constants_bytes = hex::decode("030000080000000020000001000000080000000010000000000000001e0000000000000028002080fa7e80c4f50900003fffffffffff80a0d9e61d03e8c8d007000001018088d54480d1ca0800000006d0a54cecb80b00000006d0a54cb5ee32e807a0a907000000000000a8c000000bb800001b58000001f400180000000000000002")?;
    let expected_dynamic_constants_json = json!({"preserved_cycles":3,"blocks_per_cycle":2048,"blocks_per_commitment":32,"blocks_per_roll_snapshot":256,"blocks_per_voting_period":2048,"time_between_blocks":["30","40"],"endorsers_per_block":32,"hard_gas_limit_per_operation":"1040000","hard_gas_limit_per_block":"10400000","proof_of_work_threshold":"70368744177663","tokens_per_roll":"8000000000","michelson_maximum_type_size":1000,"seed_nonce_revelation_tip":"125000","origination_size":257,"block_security_deposit":"144000000","endorsement_security_deposit":"18000000","baking_reward_per_endorsement":["1250000","187500"],"endorsement_reward":["1250000","833333"],"cost_per_byte":"1000","hard_storage_limit_per_operation":"60000","test_chain_duration":"43200","quorum_min":3000,"quorum_max":7000,"min_proposal_quorum":500,"initial_endorsers":24,"delay_per_missing_endorsement":"2"});
    let protocol_hash = ProtocolHash::try_from(proto_006::PROTOCOL_HASH)?;
    let expected_fixed_constants = proto_006::constants::FIXED.as_map();

    assert_constants_eq(
        ProtocolHash::try_from("PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb")?,
        expected_dynamic_constants_json,
        expected_fixed_constants,
        protocol_hash,
        &just_dynamic_constants_bytes,
    )
}

#[test]
fn can_deserialize_constants_007() -> Result<(), Error> {
    // 007 data
    let just_dynamic_constants_bytes = hex::decode("030000080000000010000000800000080000000010000000000000001e0000000000000014002080fa7e80c4f50900003fffffffffff80a0d9e61d03e8c8d00700000101808092f40180a0c21e00000006d0a54cecb80b00000006d0a54cb5ee32fa01a0a907000000000000f000000007d000001b58000001f400180000000000000004")?;
    let expected_dynamic_constants_json = json!({"preserved_cycles":3,"blocks_per_cycle":2048,"blocks_per_commitment":16,"blocks_per_roll_snapshot":128,"blocks_per_voting_period":2048,"time_between_blocks":["30","20"],"endorsers_per_block":32,"hard_gas_limit_per_operation":"1040000","hard_gas_limit_per_block":"10400000","proof_of_work_threshold":"70368744177663","tokens_per_roll":"8000000000","michelson_maximum_type_size":1000,"seed_nonce_revelation_tip":"125000","origination_size":257,"block_security_deposit":"512000000","endorsement_security_deposit":"64000000","baking_reward_per_endorsement":["1250000","187500"],"endorsement_reward":["1250000","833333"],"cost_per_byte":"250","hard_storage_limit_per_operation":"60000","test_chain_duration":"61440","quorum_min":2000,"quorum_max":7000,"min_proposal_quorum":500,"initial_endorsers":24,"delay_per_missing_endorsement":"4"});
    let protocol_hash = ProtocolHash::try_from(proto_007::PROTOCOL_HASH)?;
    let expected_fixed_constants = proto_007::constants::FIXED.as_map();

    assert_constants_eq(
        ProtocolHash::try_from("PsDELPH1Kxsxt8f9eWbxQeRxkjfbxoqM52jvs5Y5fBxWWh4ifpo")?,
        expected_dynamic_constants_json,
        expected_fixed_constants,
        protocol_hash,
        &just_dynamic_constants_bytes,
    )
}

#[test]
fn can_deserialize_constants_008() -> Result<(), Error> {
    // 008 data
    // This, can be done: /chains/main/blocks/2/context/raw/bytes/v1/constants
    let just_dynamic_constants_bytes = hex::decode("030000080000000010000000800000080000000010000000000000001e0000000000000014002080fa7e80c4f50900003fffffffffff80a0d9e61d03e8c8d00700000101808092f40180a0c21e00000006d0a54cecb80b00000006d0a54cb5ee32fa01a0a907000000000000f000000007d000001b58000001f400180000000000000004")?;
    // This, can be done: /chains/main/blocks/2/context/constants + removed fixed
    let expected_dynamic_constants_json = json!({"preserved_cycles":3,"blocks_per_cycle":2048,"blocks_per_commitment":16,"blocks_per_roll_snapshot":128,"blocks_per_voting_period":2048,"time_between_blocks":["30","20"],"endorsers_per_block":32,"hard_gas_limit_per_operation":"1040000","hard_gas_limit_per_block":"10400000","proof_of_work_threshold":"70368744177663","tokens_per_roll":"8000000000","michelson_maximum_type_size":1000,"seed_nonce_revelation_tip":"125000","origination_size":257,"block_security_deposit":"512000000","endorsement_security_deposit":"64000000","baking_reward_per_endorsement":["1250000","187500"],"endorsement_reward":["1250000","833333"],"cost_per_byte":"250","hard_storage_limit_per_operation":"60000","test_chain_duration":"61440","quorum_min":2000,"quorum_max":7000,"min_proposal_quorum":500,"initial_endorsers":24,"delay_per_missing_endorsement":"4"});
    let protocol_hash = ProtocolHash::try_from(proto_008::PROTOCOL_HASH)?;
    let expected_fixed_constants = proto_008::constants::FIXED.as_map();

    assert_constants_eq(
        ProtocolHash::try_from("PtEdoTezd3RHSC31mpxxo1npxFjoWWcFgQtxapi51Z8TLu6v6Uq")?,
        expected_dynamic_constants_json,
        expected_fixed_constants,
        protocol_hash,
        &just_dynamic_constants_bytes,
    )
}

#[test]
fn can_deserialize_constants_008_2() -> Result<(), Error> {
    // 008_2 data
    // This, can be done: /chains/main/blocks/2/context/raw/bytes/v1/constants
    let just_dynamic_constants_bytes = hex::decode("030000080000000010000000800000080000000010000000000000001e0000000000000014002080fa7e80c4f50900003fffffffffff80a0d9e61d03e8c8d00700000101808092f40180a0c21e00000006d0a54cecb80b00000006d0a54cb5ee32fa01a0a907000000000000f000000007d000001b58000001f400180000000000000004")?;
    // This, can be done: /chains/main/blocks/2/context/constants + removed fixed
    let expected_dynamic_constants_json = json!({"preserved_cycles":3,"blocks_per_cycle":2048,"blocks_per_commitment":16,"blocks_per_roll_snapshot":128,"blocks_per_voting_period":2048,"time_between_blocks":["30","20"],"endorsers_per_block":32,"hard_gas_limit_per_operation":"1040000","hard_gas_limit_per_block":"10400000","proof_of_work_threshold":"70368744177663","tokens_per_roll":"8000000000","michelson_maximum_type_size":1000,"seed_nonce_revelation_tip":"125000","origination_size":257,"block_security_deposit":"512000000","endorsement_security_deposit":"64000000","baking_reward_per_endorsement":["1250000","187500"],"endorsement_reward":["1250000","833333"],"cost_per_byte":"250","hard_storage_limit_per_operation":"60000","test_chain_duration":"61440","quorum_min":2000,"quorum_max":7000,"min_proposal_quorum":500,"initial_endorsers":24,"delay_per_missing_endorsement":"4"});
    let protocol_hash = ProtocolHash::try_from(proto_008_2::PROTOCOL_HASH)?;
    let expected_fixed_constants = proto_008_2::constants::FIXED.as_map();

    assert_constants_eq(
        ProtocolHash::try_from("PtEdo2ZkT9oKpimTah6x2embF25oss54njMuPzkJTEi5RqfdZFA")?,
        expected_dynamic_constants_json,
        expected_fixed_constants,
        protocol_hash,
        &just_dynamic_constants_bytes,
    )
}

fn assert_constants_eq(
    expected_protocol_hash: ProtocolHash,
    expected_dynamic_constants_json: Value,
    expected_fixed_constants: HashMap<&str, UniversalValue>,
    protocol_hash: ProtocolHash,
    just_dynamic_constants_bytes: &Vec<u8>,
) -> Result<(), Error> {
    // check protocol hash
    assert_eq!(expected_protocol_hash, protocol_hash);

    // get all constatnts (fixed + dynamic decoded)
    let constants = get_constants_for_rpc(
        &just_dynamic_constants_bytes,
        &SupportedProtocol::try_from(protocol_hash)?,
    )?;
    assert!(constants.is_some());
    let constants = constants.unwrap();

    // split to fixed and dynamic
    let mut just_dynamic_constants = constants.clone();
    just_dynamic_constants.retain(|&key, _| expected_fixed_constants.contains_key(key) == false);

    let mut just_fixed_constants = constants.clone();
    just_fixed_constants.retain(|&key, _| expected_fixed_constants.contains_key(key) == true);

    // compare fixed jsons
    assert_json_eq!(
        serde_json::to_value(expected_fixed_constants)?,
        serde_json::to_value(just_fixed_constants)?
    );
    // compare dynamic jsons
    assert_json_eq!(
        expected_dynamic_constants_json,
        serde_json::to_value(&just_dynamic_constants)?
    );
    Ok(())
}
