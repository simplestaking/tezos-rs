// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use serial_test::serial;

use crypto::hash::{ChainId, ProtocolHash};
use tezos_api::environment::{
    get_empty_operation_list_list_hash, TezosEnvironmentConfiguration, TEZOS_ENV,
};
use tezos_api::ffi::{
    ApplyBlockRequest, BeginConstructionRequest, InitProtocolContextResult,
    TezosContextConfiguration, TezosContextIrminStorageConfiguration,
    TezosContextStorageConfiguration, TezosContextTezEdgeStorageConfiguration,
    TezosRuntimeConfiguration, ValidateOperationRequest,
};
use tezos_client::client;
use tezos_messages::p2p::binary_message::BinaryRead;
use tezos_messages::p2p::encoding::prelude::*;

mod common;

fn init_test_runtime() {
    // init runtime and turn on/off ocaml logging
    client::change_runtime_configuration(TezosRuntimeConfiguration {
        log_enabled: common::is_ocaml_log_enabled(),
        debug_mode: false,
        compute_context_action_tree_hashes: false,
    })
    .unwrap();
}

fn init_test_protocol_context(
    dir_name: &str,
) -> (
    ChainId,
    BlockHeader,
    ProtocolHash,
    InitProtocolContextResult,
) {
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&test_data::TEZOS_NETWORK)
        .expect("no tezos environment configured");

    let data_dir = common::prepare_empty_dir(dir_name);
    let storage = TezosContextStorageConfiguration::Both(
        TezosContextIrminStorageConfiguration { data_dir },
        TezosContextTezEdgeStorageConfiguration {
            backend: tezos_api::ffi::ContextKvStoreConfiguration::InMemGC,
            ipc_socket_path: None,
        },
    );
    let context_config = TezosContextConfiguration {
        storage,
        genesis: tezos_env.genesis.clone(),
        protocol_overrides: tezos_env.protocol_overrides.clone(),
        commit_genesis: true,
        enable_testchain: false,
        readonly: false,
        sandbox_json_patch_context: None,
        context_stats_db_path: None,
    };

    let result = client::init_protocol_context(context_config).unwrap();

    let genesis_commit_hash = match result.genesis_commit_hash.as_ref() {
        None => panic!("we needed commit_genesis and here should be result of it"),
        Some(cr) => cr.clone(),
    };

    (
        tezos_env.main_chain_id().expect("invalid chain id"),
        tezos_env
            .genesis_header(
                genesis_commit_hash,
                get_empty_operation_list_list_hash().unwrap(),
            )
            .expect("genesis header error"),
        tezos_env.genesis_protocol().expect("protocol_hash error"),
        result,
    )
}

#[test]
#[serial]
fn test_begin_construction_and_validate_operation() -> Result<(), anyhow::Error> {
    init_test_runtime();

    // init empty context for test
    let (chain_id, genesis_block_header, ..) =
        init_test_protocol_context("mempool_test_storage_01");

    // apply block 1 and block 2
    let last_block = apply_blocks_1_2(&chain_id, genesis_block_header);

    // let's initialize prevalidator for current head
    let prevalidator = client::begin_construction(BeginConstructionRequest {
        chain_id: chain_id.clone(),
        predecessor: last_block,
        protocol_data: None,
    })?;
    assert_eq!(prevalidator.chain_id, chain_id);
    assert_eq!(
        prevalidator.context_fitness,
        Some(vec![vec![0], vec![0, 0, 0, 0, 0, 0, 0, 3]])
    );

    let operation = test_data::operation_from_hex(test_data::OPERATION_LEVEL_3);

    let result = client::validate_operation(ValidateOperationRequest {
        prevalidator,
        operation,
    })?;
    assert_eq!(result.prevalidator.chain_id, chain_id);
    assert_eq!(result.result.applied.len(), 1);
    assert_eq!(
        result.prevalidator.context_fitness,
        Some(vec![vec![0], vec![0, 0, 0, 0, 0, 0, 0, 5]])
    );

    Ok(())
}

fn apply_blocks_1_2(chain_id: &ChainId, genesis_block_header: BlockHeader) -> BlockHeader {
    // apply first block - level 1
    let apply_block_result = client::apply_block(ApplyBlockRequest {
        chain_id: chain_id.clone(),
        block_header: BlockHeader::from_bytes(
            hex::decode(test_data::BLOCK_HEADER_LEVEL_1).unwrap(),
        )
        .unwrap(),
        pred_header: genesis_block_header,
        operations: ApplyBlockRequest::convert_operations(test_data::block_operations_from_hex(
            test_data::BLOCK_HEADER_HASH_LEVEL_1,
            test_data::block_header_level1_operations(),
        )),
        max_operations_ttl: 0,
        predecessor_block_metadata_hash: None,
        predecessor_ops_metadata_hash: None,
    })
    .unwrap();
    assert_eq!(
        test_data::context_hash(test_data::BLOCK_HEADER_LEVEL_1_CONTEXT_HASH),
        apply_block_result.context_hash
    );
    assert_eq!(1, apply_block_result.max_operations_ttl);

    // apply second block - level 2
    let apply_block_result = client::apply_block(ApplyBlockRequest {
        chain_id: chain_id.clone(),
        block_header: BlockHeader::from_bytes(
            hex::decode(test_data::BLOCK_HEADER_LEVEL_2).unwrap(),
        )
        .unwrap(),
        pred_header: BlockHeader::from_bytes(hex::decode(test_data::BLOCK_HEADER_LEVEL_1).unwrap())
            .unwrap(),
        operations: ApplyBlockRequest::convert_operations(test_data::block_operations_from_hex(
            test_data::BLOCK_HEADER_HASH_LEVEL_2,
            test_data::block_header_level2_operations(),
        )),
        max_operations_ttl: apply_block_result.max_operations_ttl,
        predecessor_block_metadata_hash: None,
        predecessor_ops_metadata_hash: None,
    })
    .unwrap();
    assert_eq!(
        "lvl 2, fit 2, prio 5, 0 ops",
        &apply_block_result.validation_result_message
    );
    assert_eq!(2, apply_block_result.max_operations_ttl);

    BlockHeader::from_bytes(hex::decode(test_data::BLOCK_HEADER_LEVEL_2).unwrap()).unwrap()
}

mod test_data {
    use std::convert::TryInto;

    use crypto::hash::ContextHash;
    use tezos_api::environment::TezosEnvironment;
    use tezos_messages::p2p::binary_message::BinaryRead;
    use tezos_messages::p2p::encoding::prelude::*;

    pub const TEZOS_NETWORK: TezosEnvironment = TezosEnvironment::Alphanet;

    pub fn context_hash(hash: &str) -> ContextHash {
        ContextHash::from_base58_check(hash).unwrap()
    }

    // BMPtRJqFGQJRTfn8bXQR2grLE1M97XnUmG5vgjHMW7St1Wub7Cd
    pub const BLOCK_HEADER_HASH_LEVEL_1: &str =
        "dd9fb5edc4f29e7d28f41fe56d57ad172b7686ed140ad50294488b68de29474d";
    pub const BLOCK_HEADER_LEVEL_1: &str = include_str!("resources/block_header_level1.bytes");
    pub const BLOCK_HEADER_LEVEL_1_CONTEXT_HASH: &str =
        "CoV16kW8WgL51SpcftQKdeqc94D6ekghMgPMmEn7TSZzFA697PeE";

    pub fn block_header_level1_operations() -> Vec<Vec<String>> {
        vec![]
    }

    // BLwKksYwrxt39exDei7yi47h7aMcVY2kZMZhTwEEoSUwToQUiDV
    pub const BLOCK_HEADER_HASH_LEVEL_2: &str =
        "a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627";
    pub const BLOCK_HEADER_LEVEL_2: &str = "0000000201dd9fb5edc4f29e7d28f41fe56d57ad172b7686ed140ad50294488b68de29474d000000005c017cd804683625c2445a4e9564bf710c5528fd99a7d150d2a2a323bc22ff9e2710da4f6d0000001100000001000000000800000000000000029bd8c75dec93c276d2d8e8febc3aa6c9471cb2cb42236b3ab4ca5f1f2a0892f6000500000003ba671eef00d6a8bea20a4677fae51268ab6be7bd8cfc373cd6ac9e0a00064efcc404e1fb39409c5df255f7651e3d1bb5d91cb2172b687e5d56ebde58cfd92e1855aaafbf05";

    pub fn block_header_level2_operations() -> Vec<Vec<String>> {
        vec![vec![], vec![], vec![], vec![]]
    }

    // BLTQ5B4T4Tyzqfm3Yfwi26WmdQScr6UXVSE9du6N71LYjgSwbtc
    pub const OPERATION_LEVEL_3: &str = "a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000000236663bacdca76094fdb73150092659d463fec94eda44ba4db10973a1ad057ef53a5b3239a1b9c383af803fc275465bd28057d68f3cab46adfd5b2452e863ff0a";

    pub fn operation_from_hex(bytes: &str) -> Operation {
        Operation::from_bytes(hex::decode(bytes).unwrap()).unwrap()
    }

    pub fn block_operations_from_hex(
        block_hash: &str,
        hex_operations: Vec<Vec<String>>,
    ) -> Vec<OperationsForBlocksMessage> {
        hex_operations
            .into_iter()
            .map(|bo| {
                let ops = bo
                    .into_iter()
                    .map(|op| Operation::from_bytes(hex::decode(op).unwrap()).unwrap())
                    .collect();
                OperationsForBlocksMessage::new(
                    OperationsForBlock::new(
                        hex::decode(block_hash).unwrap().try_into().unwrap(),
                        4,
                    ),
                    Path::op(),
                    ops,
                )
            })
            .collect()
    }
}
