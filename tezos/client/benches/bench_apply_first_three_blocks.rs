// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]
extern crate test;

use std::time::Instant;
use test::Bencher;

use crypto::hash::ChainId;
use tezos_api::environment::{
    get_empty_operation_list_list_hash, TezosEnvironmentConfiguration, TEZOS_ENV,
};
use tezos_api::ffi::{
    ApplyBlockRequest, InitProtocolContextResult, TezosContextConfiguration,
    TezosContextIrminStorageConfiguration, TezosContextStorageConfiguration,
    TezosContextTezEdgeStorageConfiguration, TezosRuntimeConfiguration,
};
use tezos_client::client;
use tezos_interop::ffi;
use tezos_messages::p2p::binary_message::BinaryRead;
use tezos_messages::p2p::encoding::prelude::BlockHeader;

// not a real bench, just for approximatelly measurement of applying first three blocks
// because this is very hard to use with b.iter
// cargo bench -- --nocapture
#[bench]
fn bench_apply_first_three_block(_: &mut Bencher) {
    ffi::change_runtime_configuration(TezosRuntimeConfiguration {
        log_enabled: common::is_ocaml_log_enabled(),
        debug_mode: false,
        compute_context_action_tree_hashes: false,
    })
    .unwrap();

    let now = Instant::now();

    // init empty storage for test (not measuring)
    let storage_clocks = Instant::now();
    // init empty context for test
    let (chain_id, genesis_block_header, ..) = init_test_protocol_context(&format!(
        "bootstrap_test_storage_bench_{}",
        now.elapsed().as_nanos()
    ));
    let storage_clocks = storage_clocks.elapsed();

    // apply
    let apply_clocks = Instant::now();
    let result: Result<Vec<String>, anyhow::Error> =
        apply_first_three_blocks(chain_id, genesis_block_header);
    let apply_clocks = apply_clocks.elapsed();
    assert!(result.is_ok());

    println!(
        "\nApply first three blocks done in {:?} (storage init in {:?}) \n{}!",
        apply_clocks,
        storage_clocks,
        result.ok().unwrap().join("\n")
    );
}

fn apply_first_three_blocks(
    chain_id: ChainId,
    genesis_block_header: BlockHeader,
) -> Result<Vec<String>, anyhow::Error> {
    let mut perf_log = vec![];

    // apply first block - level 1
    let clocks = Instant::now();
    let apply_block_result = client::apply_block(ApplyBlockRequest {
        chain_id: chain_id.clone(),
        block_header: BlockHeader::from_bytes(
            hex::decode(test_data::BLOCK_HEADER_LEVEL_1).unwrap(),
        )?,
        pred_header: genesis_block_header,
        operations: ApplyBlockRequest::convert_operations(test_data::block_operations_from_hex(
            test_data::BLOCK_HEADER_HASH_LEVEL_1,
            test_data::block_header_level1_operations(),
        )),
        max_operations_ttl: 0,
        predecessor_block_metadata_hash: None,
        predecessor_ops_metadata_hash: None,
    })?;
    perf_log.push(format!("- 1. apply: {:?}", clocks.elapsed()));
    assert_eq!(
        test_data::context_hash(test_data::BLOCK_HEADER_LEVEL_1_CONTEXT_HASH),
        apply_block_result.context_hash
    );

    // apply second block - level 2
    let clocks = Instant::now();
    let apply_block_result = client::apply_block(ApplyBlockRequest {
        chain_id: chain_id.clone(),
        block_header: BlockHeader::from_bytes(
            hex::decode(test_data::BLOCK_HEADER_LEVEL_2).unwrap(),
        )?,
        pred_header: BlockHeader::from_bytes(
            hex::decode(test_data::BLOCK_HEADER_LEVEL_1).unwrap(),
        )?,
        operations: ApplyBlockRequest::convert_operations(test_data::block_operations_from_hex(
            test_data::BLOCK_HEADER_HASH_LEVEL_2,
            test_data::block_header_level2_operations(),
        )),
        max_operations_ttl: apply_block_result.max_operations_ttl,
        predecessor_block_metadata_hash: None,
        predecessor_ops_metadata_hash: None,
    })?;
    perf_log.push(format!("- 2. apply: {:?}", clocks.elapsed()));
    assert_eq!(
        "lvl 2, fit 2, prio 5, 0 ops",
        apply_block_result.validation_result_message
    );

    // apply third block - level 3
    let clocks = Instant::now();
    let apply_block_result = client::apply_block(ApplyBlockRequest {
        chain_id,
        block_header: BlockHeader::from_bytes(
            hex::decode(test_data::BLOCK_HEADER_LEVEL_3).unwrap(),
        )?,
        pred_header: BlockHeader::from_bytes(
            hex::decode(test_data::BLOCK_HEADER_LEVEL_2).unwrap(),
        )?,
        operations: ApplyBlockRequest::convert_operations(test_data::block_operations_from_hex(
            test_data::BLOCK_HEADER_HASH_LEVEL_3,
            test_data::block_header_level3_operations(),
        )),
        max_operations_ttl: apply_block_result.max_operations_ttl,
        predecessor_block_metadata_hash: None,
        predecessor_ops_metadata_hash: None,
    })?;
    perf_log.push(format!("- 3. apply: {:?}", clocks.elapsed()));
    assert_eq!(
        "lvl 3, fit 5, prio 12, 1 ops",
        apply_block_result.validation_result_message
    );

    Ok(perf_log)
}

fn init_test_protocol_context(dir_name: &str) -> (ChainId, BlockHeader, InitProtocolContextResult) {
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
        result,
    )
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
    pub const BLOCK_HEADER_LEVEL_1: &str =
        include_str!("../tests/resources/block_header_level1.bytes");
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
    pub const BLOCK_HEADER_HASH_LEVEL_3: &str =
        "61e687e852460b28f0f9540ccecf8f6cf87a5ad472c814612f0179caf4b9f673";
    pub const BLOCK_HEADER_LEVEL_3: &str = "0000000301a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000005c017ed604dfcb6b41e91650bb908618b2740a6167d9072c3230e388b24feeef04c98dc27f000000110000000100000000080000000000000005f06879947f3d9959090f27054062ed23dbf9f7bd4b3c8a6e86008daabb07913e000c00000003e5445371002b9745d767d7f164a39e7f373a0f25166794cba491010ab92b0e281b570057efc78120758ff26a33301870f361d780594911549bcb7debbacd8a142e0b76a605";

    pub fn block_header_level3_operations() -> Vec<Vec<String>> {
        vec![
            vec!["a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000000236663bacdca76094fdb73150092659d463fec94eda44ba4db10973a1ad057ef53a5b3239a1b9c383af803fc275465bd28057d68f3cab46adfd5b2452e863ff0a".to_string()],
            vec![],
            vec![],
            vec![]
        ]
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

mod common {
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};

    pub fn prepare_empty_dir(dir_name: &str) -> String {
        let path = test_storage_dir_path(dir_name);
        if path.exists() {
            fs::remove_dir_all(&path)
                .unwrap_or_else(|_| panic!("Failed to delete directory: {:?}", &path));
        }
        fs::create_dir_all(&path)
            .unwrap_or_else(|_| panic!("Failed to create directory: {:?}", &path));
        String::from(path.to_str().unwrap())
    }

    pub fn test_storage_dir_path(dir_name: &str) -> PathBuf {
        let out_dir = env::var("OUT_DIR").expect("OUT_DIR is not defined");
        Path::new(out_dir.as_str()).join(Path::new(dir_name))
    }

    pub fn is_ocaml_log_enabled() -> bool {
        env::var("OCAML_LOG_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .unwrap()
    }
}
