// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::path::{Path, PathBuf};
use std::{
    convert::{TryFrom, TryInto},
    env,
};

use anyhow::Error;
use slog::{Drain, Level, Logger};

use crypto::hash::{chain_id_from_block_hash, BlockHash, ContextHash, ProtocolHash};
use storage::chain_meta_storage::ChainMetaStorageReader;
use storage::tests_common::TmpStorage;
use storage::*;
use tezos_api::environment::TezosEnvironmentConfiguration;
use tezos_api::ffi::{
    ApplyBlockResponse, CommitGenesisResult, ContextKvStoreConfiguration, GenesisChain,
    ProtocolOverrides, TezosContextIrminStorageConfiguration, TezosContextStorageConfiguration,
    TezosContextTezEdgeStorageConfiguration,
};
use tezos_messages::p2p::binary_message::BinaryRead;
use tezos_messages::p2p::encoding::prelude::*;
use tezos_messages::Head;

#[test]
fn test_storage() -> Result<(), Error> {
    // logger
    let log = create_logger();

    // storage
    let context_dir = "__storage_for_shell".to_string();
    let tmp_storage_dir = test_storage_dir_path("__storage_for_shell");
    let tmp_storage = TmpStorage::create(tmp_storage_dir.clone())?;
    let block_storage = BlockStorage::new(tmp_storage.storage());
    let block_meta_storage = BlockMetaStorage::new(tmp_storage.storage());
    let chain_meta_storage = ChainMetaStorage::new(tmp_storage.storage());
    let operations_meta_storage = OperationsMetaStorage::new(tmp_storage.storage());

    // tezos env - sample
    let tezos_env = TezosEnvironmentConfiguration {
        genesis: GenesisChain {
            time: "2019-08-06T15:18:56Z".to_string(),
            block: "BLockGenesisGenesisGenesisGenesisGenesiscde8db4cX94".to_string(),
            protocol: "PtBMwNZT94N7gXKw4i273CKcSaBrrBnqnt3RATExNKr9KNX2USV".to_string(),
        },
        bootstrap_lookup_addresses: vec![
            "bootstrap.zeronet.fun".to_string(),
            "bootzero.tzbeta.net".to_string(),
        ],
        version: "TEZOS_ZERONET_2019-08-06T15:18:56Z".to_string(),
        protocol_overrides: ProtocolOverrides {
            user_activated_upgrades: vec![],
            user_activated_protocol_overrides: vec![],
        },
        enable_testchain: true,
        patch_context_genesis_parameters: None,
    };
    let context_storage_configuration = TezosContextStorageConfiguration::Both(
        TezosContextIrminStorageConfiguration {
            data_dir: context_dir,
        },
        TezosContextTezEdgeStorageConfiguration {
            backend: ContextKvStoreConfiguration::InMemGC,
            ipc_socket_path: None,
        },
    );

    // initialize empty storage
    let init_data = resolve_storage_init_chain_data(
        &tezos_env,
        &tmp_storage_dir,
        &context_storage_configuration,
        &None,
        &None,
        &None,
        &log,
    );
    assert!(init_data.is_ok());

    let init_data = init_data.unwrap();
    assert_eq!(
        init_data.genesis_block_header_hash,
        BlockHash::try_from(tezos_env.genesis.block.as_str())?
    );
    assert_eq!(
        init_data.chain_id,
        chain_id_from_block_hash(&BlockHash::try_from(tezos_env.genesis.block.as_str())?)?
    );

    // load current head (non)
    let current_head = chain_meta_storage.get_current_head(&init_data.chain_id);
    assert!(current_head.is_ok());
    assert!(current_head.unwrap().is_none());

    // genesis is aleady stored with context hash zero
    let genesis = block_storage.get(&init_data.genesis_block_header_hash)?;
    assert!(genesis.is_none());

    // simulate commit genesis in two steps
    let new_context_hash = "CoV16kW8WgL51SpcftQKdeqc94D6ekghMgPMmEn7TSZzFA697PeE".try_into()?;
    let _ = initialize_storage_with_genesis_block(
        &block_storage,
        &block_meta_storage,
        &init_data,
        &tezos_env,
        &new_context_hash,
        &log,
    )?;

    let commit_genesis_result = CommitGenesisResult {
        block_header_proto_json: "{block_header_proto_json}".to_string(),
        block_header_proto_metadata_bytes: Vec::new(),
        operations_proto_metadata_bytes: Vec::new(),
    };
    let _ = store_commit_genesis_result(
        &block_storage,
        &block_meta_storage,
        &chain_meta_storage,
        &operations_meta_storage,
        &init_data,
        commit_genesis_result.clone(),
    )?;

    // check genesis is on genesis
    assert_eq!(
        chain_meta_storage
            .get_genesis(&init_data.chain_id)?
            .expect("Genesis should be set")
            .block_hash(),
        &init_data.genesis_block_header_hash
    );

    // check caboose is on genesis
    assert_eq!(
        chain_meta_storage
            .get_caboose(&init_data.chain_id)?
            .expect("Caboose should be set")
            .block_hash(),
        &init_data.genesis_block_header_hash
    );

    // check current head is on genesis
    assert_eq!(
        chain_meta_storage
            .get_current_head(&init_data.chain_id)?
            .expect("Current header should be set")
            .block_hash(),
        &init_data.genesis_block_header_hash
    );

    // genesis is stored with replaced context hash
    let genesis = block_storage
        .get(&init_data.genesis_block_header_hash)?
        .expect("Genesis was not stored!");
    assert_eq!(new_context_hash, genesis.header.context().clone());

    // genesis is stored with replaced context hash
    let genesis = block_storage
        .get_by_context_hash(&new_context_hash)?
        .expect("Genesis was not assigned to context_hash!");
    assert_eq!(new_context_hash, genesis.header.context().clone());

    // check genesis jsons
    let (_, data) = block_storage
        .get_with_json_data(&init_data.genesis_block_header_hash)?
        .expect("No json data was saved");
    assert_eq!(
        data.block_header_proto_json(),
        &commit_genesis_result.block_header_proto_json
    );
    assert_eq!(
        data.block_header_proto_metadata_bytes(),
        &commit_genesis_result.block_header_proto_metadata_bytes
    );
    assert_eq!(
        data.operations_proto_metadata_bytes(),
        &commit_genesis_result.operations_proto_metadata_bytes
    );

    // simulate apply block
    let block = make_test_block_header()?;
    block_storage.put_block_header(&block)?;
    block_meta_storage.put_block_header(&block, &init_data.chain_id, &log)?;
    let mut metadata = block_meta_storage
        .get(&block.hash)?
        .expect("No metadata was saved");
    assert!(!metadata.is_applied());

    // save apply result
    let apply_result = ApplyBlockResponse {
        last_allowed_fork_level: 5,
        max_operations_ttl: 6,
        context_hash: ContextHash::from_base58_check(
            "CoVmAcMV64uAQo8XvfLr9VDuz7HVZLT4cgK1w1qYmTjQNbGwQwDd",
        )?,
        protocol_hash: ProtocolHash::try_from(
            "PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb",
        )
        .expect("failed to convert"),
        next_protocol_hash: ProtocolHash::try_from(
            "PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb",
        )
        .expect("failed to convert"),
        block_header_proto_json: "{block_header_proto_json}".to_string(),
        block_header_proto_metadata_bytes: "{block_header_proto_metadata_json}".to_string().into(),
        operations_proto_metadata_bytes: vec![vec!["{operations_proto_metadata_json}"
            .to_string()
            .into()]],
        validation_result_message: "applied".to_string(),
        forking_testchain: false,
        forking_testchain_data: None,
        block_metadata_hash: None,
        ops_metadata_hashes: None,
        ops_metadata_hash: None,
    };
    let block_additional_data = store_applied_block_result(
        &block_storage,
        &block_meta_storage,
        &block.hash,
        apply_result.clone(),
        &mut metadata,
    )?;

    // set block as current head
    chain_meta_storage.set_current_head(
        &init_data.chain_id,
        Head::new(
            block.hash.clone(),
            block.header.level(),
            block.header.fitness().to_vec(),
        ),
    )?;

    // check if data stored
    assert!(metadata.is_applied());
    let metadata = block_meta_storage
        .get(&block.hash)?
        .expect("No metadata was found");
    assert!(metadata.is_applied());

    // check additional
    let data = block_meta_storage
        .get_additional_data(&block.hash)?
        .expect("No additional data was saved");
    assert_eq!(
        data.max_operations_ttl(),
        apply_result.max_operations_ttl as u16
    );
    assert_eq!(
        data.last_allowed_fork_level(),
        apply_result.last_allowed_fork_level
    );
    assert_eq!(
        block_additional_data.max_operations_ttl(),
        apply_result.max_operations_ttl as u16
    );
    assert_eq!(
        block_additional_data.last_allowed_fork_level(),
        apply_result.last_allowed_fork_level
    );

    // check json
    let (_, data) = block_storage
        .get_with_json_data(&block.hash)?
        .expect("No json data was saved");
    assert_eq!(
        data.block_header_proto_json(),
        &apply_result.block_header_proto_json
    );
    assert_eq!(
        data.block_header_proto_metadata_bytes(),
        &apply_result.block_header_proto_metadata_bytes
    );
    assert_eq!(
        data.operations_proto_metadata_bytes(),
        &apply_result.operations_proto_metadata_bytes
    );

    // load current head - should be changed
    assert_eq!(
        chain_meta_storage
            .get_current_head(&init_data.chain_id)?
            .expect("Current header should be set")
            .block_hash(),
        &block.hash
    );

    Ok(())
}

fn make_test_block_header() -> Result<BlockHeaderWithHash, Error> {
    let message_bytes = hex::decode("00006d6e0102dd00defaf70c53e180ea148b349a6feb4795610b2abc7b07fe91ce50a90814000000005c1276780432bc1d3a28df9a67b363aa1638f807214bb8987e5f9c0abcbd69531facffd1c80000001100000001000000000800000000000c15ef15a6f54021cb353780e2847fb9c546f1d72c1dc17c3db510f45553ce501ce1de000000000003c762c7df00a856b8bfcaf0676f069f825ca75f37f2bee9fe55ba109cec3d1d041d8c03519626c0c0faa557e778cb09d2e0c729e8556ed6a7a518c84982d1f2682bc6aa753f")?;
    let block_header = BlockHeaderWithHash::new(BlockHeader::from_bytes(message_bytes)?)?;
    Ok(block_header)
}

pub fn test_storage_dir_path(dir_name: &str) -> PathBuf {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR is not defined");
    Path::new(out_dir.as_str()).join(Path::new(dir_name))
}

fn create_logger() -> Logger {
    let drain = slog_async::Async::new(
        slog_term::FullFormat::new(slog_term::TermDecorator::new().build())
            .build()
            .fuse(),
    )
    .build()
    .filter_level(Level::Info)
    .fuse();

    Logger::root(drain, slog::o!())
}
