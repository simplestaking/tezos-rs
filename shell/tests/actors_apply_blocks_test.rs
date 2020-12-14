// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]
extern crate test;

/// Big integration test for actors, covers two main use cases:
/// 1. test_scenario_for_apply_blocks_with_chain_feeder_and_check_context - see fn description
/// 2. test_scenario_for_add_operations_to_mempool_and_check_state - see fn description

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use riker::actors::*;
use slog::{info, Logger};

use crypto::hash::{BlockHash, ContextHash, HashType, OperationHash};
use shell::shell_channel::{MempoolOperationReceived, ShellChannelRef, ShellChannelTopic};
use storage::{BlockHeaderWithHash, BlockMetaStorage, BlockStorage, BlockStorageReader, ChainMetaStorage, context_key, MempoolStorage, OperationsMetaStorage, OperationsStorage};
use storage::chain_meta_storage::ChainMetaStorageReader;
use storage::context::{ContextApi, TezedgeContext};
use storage::mempool_storage::MempoolOperationType;
use storage::persistent::PersistentStorage;
use storage::tests_common::TmpStorage;
use tezos_api::environment::TezosEnvironmentConfiguration;
use tezos_messages::p2p::binary_message::MessageHash;
use tezos_messages::p2p::encoding::operations_for_blocks::OperationsForBlocksMessage;

use crate::samples::OperationsForBlocksMessageKey;

mod common;
mod samples;

#[ignore]
#[test]
fn test_actors_apply_blocks_and_check_context_and_mempool() -> Result<(), failure::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    // prepare data - we have stored 1326 request, apply just 1324, and 1325,1326 will be used for mempool test
    let (requests, operations, tezos_env) = samples::read_data_apply_block_request_until_1326();

    // start node
    let node = common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_actors_apply_blocks_and_check_context_and_mempool"))?,
        &common::prepare_empty_dir("__test_actors_apply_blocks_and_check_context_and_mempool_context"),
        "test_actors_apply_blocks_and_check_context_and_mempool",
        &tezos_env,
        None,
        None,
        tezos_identity::Identity::generate(0f64),
        (log, log_level),
    )?;

    let clocks = Instant::now();

    // 1. test - apply and context - prepare data for apply blocks and wait for current head, and check context
    assert!(
        test_scenario_for_apply_blocks_with_chain_feeder_and_check_context(
            &node.tmp_storage.storage(),
            &node.tezos_env,
            node.log.clone(),
            &requests,
            &operations,
            1324,
        ).is_ok()
    );

    // 2. test - mempool test
    assert!(
        test_scenario_for_add_operations_to_mempool_and_check_state(
            &node,
            &requests[1323],
            &requests[1324],
            &requests[1325],
        ).is_ok()
    );

    println!("\nDone in {:?}!", clocks.elapsed());

    drop(node);

    Ok(())
}

fn check_context(expected_context_hash: ContextHash, persistent_storage: &PersistentStorage) -> Result<(), failure::Error> {
    let context = TezedgeContext::new(
        BlockStorage::new(&persistent_storage),
        persistent_storage.merkle(),
    );

    // check protocol
    if let Some(data) = context.get_key_from_history(&expected_context_hash, &context_key!("protocol"))? {
        assert_eq!("PsBabyM1eUXZseaJdmXFApDSBqj8YBfwELoxZHHW77EMcAbbwAS", HashType::ProtocolHash.hash_to_b58check(&data));
    } else {
        panic!(format!("Protocol not found in context for level: {}", 2));
    }

    // check level 1324 with merkle storage
    let m = persistent_storage.merkle();
    let merkle = m.write().unwrap();
    // get final hash from last commit in merkle storage
    let merkle_last_hash = merkle.get_last_commit_hash();

    // compare with context hash of last applied expected_context_hash
    assert_eq!(*expected_context_hash, merkle_last_hash.unwrap());
    let stats = merkle.get_merkle_stats().unwrap();
    println!("Avg set exec time in ns: {}", stats.perf_stats.avg_set_exec_time_ns);

    Ok(())
}

/// Test scenario applies all requests to the apply_to_level,
/// then waits for context_listener to commit context,
/// and then validates stored context to dedicated context exported from ocaml on the same level
fn test_scenario_for_apply_blocks_with_chain_feeder_and_check_context(
    persistent_storage: &PersistentStorage,
    tezos_env: &TezosEnvironmentConfiguration,
    log: Logger,
    requests: &Vec<String>,
    operations: &HashMap<OperationsForBlocksMessageKey, OperationsForBlocksMessage>,
    apply_to_level: i32) -> Result<(), failure::Error> {
    // prepare dbs
    let block_storage = BlockStorage::new(&persistent_storage);
    let block_meta_storage = BlockMetaStorage::new(&persistent_storage);
    let chain_meta_storage = ChainMetaStorage::new(&persistent_storage);
    let operations_storage = OperationsStorage::new(&persistent_storage);
    let operations_meta_storage = OperationsMetaStorage::new(&persistent_storage);

    let chain_id = tezos_env.main_chain_id().expect("invalid chain id");

    let clocks = Instant::now();

    // let's insert stored requests to database
    for request in requests {

        // parse request
        let request = samples::from_captured_bytes(request)?;
        let header = request.block_header.clone();

        // store header to db
        let block = BlockHeaderWithHash {
            hash: header.message_hash()?,
            header: Arc::new(header),
        };
        block_storage.put_block_header(&block)?;
        block_meta_storage.put_block_header(&block, &chain_id, &log)?;
        operations_meta_storage.put_block_header(&block, &chain_id)?;

        // store operations to db
        let validation_pass: u8 = block.header.validation_pass();
        for vp in 0..validation_pass {
            if let Some(msg) = operations.get(&OperationsForBlocksMessageKey::new(block.hash.clone(), vp as i8)) {
                operations_storage.put_operations(msg)?;
                let _ = operations_meta_storage.put_operations(msg)?;
            }
        }
        assert!(operations_meta_storage.is_complete(&block.hash)?);

        if block.header.level() >= apply_to_level {
            break;
        }
    }

    let clocks = clocks.elapsed();
    println!("\n[Insert] done in {:?}!", clocks);

    // wait context_listener to finished context for applied blocks
    info!(log, "Waiting for context processing"; "level" => apply_to_level);
    let current_head_context_hash: Option<ContextHash> = loop {
        match chain_meta_storage.get_current_head(&chain_id)? {
            None => continue,
            Some(head) => {
                if head.level() >= &apply_to_level {
                    let header = block_storage.get(head.block_hash()).expect("failed to read current head").expect("current head not found");
                    // TE-168: check if context is also asynchronously stored
                    let context_hash = header.header.context();
                    let found_by_context_hash = block_storage.get_by_context_hash(&context_hash).expect("failed to read head");
                    if found_by_context_hash.is_some() {
                        break Some(context_hash.clone());
                    }
                }
            }
        }
    };
    info!(log, "Context done and successfully applied to level"; "level" => apply_to_level);

    // check context
    check_context(
        current_head_context_hash.unwrap_or_else(|| panic!("Context hash not set for apply_to_level: {}", apply_to_level)),
        &persistent_storage,
    )
}

/// Starts on mempool current state for last applied block, which is supossed to be 1324.
/// Mempool for 1324 is checked, and then operations from 1325 are stored to mempool for validation,
/// Test waits for all 6 result and than checks current mempool state, if contains `applied` 6 operations.
///
/// Than tries to validate operations from block 1326, which are `branch_delayed`.
fn test_scenario_for_add_operations_to_mempool_and_check_state(
    node: &common::infra::NodeInfrastructure,
    last_applied_request_1324: &String,
    request_1325: &str,
    request_1326: &str) -> Result<(), failure::Error> {

    // wait mempool for last_applied_block
    let last_applied_block: BlockHash = samples::from_captured_bytes(last_applied_request_1324)?.block_header.message_hash()?;
    node.wait_for_mempool_on_head("mempool_head_1324", last_applied_block.clone(), (Duration::from_secs(30), Duration::from_millis(250)))?;

    // check current mempool state, should be on last applied block 1324
    {
        let current_mempool_state = node.current_mempool_state_storage.read().expect("Failed to obtain lock");
        assert!(current_mempool_state.head().is_some());
        assert_eq!(*current_mempool_state.head().unwrap(), last_applied_block);
    }

    // check operations in mempool - should by empty all
    {
        let current_mempool_state = node.current_mempool_state_storage.read().expect("Failed to obtain lock");
        assert!(current_mempool_state.result().applied.is_empty());
        assert!(current_mempool_state.result().branch_delayed.is_empty());
        assert!(current_mempool_state.result().branch_refused.is_empty());
        assert!(current_mempool_state.result().refused.is_empty());
    }

    // add operations from 1325 to mempool - should by applied
    let mut mempool_storage = MempoolStorage::new(node.tmp_storage.storage());
    let operations_from_1325 = add_operations_to_mempool(request_1325, node.shell_channel.clone(), &mut mempool_storage)?;
    let operations_from_1325_count = operations_from_1325.len();
    assert_ne!(0, operations_from_1325_count);

    // we expect here message for every operation
    node.wait_for_mempool_contains_operations("mempool_operations_from_1325", &operations_from_1325, (Duration::from_secs(10), Duration::from_millis(250)))?;

    // check mempool current state after operations 1325
    {
        let current_mempool_state = node.current_mempool_state_storage.read().expect("Failed to obtain lock");
        assert_eq!(operations_from_1325_count, current_mempool_state.result().applied.len());
        assert!(current_mempool_state.result().branch_delayed.is_empty());
        assert!(current_mempool_state.result().branch_refused.is_empty());
        assert!(current_mempool_state.result().refused.is_empty());
    }

    // add operations from 1326 to mempool - should by branch_delay
    let operations_from_1326 = add_operations_to_mempool(request_1326, node.shell_channel.clone(), &mut mempool_storage)?;
    let operations_from_1326_count = operations_from_1326.len();
    assert_ne!(0, operations_from_1326_count);

    // we expect here message for every operation
    node.wait_for_mempool_contains_operations("mempool_operations_from_1325", &operations_from_1326, (Duration::from_secs(10), Duration::from_millis(250)))?;

    // check mempool current state after operations 1326
    {
        let current_mempool_state = node.current_mempool_state_storage.read().expect("Failed to obtain lock");
        assert_eq!(operations_from_1325_count, current_mempool_state.result().applied.len());
        assert_eq!(operations_from_1326_count, current_mempool_state.result().branch_delayed.len());
        assert!(current_mempool_state.result().branch_refused.is_empty());
        assert!(current_mempool_state.result().refused.is_empty());
    }

    Ok(())
}

fn add_operations_to_mempool(request: &str, shell_channel: ShellChannelRef, mempool_storage: &mut MempoolStorage) -> Result<HashSet<OperationHash>, failure::Error> {
    let request = samples::from_captured_bytes(request)?;
    let mut operation_hashes = HashSet::new();
    for operations in request.operations {
        for operation in operations {
            // this is done by chain_manager when received new operations

            let operation_hash = operation.message_hash()?;

            // add to mempool storage
            mempool_storage.put(
                MempoolOperationType::Pending,
                operation.into(),
                SystemTime::now(),
            )?;


            // ping channel - mempool_prevalidator listens
            shell_channel.tell(
                Publish {
                    msg: MempoolOperationReceived {
                        operation_hash: operation_hash.clone(),
                        operation_type: MempoolOperationType::Pending,
                    }.into(),
                    topic: ShellChannelTopic::ShellEvents.into(),
                },
                None,
            );

            operation_hashes.insert(operation_hash);
        }
    }

    Ok(operation_hashes)
}
