// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]
extern crate test;

/// Simple integration test for actors
///
/// (Tests are ignored, because they need protocol-runner binary)
/// Runs like: `PROTOCOL_RUNNER=./target/release/protocol-runner cargo test --release -- --ignored`
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use lazy_static::lazy_static;
use serial_test::serial;

use crypto::hash::OperationHash;
use networking::ShellCompatibilityVersion;
use shell::peer_manager::P2p;
use shell::PeerConnectionThreshold;
use storage::tests_common::TmpStorage;
use storage::{BlockMetaStorage, BlockMetaStorageReader};
use tezos_api::environment::{TezosEnvironmentConfiguration, TEZOS_ENV};
use tezos_identity::Identity;
use tezos_messages::p2p::binary_message::MessageHash;
use tezos_messages::p2p::encoding::current_head::CurrentHeadMessage;
use tezos_messages::p2p::encoding::prelude::Mempool;

pub mod common;

lazy_static! {
    pub static ref SHELL_COMPATIBILITY_VERSION: ShellCompatibilityVersion = ShellCompatibilityVersion::new("TEST_CHAIN".to_string(), vec![0], vec![0]);
    pub static ref NODE_P2P_PORT: u16 = 1234; // TODO: maybe some logic to verify and get free port
    pub static ref NODE_P2P_CFG: (P2p, ShellCompatibilityVersion) = (
        P2p {
            listener_port: *NODE_P2P_PORT,
            bootstrap_lookup_addresses: vec![],
            disable_bootstrap_lookup: true,
            disable_mempool: false,
            private_node: false,
            bootstrap_peers: vec![],
            peer_threshold: PeerConnectionThreshold::try_new(0, 10, Some(0)).expect("Invalid range"),
        },
        SHELL_COMPATIBILITY_VERSION.clone(),
    );
    pub static ref NODE_IDENTITY: Identity = tezos_identity::Identity::generate(0f64).unwrap();
}

#[ignore]
#[test]
#[serial]
fn test_process_current_branch_on_level3_then_current_head_level4() -> Result<(), failure::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = common::test_cases_data::current_branch_on_level_3::init_data(&log);
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&db.tezos_env)
        .expect("no environment configuration");

    // start node
    let node = crate::common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_01"))?,
        &common::prepare_empty_dir("__test_01_context"),
        "test_process_current_branch_on_level3_then_current_head_level4",
        &tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        (log, log_level),
    )?;

    // wait for storage initialization to genesis
    node.wait_for_new_current_head(
        "genesis",
        node.tezos_env.genesis_header_hash()?,
        (Duration::from_secs(5), Duration::from_millis(250)),
    )?;

    // connect mocked node peer with test data set
    let clocks = Instant::now();
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64)?,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_3::serve_data,
    );

    // wait for current head on level 3
    node.wait_for_new_current_head(
        "3",
        db.block_hash(3)?,
        (Duration::from_secs(60), Duration::from_millis(750)),
    )?;
    println!("\nProcessed current_branch[3] in {:?}!\n", clocks.elapsed());

    // send current_head with level4
    let clocks = Instant::now();
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(4)?,
        Mempool::default(),
    ))?;
    // wait for current head on level 4
    node.wait_for_new_current_head(
        "4",
        db.block_hash(4)?,
        (Duration::from_secs(10), Duration::from_millis(750)),
    )?;
    println!("\nProcessed current_head[4] in {:?}!\n", clocks.elapsed());

    // check context stored for all blocks
    node.wait_for_context(
        "ctx_1",
        db.context_hash(1)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "ctx_2",
        db.context_hash(2)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "ctx_3",
        db.context_hash(3)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "ctx_4",
        db.context_hash(4)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;

    // stop nodes
    drop(mocked_peer_node);
    drop(node);

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_reorg_with_different_current_branches() -> Result<(), failure::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    // prepare env data
    let (tezos_env, patch_context) = {
        let (db, patch_context) = common::test_cases_data::sandbox_branch_1_level3::init_data(&log);
        (db.tezos_env, patch_context)
    };
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&tezos_env)
        .expect("no environment configuration");

    // start node
    let node = common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_02"))?,
        &common::prepare_empty_dir("__test_02_context"),
        "test_process_reorg_with_different_current_branches",
        &tezos_env,
        patch_context,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        (log, log_level),
    )?;

    // wait for storage initialization to genesis
    node.wait_for_new_current_head(
        "genesis",
        node.tezos_env.genesis_header_hash()?,
        (Duration::from_secs(5), Duration::from_millis(250)),
    )?;

    // connect mocked node peer with data for branch_1
    let (db_branch_1, ..) = common::test_cases_data::sandbox_branch_1_level3::init_data(&node.log);
    let clocks = Instant::now();
    let mocked_peer_node_branch_1 = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE_BRANCH_1",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64)?,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::sandbox_branch_1_level3::serve_data,
    );

    // wait for current head on level 3
    node.wait_for_new_current_head(
        "branch1-3",
        db_branch_1.block_hash(3)?,
        (Duration::from_secs(30), Duration::from_millis(750)),
    )?;
    println!("\nProcessed [branch1-3] in {:?}!\n", clocks.elapsed());

    // connect mocked node peer with data for branch_2
    let clocks = Instant::now();
    let (db_branch_2, ..) = common::test_cases_data::sandbox_branch_2_level4::init_data(&node.log);
    let mocked_peer_node_branch_2 = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE_BRANCH_2",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64)?,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::sandbox_branch_2_level4::serve_data,
    );

    // wait for current head on level 4
    node.wait_for_new_current_head(
        "branch2-4",
        db_branch_2.block_hash(4)?,
        (Duration::from_secs(30), Duration::from_millis(750)),
    )?;
    println!("\nProcessed [branch2-4] in {:?}!\n", clocks.elapsed());

    ////////////////////////////////////////////
    // 1. CONTEXT - check context stored for all branches
    node.wait_for_context(
        "db_branch_1_ctx_1",
        db_branch_1.context_hash(1)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "db_branch_1_ctx_2",
        db_branch_1.context_hash(2)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "db_branch_1_ctx_3",
        db_branch_1.context_hash(3)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;

    node.wait_for_context(
        "db_branch_2_ctx_1",
        db_branch_2.context_hash(1)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "db_branch_2_ctx_2",
        db_branch_2.context_hash(2)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "db_branch_2_ctx_3",
        db_branch_2.context_hash(3)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "db_branch_2_ctx_4",
        db_branch_2.context_hash(4)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;

    ////////////////////////////////////////////
    // 2. HISTORY of blocks - check live_blocks for both branches (kind of check by chain traversal throught predecessors)
    let genesis_block_hash = node.tezos_env.genesis_header_hash()?;
    let block_meta_storage = BlockMetaStorage::new(node.tmp_storage.storage());

    let live_blocks_branch_1 =
        block_meta_storage.get_live_blocks(db_branch_1.block_hash(3)?, 10)?;
    assert_eq!(4, live_blocks_branch_1.len());
    assert!(live_blocks_branch_1.contains(&genesis_block_hash));
    assert!(live_blocks_branch_1.contains(&db_branch_1.block_hash(1)?));
    assert!(live_blocks_branch_1.contains(&db_branch_1.block_hash(2)?));
    assert!(live_blocks_branch_1.contains(&db_branch_1.block_hash(3)?));

    let live_blocks_branch_2 =
        block_meta_storage.get_live_blocks(db_branch_2.block_hash(4)?, 10)?;
    assert_eq!(5, live_blocks_branch_2.len());
    assert!(live_blocks_branch_2.contains(&genesis_block_hash));
    assert!(live_blocks_branch_2.contains(&db_branch_2.block_hash(1)?));
    assert!(live_blocks_branch_2.contains(&db_branch_2.block_hash(2)?));
    assert!(live_blocks_branch_2.contains(&db_branch_2.block_hash(3)?));
    assert!(live_blocks_branch_2.contains(&db_branch_2.block_hash(4)?));

    // stop nodes
    drop(mocked_peer_node_branch_2);
    drop(mocked_peer_node_branch_1);
    drop(node);

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_current_heads_to_level3() -> Result<(), failure::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = common::test_cases_data::dont_serve_current_branch_messages::init_data(&log);
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&db.tezos_env)
        .expect("no environment configuration");

    // start node
    let node = common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_03"))?,
        &common::prepare_empty_dir("__test_03_context"),
        "test_process_current_heads_to_level3",
        &tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        (log, log_level),
    )?;

    // wait for storage initialization to genesis
    node.wait_for_new_current_head(
        "genesis",
        node.tezos_env.genesis_header_hash()?,
        (Duration::from_secs(5), Duration::from_millis(250)),
    )?;

    // connect mocked node peer with test data set (dont_serve_data does not respond on p2p) - we just want to connect peers
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64)?,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::dont_serve_current_branch_messages::serve_data,
    );

    // send current_head with level1
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(1)?,
        Mempool::default(),
    ))?;
    // wait for current head on level 1
    node.wait_for_new_current_head(
        "1",
        db.block_hash(1)?,
        (Duration::from_secs(30), Duration::from_millis(750)),
    )?;

    // send current_head with level2
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(2)?,
        Mempool::default(),
    ))?;
    // wait for current head on level 2
    node.wait_for_new_current_head(
        "2",
        db.block_hash(2)?,
        (Duration::from_secs(30), Duration::from_millis(750)),
    )?;

    // send current_head with level3
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(3)?,
        Mempool::default(),
    ))?;
    // wait for current head on level 3
    node.wait_for_new_current_head(
        "3",
        db.block_hash(3)?,
        (Duration::from_secs(30), Duration::from_millis(750)),
    )?;

    // check context stored for all blocks
    node.wait_for_context(
        "ctx_1",
        db.context_hash(1)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "ctx_2",
        db.context_hash(2)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;
    node.wait_for_context(
        "ctx_3",
        db.context_hash(3)?,
        (Duration::from_secs(5), Duration::from_millis(150)),
    )?;

    // stop nodes
    drop(mocked_peer_node);
    drop(node);

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_current_head_with_malformed_blocks_and_check_blacklist(
) -> Result<(), failure::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = common::test_cases_data::current_branch_on_level_3::init_data(&log);
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&db.tezos_env)
        .expect("no environment configuration");

    // start node
    let node = common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_04"))?,
        &common::prepare_empty_dir("__test_04_context"),
        "test_process_current_head_with_malformed_blocks_and_check_blacklist",
        &tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        (log, log_level),
    )?;

    // register network channel listener
    let peers_mirror = Arc::new(RwLock::new(HashMap::new()));
    let _ = test_actor::NetworkChannelListener::actor(
        &node.actor_system,
        node.network_channel.clone(),
        peers_mirror.clone(),
    );

    // wait for storage initialization to genesis
    node.wait_for_new_current_head(
        "genesis",
        node.tezos_env.genesis_header_hash()?,
        (Duration::from_secs(5), Duration::from_millis(250)),
    )?;

    // connect mocked node peer with test data set
    let test_node_identity = tezos_identity::Identity::generate(0f64)?;
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE-1",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity.clone(),
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_3::serve_data,
    );

    // check connected
    assert!(mocked_peer_node
        .wait_for_connection((Duration::from_secs(5), Duration::from_millis(100)))
        .is_ok());
    test_actor::NetworkChannelListener::verify_connected(&mocked_peer_node, peers_mirror.clone())?;

    // wait for current head on level 3
    node.wait_for_new_current_head(
        "3",
        db.block_hash(3)?,
        (Duration::from_secs(130), Duration::from_millis(750)),
    )?;

    // send current_head with level4 (with hacked protocol data)
    // (Insufficient proof-of-work stamp)
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        common::test_cases_data::hack_block_header_rewrite_protocol_data_insufficient_pow(
            db.block_header(4)?,
        ),
        Mempool::default(),
    ))?;

    // peer should be now blacklisted
    test_actor::NetworkChannelListener::verify_blacklisted(
        &mocked_peer_node,
        peers_mirror.clone(),
    )?;
    drop(mocked_peer_node);

    // try to reconnect with same peer (ip/identity)
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE-2",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity.clone(),
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_3::serve_data,
    );
    // this should finished with error
    assert!(mocked_peer_node
        .wait_for_connection((Duration::from_secs(5), Duration::from_millis(100)))
        .is_err());
    drop(mocked_peer_node);

    // lets whitelist all
    node.whitelist_all();

    // try to reconnect with same peer (ip/identity)
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE-3",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_3::serve_data,
    );
    // this should finished with OK
    assert!(mocked_peer_node
        .wait_for_connection((Duration::from_secs(5), Duration::from_millis(100)))
        .is_ok());
    test_actor::NetworkChannelListener::verify_connected(&mocked_peer_node, peers_mirror.clone())?;

    // send current_head with level4 (with hacked protocol data)
    // (Invalid signature for block)
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        common::test_cases_data::hack_block_header_rewrite_protocol_data_bad_signature(
            db.block_header(4)?,
        ),
        Mempool::default(),
    ))?;

    // peer should be now blacklisted
    test_actor::NetworkChannelListener::verify_blacklisted(&mocked_peer_node, peers_mirror)?;

    // stop nodes
    drop(mocked_peer_node);
    drop(node);

    Ok(())
}

fn process_bootstrap_level1324_and_mempool_for_level1325(
    name: &str,
    current_head_wait_timeout: (Duration, Duration),
) -> Result<(), failure::Error> {
    let root_dir_temp_storage_path = common::prepare_empty_dir("__test_05");
    let root_context_db_path = &common::prepare_empty_dir("__test_05_context");
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = common::test_cases_data::current_branch_on_level_1324::init_data(&log);
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&db.tezos_env)
        .expect("no environment configuration");

    // start node
    let node = common::infra::NodeInfrastructure::start(
        TmpStorage::create(&root_dir_temp_storage_path)?,
        root_context_db_path,
        name,
        &tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        (log, log_level),
    )?;

    // wait for storage initialization to genesis
    node.wait_for_new_current_head(
        "genesis",
        node.tezos_env.genesis_header_hash()?,
        (Duration::from_secs(5), Duration::from_millis(250)),
    )?;

    ///////////////////////
    // BOOOSTRAP to 1324 //
    ///////////////////////
    // connect mocked node peer with test data set
    let clocks = Instant::now();
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64)?,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_1324::serve_data,
    );

    // wait for current head on level 1324
    node.wait_for_new_current_head("1324", db.block_hash(1324)?, current_head_wait_timeout)?;
    let current_head_reached = SystemTime::now();
    println!(
        "\nProcessed current_branch[1324] in {:?}!\n",
        clocks.elapsed()
    );

    // check context stored for all blocks
    node.wait_for_context(
        "ctx_1324",
        db.context_hash(1324)?,
        (Duration::from_secs(30), Duration::from_millis(150)),
    )?;
    println!(
        "\nApplied current_head[1324] vs finished context[1324] diff {:?}!\n",
        current_head_reached.elapsed()
    );

    /////////////////////
    // MEMPOOL testing //
    /////////////////////
    // Node - check current mempool state, should be on last applied block 1324
    {
        let block_hash_1324 = db.block_hash(1324)?;
        node.wait_for_mempool_on_head(
            "mempool_head_1324",
            block_hash_1324.clone(),
            (Duration::from_secs(30), Duration::from_millis(250)),
        )?;

        let current_mempool_state = node
            .current_mempool_state_storage
            .read()
            .expect("Failed to obtain lock");
        match current_mempool_state.head() {
            Some(head) => assert_eq!(head, &block_hash_1324),
            None => panic!("No head in mempool, but we expect one!"),
        }

        // check operations in mempool - should by empty all
        assert!(current_mempool_state.result().applied.is_empty());
        assert!(current_mempool_state.result().branch_delayed.is_empty());
        assert!(current_mempool_state.result().branch_refused.is_empty());
        assert!(current_mempool_state.result().refused.is_empty());
    }

    // client sends to node: current head 1324 + operations from 1325 as pending
    let operations_from_1325: Vec<OperationHash> = db
        .get_operations(&db.block_hash(1325)?)?
        .iter()
        .flatten()
        .map(|a| {
            a.message_typed_hash()
                .expect("Failed to decode operation has")
        })
        .collect();
    mocked_peer_node.clear_mempool();
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(1324)?,
        Mempool::new(vec![], operations_from_1325.clone()),
    ))?;

    let operations_from_1325 = HashSet::from_iter(operations_from_1325);
    // Node - check mempool current state after operations 1325 (all should be applied)
    {
        // node - we expect here message for every operation to finish
        node.wait_for_mempool_contains_operations(
            "node_mempool_operations_from_1325",
            &operations_from_1325,
            (Duration::from_secs(10), Duration::from_millis(250)),
        )?;

        let current_mempool_state = node
            .current_mempool_state_storage
            .read()
            .expect("Failed to obtain lock");
        assert_eq!(
            operations_from_1325.len(),
            current_mempool_state.result().applied.len()
        );
        for op in &operations_from_1325 {
            assert!(current_mempool_state
                .result()
                .applied
                .iter()
                .any(|a| a.hash.eq(op)))
        }
        assert!(current_mempool_state.result().branch_delayed.is_empty());
        assert!(current_mempool_state.result().branch_refused.is_empty());
        assert!(current_mempool_state.result().refused.is_empty());
    }

    // Client - check mempool, if received throught p2p all known_valid
    {
        mocked_peer_node.wait_for_mempool_contains_operations(
            "client_mempool_operations_from_1325",
            &operations_from_1325,
            (Duration::from_secs(10), Duration::from_millis(250)),
        )?;

        let test_mempool = mocked_peer_node
            .test_mempool
            .read()
            .expect("Failed to obtain lock");
        assert!(test_mempool.pending().is_empty());
        assert_eq!(test_mempool.known_valid().len(), operations_from_1325.len());
        for op in &operations_from_1325 {
            assert!(test_mempool.known_valid().contains(op));
        }
    }

    // generate merkle stats
    let merkle_stats = stats::generate_merkle_context_stats(node.tmp_storage.storage())?;

    // generate storage stats
    let mut disk_usage_stats = Vec::new();
    disk_usage_stats.extend(stats::generate_dir_stats(
        "TezEdge DBs:",
        3,
        &root_dir_temp_storage_path,
        true,
    )?);
    disk_usage_stats.extend(stats::generate_dir_stats(
        "Ocaml context:",
        3,
        &root_context_db_path,
        true,
    )?);

    // stop nodes
    drop(mocked_peer_node);
    drop(node);

    // print stats
    println!();
    println!();
    println!("==========================");
    println!("Storage disk usage stats:");
    println!("==========================");
    disk_usage_stats
        .iter()
        .for_each(|stat| println!("{}", stat));
    println!();
    println!();
    println!("==========================");
    println!("Merkle/context stats:");
    println!("==========================");
    merkle_stats.iter().for_each(|stat| println!("{}", stat));
    println!();
    println!();

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_bootstrap_level1324_and_mempool_for_level1325() -> Result<(), failure::Error> {
    process_bootstrap_level1324_and_mempool_for_level1325(
        "process_bootstrap_level1324_and_mempool_for_level1325",
        (Duration::from_secs(90), Duration::from_millis(500)),
    )
}

mod test_actor {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, SystemTime};

    use riker::actors::*;

    use crypto::hash::CryptoboxPublicKeyHash;
    use networking::p2p::network_channel::{NetworkChannelMsg, NetworkChannelRef};
    use shell::subscription::subscribe_to_network_events;

    use crate::common::test_node_peer::TestNodePeer;

    #[actor(NetworkChannelMsg)]
    pub(crate) struct NetworkChannelListener {
        peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, String>>>,
        network_channel: NetworkChannelRef,
    }

    pub type NetworkChannelListenerRef = ActorRef<NetworkChannelListenerMsg>;

    impl Actor for NetworkChannelListener {
        type Msg = NetworkChannelListenerMsg;

        fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
            subscribe_to_network_events(&self.network_channel, ctx.myself());
        }

        fn recv(
            &mut self,
            ctx: &Context<Self::Msg>,
            msg: Self::Msg,
            sender: Option<BasicActorRef>,
        ) {
            self.receive(ctx, msg, sender);
        }
    }

    impl
        ActorFactoryArgs<(
            NetworkChannelRef,
            Arc<RwLock<HashMap<CryptoboxPublicKeyHash, String>>>,
        )> for NetworkChannelListener
    {
        fn create_args(
            (network_channel, peers_mirror): (
                NetworkChannelRef,
                Arc<RwLock<HashMap<CryptoboxPublicKeyHash, String>>>,
            ),
        ) -> Self {
            Self {
                network_channel,
                peers_mirror,
            }
        }
    }

    impl Receive<NetworkChannelMsg> for NetworkChannelListener {
        type Msg = NetworkChannelListenerMsg;

        fn receive(&mut self, ctx: &Context<Self::Msg>, msg: NetworkChannelMsg, _sender: Sender) {
            self.process_network_channel_message(ctx, msg)
        }
    }

    impl NetworkChannelListener {
        pub fn name() -> &'static str {
            "network-channel-listener-actor"
        }

        pub fn actor(
            sys: &ActorSystem,
            network_channel: NetworkChannelRef,
            peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, String>>>,
        ) -> Result<NetworkChannelListenerRef, CreateError> {
            Ok(sys.actor_of_props::<NetworkChannelListener>(
                Self::name(),
                Props::new_args((network_channel, peers_mirror)),
            )?)
        }

        fn process_network_channel_message(
            &mut self,
            _: &Context<NetworkChannelListenerMsg>,
            msg: NetworkChannelMsg,
        ) {
            match msg {
                NetworkChannelMsg::PeerMessageReceived(_) => {}
                NetworkChannelMsg::PeerBootstrapped(peer_id, _, _) => {
                    self.peers_mirror.write().unwrap().insert(
                        peer_id.peer_public_key_hash.clone(),
                        "CONNECTED".to_string(),
                    );
                }
                NetworkChannelMsg::BlacklistPeer(..) => {}
                NetworkChannelMsg::PeerBlacklisted(peer_id) => {
                    self.peers_mirror.write().unwrap().insert(
                        peer_id.peer_public_key_hash.clone(),
                        "BLACKLISTED".to_string(),
                    );
                }
                _ => (),
            }
        }

        pub fn verify_connected(
            peer: &TestNodePeer,
            peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, String>>>,
        ) -> Result<(), failure::Error> {
            Self::verify_state(
                "CONNECTED",
                peer,
                peers_mirror,
                (Duration::from_secs(5), Duration::from_millis(250)),
            )
        }

        pub fn verify_blacklisted(
            peer: &TestNodePeer,
            peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, String>>>,
        ) -> Result<(), failure::Error> {
            Self::verify_state(
                "BLACKLISTED",
                peer,
                peers_mirror,
                (Duration::from_secs(5), Duration::from_millis(250)),
            )
        }

        // TODO: refactor with async/condvar, not to block main thread
        fn verify_state(
            expected_state: &str,
            peer: &TestNodePeer,
            peers_mirror: Arc<RwLock<HashMap<CryptoboxPublicKeyHash, String>>>,
            (timeout, delay): (Duration, Duration),
        ) -> Result<(), failure::Error> {
            let start = SystemTime::now();
            let peer_public_key_hash = &peer.identity.public_key.public_key_hash()?;

            let result = loop {
                let peers_mirror = peers_mirror.read().unwrap();
                if let Some(peer_state) = peers_mirror.get(peer_public_key_hash) {
                    if peer_state == expected_state {
                        break Ok(());
                    }
                }

                // kind of simple retry policy
                if start.elapsed()?.le(&timeout) {
                    std::thread::sleep(delay);
                } else {
                    break Err(
                        failure::format_err!(
                            "[{}] verify_state - peer_public_key({}) - (expected_state: {}) - timeout (timeout: {:?}, delay: {:?}) exceeded!",
                            peer.name, peer_public_key_hash.to_base58_check(), expected_state, timeout, delay
                        )
                    );
                }
            };
            result
        }
    }
}

mod stats {
    use std::path::Path;

    use fs_extra::dir::{get_dir_content2, get_size, DirOptions};
    use storage::persistent::PersistentStorage;

    pub fn generate_dir_stats<P: AsRef<Path>>(
        marker: &str,
        depth: u64,
        path: P,
        human_format: bool,
    ) -> Result<Vec<String>, failure::Error> {
        let mut stats = Vec::new();
        stats.push(String::from(""));
        stats.push(format!("{}", marker));
        stats.push(String::from("------------"));

        let mut options = DirOptions::new();
        options.depth = depth;
        let dir_content = get_dir_content2(path, &options)?;
        for directory in dir_content.directories {
            let dir_size = if human_format {
                human_readable(get_size(&directory)?)
            } else {
                get_size(&directory)?.to_string()
            };
            // print directory path and size
            stats.push(format!("{} {}", &directory, dir_size));
        }

        Ok(stats)
    }

    fn human_readable(bytes: u64) -> String {
        let mut bytes = bytes as i64;
        if -1000 < bytes && bytes < 1000 {
            return format!("{} B", bytes);
        }
        let mut ci = "kMGTPE".chars();
        while bytes <= -999_950 || bytes >= 999_950 {
            bytes /= 1000;
            ci.next();
        }

        return format!("{:.1} {}B", bytes as f64 / 1000.0, ci.next().unwrap());
    }

    pub fn generate_merkle_context_stats(
        persistent_storage: &PersistentStorage,
    ) -> Result<Vec<String>, failure::Error> {
        let mut log_for_stats = Vec::new();

        // generate stats
        let m = persistent_storage.merkle();
        let merkle = m
            .write()
            .map_err(|e| failure::format_err!("Lock error: {:?}", e))?;
        let stats = merkle.get_merkle_stats()?;

        log_for_stats.push(String::from(""));
        log_for_stats.push("Context storage global latency statistics:".to_string());
        log_for_stats.push(String::from("------------"));
        for (op, v) in stats.perf_stats.global.iter() {
            log_for_stats.push(format!("{}:", op));
            log_for_stats.push(format!(
                "\tavg: {:.0}ns, min: {:.0}ns, max: {:.0}ns, times: {}",
                v.avg_exec_time, v.op_exec_time_min, v.op_exec_time_max, v.op_exec_times
            ));
        }
        log_for_stats.push(String::from(""));
        log_for_stats.push("Context storage per-path latency statistics:".to_string());
        log_for_stats.push(String::from("------------"));
        for (node, v) in stats.perf_stats.perpath.iter() {
            log_for_stats.push(format!("{}:", node));
            for (op, v) in v.iter() {
                log_for_stats.push(format!(
                    "\t{}: avg: {:.0}ns, min: {:.0}ns, max: {:.0}ns, times: {}",
                    op, v.avg_exec_time, v.op_exec_time_min, v.op_exec_time_max, v.op_exec_times
                ));
            }
        }

        Ok(log_for_stats)
    }
}
