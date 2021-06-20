// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]
extern crate test;

// TODO - TE-281: some tests here use wait_for_context, fix that once not required anymore

/// Simple integration test for chain actors
///
///(Tests are ignored, because they need protocol-runner binary)
/// Runs like: `PROTOCOL_RUNNER=./target/release/protocol-runner cargo test --release -- --ignored`
use std::collections::{HashMap, HashSet};
use std::env;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

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

pub const SIMPLE_POW_TARGET: f64 = 0f64;

lazy_static! {
    pub static ref SHELL_COMPATIBILITY_VERSION: ShellCompatibilityVersion = ShellCompatibilityVersion::new("TEST_CHAIN".to_string(), vec![0], vec![0]);
    pub static ref NODE_P2P_PORT: u16 = 1234; // TODO: maybe some logic to verify and get free port
    pub static ref NODE_P2P_CFG: (P2p, ShellCompatibilityVersion) = (
        P2p {
            listener_port: *NODE_P2P_PORT,
            listener_address: format!("0.0.0.0:{}", *NODE_P2P_PORT).parse::<SocketAddr>().expect("Failed to parse listener address"),
            bootstrap_lookup_addresses: vec![],
            disable_bootstrap_lookup: true,
            disable_mempool: false,
            private_node: false,
            bootstrap_peers: vec![],
            peer_threshold: PeerConnectionThreshold::try_new(0, 10, Some(0)).expect("Invalid range"),
        },
        SHELL_COMPATIBILITY_VERSION.clone(),
    );
    pub static ref NODE_IDENTITY: Identity = tezos_identity::Identity::generate(SIMPLE_POW_TARGET).unwrap();
    pub static ref PEER_IDENTITY: Identity = tezos_identity::Identity::generate(SIMPLE_POW_TARGET).unwrap();
}

#[ignore]
#[test]
#[serial]
fn test_process_current_branch_on_level3_then_current_head_level4() -> Result<(), anyhow::Error> {
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
        SIMPLE_POW_TARGET,
        (log, log_level),
        (false, false),
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
        "TEST_PEER_NODE".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        PEER_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
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

    // stop nodes
    drop(mocked_peer_node);
    drop(node);

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_bootstrapping_current_branch_on_level3_then_current_heads(
) -> Result<(), anyhow::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = common::test_cases_data::current_branch_on_level_3::init_data(&log);
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&db.tezos_env)
        .expect("no environment configuration");

    // we need here test also bootstrap status
    let p2p_cfg = common::p2p_cfg_with_threshold(NODE_P2P_CFG.clone(), 1, 2, 1);

    // start node
    let node = crate::common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_07"))?,
        &common::prepare_empty_dir("__test_07_context"),
        "test_process_bootstrapping_current_branch_on_level3_then_current_heads",
        &tezos_env,
        None,
        Some(p2p_cfg),
        NODE_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
        (log, log_level),
        (false, false),
    )?;

    // wait for storage initialization to genesis
    node.wait_for_new_current_head(
        "genesis",
        node.tezos_env.genesis_header_hash()?,
        (Duration::from_secs(5), Duration::from_millis(250)),
    )?;

    // check not bootstrapped
    assert!(!node
        .bootstrap_state
        .read()
        .expect("Failed to get lock")
        .is_bootstrapped());

    // connect mocked node peer with test data set
    let clocks = Instant::now();
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64)?,
        SIMPLE_POW_TARGET,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_3::serve_data,
    );

    // send current_head with level4
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(4)?,
        Mempool::default(),
    ))?;
    // send current_head with level5
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(5)?,
        Mempool::default(),
    ))?;
    // send current_head with level6
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(6)?,
        Mempool::default(),
    ))?;
    // send current_head with level7
    mocked_peer_node.send_msg(CurrentHeadMessage::new(
        node.tezos_env.main_chain_id()?,
        db.block_header(7)?,
        Mempool::default(),
    ))?;

    // wait for current head on level 3
    node.wait_for_new_current_head(
        "7",
        db.block_hash(7)?,
        (Duration::from_secs(60), Duration::from_millis(750)),
    )?;

    println!("\nProcessed current_branch[7] in {:?}!\n", clocks.elapsed());

    // check not bootstrapped
    assert!(node
        .bootstrap_state
        .read()
        .expect("Failed to get lock")
        .is_bootstrapped());

    // stop nodes
    drop(mocked_peer_node);
    drop(node);

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_reorg_with_different_current_branches() -> Result<(), anyhow::Error> {
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
        SIMPLE_POW_TARGET,
        (log, log_level),
        (false, false),
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
        "TEST_PEER_NODE_BRANCH_1-3".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        PEER_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
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
        "TEST_PEER_NODE_BRANCH_2-4".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        PEER_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
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
fn test_process_current_heads_to_level3() -> Result<(), anyhow::Error> {
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
        SIMPLE_POW_TARGET,
        (log, log_level),
        (false, false),
    )?;

    // wait for storage initialization to genesis
    node.wait_for_new_current_head(
        "genesis",
        node.tezos_env.genesis_header_hash()?,
        (Duration::from_secs(5), Duration::from_millis(250)),
    )?;

    // connect mocked node peer with test data set (dont_serve_data does not respond on p2p) - we just want to connect peers
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        PEER_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
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

    // stop nodes
    drop(mocked_peer_node);
    drop(node);

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_current_head_with_malformed_blocks_and_check_blacklist() -> Result<(), anyhow::Error>
{
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
        SIMPLE_POW_TARGET,
        (log, log_level),
        (false, false),
    )?;

    // register network channel listener
    let peers_mirror = Arc::new(RwLock::new(HashMap::new()));
    let _ = common::infra::test_actor::NetworkChannelListener::actor(
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
    let test_node_identity = PEER_IDENTITY.clone();
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE-1".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity.clone(),
        SIMPLE_POW_TARGET,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_3::serve_data,
    );

    // check connected
    assert!(mocked_peer_node
        .wait_for_connection((Duration::from_secs(5), Duration::from_millis(100)))
        .is_ok());
    common::infra::test_actor::NetworkChannelListener::verify_connected(
        &mocked_peer_node,
        peers_mirror.clone(),
    )?;

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
    common::infra::test_actor::NetworkChannelListener::verify_blacklisted(
        &mocked_peer_node,
        peers_mirror.clone(),
    )?;
    drop(mocked_peer_node);

    // try to reconnect with same peer (ip/identity)
    let mut mocked_peer_node = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE-2".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity.clone(),
        SIMPLE_POW_TARGET,
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
        "TEST_PEER_NODE-3".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity,
        SIMPLE_POW_TARGET,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_3::serve_data,
    );
    // this should finished with OK
    assert!(mocked_peer_node
        .wait_for_connection((Duration::from_secs(5), Duration::from_millis(100)))
        .is_ok());
    common::infra::test_actor::NetworkChannelListener::verify_connected(
        &mocked_peer_node,
        peers_mirror.clone(),
    )?;

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
    common::infra::test_actor::NetworkChannelListener::verify_blacklisted(
        &mocked_peer_node,
        peers_mirror,
    )?;

    // stop nodes
    drop(mocked_peer_node);
    drop(node);

    Ok(())
}

fn process_bootstrap_level1324_and_mempool_for_level1325(
    name: &str,
    current_head_wait_timeout: (Duration, Duration),
) -> Result<(), anyhow::Error> {
    let root_dir_temp_storage_path = common::prepare_empty_dir("__test_05");
    let root_context_db_path = &common::prepare_empty_dir("__test_05_context");

    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = common::test_cases_data::current_branch_on_level_1324::init_data(&log);
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&db.tezos_env)
        .expect("no environment configuration");

    // storage for test
    let storage = TmpStorage::create(&root_dir_temp_storage_path)?;

    // start node
    let node = common::infra::NodeInfrastructure::start(
        storage,
        root_context_db_path,
        name,
        &tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
        (log, log_level),
        (true, false),
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
        "TEST_PEER_NODE".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        PEER_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_1324::serve_data,
    );

    // wait for current head on level 1324
    node.wait_for_new_current_head("1324", db.block_hash(1324)?, current_head_wait_timeout)?;
    let current_head_reached = Instant::now();
    println!(
        "\nProcessed current_branch[1324] in {:?}!\n",
        clocks.elapsed()
    );

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
fn test_process_bootstrap_level1324_and_mempool_for_level1325() -> Result<(), anyhow::Error> {
    process_bootstrap_level1324_and_mempool_for_level1325(
        "process_bootstrap_level1324_and_mempool_for_level1325",
        (Duration::from_secs(90), Duration::from_millis(500)),
    )
}

#[ignore]
#[test]
#[serial]
fn test_process_bootstrap_level1324_and_generate_action_file() -> Result<(), anyhow::Error> {
    let root_dir_temp_storage_path = common::prepare_empty_dir("__test_06");
    let root_context_db_path = &common::prepare_empty_dir("__test_06_context");
    let target_action_file = ensure_target_action_file()?;

    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = common::test_cases_data::current_branch_on_level_1324::init_data(&log);
    let tezos_env: &TezosEnvironmentConfiguration = TEZOS_ENV
        .get(&db.tezos_env)
        .expect("no environment configuration");

    // storage for test
    let storage = TmpStorage::create(&root_dir_temp_storage_path)?;

    // start node
    let node = common::infra::NodeInfrastructure::start(
        storage,
        root_context_db_path,
        "test_process_bootstrap_level1324_and_generate_action_file",
        &tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
        (log, log_level),
        (true, true),
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
    let _ = common::test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE".to_string(),
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        PEER_IDENTITY.clone(),
        SIMPLE_POW_TARGET,
        node.log.clone(),
        &node.tokio_runtime,
        common::test_cases_data::current_branch_on_level_1324::serve_data,
    );
    // wait for current head on level 1324
    let current_head_wait_timeout = (Duration::from_secs(120), Duration::from_millis(500));
    node.wait_for_new_current_head("1324", db.block_hash(1324)?, current_head_wait_timeout)?;
    let current_head_reached = Instant::now();
    println!(
        "\nProcessed current_branch[1324] in {:?}!\n",
        clocks.elapsed()
    );

    println!(
        "\nApplied current_head[1324] vs finished context[1324] diff {:?}!\n",
        current_head_reached.elapsed()
    );

    println!();
    println!();
    println!("Action file generated: {:?}", target_action_file);
    println!();
    println!();

    Ok(())
}

fn ensure_target_action_file() -> Result<PathBuf, anyhow::Error> {
    let action_file_path = env::var("TARGET_ACTION_FILE")
        .unwrap_or_else(|_| panic!("This test requires environment parameter: 'TARGET_ACTION_FILE' to point to the file, where to store recorded context action"));
    let path = PathBuf::from(action_file_path);
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(path)
}

mod stats {
    use std::path::Path;

    use fs_extra::dir::{get_dir_content2, get_size, DirOptions};

    use storage::PersistentStorage;

    pub fn generate_dir_stats<P: AsRef<Path>>(
        marker: &str,
        depth: u64,
        path: P,
        human_format: bool,
    ) -> Result<Vec<String>, anyhow::Error> {
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

    // TODO - TE-261: revise this, can it be restored?
    pub fn generate_merkle_context_stats(
        _persistent_storage: &PersistentStorage,
    ) -> Result<Vec<String>, anyhow::Error> {
        //let mut log_for_stats = Vec::new();

        //// generate stats
        //let m = persistent_storage.merkle();
        //let merkle = m
        //    .lock()
        //    .map_err(|e| anyhow::format_err!("Lock error: {:?}", e))?;
        //let stats = merkle.get_merkle_stats()?;

        //log_for_stats.push(String::from(""));
        //log_for_stats.push("Context storage global latency statistics:".to_string());
        //log_for_stats.push(String::from("------------"));
        //for (op, v) in stats.perf_stats.global.iter() {
        //    log_for_stats.push(format!("{}:", op));
        //    log_for_stats.push(format!(
        //        "\tavg: {:.0}ns, min: {:.0}ns, max: {:.0}ns, times: {}",
        //        v.avg_exec_time, v.op_exec_time_min, v.op_exec_time_max, v.op_exec_times
        //    ));
        //}
        //log_for_stats.push(String::from(""));
        //log_for_stats.push("Context storage per-path latency statistics:".to_string());
        //log_for_stats.push(String::from("------------"));
        //for (node, v) in stats.perf_stats.perpath.iter() {
        //    log_for_stats.push(format!("{}:", node));
        //    for (op, v) in v.iter() {
        //        log_for_stats.push(format!(
        //            "\t{}: avg: {:.0}ns, min: {:.0}ns, max: {:.0}ns, times: {}",
        //            op, v.avg_exec_time, v.op_exec_time_min, v.op_exec_time_max, v.op_exec_times
        //        ));
        //    }
        //}

        Ok(vec![])
    }
}
