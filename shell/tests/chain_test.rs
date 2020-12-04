// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]
extern crate test;

/// Simple integration test for actors
///
/// (Tests are ignored, because they need protocol-runner binary)
/// Runs like: `PROTOCOL_RUNNER=./target/release/protocol-runner cargo test --release -- --ignored`

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use lazy_static::lazy_static;
use serial_test::serial;

use shell::peer_manager::P2p;
use shell::PeerConnectionThreshold;
use storage::{BlockMetaStorage, BlockMetaStorageReader};
use storage::tests_common::TmpStorage;
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::current_head::CurrentHeadMessage;
use tezos_messages::p2p::encoding::prelude::Mempool;
use tezos_messages::p2p::encoding::version::NetworkVersion;

mod common;
mod samples;

lazy_static! {
    pub static ref NETWORK_VERSION: NetworkVersion = NetworkVersion::new("TEST_CHAIN".to_string(), 0, 0);
    pub static ref NODE_P2P_PORT: u16 = 1234; // TODO: maybe some logic to verify and get free port
    pub static ref NODE_P2P_CFG: (P2p, NetworkVersion) = (
        P2p {
            listener_port: NODE_P2P_PORT.clone(),
            bootstrap_lookup_addresses: vec![],
            disable_bootstrap_lookup: true,
            disable_mempool: false,
            private_node: false,
            initial_peers: vec![],
            peer_threshold: PeerConnectionThreshold::new(0, 10),
            firewall_socket_path: common::firewall_socket_path(),
        },
        NETWORK_VERSION.clone(),
    );
    pub static ref NODE_IDENTITY: Identity = tezos_identity::Identity::generate(0f64);
}

#[ignore]
#[test]
#[serial]
fn test_process_current_branch_on_level3_then_current_head_level4() -> Result<(), failure::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = test_cases_data::current_branch_on_level_3::init_data(&log);

    // start node
    let node = common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_01"))?,
        &common::prepare_empty_dir("__test_01_context"),
        "test_process_current_branch_on_level3_then_current_head_level4",
        &db.tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        (log, log_level),
    )?;

    // wait for storage initialization to genesis
    node.wait_for_new_current_head("genesis", node.tezos_env.genesis_header_hash()?, (Duration::from_secs(5), Duration::from_millis(250)))?;

    // connect mocked node peer with test data set
    let clocks = Instant::now();
    let mut mocked_peer_node = test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64),
        node.log.clone(),
        &node.tokio_runtime,
        test_cases_data::current_branch_on_level_3::serve_data,
    );

    // wait for current head on level 3
    node.wait_for_new_current_head("3", db.block_hash(3)?, (Duration::from_secs(60), Duration::from_millis(750)))?;
    println!("\nProcessed current_branch[3] in {:?}!\n", clocks.elapsed());

    // send current_head with level4
    let clocks = Instant::now();
    mocked_peer_node.send_msg(
        CurrentHeadMessage::new(
            node.tezos_env.main_chain_id()?,
            db.block_header(4)?,
            Mempool::default(),
        )
    )?;
    // wait for current head on level 4
    node.wait_for_new_current_head("4", db.block_hash(4)?, (Duration::from_secs(10), Duration::from_millis(750)))?;
    println!("\nProcessed current_head[4] in {:?}!\n", clocks.elapsed());

    // check context stored for all blocks
    node.wait_for_context("ctx_1", db.context_hash(1)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("ctx_2", db.context_hash(2)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("ctx_3", db.context_hash(3)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("ctx_4", db.context_hash(4)?, (Duration::from_secs(5), Duration::from_millis(150)))?;

    // stop nodes
    drop(node);
    drop(mocked_peer_node);

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
        let (db, patch_context) = test_cases_data::sandbox_branch_1_level3::init_data(&log);
        (db.tezos_env, patch_context)
    };

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
    node.wait_for_new_current_head("genesis", node.tezos_env.genesis_header_hash()?, (Duration::from_secs(5), Duration::from_millis(250)))?;

    // connect mocked node peer with data for branch_1
    let (db_branch_1, ..) = test_cases_data::sandbox_branch_1_level3::init_data(&node.log);
    let clocks = Instant::now();
    let mocked_peer_node_branch_1 = test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE_BRANCH_1",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64),
        node.log.clone(),
        &node.tokio_runtime,
        test_cases_data::sandbox_branch_1_level3::serve_data,
    );

    // wait for current head on level 3
    node.wait_for_new_current_head("branch1-3", db_branch_1.block_hash(3)?, (Duration::from_secs(30), Duration::from_millis(750)))?;
    println!("\nProcessed [branch1-3] in {:?}!\n", clocks.elapsed());

    drop(mocked_peer_node_branch_1);

    // connect mocked node peer with data for branch_2
    let clocks = Instant::now();
    let (db_branch_2, ..) = test_cases_data::sandbox_branch_2_level4::init_data(&node.log);
    let mocked_peer_node_branch_2 = test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE_BRANCH_2",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64),
        node.log.clone(),
        &node.tokio_runtime,
        test_cases_data::sandbox_branch_2_level4::serve_data,
    );

    // wait for current head on level 4
    node.wait_for_new_current_head("branch2-4", db_branch_2.block_hash(4)?, (Duration::from_secs(30), Duration::from_millis(750)))?;
    println!("\nProcessed [branch2-4] in {:?}!\n", clocks.elapsed());


    ////////////////////////////////////////////
    // 1. CONTEXT - check context stored for all branches
    node.wait_for_context("db_branch_1_ctx_1", db_branch_1.context_hash(1)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("db_branch_1_ctx_2", db_branch_1.context_hash(2)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("db_branch_1_ctx_3", db_branch_1.context_hash(3)?, (Duration::from_secs(5), Duration::from_millis(150)))?;

    node.wait_for_context("db_branch_2_ctx_1", db_branch_2.context_hash(1)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("db_branch_2_ctx_2", db_branch_2.context_hash(2)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("db_branch_2_ctx_3", db_branch_2.context_hash(3)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("db_branch_2_ctx_4", db_branch_2.context_hash(4)?, (Duration::from_secs(5), Duration::from_millis(150)))?;

    ////////////////////////////////////////////
    // 2. HISTORY of blocks - check live_blocks for both branches (kind of check by chain traversal throught predecessors)
    let genesis_block_hash = node.tezos_env.genesis_header_hash()?;
    let block_meta_storage = BlockMetaStorage::new(node.tmp_storage.storage());

    let live_blocks_branch_1 = block_meta_storage.get_live_blocks(db_branch_1.block_hash(3)?, 10)?;
    assert_eq!(4, live_blocks_branch_1.len());
    assert!(live_blocks_branch_1.contains(&genesis_block_hash));
    assert!(live_blocks_branch_1.contains(&db_branch_1.block_hash(1)?));
    assert!(live_blocks_branch_1.contains(&db_branch_1.block_hash(2)?));
    assert!(live_blocks_branch_1.contains(&db_branch_1.block_hash(3)?));

    let live_blocks_branch_2 = block_meta_storage.get_live_blocks(db_branch_2.block_hash(4)?, 10)?;
    assert_eq!(5, live_blocks_branch_2.len());
    assert!(live_blocks_branch_2.contains(&genesis_block_hash));
    assert!(live_blocks_branch_2.contains(&db_branch_2.block_hash(1)?));
    assert!(live_blocks_branch_2.contains(&db_branch_2.block_hash(2)?));
    assert!(live_blocks_branch_2.contains(&db_branch_2.block_hash(3)?));
    assert!(live_blocks_branch_2.contains(&db_branch_2.block_hash(4)?));

    // stop nodes
    drop(node);
    // drop(mocked_peer_node_branch_1);
    drop(mocked_peer_node_branch_2);

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_current_heads_to_level3() -> Result<(), failure::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = test_cases_data::dont_serve_current_branch_messages::init_data(&log);

    // start node
    let node = common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_03"))?,
        &common::prepare_empty_dir("__test_03_context"),
        "test_process_current_heads_to_level3",
        &db.tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        (log, log_level),
    )?;

    // wait for storage initialization to genesis
    node.wait_for_new_current_head("genesis", node.tezos_env.genesis_header_hash()?, (Duration::from_secs(5), Duration::from_millis(250)))?;

    // connect mocked node peer with test data set (dont_serve_data does not respond on p2p) - we just want to connect peers
    let mut mocked_peer_node = test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        tezos_identity::Identity::generate(0f64),
        node.log.clone(),
        &node.tokio_runtime,
        test_cases_data::dont_serve_current_branch_messages::serve_data,
    );

    // send current_head with level1
    mocked_peer_node.send_msg(
        CurrentHeadMessage::new(
            node.tezos_env.main_chain_id()?,
            db.block_header(1)?,
            Mempool::default(),
        )
    )?;
    // wait for current head on level 1
    node.wait_for_new_current_head("1", db.block_hash(1)?, (Duration::from_secs(30), Duration::from_millis(750)))?;

    // send current_head with level2
    mocked_peer_node.send_msg(
        CurrentHeadMessage::new(
            node.tezos_env.main_chain_id()?,
            db.block_header(2)?,
            Mempool::default(),
        )
    )?;
    // wait for current head on level 2
    node.wait_for_new_current_head("2", db.block_hash(2)?, (Duration::from_secs(30), Duration::from_millis(750)))?;

    // send current_head with level3
    mocked_peer_node.send_msg(
        CurrentHeadMessage::new(
            node.tezos_env.main_chain_id()?,
            db.block_header(3)?,
            Mempool::default(),
        )
    )?;
    // wait for current head on level 3
    node.wait_for_new_current_head("3", db.block_hash(3)?, (Duration::from_secs(30), Duration::from_millis(750)))?;

    // check context stored for all blocks
    node.wait_for_context("ctx_1", db.context_hash(1)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("ctx_2", db.context_hash(2)?, (Duration::from_secs(5), Duration::from_millis(150)))?;
    node.wait_for_context("ctx_3", db.context_hash(3)?, (Duration::from_secs(5), Duration::from_millis(150)))?;

    // stop nodes
    drop(node);
    drop(mocked_peer_node);

    Ok(())
}

#[ignore]
#[test]
#[serial]
fn test_process_current_head_with_malformed_blocks_and_check_blacklist() -> Result<(), failure::Error> {
    // logger
    let log_level = common::log_level();
    let log = common::create_logger(log_level);

    let db = test_cases_data::current_branch_on_level_3::init_data(&log);

    // start node
    let node = common::infra::NodeInfrastructure::start(
        TmpStorage::create(common::prepare_empty_dir("__test_04"))?,
        &common::prepare_empty_dir("__test_04_context"),
        "test_process_current_head_with_malformed_blocks_and_check_blacklist",
        &db.tezos_env,
        None,
        Some(NODE_P2P_CFG.clone()),
        NODE_IDENTITY.clone(),
        (log, log_level),
    )?;

    // register network channel listener
    let peers_mirror = Arc::new(RwLock::new(HashMap::new()));
    let _ = test_actor::NetworkChannelListener::actor(&node.actor_system, node.network_channel.clone(), peers_mirror.clone());

    // wait for storage initialization to genesis
    node.wait_for_new_current_head("genesis", node.tezos_env.genesis_header_hash()?, (Duration::from_secs(5), Duration::from_millis(250)))?;

    // connect mocked node peer with test data set
    let test_node_identity = tezos_identity::Identity::generate(0f64);
    let mut mocked_peer_node = test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE-1",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity.clone(),
        node.log.clone(),
        &node.tokio_runtime,
        test_cases_data::current_branch_on_level_3::serve_data,
    );

    // check connected
    assert!(mocked_peer_node.wait_for_connection((Duration::from_secs(5), Duration::from_millis(100))).is_ok());
    test_actor::NetworkChannelListener::verify_connected(&mocked_peer_node, peers_mirror.clone())?;

    // wait for current head on level 3
    node.wait_for_new_current_head("3", db.block_hash(3)?, (Duration::from_secs(130), Duration::from_millis(750)))?;

    // send current_head with level4 (with hacked protocol data)
    // (Insufficient proof-of-work stamp)
    mocked_peer_node.send_msg(
        CurrentHeadMessage::new(
            node.tezos_env.main_chain_id()?,
            test_cases_data::hack_block_header_rewrite_protocol_data_insufficient_pow(db.block_header(4)?),
            Mempool::default(),
        )
    )?;

    // peer should be now blacklisted
    test_actor::NetworkChannelListener::verify_blacklisted(&mocked_peer_node, peers_mirror.clone())?;
    drop(mocked_peer_node);

    // try to reconnect with same peer (ip/identity)
    let mut mocked_peer_node = test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE-2",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity.clone(),
        node.log.clone(),
        &node.tokio_runtime,
        test_cases_data::current_branch_on_level_3::serve_data,
    );
    // this should finished with error
    assert!(mocked_peer_node.wait_for_connection((Duration::from_secs(5), Duration::from_millis(100))).is_err());
    drop(mocked_peer_node);

    // lets whitelist all
    node.whitelist_all();

    // try to reconnect with same peer (ip/identity)
    let mut mocked_peer_node = test_node_peer::TestNodePeer::connect(
        "TEST_PEER_NODE-3",
        NODE_P2P_CFG.0.listener_port,
        NODE_P2P_CFG.1.clone(),
        test_node_identity,
        node.log.clone(),
        &node.tokio_runtime,
        test_cases_data::current_branch_on_level_3::serve_data,
    );
    // this should finished with OK
    assert!(mocked_peer_node.wait_for_connection((Duration::from_secs(5), Duration::from_millis(100))).is_ok());
    test_actor::NetworkChannelListener::verify_connected(&mocked_peer_node, peers_mirror.clone())?;

    // send current_head with level4 (with hacked protocol data)
    // (Invalid signature for block)
    mocked_peer_node.send_msg(
        CurrentHeadMessage::new(
            node.tezos_env.main_chain_id()?,
            test_cases_data::hack_block_header_rewrite_protocol_data_bad_signature(db.block_header(4)?),
            Mempool::default(),
        )
    )?;

    // peer should be now blacklisted
    test_actor::NetworkChannelListener::verify_blacklisted(&mocked_peer_node, peers_mirror)?;

    // stop nodes
    drop(node);
    drop(mocked_peer_node);

    Ok(())
}

/// Stored first cca first 1300 apply block data
mod test_data {
    use std::collections::HashMap;
    use std::convert::TryInto;

    use failure::format_err;

    use crypto::hash::{BlockHash, ContextHash};
    use tezos_api::environment::TezosEnvironment;
    use tezos_api::ffi::ApplyBlockRequest;
    use tezos_messages::p2p::binary_message::MessageHash;
    use tezos_messages::p2p::encoding::block_header::Level;
    use tezos_messages::p2p::encoding::prelude::{BlockHeader, OperationsForBlock, OperationsForBlocksMessage};

    use crate::samples::OperationsForBlocksMessageKey;

    pub struct Db {
        pub tezos_env: TezosEnvironment,
        requests: Vec<String>,
        headers: HashMap<BlockHash, (Level, ContextHash)>,
        operations: HashMap<OperationsForBlocksMessageKey, OperationsForBlocksMessage>,
    }

    impl Db {
        pub(crate) fn init_db((requests, operations, tezos_env): (Vec<String>, HashMap<OperationsForBlocksMessageKey, OperationsForBlocksMessage>, TezosEnvironment)) -> Db {
            let mut headers: HashMap<BlockHash, (Level, ContextHash)> = HashMap::new();

            // init headers
            for (idx, request) in requests.iter().enumerate() {
                let request = crate::samples::from_captured_bytes(request).expect("Failed to parse request");
                let block = request.block_header.message_hash().expect("Failed to decode message_hash");
                let context_hash: ContextHash = request.block_header.context().clone();
                headers.insert(block, (to_level(idx), context_hash));
            }

            Db {
                tezos_env,
                requests,
                headers,
                operations,
            }
        }

        pub fn get(&self, block_hash: &BlockHash) -> Result<Option<BlockHeader>, failure::Error> {
            match self.headers.get(block_hash) {
                Some((level, _)) => {
                    Ok(Some(self.captured_requests(*level)?.block_header))
                }
                None => Ok(None)
            }
        }

        pub fn get_operations_for_block(&self, block: &OperationsForBlock) -> Result<Option<OperationsForBlocksMessage>, failure::Error> {
            match self.operations.get(&OperationsForBlocksMessageKey::new(block.block_hash().clone(), block.validation_pass())) {
                Some(operations) => {
                    Ok(Some(operations.clone()))
                }
                None => Ok(None)
            }
        }

        pub fn block_hash(&self, searched_level: Level) -> Result<BlockHash, failure::Error> {
            let block_hash = self.headers
                .iter()
                .find(|(_, (level, _))| searched_level.eq(level))
                .map(|(k, _)| k.clone());
            match block_hash {
                Some(block_hash) => Ok(block_hash),
                None => Err(format_err!("No block_hash found for level: {}", searched_level))
            }
        }

        pub fn block_header(&self, searched_level: Level) -> Result<BlockHeader, failure::Error> {
            match self.get(&self.block_hash(searched_level)?)? {
                Some(header) => Ok(header),
                None => Err(format_err!("No block_header found for level: {}", searched_level))
            }
        }

        pub fn context_hash(&self, searched_level: Level) -> Result<ContextHash, failure::Error> {
            let context_hash = self.headers
                .iter()
                .find(|(_, (level, _))| searched_level.eq(level))
                .map(|(_, (_, context_hash))| context_hash.clone());
            match context_hash {
                Some(context_hash) => Ok(context_hash),
                None => Err(format_err!("No header found for level: {}", searched_level))
            }
        }

        /// Create new struct from captured requests by level.
        fn captured_requests(&self, level: Level) -> Result<ApplyBlockRequest, failure::Error> {
            crate::samples::from_captured_bytes(&self.requests[to_index(level)])
        }
    }

    /// requests are indexed from 0, so [0] is level 1, [1] is level 2, and so on ...
    fn to_index(level: Level) -> usize {
        (level - 1).try_into().expect("Failed to convert level to usize")
    }

    fn to_level(idx: usize) -> Level {
        (idx + 1).try_into().expect("Failed to convert index to Level")
    }
}

/// Predefined data sets as callback functions for test node peer
mod test_cases_data {
    use std::{env, fs};
    use std::path::Path;
    use std::sync::Once;

    use lazy_static::lazy_static;
    use slog::{info, Logger};

    use tezos_api::ffi::PatchContext;
    use tezos_messages::p2p::encoding::block_header::Level;
    use tezos_messages::p2p::encoding::prelude::{BlockHeader, BlockHeaderBuilder, BlockHeaderMessage, CurrentBranch, CurrentBranchMessage, PeerMessage, PeerMessageResponse};

    use crate::test_data::Db;

    lazy_static! {
        // prepared data - we have stored 1326 request for apply block + operations for CARTHAGENET
        pub static ref DB_1326_CARTHAGENET: Db = Db::init_db(
            crate::samples::read_data_apply_block_request_until_1326(),
        );
    }

    fn init_data_db_1326_carthagenet(log: &Logger) -> &'static Db {
        static INIT_DATA: Once = Once::new();
        INIT_DATA.call_once(|| {
            info!(log, "Initializing test data 1326_carthagenet...");
            let _ = DB_1326_CARTHAGENET.block_hash(1);
            info!(log, "Test data 1326_carthagenet initialized!");
        });
        &DB_1326_CARTHAGENET
    }

    pub mod dont_serve_current_branch_messages {
        use slog::Logger;

        use tezos_messages::p2p::encoding::prelude::PeerMessageResponse;

        use crate::test_cases_data::{full_data, init_data_db_1326_carthagenet};
        use crate::test_data::Db;

        pub fn init_data(log: &Logger) -> &'static Db {
            init_data_db_1326_carthagenet(log)
        }

        pub fn serve_data(message: PeerMessageResponse) -> Result<Vec<PeerMessageResponse>, failure::Error> {
            full_data(message, None, &super::DB_1326_CARTHAGENET)
        }
    }

    pub mod current_branch_on_level_3 {
        use slog::Logger;

        use tezos_messages::p2p::encoding::prelude::PeerMessageResponse;

        use crate::test_cases_data::{full_data, init_data_db_1326_carthagenet};
        use crate::test_data::Db;

        pub fn init_data(log: &Logger) -> &'static Db {
            init_data_db_1326_carthagenet(log)
        }

        pub fn serve_data(message: PeerMessageResponse) -> Result<Vec<PeerMessageResponse>, failure::Error> {
            full_data(message, Some(3), &super::DB_1326_CARTHAGENET)
        }
    }

    pub mod sandbox_branch_1_level3 {
        use std::sync::Once;

        use lazy_static::lazy_static;
        use slog::{info, Logger};

        use tezos_api::environment::TezosEnvironment;
        use tezos_api::ffi::PatchContext;
        use tezos_messages::p2p::encoding::prelude::PeerMessageResponse;

        use crate::test_cases_data::full_data;
        use crate::test_data::Db;

        lazy_static! {
            pub static ref DB: Db = Db::init_db(
                crate::samples::read_data_zip("sandbox_branch_1_level3.zip", TezosEnvironment::Sandbox),
            );
        }

        pub fn init_data(log: &Logger) -> (&'static Db, Option<PatchContext>) {
            static INIT_DATA: Once = Once::new();
            INIT_DATA.call_once(|| {
                info!(log, "Initializing test data sandbox_branch_1_level3...");
                let _ = DB.block_hash(1);
                info!(log, "Test data sandbox_branch_1_level3 initialized!");
            });
            (&DB, Some(super::read_patch_context("sandbox-patch-context.json")))
        }

        pub fn serve_data(message: PeerMessageResponse) -> Result<Vec<PeerMessageResponse>, failure::Error> {
            full_data(message, Some(3), &DB)
        }
    }

    pub mod sandbox_branch_2_level4 {
        use std::sync::Once;

        use lazy_static::lazy_static;
        use slog::{info, Logger};

        use tezos_api::environment::TezosEnvironment;
        use tezos_api::ffi::PatchContext;
        use tezos_messages::p2p::encoding::prelude::PeerMessageResponse;

        use crate::test_cases_data::full_data;
        use crate::test_data::Db;

        lazy_static! {
            pub static ref DB: Db = Db::init_db(
                crate::samples::read_data_zip("sandbox_branch_2_level4.zip", TezosEnvironment::Sandbox),
            );
        }

        pub fn init_data(log: &Logger) -> (&'static Db, Option<PatchContext>) {
            static INIT_DATA: Once = Once::new();
            INIT_DATA.call_once(|| {
                info!(log, "Initializing test data sandbox_branch_2_level4...");
                let _ = DB.block_hash(1);
                info!(log, "Test data sandbox_branch_2_level4 initialized!");
            });
            (&DB, Some(super::read_patch_context("sandbox-patch-context.json")))
        }

        pub fn serve_data(message: PeerMessageResponse) -> Result<Vec<PeerMessageResponse>, failure::Error> {
            full_data(message, Some(4), &DB)
        }
    }

    fn read_patch_context(patch_context_json: &str) -> PatchContext {
        let path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("tests")
            .join("resources")
            .join(patch_context_json);
        match fs::read_to_string(path) {
            | Ok(content) => PatchContext {
                key: "sandbox_parameter".to_string(),
                json: content,
            },
            | Err(e) => panic!("Cannot read file, reason: {:?}", e)
        }
    }

    fn full_data(message: PeerMessageResponse, desired_current_branch_level: Option<Level>, db: &Db) -> Result<Vec<PeerMessageResponse>, failure::Error> {
        match message.messages().get(0).unwrap() {
            PeerMessage::GetCurrentBranch(request) => {
                match desired_current_branch_level {
                    Some(level) => {
                        let block_hash = db.block_hash(level)?;
                        if let Some(block_header) = db.get(&block_hash)? {
                            let current_branch = CurrentBranchMessage::new(
                                request.chain_id.clone(),
                                CurrentBranch::new(
                                    block_header.clone(),
                                    vec![
                                        block_hash,
                                        block_header.predecessor().clone(),
                                    ],
                                ),
                            );
                            Ok(vec![current_branch.into()])
                        } else {
                            Ok(vec![])
                        }
                    }
                    None => Ok(vec![])
                }
            }
            PeerMessage::GetBlockHeaders(request) => {
                let mut responses: Vec<PeerMessageResponse> = Vec::new();
                for block_hash in request.get_block_headers() {
                    if let Some(block_header) = db.get(block_hash)? {
                        let msg: BlockHeaderMessage = block_header.into();
                        responses.push(msg.into());
                    }
                }
                Ok(responses)
            }
            PeerMessage::GetOperationsForBlocks(request) => {
                let mut responses: Vec<PeerMessageResponse> = Vec::new();
                for block in request.get_operations_for_blocks() {
                    if let Some(msg) = db.get_operations_for_block(block)? {
                        responses.push(msg.into());
                    }
                }
                Ok(responses)
            }
            _ => Ok(vec![])
        }
    }

    pub fn hack_block_header_rewrite_protocol_data_insufficient_pow(block_header: BlockHeader) -> BlockHeader {
        let mut protocol_data: Vec<u8> = block_header.protocol_data().clone();

        // hack first 4-bytes
        (&mut protocol_data[0..4]).rotate_left(3);

        BlockHeaderBuilder::default()
            .level(block_header.level())
            .proto(block_header.proto())
            .predecessor(block_header.predecessor().clone())
            .timestamp(block_header.timestamp())
            .validation_pass(block_header.validation_pass())
            .operations_hash(block_header.operations_hash().clone())
            .fitness(block_header.fitness().clone())
            .context(block_header.context().clone())
            .protocol_data(protocol_data)
            .build()
            .unwrap()
    }

    pub fn hack_block_header_rewrite_protocol_data_bad_signature(block_header: BlockHeader) -> BlockHeader {
        let mut protocol_data: Vec<u8> = block_header.protocol_data().clone();

        // hack last 2-bytes
        let last_3_bytes_index = protocol_data.len() - 3;
        (&mut protocol_data[last_3_bytes_index..]).rotate_left(2);

        BlockHeaderBuilder::default()
            .level(block_header.level())
            .proto(block_header.proto())
            .predecessor(block_header.predecessor().clone())
            .timestamp(block_header.timestamp())
            .validation_pass(block_header.validation_pass())
            .operations_hash(block_header.operations_hash().clone())
            .fitness(block_header.fitness().clone())
            .context(block_header.context().clone())
            .protocol_data(protocol_data)
            .build()
            .unwrap()
    }
}

/// Test node peer, which simulates p2p remote peer, communicates through real p2p socket
mod test_node_peer {
    use std::net::{Shutdown, SocketAddr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, SystemTime};

    use futures::lock::Mutex;
    use slog::{crit, debug, error, info, Logger, warn};
    use tokio::net::TcpStream;
    use tokio::runtime::{Handle, Runtime};
    use tokio::time::timeout;

    use networking::p2p::peer;
    use networking::p2p::peer::{Bootstrap, BootstrapOutput, Local};
    use networking::p2p::stream::{EncryptedMessageReader, EncryptedMessageWriter};
    use tezos_identity::Identity;
    use tezos_messages::p2p::encoding::prelude::{PeerMessage, PeerMessageResponse};
    use tezos_messages::p2p::encoding::version::NetworkVersion;

    const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
    const READ_TIMEOUT_LONG: Duration = Duration::from_secs(30);

    pub struct TestNodePeer {
        pub identity: Identity,
        pub name: &'static str,
        log: Logger,
        connected: Arc<AtomicBool>,
        /// Tokio task executor
        tokio_executor: Handle,
        /// Message sender
        tx: Arc<Mutex<Option<EncryptedMessageWriter>>>,
    }

    impl TestNodePeer {
        pub fn connect(
            name: &'static str,
            connect_to_node_port: u16,
            network_version: NetworkVersion,
            identity: Identity,
            log: Logger,
            tokio_runtime: &Runtime,
            handle_message_callback: fn(PeerMessageResponse) -> Result<Vec<PeerMessageResponse>, failure::Error>) -> TestNodePeer {
            let server_address = format!("0.0.0.0:{}", connect_to_node_port).parse::<SocketAddr>().expect("Failed to parse server address");
            let tokio_executor = tokio_runtime.handle().clone();
            let connected = Arc::new(AtomicBool::new(false));
            let tx = Arc::new(Mutex::new(None));
            {
                let identity = identity.clone();
                let connected = connected.clone();
                let tx = tx.clone();
                let log = log.clone();
                tokio_executor.spawn(async move {
                    // init socket connection to server node
                    match timeout(CONNECT_TIMEOUT, TcpStream::connect(&server_address)).await {
                        Ok(Ok(stream)) => {
                            // authenticate
                            let local = Arc::new(Local::new(
                                1235,
                                identity.public_key,
                                identity.secret_key,
                                identity.proof_of_work_stamp,
                                network_version,
                            ));
                            let bootstrap = Bootstrap::outgoing(
                                stream,
                                server_address,
                                false,
                                false,
                            );

                            match peer::bootstrap(bootstrap, local, &log).await {
                                Ok(BootstrapOutput(rx, txw, ..)) => {
                                    info!(log, "[{}] Connection successful", name; "ip" => server_address);

                                    *tx.lock().await = Some(txw);
                                    connected.store(true, Ordering::Release);

                                    // process messages
                                    Self::begin_process_incoming(name, rx, tx, connected, log, server_address, handle_message_callback).await;
                                }
                                Err(e) => {
                                    error!(log, "[{}] Connection bootstrap failed", name; "ip" => server_address, "reason" => format!("{:?}", e));
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            error!(log, "[{}] Connection failed", name; "ip" => server_address, "reason" => format!("{:?}", e));
                        }
                        Err(_) => {
                            error!(log, "[{}] Connection timed out", name; "ip" => server_address);
                        }
                    }
                });
            }

            TestNodePeer {
                identity,
                name,
                log,
                connected,
                tokio_executor,
                tx,
            }
        }

        /// Start to process incoming data
        async fn begin_process_incoming(
            name: &str,
            mut rx: EncryptedMessageReader,
            tx: Arc<Mutex<Option<EncryptedMessageWriter>>>,
            connected: Arc<AtomicBool>,
            log: Logger,
            peer_address: SocketAddr,
            handle_message_callback: fn(PeerMessageResponse) -> Result<Vec<PeerMessageResponse>, failure::Error>) {
            info!(log, "[{}] Starting to accept messages", name; "ip" => format!("{:?}", &peer_address));

            while connected.load(Ordering::Acquire) {
                match timeout(READ_TIMEOUT_LONG, rx.read_message::<PeerMessageResponse>()).await {
                    Ok(res) => match res {
                        Ok(msg) => {
                            let msg_type = msg_type(&msg);
                            info!(log, "[{}] Handle message", name; "ip" => format!("{:?}", &peer_address), "msg_type" => msg_type.clone());

                            // apply callback
                            match handle_message_callback(msg) {
                                Ok(responses) => {
                                    info!(log, "[{}] Message handled({})", name, !responses.is_empty(); "msg_type" => msg_type);
                                    for response in responses {
                                        // send back response
                                        let mut tx_lock = tx.lock().await;
                                        if let Some(tx) = tx_lock.as_mut() {
                                            tx.write_message(&response).await.expect(&format!("[{}] Failed to send message", name));
                                            drop(tx_lock);
                                        }
                                    };
                                }
                                Err(e) => error!(log, "[{}] Failed to handle message", name; "reason" => format!("{:?}", e), "msg_type" => msg_type)
                            }
                        }
                        Err(e) => {
                            crit!(log, "[{}] Failed to read peer message", name; "reason" => e);
                            break;
                        }
                    }
                    Err(_) => {
                        warn!(log, "[{}] Peer message read timed out - lets next run", name; "secs" => READ_TIMEOUT_LONG.as_secs());
                    }
                }
            }

            debug!(log, "[{}] Shutting down peer connection", name; "ip" => format!("{:?}", &peer_address));
            if let Some(tx) = tx.lock().await.take() {
                let socket = rx.unsplit(tx);
                match socket.shutdown(Shutdown::Both) {
                    Ok(()) => debug!(log, "[{}] Connection shutdown successful", name; "socket" => format!("{:?}", socket)),
                    Err(err) => error!(log, "[{}] Failed to shutdown connection", name; "err" => format!("{:?}", err), "socket" => format!("{:?}", socket)),
                }
            }
            info!(log, "[{}] Stopped to accept messages", name; "ip" => format!("{:?}", &peer_address));
        }

        const IO_TIMEOUT: Duration = Duration::from_secs(6);

        pub fn send_msg<Msg: Into<PeerMessage>>(&mut self, msg: Msg) -> Result<(), failure::Error> {
            // need to at first wait for tx to be initialized in bootstrap
            if !self.connected.load(Ordering::Acquire) {
                assert!(self.wait_for_connection((Duration::from_secs(5), Duration::from_millis(100))).is_ok());
            }

            // lets send message to open tx channel
            let msg: PeerMessageResponse = msg.into().into();
            let tx = self.tx.clone();
            let name = self.name.to_string();
            let log = self.log.clone();
            self.tokio_executor.spawn(async move {
                let mut tx_lock = tx.lock().await;
                if let Some(tx) = tx_lock.as_mut() {
                    match timeout(Self::IO_TIMEOUT, tx.write_message(&msg)).await {
                        Ok(Ok(())) => (),
                        Ok(Err(e)) => error!(log, "[{}] write_message - failed", name; "reason" => format!("{:?}", e)),
                        Err(e) => error!(log, "[{}] write_message - connection timed out", name; "reason" => format!("{:?}", e)),
                    }
                }
                drop(tx_lock);
            });

            Ok(())
        }

        // TODO: refactor with async/condvar, not to block main thread
        pub fn wait_for_connection(&mut self, (timeout, delay): (Duration, Duration)) -> Result<(), failure::Error> {
            let start = SystemTime::now();

            let result = loop {
                if self.connected.load(Ordering::Acquire) {
                    break Ok(());
                }

                // kind of simple retry policy
                if start.elapsed()?.le(&timeout) {
                    std::thread::sleep(delay);
                } else {
                    break Err(failure::format_err!("[{}] wait_for_connection - something is wrong - timeout (timeout: {:?}, delay: {:?}) exceeded!", self.name, timeout, delay));
                }
            };
            result
        }

        pub fn stop(&mut self) {
            self.connected.store(false, Ordering::Release);
        }
    }

    impl Drop for TestNodePeer {
        fn drop(&mut self) {
            self.stop();
        }
    }

    fn msg_type(msg: &PeerMessageResponse) -> String {
        msg.messages()
            .iter()
            .map(|m| match m {
                PeerMessage::Disconnect => "Disconnect",
                PeerMessage::Advertise(_) => "Advertise",
                PeerMessage::SwapRequest(_) => "SwapRequest",
                PeerMessage::SwapAck(_) => "SwapAck",
                PeerMessage::Bootstrap => "Bootstrap",
                PeerMessage::GetCurrentBranch(_) => "GetCurrentBranch",
                PeerMessage::CurrentBranch(_) => "CurrentBranch",
                PeerMessage::Deactivate(_) => "Deactivate",
                PeerMessage::GetCurrentHead(_) => "GetCurrentHead",
                PeerMessage::CurrentHead(_) => "CurrentHead",
                PeerMessage::GetBlockHeaders(_) => "GetBlockHeaders",
                PeerMessage::BlockHeader(_) => "BlockHeader",
                PeerMessage::GetOperations(_) => "GetOperations",
                PeerMessage::Operation(_) => "Operation",
                PeerMessage::GetProtocols(_) => "GetProtocols",
                PeerMessage::Protocol(_) => "Protocol",
                PeerMessage::GetOperationHashesForBlocks(_) => "GetOperationHashesForBlocks",
                PeerMessage::OperationHashesForBlock(_) => "OperationHashesForBlock",
                PeerMessage::GetOperationsForBlocks(_) => "GetOperationsForBlocks",
                PeerMessage::OperationsForBlocks(_) => "OperationsForBlocks",
            })
            .collect::<Vec<&str>>()
            .join(",")
    }
}

mod test_actor {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, SystemTime};

    use riker::actors::*;
    use slog::warn;

    use crypto::hash::HashType;
    use networking::p2p::network_channel::{NetworkChannelMsg, NetworkChannelRef, NetworkChannelTopic, PeerBootstrapped};

    use crate::test_node_peer::TestNodePeer;

    #[actor(NetworkChannelMsg)]
    pub(crate) struct NetworkChannelListener {
        peers_mirror: Arc<RwLock<HashMap<String, String>>>,
        network_channel: NetworkChannelRef,
    }

    pub type NetworkChannelListenerRef = ActorRef<NetworkChannelListenerMsg>;

    impl Actor for NetworkChannelListener {
        type Msg = NetworkChannelListenerMsg;

        fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
            self.network_channel.tell(Subscribe {
                actor: Box::new(ctx.myself()),
                topic: NetworkChannelTopic::NetworkEvents.into(),
            }, ctx.myself().into());
        }

        fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Option<BasicActorRef>) {
            self.receive(ctx, msg, sender);
        }
    }

    impl ActorFactoryArgs<(NetworkChannelRef, Arc<RwLock<HashMap<String, String>>>)> for NetworkChannelListener {
        fn create_args((network_channel, peers_mirror): (NetworkChannelRef, Arc<RwLock<HashMap<String, String>>>)) -> Self {
            Self {
                network_channel,
                peers_mirror,
            }
        }
    }

    impl Receive<NetworkChannelMsg> for NetworkChannelListener {
        type Msg = NetworkChannelListenerMsg;

        fn receive(&mut self, ctx: &Context<Self::Msg>, msg: NetworkChannelMsg, _sender: Sender) {
            match self.process_shell_channel_message(ctx, msg) {
                Ok(_) => (),
                Err(e) => warn!(ctx.system.log(), "Failed to process shell channel message"; "reason" => format!("{:?}", e)),
            }
        }
    }

    impl NetworkChannelListener {
        pub fn name() -> &'static str { "network-channel-listener-actor" }

        pub fn actor(sys: &ActorSystem, network_channel: NetworkChannelRef, peers_mirror: Arc<RwLock<HashMap<String, String>>>) -> Result<NetworkChannelListenerRef, CreateError> {
            Ok(
                sys.actor_of_props::<NetworkChannelListener>(
                    Self::name(),
                    Props::new_args((network_channel, peers_mirror)),
                )?
            )
        }

        fn process_shell_channel_message(&mut self, _: &Context<NetworkChannelListenerMsg>, msg: NetworkChannelMsg) -> Result<(), failure::Error> {
            match msg {
                NetworkChannelMsg::PeerMessageReceived(_) => {}
                NetworkChannelMsg::PeerCreated(_) => {}
                NetworkChannelMsg::PeerBootstrapped(peer) => {
                    if let PeerBootstrapped::Success { peer_id, .. } = peer {
                        let peer_public_key = HashType::CryptoboxPublicKeyHash.bytes_to_string(peer_id.peer_public_key.as_ref());
                        self.peers_mirror
                            .write()
                            .unwrap()
                            .insert(peer_public_key, "CONNECTED".to_string());
                    }
                }
                NetworkChannelMsg::BlacklistPeer(..) => {}
                NetworkChannelMsg::PeerBlacklisted(peer_id) => {
                    let peer_public_key = HashType::CryptoboxPublicKeyHash.bytes_to_string(peer_id.peer_public_key.as_ref());
                    self.peers_mirror
                        .write()
                        .unwrap()
                        .insert(peer_public_key, "BLACKLISTED".to_string());
                }
                NetworkChannelMsg::PeerDisconnected(_) => {}
            }
            Ok(())
        }

        pub fn verify_connected(peer: &TestNodePeer, peers_mirror: Arc<RwLock<HashMap<String, String>>>) -> Result<(), failure::Error> {
            Self::verify_state("CONNECTED", peer, peers_mirror, (Duration::from_secs(5), Duration::from_millis(250)))
        }

        pub fn verify_blacklisted(peer: &TestNodePeer, peers_mirror: Arc<RwLock<HashMap<String, String>>>) -> Result<(), failure::Error> {
            Self::verify_state("BLACKLISTED", peer, peers_mirror, (Duration::from_secs(5), Duration::from_millis(250)))
        }

        // TODO: refactor with async/condvar, not to block main thread
        fn verify_state(expected_state: &str, peer: &TestNodePeer, peers_mirror: Arc<RwLock<HashMap<String, String>>>, (timeout, delay): (Duration, Duration)) -> Result<(), failure::Error> {
            let start = SystemTime::now();
            let peer_public_key = HashType::CryptoboxPublicKeyHash.bytes_to_string(&hex::decode(&peer.identity.public_key)?);

            let result = loop {
                let peers_mirror = peers_mirror.read().unwrap();
                if let Some(peer_state) = peers_mirror.get(&peer_public_key) {
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
                            peer.name, peer_public_key, expected_state, timeout, delay
                        )
                    );
                }
            };
            result
        }
    }
}