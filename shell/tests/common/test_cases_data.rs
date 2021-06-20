// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Predefined data sets as callback functions for test node peer

use std::path::Path;
use std::sync::Once;
use std::{env, fs};

use lazy_static::lazy_static;
use slog::{info, Logger};

use tezos_api::ffi::PatchContext;
use tezos_messages::p2p::encoding::block_header::Level;
use tezos_messages::p2p::encoding::prelude::{
    BlockHeader, BlockHeaderBuilder, BlockHeaderMessage, CurrentBranch, CurrentBranchMessage,
    OperationMessage, PeerMessage, PeerMessageResponse,
};

use crate::common::test_data::Db;

lazy_static! {
    // prepared data - we have stored 1326 request for apply block + operations for CARTHAGENET
    pub static ref DB_1326_CARTHAGENET: Db = Db::init_db(
        crate::common::samples::read_data_apply_block_request_until_1326(),
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

    // use catcommon::test_cases_data::{full_data, init_data_db_1326_carthagenet};
    use super::*;
    use crate::common::test_data::Db;

    pub fn init_data(log: &Logger) -> &'static Db {
        init_data_db_1326_carthagenet(log)
    }

    pub fn serve_data(
        message: PeerMessageResponse,
    ) -> Result<Vec<PeerMessageResponse>, anyhow::Error> {
        full_data(message, None, &super::DB_1326_CARTHAGENET)
    }
}

pub mod current_branch_on_level_3 {
    use slog::Logger;

    use tezos_messages::p2p::encoding::prelude::PeerMessageResponse;

    use super::*;
    use crate::common::test_data::Db;

    pub fn init_data(log: &Logger) -> &'static Db {
        init_data_db_1326_carthagenet(log)
    }

    pub fn serve_data(
        message: PeerMessageResponse,
    ) -> Result<Vec<PeerMessageResponse>, anyhow::Error> {
        full_data(message, Some(3), &super::DB_1326_CARTHAGENET)
    }
}

pub mod current_branch_on_level_1324 {
    use slog::Logger;

    use tezos_messages::p2p::encoding::prelude::PeerMessageResponse;

    use super::*;
    use crate::common::test_data::Db;

    pub fn init_data(log: &Logger) -> &'static Db {
        init_data_db_1326_carthagenet(log)
    }

    pub fn serve_data(
        message: PeerMessageResponse,
    ) -> Result<Vec<PeerMessageResponse>, anyhow::Error> {
        full_data(message, Some(1324), &super::DB_1326_CARTHAGENET)
    }
}

pub mod sandbox_branch_1_level3 {
    use std::sync::Once;

    use lazy_static::lazy_static;
    use slog::{info, Logger};

    use tezos_api::environment::TezosEnvironment;
    use tezos_api::ffi::PatchContext;
    use tezos_messages::p2p::encoding::prelude::PeerMessageResponse;

    use crate::common::samples::read_data_zip;
    use crate::common::test_data::Db;

    lazy_static! {
        pub static ref DB: Db = Db::init_db(read_data_zip(
            "sandbox_branch_1_level3.zip",
            TezosEnvironment::Sandbox
        ),);
    }

    pub fn init_data(log: &Logger) -> (&'static Db, Option<PatchContext>) {
        static INIT_DATA: Once = Once::new();
        INIT_DATA.call_once(|| {
            info!(log, "Initializing test data sandbox_branch_1_level3...");
            let _ = DB.block_hash(1);
            info!(log, "Test data sandbox_branch_1_level3 initialized!");
        });
        (
            &DB,
            Some(super::read_patch_context("sandbox-patch-context.json")),
        )
    }

    pub fn serve_data(
        message: PeerMessageResponse,
    ) -> Result<Vec<PeerMessageResponse>, anyhow::Error> {
        super::full_data(message, Some(3), &DB)
    }
}

pub mod sandbox_branch_1_no_level {
    use std::sync::Once;

    use lazy_static::lazy_static;
    use slog::{info, Logger};

    use tezos_api::environment::TezosEnvironment;
    use tezos_api::ffi::PatchContext;

    use crate::common::samples::read_data_zip;
    use crate::common::test_data::Db;

    lazy_static! {
        pub static ref DB: Db = Db::init_db(read_data_zip(
            "sandbox_branch_1_level3.zip",
            TezosEnvironment::Sandbox
        ),);
    }

    pub fn init_data(log: &Logger) -> (&'static Db, Option<PatchContext>) {
        static INIT_DATA: Once = Once::new();
        INIT_DATA.call_once(|| {
            info!(log, "Initializing test data sandbox_branch_1_no_level...");
            let _ = DB.block_hash(1);
            info!(log, "Test data sandbox_branch_1_no_level initialized!");
        });
        (
            &DB,
            Some(super::read_patch_context("sandbox-patch-context.json")),
        )
    }
}

pub mod sandbox_branch_2_level4 {
    use std::sync::Once;

    use lazy_static::lazy_static;
    use slog::{info, Logger};

    use tezos_api::environment::TezosEnvironment;
    use tezos_api::ffi::PatchContext;
    use tezos_messages::p2p::encoding::prelude::PeerMessageResponse;

    use super::*;
    use crate::common::test_data::Db;

    lazy_static! {
        pub static ref DB: Db = Db::init_db(crate::common::samples::read_data_zip(
            "sandbox_branch_2_level4.zip",
            TezosEnvironment::Sandbox
        ),);
    }

    pub fn init_data(log: &Logger) -> (&'static Db, Option<PatchContext>) {
        static INIT_DATA: Once = Once::new();
        INIT_DATA.call_once(|| {
            info!(log, "Initializing test data sandbox_branch_2_level4...");
            let _ = DB.block_hash(1);
            info!(log, "Test data sandbox_branch_2_level4 initialized!");
        });
        (
            &DB,
            Some(super::read_patch_context("sandbox-patch-context.json")),
        )
    }

    pub fn serve_data(
        message: PeerMessageResponse,
    ) -> Result<Vec<PeerMessageResponse>, anyhow::Error> {
        full_data(message, Some(4), &DB)
    }
}

fn read_patch_context(patch_context_json: &str) -> PatchContext {
    let path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("tests")
        .join("resources")
        .join(patch_context_json);
    match fs::read_to_string(path) {
        Ok(content) => PatchContext {
            key: "sandbox_parameter".to_string(),
            json: content,
        },
        Err(e) => panic!("Cannot read file, reason: {:?}", e),
    }
}

fn full_data(
    message: PeerMessageResponse,
    desired_current_branch_level: Option<Level>,
    db: &Db,
) -> Result<Vec<PeerMessageResponse>, anyhow::Error> {
    match message.message() {
        PeerMessage::GetCurrentBranch(request) => match desired_current_branch_level {
            Some(level) => {
                let block_hash = db.block_hash(level)?;
                if let Some(block_header) = db.get(&block_hash)? {
                    let current_branch = CurrentBranchMessage::new(
                        request.chain_id.clone(),
                        CurrentBranch::new(
                            block_header.clone(),
                            vec![block_hash, block_header.predecessor().clone()],
                        ),
                    );
                    Ok(vec![current_branch.into()])
                } else {
                    Ok(vec![])
                }
            }
            None => Ok(vec![]),
        },
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
        PeerMessage::GetOperations(request) => {
            let mut responses: Vec<PeerMessageResponse> = Vec::new();
            for operation_hash in request.get_operations() {
                if let Some(operation) = db.get_operation(operation_hash)? {
                    responses.push(OperationMessage::from(operation).into());
                }
            }
            Ok(responses)
        }
        _ => Ok(vec![]),
    }
}

pub fn hack_block_header_rewrite_protocol_data_insufficient_pow(
    block_header: BlockHeader,
) -> BlockHeader {
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

pub fn hack_block_header_rewrite_protocol_data_bad_signature(
    block_header: BlockHeader,
) -> BlockHeader {
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
