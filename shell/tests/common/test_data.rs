// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Stored first cca first 1300 apply block data

use std::collections::HashMap;
use std::convert::TryInto;

use anyhow::format_err;

use crypto::hash::{BlockHash, ContextHash, OperationHash};
use tezos_api::environment::TezosEnvironment;
use tezos_api::ffi::ApplyBlockRequest;
use tezos_messages::p2p::binary_message::MessageHash;
use tezos_messages::p2p::encoding::block_header::Level;
use tezos_messages::p2p::encoding::prelude::{
    BlockHeader, Operation, OperationsForBlock, OperationsForBlocksMessage,
};

use crate::common::samples::OperationsForBlocksMessageKey;

pub struct Db {
    pub tezos_env: TezosEnvironment,
    requests: Vec<String>,
    headers: HashMap<BlockHash, (Level, ContextHash)>,
    operations: HashMap<OperationsForBlocksMessageKey, OperationsForBlocksMessage>,
    operation_hashes: HashMap<OperationHash, Level>,
}

impl Db {
    pub(crate) fn init_db(
        (requests, operations, tezos_env): (
            Vec<String>,
            HashMap<OperationsForBlocksMessageKey, OperationsForBlocksMessage>,
            TezosEnvironment,
        ),
    ) -> Db {
        let mut headers: HashMap<BlockHash, (Level, ContextHash)> = HashMap::new();
        let mut operation_hashes: HashMap<OperationHash, Level> = HashMap::new();

        // init headers
        for (idx, request) in requests.iter().enumerate() {
            let level = to_level(idx);
            let request = crate::common::samples::from_captured_bytes(request)
                .expect("Failed to parse request");

            let block = request
                .block_header
                .message_typed_hash()
                .expect("Failed to decode message_hash");
            let context_hash: ContextHash = request.block_header.context().clone();
            headers.insert(block, (level, context_hash));

            for ops in request.operations {
                for op in ops {
                    operation_hashes.insert(
                        op.message_typed_hash()
                            .expect("Failed to compute message hash"),
                        level,
                    );
                }
            }
        }

        Db {
            tezos_env,
            requests,
            headers,
            operations,
            operation_hashes,
        }
    }

    pub fn get(&self, block_hash: &BlockHash) -> Result<Option<BlockHeader>, anyhow::Error> {
        match self.headers.get(block_hash) {
            Some((level, _)) => Ok(Some(self.captured_requests(*level)?.block_header)),
            None => Ok(None),
        }
    }

    pub fn get_operation(
        &self,
        operation_hash: &OperationHash,
    ) -> Result<Option<Operation>, anyhow::Error> {
        match self.operation_hashes.get(operation_hash) {
            Some(level) => {
                let mut found = None;
                for ops in self.captured_requests(*level)?.operations {
                    for op in ops {
                        if op.message_typed_hash::<OperationHash>()?.eq(operation_hash) {
                            found = Some(op);
                            break;
                        }
                    }
                }
                Ok(found)
            }
            None => Ok(None),
        }
    }

    pub fn get_operations(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Vec<Vec<Operation>>, anyhow::Error> {
        match self.headers.get(block_hash) {
            Some((level, _)) => Ok(self.captured_requests(*level)?.operations),
            None => Ok(vec![]),
        }
    }

    pub fn get_operations_for_block(
        &self,
        block: &OperationsForBlock,
    ) -> Result<Option<OperationsForBlocksMessage>, anyhow::Error> {
        match self.operations.get(&OperationsForBlocksMessageKey::new(
            block.block_hash().clone(),
            block.validation_pass(),
        )) {
            Some(operations) => Ok(Some(operations.clone())),
            None => Ok(None),
        }
    }

    pub fn block_hash(&self, searched_level: Level) -> Result<BlockHash, anyhow::Error> {
        let block_hash = self
            .headers
            .iter()
            .find(|(_, (level, _))| searched_level.eq(level))
            .map(|(k, _)| k.clone());
        match block_hash {
            Some(block_hash) => Ok(block_hash),
            None => Err(format_err!(
                "No block_hash found for level: {}",
                searched_level
            )),
        }
    }

    pub fn block_header(&self, searched_level: Level) -> Result<BlockHeader, anyhow::Error> {
        match self.get(&self.block_hash(searched_level)?)? {
            Some(header) => Ok(header),
            None => Err(format_err!(
                "No block_header found for level: {}",
                searched_level
            )),
        }
    }

    pub fn context_hash(&self, searched_level: Level) -> Result<ContextHash, anyhow::Error> {
        let context_hash = self
            .headers
            .iter()
            .find(|(_, (level, _))| searched_level.eq(level))
            .map(|(_, (_, context_hash))| context_hash.clone());
        match context_hash {
            Some(context_hash) => Ok(context_hash),
            None => Err(format_err!("No header found for level: {}", searched_level)),
        }
    }

    /// Create new struct from captured requests by level.
    fn captured_requests(&self, level: Level) -> Result<ApplyBlockRequest, anyhow::Error> {
        crate::common::samples::from_captured_bytes(&self.requests[to_index(level)])
    }
}

/// requests are indexed from 0, so [0] is level 1, [1] is level 2, and so on ...
fn to_index(level: Level) -> usize {
    (level - 1)
        .try_into()
        .expect("Failed to convert level to usize")
}

fn to_level(idx: usize) -> Level {
    (idx + 1)
        .try_into()
        .expect("Failed to convert index to Level")
}
