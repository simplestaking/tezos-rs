// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
use std::collections::{HashMap, HashSet};
use std::pin::Pin;

use anyhow::format_err;
use futures::task::{Context, Poll};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slog::{warn, Logger};
use tokio::time::{interval_at, Interval};
use tokio::time::{Duration, Instant};

use crypto::hash::{BlockHash, ChainId, ProtocolHash};
use shell::mempool::CurrentMempoolStateStorageRef;
use storage::{BlockHeaderWithHash, BlockMetaStorage, BlockMetaStorageReader, PersistentStorage};
use tezos_messages::ts_to_rfc3339;

use crate::rpc_actor::RpcCollectedStateRef;
use crate::services::mempool_services::get_pending_operations;

pub const MONITOR_TIMER_MILIS: u64 = 100;

/// Object containing information to recreate the block header shell information
#[derive(Serialize, Debug, Clone)]
struct BlockHeaderMonitorInfo {
    pub hash: String,
    pub level: i32,
    pub proto: u8,
    pub predecessor: String,
    pub timestamp: String,
    pub validation_pass: u8,
    pub operations_hash: String,
    pub fitness: Vec<String>,
    pub context: String,
    pub protocol_data: String,
}

impl From<&BlockHeaderWithHash> for BlockHeaderMonitorInfo {
    fn from(block: &BlockHeaderWithHash) -> Self {
        BlockHeaderMonitorInfo {
            hash: block.hash.to_base58_check(),
            level: block.header.level(),
            proto: block.header.proto(),
            predecessor: block.header.predecessor().to_base58_check(),
            timestamp: ts_to_rfc3339(block.header.timestamp()),
            validation_pass: block.header.validation_pass(),
            operations_hash: block.header.operations_hash().to_base58_check(),
            fitness: block
                .header
                .fitness()
                .iter()
                .map(|x| hex::encode(&x))
                .collect(),
            context: block.header.context().to_base58_check(),
            protocol_data: hex::encode(block.header.protocol_data()),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct MempoolOperationsQuery {
    pub applied: bool,
    pub refused: bool,
    pub branch_delayed: bool,
    pub branch_refused: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitoredOperation {
    signature: String,
    branch: String,
    contents: Value,

    #[serde(skip_deserializing)]
    protocol: Option<String>,
    #[serde(skip_serializing)]
    hash: String,
    #[serde(skip_serializing)]
    error: Option<Value>,
}

pub struct HeadMonitorStream {
    block_meta_storage: BlockMetaStorage,

    state: RpcCollectedStateRef,
    last_checked_head: Option<BlockHash>,
    delay: Option<Interval>,
    protocol: Option<ProtocolHash>,
}

pub struct OperationMonitorStream {
    chain_id: ChainId,
    current_mempool_state_storage: CurrentMempoolStateStorageRef,
    state: RpcCollectedStateRef,
    last_checked_head: BlockHash,
    log: Logger,
    delay: Option<Interval>,
    streamed_operations: Option<HashSet<String>>,
    query: MempoolOperationsQuery,
}

impl OperationMonitorStream {
    pub fn new(
        chain_id: ChainId,
        current_mempool_state_storage: CurrentMempoolStateStorageRef,
        state: RpcCollectedStateRef,
        log: Logger,
        last_checked_head: BlockHash,
        mempool_operaions_query: MempoolOperationsQuery,
    ) -> Self {
        Self {
            chain_id,
            current_mempool_state_storage,
            state,
            last_checked_head,
            log,
            delay: None,
            query: mempool_operaions_query,
            streamed_operations: None,
        }
    }

    fn yield_operations(&mut self) -> Poll<Option<Result<String, anyhow::Error>>> {
        let OperationMonitorStream {
            chain_id,
            current_mempool_state_storage,
            log,
            query,
            streamed_operations,
            ..
        } = self;

        let (mempool_operations, protocol_hash) = if let Ok((ops, protocol_hash)) =
            get_pending_operations(&chain_id, current_mempool_state_storage.clone())
        {
            (ops, protocol_hash)
        } else {
            return Poll::Pending;
        };
        let mut requested_ops: HashMap<String, Value> = HashMap::new();

        // fill in the resulting vector according to the querry
        if query.applied {
            let applied: HashMap<_, _> = mempool_operations
                .applied
                .into_iter()
                .map(|v| (v["hash"].to_string(), serde_json::to_value(v).unwrap()))
                .collect();
            requested_ops.extend(applied);
        }
        if query.branch_delayed {
            let branch_delayed: HashMap<_, _> = mempool_operations
                .branch_delayed
                .into_iter()
                .map(|v| (v["hash"].to_string(), v))
                .collect();
            requested_ops.extend(branch_delayed);
        }
        if query.branch_refused {
            let branch_refused: HashMap<_, _> = mempool_operations
                .branch_refused
                .into_iter()
                .map(|v| (v["hash"].to_string(), v))
                .collect();
            requested_ops.extend(branch_refused);
        }
        if query.refused {
            let refused: HashMap<_, _> = mempool_operations
                .refused
                .into_iter()
                .map(|v| (v["hash"].to_string(), v))
                .collect();
            requested_ops.extend(refused);
        }

        if let Some(streamed_operations) = streamed_operations {
            let to_yield: Vec<MonitoredOperation> = requested_ops
                .clone()
                .into_iter()
                .filter(|(k, _)| !streamed_operations.contains(k))
                .map(|(_, v)| {
                    let mut monitor_op: MonitoredOperation = serde_json::from_value(v).unwrap();
                    monitor_op.protocol = protocol_hash.as_ref().map(|ph| ph.to_base58_check());
                    monitor_op
                })
                .collect();

            for op_hash in requested_ops.keys() {
                streamed_operations.insert(op_hash.to_string());
            }

            if to_yield.is_empty() {
                Poll::Pending
            } else {
                let mut to_yield_string = serde_json::to_string(&to_yield)?;
                to_yield_string = to_yield_string.replace("\\", "");
                to_yield_string.push('\n');
                Poll::Ready(Some(Ok(to_yield_string)))
            }
        } else {
            // first poll, yield the operations in mempool, or an empty vector if mempool is empty
            let mut streamed_operations = HashSet::<String>::new();
            let to_yield: Vec<MonitoredOperation> = requested_ops
                .into_iter()
                .map(|(k, v)| {
                    streamed_operations.insert(k);
                    let mut monitor_op: MonitoredOperation = match serde_json::from_value(v) {
                        Ok(json_value) => json_value,
                        Err(e) => {
                            warn!(log, "Wont yield errored op: {}", e);
                            return Err(e);
                        }
                    };
                    monitor_op.protocol = protocol_hash.as_ref().map(|ph| ph.to_base58_check());
                    Ok(monitor_op)
                })
                .filter_map(Result::ok)
                .collect();

            self.streamed_operations = Some(streamed_operations);
            let mut to_yield_string = serde_json::to_string(&to_yield)?;
            to_yield_string = to_yield_string.replace("\\", "");
            to_yield_string.push('\n');
            Poll::Ready(Some(Ok(to_yield_string)))
        }
    }
}

impl HeadMonitorStream {
    pub fn new(
        state: RpcCollectedStateRef,
        protocol: Option<ProtocolHash>,
        persistent_storage: &PersistentStorage,
    ) -> Self {
        Self {
            state,
            protocol,
            last_checked_head: None,
            delay: None,
            block_meta_storage: BlockMetaStorage::new(persistent_storage),
        }
    }

    fn yield_head(
        &self,
        current_head: &BlockHeaderWithHash,
    ) -> Result<Option<String>, anyhow::Error> {
        let HeadMonitorStream { protocol, .. } = self;

        if let Some(protocol) = &protocol {
            let block_additional_data = match self
                .block_meta_storage
                .get_additional_data(&current_head.hash)?
            {
                Some(block_additional_data) => block_additional_data,
                None => {
                    return Err(format_err!(
                        "Missing block additional data for block_hash: {}",
                        current_head.hash.to_base58_check(),
                    ));
                }
            };

            if &block_additional_data.next_protocol_hash != protocol {
                return Ok(None);
            }
        }

        // serialize the struct to a json string to yield by the stream
        let mut head_string = serde_json::to_string(&BlockHeaderMonitorInfo::from(current_head))?;

        // push a newline character to the stream
        head_string.push('\n');

        Ok(Some(head_string))
    }
}

impl Stream for HeadMonitorStream {
    type Item = Result<String, anyhow::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<String, anyhow::Error>>> {
        // Note: the stream only ends on the client dropping the connection

        // create or get a delay future, that blocks for MONITOR_TIMER_MILIS
        let delay = self.delay.get_or_insert_with(|| {
            interval_at(Instant::now(), Duration::from_millis(MONITOR_TIMER_MILIS))
        });

        // poll the delay future
        match delay.poll_tick(cx) {
            Poll::Pending => Poll::Pending,
            _ => {
                // get rid of the used delay
                self.delay = None;

                let state = self.state.read().unwrap();
                let current_head = state.current_head().clone();

                // drop the immutable borrow so we can borrow self again as mutable
                // TODO: refactor this drop (remove if possible)
                drop(state);

                // if last_checked_head is None, this is the first poll, yield the current_head
                let last_checked_head = if let Some(head_hash) = &self.last_checked_head {
                    head_hash
                } else {
                    // first poll
                    if let Some(current_head) = current_head {
                        self.last_checked_head = Some(current_head.hash.clone());
                        // If there is no head with the desired protocol, [yield_head] returns Ok(None) which is transposed to None, meaning we
                        // would end the stream, in this case, we need to Pend.
                        if let Some(head_string_result) = self.yield_head(&current_head).transpose()
                        {
                            return Poll::Ready(Some(head_string_result));
                        } else {
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        };
                    } else {
                        // No current head found, storage not ready yet
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                };

                if let Some(current_head) = current_head {
                    if last_checked_head == &current_head.hash {
                        // current head not changed, yield nothing
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        // Head change, yield new head
                        self.last_checked_head = Some(current_head.hash.clone());
                        // If there is no head with the desired protocol, [yield_head] returns Ok(None) which is transposed to None, meaning we
                        // would end the stream, in this case, we need to Pend.
                        if let Some(head_string_result) = self.yield_head(&current_head).transpose()
                        {
                            Poll::Ready(Some(head_string_result))
                        } else {
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    }
                } else {
                    // No current head found, storage not ready yet, wait
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    }
}

impl Stream for OperationMonitorStream {
    type Item = Result<String, anyhow::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<String, anyhow::Error>>> {
        // create or get a delay future, that blocks for MONITOR_TIMER_MILIS
        let delay = self.delay.get_or_insert_with(|| {
            interval_at(Instant::now(), Duration::from_millis(MONITOR_TIMER_MILIS))
        });

        // poll the delay future
        match delay.poll_tick(cx) {
            Poll::Pending => Poll::Pending,
            _ => {
                // get rid of the used delay
                self.delay = None;

                let state = self.state.read().unwrap();
                let current_head = state.current_head().clone();

                // drop the immutable borrow so we can borrow self again as mutable
                // TODO: refactor this drop (remove if possible)
                drop(state);

                if let Some(current_head) = current_head {
                    if self.last_checked_head == current_head.hash {
                        // current head not changed, check for new operations
                        let yielded = self.yield_operations();
                        match yielded {
                            Poll::Pending => {
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                            _ => yielded,
                        }
                    } else {
                        // Head change, end stream
                        Poll::Ready(None)
                    }
                } else {
                    // No current head found, storage not ready yet
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    }
}

// TODO: add tests for both Streams!
