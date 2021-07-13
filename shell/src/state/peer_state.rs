// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::cmp;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use networking::p2p::network_channel::{NetworkChannelMsg, NetworkChannelRef, NetworkChannelTopic};
use riker::actors::*;

use crypto::hash::{BlockHash, OperationHash};
use networking::{PeerAddress, PeerId};
use storage::mempool_storage::MempoolOperationType;
use storage::BlockHeaderWithHash;
use tezos_messages::p2p::encoding::block_header::Level;
use tezos_messages::p2p::encoding::limits;
use tezos_messages::p2p::encoding::prelude::{
    GetOperationsMessage, MetadataMessage, PeerMessageResponse,
};

use crate::state::synchronization_state::UpdateIsBootstrapped;
use crate::state::StateError;

/// Limit to how many mempool operations to request in a batch
const MEMPOOL_OPERATIONS_BATCH_SIZE: usize = limits::MEMPOOL_MAX_OPERATIONS;

/// Holds information about a specific peer.
pub struct PeerState {
    /// PeerId identification (actor_ref + public key)
    pub(crate) peer_id: Arc<PeerId>,
    /// Has peer enabled mempool
    pub(crate) mempool_enabled: bool,
    /// Is bootstrapped flag
    pub(crate) is_bootstrapped: bool,

    /// Shareable data queues for scheduling of data download (blocks, operations)
    pub(crate) queues: Arc<DataQueues>,

    // TODO: TE-386 - global queue for requested operations
    /// Missing operations - we use this map for lazy/gracefull receiving
    pub(crate) missing_operations_for_blocks: HashMap<BlockHash, HashSet<i8>>,

    /// Level of the current head received from peer
    pub(crate) current_head_level: Option<i32>,
    /// Last time we received updated head from peer
    pub(crate) current_head_update_last: Instant,

    /// Last time we requested current head from the peer
    pub(crate) current_head_request_last: Instant,
    /// Last time we received current_head from the peer
    pub(crate) current_head_response_last: Instant,

    /// Last time we requested mempool operations from the peer
    pub(crate) mempool_operations_request_last: Instant,
    /// Last time we received mempool operations from the peer
    pub(crate) mempool_operations_response_last: Instant,

    /// Missing mempool operation hashes. Peer will be asked to provide operations for those hashes.
    /// After peer is asked for operation, this hash will be moved to `queued_mempool_operations`.
    pub(crate) missing_mempool_operations: Vec<(OperationHash, MempoolOperationType)>,
    /// Queued mempool operations. This map holds an operation hash and
    /// a tuple of type of a mempool operation with its time to live.
    pub(crate) queued_mempool_operations: HashMap<OperationHash, MempoolOperationType>,

    /// Collected stats about p2p messages
    pub(crate) message_stats: MessageStats,
}

impl PeerState {
    pub fn new(
        peer_id: Arc<PeerId>,
        peer_metadata: &MetadataMessage,
        limits: DataQueuesLimits,
    ) -> Self {
        PeerState {
            peer_id,
            mempool_enabled: !peer_metadata.disable_mempool(),
            is_bootstrapped: false,
            queues: Arc::new(DataQueues::new(limits)),
            missing_operations_for_blocks: HashMap::default(),
            missing_mempool_operations: Vec::new(),
            queued_mempool_operations: HashMap::default(),
            current_head_level: None,
            current_head_update_last: Instant::now(),
            current_head_request_last: Instant::now(),
            current_head_response_last: Instant::now(),
            mempool_operations_request_last: Instant::now(),
            mempool_operations_response_last: Instant::now(),
            message_stats: MessageStats::default(),
        }
    }

    fn available_mempool_operations_queue_capacity(&self) -> usize {
        let queued_count = self.queued_mempool_operations.len();
        if queued_count < MEMPOOL_OPERATIONS_BATCH_SIZE {
            MEMPOOL_OPERATIONS_BATCH_SIZE - queued_count
        } else {
            0
        }
    }

    /// Returns true, if was updated
    pub fn update_current_head(&mut self, block_header: &BlockHeaderWithHash) -> bool {
        // TODO: maybe fitness check?
        if self.current_head_level.is_none()
            || (block_header.header.level() >= self.current_head_level.unwrap())
        {
            self.current_head_level = Some(block_header.header.level());
            self.current_head_update_last = Instant::now();
            true
        } else {
            false
        }
    }

    /// Returns true, if was updated
    pub fn update_current_head_level(&mut self, new_level: Level) -> bool {
        // TODO: maybe fitness check?
        if self.current_head_level.is_none() || (self.current_head_level.unwrap() <= new_level) {
            self.current_head_level = Some(new_level);
            self.current_head_update_last = Instant::now();
            true
        } else {
            false
        }
    }

    pub fn clear(&mut self) {
        self.missing_mempool_operations.clear();
        // self.queued_block_headers.clear();
        // self.queued_block_operations.clear();
        self.queued_mempool_operations.clear();
        self.missing_operations_for_blocks.clear();
    }

    pub fn add_missing_mempool_operations(
        &mut self,
        operation_hash: OperationHash,
        mempool_type: MempoolOperationType,
    ) {
        if self
            .missing_mempool_operations
            .iter()
            .any(|(op_hash, _)| op_hash.eq(&operation_hash))
        {
            // ignore already scheduled
            return;
        }
        if self.queued_mempool_operations.contains_key(&operation_hash) {
            // ignore already scheduled
            return;
        }

        self.missing_mempool_operations
            .push((operation_hash, mempool_type));
    }

    pub fn schedule_missing_operations_for_mempool(
        network_channel: &NetworkChannelRef,
        peers: &mut HashMap<PeerAddress, PeerState>,
    ) {
        peers
            .values_mut()
            .filter(|peer| !peer.missing_mempool_operations.is_empty())
            .filter(|peer| peer.available_mempool_operations_queue_capacity() > 0)
            .for_each(|peer| {
                let num_opts_to_get = cmp::min(
                    peer.missing_mempool_operations.len(),
                    peer.available_mempool_operations_queue_capacity(),
                );
                let ops_to_enqueue = peer
                    .missing_mempool_operations
                    .drain(0..num_opts_to_get)
                    .collect::<Vec<_>>();

                ops_to_enqueue
                    .iter()
                    .cloned()
                    .for_each(|(op_hash, op_type)| {
                        peer.queued_mempool_operations.insert(op_hash, op_type);
                    });

                let ops_to_get: Vec<OperationHash> = ops_to_enqueue
                    .into_iter()
                    .map(|(op_hash, _)| op_hash)
                    .collect();

                peer.mempool_operations_request_last = Instant::now();

                if limits::GET_OPERATIONS_MAX_LENGTH > 0 {
                    ops_to_get
                        .chunks(limits::GET_OPERATIONS_MAX_LENGTH)
                        .for_each(|ops_to_get| {
                            tell_peer(
                                &network_channel,
                                peer,
                                GetOperationsMessage::new(ops_to_get.into()).into(),
                            )
                        });
                } else {
                    tell_peer(
                        &network_channel,
                        peer,
                        GetOperationsMessage::new(ops_to_get).into(),
                    );
                }
            });
    }
}

/// Hold stats about peer received messages
pub struct MessageStats {
    unexpected_response_block: usize,
    unexpected_response_operations: usize,
}

impl MessageStats {
    pub fn increment_unexpected_response_block(&mut self) {
        self.unexpected_response_block += 1;
    }

    pub fn increment_unexpected_response_operations(&mut self) {
        self.unexpected_response_operations += 1;
    }
}

impl Default for MessageStats {
    fn default() -> Self {
        Self {
            unexpected_response_block: 0,
            unexpected_response_operations: 0,
        }
    }
}

pub type MissingOperations = HashSet<i8>;
pub type BlockHeaderQueueRef = Arc<Mutex<HashMap<Arc<BlockHash>, Instant>>>;
pub type BlockOperationsQueueRef =
    Arc<Mutex<HashMap<Arc<BlockHash>, (MissingOperations, Instant)>>>;

#[derive(Clone, Debug)]
pub struct DataQueues {
    pub(crate) limits: DataQueuesLimits,

    /// Queued blocks shared with peer_branch_bootstrapper
    pub(crate) queued_block_headers: BlockHeaderQueueRef,

    /// Queued block operations
    pub(crate) queued_block_operations: BlockOperationsQueueRef,
}

impl DataQueues {
    pub fn new(limits: DataQueuesLimits) -> Self {
        Self {
            limits,
            queued_block_headers: Arc::new(Mutex::new(HashMap::default())),
            queued_block_operations: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    pub fn get_already_queued_block_headers_and_max_capacity(
        &self,
    ) -> Result<(HashSet<Arc<BlockHash>>, usize), StateError> {
        // lock, get queued and release
        let already_queued: HashSet<Arc<BlockHash>> =
            self.queued_block_headers.lock()?.keys().cloned().collect();

        let available =
            if already_queued.len() < self.limits.max_queued_block_headers_count as usize {
                (self.limits.max_queued_block_headers_count as usize) - already_queued.len()
            } else {
                0
            };

        Ok((already_queued, available))
    }

    pub fn get_already_queued_block_operations_and_max_capacity(
        &self,
    ) -> Result<(HashSet<Arc<BlockHash>>, usize), StateError> {
        // lock, get queued and release
        let already_queued: HashSet<Arc<BlockHash>> = self
            .queued_block_operations
            .lock()?
            .iter()
            .map(|(b, _)| b.clone())
            .collect();

        let available =
            if already_queued.len() < self.limits.max_queued_block_operations_count as usize {
                (self.limits.max_queued_block_operations_count as usize) - already_queued.len()
            } else {
                0
            };

        Ok((already_queued, available))
    }

    /// Returns tuple with block and duration how long is pending
    pub(crate) fn find_any_block_header_response_pending(
        &self,
        timeout: Duration,
    ) -> Result<Option<(Arc<BlockHash>, Duration)>, StateError> {
        Ok(self
            .queued_block_headers
            .lock()?
            .iter()
            .find(|(_, requested_time)| requested_time.elapsed().gt(&timeout))
            .map(|(block, requested_time)| (block.clone(), requested_time.elapsed())))
    }

    /// Returns tuple with block and duration how long is pending
    pub(crate) fn find_any_block_operations_response_pending(
        &self,
        timeout: Duration,
    ) -> Result<Option<(Arc<BlockHash>, Duration)>, StateError> {
        Ok(self
            .queued_block_operations
            .lock()?
            .iter()
            .find(|(_, (_, requested_time))| requested_time.elapsed().gt(&timeout))
            .map(|(block, (_, requested_time))| (block.clone(), requested_time.elapsed())))
    }
}

#[derive(Debug, Clone)]
pub struct DataQueuesLimits {
    /// Limit to how many blocks to request from peer
    /// Note: This limits speed of downloading chunked history
    pub(crate) max_queued_block_headers_count: u16,
    /// Limit to how many block operations to request from peer
    pub(crate) max_queued_block_operations_count: u16,
}

impl UpdateIsBootstrapped for PeerState {
    fn set_is_bootstrapped(&mut self, new_status: bool) {
        self.is_bootstrapped = new_status;
    }

    fn is_bootstrapped(&self) -> bool {
        self.is_bootstrapped
    }
}

pub fn tell_peer(
    network_channel: &NetworkChannelRef,
    peer: &PeerState,
    msg: Arc<PeerMessageResponse>,
) {
    network_channel.tell(
        Publish {
            msg: NetworkChannelMsg::SendMessage(peer.peer_id.clone(), msg),
            topic: NetworkChannelTopic::NetworkCommands.into(),
        },
        None,
    );
}
