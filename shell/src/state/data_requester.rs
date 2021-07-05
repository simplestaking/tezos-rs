// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! We need to fetch different data from p2p or send data to protocol validation
//! Main purpose of this module is to synchronize this request/responses per peers and handle queues management for peer
//!
//! We dont handle unique requests accross different peers, but if we want to, we just need to add here some synchronization.
//! Now we just handle unique requests per peer.

use std::sync::Arc;
use std::time::Instant;

use riker::actors::*;
use slog::{warn, Logger};

use crypto::hash::{BlockHash, ChainId};
use networking::p2p::peer::SendMessage;
use networking::PeerId;
use storage::{BlockMetaStorage, BlockMetaStorageReader, OperationsMetaStorage};
use tezos_messages::p2p::encoding::limits;
use tezos_messages::p2p::encoding::prelude::{
    GetBlockHeadersMessage, GetOperationsForBlocksMessage, OperationsForBlock, PeerMessageResponse,
};

use crate::chain_feeder::{ApplyBlock, ChainFeederRef, ScheduleApplyBlock};
use crate::peer_branch_bootstrapper::PeerBranchBootstrapperRef;
use crate::shell_channel::InjectBlockOneshotResultCallback;
use crate::state::peer_state::{
    BlockHeaderQueueRef, BlockOperationsQueueRef, DataQueues, MissingOperations, PeerState,
};
use crate::state::{ApplyBlockBatch, StateError};
use crate::validation;
use crate::validation::CanApplyStatus;

/// Shareable ref between threads
pub type DataRequesterRef = Arc<DataRequester>;

/// Requester manages global request/response queues for data
/// and also manages local queues for every peer.
pub struct DataRequester {
    pub(crate) block_meta_storage: BlockMetaStorage,
    pub(crate) operations_meta_storage: OperationsMetaStorage,

    /// Chain feeder - actor, which is responsible to apply_block to context
    block_applier: ChainFeederRef,
}

impl DataRequester {
    pub fn new(
        block_meta_storage: BlockMetaStorage,
        operations_meta_storage: OperationsMetaStorage,
        block_applier: ChainFeederRef,
    ) -> Self {
        Self {
            block_meta_storage,
            operations_meta_storage,
            block_applier,
        }
    }

    /// Tries to schedule blocks downloading from peer
    ///
    /// Returns true if was scheduled and p2p message was sent
    pub fn fetch_block_headers(
        &self,
        mut blocks_to_download: Vec<Arc<BlockHash>>,
        peer: &PeerId,
        peer_queues: &DataQueues,
    ) -> Result<bool, StateError> {
        // check if empty
        if blocks_to_download.is_empty() {
            return Ok(false);
        }

        // get queue locks
        let mut peer_queued_block_headers = peer_queues.queued_block_headers.lock()?;

        // calculate available capacity
        let available_capacity = if peer_queued_block_headers.len()
            < peer_queues.limits.max_queued_block_headers_count as usize
        {
            (peer_queues.limits.max_queued_block_headers_count as usize)
                - peer_queued_block_headers.len()
        } else {
            // full queue, we cannot schedule more
            return Ok(false);
        };

        // fillter non-queued and trim to queue capacity
        blocks_to_download.retain(|block_hash| {
            if peer_queued_block_headers.contains_key(block_hash.as_ref()) {
                // already scheduled, so skip
                false
            } else {
                // TODO: change this do contains, when fixed
                let already_downloaded =
                    if let Ok(Some(metadata)) = self.block_meta_storage.get(block_hash) {
                        metadata.is_downloaded()
                    } else {
                        false
                    };
                !already_downloaded
            }
        });
        blocks_to_download.truncate(available_capacity);

        // if empty finish
        if blocks_to_download.is_empty() {
            return Ok(false);
        }

        // add to queue
        blocks_to_download.iter().cloned().for_each(|btd| {
            peer_queued_block_headers.insert(btd, Instant::now());
        });

        // release lock
        drop(peer_queued_block_headers);

        // send p2p msg - now we can fire msg to peer
        if limits::GET_BLOCK_HEADERS_MAX_LENGTH > 0 {
            blocks_to_download
                .chunks(limits::GET_BLOCK_HEADERS_MAX_LENGTH)
                .for_each(|blocks_to_download| {
                    tell_peer(
                        GetBlockHeadersMessage::new(
                            blocks_to_download
                                .iter()
                                .map(|b| b.as_ref().clone())
                                .collect::<Vec<BlockHash>>(),
                        )
                        .into(),
                        peer,
                    );
                });
        } else {
            tell_peer(
                GetBlockHeadersMessage::new(
                    blocks_to_download
                        .iter()
                        .map(|b| b.as_ref().clone())
                        .collect::<Vec<BlockHash>>(),
                )
                .into(),
                peer,
            );
        }

        Ok(true)
    }

    /// Tries to schedule blocks operations downloading from peer
    ///
    /// Returns true if was scheduled and p2p message was sent
    pub fn fetch_block_operations<SC: FnMut(Arc<BlockHash>)>(
        &self,
        mut blocks_to_download: Vec<Arc<BlockHash>>,
        peer: &PeerId,
        peer_queues: &DataQueues,
        mut on_operations_already_downloaded: SC,
    ) -> Result<bool, StateError> {
        // check if empty
        if blocks_to_download.is_empty() {
            return Ok(false);
        }

        // get queue locks
        let mut peer_queued_block_headers = peer_queues.queued_block_operations.lock()?;

        // calculate available capacity
        let available_capacity = if peer_queued_block_headers.len()
            < peer_queues.limits.max_queued_block_operations_count as usize
        {
            (peer_queues.limits.max_queued_block_operations_count as usize)
                - peer_queued_block_headers.len()
        } else {
            // full queue, we cannot schedule more
            return Ok(false);
        };

        // fillter non-queued and trim to queue capacity
        blocks_to_download
            .retain(|block_hash| !peer_queued_block_headers.contains_key(block_hash.as_ref()));
        blocks_to_download.truncate(available_capacity);

        // collect missing validation_passes
        let blocks_to_download: Vec<(Arc<BlockHash>, MissingOperations)> = blocks_to_download
            .into_iter()
            .filter_map(|b| {
                if let Ok(Some(metadata)) = self.operations_meta_storage.get(&b) {
                    if let Some(missing_operations) = metadata.get_missing_validation_passes() {
                        if !missing_operations.is_empty() {
                            Some((b, missing_operations.iter().map(|vp| *vp as i8).collect()))
                        } else {
                            on_operations_already_downloaded(b);
                            None
                        }
                    } else {
                        on_operations_already_downloaded(b);
                        None
                    }
                } else {
                    // this can happen, between calls in chain_state.rs:
                    // block_storage.put_block_header
                    // ..
                    // self.process_block_header_operations
                    //
                    // we just wait for a next scheduling run
                    None
                }
            })
            .collect();

        // if empty just finish
        if blocks_to_download.is_empty() {
            return Ok(false);
        }

        // add to queue
        blocks_to_download
            .iter()
            .cloned()
            .for_each(|(block, missing_operations)| {
                let _ =
                    peer_queued_block_headers.insert(block, (missing_operations, Instant::now()));
            });

        // release lock
        drop(peer_queued_block_headers);

        // send p2p msg - now we can fire msg to peer
        if limits::GET_OPERATIONS_FOR_BLOCKS_MAX_LENGTH > 0 {
            let blocks_to_download: Vec<OperationsForBlock> = blocks_to_download
                .into_iter()
                .flat_map(|(block, missing_ops)| {
                    missing_ops
                        .into_iter()
                        .map(move |vp| OperationsForBlock::new(block.as_ref().clone(), vp))
                })
                .collect();

            blocks_to_download
                .chunks(limits::GET_OPERATIONS_FOR_BLOCKS_MAX_LENGTH)
                .for_each(|blocks_to_download| {
                    tell_peer(
                        GetOperationsForBlocksMessage::new(blocks_to_download.into()).into(),
                        peer,
                    );
                });
        } else {
            tell_peer(
                GetOperationsForBlocksMessage::new(
                    blocks_to_download
                        .into_iter()
                        .flat_map(|(block, missing_ops)| {
                            missing_ops
                                .into_iter()
                                .map(move |vp| OperationsForBlock::new(block.as_ref().clone(), vp))
                        })
                        .collect(),
                )
                .into(),
                peer,
            );
        }

        Ok(true)
    }

    /// Handle received block.
    ///
    /// Returns:
    ///     None - if was not scheduled for peer => unexpected block
    ///     Some(lock) - if was scheduled, lock is released, when comes out of the scope
    pub fn block_header_received(
        &self,
        block_hash: &BlockHash,
        peer: &mut PeerState,
        log: &Logger,
    ) -> Result<Option<RequestedBlockDataLock>, StateError> {
        // if was not scheduled, just return None
        if !peer
            .queues
            .queued_block_headers
            .lock()?
            .contains_key(block_hash)
        {
            warn!(log, "Received unexpected block header from peer"; "block_header_hash" => block_hash.to_base58_check());
            peer.message_stats.increment_unexpected_response_block();
            return Ok(None);
        }

        // if contains, return data lock, when this lock will go out if the scope, then drop will be triggered, and queues will be emptied
        Ok(Some(RequestedBlockDataLock {
            block_hash: Arc::new(block_hash.clone()),
            queued_block_headers: peer.queues.queued_block_headers.clone(),
        }))
    }

    /// Handle received block operations which we requested.
    ///
    /// Returns:
    ///     None - if was not scheduled for peer => unexpected block/operations
    ///     Some(lock) - if was scheduled, lock is released, when comes out of the scope
    pub fn block_operations_received(
        &self,
        operations_for_block: &OperationsForBlock,
        peer: &mut PeerState,
        log: &Logger,
    ) -> Result<Option<RequestedOperationDataLock>, StateError> {
        let block_hash = operations_for_block.block_hash();
        let validation_pass = operations_for_block.validation_pass();

        // if was not scheduled, just return None
        match peer
            .queues
            .queued_block_operations
            .lock()?
            .get_mut(block_hash)
        {
            Some((missing_operations, _)) => {
                if !missing_operations.contains(&validation_pass) {
                    warn!(log, "Received unexpected block header operation's validation pass from peer"; "block_header_hash" => block_hash.to_base58_check(), "validation_pass" => validation_pass);
                    peer.message_stats
                        .increment_unexpected_response_operations();
                    return Ok(None);
                }
            }
            None => {
                warn!(log, "Received unexpected block header operation from peer"; "block_header_hash" => block_hash.to_base58_check(), "validation_pass" => validation_pass);
                peer.message_stats
                    .increment_unexpected_response_operations();
                return Ok(None);
            }
        }

        // if contains, return data lock, when this lock will go out if the scope, then drop will be triggered, and queues will be emptied
        Ok(Some(RequestedOperationDataLock {
            validation_pass,
            block_hash: Arc::new(block_hash.clone()),
            queued_block_operations: peer.queues.queued_block_operations.clone(),
        }))
    }

    pub fn try_apply_block(
        &self,
        chain_id: Arc<ChainId>,
        block_hash: BlockHash,
        result_callback: Option<InjectBlockOneshotResultCallback>,
    ) -> Result<(), StateError> {
        self.call_apply_block(chain_id, ApplyBlockBatch::one(block_hash), result_callback)
    }

    fn call_apply_block(
        &self,
        chain_id: Arc<ChainId>,
        batch: ApplyBlockBatch,
        result_callback: Option<InjectBlockOneshotResultCallback>,
    ) -> Result<(), StateError> {
        // check batch, if the start block is ok and can be applied
        // if start is already applied, we fold the bath to next block (if any)
        let batch = {
            let mut batch_to_use = batch;
            loop {
                // get block metadata
                let block_metadata =
                    match self.block_meta_storage.get(&batch_to_use.block_to_apply)? {
                        Some(block_metadata) => block_metadata,
                        None => {
                            return Err(StateError::ProcessingError {
                                reason: format!(
                                    "No metadata found for block_hash: {}",
                                    batch_to_use.block_to_apply.to_base58_check()
                                ),
                            });
                        }
                    };

                // check if can be applied
                match validation::can_apply_block(
                    (&batch_to_use.block_to_apply, &block_metadata),
                    |bh| self.operations_meta_storage.is_complete(bh),
                    |predecessor| self.block_meta_storage.is_applied(predecessor),
                )? {
                    CanApplyStatus::Ready => break batch_to_use,
                    CanApplyStatus::AlreadyApplied => {
                        // if we dont have successors, we need to finished,
                        // if we have, we can shift the batch and try again
                        batch_to_use = match batch_to_use.shift() {
                            Some(shifted_batch) => shifted_batch,
                            None => return Err(StateError::ProcessingError {
                                reason: "Block cannot be applied, because the whole batch is already applied".to_string(),
                            })
                        };
                    }
                    CanApplyStatus::MissingPredecessor => {
                        return Err(StateError::ProcessingError {
                            reason: format!(
                                "Block {} cannot be applied because missing predecessor block",
                                batch_to_use.block_to_apply.to_base58_check()
                            ),
                        })
                    }
                    CanApplyStatus::PredecessorNotApplied => {
                        return Err(StateError::ProcessingError {
                            reason: format!(
                                "Block {} cannot be applied because predecessor block is not applied yet",
                                batch_to_use.block_to_apply.to_base58_check()
                            ),
                        })
                    }
                    CanApplyStatus::MissingOperations => {
                        return Err(StateError::ProcessingError {
                            reason: format!(
                                "Block {} cannot be applied because missing operations",
                                batch_to_use.block_to_apply.to_base58_check()
                            ),
                        })
                    }
                }
            }
        };

        // try to call apply
        self.block_applier.tell(
            ApplyBlock::new(chain_id, batch, result_callback, None, None),
            None,
        );

        Ok(())
    }

    pub fn call_schedule_apply_block(
        &self,
        chain_id: Arc<ChainId>,
        batch: ApplyBlockBatch,
        bootstrapper: Option<PeerBranchBootstrapperRef>,
    ) {
        // try to call apply
        self.block_applier
            .tell(ScheduleApplyBlock::new(chain_id, batch, bootstrapper), None);
    }
}

/// Simple lock, when we want to remove data from queues,
/// but make sure that it was handled, and nobody will put the same data to queue, while we are handling them
///
/// When this lock goes out of the scope, then queues will be clear for block_hash
#[derive(Debug)]
pub struct RequestedBlockDataLock {
    pub block_hash: Arc<BlockHash>,
    queued_block_headers: BlockHeaderQueueRef,
}

impl Drop for RequestedBlockDataLock {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.queued_block_headers.lock() {
            queue.remove(&self.block_hash);
        }
    }
}

/// Simple lock, when we want to remove data from queues,
/// but make sure that it was handled, and nobody will put the same data to queue, while we are handling them
///
/// When this lock goes out of the scope, then queues will be clear for block_hash
#[derive(Debug)]
pub struct RequestedOperationDataLock {
    validation_pass: i8,
    block_hash: Arc<BlockHash>,
    queued_block_operations: BlockOperationsQueueRef,
}

impl Drop for RequestedOperationDataLock {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.queued_block_operations.lock() {
            if let Some((missing_operations, _)) = queue.get_mut(&self.block_hash) {
                missing_operations.remove(&self.validation_pass);
                if missing_operations.is_empty() {
                    queue.remove(&self.block_hash);
                }
            }
        }
    }
}

fn tell_peer(msg: Arc<PeerMessageResponse>, peer: &PeerId) {
    peer.peer_ref.tell(SendMessage::new(msg), None);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use serial_test::serial;
    use slog::Level;

    use networking::p2p::network_channel::NetworkChannel;
    use storage::tests_common::TmpStorage;
    use storage::{
        block_meta_storage, operations_meta_storage, BlockMetaStorage, OperationsMetaStorage,
    };
    use tezos_messages::p2p::encoding::prelude::OperationsForBlock;

    use crate::shell_channel::ShellChannel;
    use crate::state::data_requester::DataRequester;
    use crate::state::tests::prerequisites::{
        chain_feeder_mock, create_logger, create_test_actor_system, create_test_tokio_runtime,
        test_peer,
    };
    use crate::state::tests::{block, block_ref};
    use crate::state::ApplyBlockBatch;
    use crate::state::StateError;
    use crypto::hash::ChainId;

    macro_rules! assert_block_queue_contains {
        ($expected:expr, $queues:expr, $block:expr) => {{
            assert_eq!(
                $expected,
                $queues
                    .queued_block_headers
                    .lock()
                    .unwrap()
                    .contains_key($block)
            );
        }};
    }

    macro_rules! assert_operations_queue_contains {
        ($expected:expr, $queues:expr, $block:expr, $validation_passes:expr) => {{
            assert_eq!(
                $expected,
                $queues
                    .queued_block_operations
                    .lock()
                    .unwrap()
                    .contains_key($block)
            );

            match $queues.queued_block_operations.lock().unwrap().get($block) {
                Some((missing_operations, _)) => assert_eq!(missing_operations, $validation_passes),
                None => {
                    if $expected {
                        panic!("test failed");
                    }
                }
            };
        }};
    }

    macro_rules! hash_set {
        ( $( $x:expr ),* ) => {
            {
                let mut temp_set = HashSet::new();
                $(
                    temp_set.insert($x);
                )*
                temp_set
            }
        };
    }

    #[test]
    #[serial]
    fn test_requester_fetch_and_receive_block() -> Result<(), failure::Error> {
        // prerequizities
        let log = create_logger(Level::Debug);
        let tokio_runtime = create_test_tokio_runtime();
        let actor_system = create_test_actor_system(log.clone());
        let network_channel =
            NetworkChannel::actor(&actor_system).expect("Failed to create network channel");
        let shell_channel =
            ShellChannel::actor(&actor_system).expect("Failed to create network channel");
        let storage = TmpStorage::create_to_out_dir("__test_requester_fetch_and_receive_block")?;
        let mut peer1 = test_peer(&actor_system, network_channel, &tokio_runtime, 7777, &log);
        let (chain_feeder_mock, _) =
            chain_feeder_mock(&actor_system, "mocked_chain_feeder", shell_channel)?;

        // requester instance
        let data_requester = DataRequester::new(
            BlockMetaStorage::new(storage.storage()),
            OperationsMetaStorage::new(storage.storage()),
            chain_feeder_mock,
        );

        // try schedule nothing
        assert!(matches!(
            data_requester.fetch_block_headers(vec![], &peer1.peer_id, &peer1.queues),
            Ok(false)
        ));

        // try schedule block1
        let block1 = block_ref(1);
        assert!(matches!(
            data_requester.fetch_block_headers(vec![block1.clone()], &peer1.peer_id, &peer1.queues),
            Ok(true)
        ));

        // scheduled just for the peer1
        assert_block_queue_contains!(true, peer1.queues, &block1);

        // try schedule block1 once more
        assert!(matches!(
            data_requester.fetch_block_headers(vec![block1.clone()], &peer1.peer_id, &peer1.queues),
            Ok(false)
        ));

        // try receive be peer1 - hold lock
        let scheduled = data_requester.block_header_received(&block1, &mut peer1, &log)?;
        assert!(scheduled.is_some());

        // block is still scheduled
        assert_block_queue_contains!(true, peer1.queues, &block1);

        // try schedule block1 once more while holding the lock (should not succeed, because block1 was not removed from queues, becuase we still hold the lock)
        assert!(matches!(
            data_requester.fetch_block_headers(vec![block1.clone()], &peer1.peer_id, &peer1.queues),
            Ok(false)
        ));

        // now drop/release lock
        drop(scheduled);

        // block is not scheduled now
        assert_block_queue_contains!(false, peer1.queues, &block1);

        // we can reschedule it once more now
        assert!(matches!(
            data_requester.fetch_block_headers(vec![block1], &peer1.peer_id, &peer1.queues),
            Ok(true)
        ));

        Ok(())
    }

    #[test]
    #[serial]
    fn test_requester_fetch_and_receive_block_operations() -> Result<(), failure::Error> {
        // prerequizities
        let log = create_logger(Level::Debug);
        let tokio_runtime = create_test_tokio_runtime();
        let actor_system = create_test_actor_system(log.clone());
        let network_channel =
            NetworkChannel::actor(&actor_system).expect("Failed to create network channel");
        let shell_channel =
            ShellChannel::actor(&actor_system).expect("Failed to create network channel");
        let storage =
            TmpStorage::create_to_out_dir("__test_requester_fetch_and_receive_block_operations")?;
        let mut peer1 = test_peer(&actor_system, network_channel, &tokio_runtime, 7777, &log);
        let (chain_feeder_mock, _) =
            chain_feeder_mock(&actor_system, "mocked_chain_feeder", shell_channel)?;

        // requester instance
        let data_requester = DataRequester::new(
            BlockMetaStorage::new(storage.storage()),
            OperationsMetaStorage::new(storage.storage()),
            chain_feeder_mock,
        );

        // prepare missing operations in db for block with 4 validation_pass
        let block1 = block_ref(1);
        OperationsMetaStorage::new(storage.storage())
            .put(&block1, &operations_meta_storage::Meta::new(4))?;

        // try schedule nothiing
        assert!(matches!(
            data_requester.fetch_block_operations(vec![], &peer1.peer_id, &peer1.queues, |_| ()),
            Ok(false)
        ));

        // try schedule block1
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                |_| ()
            ),
            Ok(true)
        ));

        // scheduled just for the peer1
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![0, 1, 2, 3]);

        // try schedule block1 once more
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                |_| ()
            ),
            Ok(false)
        ));

        // try receive be peer1 - hold lock - validation pass 0
        let scheduled = data_requester.block_operations_received(
            &OperationsForBlock::new(block1.as_ref().clone(), 0),
            &mut peer1,
            &log,
        )?;
        assert!(scheduled.is_some());

        // block is still scheduled
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![0, 1, 2, 3]);

        // try schedule block1 once more while holding the lock (should not succeed, because block1 was not removed from queues, becuase we still hold the lock)
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                |_| ()
            ),
            Ok(false)
        ));

        // now drop/release lock
        drop(scheduled);

        // block is still scheduled but less validation passes
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![1, 2, 3]);

        // we can reschedule it once more now
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                |_| ()
            ),
            Ok(false)
        ));

        // download all missing
        assert!(data_requester
            .block_operations_received(
                &OperationsForBlock::new(block1.as_ref().clone(), 1),
                &mut peer1,
                &log,
            )?
            .is_some());
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![2, 3]);

        assert!(data_requester
            .block_operations_received(
                &OperationsForBlock::new(block1.as_ref().clone(), 3),
                &mut peer1,
                &log,
            )?
            .is_some());
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![2]);

        assert!(data_requester
            .block_operations_received(
                &OperationsForBlock::new(block1.as_ref().clone(), 2),
                &mut peer1,
                &log,
            )?
            .is_some());
        assert_operations_queue_contains!(false, peer1.queues, &block1, &HashSet::default());

        // we can reschedule it once more now
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1],
                &peer1.peer_id,
                &peer1.queues,
                |_| ()
            ),
            Ok(true)
        ));

        Ok(())
    }

    #[test]
    #[serial]
    fn test_call_apply_block() -> Result<(), failure::Error> {
        // prerequizities
        let log = create_logger(Level::Debug);
        let actor_system = create_test_actor_system(log.clone());
        let shell_channel =
            ShellChannel::actor(&actor_system).expect("Failed to create network channel");
        let storage = TmpStorage::create_to_out_dir("__test_try_schedule_apply_block_one")?;
        let block_meta_storage = BlockMetaStorage::new(storage.storage());
        let (chain_feeder_mock, _) =
            chain_feeder_mock(&actor_system, "mocked_chain_feeder", shell_channel)?;

        // requester instance
        let data_requester = DataRequester::new(
            BlockMetaStorage::new(storage.storage()),
            OperationsMetaStorage::new(storage.storage()),
            chain_feeder_mock,
        );

        // prepare missing operations in db for block with 4 validation_pass
        let block1 = block_ref(1);
        let batch_with_block1 = ApplyBlockBatch::batch(block1.clone(), Vec::new());
        let chain_id = Arc::new(ChainId::from_base58_check("NetXgtSLGNJvNye")?);

        // try call apply - no metadata
        assert!(matches!(
            data_requester.call_apply_block(
                chain_id.clone(),
                batch_with_block1.clone(),
                None,
            ),
            Err(StateError::ProcessingError {reason}) if reason.contains("No metadata found")
        ));

        // save applied predecessor
        let block0 = block(0);
        block_meta_storage.put(
            &block0,
            &block_meta_storage::Meta::genesis_meta(&block0, chain_id.as_ref(), true),
        )?;
        // save block
        block_meta_storage.put(
            &block1,
            &block_meta_storage::Meta::new(
                false,
                Some(block0.clone()),
                1,
                chain_id.as_ref().clone(),
            ),
        )?;

        // try schedule, false, because missing operations
        assert!(matches!(
            data_requester.call_apply_block(
                chain_id.clone(),
                batch_with_block1.clone(),
                None,
            ),
            Err(StateError::ProcessingError {reason}) if reason.contains("cannot be applied")
        ));

        // save operations - validation_pass 0 - no operations missing
        OperationsMetaStorage::new(storage.storage())
            .put(&block1, &operations_meta_storage::Meta::new(0))?;

        // try schedule - ok
        assert!(matches!(
            data_requester.call_apply_block(chain_id.clone(), batch_with_block1.clone(), None),
            Ok(())
        ));

        // try call - is already applied
        block_meta_storage.put(
            &block1,
            &block_meta_storage::Meta::new(true, Some(block0), 1, chain_id.as_ref().clone()),
        )?;
        assert!(matches!(
            data_requester.call_apply_block(
                chain_id,
                batch_with_block1,
                None,
            ),
            Err(StateError::ProcessingError {reason}) if reason.contains("is already applied")
        ));

        Ok(())
    }
}
