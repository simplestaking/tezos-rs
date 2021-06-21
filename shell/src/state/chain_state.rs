// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashSet;
use std::sync::Arc;

use networking::p2p::network_channel::NetworkChannelRef;
use riker::actors::*;
use slog::Logger;

use crypto::hash::{BlockHash, ChainId, ProtocolHash};
use crypto::seeded_step::{Seed, Step};
use networking::PeerId;
use storage::block_meta_storage::Meta;
use storage::chain_meta_storage::ChainMetaStorageReader;
use storage::PersistentStorage;
use storage::{
    BlockHeaderWithHash, BlockMetaStorage, BlockMetaStorageReader, BlockStorage,
    BlockStorageReader, ChainMetaStorage, OperationsMetaStorage, OperationsStorage, StorageError,
};
use tezos_messages::p2p::encoding::current_branch::CurrentBranchMessage;
use tezos_messages::p2p::encoding::prelude::{CurrentHeadMessage, OperationsForBlocksMessage};
use tezos_messages::p2p::encoding::{block_header::BlockHeader, limits::HISTORY_MAX_SIZE};
use tezos_messages::Head;
use tezos_wrapper::service::{ProtocolController, ProtocolServiceError};

use crate::chain_feeder::ChainFeederRef;
use crate::peer_branch_bootstrapper::{
    PeerBranchBootstrapper, PeerBranchBootstrapperConfiguration, PeerBranchBootstrapperRef,
    StartBranchBootstraping, UpdateBlockState, UpdateOperationsState,
};
use crate::shell_channel::ShellChannelRef;
use crate::state::bootstrap_state::InnerBlockState;
use crate::state::data_requester::{DataRequester, DataRequesterRef};
use crate::state::head_state::CurrentHeadRef;
use crate::state::peer_state::{DataQueuesLimits, PeerState};
use crate::state::StateError;
use crate::validation;

/// Constants for controlling bootstrap speed
///
/// Note: if needed, cound be refactored to cfg and struct
pub(crate) mod bootstrap_constants {
    use std::time::Duration;

    use crate::state::peer_state::DataQueuesLimits;

    /// Timeout for request of block header
    pub(crate) const BLOCK_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
    /// Timeout for request of block operations
    pub(crate) const BLOCK_OPERATIONS_TIMEOUT: Duration = Duration::from_secs(30);

    /// If we have empty bootstrap pipelines for along time, we disconnect peer, means, peer is not provoding us a new current heads/branches
    pub(crate) const MISSING_NEW_BRANCH_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(90);

    /// We can validate just few branches/head from one peer, so we limit it by this constant
    pub(crate) const MAX_BOOTSTRAP_BRANCHES_PER_PEER: usize = 2;

    /// We tries to apply downloaded blocks in batch to speedup and save resources
    pub(crate) const MAX_BLOCK_APPLY_BATCH: usize = 100;

    /// Constants for peer's queue
    pub(crate) const LIMITS: DataQueuesLimits = DataQueuesLimits {
        max_queued_block_headers_count: 10,
        max_queued_block_operations_count: 15,
    };
}

pub enum BlockAcceptanceResult {
    AcceptBlock,
    IgnoreBlock,
    UnknownBranch,
    MutlipassValidationError(ProtocolServiceError),
}

/// Holds and manages state of the chain
pub struct BlockchainState {
    /// persistent block storage
    block_storage: BlockStorage,
    ///persistent block metadata storage
    block_meta_storage: BlockMetaStorage,
    ///persistent chain metadata storage
    chain_meta_storage: ChainMetaStorage,
    /// Operations storage
    operations_storage: OperationsStorage,
    /// Operations metadata storage
    operations_meta_storage: OperationsMetaStorage,

    /// Utility for managing different data requests (block, operations, block apply)
    requester: DataRequesterRef,

    /// Actor resposible for bootstrapping branches of peers per one chain_id
    peer_branch_bootstrapper: Option<PeerBranchBootstrapperRef>,

    /// Common shell channel
    shell_channel: ShellChannelRef,

    network_channel: NetworkChannelRef,

    chain_id: Arc<ChainId>,
    chain_genesis_block_hash: Arc<BlockHash>,
}

impl BlockchainState {
    pub fn new(
        network_channel: NetworkChannelRef,
        block_applier: ChainFeederRef,
        persistent_storage: &PersistentStorage,
        shell_channel: ShellChannelRef,
        chain_id: Arc<ChainId>,
        chain_genesis_block_hash: Arc<BlockHash>,
    ) -> Self {
        BlockchainState {
            requester: DataRequesterRef::new(DataRequester::new(
                BlockMetaStorage::new(&persistent_storage),
                OperationsMetaStorage::new(&persistent_storage),
                network_channel.clone(),
                block_applier,
            )),
            peer_branch_bootstrapper: None,
            block_storage: BlockStorage::new(persistent_storage),
            block_meta_storage: BlockMetaStorage::new(persistent_storage),
            chain_meta_storage: ChainMetaStorage::new(persistent_storage),
            operations_storage: OperationsStorage::new(persistent_storage),
            operations_meta_storage: OperationsMetaStorage::new(persistent_storage),
            shell_channel,
            network_channel,
            chain_id,
            chain_genesis_block_hash,
        }
    }

    pub(crate) fn requester(&self) -> &DataRequesterRef {
        &self.requester
    }

    pub(crate) fn peer_branch_bootstrapper(&self) -> Option<&PeerBranchBootstrapperRef> {
        self.peer_branch_bootstrapper.as_ref()
    }

    pub(crate) fn data_queues_limits(&self) -> DataQueuesLimits {
        bootstrap_constants::LIMITS.clone()
    }

    /// Validate if we can accept branch
    pub fn can_accept_branch(
        &self,
        branch: &CurrentBranchMessage,
        current_head: &CurrentHeadRef,
    ) -> Result<bool, StateError> {
        // validate chain which we operate on
        if self.chain_id.as_ref() != branch.chain_id() {
            return Ok(false);
        }
        if branch.current_branch().current_head().level() <= 0 {
            return Ok(false);
        }

        if let Some(current_head) = current_head.read()?.as_ref() {
            // (only_if_fitness_increases) we can accept branch if increases fitness
            if validation::is_fitness_increases(
                current_head,
                branch.current_branch().current_head().fitness(),
            ) {
                return Ok(true);
            }

            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Validate if we can accept head
    pub fn can_accept_head(
        &self,
        head: &CurrentHeadMessage,
        current_head: &CurrentHeadRef,
        api: &ProtocolController,
    ) -> Result<BlockAcceptanceResult, StateError> {
        // validate chain which we operate on
        if self.chain_id.as_ref() != head.chain_id() {
            return Ok(BlockAcceptanceResult::IgnoreBlock);
        }

        let validated_header: &BlockHeader = head.current_block_header();
        if validated_header.level() <= 0 {
            return Ok(BlockAcceptanceResult::IgnoreBlock);
        }

        // we need our current head at first
        if let Some(current_head) = current_head.read()?.as_ref() {
            // same header means only mempool operations were changed
            if validation::is_same_head(current_head, validated_header)? {
                return Ok(BlockAcceptanceResult::AcceptBlock);
            }

            // (future block)
            if validation::is_future_block(&validated_header)? {
                return Ok(BlockAcceptanceResult::IgnoreBlock);
            }

            // (only_if_fitness_increases) we can accept head if increases fitness
            if !validation::is_fitness_increases_or_same(current_head, validated_header.fitness()) {
                return Ok(BlockAcceptanceResult::IgnoreBlock);
            }

            // lets try to find protocol for validated_block
            let (protocol_hash, predecessor_header, missing_predecessor) =
                self.resolve_protocol(validated_header, &current_head)?;

            // if missing predecessor
            if missing_predecessor {
                return Ok(BlockAcceptanceResult::UnknownBranch);
            }

            // if we have protocol lets validate
            if let Some(protocol_hash) = protocol_hash {
                // lets check strict multipass validation
                match validation::check_multipass_validation(
                    head.chain_id(),
                    protocol_hash,
                    &validated_header,
                    predecessor_header,
                    api,
                ) {
                    Some(error) => Ok(BlockAcceptanceResult::MutlipassValidationError(error)),
                    None => Ok(BlockAcceptanceResult::AcceptBlock),
                }
            } else {
                // if we came here, we dont know protocol to trigger validation
                // so probably we dont have predecessor procesed yet
                // so we trigger current branch request from peer
                Ok(BlockAcceptanceResult::UnknownBranch)
            }
        } else {
            Ok(BlockAcceptanceResult::IgnoreBlock)
        }
    }

    /// Returns triplet:
    /// 1. protocol_hash
    /// 2. applied_predecessor (only if is already applied)
    /// 3. (bool) missing_predecessor,
    fn resolve_protocol(
        &self,
        validated_header: &BlockHeader,
        current_head: &Head,
    ) -> Result<(Option<ProtocolHash>, Option<BlockHeaderWithHash>, bool), StateError> {
        // TODO: TE-238 - store proto_level and protocol_hash and map - optimization - detect change protocol event

        let (protocol_hash, predecessor_header, missing_predecessor) = match self
            .block_meta_storage
            .get(validated_header.predecessor())?
        {
            Some(predecessor_meta) => {
                match predecessor_meta.is_applied() {
                    true => {
                        // if predecessor is applied, than we have exact protocol
                        match self
                            .block_meta_storage
                            .get_additional_data(validated_header.predecessor())?
                        {
                            Some(predecessor_additional_data) => {
                                if let Some(predecessor_header) =
                                    self.block_storage.get(validated_header.predecessor())?
                                {
                                    // return next_protocol and header
                                    (
                                        Some(predecessor_additional_data.next_protocol_hash),
                                        Some(predecessor_header),
                                        false,
                                    )
                                } else {
                                    // return next_protocol and header
                                    (
                                        Some(predecessor_additional_data.next_protocol_hash),
                                        None,
                                        true,
                                    )
                                }
                            }
                            None => {
                                return Err(StateError::ProcessingError {
                                    reason: format!(
                                        "Missing data for applied predecessor: {}!",
                                        validated_header.predecessor().to_base58_check()
                                    ),
                                });
                            }
                        }
                    }
                    false => {
                        // if we dont have applied predecessor, we dont know protocol_hash
                        (None, None, false)
                    }
                }
            }
            None => {
                // if we dont have predecessor stored
                (None, None, true)
            }
        };

        if protocol_hash.is_some() {
            // if we have protocol by predecessor
            Ok((protocol_hash, predecessor_header, missing_predecessor))
        } else {
            // check protocol by current head for the same proto_level
            // if predecessor is applied, than we have exact protocol
            match self.block_storage.get(current_head.block_hash())? {
                Some(current_head_header) => {
                    if current_head_header.header.proto() == validated_header.proto() {
                        if let Some(metadata) = self
                            .block_meta_storage
                            .get_additional_data(current_head.block_hash())?
                        {
                            // return protocol
                            Ok((
                                Some(metadata.next_protocol_hash),
                                predecessor_header,
                                missing_predecessor,
                            ))
                        } else {
                            return Err(StateError::ProcessingError {
                                reason: format!(
                                    "Missing `next_protocol` attribute for applied predecessor: {}!",
                                    validated_header.predecessor().to_base58_check()
                                ),
                            });
                        }
                    } else {
                        Ok((None, predecessor_header, missing_predecessor))
                    }
                }
                None => {
                    return Err(StateError::ProcessingError {
                        reason: format!(
                            "Missing data for applied current_head: {}!",
                            current_head.block_hash().to_base58_check()
                        ),
                    });
                }
            }
        }
    }

    /// Resolves missing blocks and schedules them for download from network
    pub fn schedule_history_bootstrap(
        &mut self,
        sys: &ActorSystem,
        peer: &mut PeerState,
        block_header: &BlockHeaderWithHash,
        mut history: Vec<BlockHash>,
    ) -> Result<(), StateError> {
        // add predecessor (if not present in history)
        if !history.contains(block_header.header.predecessor()) {
            history.insert(0, block_header.header.predecessor().clone());
        };
        // add block itself (if not present in history)
        if !history.contains(&block_header.hash) {
            history.insert(0, block_header.hash.clone());
        };

        // prepare bootstrap pipeline for this peer and rehydrate to prevent stuck of applying blocks
        // schedule download missing blocks - download history
        // at first schedule history - we try to prioritize download from the beginning, so the history is reversed here

        // collect for every block if it is applied and get the "last applied" = highest block index
        let mut last_applied_idx: Option<usize> = None;
        let branch_history_locator_lowest_level_first: Vec<(BlockHash, bool)> = history
            .into_iter()
            .rev()
            .enumerate()
            .map(|(idx, history_block_hash)| {
                match self.block_meta_storage.get(&history_block_hash) {
                    Ok(Some(metadata)) => {
                        if metadata.is_applied() {
                            last_applied_idx = Some(idx);
                        }
                        (history_block_hash, metadata.is_applied())
                    }
                    _ => (history_block_hash, false),
                }
            })
            .collect();

        // prepare bootstrap pipeline for this history according to last known applied block (if None, then use genesis)
        // and we are just interested in history after last applied block
        let (last_applied_block, missing_history): (BlockHash, Vec<BlockHash>) =
            match last_applied_idx {
                Some(last_applied_idx) => {
                    // we split history
                    // all before index we throw away
                    if let Some((last_applied, _)) =
                        branch_history_locator_lowest_level_first.get(last_applied_idx)
                    {
                        (
                            last_applied.clone(),
                            branch_history_locator_lowest_level_first
                                .iter()
                                .enumerate()
                                .filter(|(index, (_, _))| index > &last_applied_idx)
                                .map(|(_, (b, _))| b.clone())
                                .collect(),
                        )
                    } else {
                        // fall back to start from genesis
                        (
                            self.chain_genesis_block_hash.as_ref().clone(),
                            branch_history_locator_lowest_level_first
                                .into_iter()
                                .map(|(b, _)| b)
                                .collect(),
                        )
                    }
                }
                None => {
                    // fall back to start from genesis
                    (
                        self.chain_genesis_block_hash.as_ref().clone(),
                        branch_history_locator_lowest_level_first
                            .into_iter()
                            .map(|(b, _)| b)
                            .collect(),
                    )
                }
            };

        // if we miss something, we will run "peer branch bootstrapper"
        if !missing_history.is_empty() {
            if self.peer_branch_bootstrapper.is_none() {
                self.peer_branch_bootstrapper = Some(
                    PeerBranchBootstrapper::actor(
                        sys,
                        self.chain_id.clone(),
                        self.requester.clone(),
                        self.shell_channel.clone(),
                        self.network_channel.clone(),
                        PeerBranchBootstrapperConfiguration::new(
                            bootstrap_constants::BLOCK_HEADER_TIMEOUT,
                            bootstrap_constants::BLOCK_OPERATIONS_TIMEOUT,
                            bootstrap_constants::MISSING_NEW_BRANCH_BOOTSTRAP_TIMEOUT,
                            bootstrap_constants::MAX_BOOTSTRAP_BRANCHES_PER_PEER,
                            bootstrap_constants::MAX_BLOCK_APPLY_BATCH,
                        ),
                    )
                    .map_err(|e| StateError::ProcessingError {
                        reason: format!("{}", e),
                    })?,
                );
            };

            if let Some(peer_branch_bootstrapper) = self.peer_branch_bootstrapper.as_ref() {
                peer_branch_bootstrapper.tell(
                    StartBranchBootstraping::new(
                        peer.peer_id.clone(),
                        peer.queues.clone(),
                        self.chain_id.clone(),
                        last_applied_block,
                        missing_history,
                        block_header.header.level(),
                    ),
                    None,
                );
            }
        }

        Ok(())
    }

    /// Process block_header, stores/updates storages,
    /// schedules missing stuff to peer
    ///
    /// Returns bool - true, if it is a new block or false for previosly stored
    pub fn process_block_header_from_peer(
        &mut self,
        received_block: &BlockHeaderWithHash,
        log: &Logger,
        peer_id: &Arc<PeerId>,
    ) -> Result<bool, StorageError> {
        // store block
        let is_new_block = self.block_storage.put_block_header(received_block)?;

        // update block metadata
        let block_metadata =
            self.block_meta_storage
                .put_block_header(received_block, &self.chain_id, &log)?;

        // update operations metadata for block
        let (are_operations_complete, _) = self.process_block_header_operations(received_block)?;

        // ping branch bootstrapper with received block and actual state
        if let Some(peer_branch_bootstrapper) = self.peer_branch_bootstrapper() {
            peer_branch_bootstrapper.tell(
                UpdateBlockState::new(
                    received_block.hash.clone(),
                    InnerBlockState {
                        applied: block_metadata.is_applied(),
                        operations_downloaded: are_operations_complete,
                    },
                    peer_id.clone(),
                ),
                None,
            );
        }

        Ok(is_new_block)
    }

    /// Process block_header, stores/updates storages, schedules missing stuff
    ///
    /// Returns:
    /// [metadata] - block header metadata
    /// [is_new_block] - if it is a new block or previosly stored
    /// [are_operations_complete] - if operations are completed
    ///
    pub fn process_injected_block_header(
        &mut self,
        chain_id: &ChainId,
        block_header: &BlockHeaderWithHash,
        log: &Logger,
    ) -> Result<(Meta, bool, bool), StorageError> {
        // store block
        let is_new_block = self.block_storage.put_block_header(block_header)?;

        // update block metadata
        let metadata = self
            .block_meta_storage
            .put_block_header(block_header, chain_id, &log)?;

        // update operations metadata
        let are_operations_complete =
            self.process_injected_block_header_operations(block_header)?;

        Ok((metadata, is_new_block, are_operations_complete))
    }

    /// Process block header. This will create record in meta storage with
    /// unseen operations for the block header.
    ///
    /// If block header is not already present in storage, return `true`.
    ///
    /// Returns tuple:
    ///     (
    ///         are_operations_complete,
    ///         missing_validation_passes
    ///     )
    fn process_block_header_operations(
        &mut self,
        block_header: &BlockHeaderWithHash,
    ) -> Result<(bool, Option<HashSet<u8>>), StorageError> {
        match self.operations_meta_storage.get(&block_header.hash)? {
            Some(meta) => Ok((meta.is_complete(), meta.get_missing_validation_passes())),
            None => self.operations_meta_storage.put_block_header(block_header),
        }
    }

    /// Process injected block header. This will create record in meta storage.
    /// As the the header is injected via RPC, the operations are as well, so we
    /// won't mark its operations as missing
    ///
    /// Returns true, if validation_passes are completed (can happen, when validation_pass = 0)
    fn process_injected_block_header_operations(
        &mut self,
        block_header: &BlockHeaderWithHash,
    ) -> Result<bool, StorageError> {
        match self.operations_meta_storage.get(&block_header.hash)? {
            Some(meta) => Ok(meta.is_complete()),
            None => self
                .operations_meta_storage
                .put_block_header(block_header)
                .map(|(is_complete, _)| is_complete),
        }
    }

    /// Processes comming operations from peers
    ///
    /// Returns true, if new block is successfully downloaded by this call
    pub fn process_block_operations_from_peer(
        &mut self,
        block_hash: BlockHash,
        message: &OperationsForBlocksMessage,
        peer_id: &Arc<PeerId>,
    ) -> Result<bool, StateError> {
        // TODO: TE-369 - optimize double check
        // we need to differ this flag
        let (are_operations_complete, was_block_finished_now) =
            if self.operations_meta_storage.is_complete(&block_hash)? {
                (true, false)
            } else {
                // update operations metadata for block
                let (are_operations_complete, _) = self.process_block_operations(message)?;
                (are_operations_complete, are_operations_complete)
            };

        if are_operations_complete {
            // ping branch bootstrapper with received operations
            if let Some(peer_branch_bootstrapper) = self.peer_branch_bootstrapper.as_ref() {
                peer_branch_bootstrapper.tell(
                    UpdateOperationsState::new(block_hash, peer_id.clone()),
                    None,
                );
            }
        }

        Ok(was_block_finished_now)
    }

    /// Process block operations. This will mark operations in store for the block as seen.
    ///
    /// Returns tuple:
    ///     (
    ///         are_operations_complete,
    ///         missing_validation_passes
    ///     )
    pub fn process_block_operations(
        &mut self,
        message: &OperationsForBlocksMessage,
    ) -> Result<(bool, Option<HashSet<u8>>), StorageError> {
        if self
            .operations_meta_storage
            .is_complete(message.operations_for_block().hash())?
        {
            return Ok((true, None));
        }

        self.operations_storage.put_operations(message)?;
        self.operations_meta_storage.put_operations(message)
    }

    #[inline]
    pub fn get_chain_id(&self) -> &Arc<ChainId> {
        &self.chain_id
    }

    pub fn get_history(
        &self,
        head: &BlockHash,
        seed: &Seed,
    ) -> Result<Vec<BlockHash>, StorageError> {
        Self::compute_history(
            &self.block_meta_storage,
            self.chain_meta_storage.get_caboose(&self.chain_id)?,
            head,
            HISTORY_MAX_SIZE,
            seed,
        )
    }

    /// Resulted history is sorted: "from oldest block to newest"
    fn compute_history(
        block_meta_storage: &BlockMetaStorage,
        caboose: Option<Head>,
        head: &BlockHash,
        max_size: u8,
        seed: &Seed,
    ) -> Result<Vec<BlockHash>, StorageError> {
        if max_size == 0 {
            return Ok(vec![]);
        }

        // init steping
        let mut step = Step::init(seed, &head);

        // iterate and get history records
        let mut counter = max_size;
        let mut current_block_hash = head.clone();
        let mut history = Vec::with_capacity(max_size as usize);

        while counter > 0 {
            // evaluate next step, means we want predecessor at this distance
            let distance = step.next();

            // need to find predecesor at requested distance
            match block_meta_storage.find_block_at_distance(current_block_hash.clone(), distance)? {
                Some(predecessor) => {
                    // add to history
                    history.push(predecessor.clone());

                    // decrement counter and continue
                    counter -= 1;
                    current_block_hash = predecessor;
                }
                None => {
                    // close history with caboose, if predecessor was not found in this run
                    if let Some(caboose) = caboose {
                        if let Some(last) = history.last() {
                            if !last.eq(caboose.block_hash()) {
                                history.push(caboose.into());
                            }
                        } else {
                            // this covers genesis case, when we dont want to add genesis to history for genesis block
                            if !current_block_hash.eq(caboose.block_hash()) {
                                history.push(caboose.into());
                            }
                        }
                    }
                    break;
                }
            }
        }

        Ok(history)
    }
}

#[cfg(test)]
mod tests {
    use slog::Level;

    use crypto::hash::chain_id_from_block_hash;
    use storage::tests_common::TmpStorage;

    use crate::state::tests::prerequisites::create_logger;

    use super::*;

    /// This test is rewritten according to [test_state.ml -> test_locator]
    #[test]
    fn test_history_and_compute_locator() -> Result<(), failure::Error> {
        let log = create_logger(Level::Debug);
        let storage = TmpStorage::create_to_out_dir("__test_history")?;
        let block_meta_storage = BlockMetaStorage::new(storage.storage());
        let block_storage = BlockStorage::new(storage.storage());

        /*
         * Genesis - A1 - A2 - A3 - A4 - A5 - A6 - A7 - A8
         *                      \
         *                       B1 - B2 - B3 - B4 - B5 - B6 - B7 - B8
         */
        let blocksdb = data::init_blocks();

        // init with genesis
        let (genesis_hash, genesis_header) =
            (blocksdb.block_hash("Genesis"), blocksdb.header("Genesis"));
        let chain_id = chain_id_from_block_hash(&genesis_hash)?;
        block_storage.put_block_header(&genesis_header)?;
        block_meta_storage.put(
            &genesis_hash,
            &Meta::genesis_meta(&genesis_hash, &chain_id, true),
        )?;

        // store branch1 - root genesis
        data::store_branch(
            &["A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8"],
            &chain_id,
            &blocksdb,
            &block_storage,
            &block_meta_storage,
            &log,
        );
        // store branch2 - root A3
        data::store_branch(
            &["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8"],
            &chain_id,
            &blocksdb,
            &block_storage,
            &block_meta_storage,
            &log,
        );
        // store branch3 - root A3 (C0, C1, C2 ... C62)
        let c_branch = {
            let mut b = Vec::with_capacity(10_000);
            for i in 0..63 {
                b.push(format!("C{}", i));
            }
            b
        };
        data::store_branch(
            &c_branch.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
            &chain_id,
            &blocksdb,
            &block_storage,
            &block_meta_storage,
            &log,
        );

        // check if reorg stored - A3 - should also contains successor A4 and B1 and C0
        let reorg_block_meta = block_meta_storage.get(&blocksdb.block_hash("A3"))?.unwrap();
        assert!(reorg_block_meta
            .successors()
            .contains(&blocksdb.block_hash("A4")));
        assert!(reorg_block_meta
            .successors()
            .contains(&blocksdb.block_hash("B1")));
        assert!(reorg_block_meta
            .successors()
            .contains(&blocksdb.block_hash("C0")));

        // calculates and checks
        let caboose = Some(Head::new(
            blocksdb.block_hash("Genesis"),
            Meta::GENESIS_LEVEL,
            vec![],
        ));

        data::assert_history(
            &["A7", "A6", "A5", "A4", "A3", "A2"],
            &blocksdb,
            BlockchainState::compute_history(
                &block_meta_storage,
                caboose.clone(),
                &blocksdb.block_hash("A8"),
                6,
                &Seed::new(
                    &data::generate_key_string('s'),
                    &data::generate_key_string('r'),
                ),
            )?,
        );

        data::assert_history(
            &["B7", "B6", "B5", "B4", "B3", "B2", "B1", "A3"],
            &blocksdb,
            BlockchainState::compute_history(
                &block_meta_storage,
                caboose.clone(),
                &blocksdb.block_hash("B8"),
                8,
                &Seed::new(
                    &data::generate_key_string('s'),
                    &data::generate_key_string('r'),
                ),
            )?,
        );

        data::assert_history(
            &["B7", "B6", "B5", "B4"],
            &blocksdb,
            BlockchainState::compute_history(
                &block_meta_storage,
                caboose.clone(),
                &blocksdb.block_hash("B8"),
                4,
                &Seed::new(
                    &data::generate_key_string('s'),
                    &data::generate_key_string('r'),
                ),
            )?,
        );

        data::assert_history(
            &[],
            &blocksdb,
            BlockchainState::compute_history(
                &block_meta_storage,
                caboose.clone(),
                &blocksdb.block_hash("A5"),
                0,
                &Seed::new(
                    &data::generate_key_string('s'),
                    &data::generate_key_string('r'),
                ),
            )?,
        );

        data::assert_history(
            &["A4", "A3", "A2", "A1", "Genesis"],
            &blocksdb,
            BlockchainState::compute_history(
                &block_meta_storage,
                caboose.clone(),
                &blocksdb.block_hash("A5"),
                100,
                &Seed::new(
                    &data::generate_key_string('s'),
                    &data::generate_key_string('r'),
                ),
            )?,
        );

        data::assert_history(
            &[
                "C61", "C60", "C59", "C58", "C57", "C56", "C55", "C54", "C53", "C52", "C51", "C49",
                "C46", "C44", "C43", "C42", "C41", "C40", "C39", "C37", "C34", "C29", "C26", "C20",
                "C15", "C13", "C9", "C7", "C3",
            ],
            &blocksdb,
            BlockchainState::compute_history(
                &block_meta_storage,
                caboose.clone(),
                &blocksdb.block_hash("C62"),
                29,
                &Seed::new(
                    &data::generate_key_string('s'),
                    &data::generate_key_string('r'),
                ),
            )?,
        );

        data::assert_history(
            &[],
            &blocksdb,
            BlockchainState::compute_history(
                &block_meta_storage,
                caboose,
                &blocksdb.block_hash("Genesis"),
                5,
                &Seed::new(
                    &data::generate_key_string('s'),
                    &data::generate_key_string('r'),
                ),
            )?,
        );

        Ok(())
    }

    mod data {
        use std::{collections::HashMap, convert::TryInto};

        use itertools::Itertools;
        use slog::Logger;

        use crypto::hash::{BlockHash, ChainId, CryptoboxPublicKeyHash, HashType};
        use storage::{
            BlockHeaderWithHash, BlockMetaStorage, BlockMetaStorageReader, BlockStorage,
        };
        use tezos_messages::p2p::binary_message::BinaryRead;
        use tezos_messages::p2p::encoding::block_header::BlockHeader;

        macro_rules! init_block {
            ($blocks:ident, $name:expr, $block_hash_expected:expr, $block_header_hex_data:expr) => {
                init_block!(
                    $blocks,
                    $name,
                    $block_hash_expected,
                    $block_hash_expected,
                    $block_header_hex_data
                );
            };

            ($blocks:ident, $name:expr, $block_hash:expr, $block_hash_expected:expr, $block_header_hex_data:expr) => {
                let block_hash =
                    BlockHash::from_base58_check($block_hash).expect("Failed to create block hash");
                let block_hash_expected = BlockHash::from_base58_check($block_hash_expected)
                    .expect("Failed to create block hash");
                $blocks.insert(
                    $name,
                    (
                        block_hash.clone(),
                        BlockHeaderWithHash::new(
                            BlockHeader::from_bytes(
                                hex::decode($block_header_hex_data)
                                    .expect("Failed to decode hex data"),
                            )
                            .expect("Failed to decode block header"),
                        )
                        .expect("Failed to create block header with hash"),
                    ),
                );
                let (hash, header) = $blocks.get($name).unwrap();
                assert_eq!(block_hash, *hash);
                assert_eq!(block_hash_expected, header.hash);
            };
        }

        pub struct BlocksDb {
            blocks: HashMap<&'static str, (BlockHash, BlockHeaderWithHash)>,
        }

        impl BlocksDb {
            pub fn block_hash(&self, name: &str) -> BlockHash {
                let (hash, _) = self.blocks.get(name).unwrap();
                hash.clone()
            }

            pub fn name(&self, block_hash: &BlockHash) -> String {
                self.blocks
                    .iter()
                    .find(|(_, (bh, _))| bh.eq(block_hash))
                    .map(|(k, _)| k.to_string())
                    .unwrap()
            }

            pub fn header(&self, name: &str) -> BlockHeaderWithHash {
                let (_, header) = self.blocks.get(name).unwrap();
                header.clone()
            }
        }

        /// These block where dump from [test_state.ml -> build_valid_chain]
        pub(crate) fn init_blocks() -> BlocksDb {
            let mut blocks = HashMap::new();

            // Genesis - BLockGenesisGenesisGenesisGenesisGenesisGeneskvg68z -> BLqDCPdEsAv9giMTD5Uv7Z8HVF3mJAfZw3iH1XvR9p2B8uSsgdn
            // {"level":0,"proto":0,"predecessor":"BLockGenesisGenesisGenesisGenesisGenesisGeneskvg68z","timestamp":"1970-01-01T00:00:00Z","validation_pass":0,"operations_hash":"LLoZS2LW3rEi7KYU4ouBQtorua37aWWCtpDmv1n2x3xoKi6sVXLWp","fitness":[],"context":"CoVea41f9dPhkymYEfPsXV5FmyzDb91iz1Grk6zasb31zv9nEZjN","protocol_data":""}
            init_block!(blocks, "Genesis", "BLockGenesisGenesisGenesisGenesisGenesisGeneskvg68z", "BLqDCPdEsAv9giMTD5Uv7Z8HVF3mJAfZw3iH1XvR9p2B8uSsgdn", "00000000008fcf233671b6a04fcf679d2a381c2544ea6c1ea29ba6157776ed8424418da9c10000000000000000000e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a800000000844a8057887e732f873636fae3ef3dd0674646b6c3b27182034e41cd01ce2de1");

            // A1
            // {"level":1,"proto":0,"predecessor":"BLockGenesisGenesisGenesisGenesisGenesisGeneskvg68z","timestamp":"1970-01-01T00:00:06Z","validation_pass":1,"operations_hash":"LLoaE6dT25pScPXK1u6udM3qTyJF1XX4C9zhbDB8iRFvMrDJCxpeV","fitness":["0000000000000001"],"context":"CoVTPCEqHAJQ1knCdXo7BUZjWkYHKqudzct4YjwK2qMxpy9f9smh","protocol_data":"000000024131"}
            init_block!(blocks, "A1", "BMPpsdYyqRPUx4bPM187UguF29NUAxNhh5tXXrSNMbb6UKZf2iM", "00000001008fcf233671b6a04fcf679d2a381c2544ea6c1ea29ba6157776ed8424418da9c100000000000000060176f52f440c8e4ab99a2a4ef76474c4bcf4ee09238150ed77fd73fb3be1fe4af30000000c0000000800000000000000016ae39b9a0de0eff1d52ae16a9282747edf61f89e4bc72b943221ca94fd3b9b48000000024131");

            // A2
            // {"level":2,"proto":0,"predecessor":"BMPpsdYyqRPUx4bPM187UguF29NUAxNhh5tXXrSNMbb6UKZf2iM","timestamp":"1970-01-01T00:00:16Z","validation_pass":1,"operations_hash":"LLoZPCKN1ZAUGopsojjhxEHMegPKYtLL6MXsR9vmUEtB6f8vsBW3m","fitness":["0000000000000002"],"context":"CoVG7LmDBWkBCJuNSnyrQtt9maotfYRjQTtgEiq8VV9xVSCFnpvF","protocol_data":"000000024132"}
            init_block!(blocks, "A2", "BLq4AAun3eFcHWA8BJZgnUy6DEkSRZde69MziAh2ct4wCE1weCY", "0000000200dd7c2bbc9594cd39fe49f29637f6bff5ca9d50f11dd341508bfacf714b7e28e300000000000000100107eb8155a0827d5ef666e43b15acdea8adb58e3817aa6ca7ac4a9c55eb41ebd20000000c000000080000000000000002514aa7bfad26b4289f9225f722803ad5d62294c931e8ccd8d237ea7e87afb236000000024132");

            // A3
            // {"level":3,"proto":0,"predecessor":"BLq4AAun3eFcHWA8BJZgnUy6DEkSRZde69MziAh2ct4wCE1weCY","timestamp":"1970-01-01T00:00:24Z","validation_pass":1,"operations_hash":"LLobEmNi6YBNnkPhA96raqZgXxpzdUtTrchWEiN5xERnja9DYVXoF","fitness":["0000000000000003"],"context":"CoWUk7yRZFKZXqXbkBdbabNdaWAxkdBcZvr6gK2iSdZSNgdzEWnu","protocol_data":"000000024133"}
            init_block!(blocks, "A3", "BLMw95k8rwLf2aZmGiRh9jFMuKGgQXipKUMHk4WLE8YM4Z2WJ86", "000000030093131926e0640848ba856c0dd8efd864f7d9a0b5c263a5e03f869f5dc598943b000000000000001801fc2bb2c1c5997df6928a6dc2099f44125464dd8dc38fcf24b67db2a79fc82c0f0000000c000000080000000000000003f1acd04bbdb7838be6b8f74d74963cbb6b89633114e986ae1674eb80f0a6420c000000024133");

            // A4
            // {"level":4,"proto":0,"predecessor":"BLMw95k8rwLf2aZmGiRh9jFMuKGgQXipKUMHk4WLE8YM4Z2WJ86","timestamp":"1970-01-01T00:00:26Z","validation_pass":1,"operations_hash":"LLoazNce1rhdo8XCbVxfCzcHkUMXMwjHd66Pr4P56qygx2WPrQsN6","fitness":["0000000000000004"],"context":"CoUmdAefV4kn2Z9G7qHypqedCbTrSfJGaeTrAYvAZ9LEJH2ycqNa","protocol_data":"000000024134"}
            init_block!(blocks, "A4", "BME4s6XySdprEvUwyG9A5YvcgsDnKzBKKSVQ6UoZ6mk3rBR7jNt", "0000000400557e36cf1cae93bda2831222102b7c4ce19a131c3d3d01aec176a7b00d1ee98a000000000000001a01db7daffe2c9e790d589a6b3ce96dc49efbb10fbb50f1e636707f751a5ae1f30b0000000c000000080000000000000004109c7826a2c620d2c5cac965b853a9bc98d6385ace6601a3a1515d518143f9af000000024134");

            // A5
            // {"level":5,"proto":0,"predecessor":"BME4s6XySdprEvUwyG9A5YvcgsDnKzBKKSVQ6UoZ6mk3rBR7jNt","timestamp":"1970-01-01T00:00:34Z","validation_pass":1,"operations_hash":"LLoaWkxXDhZFViUeF3UaSKuLUJbs9Mxx9QtAVXFfhWFSbsarJdR6B","fitness":["0000000000000005"],"context":"CoVNuije97tRC5t9nT8Wf79f8n4JLravT3tKdTu8u6SSWoo7ETBM","protocol_data":"000000024135"}
            init_block!(blocks, "A5", "BKjP9MgvzqEDCdXaVvy2Yt5C52UtpUa9puUQ6konopdi9UKFFWp", "0000000500c7539814cb12b4fc92c3768ad7dc7322e6abeb3b9b563b556ecb69d341b260040000000000000022019cc9c998792c4ef6eb7c797cc8aa6c3dddd360d611746a3e827fc1dbecb5fe850000000c00000008000000000000000560bb24813522ffed0aabe1b776e9a2317639e4c57cf51ceb4145d8d75976447f000000024135");

            // A6
            // {"level":6,"proto":0,"predecessor":"BKjP9MgvzqEDCdXaVvy2Yt5C52UtpUa9puUQ6konopdi9UKFFWp","timestamp":"1970-01-01T00:00:35Z","validation_pass":1,"operations_hash":"LLoaKzR9mfrjup9r9onHtTdemXuiUxEtE4UTFGyRKTUxnQrD1Dkg4","fitness":["0000000000000006"],"context":"CoVSgGeehEaqSrikvKReCXwvxGxKc3Y1jpNjoN9YGUB1RjdGbhqf","protocol_data":"000000024136"}
            init_block!(blocks, "A6", "BKm9XUEw7yWXEF8ERb9QQXncRe44smMJy8hgkpE57Pv2U6mLNtn", "0000000600027f8016df677b17a3b62d9f4e34ce9544289554a546ce5d1db87cbe2a4f927b00000000000000230184569a705486fd7bb7231ae792a28a499d94be7240d69671a5bc72ad1d1b65b60000000c00000008000000000000000669497732182ffb3e538a9e25c1443b7d1a989daae42e435edcd3ff3ef2ef7d12000000024136");

            // A7
            // {"level":7,"proto":0,"predecessor":"BKm9XUEw7yWXEF8ERb9QQXncRe44smMJy8hgkpE57Pv2U6mLNtn","timestamp":"1970-01-01T00:00:39Z","validation_pass":1,"operations_hash":"LLoaB9R6FQkYCnaj9DLLntenyvzHhan4HdGyvk2y4Yy1NCNfQkd8K","fitness":["0000000000000007"],"context":"CoUnTVf8rkYTb82tVQDbB4amRcJcM8BQY5Yb7o3NcqejK3iPPW8G","protocol_data":"000000024137"}
            init_block!(blocks, "A7", "BLdUbYSyRkYiaYJ2N5GX9UHR8eHeaYyTGXoDRCx9BA9dQMvGvEo", "0000000700068192385150f050be916e7c5e3f07170485e496f9863b6f3fb6d18b6bd0c637000000000000002701704147069176a05aab67929dd7bb6c6dca3473aa9a908deb1e0ea4d74c75c0930000000c0000000800000000000000071280d005273dcf3f67d043b6b5f539d2f74dd73e6f0eb4a5d660f2275fb67912000000024137");

            // A8
            // {"level":8,"proto":0,"predecessor":"BLdUbYSyRkYiaYJ2N5GX9UHR8eHeaYyTGXoDRCx9BA9dQMvGvEo","timestamp":"1970-01-01T00:00:45Z","validation_pass":1,"operations_hash":"LLoZjnGfgrcCz7MGQhtjGv1UprjcCnyMFB4wAbzTQGfegXmBrL4ze","fitness":["0000000000000008"],"context":"CoWKW6oSENmEMm1KRkKUcKu6t2Z7EbxxpMPPLjf7QPsX1FU5rtyX","protocol_data":"000000024138"}
            init_block!(blocks, "A8", "BKjtHh7SVHFMncnvYdxo1MZ78vHRC26CCEb8BYgGPgYhm98J5cY", "000000080078c8a8ead1a1d3552d63ce5ab671e67e14e07838ad02cc862f4d1cacc24f0060000000000000002d0136a960337b7cdd7dac3ff954699a6ce4e8447d1bc491a4d16af21b181dcf8a9d0000000c000000080000000000000008dcb0c9d7bd04a8039cb9e81141377dea11c93d44fbde0ee5f004a8ef2aeb8ded000000024138");

            // B1
            // {"level":4,"proto":0,"predecessor":"BLMw95k8rwLf2aZmGiRh9jFMuKGgQXipKUMHk4WLE8YM4Z2WJ86","timestamp":"1970-01-01T00:00:32Z","validation_pass":1,"operations_hash":"LLoaHSv7gZeFvmDtRHx3Mz4gdzFsKZ2sKsF9aSGagaU5Hyev3sMxL","fitness":["0000000000000004"],"context":"CoWHe9YkhrSXASSmysMv2yTpA9kmxPzCUBft8j4K3ua5QmshywVt","protocol_data":"000000024231"}
            init_block!(blocks, "B1", "BLnnrZmgGM7266JbFTByPPjEMhY4yLmZq7jswsCEEAG49JQRafJ", "0000000400557e36cf1cae93bda2831222102b7c4ce19a131c3d3d01aec176a7b00d1ee98a0000000000000020017e9057f774f7dcc849651ee2ca66ace961921b4ad93bf8f559e34cf638da40440000000c000000080000000000000004d876e18398aa6f0299e7920079c545fcec9dad9dbee54392ccd856b4857d795a000000024231");

            // B2
            // {"level":5,"proto":0,"predecessor":"BLnnrZmgGM7266JbFTByPPjEMhY4yLmZq7jswsCEEAG49JQRafJ","timestamp":"1970-01-01T00:00:41Z","validation_pass":1,"operations_hash":"LLoapJhxXKpPX27pkThLv51MSdWuBJS73Az1wWQkNTg4KuBjwTCW2","fitness":["0000000000000005"],"context":"CoV7HqdQrymGjWgxHjPBEHRePHn2uxgcnmVNKZMdwBBmhtPgoPhZ","protocol_data":"000000024232"}
            init_block!(blocks, "B2", "BKrKqDUnsoMVS872iYPBrpxGZGNDUgSJgpuo4X3g37vpqoG81Gg", "00000005008def2aa446ea14813fb3f83455d383a1d97a8d59befe548f4e809d1e4688599f000000000000002901c4a1b9a56fa0b7bda340ddbbebc341d30d4c98afa6c9916377642e3cbd517af80000000c0000000800000000000000053d44598b2ef87707a51d4fb2ba6004210368a13f996f26db874a3f0eaf72d2a2000000024232");

            // B3
            // {"level":6,"proto":0,"predecessor":"BKrKqDUnsoMVS872iYPBrpxGZGNDUgSJgpuo4X3g37vpqoG81Gg","timestamp":"1970-01-01T00:00:47Z","validation_pass":1,"operations_hash":"LLoax8ixMAWcMacdW4HqgAyeXVByykoYLYzN2yBftEnee436DwMcD","fitness":["0000000000000006"],"context":"CoUySwpvuRn5wLAvuJgJvRykeiYcYwgyb8kqXnPEsr1k1gTHgii1","protocol_data":"000000024233"}
            init_block!(blocks, "B3", "BLFc7roUTEU9kxnvoenJuFQiaybNpeCLet5MrySYKC5ByGGeuzq", "00000006001243413091fd56379c270ab1713bb64d664be7344b8c36f2163d543c425889d2000000000000002f01d667e952c00d8f3f3c91a9ab64e71420e1a754816cf48d06403b0d41a44c4f350000000c0000000800000000000000062b756284973374c23d90c25ff741c68946f0c3762f77fbb0b3dc72b5a59e5a6d000000024233");

            // B4
            // {"level":7,"proto":0,"predecessor":"BLFc7roUTEU9kxnvoenJuFQiaybNpeCLet5MrySYKC5ByGGeuzq","timestamp":"1970-01-01T00:00:49Z","validation_pass":1,"operations_hash":"LLoZwmsD4hAdfdsVHcxEdA7gP8Xfoua4oCnndodosMtysTpv31wXo","fitness":["0000000000000007"],"context":"CoVTTtAGew3XLidqRcsJC9RvyAEH4quMq3i77636k3Gsq7za5Cxs","protocol_data":"000000024234"}
            init_block!(blocks, "B4", "BLdkVcEw2Sb1RhtoQwEUirbHZLU9yMWDrPTiBfbNGB2ky5v9Yek", "0000000700471fe862a4e8f980072e68a4587072d3b5e058434cfdc3bb8aff406179703aa600000000000000310151e4ac8c070ea450cf258b0b222fb557779d268f86a2f021fed622f6b2128aa30000000c0000000800000000000000076b12981ebf5ce0b849bfa6ed9ad019bd7167399ee724a3b3cb7136c442eb6e30000000024234");

            // B5
            // {"level":8,"proto":0,"predecessor":"BLdkVcEw2Sb1RhtoQwEUirbHZLU9yMWDrPTiBfbNGB2ky5v9Yek","timestamp":"1970-01-01T00:00:51Z","validation_pass":1,"operations_hash":"LLoaxaJFjrXDg4dvH7LHYEjag5EVbwu4wMND38Jzs6N5JfqPEUAw1","fitness":["0000000000000008"],"context":"CoV4FMzT92zoeCq4eZ3fW7veMgdtxjRgQk3GvN1aoAgpRF8qF7Rt","protocol_data":"000000024235"}
            init_block!(blocks, "B5", "BMGAKWbFsrQCUZ5xNdGqijNme1B8vLhKJMZZeVJGu78LuhT7TXC", "00000008007967fcbe82569f2fc09c02ef7ea7054018d9a885a3b3a02c9d03692e845fb49f000000000000003301d7683784837e5d7690d8bcc739a47a85ab16d3430735f43b290177d68b857fef0000000c000000080000000000000008365bb13160722feb4f30d165a72fa0de690525047fb4758f4ecf8eaad1f3629d000000024235");

            // B6
            // {"level":9,"proto":0,"predecessor":"BMGAKWbFsrQCUZ5xNdGqijNme1B8vLhKJMZZeVJGu78LuhT7TXC","timestamp":"1970-01-01T00:00:52Z","validation_pass":1,"operations_hash":"LLoZitHnwzHrMoR3e76u7sSd55KUBdvirJQ4AFzSwf2viabxU35tw","fitness":["0000000000000009"],"context":"CoVDdibCGLSFSJYcSMifeEQuAya5JbRoiZ6et2xoTTnCnMNoUNKV","protocol_data":"000000024236"}
            init_block!(blocks, "B6", "BLajmWuGgs9CpZ4Ld8Dh8Yk8pyt6b94YmdQrqdqaPARSSwKDSbt", "0000000900cc14d3ca2db32d79d16a0ffe3c19ce8bb78dc3d59f02dd8a999a42ecba3d53d900000000000000340134a06c5947a8f0c53d2c0835d875e3402052b85f4bf681666e816822665e455b0000000c0000000800000000000000094bab402b78fbb979f1adf1c9075da406d12b726e7fff96b98838de920ea551eb000000024236");

            // B7
            // {"level":10,"proto":0,"predecessor":"BLajmWuGgs9CpZ4Ld8Dh8Yk8pyt6b94YmdQrqdqaPARSSwKDSbt","timestamp":"1970-01-01T00:01:00Z","validation_pass":1,"operations_hash":"LLoa2CeGtHGGUeuTCR3w5uwzNXSXCAcFtiXdw9eqF6jwjWYYRtC2D","fitness":["000000000000000a"],"context":"CoW1RTwHXdxdjbq5HWLrjMjAnXexiSKmQ2EuE17ZL5SBRivQVSXz","protocol_data":"000000024237"}
            init_block!(blocks, "B7", "BL21S8exMDVE1B52utL7M7QYjYePaqLrR9AUaiv3KgPQq4goxeS", "0000000a007290e04648e9f26746f8bf4bfaafc9267507ad0dd1d008fc8cc7df6dfddd3dbb000000000000003c015bf21b96c62b0c9817c9925d997e507ca2201891b7eab0d5fc7d23aefe08a6100000000c00000008000000000000000ab3a34d71902767728c9dbd7f7ac4e034d1a0ab6c85ecb51d001d9d704ac2c5e1000000024237");

            // B8
            // {"level":11,"proto":0,"predecessor":"BL21S8exMDVE1B52utL7M7QYjYePaqLrR9AUaiv3KgPQq4goxeS","timestamp":"1970-01-01T00:01:10Z","validation_pass":1,"operations_hash":"LLob1Fn5M6uF6pCD4oNYg4RTZfuc2NjWA3gJTZ63QQHvzi6sWMV3d","fitness":["000000000000000b"],"context":"CoVx8V75oaAPHhjhrnD74DpHMpP6pG74NmLsjvUTJ1bZcW6X56ee","protocol_data":"000000024238"}
            init_block!(blocks, "B8", "BLMiPQmJj61k2Zh8ja2m385fTavG2NMgAyULkybGrScVqV16DAG", "0000000b00283fa982d1e7d8824c531d0d5514eff8c280916a6e6801c63825f69187e78e00000000000000004601dd7e7168bf8e558b98421431cbc1209290dee69bcbd5733de730a8a01be281b10000000c00000008000000000000000bac294aa496751d2b44ae45ca326b1b15b8ce8492338c256f08291ae2d5b7838a000000024238");

            // C0 .. C62
            init_block!(blocks, "C0", "BMB77NL7PzoAT9xXdkGfRxz1EoL3BKjBepNDqCWR33wuVetgVZQ", "0000000400557e36cf1cae93bda2831222102b7c4ce19a131c3d3d01aec176a7b00d1ee98a000000000000001b0111a8cb3d9bc12d12f9ec1d7427c4666922553d307a7e8d74f23240c940c93a390000000c0000000800000000000000040475077c1431c58b92f32cd157a2a43a7ce9b2bfbacbf9a492223fc3b590c109000000024330");
            init_block!(blocks, "C1", "BMVgw5Lb7ZaFrcnHXCG8DcRfsR8tZmCwfna6ssjegJewF6jqseQ", "0000000500c09a443320916085e5126e259316ec20dbf645cd15398c909fde2403d377988f000000000000001c01e4d8aee2dba6b2baf9056c1241c3c7a0fe075a3bec5992417b853e7efaed4f6f0000000c00000008000000000000000510c50ce9fd9c8570939a75915ce72f04260c54b46fbe0b8a6f7d458e67e55a71000000024331");
            init_block!(blocks, "C2", "BLdPpuJ4sdu4yYBwMZN35sMZ2tchRDLhcWaV1uuJLyX5xaGNwRN", "0000000600eacc43f7a4c0f209160863e71016c009f6bdab53ac79abf81b2a77c5b748e665000000000000002401a699a21863006f829bd95d4fb05499dae93c85610e95b6d72afeb30387147bcb0000000c000000080000000000000006e2401c3706d20b3a9a9790fdfcc7bf5cfee564332dd26066b9a0825944520f45000000024332");
            init_block!(blocks, "C3", "BKpJ17no1q6ZjUCv8Nhnt7HTvY1PuzkThgM315gCWBC33KTFhQX", "00000007007898dba453465daf1ed247577d6b70da42f99bf052d540fa13c5dc23ee928173000000000000002a014cee2ba719975eb766add0b54ae0628836ed4b4f0343bc673893292a28338beb0000000c000000080000000000000007c001966af3b8797c88b2a82a7df4116273eee252dcf5e0e12823157f652f909a000000024333");
            init_block!(blocks, "C4", "BLTJcUD4T28TDNMg5iYV13oRmJxVwF6YBZb5FmEkfQWgvzLizua", "00000008000da65ce648184a819340ede820d6edf94e551d5d794ce7665d5124c7176ae616000000000000003301be9073096430120b6ca33475e1d3f798881c1aa52dbf32da1cd74b4cd8eb1d1e0000000c0000000800000000000000081bbca6cb7086ebaa8d7d92da50b5d09b12b3cdff1fea1375673e8a4c00136e08000000024334");
            init_block!(blocks, "C5", "BKtBvqbn3q7ih8tHyVfKtY1jYkcD1ikBiYMyQ3oqpwPQrLK3hWh", "000000090061afce667917c8acedb655139b702dc9ca82fb7bb538b7858b6ee731d4d8fcae000000000000003601c39f1c3b6fe170ff96f91ee52e9739866174ba7847f7d3b07cec1210893f28ef0000000c000000080000000000000009567fc1975d2770a4344f78bd932db734ce2f2e12751adfc218505d492bdb2492000000024335");
            init_block!(blocks, "C6", "BM1CqMRP7mzJvdkcVNniEHUwDsf5hZ7HXkZ4tcvCyvs5i41hjNQ", "0000000a00167e9bc0700a310e9c1ed539f0ed0d878ff3777aef1761e15965f5975af8d6ca000000000000003801e3feac2a4dcd00fd7c06c1497289574e26add9ccf95131ad46b8db22f41638f40000000c00000008000000000000000a403b1f1080d741c43c16eb6ed75c8ba61e4e6e863f81e456296657c024510594000000024336");
            init_block!(blocks, "C7", "BKjMEcwVzVj9stHJq63Qx8t1S3ptoNRgxrcGBjcw1owSHY4QLdZ", "0000000b00aa1ed691c61cb5724f9f9f69a6db026384c8e18c3c523ae31ba40815fbe6d0fe000000000000003b01905fb5691ae80a4f73286d6e3a4a0aeac246058221ab627a8bcf6b92bf8fa87f0000000c00000008000000000000000b4af9a9c901950df870a634fd51251c6d6cff4917d48498b96f0948f976d05476000000024337");
            init_block!(blocks, "C8", "BM3yuxnuAP9r6xYRnKjdEhJ5cDGySGKNWFSioVmKGVTN19iUhS6", "0000000c00026c5d9aaf026c025b123b0a172e75bf25620ce835e4b9951c89eeaca4aeb8cb000000000000003e012b51e7319f13fa7debdb6187abec8b70502722160ca3ee5b6be90a25911613900000000c00000008000000000000000ce1b26d1c50bcb8b4dcc6c6d5b7deeafc20c2d9a54aece051b81ccc9db5ae9731000000024338");
            init_block!(blocks, "C9", "BLtNtGuQ5eHZMNVgiVyqqvKTF8TfXxvMKepJpo876CBvJAKdECL", "0000000d00b06d2f98b784461042ac8b0ea7a17f0caf0f4aeaa3759ed4ea1275ddf1acbd960000000000000046010f6484be06513b2f0940616087d0e2c2737959c6456de3c6e7171b372c09ebe50000000c00000008000000000000000d10b57e5d6cd2e7be5c89016bd02dc0b51e5458e213be72969b099da369b11295000000024339");
            init_block!(blocks, "C10", "BM3sGEDAq6bLvRJ4TBjF1w9rRCeX43S9svKpaFBeQbAfwVHh84Z", "0000000e009a9e9bc29c8a6888e0f2f01dffa530e6622de0ddb3eb7b976897a1688573fe620000000000000050013401ce1ee2e0fcfa7922bff5f545d9118582ad0b6828f92869dab33a98d35ff30000000c00000008000000000000000ecd5f95a8f4b1a684c24bacaab0718a1edf967a2c52185c5252971b9bc9165e0700000003433130");
            init_block!(blocks, "C11", "BLMQi5d32Wa1dW254XNpbWEgUQ9vAhFrbKD75CpHsvAbqGhehxK", "0000000f00b02a8888fc51441c0b39e942954bd86410963512ca6573910a5a57f9014b990f000000000000005301dcefe14f40c85c206100f6061b10012cf4b7335c999345d9ae909475aa5aed740000000c00000008000000000000000f15a00c9f8e28f4dbf954e44a2b256aa1598a0567543c16e17b228218ad9b8ec900000003433131");
            init_block!(blocks, "C12", "BLKy1sUkC6ADRA8GTjNneZcEkfkuBwstMhnKEP4wonBen56zEwF", "0000001000544d3b6f2a2e6f19dfde4dcf79c9ad81da3c8428e588a096c2f974a8ec58ba06000000000000005701296beebff52541d01d56484ff3763e5129ee2ffe0b38e9d1a69eab6a8b8661830000000c000000080000000000000010f3d7b71bb74864624440098e13d08ee66994f5afa7ac6de8f4b724123903035200000003433132");
            init_block!(blocks, "C13", "BLkkmTECd6tzp2tMA6psAWcq6hkv4wFDVTjsfcL1EZSGim6A8E5", "00000011005106741721b4ef4858a3ce79293d655ae40b05e9ff35f52f7d34742a9ecf0ff5000000000000005d019372c394d3f6231785052e6255a494e57f121c61f19e362eb47b1c0671b574b50000000c00000008000000000000001149f737837507403ce2d6deeb67f5829af2f31edaa25628141f06b46f69a09baf00000003433133");
            init_block!(blocks, "C14", "BLUhY5b9mxVBWEvKLER1cRxj3MgesiQe6kM3Uk7FYJdXjr6VtMz", "0000001200894fae2becec7461e37b353d17bada20ff805ba2f4a31d66d08c062baab6046a000000000000006301a30a0c473f1bd51290bc517d0b6e4fc7cebc6a9c67b2df6521b2779ad77330f00000000c0000000800000000000000127793e3228794a33f1469fa9a1ffbcf17d6981042b334ce482512bb83887880c400000003433134");
            init_block!(blocks, "C15", "BLtTog3uz1sSmPMQPzz9JaJAur4gkvXyWDCKwJ2e2QShmP68PSu", "000000130064dad5960656533b9b8c509b348b6cf1799bc4a872326b7ddfc37bbd201ad039000000000000006401e81329b54baad30c536d56d31788a28eadbd47e726297294f3743701e559daee0000000c00000008000000000000001331d24acb48296363152649d655ea3eae24a4c8ee37ef84f9ade9f51f579ab77200000003433135");
            init_block!(blocks, "C16", "BKoPQFr7vJ74xEdThJNuFLCbtK9uTgrVFXQLrbR4mJnRBB44kZv", "00000014009acfec7874b2423950752203166d6e3cb48ab9b79e9bd2a1e97d0ebb075f8ad6000000000000006c01a358f109f59989da34234b34707db28dd7521ec128887edc9c799d783ca4d3d40000000c000000080000000000000014a2007ad6432a0e3f5283b95231477be720c7df633bda5fe18f72fe40239e4d0700000003433136");
            init_block!(blocks, "C17", "BLhhi3itJBkyk9E7sfVMV3hrU9GesRjp5ry9yfq1JNGrLpg2vqb", "00000015000b97312f2d09cab0d29233af6850be99688f7fbf9892bcca9be9098449aff4d7000000000000007201400217a9b4114f586c1126cf0432f977698a9470e85ef553efcdd353247532e50000000c00000008000000000000001572a9b4111bee0524e1f6cbb30baab169dbb791188d39a37d981997137d37496d00000003433137");
            init_block!(blocks, "C18", "BMd2BjMSvftbNGwhkH2tGaBhj2ZdXnLCRm2xhUnjLqwkGn7YFmY", "000000160082613015442518ded9a9fcae8befb54e478bce78bb629cdb8ff4aed6c8e72575000000000000007801ed08f243b90e75dc4128540cebc2fb07e2da7ebb0831f403f19c71d728682fd00000000c00000008000000000000001608410b7df6e50d4f109b2f3106f6b83db74dda86499be0450ca31b68c149358c00000003433138");
            init_block!(blocks, "C19", "BLKfxrnEoKH4hLnSWuBtm4UqyBbPdyes2WXsYUWBPY8CgA66grN", "0000001700fb722c13be8ad02ea65755f2d43a12640a54fb60ff23a6172a2745f0270de7be000000000000007e01c57bbb149a3c0a3ef07efefe9e01979ca70282e66ac1cc0a9962dd34253120e30000000c000000080000000000000017d0b286381788229175f9a70fec25c29907af8be016f1ff6e3a4303fa786441c500000003433139");
            init_block!(blocks, "C20", "BMAwCKGzPB32dTHv1vo8JfTHq1pPhumRFL1rsbXkxPcNV25bkVv", "0000001800505b8edbe383ac50aa1967ad99c35ad733438b2bb7242be265bc5762df71c614000000000000007f013da9c3ddc33e0de83c0834fd91777702c86ed062475ab308bc185603152649e10000000c000000080000000000000018b1d67a75b355d03484be551bc70dfdc1d5d44cf6bece3c13a69ab84d31d2940700000003433230");
            init_block!(blocks, "C21", "BLSry6WQkC97QUm3KH1xH8wvPpj1ApVyrQfbft6EmK3fqoUwArz", "0000001900c036e694a648430c4c5d7624ec5f5b398c8d09231a0b314539361b10fa4d6b17000000000000008401dcbdbdf6a06858b5e89cc3e50da04b17e19b08a606ed8374b762ce427e4184fc0000000c00000008000000000000001993a3a8edeea1c73aaaf3632cb1efc4de2a2c5d3e31e9cd43fb1f6ad3fef2b64b00000003433231");
            init_block!(blocks, "C22", "BM7345dMsHxP38FU195JjHJvwLSMPeVU9kksVkQAba4hx4tyHyt", "0000001a0060aecbfa55a9c035d2dfd55991feb25cefbdc680e7e3a681f773d3db71d8a0c2000000000000008e014160bfc6878dca54d2ced4f35692ac42f549f1468f8ab30c4fc07f57444620a20000000c00000008000000000000001a37e57a74ce69cd5e86578c40fc2f54c5671cb9a09621c326623f1b5bdaeab02c00000003433232");
            init_block!(blocks, "C23", "BMMEjA9ST4ij3c1nkYhuSvNGa7q7HnRykA48uBZWv7oxsGDjsvd", "0000001b00b75c7e25d14c7207b0b4aeb9a99f85bf3526148331bf15396e454b789f04fa53000000000000009001a6acdc757e075b333ab2ff347e3ecc3c36a22e645bb3d3e03081f0cbb06b87cd0000000c00000008000000000000001bf4a411e943d46b1f9bd255b67bb249de437678aa9e918c8763c32c932ecccd0000000003433233");
            init_block!(blocks, "C24", "BM3kaq57pfxKz6RynGCCLy96MX3etZj5s3w7C7yf9HYJcR1H8vo", "0000001c00d79b65f4cda769ed54d2c49eef1d8e95620843a5890b11dd38a0f0d4a47811f70000000000000091016d129942f928dd3db3df1232d1ea60854b6b02abee809250f4c4c1b24259a8880000000c00000008000000000000001c4e068a689addb55b59cea8c44beed088d0c166952d27da6e149d5193f5a37e4c00000003433234");
            init_block!(blocks, "C25", "BMNis1x6CYNvCq2BeMmc3Nt7NbMa6d5c1YVQFZMSgAxSWiCYCam", "0000001d00afe797d4e3db6de89d124323becee7bad6afc25db368fdccc6672fca8c5960a20000000000000099016627e7ef27b754f7a2da55e1ddd95ba41d69a5bcd91e997f7960a58b2b00257d0000000c00000008000000000000001da658abcfb4de40b0305e6572063239c0a513f4aa89de9b7f677e383f3b2258b100000003433235");
            init_block!(blocks, "C26", "BLBgwr6PwMZ43WME3CmZcA8vJohYk2YiB85nRLHKFLkGxTiDxUT", "0000001e00dafaa732f770711144b3004ffc43671058cb49ee2f6217c5f2ff7979468c848d000000000000009d0178dab20090a51ecdbd1035f712473e0128f06bca94713f89dd846cc06e7109940000000c00000008000000000000001e0463878b1a860f4728adc366e602b8b5aa4694ed272e91d6d8e722e1b7af2edc00000003433236");
            init_block!(blocks, "C27", "BMGjBELVLUdh9DK5S7bysiuKwf1jxQCh83BTNcvpMLwj4Eo3wS5", "0000001f003e3b2bb4632a2785e73ac3677c08746f2e879b98bc29b381a33517615e313ba100000000000000a101892000f19f368255a31539b71af2999b79b59a18e24297e2f7aa54fbbc5dbea50000000c00000008000000000000001f72852881bd7d1556f999d474dcdbe344a7ce9837544792313066dc75d3b2b43500000003433237");
            init_block!(blocks, "C28", "BLsFJ2MYMPPD4USbhoJnd9tceCn3MrJPurWMCXpnzY14fvZwLyA", "0000002000cd5e1fd980055b68392cc78f125fbebe8099ef317e40e1d277be132055a6702c00000000000000a701b3044ba9ed0a427239aa2550d36f872bc1b1a485e0086fa2416a0adec2fb32a70000000c000000080000000000000020db31d918712092305e3c7e7c4b91a7d1ec8a2f86c0bc2b3bb01d4c0c2347d9b500000003433238");
            init_block!(blocks, "C29", "BLW9kaF2gtUB9D6KL1rD7iVWU2iKrgzFY8YayHvcsVfM61fYTbh", "0000002100980d41a1e110a9da69fb6948bfe0fea67ae6ead97783821d0f8e815e945fc6a000000000000000af01c512b74992b12c16bd51b318447a52125d1d880517fbebca1bb9ca3f11a80bec0000000c000000080000000000000021d7e9fff86bc1d2bd9b114f0f286ecae4ce3441471ccaf778953446414b20236800000003433239");
            init_block!(blocks, "C30", "BLbkbMee34DBHSAQuHSaVhWG5ijriCkiose2FSAfKVJCnWyHRyx", "00000022006826d89496a6b572f8a10146d6f0b44ada67da157e045ed9d44be4ca768c173e00000000000000b501c3416bc7d128898aa14253b5f353d96afa8d58958ad7cfc3c3c005888bf25dc00000000c000000080000000000000022995ff621f8cbd3fc36426aeabd5e9e3b0fc5ad6cba6bb513668a321f5483001e00000003433330");
            init_block!(blocks, "C31", "BLpNA2VPt9UQnAw7aqZK9ZMtTvbCuWcwxrwmhTHECLiVeLEsqwB", "000000230074de6c045cbe91c1c1edbe46d32cdae168af7539d47315f88ef66016f3bc244000000000000000bd01adb6bcd5ea92d3947e8ac7d50fa86ffaf3dd76f1170a8843f403c3e3f4d32fbb0000000c00000008000000000000002381badbbbc330c84a7fab3a6b586c3b87af9aaaf4c0a6355010e427a8334b51d800000003433331");
            init_block!(blocks, "C32", "BLkctALdiGp8H76WzgK7UdhdwYvTvv3HeksmmaEVrmjWyRNGWqn", "0000002400918230dd398ca14eb90b4eabbb1e194e7311392e1542b20d2c8d3b48f9e9f55800000000000000bf010c23b5c1d0be3e5e2ae86a18061e77c7863d8ac649cc4b77cadee6e332cf65390000000c0000000800000000000000246a5d5916e647e0561e1d2145d9d6fbe9ebb4f8678833bd192cc67249102e01b000000003433332");
            init_block!(blocks, "C33", "BMLmtJZAoGc5ZTPzEnWhHu7AmUWWAqEADv7kJMtedonNGZAQVfw", "00000025008900a9c3b83551d2d939d5776f00fb97b16053b2d2e0d7f5d0610f7549387f7b00000000000000c1012751e956ab367aa880ffec28ec3a6e87f6e89eed1394f6432d085dcc43e2ae870000000c0000000800000000000000255216a1e9c775b977db0c7f58a72377977cfd8201e3a8b72aacefe72a6ca4b12f00000003433333");
            init_block!(blocks, "C34", "BLZ6mscLGrrM6uCxG1YoBsVVJUBk9UiAPvhEX569FJrbkNgEk7H", "0000002600d68e620729beccc21cc25b0114f7c35c85b2d5719a8f4a60844e27de7f1ef97b00000000000000c901da2bb0196e325b7e2e64493058832011267add88f9f606a41be9c0b569a5bae30000000c00000008000000000000002676f087b19a04af5ded2ee1a7b9017b472beca31b325f42a88032e7a67a60006100000003433334");
            init_block!(blocks, "C35", "BMFFjBGTKHNfoqhumbyMWDJxxKHboX36hbjVkFLzAUa5UVZs2zo", "00000027006ed8d7a317eb0a9f91f86cb1548023fd1ebd1f5b59bba0bc53a1fd41ed09735e00000000000000cb010b8d794de4a9f570e64c4e531cda7d39715c80458513b382110036e4aaa7a6500000000c000000080000000000000027f3e923a91992e33e4c8f40bcf1a54ba717a80ce22f559624a2bc8d20ecd25b5200000003433335");
            init_block!(blocks, "C36", "BLMUyVry1d24zkKWArZDVKsgQ3Q4vTHD49WWsP5FuNEP6NVPkFN", "0000002800ca05bf6d303ccbf4e5cda6d86af5c7f9ed2b3de154c7264cf57d581463e1d5e300000000000000cc0132900d83bc12efd04a46f585f6e29a34a86b88147629fe04cb602ee9ab5473b60000000c000000080000000000000028d3985419dfd09d41bf7d0f268b6a4575dc6a0ff6e5d23c720854f959206ccba200000003433336");
            init_block!(blocks, "C37", "BMPbdtU5JKvesM3RY6qjwuNjE8fxdv8bJ1BbGXhZdcriHnsHRBT", "00000029005477fc099ccdc2bbb9f35b20ffaf2725e7267a31cdb572221daaae7b02b8666300000000000000ce010c2fefd393cf880a7ed1f2a6530b2859311f5a2136dae7969d7cdfa31d3168860000000c000000080000000000000029ce5134834013ba0139555277eb17286121fa8bc072e2efd00f8bb774e146b37c00000003433337");
            init_block!(blocks, "C38", "BMaYg4xS2j5NCSEPRjSAJZ2jSx9KkEUatbm8NJr6rk2ZnZKnCrT", "0000002a00dcf782697587a5d2e5540e7b2fae363ab91ea9f4fa656473a49ee944a46724fc00000000000000d501132efd9278381c251d1d242d99f68abbd78a234f9b90b59371cd4cfc1a6c1db70000000c00000008000000000000002aed9549860de8cfc4ae5ed55b2ac16a02194a80724facf14a8afa52beeb126bba00000003433338");
            init_block!(blocks, "C39", "BL9MkkS7jbuNMe6oyJYGqCro6JXJFyj5Q6PN9t8x6wNqk42sWbs", "0000002b00f5d3e47fe9b469e9eb4abffe1872b3a92e46d7a7a9535cdf92b25d558130618700000000000000dd01f6cc00542330b0e7de86e82a5c06a48bb908387c8c85c9f24b2d69f730c319690000000c00000008000000000000002b83274b587670b7760c84ce5ed83ca88eb5a4eb0eb74bd88e1ed59d7561201a3900000003433339");
            init_block!(blocks, "C40", "BLUAdVpZCYp78QdgU2tiWKPqk4JjrPvWN7mfPojT8fVesymT6qi", "0000002c0038f04680cbb4f45cda6f00adfa1290c3f9283508b803f67b0f4240de3935b7b800000000000000e601377ac01830696e6efcb3d452fe9f726f034beedffd71effa5e8a05243c77908b0000000c00000008000000000000002cf12beb2fcb904f140b8613eae2a83f74e2ded69baedf2b51f8bc615575c2db8600000003433430");
            init_block!(blocks, "C41", "BL2YFd6KtLrY8gjTpQQTGm5UBZx6bpF5guwbB9EcwN9TE9pzrQe", "0000002d0063a5163274667d6502e4265f0fbe39a5a4c1b5607c2b404b5814635e8934de5b00000000000000ef015c2be78435e391bcd2db71ac6f6ad9e357a5d563246497c0fd4346a4adf7e6d80000000c00000008000000000000002d28210dc2a3e3da82961b314ac6d05def514ce51b3c28c2549277954edc0b1b2d00000003433431");
            init_block!(blocks, "C42", "BL1Pe2p3WdkfomHiK6GKhUb84t1CFX51DyeLkmqmjELYN4WLseq", "0000002e00297487a8477b2f7846b7b518936b5d114b507b7d8226f1b53ed49cf10889a9c300000000000000f9018c90282935c4e470ea4cfea7b447a1b569178204d872868a508527ad801cd25b0000000c00000008000000000000002e8605ea4d886eb2b9b33612fa0f0be5e21a323b4328b44b65d86feaf92fc5cd8b00000003433432");
            init_block!(blocks, "C43", "BMeCjqzjuoxP8zX5qsYU98kbqKktXF12eZG3cKdVeGFQEiW6sZ1", "0000002f0026d8ec994ba302c55a5b04f6d8d31393487252f400e26c4debaf8e7fb40d74be00000000000000fa0153d186a48cd565e301d54c5669b6f40a434f083b913cd0e64b6567066a73c6710000000c00000008000000000000002f5ddd68f6ad7a196c3df6a0e548987bb4682677a0f9100c69b066ee0eabf21c3000000003433433");
            init_block!(blocks, "C44", "BLDkMSC6Zq6cnfy5hbxPJ75phdLQX9byJh5YqgXrY25C7Xdn4nC", "0000003000fe2138a4f6f62ac2f78194c8e7117c96b36b397f4a4f5dc57d214430352a56db00000000000000fb01a71ad20e1c9be4cfc34a21f6958f65070d909fa68ad99f20973b3980535c04f50000000c0000000800000000000000308ab1b2eb923b08fe5e8a7ea477726c6b81ed987f3fa3f8a04428a5e699eb5abd00000003433434");
            init_block!(blocks, "C45", "BL5MMUTAAdHAy5a4A2TXAHanBTKNW8cb5zLAhvdSowVScACo2uZ", "000000310042e7df129523f359fe10160672797106af7ef79c60324b64dc49f268c5cb706b00000000000000fd01bacdd272d36abceda150531353d10b7a4dd9c7c6c1cbcafe6d8fd42b217fd4900000000c000000080000000000000031470a9d42ab6b9b39be0aec4ca356c501ee0205cde0d19ed56de7ae78ce5b461200000003433435");
            init_block!(blocks, "C46", "BMbFmD8FJCPBDuK2D3CikttJ3dh6ARVSN62YRaNtuZQpA6PNN4X", "00000032002fd722e1a9f9e9540339fa6ee52059d9f1ff3abc7031dd1748c239aa7648e0270000000000000107012071daf704d52361d9b644be9e52a1ab53fda31e1a802245b5247a9448d102c90000000c000000080000000000000032eca3ab01ca7bef74ce7a22c1a14abf144667e714546a0751f11527ddf362664000000003433436");
            init_block!(blocks, "C47", "BL1Lc69pokh8C3GczpKchhSECEHgLWvMfh1atXt29NBVsu4N154", "0000003300f76faf6b3c115fede24f44314238a36fc0eeb915fdd035f8b796d78169ba0c14000000000000010f011d2bccca802a7fdf56a5c442d4da4cc119a1e28fb84bbed8d3ea4599a9ced8e80000000c00000008000000000000003302b690e499ff4e1a6e8ca47bf64b4762c095c17746fa179cb3e5101c60bb2fc000000003433437");
            init_block!(blocks, "C48", "BMZRdcugWgXq4XqCP5EFMduaTKTcurKQ2NLkU7RDjXzGVtq4he1", "000000340026ba85bc8855fd67deb6b45251ebed495e922be5be26bbabcea5525c2f5182e7000000000000011101ec9c26e6304cc8155807232adf62f12df13941059feb7fceba038968ebc675490000000c000000080000000000000034fdecd76ef349624994e25ceb9160f7350d975e6ecf54d7027df21755dfd67cf600000003433438");
            init_block!(blocks, "C49", "BMcuzuh4TbeftaMosQZ1mhdRVimLSQUKTaavApmhADpRdq4ten2", "0000003500f3480921b07baf30ce41b667e6c60a5f51f7747ee0357b95241c131d47fe18db000000000000011601dd8ded3fbd18b122f8282ac50994d73146b7a29b156ae06c3eac787488ab908a0000000c000000080000000000000035e834b3fa4680d7cfe104adbfab747fac1bbb4ef66402f1d18f22353859cfeda500000003433439");
            init_block!(blocks, "C50", "BLyVJFHoyUAiuCiCtpkSEnEC1rA8hWucmQ6dVtByuSJh46psYzS", "0000003600fb342b7e6928076ac909ecf6da2717b25331245658e66c6d1654d7e33e3c7c57000000000000011a0145516888c417c4a23350328b70ab8704d65911fc5114f7ce6a35d8f37e25ba330000000c000000080000000000000036d22344c9de5db30423993451dd6ba9d154b73f76def7cbd17b3562800b9fac6b00000003433530");
            init_block!(blocks, "C51", "BKjCguoaSb9H3tECJ8ZEVx6Sru19LMkhDZwQHagnREa96kWY7gM", "0000003700a639479516f50b9b8747615874eb925361ac9d27ca0417a1a694082c0a2ff45d000000000000012101df90d6be232434bb8b4135d0d45a8cb6036351cc173e575fa584ecd4d717b81c0000000c00000008000000000000003704959ad03f42089684bfc347e27f0ed315c1d06d32cc2edda3205bab79b3431700000003433531");
            init_block!(blocks, "C52", "BLvcqnGrUAPrXAuPan62z3tkn16hQxsCNnuCbacf2rVDvG4PGky", "00000038000216b5c36404ab9b9763f69a031bda505ecfc603c3f06ec378210827db614692000000000000012801dc0e2151df4d72ed3566849832eadbf5a269b19cbe71a9f946faf9779e74afeb0000000c000000080000000000000038ce1aaaa0af5a65dfc3f4c900b4a6a4f88bffd2127c8b0d91b7251fa2d793d2e800000003433532");
            init_block!(blocks, "C53", "BKwTjzuZV1eGYPUmm1wjuxuM35spAVtgtg8jaDdt4aEBoayd97T", "00000039009fb50b847cb84b6af13e9271ac6c906dab33f44d150d541eed72ebc2c2e44a9f000000000000013001a05bd8f8bc72c59648ae77c6c719bf1afdf5ab4a134b5e0c87431a2bfdd9f3640000000c000000080000000000000039be993097ff66814e6ee38ac19afee06ccd4f3cac35a246a9ab99cf0c9c3c170200000003433533");
            init_block!(blocks, "C54", "BMe3DvJ4CLgr5Mcvck7WS7iPz5iWyUjGtqcFEk4pDuRydrffgtd", "0000003a001deced1f981f801d928854c126a9178e34d2c628969466969a573adc05e69011000000000000013201347c19bce168a62293c60ce5120bee9379dda431c04fff7d3c9f36e3a12c03300000000c00000008000000000000003a75008a556df28c942270d74087012d48d236ba40d77ff19dcf6cd7c5a0c46b1600000003433534");
            init_block!(blocks, "C55", "BMe2TEQhegQpJgCJGFupahnGesRWyzyL4TDzjoPQ6Hc7UxLuJv7", "0000003b00fdc1da0b5e5e0f36d7c6b127c9ce07fa619dda9358cc34a17294f7cc3200428e0000000000000135018de390eaa82ad17b10c43efc9f261096d8fe48f6b3d427bfe464e0255150fb4b0000000c00000008000000000000003b66a3ec3228c47d7cc3efb4b6fddf8b396442dc3ca5c00e950342ad3674ab9c2500000003433535");
            init_block!(blocks, "C56", "BMFjwwLNS2pPQCPJcUzLsanRuEXLqX7svKGJErfZz2CXVbho34v", "0000003c00fdba2145a6f2d79d62c8f89bd996c04debe3bace9ad11a64fbd5479606370767000000000000013e01514988daf2e095eeffc197f80a4fc7eeb1f45a94786618fb6967987d1c8b37090000000c00000008000000000000003ceb6eef17bd5a7e0e90637b3c998892b1c5ff55cd356db0ff574841c91efd10c300000003433536");
            init_block!(blocks, "C57", "BLCkjArU4t4LXTeuitYSfCw9jQX1gc9efcpeeUAibKH1fPetGrZ", "0000003d00cb2091f718d84033ec34be04e7a27a1a698f8d5a75cbcb1bbc5b0a4f866bf5780000000000000144013a76948f63f5b84e6a6e0025ac418f5efda6a4c5359d1fe0d7c795defb291a590000000c00000008000000000000003da36cb411145ad7337688761e012d6506c90e25ec988b6f983eacd32f3a91f11600000003433537");
            init_block!(blocks, "C58", "BLYJGvLSDWSCr5gx2NixGCuFTQBQqJzoFLozMuTFVG3bRWvGbCv", "0000003e0040a6590b9fa1914cef2d4d4a1dcb0744082ccb2a8e540119ecb311ab6a732e0e000000000000014b0170b8ad343c1970e40a5f05999ebf29d9a6857c5944384c5f7bf6dba511db48cb0000000c00000008000000000000003ec4ec032fba9b3e0f7da2c0d098e44c120e95b22057f3fd5a2c917232a576cb4400000003433538");
            init_block!(blocks, "C59", "BMbyee6fLK6tit6ERVSWBGXNbZ4rDjUYGRFuiM7EAkztHEv7D5t", "0000003f006d06d325b0e7ff5c52e98ccd8f34b6244a37776bb32ba610144589ce9aaf879c000000000000014d01439b8ec5ba5a5a2778aaf75c4e97311fd97ffa4e60cee98d1f54488469272fc60000000c00000008000000000000003f948edacc677face22fa96d5d16cc07dec023f4d4113b1a3a882bbd447bf95e8300000003433539");
            init_block!(blocks, "C60", "BLnaZ6mmEKTpRr4BT7Du9cMfRzLRc9cu5CH4iSagRNnW1uTPjpv", "0000004000f91379fa16e9d157f42bd7f0d127f808a64269cf3689295010882b95eb479765000000000000015201542c59268cc1d6e005e936ff603ed4087ec5d258125c88d458ce56d24d3dc2670000000c0000000800000000000000406fd7d849180d0f9d7130c0a29903b73055bf359099d8728863b0c7244f14741000000003433630");
            init_block!(blocks, "C61", "BKvquefVTbCA1U5JXcUgxorXSqqufJyrnBXKsv1Uqz8Rh7yGiwY", "00000041008d73e24a7cbec1ceb084ccad7c4aee48cb9f4c0e45c1e73b6982d7cfb807ee3b000000000000015501c19a3aff0e22fe364d29bc97e1ff40f958e88fba666f8ca86a8afa86ae3e3c140000000c00000008000000000000004135b2016f0cf272eb96301a6b058d5b2f6fca697c7b55ee111801caf8c64a92cc00000003433631");
            init_block!(blocks, "C62", "BLrJE6yTjLLuEYgyqLxDBduEuA5S1uCkiq499tCK81TLoqTbNmm", "00000042001c85ccc235859958299792942c2b181cb80a5ec25597e2fdeda9def54e699bbb000000000000015e01913e75202147d6cad8a51aa3396844ba3a425f2173df24b6571509d2f3c403d80000000c0000000800000000000000426adb74d5a5336cc1a5917365dcc880eedd6743f3baa25a7499e971e293dff96e00000003433632");

            BlocksDb { blocks }
        }

        pub(crate) fn store_branch(
            branch: &[&str],
            chain_id: &ChainId,
            blocks: &BlocksDb,
            block_storage: &BlockStorage,
            block_meta_storage: &BlockMetaStorage,
            log: &Logger,
        ) {
            let head_branch = branch.iter().fold1(|predecessor, successor| {
                let new_block = blocks.header(predecessor);
                block_storage
                    .put_block_header(&new_block)
                    .expect("failed to store block");
                let meta = block_meta_storage
                    .put_block_header(&new_block, &chain_id, &log)
                    .expect("failed to store block metadata");
                block_meta_storage
                    .store_predecessors(&new_block.hash, &meta)
                    .expect("failed to store block predecessors");

                let new_block = blocks.header(successor);
                block_storage
                    .put_block_header(&new_block)
                    .expect("failed to store block");
                let meta = block_meta_storage
                    .put_block_header(&new_block, &chain_id, &log)
                    .expect("failed to store block metadata");
                block_meta_storage
                    .store_predecessors(&new_block.hash, &meta)
                    .expect("failed to store block predecessors");

                successor
            });
            assert_eq!(head_branch.unwrap(), branch.iter().last().unwrap());

            // check stored correctly (multi successors)
            branch.iter().fold1(|predecessor, successor| {
                let predecessor_hash = blocks.block_hash(predecessor);
                assert!(block_meta_storage
                    .contains(&predecessor_hash)
                    .expect("block not stored"));

                let meta = block_meta_storage
                    .get(&predecessor_hash)
                    .expect("block metadata not stored");
                assert!(meta.is_some());
                let meta = meta.unwrap();

                // should contains successor
                let successor_hash = blocks.block_hash(successor);
                assert!(meta.successors().contains(&successor_hash));

                successor
            });
        }

        pub(crate) fn generate_key_string(c: char) -> CryptoboxPublicKeyHash {
            std::iter::repeat(c)
                .map(|c| c as u8)
                .take(HashType::CryptoboxPublicKeyHash.size())
                .collect::<Vec<_>>()
                .try_into()
                .unwrap()
        }

        pub(crate) fn assert_history(
            expected_history: &[&'static str],
            blocks: &BlocksDb,
            history: Vec<BlockHash>,
        ) {
            assert_eq!(
                expected_history.len(),
                history.len(),
                "Expected: {:?}, but got: {:?}",
                expected_history,
                history
                    .iter()
                    .map(|h| blocks.name(h))
                    .collect::<Vec<String>>()
            );

            let expected_block = |idx: usize| -> BlockHash {
                let b = expected_history[idx];
                blocks.block_hash(b)
            };

            for (idx, bh) in history.iter().enumerate() {
                assert_eq!(
                    expected_block(idx),
                    *bh,
                    "Expected: {}, but got: {}",
                    expected_history[idx],
                    blocks.name(bh)
                );
            }
        }
    }
}
