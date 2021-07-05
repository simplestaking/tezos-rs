// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Temporary bootstrap state which helps to download and validate branches from peers
//!
//! - every peer has his own BootstrapState
//! - bootstrap state is initialized from branch history, which is splitted to partitions
//! - it is king of bingo, where we prepare block intervals, and we check/mark what is downloaded/applied, and what needs to be downloaded or applied

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use riker::actors::*;
use slog::{info, warn, Logger};

use crypto::hash::{BlockHash, ChainId};
use networking::PeerId;
use storage::BlockHeaderWithHash;
use tezos_messages::p2p::encoding::block_header::Level;

use crate::peer_branch_bootstrapper::{
    PeerBranchBootstrapperConfiguration, PeerBranchBootstrapperRef,
};
use crate::shell_channel::{ShellChannelMsg, ShellChannelRef, ShellChannelTopic};
use crate::state::data_requester::{DataRequester, DataRequesterRef};
use crate::state::peer_state::DataQueues;
use crate::state::synchronization_state::PeerBranchSynchronizationDone;
use crate::state::{ApplyBlockBatch, StateError};

type BlockRef = Arc<BlockHash>;

pub enum AddBranchState {
    /// bool - was_merged - true was merged to existing one, false - new branch
    Added(bool),
    /// Means ignored
    Ignored,
}

/// BootstrapState helps to easily manage/mutate inner state
pub struct BootstrapState {
    /// Holds peers info
    pub(crate) peers: HashMap<ActorUri, PeerBootstrapState>,

    /// Holds unique blocks cache, shared for all branch bootstraps to minimalize memory usage
    block_state_db: BlockStateDb,

    /// Data requester
    data_requester: DataRequesterRef,

    /// Shell channel
    shell_channel: ShellChannelRef,
}

impl BootstrapState {
    pub fn new(data_requester: DataRequesterRef, shell_channel: ShellChannelRef) -> Self {
        Self {
            peers: Default::default(),
            block_state_db: BlockStateDb::new(512),
            data_requester,
            shell_channel,
        }
    }

    pub fn peers_count(&self) -> usize {
        self.peers.len()
    }

    pub fn clean_peer_data(&mut self, peer_actor_uri: &ActorUri) {
        if let Some(mut state) = self.peers.remove(peer_actor_uri) {
            state.branches.clear();
        }
    }

    pub fn check_stalled_peers<DP: Fn(&PeerId)>(
        &mut self,
        cfg: &PeerBranchBootstrapperConfiguration,
        log: &Logger,
        disconnect_peer: DP,
    ) {
        let stalled_peers = self.peers
            .values()
            .filter_map(|PeerBootstrapState { empty_bootstrap_state, peer_id, peer_queues, .. }| {
                let mut is_stalled = None;
                if let Some(empty_bootstrap_state) = empty_bootstrap_state.as_ref() {
                    // 1. check empty bootstrap branches
                    if empty_bootstrap_state.elapsed() > cfg.missing_new_branch_bootstrap_timeout {
                        is_stalled = Some((peer_id.clone(), format!("Peer did not sent new curent_head/current_branch for a long time (timeout: {:?})", cfg.missing_new_branch_bootstrap_timeout)));
                    }
                }
                // 2. check penalty peer for not responding to our block header requests on time
                if is_stalled.is_none() {
                    match peer_queues.find_any_block_header_response_pending(cfg.block_header_timeout)
                    {
                        Ok(response_pending) => {
                            if let Some((pending_block, elapsed)) = response_pending {
                                is_stalled = Some((peer_id.clone(), format!("Peer did not respond to our request for block header {} on time (elapsed: {:?}, timeout: {:?})", pending_block.to_base58_check(), elapsed, cfg.block_header_timeout)));
                            }
                        }
                        Err(e) => {
                            warn!(log, "Failed to resolve, if block response pending, for peer (so behave as ok)";
                                                "reason" => format!("{}", e),
                                                "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
                        }
                    }
                }

                // 2. check penalty peer for not responding to our block header requests on time
                if is_stalled.is_none() {
                    match peer_queues
                        .find_any_block_operations_response_pending(cfg.block_operations_timeout)
                    {
                        Ok(response_pending) => {
                            if let Some((pending_block, elapsed)) = response_pending {
                                is_stalled = Some((peer_id.clone(), format!("Peer did not respond to our request for block operations {} on time (elapsed: {:?}, timeout: {:?})", pending_block.to_base58_check(), elapsed, cfg.block_operations_timeout)));
                            }
                        }
                        Err(e) => {
                            warn!(log, "Failed to resolve, if block operations response pending, for peer (so behave as ok)";
                                                "reason" => format!("{}", e),
                                                "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
                        }
                    }
                }
                is_stalled
            })
            .collect::<Vec<_>>();

        for (peer_id, reason) in stalled_peers {
            warn!(log, "Disconnecting peer, because of stalled bootstrap pipeline";
                       "reason" => reason,
                       "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());

            self.clean_peer_data(peer_id.peer_ref.uri());
            disconnect_peer(&peer_id);
        }
    }

    pub fn block_apply_failed(&mut self, failed_block: &BlockHash, log: &Logger) {
        self.peers
            .values_mut()
            .for_each(|PeerBootstrapState { branches, peer_id, empty_bootstrap_state, .. }| {
                branches
                    .retain(|branch| {
                        if branch.contains_block(&failed_block) {
                            warn!(log, "Peer's branch bootstrap contains failed block, so this branch bootstrap is removed";
                               "block_hash" => failed_block.to_base58_check(),
                               "to_level" => &branch.to_level,
                               "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
                            false
                        } else {
                            true
                        }
                    });

                if branches.is_empty() && empty_bootstrap_state.is_none() {
                    *empty_bootstrap_state = Some(Instant::now());
                }
            });

        // remove from cache state
        self.block_state_db
            .remove_with_all_predecessors(failed_block);
    }

    pub fn blocks_scheduled_count(&self) -> usize {
        self.block_state_db.blocks.len()
    }

    pub fn block_intervals_stats(&self) -> (usize, (usize, usize, usize)) {
        self.peers
            .iter()
            .fold((0, (0, 0, 0)), |stats_acc, (_, peer_state)| {
                peer_state.branches.iter().fold(
                    stats_acc,
                    |(
                        branches_count,
                        (
                            mut intervals_count,
                            mut intervals_blocks_open,
                            mut intervals_scheduled_for_apply,
                        ),
                    ),
                     branch| {
                        for interval in &branch.intervals {
                            intervals_count += 1;
                            if matches!(interval.state, BranchIntervalState::Open) {
                                intervals_blocks_open += 1;
                            }
                            if matches!(interval.state, BranchIntervalState::ScheduledForApply) {
                                intervals_scheduled_for_apply += 1;
                            }
                        }
                        (
                            branches_count + 1,
                            (
                                intervals_count,
                                intervals_blocks_open,
                                intervals_scheduled_for_apply,
                            ),
                        )
                    },
                )
            })
    }

    pub fn next_lowest_missing_blocks(&self) -> Vec<(usize, &BlockRef)> {
        self.peers
            .values()
            .filter_map(|peer_state| {
                Some(
                    peer_state
                        .branches
                        .iter()
                        .filter_map(|branch| {
                            // find first not downloaded interval
                            branch
                                .intervals
                                .iter()
                                .enumerate()
                                .find(|(_, interval)| {
                                    matches!(interval.state, BranchIntervalState::Open)
                                })
                                .map(|(interval_idx, interval)| (interval_idx, &interval.seek))
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect()
    }

    pub fn peers_branches_level(&self) -> Vec<(Level, &BlockRef)> {
        self.peers
            .values()
            .filter_map(|peer_state| {
                Some(
                    peer_state
                        .branches
                        .iter()
                        .filter_map(|branch| {
                            if let Some(last_interval) = branch.intervals.last() {
                                Some((branch.to_level, &last_interval.end))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect()
    }

    /// Return Some(true), if was merged to existing one, Some(false), if was add as a new branch to bootstrap, None if was ignored
    pub fn add_new_branch(
        &mut self,
        peer_id: Arc<PeerId>,
        peer_queues: Arc<DataQueues>,
        last_applied_block: BlockHash,
        missing_history: Vec<BlockHash>,
        to_level: Level,
        max_bootstrap_branches_per_peer: usize,
        log: &Logger,
    ) -> AddBranchState {
        let BootstrapState {
            peers,
            block_state_db,
            ..
        } = self;

        // add branch to inner state
        if let Some(peer_state) = peers.get_mut(peer_id.peer_ref.uri()) {
            let mut was_merged = false;
            for branch in peer_state.branches.iter_mut() {
                if branch.merge(&to_level, &missing_history, block_state_db) {
                    was_merged = true;
                    peer_state.empty_bootstrap_state = None;
                    break;
                }
            }

            if !was_merged {
                // check if new missing_history last block is already in any branch, that we do not add the same one twice or it is lower and ignore the new history
                if let Some(last_block_from_missing_history) = missing_history.last() {
                    if peer_state
                        .branches
                        .iter()
                        .any(|branch| branch.contains_block(&last_block_from_missing_history))
                    {
                        return AddBranchState::Ignored;
                    }
                }

                // we handle just finite branches from one peer
                if peer_state.branches.len() >= max_bootstrap_branches_per_peer {
                    info!(log, "Peer has started already maximum ({}) branch pipelines, so we dont start new one", max_bootstrap_branches_per_peer;
                                    "to_level" => &to_level,
                                    "branches" => {
                                        peer_state
                                            .branches
                                            .iter()
                                            .filter_map(|branch| {
                                                if let Some(last_interval) = branch.intervals.last() {
                                                    Some(format!("{} ({})", branch.to_level, last_interval.end.to_base58_check()))
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect::<Vec<_>>().join(", ")
                                    },
                                    "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
                    return AddBranchState::Ignored;
                }

                let new_branch = BranchState::new(
                    last_applied_block,
                    missing_history,
                    to_level,
                    block_state_db,
                );

                peer_state.branches.push(new_branch);
                peer_state.empty_bootstrap_state = None;
            }
            AddBranchState::Added(was_merged)
        } else {
            let new_branch = BranchState::new(
                last_applied_block,
                missing_history,
                to_level,
                block_state_db,
            );

            self.peers.insert(
                peer_id.peer_ref.uri().clone(),
                PeerBootstrapState {
                    peer_id,
                    peer_queues,
                    branches: vec![new_branch],
                    empty_bootstrap_state: None,
                    is_bootstrapped: false,
                    is_already_scheduled_ping_for_process_all_bootstrap_pipelines: false,
                },
            );
            AddBranchState::Added(false)
        }
    }

    /// Return true), if was merged to existing one, false if was not merged (missing branch)
    pub fn try_update_branch(&mut self, peer_id: Arc<PeerId>, block: &BlockHeaderWithHash) -> bool {
        let BootstrapState {
            peers,
            block_state_db,
            ..
        } = self;

        // find peer state
        if let Some(peer_state) = peers.get_mut(peer_id.peer_ref.uri()) {
            let mut was_merged = false;
            let to_level = block.header.level();
            let missing_history = vec![block.header.predecessor().clone(), block.hash.clone()];

            for branch in peer_state.branches.iter_mut() {
                if branch.merge(&to_level, &missing_history, block_state_db) {
                    was_merged = true;
                    peer_state.empty_bootstrap_state = None;
                    break;
                }
            }
            was_merged
        } else {
            false
        }
    }

    pub fn schedule_blocks_to_download(&mut self, filter_peer: &Arc<PeerId>, log: &Logger) {
        let BootstrapState {
            peers,
            block_state_db,
            data_requester,
            ..
        } = self;

        // collect missing blocks for peers
        if let Some(PeerBootstrapState {
            peer_id,
            peer_queues,
            branches,
            ..
        }) = peers.get_mut(&filter_peer.peer_ref.uri())
        {
            // check peers blocks queue
            let (already_queued, mut available_queue_capacity) = match peer_queues
                .get_already_queued_block_headers_and_max_capacity()
            {
                Ok(queued_and_capacity) => queued_and_capacity,
                Err(e) => {
                    warn!(log, "Failed to get available blocks queue capacity for peer, so ignore this run for peer"; "reason" => e,
                        "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
                    return;
                }
            };

            // find next blocks
            let mut missing_blocks = Vec::with_capacity(available_queue_capacity);
            for branch in branches {
                if available_queue_capacity == 0 {
                    break;
                }
                branch.collect_next_block_headers_to_download(
                    available_queue_capacity,
                    &already_queued,
                    &mut missing_blocks,
                    block_state_db,
                    data_requester,
                );
                available_queue_capacity = available_queue_capacity
                    .checked_sub(missing_blocks.len())
                    .unwrap_or(0);
            }

            // schedule blocks to download
            if let Err(e) =
                data_requester.fetch_block_headers(missing_blocks, peer_id, &peer_queues)
            {
                warn!(log, "Failed to schedule block headers for download from peer"; "reason" => e,
                        "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
            }
        }
    }

    pub fn schedule_operations_to_download(&mut self, filter_peer: &Arc<PeerId>, log: &Logger) {
        // collect missing blocks for peers
        if let Some(PeerBootstrapState {
            peer_id,
            peer_queues,
            branches,
            ..
        }) = self.peers.get_mut(&filter_peer.peer_ref.uri())
        {
            // check peers blocks queue
            let (already_queued, mut available_queue_capacity) = match peer_queues
                .get_already_queued_block_operations_and_max_capacity()
            {
                Ok(queued_and_capacity) => queued_and_capacity,
                Err(e) => {
                    warn!(log, "Failed to get available operations queue capacity for peer, so ignore this run for peer"; "reason" => e,
                        "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
                    return;
                }
            };

            // find next blocks
            let mut missing_blocks = Vec::with_capacity(available_queue_capacity);
            for branch in branches {
                if available_queue_capacity == 0 {
                    break;
                }
                branch.collect_next_block_operations_to_download(
                    available_queue_capacity,
                    &already_queued,
                    &mut missing_blocks,
                );
                available_queue_capacity = available_queue_capacity
                    .checked_sub(missing_blocks.len())
                    .unwrap_or(0);
            }

            // try schedule requests to p2p (also handle already downloaded block operations)
            let mut already_downloaded: HashSet<Arc<BlockHash>> = HashSet::default();
            if let Err(e) = self.data_requester.fetch_block_operations(
                missing_blocks,
                peer_id,
                &peer_queues,
                |already_downloaded_block| {
                    let _ = already_downloaded.insert(already_downloaded_block);
                },
            ) {
                warn!(log, "Failed to schedule block operations for download from peer"; "reason" => e,
                        "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
            }
            // clear already downloaded
            already_downloaded
                .drain()
                .for_each(|already_downloaded_block| {
                    self.block_operations_downloaded(&already_downloaded_block)
                });
        }
    }

    pub fn schedule_blocks_for_apply(
        &mut self,
        filter_peer: &Arc<PeerId>,
        max_block_apply_batch: usize,
        chain_id: &Arc<ChainId>,
        peer_branch_bootstrapper: &PeerBranchBootstrapperRef,
        log: &slog::Logger,
    ) {
        let BootstrapState {
            peers,
            block_state_db,
            data_requester,
            ..
        } = self;

        if let Some(PeerBootstrapState {
            peer_id, branches, ..
        }) = peers.get_mut(&filter_peer.peer_ref.uri())
        {
            // check unprocessed downloaded intervals
            let mut branches_to_remove: HashSet<Level> = HashSet::default();
            for branch in branches.iter_mut() {
                if let Err(error) = branch.schedule_next_block_to_apply(
                    max_block_apply_batch,
                    block_state_db,
                    data_requester,
                    chain_id,
                    peer_branch_bootstrapper,
                ) {
                    warn!(log, "Failed to schedule blocks for apply";
                               "reason" => error,
                               "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
                    branches_to_remove.insert(branch.to_level);
                }
            }

            // remove failed
            branches.retain(|branch| {
                let remove = branches_to_remove.contains(&branch.to_level);
                if remove {
                    warn!(log, "Removing branch from peers branch bootstrap pipelines";
                           "to_level" => &branch.to_level,
                           "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());
                }
                !remove
            });
        }
    }

    pub fn block_downloaded(
        &mut self,
        block_hash: BlockHash,
        new_state: InnerBlockState,
        log: &Logger,
    ) {
        if new_state.applied {
            self.block_applied(&block_hash, log);
        } else if new_state.operations_downloaded {
            self.block_operations_downloaded(&block_hash);
        }
    }

    pub fn block_operations_downloaded(&mut self, block_hash: &BlockHash) {
        // update pipelines - remove missing operations
        self.peers.values_mut().for_each(|peer_state| {
            peer_state
                .branches
                .iter_mut()
                .for_each(|branch| branch.block_operations_downloaded(&block_hash))
        });
    }

    pub fn block_applied(&mut self, block_hash: &BlockHash, log: &Logger) {
        let BootstrapState {
            peers,
            block_state_db,
            ..
        } = self;

        // update db
        let mut all_applied_predecessors = block_state_db.remove_with_all_predecessors(block_hash);

        // update pipelines - this trimes branch intervals
        peers.values_mut().for_each(|peer_state| {
            peer_state.branches.iter_mut().for_each(|branch| {
                branch.block_applied(&mut all_applied_predecessors, block_state_db)
            })
        });

        // check if we finished any bootstrap
        self.check_bootstrapped_branches(&None, log)
    }

    pub fn check_bootstrapped_branches(&mut self, filter_peer: &Option<Arc<PeerId>>, log: &Logger) {
        let BootstrapState {
            peers,
            shell_channel,
            ..
        } = self;

        // remove all finished branches for every peer
        for PeerBootstrapState {
            branches,
            peer_id,
            is_bootstrapped,
            empty_bootstrap_state,
            ..
        } in peers.values_mut()
        {
            // filter processing by peer (if requested)
            if let Some(filter_peer) = filter_peer.as_ref() {
                if !filter_peer.peer_ref.eq(&peer_id.peer_ref) {
                    continue;
                }
            }
            branches
                .retain(|branch| {
                    if branch.is_done() {
                        info!(log, "Finished branch bootstrapping process";
                            "to_level" => &branch.to_level,
                            "peer_id" => peer_id.peer_id_marker.clone(), "peer_ip" => peer_id.peer_address.to_string(), "peer" => peer_id.peer_ref.name(), "peer_uri" => peer_id.peer_ref.uri().to_string());

                        // send for peer just once
                        if !(*is_bootstrapped) {
                            *is_bootstrapped = true;
                            shell_channel.tell(
                                Publish {
                                    msg: ShellChannelMsg::PeerBranchSynchronizationDone(
                                        PeerBranchSynchronizationDone::new(peer_id.clone(), branch.to_level),
                                    ),
                                    topic: ShellChannelTopic::ShellCommands.into(),
                                },
                                None,
                            );
                        }

                        false
                    } else {
                        true
                    }
                });

            if branches.is_empty() && empty_bootstrap_state.is_none() {
                *empty_bootstrap_state = Some(Instant::now());
            }
        }
    }
}

/// BootstrapState helps to easily manage/mutate inner state
pub struct BranchState {
    /// Level of the highest block from all intervals
    to_level: Level,

    /// Partitions are expected to be ordered from the lowest_level/oldest block
    intervals: Vec<BranchInterval>,

    /// Kind of cache for speedup lookup for missing operations instead of looping all the time
    missing_operations: HashSet<BlockRef>,

    /// Ordered blocks for this branch, which are ready to apply
    blocks_to_apply: Vec<BlockHash>,
}

impl BranchState {
    /// Creates new pipeline, it must always start with applied block (at least with genesis),
    /// This block is used to define start of the branch
    pub fn new(
        start_block: BlockHash,
        blocks: Vec<BlockHash>,
        to_level: Level,
        block_state_db: &mut BlockStateDb,
    ) -> BranchState {
        let start_block = block_state_db.get_block_ref(start_block);
        let blocks = blocks
            .into_iter()
            .map(|block_hash| block_state_db.get_block_ref(block_hash))
            .collect::<Vec<_>>();

        BranchState {
            intervals: BranchInterval::split(start_block, blocks),
            to_level,
            missing_operations: Default::default(),
            blocks_to_apply: Default::default(),
        }
    }

    /// Returns true if any interval contains requested block
    pub fn contains_block(&self, block: &BlockHash) -> bool {
        if self.blocks_to_apply.iter().any(|b| b.eq(block)) {
            return true;
        }

        self.intervals.iter().any(|interval| {
            interval.start.as_ref().eq(block)
                || interval.end.as_ref().eq(block)
                || interval.seek.as_ref().eq(block)
        })
    }

    /// Tries to merge/extends existing bootstrap (optimization)
    /// We can merge, only if the last block has continuation in new_missing_history (H1-H2-H3 merge with H2-H3-H4-H5 results to  H1-H2-H3-H4-H5)
    pub fn merge(
        &mut self,
        new_bootstrap_level: &Level,
        new_missing_history: &[BlockHash],
        block_state_db: &mut BlockStateDb,
    ) -> bool {
        if let Some(last_interval) = self.intervals.last() {
            // we need to find this last block in new_bootstrap
            if let Some(found_position) = new_missing_history
                .iter()
                .position(|b| b.eq(last_interval.end.as_ref()))
            {
                // check if we have more elements after found, means, we can add new interval
                if (found_position + 1) < new_missing_history.len() {
                    let extended_missing_history = new_missing_history[(found_position + 1)..]
                        .iter()
                        .map(|bh| block_state_db.get_block_ref(bh.clone()))
                        .collect::<Vec<_>>();

                    // split to intervals
                    let new_intervals =
                        BranchInterval::split(last_interval.end.clone(), extended_missing_history);

                    if !new_intervals.is_empty() {
                        self.intervals.extend(new_intervals);
                        self.to_level = *new_bootstrap_level;
                        return true;
                    }
                }
            }
        }

        false
    }

    /// This finds block, which should be downloaded first and refreshes state for all touched blocks
    pub fn collect_next_block_headers_to_download(
        &mut self,
        requested_count: usize,
        ignored_blocks: &HashSet<BlockRef>,
        blocks_to_download: &mut Vec<BlockRef>,
        block_state_db: &mut BlockStateDb,
        data_requester: &DataRequester,
    ) {
        if requested_count == 0 {
            return;
        }

        // 1. we limit/allow to download next interval, only if we downloaded first interval, because we want to start block application as-soon-as-possible
        // kind of optimization, because if we downloaded first interval, we could scheduled blocks for apply,
        // which could go in parallel with downloading the rest of the chain,
        // 2. and another thing is, that if we download blocks from random intervals, history can be sampled differently and can happen,
        // that we download last blocks first and block application start just when we download the whole network
        // 3. if we find invalid block at the begining we can stop branch downloading as-soon-as-possible
        let mut is_previous_interval_downloaded = true;

        // use for shifting, if we find already applied block, we can trim the the whole intervals before and speedup boostrapping
        let mut last_applied_block = None;

        // lets iterate intervals
        for interval in self.intervals.iter_mut() {
            if !is_previous_interval_downloaded {
                // if previous interval is not downloaded, we just finish and wait
                break;
            }

            // if interval is not Open, just skip it to the next one
            if !matches!(interval.state, BranchIntervalState::Open) {
                is_previous_interval_downloaded = true;
                continue;
            }

            // shift and find next missing predecessor
            let (missing_block, applied_block) = interval.shift_and_find_next_missing_predecessor(
                &mut self.missing_operations,
                block_state_db,
                data_requester,
            );
            is_previous_interval_downloaded = !matches!(interval.state, BranchIntervalState::Open);

            // handle missing
            if let Some(missing_block) = missing_block {
                // skip already scheduled
                let can_schedule = if ignored_blocks.contains(&missing_block)
                    || blocks_to_download.contains(&missing_block)
                {
                    false
                } else {
                    true
                };
                if can_schedule {
                    blocks_to_download.push(missing_block);
                }
            }
            // handle last applied
            if let Some(applied_block) = applied_block {
                last_applied_block = Some(applied_block);
            }

            // check if we have enought
            if blocks_to_download.len() >= requested_count {
                break;
            }
        }

        // if found applied block, handle it and trim interval from the begining
        if let Some(last_applied_block) = last_applied_block {
            let mut applied_blocks =
                block_state_db.remove_with_all_predecessors(&last_applied_block);
            if !applied_blocks.contains(&last_applied_block) {
                applied_blocks.insert(last_applied_block);
            }
            self.block_applied(&mut applied_blocks, block_state_db);
        }
    }

    /// This finds block, which we miss operations and should be downloaded first and also refreshes state for all touched blocks
    pub fn collect_next_block_operations_to_download(
        &mut self,
        requested_count: usize,
        ignored_blocks: &HashSet<BlockRef>,
        blocks_to_download: &mut Vec<BlockRef>,
    ) {
        if requested_count == 0 {
            return;
        }

        for b in self.missing_operations.iter() {
            // skip already scheduled
            if ignored_blocks.contains(b) || blocks_to_download.contains(b) {
                continue;
            }

            // check result
            if blocks_to_download.len() >= requested_count {
                break;
            }

            // schedule for download
            blocks_to_download.push(b.clone());
        }
    }

    pub fn schedule_next_block_to_apply(
        &mut self,
        max_block_apply_batch: usize,
        block_state_db: &mut BlockStateDb,
        data_requester: &DataRequester,
        chain_id: &Arc<ChainId>,
        peer_branch_bootstrapper: &PeerBranchBootstrapperRef,
    ) -> Result<(), StateError> {
        // we can apply blocks just from the first "scheduled" interval
        // interval is removed all the time, when the last block of the interval is applied
        for interval in self.intervals.iter_mut() {
            // check interval state
            match interval.state {
                BranchIntervalState::Open => break,
                BranchIntervalState::Downloaded => {
                    self.blocks_to_apply.extend(
                        interval.collect_blocks_for_apply(&block_state_db, data_requester)?,
                    );
                    interval.state = BranchIntervalState::ScheduledForApply;
                    continue;
                }
                BranchIntervalState::ScheduledForApply => continue,
            }
        }

        let mut batch_for_apply: Option<ApplyBlockBatch> = None;
        let mut last_applied = None;
        let mut last_already_scheduled = None;

        // check blocks for apply - get first non-applied/non-scheduled block and create batch
        for b in self.blocks_to_apply.iter() {
            // get block state
            match block_state_db.blocks.get(b) {
                Some(_) => {
                    // skip already scheduled/applied
                    last_already_scheduled = Some(b);
                    // continue to check next block
                    continue;
                }
                None => {
                    // get actual state from db
                    match data_requester.block_meta_storage.get(&b)? {
                        Some(block_metadata) => {
                            // check if already applied
                            if block_metadata.is_applied() {
                                last_already_scheduled = Some(b);
                                last_applied = Some(b.clone());
                                continue;
                            }

                            // block should be downloaded
                            if !block_metadata.is_downloaded() {
                                return Err(StateError::ProcessingError {
                                    reason: format!(
                                        "Block should be downloaded, but it is not, block: {}",
                                        b.to_base58_check()
                                    ),
                                });
                            }

                            // check missing operations
                            if !matches!(
                                data_requester.operations_meta_storage.is_complete(&b),
                                Ok(true)
                            ) {
                                // if operations not found, stop scheduling and trigger operations download, we need to wait
                                self.missing_operations
                                    .insert(block_state_db.get_block_ref(b.clone()));
                                break;
                            }

                            // check missing predecessor
                            let predecessor = match block_metadata.take_predecessor() {
                                Some(predecessor) => predecessor,
                                None => return Err(StateError::ProcessingError {
                                    reason: format!(
                                        "Block does not have predecessor, this should not happen, block: {}",
                                        b.to_base58_check()
                                    ),
                                })
                            };

                            // add to the cache state
                            let state = BlockState {
                                predecessor_block_hash: block_state_db.get_block_ref(predecessor),
                            };

                            // block ref
                            let block_ref = block_state_db.get_block_ref(b.clone());

                            block_state_db.blocks.insert(block_ref.clone(), state);

                            // start batch or continue existing one
                            if let Some(mut batch) = batch_for_apply {
                                batch.add_successor(block_ref);
                                if batch.successors_size() >= max_block_apply_batch {
                                    // schedule batch
                                    data_requester.call_schedule_apply_block(
                                        chain_id.clone(),
                                        batch,
                                        Some(peer_branch_bootstrapper.clone()),
                                    );
                                    // start new one
                                    batch_for_apply = None;
                                } else {
                                    batch_for_apply = Some(batch);
                                }
                            } else {
                                batch_for_apply = Some(ApplyBlockBatch::start_batch(
                                    block_ref,
                                    max_block_apply_batch,
                                ));
                            }

                            last_already_scheduled = Some(b);
                        }
                        None => {
                            return Err(StateError::ProcessingError {
                                reason: format!(
                                    "Block should be already downloaded, but it is not, block: {}",
                                    b.to_base58_check()
                                ),
                            });
                        }
                    }
                }
            };
        }

        if let Some(batch) = batch_for_apply {
            // schedule last batch
            data_requester.call_schedule_apply_block(
                chain_id.clone(),
                batch,
                Some(peer_branch_bootstrapper.clone()),
            );
        }

        // remove all previous already scheduled
        if let Some(last_already_scheduled) = last_already_scheduled {
            if let Some(position) = self
                .blocks_to_apply
                .iter()
                .position(|b| b.eq(last_already_scheduled))
            {
                for _ in 0..(position + 1) {
                    if !self.blocks_to_apply.is_empty() {
                        self.blocks_to_apply.remove(0);
                    }
                }
            }
        }

        // handle last applied found
        if let Some(last_applied_block) = last_applied {
            let mut applied_blocks =
                block_state_db.remove_with_all_predecessors(&last_applied_block);
            if !applied_blocks.contains(&last_applied_block) {
                applied_blocks.insert(block_state_db.get_block_ref(last_applied_block.clone()));
            }
            self.block_applied(&mut applied_blocks, block_state_db);
        }

        Ok(())
    }

    /// Notify state requested_block_hash has been applied
    ///
    /// <BM> callback wchic return (bool as downloaded, bool as applied, bool as are_operations_complete)
    pub fn block_applied(
        &mut self,
        applied_blocks: &mut HashSet<BlockRef>,
        block_state_db: &mut BlockStateDb,
    ) {
        // 1. remove the chain before the block from self.blocks_to_apply
        let mut remove_to_block_idx_inclusive = None;
        for (block_idx, block) in self.blocks_to_apply.iter().enumerate() {
            if applied_blocks.contains(block) {
                // find highest
                remove_to_block_idx_inclusive = Some(block_idx);
            }
        }
        // we want to remove all blocks before this applied blocks, we dont need them anymore
        if let Some(remove_to_block_idx_inclusive) = remove_to_block_idx_inclusive {
            // we dont want to remove the block, just the blocks before, because at least first block must be applied
            for _ in 0..(remove_to_block_idx_inclusive + 1) {
                if !self.blocks_to_apply.is_empty() {
                    let block_hash = self.blocks_to_apply.remove(0);
                    applied_blocks.extend(block_state_db.remove_with_all_predecessors(&block_hash));
                }
            }
        }

        // 2. remove intervals, if contained in applied_blocks
        // find first interval with the applied block block
        let mut interval_to_handle = None;
        for (interval_idx, interval) in self.intervals.iter_mut().enumerate() {
            if applied_blocks.contains(&interval.start)
                || applied_blocks.contains(&interval.seek)
                || applied_blocks.contains(&interval.end)
            {
                interval_to_handle = Some((interval_idx, interval));
                break;
            }
        }

        if let Some((interval_idx, interval)) = interval_to_handle {
            // remove the whole interval, if applied block is the end
            if applied_blocks.contains(&interval.end) {
                let removed_interval = self.intervals.remove(interval_idx);
                applied_blocks
                    .extend(block_state_db.remove_with_all_predecessors(&removed_interval.start));
                applied_blocks
                    .extend(block_state_db.remove_with_all_predecessors(&removed_interval.seek));
                applied_blocks
                    .extend(block_state_db.remove_with_all_predecessors(&removed_interval.end));
            }

            // here we also need to remove all previous interval, because we dont need them anymore, when higher block was applied
            if interval_idx > 0 {
                // so, remove all previous
                for _ in 0..interval_idx {
                    let removed_interval = self.intervals.remove(0);
                    applied_blocks.extend(
                        block_state_db.remove_with_all_predecessors(&removed_interval.start),
                    );
                    applied_blocks.extend(
                        block_state_db.remove_with_all_predecessors(&removed_interval.seek),
                    );
                    applied_blocks
                        .extend(block_state_db.remove_with_all_predecessors(&removed_interval.end));
                }
            }
        };
    }

    fn block_operations_downloaded(&mut self, block_hash: &BlockHash) {
        self.missing_operations.remove(block_hash);
    }

    fn is_done(&self) -> bool {
        self.intervals.is_empty()
    }
}

enum BranchIntervalState {
    /// Interval has not downloaded all block headers
    Open,
    /// Interval has downloaded all block headers
    Downloaded,
    /// Interval is scheduled for block application
    ScheduledForApply,
}

/// Partions represents sequence from history, ordered from lowest_level/oldest block
///
/// History:
///     (bh1, bh5, bh8, bh10, bh13)
/// Is splitted to:
///     (bh1, bh5)
///     (bh5, bh8)
///     (bh8, bh10)
///     (bh10, bh13)
struct BranchInterval {
    /// Interval's state
    state: BranchIntervalState,
    /// start of interval
    start: BlockRef,
    /// actual seeking/moving block, moves from end to start
    /// it is initialized to end
    /// if seek matches start, interval is downloaded
    seek: BlockRef,
    /// end of interval
    end: BlockRef,
}

impl BranchInterval {
    fn new(start: BlockRef) -> Self {
        Self::new_start_end(start.clone(), start)
    }

    fn new_start_end(start: BlockRef, end: BlockRef) -> Self {
        Self {
            state: BranchIntervalState::Open,
            start,
            seek: end.clone(),
            end,
        }
    }

    fn split(first_block: BlockRef, mut blocks: Vec<BlockRef>) -> Vec<BranchInterval> {
        let mut intervals: Vec<BranchInterval> = Vec::with_capacity(blocks.len() / 2);

        // insert first interval
        intervals.push(BranchInterval::new(first_block));

        // now split to interval the rest of the blocks
        blocks.drain(..).for_each(|bh| {
            let new_interval = match intervals.last_mut() {
                Some(last_part) => {
                    if last_part.start.as_ref() != last_part.end.as_ref() {
                        // previous is full (has different start/end), so add new one
                        Some(BranchInterval::new_start_end(last_part.end.clone(), bh))
                    } else {
                        // close interval
                        last_part.end = bh;
                        last_part.seek = last_part.end.clone();
                        None
                    }
                }
                None => Some(BranchInterval::new(bh)),
            };

            if let Some(new_interval) = new_interval {
                intervals.push(new_interval);
            }
        });

        intervals
    }

    fn shift_and_find_next_missing_predecessor(
        &mut self,
        missing_block_operations: &mut HashSet<BlockRef>,
        block_state_db: &BlockStateDb,
        data_requester: &DataRequester,
    ) -> (Option<BlockRef>, Option<BlockRef>) {
        if !matches!(self.state, BranchIntervalState::Open) {
            // if interval is downloaded, there is nothing to do
            return (None, None);
        }

        // find the last known predecessor for stop block or genesis
        let (oldest_known_predecessor, newest_applied_found) = find_last_known_predecessor(
            &self.seek,
            &self.start,
            missing_block_operations,
            block_state_db,
            data_requester,
        );

        if let Some((oldest_known_predecessor, can_close_interval)) = oldest_known_predecessor {
            // here we move seek
            self.seek = oldest_known_predecessor;
            if can_close_interval {
                self.state = BranchIntervalState::Downloaded;
            }
        }

        if self.seek.as_ref().eq(&self.start) {
            // now we are done, we reached the start
            self.state = BranchIntervalState::Downloaded;
        }

        if !matches!(self.state, BranchIntervalState::Open) {
            (None, newest_applied_found)
        } else {
            // if we are not downloaded, we want to download the actual seek
            (Some(self.seek.clone()), newest_applied_found)
        }
    }

    fn collect_blocks_for_apply(
        &self,
        block_state_db: &BlockStateDb,
        data_requester: &DataRequester,
    ) -> Result<Vec<BlockHash>, StateError> {
        if !matches!(self.state, BranchIntervalState::Downloaded) {
            // if interval is not downloaded, there is nothing to do
            return Ok(vec![]);
        }

        // if end of interval is scheduled for apply, just finish, nothing more to apply here
        if block_state_db.blocks.contains_key(&self.end) {
            // if scheduled for apply, just finish, nothing more to apply
            return Ok(vec![]);
        }

        // start with end
        if let Some(end_metadata) = data_requester.block_meta_storage.get(&self.end)? {
            if end_metadata.is_applied() {
                // if applied, just finish, nothing more to apply
                return Ok(vec![]);
            }

            // TODO: we should maybe estimated and calculate count in interval when shifting [find_last_known_predecessor]
            // collect all blocks from end to start
            let mut interval_blocks = Vec::with_capacity(128);

            // now start add for apply
            interval_blocks.push(self.end.as_ref().clone());

            // iterate predecessor to the start
            let mut predecessor_selector = end_metadata.take_predecessor();
            while let Some(predecessor) = predecessor_selector {
                // if scheduled for apply or applied, just finish
                if block_state_db.blocks.contains_key(&predecessor) {
                    // if scheduled for apply, just finish, nothing more to apply
                    break;
                }

                // iterate predecessor's predecessor
                predecessor_selector = match data_requester.block_meta_storage.get(&predecessor) {
                    Ok(Some(predecessor_metadata)) => {
                        if predecessor_metadata.is_applied() {
                            // if applied, just finish, nothing more to apply
                            break;
                        }

                        // if reached start of interval, just finish, we are at the end of interval
                        if self.start.as_ref().eq(&predecessor) {
                            // predecessor is not scheduled or applied, so we add it
                            interval_blocks.push(predecessor);
                            break;
                        }
                        // if reached seek of interval, just finish, we are at the end of interval
                        if self.seek.as_ref().eq(&predecessor) {
                            // predecessor is not scheduled or applied, so we add it
                            interval_blocks.push(predecessor);
                            break;
                        }

                        // now iterate predecessor's predecessor
                        match predecessor_metadata.take_predecessor() {
                            Some(pred_predecessor) => {
                                if pred_predecessor.eq(&predecessor) {
                                    // predecessor is not scheduled or applied, so we add it
                                    interval_blocks.push(predecessor);

                                    // found genesis
                                    break;
                                } else {
                                    // predecessor is not scheduled or applied, so we add it
                                    interval_blocks.push(predecessor);

                                    Some(pred_predecessor)
                                }
                            }
                            None => {
                                return Err(StateError::ProcessingError {
                                    reason: format!(
                                        "Invalid interval/block - block does not have predecessor: {}",
                                        predecessor.to_base58_check()
                                    ),
                                });
                            }
                        }
                    }
                    _ => {
                        return Err(StateError::ProcessingError {
                            reason: format!(
                                "Invalid interval - missing or failed to get block data: {}",
                                predecessor.to_base58_check()
                            ),
                        });
                    }
                }
            }

            // reverse interval
            interval_blocks.reverse();

            // check if end matches interval
            let is_end_ok = if let Some(last) = interval_blocks.last() {
                self.end.as_ref().eq(last)
            } else {
                false
            };

            if is_end_ok {
                Ok(interval_blocks)
            } else {
                Err(StateError::ProcessingError {
                    reason: format!("Invalid interval, is_end_ok: {}", is_end_ok),
                })
            }
        } else {
            Err(StateError::ProcessingError {
                reason: format!(
                    "Invalid interval, missing end block: {}",
                    self.end.to_base58_check()
                ),
            })
        }
    }
}

/// Returns tuple:
///     1. option block_hash + bool, bool means can close interval and there is no need to iterate it to start
///     2. option block_hash, applied block found, we dont need to continue
fn find_last_known_predecessor(
    block_from: &BlockRef,
    block_to: &BlockRef,
    missing_operations: &mut HashSet<BlockRef>,
    block_state_db: &BlockStateDb,
    data_requester: &DataRequester,
) -> (Option<(BlockRef, bool)>, Option<BlockRef>) {
    // check if we can close interval from cache
    if block_state_db.blocks.contains_key(block_from) {
        return (Some((block_from.clone(), true)), None);
    }

    if block_to.as_ref().eq(&block_from) {
        return (Some((block_to.clone(), true)), None);
    }

    // find actual block data in database
    let predecessor_selector = match data_requester.block_meta_storage.get(&block_from) {
        Ok(Some(block_metadata)) => {
            if block_metadata.is_applied() {
                // if block is already applied, we finish, there is no need to continue
                return (Some((block_from.clone(), true)), Some(block_from.clone()));
            }

            if !block_metadata.is_downloaded() {
                return (Some((block_from.clone(), false)), None);
            } else {
                // check missing operations
                if !matches!(
                    data_requester
                        .operations_meta_storage
                        .is_complete(&block_from),
                    Ok(true)
                ) {
                    missing_operations.insert(block_from.clone());
                }

                // check predecessor
                match block_metadata.take_predecessor() {
                    Some(predecessor) => {
                        if predecessor.eq(&block_from) {
                            // stop on genesis
                            return (
                                Some((block_state_db.get_block_ref(predecessor), true)),
                                None,
                            );
                        }
                        predecessor
                    }
                    None => {
                        // should not happen, once predecessor will not be Option, it will be removed
                        return (Some((block_from.clone(), false)), None);
                    }
                }
            }
        }
        _ => {
            // if any problem or not found, we try to download the header
            return (Some((block_from.clone(), false)), None);
        }
    };

    // find in loop known predecessor
    let mut predecessor_selector = Some(predecessor_selector);
    while let Some(block) = predecessor_selector {
        // check if we can close interval from cache
        if block_state_db.blocks.contains_key(&block) {
            return (Some((block_state_db.get_block_ref(block), true)), None);
        }

        if block_to.as_ref().eq(&block) {
            return (Some((block_state_db.get_block_ref(block), true)), None);
        }

        // find and check predecessor block
        match data_requester.block_meta_storage.get(&block) {
            Ok(Some(block_metadata)) => {
                if block_metadata.is_applied() {
                    // if block is already applied, we finish, there is no need to continue
                    return (
                        Some((block_state_db.get_block_ref(block.clone()), true)),
                        Some(block_state_db.get_block_ref(block)),
                    );
                }

                if !block_metadata.is_downloaded() {
                    return (Some((block_state_db.get_block_ref(block), false)), None);
                } else {
                    // check missing operations
                    if !matches!(
                        data_requester.operations_meta_storage.is_complete(&block),
                        Ok(true)
                    ) {
                        missing_operations.insert(block_state_db.get_block_ref(block.clone()));
                    }

                    // check predecessor
                    match block_metadata.take_predecessor() {
                        Some(predecessor) => {
                            if predecessor.eq(&block) {
                                // stop on genesis
                                return (
                                    Some((block_state_db.get_block_ref(predecessor), true)),
                                    None,
                                );
                            }
                            predecessor_selector = Some(predecessor)
                        }
                        None => {
                            // should not happen, once predecessor will not be Option, it will be removed
                            return (Some((block_state_db.get_block_ref(block), false)), None);
                        }
                    }
                }
            }
            _ => {
                // if any problem or not found, we try to download the header
                return (Some((block_state_db.get_block_ref(block), false)), None);
            }
        }
    }

    (None, None)
}

#[derive(Clone, Debug)]
pub struct InnerBlockState {
    pub operations_downloaded: bool,
    pub applied: bool,
}

#[derive(Clone, Debug)]
pub struct BlockState {
    predecessor_block_hash: BlockRef,
}

/// Internal state for peer
pub struct PeerBootstrapState {
    /// Peer's identification
    pub(crate) peer_id: Arc<PeerId>,
    /// Peer's shared queues - used just for validation, that we requested some data
    peer_queues: Arc<DataQueues>,

    /// List of branches from peer
    branches: Vec<BranchState>,

    /// if peer was bootstrapped
    is_bootstrapped: bool,

    /// Indicates stalled peer, after all branches were resolved and cleared, how long we did not receive new branch
    empty_bootstrap_state: Option<Instant>,

    /// See [PeerBranchBootstrapper.is_already_scheduled_ping_for_process_all_bootstrap_pipelines]
    pub(crate) is_already_scheduled_ping_for_process_all_bootstrap_pipelines: bool,
}

/// Works like simple cache for sharing info between pipelines.
/// Stores all downloaded blocks with predecessors, which are scheduled for block application,
/// it means, that if block is found in this DB, then all his predecessors are downloaded
pub struct BlockStateDb {
    blocks: HashMap<BlockRef, BlockState>,
}

impl BlockStateDb {
    fn new(initial_capacity: usize) -> Self {
        Self {
            blocks: HashMap::with_capacity(initial_capacity),
        }
    }

    /// Checks existing BlockRef from db, to minimalize BlockHash creation in memory
    pub fn get_block_ref(&self, block_hash: BlockHash) -> BlockRef {
        if let Some((block_ref, _)) = self.blocks.get_key_value(&block_hash) {
            block_ref.clone()
        } else {
            Arc::new(block_hash)
        }
    }

    pub fn remove_with_all_predecessors(&mut self, block_hash: &BlockHash) -> HashSet<BlockRef> {
        let mut removed_blocks = HashSet::default();
        // mark as applied - remove with all known predecessors
        let predecessor =
            if let Some((block_ref, block_state)) = self.blocks.remove_entry(block_hash) {
                removed_blocks.insert(block_ref);
                Some(block_state.predecessor_block_hash)
            } else {
                None
            };

        // also mark all predecessors as applied
        let mut predecessor_selector = predecessor;
        while let Some(predecessor) = predecessor_selector {
            predecessor_selector = match self.blocks.remove_entry(&predecessor) {
                Some((predecessor_block_ref, predecessor_state)) => {
                    removed_blocks.insert(predecessor_block_ref);
                    Some(predecessor_state.predecessor_block_hash)
                }
                None => None,
            };
        }

        removed_blocks
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use networking::p2p::network_channel::NetworkChannel;

    use crate::shell_channel::ShellChannel;
    use crate::state::peer_state::DataQueuesLimits;
    use crate::state::tests::block;
    use crate::state::tests::prerequisites::{
        chain_feeder_mock, create_logger, create_test_actor_system, create_test_tokio_runtime,
        test_peer,
    };

    use super::*;
    use storage::tests_common::TmpStorage;
    use storage::{BlockMetaStorage, OperationsMetaStorage};

    // macro_rules! hash_set {
    //     ( $( $x:expr ),* ) => {
    //         {
    //             let mut temp_set = HashSet::new();
    //             $(
    //                 temp_set.insert($x);
    //             )*
    //             temp_set
    //         }
    //     };
    // }

    fn assert_interval(
        tested: &BranchInterval,
        (expected_left, expected_right): (BlockHash, BlockHash),
    ) {
        assert_eq!(tested.start.as_ref(), &expected_left);
        assert_eq!(tested.seek.as_ref(), &expected_right);
        assert_eq!(tested.end.as_ref(), &expected_right);
    }

    #[test]
    #[serial]
    fn test_bootstrap_state_add_new_branch() {
        // actors stuff
        let log = create_logger(slog::Level::Info);
        let sys = create_test_actor_system(log.clone());
        let runtime = create_test_tokio_runtime();
        let network_channel =
            NetworkChannel::actor(&sys).expect("Failed to create network channel");
        let shell_channel = ShellChannel::actor(&sys).expect("Failed to create network channel");
        let storage = TmpStorage::create_to_out_dir("__test_bootstrap_state_add_new_branch")
            .expect("failed to create tmp storage");
        let (chain_feeder_mock, _) = chain_feeder_mock(
            &sys,
            "mocked_chain_feeder_bootstrap_state",
            shell_channel.clone(),
        )
        .expect("failed to create chain_feeder_mock");
        let data_requester = Arc::new(DataRequester::new(
            BlockMetaStorage::new(storage.storage()),
            OperationsMetaStorage::new(storage.storage()),
            chain_feeder_mock,
        ));

        // peer1
        let peer_id = test_peer(&sys, network_channel, &runtime, 1234, &log).peer_id;
        let peer_queues = Arc::new(DataQueues::new(DataQueuesLimits {
            max_queued_block_headers_count: 10,
            max_queued_block_operations_count: 10,
        }));

        // empty state
        let mut state = BootstrapState::new(data_requester, shell_channel);

        // genesis
        let last_applied = block(0);

        // history blocks
        let history: Vec<BlockHash> = vec![
            block(2),
            block(5),
            block(8),
            block(10),
            block(13),
            block(15),
            block(20),
        ];

        // add new branch to empty state
        assert!(matches!(
            state.add_new_branch(
                peer_id.clone(),
                peer_queues.clone(),
                last_applied.clone(),
                history,
                20,
                5,
                &log,
            ),
            AddBranchState::Added(false)
        ));

        // check state
        let peer_bootstrap_state = state.peers.get_mut(peer_id.peer_ref.uri()).unwrap();
        assert!(peer_bootstrap_state.empty_bootstrap_state.is_none());
        assert_eq!(1, peer_bootstrap_state.branches.len());
        let pipeline = &peer_bootstrap_state.branches[0];
        assert_eq!(pipeline.intervals.len(), 7);
        assert_interval(&pipeline.intervals[0], (block(0), block(2)));
        assert_interval(&pipeline.intervals[1], (block(2), block(5)));
        assert_interval(&pipeline.intervals[2], (block(5), block(8)));
        assert_interval(&pipeline.intervals[3], (block(8), block(10)));
        assert_interval(&pipeline.intervals[4], (block(10), block(13)));
        assert_interval(&pipeline.intervals[5], (block(13), block(15)));
        assert_interval(&pipeline.intervals[6], (block(15), block(20)));

        // try add new branch as the same as before - could happen when we are bootstrapped and receives the same CurrentHead with different operations
        // history blocks
        let history: Vec<BlockHash> = vec![
            block(2),
            block(5),
            block(8),
            block(10),
            block(13),
            block(15),
            block(20),
        ];

        // add new branch to empty state
        assert!(matches!(
            state.add_new_branch(
                peer_id.clone(),
                peer_queues.clone(),
                last_applied.clone(),
                history,
                20,
                5,
                &log,
            ),
            AddBranchState::Ignored
        ));
        // check state - still 1
        let peer_bootstrap_state = state.peers.get_mut(peer_id.peer_ref.uri()).unwrap();
        assert!(peer_bootstrap_state.empty_bootstrap_state.is_none());
        assert_eq!(1, peer_bootstrap_state.branches.len());

        // try add new branch lower as before
        // history blocks
        let history: Vec<BlockHash> = vec![block(2), block(5), block(8), block(10)];

        // add new branch to empty state
        assert!(matches!(
            state.add_new_branch(
                peer_id.clone(),
                peer_queues.clone(),
                last_applied.clone(),
                history,
                20,
                5,
                &log,
            ),
            AddBranchState::Ignored
        ));
        // check state - still 1
        let peer_bootstrap_state = state.peers.get_mut(peer_id.peer_ref.uri()).unwrap();
        assert!(peer_bootstrap_state.empty_bootstrap_state.is_none());
        assert_eq!(1, peer_bootstrap_state.branches.len());

        // try add new branch merge new - ok
        let history_to_merge: Vec<BlockHash> = vec![
            block(13),
            block(15),
            block(20),
            block(22),
            block(23),
            block(25),
            block(29),
        ];
        assert!(matches!(
            state.add_new_branch(
                peer_id.clone(),
                peer_queues.clone(),
                last_applied.clone(),
                history_to_merge,
                29,
                5,
                &log,
            ),
            AddBranchState::Added(true)
        ));

        // check state - branch was extended
        let peer_bootstrap_state = state.peers.get_mut(peer_id.peer_ref.uri()).unwrap();
        assert!(peer_bootstrap_state.empty_bootstrap_state.is_none());
        assert_eq!(1, peer_bootstrap_state.branches.len());

        let pipeline = &peer_bootstrap_state.branches[0];
        assert_eq!(pipeline.intervals.len(), 11);
        assert_interval(&pipeline.intervals[0], (block(0), block(2)));
        assert_interval(&pipeline.intervals[1], (block(2), block(5)));
        assert_interval(&pipeline.intervals[2], (block(5), block(8)));
        assert_interval(&pipeline.intervals[3], (block(8), block(10)));
        assert_interval(&pipeline.intervals[4], (block(10), block(13)));
        assert_interval(&pipeline.intervals[5], (block(13), block(15)));
        assert_interval(&pipeline.intervals[6], (block(15), block(20)));
        assert_interval(&pipeline.intervals[7], (block(20), block(22)));
        assert_interval(&pipeline.intervals[8], (block(22), block(23)));
        assert_interval(&pipeline.intervals[9], (block(23), block(25)));
        assert_interval(&pipeline.intervals[10], (block(25), block(29)));

        // add next branch
        let history_to_merge: Vec<BlockHash> = vec![
            block(113),
            block(115),
            block(120),
            block(122),
            block(123),
            block(125),
            block(129),
        ];

        assert!(matches!(
            state.add_new_branch(
                peer_id.clone(),
                peer_queues.clone(),
                last_applied.clone(),
                history_to_merge,
                129,
                5,
                &log,
            ),
            AddBranchState::Added(false)
        ));

        // check state - branch was extended
        let peer_bootstrap_state = state.peers.get_mut(peer_id.peer_ref.uri()).unwrap();
        assert!(peer_bootstrap_state.empty_bootstrap_state.is_none());
        assert_eq!(2, peer_bootstrap_state.branches.len());

        // try add next branch - max branches 2 - no added
        let history_to_merge: Vec<BlockHash> = vec![
            block(213),
            block(215),
            block(220),
            block(222),
            block(223),
            block(225),
            block(229),
        ];

        assert!(matches!(
            state.add_new_branch(
                peer_id.clone(),
                peer_queues,
                last_applied,
                history_to_merge,
                229,
                2,
                &log,
            ),
            AddBranchState::Ignored
        ));
        // check state - branch was extended - no
        let peer_bootstrap_state = state.peers.get_mut(peer_id.peer_ref.uri()).unwrap();
        assert!(peer_bootstrap_state.empty_bootstrap_state.is_none());
        assert_eq!(2, peer_bootstrap_state.branches.len());
    }
    //
    // #[test]
    // fn test_bootstrap_state_split_to_intervals() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     // create
    //     let pipeline = BranchState::new(last_applied.clone(), history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     assert!(pipeline.contains_block_to_apply(&block(0)));
    //     assert!(pipeline.contains_block_to_apply(&block(2)));
    //     assert!(pipeline.contains_block_to_apply(&block(5)));
    //     assert!(pipeline.contains_block_to_apply(&block(10)));
    //     assert!(pipeline.contains_block_to_apply(&block(13)));
    //     assert!(pipeline.contains_block_to_apply(&block(15)));
    //     assert!(pipeline.contains_block_to_apply(&block(20)));
    //     assert!(!pipeline.contains_block_to_apply(&block(1)));
    //     assert!(!pipeline.contains_block_to_apply(&block(3)));
    //     assert!(!pipeline.contains_block_to_apply(&block(4)));
    //
    //     // check applied is just first block
    //     pipeline
    //         .intervals
    //         .iter()
    //         .map(|i| &i.blocks)
    //         .flatten()
    //         .for_each(|b| {
    //             let block_state = block_state_db.blocks.get(b).unwrap();
    //             if b.as_ref().eq(&last_applied) {
    //                 assert!(block_state.applied);
    //                 assert!(block_state.block_downloaded);
    //                 assert!(block_state.operations_downloaded);
    //             } else {
    //                 assert!(!block_state.applied);
    //                 assert!(!block_state.block_downloaded);
    //                 assert!(!block_state.operations_downloaded);
    //             }
    //         })
    // }
    //
    // #[test]
    // fn test_bootstrap_state_merge() {
    //     // genesis
    //     let last_applied = block(0);
    //
    //     // history blocks
    //     let history1: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline1 = BranchState::new(last_applied, history1, 20, &mut block_state_db);
    //     assert_eq!(pipeline1.intervals.len(), 7);
    //     assert_eq!(pipeline1.to_level, 20);
    //
    //     // try merge lower - nothing
    //     let history_to_merge: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //     ];
    //     assert!(!pipeline1.merge(&100, &history_to_merge, &mut block_state_db));
    //     assert_eq!(pipeline1.intervals.len(), 7);
    //     assert_eq!(pipeline1.to_level, 20);
    //
    //     // try merge different - nothing
    //     let history_to_merge: Vec<BlockHash> = vec![
    //         block(1),
    //         block(4),
    //         block(7),
    //         block(9),
    //         block(12),
    //         block(14),
    //         block(16),
    //     ];
    //     assert!(!pipeline1.merge(&100, &history_to_merge, &mut block_state_db));
    //     assert_eq!(pipeline1.intervals.len(), 7);
    //     assert_eq!(pipeline1.to_level, 20);
    //
    //     // try merge the same - nothing
    //     let history_to_merge: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //     assert!(!pipeline1.merge(&100, &history_to_merge, &mut block_state_db));
    //     assert_eq!(pipeline1.intervals.len(), 7);
    //     assert_eq!(pipeline1.to_level, 20);
    //
    //     // try merge the one new - nothing
    //     let history_to_merge: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //         block(22),
    //     ];
    //     assert!(pipeline1.merge(&22, &history_to_merge, &mut block_state_db));
    //     assert_eq!(pipeline1.intervals.len(), 8);
    //     assert_eq!(pipeline1.to_level, 22);
    //
    //     // try merge new - ok
    //     let history_to_merge: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //         block(22),
    //         block(23),
    //         block(25),
    //         block(29),
    //     ];
    //     assert!(pipeline1.merge(&29, &history_to_merge, &mut block_state_db));
    //     assert_eq!(pipeline1.intervals.len(), 11);
    //     assert_eq!(pipeline1.to_level, 29);
    //
    //     assert_interval(&pipeline1.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline1.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline1.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline1.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline1.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline1.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline1.intervals[6], (block(15), block(20)));
    //     assert_interval(&pipeline1.intervals[7], (block(20), block(22)));
    //     assert_interval(&pipeline1.intervals[8], (block(22), block(23)));
    //     assert_interval(&pipeline1.intervals[9], (block(23), block(25)));
    //     assert_interval(&pipeline1.intervals[10], (block(25), block(29)));
    // }

    // #[test]
    // fn test_bootstrap_state_block_downloaded() -> Result<(), StateError> {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline = BranchState::new(last_applied, history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     // register downloaded block 2 with his predecessor 1
    //     assert!(!pipeline.intervals[0].all_blocks_downloaded);
    //     assert_eq!(pipeline.intervals[0].blocks.len(), 2);
    //     let mut result = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         1,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut result,
    //         &block_state_db,
    //     );
    //     assert_eq!(1, result.len());
    //     assert_eq!(result[0].as_ref(), &block(2));
    //
    //     block_state_db.mark_block_downloaded(
    //         &block(2),
    //         block(1),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //
    //     let mut result = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         1,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut result,
    //         &block_state_db,
    //     );
    //     assert_eq!(1, result.len());
    //     assert_eq!(result[0].as_ref(), &block(1));
    //
    //     // register downloaded block 1
    //     block_state_db.mark_block_downloaded(
    //         &block(1),
    //         block(0),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //
    //     // get next blocks
    //     let mut result = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         3,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut result,
    //         &block_state_db,
    //     );
    //
    //     // interval 0 is closed
    //     assert!(pipeline.intervals[0].all_blocks_downloaded);
    //     assert_eq!(pipeline.intervals[0].blocks.len(), 3);
    //     assert_eq!(3, result.len());
    //     assert_eq!(result[0].as_ref(), &block(5));
    //     assert_eq!(result[1].as_ref(), &block(8));
    //     assert_eq!(result[2].as_ref(), &block(10));
    //
    //     // next interval 1 is opened
    //     assert!(!pipeline.intervals[1].all_blocks_downloaded);
    //     assert_eq!(pipeline.intervals[1].blocks.len(), 2);
    //
    //     // register downloaded block 5 with his predecessor 4
    //     block_state_db.mark_block_downloaded(
    //         &block(5),
    //         block(4),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     // register downloaded block 4 as applied
    //     block_state_db.mark_block_downloaded(
    //         &block(4),
    //         block(3),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //
    //     // first interval with block 2 and block 3 were removed, because if 4 is applied, 2/3 must be also
    //     pipeline.intervals[0].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     assert!(pipeline.intervals[0].all_blocks_downloaded);
    //     assert_eq!(pipeline.intervals[0].blocks.len(), 3);
    //     assert_eq!(pipeline.intervals[0].blocks[0].as_ref(), &block(0));
    //     assert_eq!(pipeline.intervals[0].blocks[1].as_ref(), &block(1));
    //     assert_eq!(pipeline.intervals[0].blocks[2].as_ref(), &block(2));
    //
    //     pipeline.intervals[1].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     assert!(!pipeline.intervals[1].all_blocks_downloaded);
    //     assert_eq!(pipeline.intervals[1].blocks.len(), 4);
    //     assert_eq!(pipeline.intervals[1].blocks[0].as_ref(), &block(2));
    //     assert_eq!(pipeline.intervals[1].blocks[1].as_ref(), &block(3));
    //     assert_eq!(pipeline.intervals[1].blocks[2].as_ref(), &block(4));
    //     assert_eq!(pipeline.intervals[1].blocks[3].as_ref(), &block(5));
    //
    //     Ok(())
    // }
    //
    // #[test]
    // fn test_bootstrap_state_block_applied_marking() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline = BranchState::new(last_applied, history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     // check intervals
    //     assert_eq!(pipeline.intervals.len(), 7);
    //
    //     // trigger that block 2 is download with predecessor 1
    //     block_state_db.mark_block_downloaded(
    //         &block(2),
    //         block(1),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     pipeline.intervals[0].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //
    //     // mark 1 as applied (half of interval)
    //     block_state_db.remove_with_all_predecessors(&block(1));
    //     pipeline.block_applied(&block(1), &block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     // begining of interval is changed to block1
    //     assert_eq!(pipeline.intervals[0].blocks.len(), 2);
    //     assert_eq!(pipeline.intervals[0].blocks[0].as_ref(), &block(1));
    //     assert_eq!(pipeline.intervals[0].blocks[1].as_ref(), &block(2));
    //
    //     // trigger that block 2 is applied
    //     block_state_db.remove_with_all_predecessors(&block(2));
    //     pipeline.block_applied(&block(2), &block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 6);
    //
    //     // trigger that block 8 is applied
    //     block_state_db.remove_with_all_predecessors(&block(8));
    //     pipeline.block_applied(&block(8), &block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 4);
    //
    //     // download 20 -> 19 -> 18 -> 17
    //     block_state_db.mark_block_downloaded(
    //         &block(20),
    //         block(19),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(19),
    //         block(18),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(18),
    //         block(17),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     assert!(!block_state_db.blocks.get(&block(20)).unwrap().applied);
    //     assert!(!block_state_db.blocks.get(&block(19)).unwrap().applied);
    //     assert!(!block_state_db.blocks.get(&block(18)).unwrap().applied);
    //
    //     // trigger that last block is applied
    //     block_state_db.remove_with_all_predecessors(&block(20));
    //
    //     // direct predecessors should be marked as applied
    //     assert!(block_state_db.blocks.get(&block(20)).unwrap().applied);
    //     assert!(!block_state_db.blocks.contains_key(&block(19)));
    //     assert!(!block_state_db.blocks.contains_key(&block(18)));
    //
    //     // mark pipeline - should remove all intervals now
    //     pipeline.block_applied(&block(20), &block_state_db);
    //
    //     // interval 0 was removed
    //     assert!(pipeline.is_done());
    // }
    //
    // #[test]
    // fn test_collect_next_blocks_to_download() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline = BranchState::new(last_applied, history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     // check 2. inerval -  is not downloaded
    //     assert!(!pipeline.intervals[1].all_blocks_downloaded);
    //     assert_eq!(2, pipeline.intervals[1].blocks.len());
    //
    //     // try to get blocks for download - max 5
    //     let mut blocks_to_download = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         5,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut blocks_to_download,
    //         &block_state_db,
    //     );
    //     assert_eq!(5, blocks_to_download.len());
    //     assert_eq!(blocks_to_download[0].as_ref(), &block(2));
    //     assert_eq!(blocks_to_download[1].as_ref(), &block(5));
    //     assert_eq!(blocks_to_download[2].as_ref(), &block(8));
    //     assert_eq!(blocks_to_download[3].as_ref(), &block(10));
    //     assert_eq!(blocks_to_download[4].as_ref(), &block(13));
    //
    //     // try to get blocks for download - max 2
    //     let mut blocks_to_download = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         2,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut blocks_to_download,
    //         &block_state_db,
    //     );
    //     assert_eq!(2, blocks_to_download.len());
    //     assert_eq!(blocks_to_download[0].as_ref(), &block(2));
    //     assert_eq!(blocks_to_download[1].as_ref(), &block(5));
    //
    //     // try to get blocks for download - max 2 interval with ignored
    //     let mut blocks_to_download = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         2,
    //         &configuration(),
    //         &hash_set![block_ref(5)],
    //         &mut blocks_to_download,
    //         &block_state_db,
    //     );
    //     assert_eq!(2, blocks_to_download.len());
    //     assert_eq!(blocks_to_download[0].as_ref(), &block(2));
    //     assert_eq!(blocks_to_download[1].as_ref(), &block(8));
    // }
    //
    // #[test]
    // fn test_collect_next_blocks_to_download_feed_interval_on_collect() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //         block(25),
    //         block(30),
    //         block(35),
    //         block(40),
    //         block(43),
    //         block(46),
    //         block(49),
    //         block(52),
    //         block(53),
    //         block(56),
    //         block(59),
    //         block(60),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline1 = BranchState::new(
    //         last_applied.clone(),
    //         history.clone(),
    //         60,
    //         &mut block_state_db,
    //     );
    //     assert_eq!(pipeline1.intervals.len(), 19);
    //     assert_interval(&pipeline1.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline1.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline1.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline1.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline1.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline1.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline1.intervals[6], (block(15), block(20)));
    //     assert_interval(&pipeline1.intervals[7], (block(20), block(25)));
    //     assert_interval(&pipeline1.intervals[8], (block(25), block(30)));
    //     assert_interval(&pipeline1.intervals[9], (block(30), block(35)));
    //     assert_interval(&pipeline1.intervals[10], (block(35), block(40)));
    //     assert_interval(&pipeline1.intervals[11], (block(40), block(43)));
    //     assert_interval(&pipeline1.intervals[12], (block(43), block(46)));
    //     assert_interval(&pipeline1.intervals[13], (block(46), block(49)));
    //     assert_interval(&pipeline1.intervals[14], (block(49), block(52)));
    //     assert_interval(&pipeline1.intervals[15], (block(52), block(53)));
    //     assert_interval(&pipeline1.intervals[16], (block(53), block(56)));
    //     assert_interval(&pipeline1.intervals[17], (block(56), block(59)));
    //     assert_interval(&pipeline1.intervals[18], (block(59), block(60)));
    //
    //     // try to get schedule blocks for download
    //     let mut blocks_to_download1 = Vec::new();
    //     pipeline1.collect_next_block_headers_to_download(
    //         15,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut blocks_to_download1,
    //         &block_state_db,
    //     );
    //     assert_eq!(15, blocks_to_download1.len());
    //     assert_eq!(blocks_to_download1[0].as_ref(), &block(2));
    //     assert_eq!(blocks_to_download1[1].as_ref(), &block(5));
    //     assert_eq!(blocks_to_download1[2].as_ref(), &block(8));
    //     assert_eq!(blocks_to_download1[3].as_ref(), &block(10));
    //     assert_eq!(blocks_to_download1[4].as_ref(), &block(13));
    //     assert_eq!(blocks_to_download1[5].as_ref(), &block(15));
    //     assert_eq!(blocks_to_download1[6].as_ref(), &block(20));
    //     assert_eq!(blocks_to_download1[7].as_ref(), &block(25));
    //     assert_eq!(blocks_to_download1[8].as_ref(), &block(30));
    //     assert_eq!(blocks_to_download1[9].as_ref(), &block(35));
    //     assert_eq!(blocks_to_download1[10].as_ref(), &block(40));
    //     assert_eq!(blocks_to_download1[11].as_ref(), &block(43));
    //     assert_eq!(blocks_to_download1[12].as_ref(), &block(46));
    //     assert_eq!(blocks_to_download1[13].as_ref(), &block(49));
    //     assert_eq!(blocks_to_download1[14].as_ref(), &block(52));
    //
    //     // download 8
    //     block_state_db.mark_block_downloaded(
    //         &block(8),
    //         block(7),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 7
    //     block_state_db.mark_block_downloaded(
    //         &block(7),
    //         block(6),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 20
    //     block_state_db.mark_block_downloaded(
    //         &block(20),
    //         block(19),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 19
    //     block_state_db.mark_block_downloaded(
    //         &block(19),
    //         block(18),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 18
    //     block_state_db.mark_block_downloaded(
    //         &block(18),
    //         block(17),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 17
    //     block_state_db.mark_block_downloaded(
    //         &block(17),
    //         block(16),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //
    //     // start new pipeline
    //     let history: Vec<BlockHash> = vec![
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //         block(25),
    //         block(30),
    //         block(35),
    //         block(40),
    //         block(43),
    //         block(46),
    //         block(49),
    //         block(52),
    //         block(53),
    //         block(56),
    //         block(59),
    //         block(60),
    //         block(63),
    //         block(65),
    //     ];
    //     let mut pipeline2 = BranchState::new(
    //         last_applied.clone(),
    //         history.clone(),
    //         60,
    //         &mut block_state_db,
    //     );
    //     assert_eq!(pipeline2.intervals.len(), 19);
    //     assert_interval(&pipeline2.intervals[0], (block(0), block(8)));
    //     assert_interval(&pipeline2.intervals[1], (block(8), block(10)));
    //     assert_interval(&pipeline2.intervals[2], (block(10), block(13)));
    //     assert_interval(&pipeline2.intervals[3], (block(13), block(15)));
    //     assert_interval(&pipeline2.intervals[4], (block(15), block(20)));
    //     assert_interval(&pipeline2.intervals[5], (block(20), block(25)));
    //     assert_interval(&pipeline2.intervals[6], (block(25), block(30)));
    //     assert_interval(&pipeline2.intervals[7], (block(30), block(35)));
    //     assert_interval(&pipeline2.intervals[8], (block(35), block(40)));
    //     assert_interval(&pipeline2.intervals[9], (block(40), block(43)));
    //     assert_interval(&pipeline2.intervals[10], (block(43), block(46)));
    //     assert_interval(&pipeline2.intervals[11], (block(46), block(49)));
    //     assert_interval(&pipeline2.intervals[12], (block(49), block(52)));
    //     assert_interval(&pipeline2.intervals[13], (block(52), block(53)));
    //     assert_interval(&pipeline2.intervals[14], (block(53), block(56)));
    //     assert_interval(&pipeline2.intervals[15], (block(56), block(59)));
    //     assert_interval(&pipeline2.intervals[16], (block(59), block(60)));
    //     assert_interval(&pipeline2.intervals[17], (block(60), block(63)));
    //     assert_interval(&pipeline2.intervals[18], (block(63), block(65)));
    //
    //     // try to get schedule blocks for download
    //     let mut blocks_to_download2 = Vec::new();
    //     pipeline2.collect_next_block_headers_to_download(
    //         15,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut blocks_to_download2,
    //         &block_state_db,
    //     );
    //
    //     // check pre-feeded intervals (with blocks downloaded before)
    //     assert_eq!(pipeline2.intervals[0].blocks.len(), 4);
    //     assert_eq!(pipeline2.intervals[0].blocks[0].as_ref(), &block(0));
    //     assert_eq!(pipeline2.intervals[0].blocks[1].as_ref(), &block(6));
    //     assert_eq!(pipeline2.intervals[0].blocks[2].as_ref(), &block(7));
    //     assert_eq!(pipeline2.intervals[0].blocks[3].as_ref(), &block(8));
    //
    //     assert_eq!(pipeline2.intervals[1].blocks.len(), 2);
    //     assert_eq!(pipeline2.intervals[1].blocks[0].as_ref(), &block(8));
    //     assert_eq!(pipeline2.intervals[1].blocks[1].as_ref(), &block(10));
    //
    //     assert_eq!(pipeline2.intervals[2].blocks.len(), 2);
    //     assert_eq!(pipeline2.intervals[2].blocks[0].as_ref(), &block(10));
    //     assert_eq!(pipeline2.intervals[2].blocks[1].as_ref(), &block(13));
    //
    //     assert_eq!(pipeline2.intervals[3].blocks.len(), 2);
    //     assert_eq!(pipeline2.intervals[3].blocks[0].as_ref(), &block(13));
    //     assert_eq!(pipeline2.intervals[3].blocks[1].as_ref(), &block(15));
    //
    //     assert_eq!(pipeline2.intervals[4].blocks.len(), 6);
    //     assert_eq!(pipeline2.intervals[4].blocks[0].as_ref(), &block(15));
    //     assert_eq!(pipeline2.intervals[4].blocks[1].as_ref(), &block(16));
    //     assert_eq!(pipeline2.intervals[4].blocks[2].as_ref(), &block(17));
    //     assert_eq!(pipeline2.intervals[4].blocks[3].as_ref(), &block(18));
    //     assert_eq!(pipeline2.intervals[4].blocks[4].as_ref(), &block(19));
    //     assert_eq!(pipeline2.intervals[4].blocks[5].as_ref(), &block(20));
    //
    //     assert_eq!(pipeline2.intervals[5].blocks.len(), 2);
    //     assert_eq!(pipeline2.intervals[5].blocks[0].as_ref(), &block(20));
    //     assert_eq!(pipeline2.intervals[5].blocks[1].as_ref(), &block(25));
    //
    //     assert_eq!(15, blocks_to_download2.len());
    //     assert_eq!(blocks_to_download2[0].as_ref(), &block(6));
    //     assert_eq!(blocks_to_download2[1].as_ref(), &block(10));
    //     assert_eq!(blocks_to_download2[2].as_ref(), &block(13));
    //     assert_eq!(blocks_to_download2[3].as_ref(), &block(15));
    //     assert_eq!(blocks_to_download2[4].as_ref(), &block(16));
    //     assert_eq!(blocks_to_download2[5].as_ref(), &block(25));
    //     assert_eq!(blocks_to_download2[6].as_ref(), &block(30));
    //     assert_eq!(blocks_to_download2[7].as_ref(), &block(35));
    //     assert_eq!(blocks_to_download2[8].as_ref(), &block(40));
    //     assert_eq!(blocks_to_download2[9].as_ref(), &block(43));
    //     assert_eq!(blocks_to_download2[10].as_ref(), &block(46));
    //     assert_eq!(blocks_to_download2[11].as_ref(), &block(49));
    //     assert_eq!(blocks_to_download2[12].as_ref(), &block(52));
    //     assert_eq!(blocks_to_download2[13].as_ref(), &block(53));
    //     assert_eq!(blocks_to_download2[14].as_ref(), &block(56));
    // }
    //
    // #[test]
    // fn test_shift_and_check_all_blocks_downloaded() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //         block(25),
    //         block(30),
    //         block(35),
    //         block(40),
    //         block(43),
    //         block(46),
    //         block(49),
    //         block(52),
    //         block(53),
    //         block(56),
    //         block(59),
    //         block(60),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline1 = BranchState::new(
    //         last_applied.clone(),
    //         history.clone(),
    //         60,
    //         &mut block_state_db,
    //     );
    //     assert_eq!(pipeline1.intervals.len(), 19);
    //     assert_interval(&pipeline1.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline1.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline1.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline1.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline1.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline1.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline1.intervals[6], (block(15), block(20)));
    //     assert_interval(&pipeline1.intervals[7], (block(20), block(25)));
    //     assert_interval(&pipeline1.intervals[8], (block(25), block(30)));
    //     assert_interval(&pipeline1.intervals[9], (block(30), block(35)));
    //     assert_interval(&pipeline1.intervals[10], (block(35), block(40)));
    //     assert_interval(&pipeline1.intervals[11], (block(40), block(43)));
    //     assert_interval(&pipeline1.intervals[12], (block(43), block(46)));
    //     assert_interval(&pipeline1.intervals[13], (block(46), block(49)));
    //     assert_interval(&pipeline1.intervals[14], (block(49), block(52)));
    //     assert_interval(&pipeline1.intervals[15], (block(52), block(53)));
    //     assert_interval(&pipeline1.intervals[16], (block(53), block(56)));
    //     assert_interval(&pipeline1.intervals[17], (block(56), block(59)));
    //     assert_interval(&pipeline1.intervals[18], (block(59), block(60)));
    //
    //     // download 8
    //     block_state_db.mark_block_downloaded(
    //         &block(8),
    //         block(7),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 7
    //     block_state_db.mark_block_downloaded(
    //         &block(7),
    //         block(6),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 20
    //     block_state_db.mark_block_downloaded(
    //         &block(20),
    //         block(19),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 19
    //     block_state_db.mark_block_downloaded(
    //         &block(19),
    //         block(18),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     // download 18
    //     block_state_db.mark_block_downloaded(
    //         &block(18),
    //         block(17),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //
    //     // check pre-feeded intervals (with blocks downloaded before)
    //     assert_eq!(pipeline1.intervals[0].blocks.len(), 2);
    //     assert_eq!(pipeline1.intervals[0].blocks[0].as_ref(), &block(0));
    //     assert_eq!(pipeline1.intervals[0].blocks[1].as_ref(), &block(2));
    //
    //     assert_eq!(pipeline1.intervals[1].blocks.len(), 2);
    //     assert_eq!(pipeline1.intervals[1].blocks[0].as_ref(), &block(2));
    //     assert_eq!(pipeline1.intervals[1].blocks[1].as_ref(), &block(5));
    //
    //     // mark 8 as downloaded in pipeline, should shift to 5
    //     pipeline1.intervals[2].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline1.missing_operations,
    //     );
    //     assert_eq!(pipeline1.intervals[2].blocks.len(), 4);
    //     assert_eq!(pipeline1.intervals[2].blocks[0].as_ref(), &block(5));
    //     assert_eq!(pipeline1.intervals[2].blocks[1].as_ref(), &block(6));
    //     assert_eq!(pipeline1.intervals[2].blocks[2].as_ref(), &block(7));
    //     assert_eq!(pipeline1.intervals[2].blocks[3].as_ref(), &block(8));
    //
    //     // mark 20 as downloaded in pipeline, should shift to 16
    //     pipeline1.intervals[6].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline1.missing_operations,
    //     );
    //     assert_eq!(pipeline1.intervals[6].blocks.len(), 5);
    //     assert_eq!(pipeline1.intervals[6].blocks[0].as_ref(), &block(15));
    //     assert_eq!(pipeline1.intervals[6].blocks[1].as_ref(), &block(17));
    //     assert_eq!(pipeline1.intervals[6].blocks[2].as_ref(), &block(18));
    //     assert_eq!(pipeline1.intervals[6].blocks[3].as_ref(), &block(19));
    //     assert_eq!(pipeline1.intervals[6].blocks[4].as_ref(), &block(20));
    // }
    //
    // #[test]
    // fn test_collect_next_block_operations_to_download_scheduled_by_find_next_for_apply() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline = BranchState::new(last_applied, history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     // check 2. inerval -  is not downloaded
    //     assert!(!pipeline.intervals[1].all_blocks_downloaded);
    //     assert_eq!(2, pipeline.intervals[1].blocks.len());
    //
    //     // shifting is checking missing operations
    //     assert!(pipeline.missing_operations.is_empty());
    //     pipeline.schedule_next_block_to_apply(100, &block_state_db);
    //     assert!(!pipeline.missing_operations.is_empty());
    //
    //     // try to get blocks for download - max 1 interval
    //     let mut result = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         1,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut result,
    //         &block_state_db,
    //     );
    //     assert_eq!(1, result.len());
    //     assert_eq!(result[0].as_ref(), &block(2));
    // }
    //
    // #[test]
    // fn test_collect_next_block_operations_to_download_scheduled_by_shifting() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline = BranchState::new(last_applied, history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     // check 2. inerval -  is not downloaded
    //     assert!(!pipeline.intervals[1].all_blocks_downloaded);
    //     assert_eq!(2, pipeline.intervals[1].blocks.len());
    //
    //     // try to get blocks for download - max 1 interval
    //     // no blocks downloaded, so no operations
    //     let mut result = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         1,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut result,
    //         &block_state_db,
    //     );
    //     assert_eq!(0, result.len());
    //
    //     // mark 2 as downloaded
    //     block_state_db.mark_block_downloaded(
    //         &block(2),
    //         block(1),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     pipeline.intervals[0].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[1].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[2].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[3].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //
    //     // try to get blocks for download - max 4
    //     let mut result = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         4,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut result,
    //         &block_state_db,
    //     );
    //     assert_eq!(1, result.len());
    //     assert_eq!(result[0].as_ref(), &block(2));
    //
    //     // mark as downloaded
    //     block_state_db.mark_block_downloaded(
    //         &block(5),
    //         block(4),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(8),
    //         block(7),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(7),
    //         block(6),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(10),
    //         block(9),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     pipeline.intervals[0].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[1].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[2].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[3].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //
    //     // try to get blocks for download - max 4 with ignored
    //     let mut result = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         4,
    //         &configuration(),
    //         &hash_set![block_ref(5)],
    //         &mut result,
    //         &block_state_db,
    //     );
    //     assert_eq!(4, result.len());
    //     assert!(result.contains(&block_ref(2)));
    //     assert!(result.contains(&block_ref(7)));
    //     assert!(result.contains(&block_ref(8)));
    //     assert!(result.contains(&block_ref(10)));
    // }
    //
    // #[test]
    // fn test_download_all_blocks_and_operations() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline = BranchState::new(last_applied, history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     // download blocks and operations from 0 to 8
    //     block_state_db.mark_block_downloaded(
    //         &block(8),
    //         block(7),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(7),
    //         block(6),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(6),
    //         block(5),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(5),
    //         block(4),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(4),
    //         block(3),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(3),
    //         block(2),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(2),
    //         block(1),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(1),
    //         block(0),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //
    //     // check all downloaded inervals
    //     pipeline.intervals[0].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[1].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //
    //     assert!(pipeline.intervals[0].all_blocks_downloaded);
    //     assert!(pipeline.intervals[1].all_blocks_downloaded);
    // }
    //
    // #[test]
    // fn test_find_next_block_to_apply_batch() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline = BranchState::new(last_applied, history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     // download blocks and operations from 0 to 8
    //     block_state_db.mark_block_downloaded(
    //         &block(8),
    //         block(7),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(7),
    //         block(6),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(6),
    //         block(5),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(5),
    //         block(4),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(4),
    //         block(3),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(3),
    //         block(2),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(2),
    //         block(1),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(1),
    //         block(0),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: true,
    //         },
    //     );
    //
    //     // we need to shift intervals
    //     pipeline.intervals[0].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[1].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[2].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     pipeline.intervals[3].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //
    //     // next for apply with max batch 0
    //     let next_batch = pipeline.schedule_next_block_to_apply(0, &block_state_db);
    //     assert!(next_batch.is_some());
    //     let next_batch = next_batch.unwrap();
    //     assert_eq!(next_batch.block_to_apply.as_ref(), &block(1));
    //     assert_eq!(0, next_batch.successors_size());
    //
    //     // next for apply with max batch 1
    //     let next_batch = pipeline.schedule_next_block_to_apply(1, &block_state_db);
    //     assert!(next_batch.is_some());
    //     let next_batch = next_batch.unwrap();
    //     assert_eq!(next_batch.block_to_apply.as_ref(), &block(1));
    //     assert_eq!(1, next_batch.successors_size());
    //     assert_eq!(next_batch.successors[0].as_ref(), &block(2));
    //
    //     // next for apply with max batch 100
    //     let next_batch = pipeline.schedule_next_block_to_apply(100, &block_state_db);
    //     assert!(next_batch.is_some());
    //     let next_batch = next_batch.unwrap();
    //     assert_eq!(next_batch.block_to_apply.as_ref(), &block(1));
    //     assert_eq!(7, next_batch.successors_size());
    //     assert_eq!(next_batch.successors[0].as_ref(), &block(2));
    //     assert_eq!(next_batch.successors[1].as_ref(), &block(3));
    //     assert_eq!(next_batch.successors[2].as_ref(), &block(4));
    //     assert_eq!(next_batch.successors[3].as_ref(), &block(5));
    //     assert_eq!(next_batch.successors[4].as_ref(), &block(6));
    //     assert_eq!(next_batch.successors[5].as_ref(), &block(7));
    //     assert_eq!(next_batch.successors[6].as_ref(), &block(8));
    // }
    //
    // #[test]
    // fn test_mark_block_scheduled() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     let mut pipeline = BranchState::new(last_applied, history, 20, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 7);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline.intervals[6], (block(15), block(20)));
    //
    //     let mut headers_to_download = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         1,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut headers_to_download,
    //         &block_state_db,
    //     );
    //     assert_eq!(1, headers_to_download.len());
    //     assert_eq!(headers_to_download[0].as_ref(), &block(2));
    //
    //     block_state_db.mark_scheduled_for_block_header_download(&block(2));
    //
    //     // cfg with reschedule timeout 1000 ms
    //     let mut cfg = configuration();
    //     cfg.block_data_reschedule_timeout = Duration::from_millis(1000);
    //
    //     let mut headers_to_download = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         1,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut headers_to_download,
    //         &block_state_db,
    //     );
    //     assert_eq!(1, headers_to_download.len());
    //     assert_eq!(headers_to_download[0].as_ref(), &block(5));
    //
    //     // sleep a little bit
    //     std::thread::sleep(Duration::from_millis(100));
    //
    //     // try with changed timeout
    //     cfg.block_data_reschedule_timeout = Duration::from_millis(10);
    //
    //     let mut headers_to_download = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         1,
    //         &cfg,
    //         &HashSet::default(),
    //         &mut headers_to_download,
    //         &block_state_db,
    //     );
    //     assert_eq!(1, headers_to_download.len());
    //     assert_eq!(headers_to_download[0].as_ref(), &block(2));
    //
    //     // block downloaded removes timeout
    //     assert!(block_state_db
    //         .blocks
    //         .get(&block(2))
    //         .unwrap()
    //         .block_header_requested
    //         .is_some());
    //     block_state_db.mark_block_downloaded(
    //         &block(2),
    //         block(1),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: false,
    //             applied: false,
    //         },
    //     );
    //     pipeline.intervals[0].shift_and_find_next_missing_predecessor(
    //         &block_state_db,
    //         &mut pipeline.missing_operations,
    //     );
    //     assert!(block_state_db
    //         .blocks
    //         .get(&block(2))
    //         .unwrap()
    //         .block_header_requested
    //         .is_none());
    //
    //     // operations
    //     cfg.block_data_reschedule_timeout = Duration::from_millis(1000);
    //     block_state_db.mark_scheduled_for_block_operations_download(&block(2));
    //
    //     let mut operations_to_download = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         1,
    //         &cfg,
    //         &HashSet::default(),
    //         &mut operations_to_download,
    //         &block_state_db,
    //     );
    //     assert_eq!(0, operations_to_download.len());
    //
    //     // sleep a little bit
    //     std::thread::sleep(Duration::from_millis(100));
    //
    //     // try with changed timeout
    //     cfg.block_data_reschedule_timeout = Duration::from_millis(10);
    //
    //     let mut operations_to_download = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         1,
    //         &cfg,
    //         &HashSet::default(),
    //         &mut operations_to_download,
    //         &block_state_db,
    //     );
    //     assert_eq!(1, operations_to_download.len());
    //     assert_eq!(operations_to_download[0].as_ref(), &block(2));
    //
    //     // block operations downloaded removes timeout
    //     block_state_db.mark_block_operations_downloaded(&block(2));
    //     assert!(block_state_db
    //         .blocks
    //         .get(&block(2))
    //         .unwrap()
    //         .block_operations_requested
    //         .is_none());
    // }
    //
    // #[test]
    // fn test_bootstrap_state_schedule_unique_blocks_for_download() {
    //     // common shared db
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     // genesis
    //     let last_applied1 = block(0);
    //     let last_applied2 = block(0);
    //     // history blocks
    //     let history1: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //     let history2: Vec<BlockHash> = vec![
    //         block(2),
    //         block(5),
    //         block(8),
    //         block(10),
    //         block(13),
    //         block(15),
    //         block(20),
    //     ];
    //
    //     // create pipeline1
    //     let mut pipeline1 =
    //         BranchState::new(last_applied1.clone(), history1, 20, &mut block_state_db);
    //     assert_eq!(pipeline1.intervals.len(), 7);
    //     assert_interval(&pipeline1.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline1.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline1.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline1.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline1.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline1.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline1.intervals[6], (block(15), block(20)));
    //     assert_eq!(8, block_state_db.blocks.len());
    //
    //     // create pipeline2
    //     let mut pipeline2 =
    //         BranchState::new(last_applied2.clone(), history2, 20, &mut block_state_db);
    //     assert_eq!(pipeline2.intervals.len(), 7);
    //     assert_interval(&pipeline2.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline2.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline2.intervals[2], (block(5), block(8)));
    //     assert_interval(&pipeline2.intervals[3], (block(8), block(10)));
    //     assert_interval(&pipeline2.intervals[4], (block(10), block(13)));
    //     assert_interval(&pipeline2.intervals[5], (block(13), block(15)));
    //     assert_interval(&pipeline2.intervals[6], (block(15), block(20)));
    //     assert_eq!(8, block_state_db.blocks.len());
    //
    //     // cfg
    //     let mut cfg = configuration();
    //     cfg.block_data_reschedule_timeout = Duration::from_secs(15);
    //
    //     // simulate schedulue for pipeline1
    //     let mut result1 = Vec::new();
    //     pipeline1.collect_next_block_headers_to_download(
    //         10,
    //         &cfg,
    //         &HashSet::default(),
    //         &mut result1,
    //         &block_state_db,
    //     );
    //     assert_eq!(7, result1.len());
    //     assert_eq!(result1[0].as_ref(), &block(2));
    //     assert_eq!(result1[1].as_ref(), &block(5));
    //     assert_eq!(result1[2].as_ref(), &block(8));
    //     assert_eq!(result1[3].as_ref(), &block(10));
    //     assert_eq!(result1[4].as_ref(), &block(13));
    //     assert_eq!(result1[5].as_ref(), &block(15));
    //     assert_eq!(result1[6].as_ref(), &block(20));
    //     result1
    //         .iter()
    //         .for_each(|b| block_state_db.mark_scheduled_for_block_header_download(&b));
    //
    //     // simulate schedulue for pipeline2
    //     let mut result2 = Vec::new();
    //     pipeline2.collect_next_block_headers_to_download(
    //         10,
    //         &cfg,
    //         &HashSet::default(),
    //         &mut result2,
    //         &block_state_db,
    //     );
    //     assert_eq!(0, result2.len());
    // }
    //
    // #[test]
    // fn test_bootstrap_state_downloading_blocks_and_operations() {
    //     // genesis
    //     let last_applied = block(0);
    //     // history blocks
    //     let history: Vec<BlockHash> = vec![block(2), block(5), block(8)];
    //
    //     // create
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     // create branch pipeline
    //     let mut pipeline = BranchState::new(last_applied, history, 8, &mut block_state_db);
    //     assert_eq!(pipeline.intervals.len(), 3);
    //     assert_interval(&pipeline.intervals[0], (block(0), block(2)));
    //     assert_interval(&pipeline.intervals[1], (block(2), block(5)));
    //     assert_interval(&pipeline.intervals[2], (block(5), block(8)));
    //     assert!(pipeline.missing_operations.is_empty());
    //
    //     // schedule blocks for download
    //     let mut missing_block_headers = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         10,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut missing_block_headers,
    //         &block_state_db,
    //     );
    //     assert_eq!(3, missing_block_headers.len());
    //     assert_eq!(missing_block_headers[0].as_ref(), &block(2));
    //     assert_eq!(missing_block_headers[1].as_ref(), &block(5));
    //     assert_eq!(missing_block_headers[2].as_ref(), &block(8));
    //     assert!(pipeline.missing_operations.is_empty());
    //
    //     // check missing operations (this works just for downloaded blocks)
    //     let mut missing_block_operations = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         10,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut missing_block_operations,
    //         &block_state_db,
    //     );
    //     assert!(missing_block_operations.is_empty());
    //
    //     // download missing blocks
    //     block_state_db.mark_block_downloaded(
    //         &block(2),
    //         block(1),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(5),
    //         block(4),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(8),
    //         block(7),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             applied: false,
    //             operations_downloaded: false,
    //         },
    //     );
    //
    //     // download next blocks
    //     let mut missing_block_headers = Vec::new();
    //     pipeline.collect_next_block_headers_to_download(
    //         10,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut missing_block_headers,
    //         &block_state_db,
    //     );
    //     assert_eq!(3, missing_block_headers.len());
    //     assert_eq!(missing_block_headers[0].as_ref(), &block(1));
    //     assert_eq!(missing_block_headers[1].as_ref(), &block(4));
    //     assert_eq!(missing_block_headers[2].as_ref(), &block(7));
    //     assert!(!pipeline.missing_operations.is_empty());
    //     assert!(pipeline.missing_operations.contains(&block_ref(2)));
    //     assert!(pipeline.missing_operations.contains(&block_ref(5)));
    //     assert!(pipeline.missing_operations.contains(&block_ref(8)));
    //
    //     // check missing operations
    //     let mut missing_block_operations = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         10,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut missing_block_operations,
    //         &block_state_db,
    //     );
    //     assert!(!missing_block_operations.is_empty());
    //     assert!(missing_block_operations.contains(&block_ref(2)));
    //     assert!(missing_block_operations.contains(&block_ref(5)));
    //     assert!(missing_block_operations.contains(&block_ref(8)));
    //
    //     // download operations
    //     block_state_db.mark_block_operations_downloaded(&block(2));
    //     pipeline.block_operations_downloaded(&block(2));
    //     assert_eq!(pipeline.missing_operations.len(), 2);
    //     assert!(pipeline.missing_operations.contains(&block_ref(5)));
    //     assert!(pipeline.missing_operations.contains(&block_ref(8)));
    //
    //     block_state_db.mark_block_operations_downloaded(&block(8));
    //     pipeline.block_operations_downloaded(&block(8));
    //     assert_eq!(pipeline.missing_operations.len(), 1);
    //     assert!(pipeline.missing_operations.contains(&block_ref(5)));
    //
    //     block_state_db.mark_block_operations_downloaded(&block(5));
    //     pipeline.block_operations_downloaded(&block(5));
    //     assert!(pipeline.missing_operations.is_empty());
    //
    //     // check missing operations
    //     let mut missing_block_operations = Vec::new();
    //     pipeline.collect_next_block_operations_to_download(
    //         10,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut missing_block_operations,
    //         &block_state_db,
    //     );
    //     assert!(missing_block_operations.is_empty());
    // }
    //
    // #[test]
    // fn test_bootstrap_state_shift_interval_with_shrink_to_applied_block() {
    //     // common shared db
    //     let mut block_state_db = BlockStateDb::new(50);
    //
    //     // genesis
    //     let last_applied1 = block(0);
    //     // history blocks
    //     let history1: Vec<BlockHash> = vec![block(2)];
    //
    //     // create pipeline1
    //     let mut pipeline1 =
    //         BranchState::new(last_applied1.clone(), history1, 20, &mut block_state_db);
    //     assert_eq!(pipeline1.intervals.len(), 1);
    //     assert_interval(&pipeline1.intervals[0], (block(0), block(2)));
    //     assert_eq!(2, block_state_db.blocks.len());
    //
    //     // download all
    //     block_state_db.mark_block_downloaded(
    //         &block(2),
    //         block(1),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: true,
    //             applied: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(1),
    //         block(0),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: true,
    //             applied: false,
    //         },
    //     );
    //     block_state_db.remove_with_all_predecessors(&block(2));
    //     pipeline1.block_applied(&block(2), &block_state_db);
    //     assert!(pipeline1.is_done());
    //     assert_eq!(block_state_db.blocks.len(), 1);
    //     assert!(block_state_db.blocks.contains_key(&block(2)));
    //
    //     // create pipeline2
    //     let last_applied2 = block(0);
    //     let history2: Vec<BlockHash> = vec![block(5), block(13), block(15), block(20)];
    //     let mut pipeline2 =
    //         BranchState::new(last_applied2.clone(), history2, 20, &mut block_state_db);
    //     assert_eq!(pipeline2.intervals.len(), 4);
    //     assert_interval(&pipeline2.intervals[0], (block(0), block(5)));
    //     assert_interval(&pipeline2.intervals[1], (block(5), block(13)));
    //     assert_interval(&pipeline2.intervals[2], (block(13), block(15)));
    //     assert_interval(&pipeline2.intervals[3], (block(15), block(20)));
    //
    //     // download
    //     block_state_db.mark_block_downloaded(
    //         &block(5),
    //         block(4),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: true,
    //             applied: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(4),
    //         block(3),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: true,
    //             applied: false,
    //         },
    //     );
    //     block_state_db.mark_block_downloaded(
    //         &block(3),
    //         block(2),
    //         InnerBlockState {
    //             block_downloaded: true,
    //             operations_downloaded: true,
    //             applied: false,
    //         },
    //     );
    //
    //     // collect next missing with sift
    //     let mut missing_block_headers = Vec::new();
    //     pipeline2.collect_next_block_headers_to_download(
    //         10,
    //         &configuration(),
    //         &HashSet::default(),
    //         &mut missing_block_headers,
    //         &block_state_db,
    //     );
    //     assert_eq!(missing_block_headers.len(), 3);
    //     assert_eq!(missing_block_headers[0].as_ref(), &block(13));
    //     assert_eq!(missing_block_headers[1].as_ref(), &block(15));
    //     assert_eq!(missing_block_headers[2].as_ref(), &block(20));
    //
    //     assert_eq!(pipeline2.intervals[0].blocks.len(), 4);
    //     assert_eq!(pipeline2.intervals[0].blocks[0].as_ref(), &block(2));
    //     assert_eq!(pipeline2.intervals[0].blocks[1].as_ref(), &block(3));
    //     assert_eq!(pipeline2.intervals[0].blocks[2].as_ref(), &block(4));
    //     assert_eq!(pipeline2.intervals[0].blocks[3].as_ref(), &block(5));
    // }

    // fn assert_interval(
    //     tested: &BranchInterval,
    //     (expected_left, expected_right): (BlockHash, BlockHash),
    // ) {
    //     assert_eq!(tested.blocks.len(), 2);
    //     assert_eq!(tested.blocks[0].as_ref(), &expected_left);
    //     assert_eq!(tested.blocks[1].as_ref(), &expected_right);
    // }
    //
    // fn configuration() -> PeerBranchBootstrapperConfiguration {
    //     PeerBranchBootstrapperConfiguration::new(
    //         Duration::from_secs(1),
    //         Duration::from_secs(1),
    //         Duration::from_secs(1),
    //         Duration::from_secs(1),
    //         1,
    //         100,
    //     )
    // }
}
