// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

// TODO - TE-261: reimplement using the timings database or remove

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Mutex, MutexGuard, RwLock};

use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use serde::{Deserialize, Serialize};

use crypto::hash::BlockHash;
use storage::PersistentStorage;
use storage::{BlockStorage, BlockStorageReader};
//use tezos_context::channel::ContextAction;

use crate::rpc_actor::RpcCollectedStateRef;
use crate::services::dev_services::{ensure_context_action_storage, get_block_actions_by_hash};

#[derive(Serialize, Deserialize)]
pub struct ActionTypeStats {
    total_time: f64,
    total_actions: u32,
}

#[derive(Serialize, Deserialize)]
pub struct ActionStats {
    total_time: f64,
    total_actions: u32,
    action_type_stats: HashMap<String, ActionTypeStats>,
    key_length_max: usize,
    key_length_sum: u64,
    val_length_max: usize,
    val_length_sum: u64,
}

fn compute_key_length(key: Option<&Vec<String>>) -> usize {
    match key {
        Some(key) => key.iter().fold(0, |acc, item| acc + item.len()),
        None => 0,
    }
}

fn add_action<'a>(
    stats: &mut MutexGuard<HashMap<&'a str, ActionStats>>,
    action_name: &'a str,
    key: Option<&Vec<String>>,
    value: Option<&Vec<u8>>,
    time: f64,
) {
    let mut action_stats = stats.entry(action_name).or_insert(ActionStats {
        action_type_stats: HashMap::new(),
        total_time: 0_f64,
        total_actions: 0,
        key_length_max: 0,
        val_length_max: 0,
        val_length_sum: 0,
        key_length_sum: 0,
    });
    action_stats.total_time += time;
    action_stats.total_actions += 1;

    let key_len = compute_key_length(key);
    if key_len > action_stats.key_length_max {
        action_stats.key_length_max = key_len;
    }
    action_stats.key_length_sum += key_len as u64;

    let val_len = if let Some(value) = value {
        value.len()
    } else {
        0
    };
    action_stats.val_length_sum += val_len as u64;
    if value.is_some() && (val_len > action_stats.val_length_max) {
        action_stats.val_length_max = val_len;
    }

    if let Some(key) = key {
        if let Some(first) = key.get(0) {
            let action_type = if first == "data" {
                if let Some(second) = key.get(1) {
                    second
                } else {
                    first
                }
            } else {
                first
            };
            let actions_type_stats = action_stats
                .action_type_stats
                .entry(action_type.clone())
                .or_insert(ActionTypeStats {
                    total_time: 0_f64,
                    total_actions: 0,
                });
            actions_type_stats.total_time += time;
            actions_type_stats.total_actions += 1;
        }
    }
}

struct TopN<T: Ord> {
    data: RwLock<BinaryHeap<Reverse<T>>>,
    // the Reverse makes it a min-heap
    max: usize,
}

impl<T: Ord + Clone> TopN<T> {
    pub fn new(max: usize) -> TopN<T> {
        TopN {
            data: RwLock::new(BinaryHeap::new()),
            max,
        }
    }

    pub fn add(&self, val: &T) {
        let mut should_add;
        {
            let data = self.data.read().expect("Unable to get a read-only lock!");
            should_add = if data.len() < self.max {
                true
            } else {
                data.peek().unwrap().0 < *val
            };

            if !should_add {
                return;
            }; // no writing should take place -> exit
        } // drop the read lock

        let mut data = self.data.write().expect("Unable to get a write lock!");

        if data.len() < self.max {
            // heap not full, add and exit
            return data.push(Reverse(val.clone()));
        }

        should_add = if data.is_empty() {
            true
        } else {
            data.peek().unwrap().0 < *val
        };
        if should_add {
            // a new larger element, add it to the heap
            data.pop();
            data.push(Reverse(val.clone()));
        };
    }
}

#[derive(Serialize)]
pub struct StatsResponse<'a> {
    fat_tail: Vec<ContextAction>,
    stats: HashMap<&'a str, ActionStats>,
}

fn remove_values(mut actions: Vec<ContextAction>) -> Vec<ContextAction> {
    actions.iter_mut().for_each(|action| {
        if let ContextAction::Set {
            ref mut value,
            ref mut value_as_json,
            ..
        } = action
        {
            value.clear();
            *value_as_json = None;
        }
    });
    actions
}

fn fat_tail_vec(fat_tail: TopN<ContextAction>) -> Vec<ContextAction> {
    fat_tail
        .data
        .into_inner()
        .expect("Unable to get fat tail lock data!")
        .into_sorted_vec()
        .into_iter()
        .map(move |reverse_wrapped| reverse_wrapped.0)
        .collect()
}

pub(crate) fn compute_storage_stats<'a>(
    _state: &RpcCollectedStateRef,
    from_block: &BlockHash,
    persistent_storage: &PersistentStorage,
) -> Result<StatsResponse<'a>, anyhow::Error> {
    let context_action_storage = ensure_context_action_storage(persistent_storage)?;
    let block_storage = BlockStorage::new(persistent_storage);
    let stats: Mutex<HashMap<&str, ActionStats>> = Mutex::new(HashMap::new());
    let fat_tail: TopN<ContextAction> = TopN::new(100);

    let blocks = block_storage.get_multiple_without_json(from_block, std::usize::MAX)?;
    blocks.par_iter().for_each(|block| {
        let actions = get_block_actions_by_hash(&context_action_storage, &block.hash)
            .expect("Failed to extract actions from a block!");
        {
            let mut stats = stats.lock().expect("Unable to lock mutex!");
            actions.iter().for_each(|action| match action {
                ContextAction::Set {
                    key,
                    value,
                    start_time,
                    end_time,
                    ..
                } => add_action(
                    &mut stats,
                    "SET",
                    Some(key),
                    Some(value),
                    *end_time - *start_time,
                ),
                ContextAction::Delete {
                    key,
                    start_time,
                    end_time,
                    ..
                } => add_action(&mut stats, "DEL", Some(key), None, *end_time - *start_time),
                ContextAction::RemoveRecursively {
                    key,
                    start_time,
                    end_time,
                    ..
                } => add_action(
                    &mut stats,
                    "REMREC",
                    Some(key),
                    None,
                    *end_time - *start_time,
                ),
                ContextAction::Copy {
                    start_time,
                    end_time,
                    ..
                } => add_action(&mut stats, "COPY", None, None, *end_time - *start_time),
                ContextAction::Checkout {
                    start_time,
                    end_time,
                    ..
                } => add_action(&mut stats, "CHECKOUT", None, None, *end_time - *start_time),
                ContextAction::Commit {
                    start_time,
                    end_time,
                    ..
                } => add_action(&mut stats, "COMMIT", None, None, *end_time - *start_time),
                ContextAction::Mem {
                    key,
                    start_time,
                    end_time,
                    ..
                } => add_action(&mut stats, "MEM", Some(key), None, *end_time - *start_time),
                ContextAction::DirMem {
                    key,
                    start_time,
                    end_time,
                    ..
                } => add_action(
                    &mut stats,
                    "DIRMEM",
                    Some(key),
                    None,
                    *end_time - *start_time,
                ),
                ContextAction::Get {
                    key,
                    start_time,
                    end_time,
                    ..
                } => add_action(&mut stats, "GET", Some(key), None, *end_time - *start_time),
                ContextAction::Fold {
                    key,
                    start_time,
                    end_time,
                    ..
                } => add_action(&mut stats, "FOLD", Some(key), None, *end_time - *start_time),
                ContextAction::Shutdown => {}
            });
        } // drop the stats mutex here

        actions.par_iter().for_each(|action| fat_tail.add(action));
    });

    Ok(StatsResponse {
        fat_tail: remove_values(fat_tail_vec(fat_tail)),
        stats: stats
            .into_inner()
            .expect("Unable to access the stat contents of mutex!"),
    })
}
