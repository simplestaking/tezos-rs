// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashSet;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use anyhow::Error;

use crate::hash::EntryHash;
use crate::persistent::database::DBError;
use crate::persistent::KeyValueStoreBackend;
use crate::working_tree::Entry;
use crate::{
    gc::{GarbageCollectionError, GarbageCollector},
    persistent::{Flushable, Persistable},
};
use crate::{ContextKeyValueStoreSchema, ContextValue};

const COUNT_OF_KEYS_TO_CLEANUP_IN_SINGLE_GC_ITERATION: usize = 2048;

/// Finds the value with hash `key` in one of the cycle stores (trying from newest to oldest)
fn stores_get<T, S>(stores: &S, key: &EntryHash) -> Option<ContextValue>
where
    T: KeyValueStoreBackend<ContextKeyValueStoreSchema>,
    S: Deref<Target = Vec<T>>,
{
    stores
        .iter()
        .rev()
        .find_map(|store| store.get(key).unwrap_or(None))
}

/// Returns store index containing the entry with hash `key`  (trying from newest to oldest)
fn stores_containing<T, S>(stores: &S, key: &EntryHash) -> Option<usize>
where
    T: KeyValueStoreBackend<ContextKeyValueStoreSchema>,
    S: Deref<Target = Vec<T>>,
{
    stores
        .iter()
        .enumerate()
        .find(|(_, store)| store.contains(key).unwrap_or(false))
        .map(|(index, _)| index)
}

/// Returns `true` if any of the stores contains an entry with hash `key`, and `false` otherwise
fn stores_contains<T, S>(stores: &S, key: &EntryHash) -> bool
where
    T: KeyValueStoreBackend<ContextKeyValueStoreSchema>,
    S: Deref<Target = Vec<T>>,
{
    stores
        .iter()
        .rev()
        .any(|store| store.contains(key).unwrap_or(false))
}

/// Searches the stores for an entry with hash `key`, and if found, the value is deleted.
///
/// The return value is `None` if the value was not found, or Some((store_idx, value)) otherwise.
fn stores_delete<T, S>(stores: &mut S, key: &EntryHash) -> Option<(usize, ContextValue)>
where
    T: KeyValueStoreBackend<ContextKeyValueStoreSchema>,
    S: DerefMut<Target = Vec<T>>,
{
    stores
        .iter_mut()
        .enumerate()
        .rev()
        .find_map(|(index, store)| {
            store
                .try_delete(key)
                .unwrap_or(None)
                .map(|value| (index, value))
        })
}

/// Commands used by MarkMoveGCed to interact with GC thread.
pub enum CmdMsg {
    StartNewCycle,
    Exit,
    MarkReused(EntryHash),
}

/// Garbage Collected Key Value Store
pub struct MarkMoveGCed<T: KeyValueStoreBackend<ContextKeyValueStoreSchema>> {
    /// Stores for each cycle, older to newer
    stores: Arc<RwLock<Vec<T>>>,
    /// Current in-process cycle store
    current: T,
    /// GC thread
    is_busy: Arc<AtomicBool>,
    msg_cnt: Arc<AtomicUsize>,
    _thread: thread::JoinHandle<()>,
    /// Channel to communicate with GC thread from main thread
    msg: Mutex<mpsc::Sender<CmdMsg>>,
}

impl<T: 'static + KeyValueStoreBackend<ContextKeyValueStoreSchema> + Send + Sync + Default>
    MarkMoveGCed<T>
{
    pub fn new(cycle_count: usize) -> Self {
        assert!(
            cycle_count > 1,
            "cycle_count less than 2 for MarkMoveGCed not supported"
        );

        let (tx, rx) = mpsc::channel();
        let stores = Arc::new(RwLock::new(
            (0..(cycle_count - 1)).map(|_| Default::default()).collect(),
        ));
        let msg_cnt = Arc::new(AtomicUsize::new(0));
        let msg_cnt_ref = msg_cnt.clone();
        let busy = Arc::new(AtomicBool::new(true));
        let busy_ref = busy.clone();

        Self {
            stores: stores.clone(),
            _thread: thread::spawn(move || {
                kvstore_gc_thread_fn(stores, rx, busy.clone(), msg_cnt).unwrap()
            }),
            msg: Mutex::new(tx),
            is_busy: busy_ref,
            msg_cnt: msg_cnt_ref,
            current: Default::default(),
        }
    }

    /// Finds an entry with hash `key` in one of the archived cycle stores (trying from newest to oldest)
    fn stores_get(&self, key: &EntryHash) -> Result<Option<ContextValue>, DBError> {
        Ok(stores_get(&self.stores.read()?, key))
    }

    /// Returns `true` if any of the archived cycle stores contains a value with hash `key`, and `false` otherwise
    fn stores_contains(&self, key: &EntryHash) -> Result<bool, DBError> {
        Ok(stores_contains(&self.stores.read()?, key))
    }

    /// Marks an entry as "reused" in the current cycle.
    ///
    /// Entries that are reused/referenced in current cycle
    /// will be preserved after garbage collection.
    fn mark_single_reused(&mut self, key: EntryHash) -> Result<(), GarbageCollectionError> {
        self.msg_cnt.fetch_add(1, Ordering::Acquire);

        self.msg.lock()?.send(CmdMsg::MarkReused(key)).map_err(|_| {
            GarbageCollectionError::GarbageCollectorError {
                error: "cannot send message to GC thread".to_string(),
            }
        })
    }

    fn mark_reused(
        &mut self,
        reused_keys: &HashSet<EntryHash>,
    ) -> Result<(), GarbageCollectionError> {
        for i in reused_keys.iter() {
            self.mark_single_reused(*i)?;
        }
        Ok(())
    }

    /// Starts a new cycle.
    ///
    /// Garbage collector will start collecting the oldest cycle.
    pub fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError> {
        self.msg_cnt.fetch_add(1, Ordering::Acquire);
        self.stores.write()?.push(mem::take(&mut self.current));
        self.msg.lock()?.send(CmdMsg::StartNewCycle).map_err(|_| {
            GarbageCollectionError::GarbageCollectorError {
                error: "cannot send message to GC thread".to_string(),
            }
        })
    }

    /// Waits for garbage collector to finish collecting the oldest cycle.
    /// [used only for testing purposes]
    #[allow(dead_code)]
    fn wait_for_gc_finish(&self) {
        while self.is_busy.load(Ordering::Acquire) || self.msg_cnt.load(Ordering::Acquire) > 0 {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl<T: 'static + KeyValueStoreBackend<ContextKeyValueStoreSchema> + Send + Sync + Default>
    KeyValueStoreBackend<ContextKeyValueStoreSchema> for MarkMoveGCed<T>
{
    fn put(&self, key: &EntryHash, value: &ContextValue) -> Result<(), DBError> {
        self.current.put(key, value)
    }

    fn delete(&self, key: &EntryHash) -> Result<(), DBError> {
        self.current.delete(key)
    }

    fn merge(&self, key: &EntryHash, value: &ContextValue) -> Result<(), DBError> {
        self.current.merge(key, value)
    }

    fn get(&self, key: &EntryHash) -> Result<Option<ContextValue>, DBError> {
        Ok(match self.current.get(key)? {
            Some(v) => Some(v),
            None => self.stores_get(key)?,
        })
    }

    fn contains(&self, key: &EntryHash) -> Result<bool, DBError> {
        Ok(self.current.contains(key)? || self.stores_contains(key)?)
    }

    fn write_batch(&self, batch: Vec<(EntryHash, ContextValue)>) -> Result<(), DBError> {
        Ok(self.current.write_batch(batch)?)
    }

    fn total_get_mem_usage(&self) -> Result<usize, DBError> {
        let memory: Result<Vec<_>, _> = self
            .stores
            .read()?
            .iter()
            .map(|store| store.total_get_mem_usage())
            .collect();

        Ok(memory?.iter().sum::<usize>() + self.current.total_get_mem_usage()?)
    }

    fn retain(&self, predicate: &dyn Fn(&EntryHash) -> bool) -> Result<(), DBError> {
        self.current.retain(predicate)
    }
}

/// Garbage collector main function
fn kvstore_gc_thread_fn<T: KeyValueStoreBackend<ContextKeyValueStoreSchema>>(
    stores: Arc<RwLock<Vec<T>>>,
    rx: mpsc::Receiver<CmdMsg>,
    is_busy: Arc<AtomicBool>,
    msg_cnt: Arc<AtomicUsize>,
) -> Result<(), GarbageCollectionError> {
    // number of preserved archived cycles
    let len = stores.read().unwrap().len();
    // maintains incoming references (hashes) for each archived cycle store.
    let mut reused_keys = vec![HashSet::new(); len];
    // this is filled when garbage collection starts and it contains keys
    // that are reused from the oldest store and needs to be moved to newer
    // stores so that after destroying oldest store they are preserved.
    let mut todo_keys: Vec<EntryHash> = vec![];
    let mut received_exit_msg = false;

    loop {
        // wait (block) for main thread events if there are no items to garbage collect
        let wait_for_events = reused_keys.len() == len && !received_exit_msg;

        // check if there are not processed eventsda
        is_busy.swap(
            msg_cnt.load(Ordering::Acquire) > 0 || !todo_keys.is_empty() || reused_keys.len() > len,
            Ordering::Acquire,
        );

        let msg = if wait_for_events {
            match rx.recv() {
                Ok(value) => Some(value),
                Err(_) => {
                    return Ok(());
                }
            }
        } else {
            match rx.try_recv() {
                Ok(value) => Some(value),
                Err(_) => None,
            }
        };

        // process messages received from the main thread
        match msg {
            Some(CmdMsg::StartNewCycle) => {
                // new cycle started, we add a new reused/referenced keys HashSet for it
                reused_keys.push(Default::default());
                msg_cnt.fetch_sub(1, Ordering::Acquire);
            }
            Some(CmdMsg::Exit) => {
                received_exit_msg = true;
            }
            Some(CmdMsg::MarkReused(key)) => {
                if let Some(index) = stores_containing(&stores.read()?, &key) {
                    // only way index can be greater than reused_keys.len() is if GC thread
                    // lags behind (gc has pending 1-2 cycles to collect). When we still haven't
                    // received event from main thread that new cycle has started, yet it has.
                    // So we might receive `key` that was only in `current` store (when this event
                    // was sent by main thread). So if gc had't lagged behind, we wouldn't have found
                    // entry with that `key`. So this entry shouldn't be marked as reused.
                    if index < reused_keys.len() {
                        let keys = &mut reused_keys[index];
                        keys.insert(key);
                    }
                }
                msg_cnt.fetch_sub(1, Ordering::Acquire);
            }
            None => {
                // we exit only if there are no remaining keys to be processed
                if received_exit_msg && todo_keys.is_empty() && reused_keys.len() == len {
                    is_busy.swap(false, Ordering::Acquire);
                    return Ok(());
                }
            }
        }

        // if reused_keys.len() > len  that means that we need to start garbage collecting oldest cycle,
        // reused_keys[0].len() > 0  If no keys are reused we can just drop the cycle.
        if reused_keys.len() > len && !reused_keys[0].is_empty() {
            todo_keys.extend(mem::take(&mut reused_keys[0]).into_iter());
        }

        if !todo_keys.is_empty() {
            let mut stores = stores.write()?;

            // it will block if new cycle begins and we still haven't garbage collected prev cycle.
            // Higher the max_iter (2048) will be, longer gc will block the main thread, but GC will
            // finish faster and will lag behind less.
            // Smaller the max_iter (2048) will be, longer it will take for gc to finish and it will
            // lag behind more (which will increase memory usage), but it will slow down main thread less
            // as the lock will be released on more frequent intervals.
            // TODO: it would be more optimized if this number can change dynamically based on
            // how much gc lags behind and such different parameters.
            let max_iter = if stores.len() <= 1 + len {
                COUNT_OF_KEYS_TO_CLEANUP_IN_SINGLE_GC_ITERATION
            } else {
                usize::MAX
            };

            for _ in 0..max_iter {
                let key = match todo_keys.pop() {
                    Some(key) => key,
                    None => break,
                };

                let (_store_index, entry_bytes) = match stores_delete(&mut stores, &key) {
                    Some(result) => result,
                    // it's possible entry was deleted already when iterating on some other root during gc.
                    // So it's perfectly normal if referenced entry isn't found.
                    None => continue,
                };

                let entry: Entry = match bincode::deserialize(&entry_bytes) {
                    Ok(value) => value,
                    Err(err) => {
                        eprintln!("WorkingTree GC: error while decerializing entry: {:?}", err);
                        continue;
                    }
                };

                // TODO: it would be better if we would move entries to the newest store
                // they were referenced in. This way entries won't live longer then they
                // have to (unlike how its now). This can be achieved if we keep cycle
                // information with `reused_keys`. So if it is Map instead of Set and
                // and we store maximum cycle in which it was referenced in as a value.
                // Then we can move each entry to a given store based on that value.
                if let Err(err) = stores.last_mut().unwrap().put(&key.clone(), &entry_bytes) {
                    eprintln!(
                        "WorkingTree GC: error while adding entry to store: {:?}",
                        err
                    );
                }

                match entry {
                    Entry::Blob(_) => {}
                    Entry::Tree(tree) => {
                        // Push every entry in this directory
                        for node in tree.values() {
                            todo_keys.push(node.entry_hash()?);
                        }
                    }
                    Entry::Commit(commit) => {
                        // Push the root tree for this commit
                        todo_keys.push(commit.root_hash);
                    }
                }
            }
        }

        // If we have reused key sets for more cycles than we want to keep, and we are
        // done collecting the old cycle, we drop that set.
        if reused_keys.len() > len && reused_keys[0].is_empty() && todo_keys.is_empty() {
            drop(reused_keys.drain(..1));
            drop(stores.write()?.drain(..1));
        }
    }
}

impl<T: 'static + KeyValueStoreBackend<ContextKeyValueStoreSchema> + Send + Sync + Default>
    GarbageCollector for MarkMoveGCed<T>
{
    fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError> {
        self.new_cycle_started()
    }

    fn block_applied(&mut self, entries: HashSet<EntryHash>) -> Result<(), GarbageCollectionError> {
        self.mark_reused(&entries)?;
        Ok(())
    }
}

impl<T: KeyValueStoreBackend<ContextKeyValueStoreSchema>> Flushable for MarkMoveGCed<T> {
    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl<T: KeyValueStoreBackend<ContextKeyValueStoreSchema>> Persistable for MarkMoveGCed<T> {
    fn is_persistent(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::kv_store::btree_map::BTreeMapBackend;
    use crate::kv_store::stats::size_of_vec;
    use crate::working_tree::Entry;

    use super::*;

    fn empty_kvstore_gced(
        cycle_count: usize,
    ) -> MarkMoveGCed<BTreeMapBackend<EntryHash, ContextValue>> {
        MarkMoveGCed::new(cycle_count)
    }

    fn entry_hash(key: &[u8]) -> EntryHash {
        assert!(key.len() < 32);
        let bytes: Vec<u8> = key
            .iter()
            .chain(std::iter::repeat(&0u8))
            .take(32)
            .cloned()
            .collect();

        EntryHash::try_from(bytes).unwrap()
    }

    fn blob(value: Vec<u8>) -> Entry {
        Entry::Blob(value)
    }

    fn blob_serialized(value: Vec<u8>) -> Vec<u8> {
        bincode::serialize(&blob(value)).unwrap()
    }

    fn get<
        T: 'static + KeyValueStoreBackend<ContextKeyValueStoreSchema> + Sync + Send + Default,
    >(
        store: &MarkMoveGCed<T>,
        key: &[u8],
    ) -> Option<Entry> {
        store
            .get(&entry_hash(key))
            .unwrap()
            .map(|x| bincode::deserialize(&x[..]).unwrap())
    }

    fn put<
        T: 'static + KeyValueStoreBackend<ContextKeyValueStoreSchema> + Sync + Send + Default,
    >(
        store: &mut MarkMoveGCed<T>,
        key: &[u8],
        value: Entry,
    ) {
        store
            .put(&entry_hash(key), &bincode::serialize(&value).unwrap())
            .unwrap();
    }

    fn mark_reused<
        T: 'static + KeyValueStoreBackend<ContextKeyValueStoreSchema> + Sync + Send + Default,
    >(
        store: &mut MarkMoveGCed<T>,
        key: &[u8],
    ) {
        store.mark_single_reused(entry_hash(key)).unwrap();
    }

    #[test]
    fn test_key_reused_exists() {
        let store = &mut empty_kvstore_gced(3);

        store.wait_for_gc_finish();
        put(store, &[1], blob(vec![1]));
        put(store, &[2], blob(vec![2]));
        store.new_cycle_started().unwrap();
        put(store, &[3], blob(vec![3]));
        store.new_cycle_started().unwrap();
        put(store, &[4], blob(vec![4]));
        mark_reused(store, &[1]);
        store.new_cycle_started().unwrap();

        store.wait_for_gc_finish();

        assert_eq!(get(store, &[1]), Some(blob(vec![1])));
        assert_eq!(get(store, &[2]), None);
        assert_eq!(get(store, &[3]), Some(blob(vec![3])));
        assert_eq!(get(store, &[4]), Some(blob(vec![4])));
    }

    #[test]
    fn test_stats() {
        let store = &mut empty_kvstore_gced(3);

        let kv1 = (entry_hash(&[1]), blob_serialized(vec![1]));
        let kv2 = (entry_hash(&[2]), blob_serialized(vec![1, 2]));
        let kv3 = (entry_hash(&[3]), blob_serialized(vec![1, 2, 3]));
        let kv4 = (entry_hash(&[4]), blob_serialized(vec![1, 2, 3, 4]));

        store.wait_for_gc_finish();

        store.put(&kv1.0.clone(), &kv1.1).unwrap();
        store.put(&kv2.0.clone(), &kv2.1).unwrap();
        store.new_cycle_started().unwrap();
        store.wait_for_gc_finish();

        assert_eq!(
            2 * std::mem::size_of::<EntryHash>() + size_of_vec(&kv1.1) + size_of_vec(&kv2.1),
            store.total_get_mem_usage().unwrap()
        );

        store.put(&kv3.0.clone(), &kv3.1).unwrap();
        store.new_cycle_started().unwrap();
        store.wait_for_gc_finish();
        assert_eq!(
            3 * std::mem::size_of::<EntryHash>()
                + size_of_vec(&kv1.1)
                + size_of_vec(&kv2.1)
                + size_of_vec(&kv3.1),
            store.total_get_mem_usage().unwrap()
        );

        store.put(&kv4.0.clone(), &kv4.1).unwrap();
        store.mark_single_reused(kv1.0).unwrap();
        store.wait_for_gc_finish();

        assert_eq!(
            4 * std::mem::size_of::<EntryHash>()
                + size_of_vec(&kv1.1)
                + size_of_vec(&kv2.1)
                + size_of_vec(&kv3.1)
                + size_of_vec(&kv4.1),
            store.total_get_mem_usage().unwrap()
        );

        store.new_cycle_started().unwrap();
        store.wait_for_gc_finish();
        assert_eq!(
            3 * std::mem::size_of::<EntryHash>()
                + size_of_vec(&kv1.1)
                + size_of_vec(&kv3.1)
                + size_of_vec(&kv4.1),
            store.total_get_mem_usage().unwrap()
        );

        store.new_cycle_started().unwrap();
        store.wait_for_gc_finish();
        assert_eq!(
            2 * std::mem::size_of::<EntryHash>()
                + size_of_vec(&kv1.1) // inserted
                + size_of_vec(&kv4.1), // reused
            store.total_get_mem_usage().unwrap()
        );

        store.new_cycle_started().unwrap();
        store.wait_for_gc_finish();
        assert_eq!(0, store.total_get_mem_usage().unwrap());
    }
}
