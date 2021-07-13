// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    borrow::Cow,
    collections::{hash_map::DefaultHasher, BTreeMap, VecDeque},
    hash::Hasher,
    sync::Arc,
};

use crossbeam_channel::Sender;
use crypto::hash::ContextHash;

use crate::{
    gc::{
        worker::{Command, Cycles, GCThread, PRESERVE_CYCLE_COUNT},
        GarbageCollectionError, GarbageCollector,
    },
    hash::EntryHash,
    persistent::{DBError, Flushable, KeyValueStoreBackend, Persistable},
    Map,
};

use tezos_spsc::Consumer;

use super::{entries::Entries, HashIdError};
use super::{HashId, VacantEntryHash};

#[derive(Debug)]
#[allow(dead_code)]
pub struct RepositoryMemoryUsage {
    /// Number of bytes for all values Arc<[u8]>
    values_bytes: usize,
    /// Number of values in the store (when it's a `Some(Arc<[u8]>)`, not a `None`)
    values_occupied: usize,
    /// Capacity of the Vec for the values
    values_capacity: usize,
    /// Length of the Vec for the values
    values_length: usize,
    /// Capacity of the Vec for the hashes
    hashes_capacity: usize,
    /// Capacity of the Vec for the hashes
    hashes_length: usize,
}

#[derive(Debug)]
pub struct HashValueStore {
    hashes: Entries<HashId, EntryHash>,
    values: Entries<HashId, Option<Arc<[u8]>>>,
    free_ids: Option<Consumer<HashId>>,
    new_ids: Vec<HashId>,
    values_bytes: usize,
    values_occupied: usize,
}

impl HashValueStore {
    pub(crate) fn new<T>(consumer: T) -> Self
    where
        T: Into<Option<Consumer<HashId>>>,
    {
        Self {
            hashes: Entries::new(),
            values: Entries::new(),
            free_ids: consumer.into(),
            new_ids: Vec::with_capacity(1024),
            values_bytes: 0,
            values_occupied: 0,
        }
    }

    pub fn get_memory_usage(&self) -> RepositoryMemoryUsage {
        RepositoryMemoryUsage {
            values_bytes: self.values_bytes,
            values_occupied: self.values_occupied,
            values_capacity: self.values.capacity(),
            values_length: self.values.len(),
            hashes_capacity: self.hashes.capacity(),
            hashes_length: self.hashes.len(),
        }
    }

    pub(crate) fn clear(&mut self) {
        *self = Self {
            hashes: Entries::new(),
            values: Entries::new(),
            free_ids: self.free_ids.take(),
            new_ids: Vec::new(),
            values_bytes: 0,
            values_occupied: 0,
        }
    }

    pub(crate) fn get_vacant_entry_hash(&mut self) -> Result<VacantEntryHash, HashIdError> {
        let (hash_id, entry) = if let Some(free_id) = self.get_free_id() {
            if let Some(old_value) = self.values.set(free_id, None)? {
                self.values_occupied = self.values_occupied.saturating_sub(1);
                self.values_bytes = self.values_bytes.saturating_sub(old_value.len());
            }
            (free_id, self.hashes.get_mut(free_id)?.ok_or(HashIdError)?)
        } else {
            self.hashes.get_vacant_entry()?
        };
        self.new_ids.push(hash_id);

        Ok(VacantEntryHash {
            entry: Some(entry),
            hash_id,
        })
    }

    fn get_free_id(&mut self) -> Option<HashId> {
        self.free_ids.as_mut()?.pop().ok()
    }

    pub(crate) fn insert_value_at(
        &mut self,
        hash_id: HashId,
        value: Arc<[u8]>,
    ) -> Result<(), HashIdError> {
        self.values_bytes = self.values_bytes.saturating_add(value.len());
        if let Some(old) = self.values.insert_at(hash_id, Some(value))? {
            self.values_bytes = self.values_bytes.saturating_sub(old.len());
        } else {
            self.values_occupied = self.values_occupied.saturating_add(1);
        }
        Ok(())
    }

    pub(crate) fn get_hash(&self, hash_id: HashId) -> Result<Option<&EntryHash>, HashIdError> {
        self.hashes.get(hash_id)
    }

    pub(crate) fn get_value(&self, hash_id: HashId) -> Result<Option<&[u8]>, HashIdError> {
        match self.values.get(hash_id)? {
            Some(value) => Ok(value.as_ref().map(|v| v.as_ref())),
            None => Ok(None),
        }
    }

    pub(crate) fn contains(&self, hash_id: HashId) -> Result<bool, HashIdError> {
        Ok(self.values.get(hash_id)?.unwrap_or(&None).is_some())
    }

    fn take_new_ids(&mut self) -> Vec<HashId> {
        let new_ids = self.new_ids.clone();
        self.new_ids.clear();
        new_ids
    }
}

pub struct InMemory {
    current_cycle: BTreeMap<HashId, Option<Arc<[u8]>>>,
    pub hashes: HashValueStore,
    sender: Option<Sender<Command>>,
    pub context_hashes: Map<u64, HashId>,
    context_hashes_cycles: VecDeque<Vec<u64>>,
}

impl Default for InMemory {
    fn default() -> Self {
        Self::new()
    }
}

impl GarbageCollector for InMemory {
    fn new_cycle_started(&mut self) -> Result<(), GarbageCollectionError> {
        self.new_cycle_started();
        Ok(())
    }

    fn block_applied(
        &mut self,
        referenced_older_entries: Vec<HashId>,
    ) -> Result<(), GarbageCollectionError> {
        self.block_applied(referenced_older_entries);
        Ok(())
    }
}

impl Flushable for InMemory {
    fn flush(&self) -> Result<(), failure::Error> {
        Ok(())
    }
}

impl Persistable for InMemory {
    fn is_persistent(&self) -> bool {
        false
    }
}

impl KeyValueStoreBackend for InMemory {
    fn write_batch(&mut self, batch: Vec<(HashId, Arc<[u8]>)>) -> Result<(), DBError> {
        self.write_batch(batch)
    }

    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        self.contains(hash_id)
    }

    fn put_context_hash(&mut self, hash_id: HashId) -> Result<(), DBError> {
        self.put_context_hash_impl(hash_id)
    }

    fn get_context_hash(&self, context_hash: &ContextHash) -> Result<Option<HashId>, DBError> {
        Ok(self.get_context_hash_impl(context_hash))
    }

    fn get_hash(&self, hash_id: HashId) -> Result<Option<Cow<EntryHash>>, DBError> {
        Ok(self.get_hash(hash_id)?.map(|h| Cow::Borrowed(h)))
    }

    fn get_value(&self, hash_id: HashId) -> Result<Option<Cow<[u8]>>, DBError> {
        Ok(self.get_value(hash_id)?.map(|v| Cow::Borrowed(v)))
    }

    fn get_vacant_entry_hash(&mut self) -> Result<VacantEntryHash, DBError> {
        self.get_vacant_entry_hash()
    }

    fn clear_entries(&mut self) -> Result<(), DBError> {
        // `InMemory` has its own garbage collection
        Ok(())
    }

    fn memory_usage(&self) -> RepositoryMemoryUsage {
        self.hashes.get_memory_usage()
    }
}

impl InMemory {
    pub fn new() -> Self {
        // TODO - TE-210: Remove once we hace proper support for history modes.
        let garbage_collector_disabled = std::env::var("DISABLE_INMEM_CONTEXT_GC")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .expect("Provided `DISABLE_INMEM_CONTEXT_GC` value cannot be converted to bool");

        let (sender, cons) = if garbage_collector_disabled {
            (None, None)
        } else {
            let (sender, recv) = crossbeam_channel::unbounded();
            let (prod, cons) = tezos_spsc::bounded(2_000_000);

            std::thread::spawn(move || {
                GCThread {
                    cycles: Cycles::default(),
                    recv,
                    free_ids: prod,
                    pending: Vec::new(),
                }
                .run()
            });

            (Some(sender), Some(cons))
        };

        let current_cycle = Default::default();
        let hashes = HashValueStore::new(cons);
        let context_hashes = Default::default();

        let mut context_hashes_cycles = VecDeque::with_capacity(PRESERVE_CYCLE_COUNT);
        for _ in 0..PRESERVE_CYCLE_COUNT {
            context_hashes_cycles.push_back(Default::default())
        }

        Self {
            current_cycle,
            hashes,
            sender,
            context_hashes,
            context_hashes_cycles,
        }
    }

    pub(crate) fn get_vacant_entry_hash(&mut self) -> Result<VacantEntryHash, DBError> {
        self.hashes.get_vacant_entry_hash().map_err(Into::into)
    }

    pub(crate) fn get_hash(&self, hash_id: HashId) -> Result<Option<&EntryHash>, DBError> {
        self.hashes.get_hash(hash_id).map_err(Into::into)
    }

    pub(crate) fn get_value(&self, hash_id: HashId) -> Result<Option<&[u8]>, DBError> {
        self.hashes.get_value(hash_id).map_err(Into::into)
    }

    fn contains(&self, hash_id: HashId) -> Result<bool, DBError> {
        self.hashes.contains(hash_id).map_err(Into::into)
    }

    pub fn write_batch(&mut self, batch: Vec<(HashId, Arc<[u8]>)>) -> Result<(), DBError> {
        for (hash_id, value) in batch {
            self.hashes.insert_value_at(hash_id, Arc::clone(&value))?;
            self.current_cycle.insert(hash_id, Some(value));
        }
        Ok(())
    }

    pub fn new_cycle_started(&mut self) {
        if let Some(sender) = &self.sender {
            let values_in_cycle = std::mem::take(&mut self.current_cycle);
            let new_ids = self.hashes.take_new_ids();

            if let Err(e) = sender.try_send(Command::StartNewCycle {
                values_in_cycle,
                new_ids,
            }) {
                eprintln!("Fail to send Command::StartNewCycle to GC worker: {:?}", e);
            }

            if let Some(unused) = self.context_hashes_cycles.pop_front() {
                for hash in unused {
                    self.context_hashes.remove(&hash);
                }
            }
            self.context_hashes_cycles.push_back(Default::default());
        }
    }

    pub fn block_applied(&mut self, reused: Vec<HashId>) {
        if let Some(sender) = &self.sender {
            if let Err(e) = sender.send(Command::MarkReused { reused }) {
                eprintln!("Fail to send Command::MarkReused to GC worker: {:?}", e);
            }
        }
    }

    pub fn get_context_hash_impl(&self, context_hash: &ContextHash) -> Option<HashId> {
        let mut hasher = DefaultHasher::new();
        hasher.write(context_hash.as_ref());
        let hashed = hasher.finish();

        self.context_hashes.get(&hashed).cloned()
    }

    pub fn put_context_hash_impl(&mut self, commit_hash_id: HashId) -> Result<(), DBError> {
        let commit_hash = self
            .hashes
            .get_hash(commit_hash_id)?
            .ok_or(DBError::MissingEntry {
                hash_id: commit_hash_id,
            })?;

        let mut hasher = DefaultHasher::new();
        hasher.write(&commit_hash[..]);
        let hashed = hasher.finish();

        self.context_hashes.insert(hashed, commit_hash_id);
        if let Some(back) = self.context_hashes_cycles.back_mut() {
            back.push(hashed);
        };

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn put_entry_hash(&mut self, entry_hash: EntryHash) -> HashId {
        let vacant = self.get_vacant_entry_hash().unwrap();
        vacant.write_with(|entry| *entry = entry_hash)
    }
}
