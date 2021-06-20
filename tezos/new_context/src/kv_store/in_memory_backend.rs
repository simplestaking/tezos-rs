// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Error;

use crate::gc::NotGarbageCollected;
use crate::hash::EntryHash;
use crate::kv_store::stats::StorageBackendStats;
use crate::persistent::database::DBError;
use crate::persistent::{Flushable, KeyValueStoreBackend, Persistable};
use crate::{ContextKeyValueStoreSchema, ContextValue};

#[derive(Default)]
pub struct HashMapWithStats {
    inner: HashMap<EntryHash, ContextValue>,
    stats: StorageBackendStats,
}

impl HashMapWithStats {
    pub fn insert(&mut self, key: EntryHash, value: ContextValue) -> Option<ContextValue> {
        let stats = StorageBackendStats::from((&key, &value));
        match self.inner.insert(key, value) {
            Some(prev) => {
                self.stats -= StorageBackendStats::from((&key, &prev));
                self.stats += stats;
                Some(prev)
            }
            None => {
                self.stats += stats;
                None
            }
        }
    }

    pub fn remove(&mut self, key: &EntryHash) -> Option<ContextValue> {
        match self.inner.remove(key) {
            Some(prev) => {
                self.stats -= StorageBackendStats::from((key, &prev));
                Some(prev)
            }
            None => None,
        }
    }

    pub fn get(&self, key: &EntryHash) -> Option<&ContextValue> {
        self.inner.get(key)
    }

    pub fn contains_key(&self, key: &EntryHash) -> bool {
        self.inner.contains_key(key)
    }

    pub fn iter(&self) -> std::collections::hash_map::Iter<EntryHash, ContextValue> {
        self.inner.iter()
    }

    pub fn get_memory_usage(&self) -> StorageBackendStats {
        self.stats
    }
}

#[derive(Default)]
pub struct InMemoryBackend {
    inner: Arc<RwLock<HashMapWithStats>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        InMemoryBackend {
            inner: Arc::new(RwLock::new(HashMapWithStats::default())),
        }
    }
}

impl NotGarbageCollected for InMemoryBackend {}

impl KeyValueStoreBackend<ContextKeyValueStoreSchema> for InMemoryBackend {
    fn retain(&self, predicate: &dyn Fn(&EntryHash) -> bool) -> Result<(), DBError> {
        let garbage_keys: Vec<_> = self
            .inner
            .read()?
            .iter()
            .filter_map(|(k, _)| if !predicate(k) { Some(*k) } else { None })
            .collect();

        for k in garbage_keys {
            self.delete(&k)?;
        }
        Ok(())
    }

    fn put(&self, key: &EntryHash, value: &ContextValue) -> Result<(), DBError> {
        let mut w = self.inner.write()?;
        w.insert(*key, value.clone());
        Ok(())
    }

    fn delete(&self, key: &EntryHash) -> Result<(), DBError> {
        let mut w = self.inner.write()?;
        w.remove(key);
        Ok(())
    }

    fn merge(&self, key: &EntryHash, value: &ContextValue) -> Result<(), DBError> {
        let mut w = self.inner.write()?;

        w.insert(*key, value.clone());
        Ok(())
    }

    fn get(&self, key: &EntryHash) -> Result<Option<ContextValue>, DBError> {
        let db = self.inner.clone();
        let r = db.read()?;

        match r.get(key) {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }

    fn contains(&self, key: &EntryHash) -> Result<bool, DBError> {
        let db = self.inner.clone();
        let r = db.read()?;
        Ok(r.contains_key(key))
    }

    fn write_batch(&self, batch: Vec<(EntryHash, ContextValue)>) -> Result<(), DBError> {
        for (k, v) in batch {
            self.merge(&k, &v)?;
        }
        Ok(())
    }

    fn total_get_mem_usage(&self) -> Result<usize, DBError> {
        let r = self.inner.read()?;
        Ok(r.get_memory_usage().total_as_bytes())
    }
}

impl Flushable for InMemoryBackend {
    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl Persistable for InMemoryBackend {
    fn is_persistent(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::kv_store::in_memory_backend::InMemoryBackend;
    use crate::kv_store::stats::size_of_vec;
    use crate::kv_store::test_support::{blob_serialized, entry_hash};
    use crate::persistent::KeyValueStoreBackend;
    use crate::EntryHash;

    #[test]
    fn test_memory_consumption_in_memory() {
        let entry1 = entry_hash(&[1]);
        let value1 = blob_serialized(vec![1, 2, 3, 3, 5]);
        let entry2 = entry_hash(&[2]);
        let value2 = blob_serialized(vec![11, 22, 33]);

        let storage = InMemoryBackend::default();
        assert_eq!(0, storage.total_get_mem_usage().unwrap());

        // insert first entry
        storage.put(&entry1, &value1).unwrap();
        assert_eq!(
            std::mem::size_of::<EntryHash>() + size_of_vec(&value1),
            storage.total_get_mem_usage().unwrap()
        );

        // change value under key
        storage.merge(&entry1, &value2).unwrap();
        assert_eq!(
            std::mem::size_of::<EntryHash>() + size_of_vec(&value2),
            storage.total_get_mem_usage().unwrap()
        );

        storage.put(&entry2, &value2).unwrap();
        assert_eq!(
            2 * std::mem::size_of::<EntryHash>() + size_of_vec(&value2) + size_of_vec(&value2),
            storage.total_get_mem_usage().unwrap()
        );

        //remove first entry
        storage.delete(&entry1).unwrap();
        assert_eq!(
            std::mem::size_of::<EntryHash>() + size_of_vec(&value2),
            storage.total_get_mem_usage().unwrap()
        );

        //remove second entry
        storage.delete(&entry2).unwrap();
        assert_eq!(0, storage.total_get_mem_usage().unwrap());
    }
}
