// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::ops::{AddAssign, DerefMut, SubAssign};
use std::sync::RwLock;

use crate::gc::NotGarbageCollected;
use crate::hash::EntryHash;
use crate::kv_store::stats::StorageBackendStats;
use crate::persistent::database::DBError;
use crate::persistent::{Flushable, KeyValueStoreBackend, Persistable};
use crate::{ContextKeyValueStoreSchema, ContextValue};

/// In Memory Key Value Store implemented with [BTreeMap](std::collections::BTreeMap)
#[derive(Debug)]
pub struct BTreeMapBackend<K: Ord, V> {
    kv_map: RwLock<BTreeMap<K, V>>,
    stats: RwLock<StorageBackendStats>,
}

impl<K: Ord, V> Default for BTreeMapBackend<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord, V> BTreeMapBackend<K, V> {
    pub fn new() -> Self {
        Self {
            kv_map: RwLock::new(BTreeMap::new()),
            stats: Default::default(),
        }
    }
}

impl NotGarbageCollected for BTreeMapBackend<EntryHash, ContextValue> {}

impl KeyValueStoreBackend<ContextKeyValueStoreSchema> for BTreeMapBackend<EntryHash, ContextValue> {
    fn total_get_mem_usage(&self) -> Result<usize, DBError> {
        Ok(self.stats.read()?.total_as_bytes())
    }

    /// put kv in map if key doesn't exist. If it does then return false.
    fn put(&self, key: &EntryHash, value: &ContextValue) -> Result<(), DBError> {
        let mut map = self.kv_map.write()?;

        match map.insert(*key, value.clone()) {
            Some(prev) => {
                self.stats
                    .write()?
                    .deref_mut()
                    .add_assign(StorageBackendStats::from((key, value)));
                self.stats
                    .write()?
                    .deref_mut()
                    .sub_assign(StorageBackendStats::from((key, &prev)));
                Ok(())
            }
            None => {
                self.stats
                    .write()?
                    .deref_mut()
                    .add_assign(StorageBackendStats::from((key, value)));
                Ok(())
            }
        }
    }

    fn merge(&self, key: &EntryHash, value: &ContextValue) -> Result<(), DBError> {
        self.stats
            .write()?
            .deref_mut()
            .add_assign(StorageBackendStats::from((key, value)));
        if let Some(prev) = self.kv_map.write()?.insert(*key, value.clone()) {
            self.stats
                .write()?
                .deref_mut()
                .sub_assign(StorageBackendStats::from((key, &prev)));
        };
        Ok(())
    }

    fn delete(&self, key: &EntryHash) -> Result<(), DBError> {
        let removed_key = self.kv_map.write()?.remove(key);

        if let Some(v) = &removed_key {
            self.stats
                .write()?
                .deref_mut()
                .sub_assign(StorageBackendStats::from((key, v)));
        }

        Ok(())
    }

    fn get(&self, key: &EntryHash) -> Result<Option<ContextValue>, DBError> {
        Ok(self.kv_map.read()?.get(key).cloned())
    }

    fn contains(&self, key: &EntryHash) -> Result<bool, DBError> {
        Ok(self.kv_map.read()?.contains_key(key))
    }

    fn retain(&self, predicate: &dyn Fn(&EntryHash) -> bool) -> Result<(), DBError> {
        let garbage_keys: Vec<_> = self
            .kv_map
            .read()?
            .iter()
            .filter_map(|(k, _)| if !predicate(k) { Some(*k) } else { None })
            .collect();

        for k in garbage_keys {
            self.delete(&k)?;
        }
        Ok(())
    }

    fn write_batch(&self, batch: Vec<(EntryHash, ContextValue)>) -> Result<(), DBError> {
        for (k, v) in batch {
            self.merge(&k, &v)?;
        }
        Ok(())
    }
}

impl Flushable for BTreeMapBackend<EntryHash, ContextValue> {
    fn flush(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

impl Persistable for BTreeMapBackend<EntryHash, ContextValue> {
    fn is_persistent(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::kv_store::btree_map::BTreeMapBackend;
    use crate::kv_store::stats::size_of_vec;
    use crate::kv_store::test_support::{blob_serialized, entry_hash};
    use crate::persistent::KeyValueStoreBackend;
    use crate::EntryHash;

    #[test]
    fn test_memory_consumption_btree() {
        let entry1 = entry_hash(&[1]);
        let value1 = blob_serialized(vec![1, 2, 3, 3, 5]);
        let entry2 = entry_hash(&[2]);
        let value2 = blob_serialized(vec![11, 22, 33]);

        let storage = BTreeMapBackend::default();
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
