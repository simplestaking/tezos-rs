// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

// TODO - TE-261: fix for new context or remove

use std::io::Read;
use std::ops::Deref;

use bytes::Buf;


use crate::gc::NotGarbageCollected;
use crate::hash::EntryHash;
use crate::persistent::database::DBError;
use crate::persistent::{Flushable, KeyValueStoreBackend, Persistable};
use crate::{ContextKeyValueStoreSchema, ContextValue};

pub struct SledBackend {
    db: sled::Db,
    inner: sled::Tree,
}

impl SledBackend {
    pub fn new(db: sled::Db) -> Self {
        // TODO TE-437 - get rid of deref call
        SledBackend {
            inner: db.deref().clone(),
            db,
        }
    }
}

impl NotGarbageCollected for SledBackend {}

impl KeyValueStoreBackend<ContextKeyValueStoreSchema> for SledBackend {
    fn retain(&self, predicate: &dyn Fn(&EntryHash) -> bool) -> Result<(), DBError> {
        let garbage_keys: Vec<_> = self
            .inner
            .iter()
            .filter_map(|i| match i {
                Err(_) => None,
                Ok((k, _)) => {
                    let mut buffer = [0_u8; 32];
                    k.to_vec().reader().read_exact(&mut buffer).ok()?;
                    if !predicate(&buffer) {
                        Some(buffer)
                    } else {
                        None
                    }
                }
            })
            .collect();

        for k in garbage_keys {
            self.delete(&k)?;
        }
        Ok(())
    }

    fn put(&self, key: &EntryHash, value: &ContextValue) -> Result<(), DBError> {
        self.inner.insert(&key.as_ref()[..], value.clone())?;
        Ok(())
    }

    fn delete(&self, key: &EntryHash) -> Result<(), DBError> {
        self.inner.remove(&key.as_ref()[..])?;
        Ok(())
    }

    fn merge(&self, key: &EntryHash, value: &ContextValue) -> Result<(), DBError> {
        self.inner.insert(&key.as_ref()[..], value.clone())?;
        Ok(())
    }

    fn get(&self, key: &EntryHash) -> Result<Option<ContextValue>, DBError> {
        Ok(self.inner.get(&key.as_ref()[..])?.map(|ivec| ivec.to_vec()))
    }

    fn contains(&self, key: &EntryHash) -> Result<bool, DBError> {
        Ok(self.inner.contains_key(&key.as_ref()[..])?)
    }

    fn write_batch(&self, batch: Vec<(EntryHash, ContextValue)>) -> Result<(), DBError> {
        for (k, v) in batch {
            self.merge(&k, &v)?;
        }
        Ok(())
    }

    fn total_get_mem_usage(&self) -> Result<usize, DBError> {
        self.db
            .size_on_disk()
            .map(|size| size as usize)
            .map_err(|e| DBError::SledDBError { error: e })
    }
}

impl Flushable for SledBackend {
    fn flush(&self) -> Result<(), Error> {
        match self.db.flush() {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::format_err!(
                "Failed to flush sled db for context, reason: {:?}",
                e
            )),
        }
    }
}

impl Persistable for SledBackend {
    fn is_persistent(&self) -> bool {
        true
    }
}
