// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::Arc;

use std::convert::TryFrom;

use rocksdb::{Cache, ColumnFamilyDescriptor};
use serde::{Deserialize, Serialize};

use crypto::hash::{ChainId, HashType};
use tezos_messages::Head;

use crate::persistent::database::{default_table_options, RocksDbKeyValueSchema};
use crate::persistent::{
    BincodeEncoded, Decoder, Encoder, KeyValueSchema, KeyValueStoreWithSchema, SchemaError,
};
use crate::{PersistentStorage, StorageError};

pub type ChainMetaStorageKv = dyn KeyValueStoreWithSchema<ChainMetaStorage> + Sync + Send;

pub trait ChainMetaStorageReader: Sync + Send {
    /// Load current head for chain_id from dedicated storage
    fn get_current_head(&self, chain_id: &ChainId) -> Result<Option<Head>, StorageError>;

    /// Load caboose for chain_id from dedicated storage
    ///
    /// `caboose` vs `save_point`:
    /// - save_point is the lowest block for which we also have the metadata information
    /// - caboose - so in particular it is the lowest block for which we have stored the context
    fn get_caboose(&self, chain_id: &ChainId) -> Result<Option<Head>, StorageError>;

    /// Load genesis for chain_id from dedicated storage
    fn get_genesis(&self, chain_id: &ChainId) -> Result<Option<Head>, StorageError>;
}

/// Represents storage of the chain metadata (current_head, test_chain, ...).
/// Metadata are related to concrete chain_id, which is used as a part of key.
///
/// Reason for this storage is that, for example current head cannot be easily selected from block_meta_storage (lets say by level),
/// because there are some computations (with fitness) that need to be done...
///
/// This storage differs from the other in regard that it is not exposing key-value pair
/// but instead it provides get_ and set_ methods for each property prefixed with chain_id.
///
/// Maybe this properties split is not very nice but, we need to access properties separatly,
/// if we stored metadata grouped to struct K-V: <chain_id> - MetadataStruct,
/// we need to all the time deserialize whole sturct.
///
/// e.g. storage key-value will looks like:
/// (<main_chain_id>, 'current_head') - block_hash_xyz
/// (<main_chain_id>, 'test_chain_id') - chain_id_xyz
///
#[derive(Clone)]
pub struct ChainMetaStorage {
    kv: Arc<ChainMetaStorageKv>,
}

impl ChainMetaStorage {
    pub fn new(persistent_storage: &PersistentStorage) -> Self {
        Self {
            kv: persistent_storage.db(),
        }
    }

    #[inline]
    pub fn set_current_head(&self, chain_id: &ChainId, head: Head) -> Result<(), StorageError> {
        self.kv
            .put(
                &MetaKey::key_current_head(chain_id.clone()),
                &MetadataValue::Head(head),
            )
            .map_err(StorageError::from)
    }

    #[inline]
    pub fn set_caboose(&self, chain_id: &ChainId, head: Head) -> Result<(), StorageError> {
        self.kv
            .put(
                &MetaKey::key_caboose(chain_id.clone()),
                &MetadataValue::Head(head),
            )
            .map_err(StorageError::from)
    }

    #[inline]
    pub fn set_genesis(&self, chain_id: &ChainId, head: Head) -> Result<(), StorageError> {
        self.kv
            .put(
                &MetaKey::key_genesis(chain_id.clone()),
                &MetadataValue::Head(head),
            )
            .map_err(StorageError::from)
    }

    #[inline]
    pub fn get_test_chain_id(&self, chain_id: &ChainId) -> Result<Option<ChainId>, StorageError> {
        self.kv
            .get(&MetaKey::key_test_chain_id(chain_id.clone()))
            .map(|result| match result {
                Some(MetadataValue::TestChainId(value)) => Some(value),
                _ => None,
            })
            .map_err(StorageError::from)
    }

    #[inline]
    pub fn set_test_chain_id(
        &self,
        chain_id: &ChainId,
        test_chain_id: &ChainId,
    ) -> Result<(), StorageError> {
        self.kv
            .put(
                &MetaKey::key_test_chain_id(chain_id.clone()),
                &MetadataValue::TestChainId(test_chain_id.clone()),
            )
            .map_err(StorageError::from)
    }

    #[inline]
    pub fn remove_test_chain_id(&self, chain_id: &ChainId) -> Result<(), StorageError> {
        self.kv
            .delete(&MetaKey::key_test_chain_id(chain_id.clone()))
            .map_err(StorageError::from)
    }
}

impl ChainMetaStorageReader for ChainMetaStorage {
    #[inline]
    fn get_current_head(&self, chain_id: &ChainId) -> Result<Option<Head>, StorageError> {
        self.kv
            .get(&MetaKey::key_current_head(chain_id.clone()))
            .map(|result| match result {
                Some(MetadataValue::Head(value)) => Some(value),
                _ => None,
            })
            .map_err(StorageError::from)
    }

    #[inline]
    fn get_caboose(&self, chain_id: &ChainId) -> Result<Option<Head>, StorageError> {
        self.kv
            .get(&MetaKey::key_caboose(chain_id.clone()))
            .map(|result| match result {
                Some(MetadataValue::Head(value)) => Some(value),
                _ => None,
            })
            .map_err(StorageError::from)
    }

    #[inline]
    fn get_genesis(&self, chain_id: &ChainId) -> Result<Option<Head>, StorageError> {
        self.kv
            .get(&MetaKey::key_genesis(chain_id.clone()))
            .map(|result| match result {
                Some(MetadataValue::Head(value)) => Some(value),
                _ => None,
            })
            .map_err(StorageError::from)
    }
}

impl KeyValueSchema for ChainMetaStorage {
    type Key = MetaKey;
    type Value = MetadataValue;
}

impl RocksDbKeyValueSchema for ChainMetaStorage {
    fn descriptor(cache: &Cache) -> ColumnFamilyDescriptor {
        let cf_opts = default_table_options(cache);
        ColumnFamilyDescriptor::new(Self::name(), cf_opts)
    }

    #[inline]
    fn name() -> &'static str {
        "chain_meta_storage"
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MetaKey {
    chain_id: ChainId,
    key: String,
}

impl MetaKey {
    const LEN_CHAIN_ID: usize = HashType::ChainId.size();
    const IDX_CHAIN_ID: usize = 0;
    const IDX_KEY: usize = Self::IDX_CHAIN_ID + Self::LEN_CHAIN_ID;

    const KEY_CURRENT_HEAD: &'static str = "ch";
    const KEY_CABOOSE: &'static str = "cbs";
    const KEY_GENESIS: &'static str = "gns";
    const KEY_TEST_CHAIN_ID: &'static str = "tcid";

    fn key_current_head(chain_id: ChainId) -> MetaKey {
        MetaKey {
            chain_id,
            key: Self::KEY_CURRENT_HEAD.to_string(),
        }
    }

    fn key_caboose(chain_id: ChainId) -> MetaKey {
        MetaKey {
            chain_id,
            key: Self::KEY_CABOOSE.to_string(),
        }
    }

    fn key_genesis(chain_id: ChainId) -> MetaKey {
        MetaKey {
            chain_id,
            key: Self::KEY_GENESIS.to_string(),
        }
    }

    fn key_test_chain_id(chain_id: ChainId) -> MetaKey {
        MetaKey {
            chain_id,
            key: Self::KEY_TEST_CHAIN_ID.to_string(),
        }
    }
}

impl Encoder for MetaKey {
    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        if self.chain_id.as_ref().len() == Self::LEN_CHAIN_ID {
            let mut bytes = Vec::with_capacity(Self::LEN_CHAIN_ID);
            bytes.extend(self.chain_id.as_ref());
            bytes.extend(self.key.encode()?);
            Ok(bytes)
        } else {
            Err(SchemaError::EncodeError)
        }
    }
}

impl Decoder for MetaKey {
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        if bytes.len() > Self::LEN_CHAIN_ID {
            let chain_id = ChainId::try_from(&bytes[Self::IDX_CHAIN_ID..Self::IDX_KEY])?;
            let key = String::decode(&bytes[Self::IDX_KEY..])?;
            Ok(MetaKey { chain_id, key })
        } else {
            Err(SchemaError::DecodeError)
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum MetadataValue {
    Head(Head),
    TestChainId(ChainId),
}

impl BincodeEncoded for MetadataValue {}

impl BincodeEncoded for Head {}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use anyhow::Error;

    use crate::tests_common::TmpStorage;

    use super::*;

    #[test]
    fn test_current_head() -> Result<(), Error> {
        let tmp_storage = TmpStorage::create_to_out_dir("__test_current_head")?;
        let index = ChainMetaStorage::new(tmp_storage.storage());

        let chain_id1 = "NetXgtSLGNJvNye".try_into()?;
        let block_1 = Head::new(
            "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe".try_into()?,
            1,
            vec![],
        );

        let chain_id2 = "NetXjD3HPJJjmcd".try_into()?;
        let block_2 = Head::new(
            "BLockGenesisGenesisGenesisGenesisGenesisd6f5afWyME7".try_into()?,
            2,
            vec![],
        );

        // no current heads
        assert!(index.get_current_head(&chain_id1)?.is_none());
        assert!(index.get_current_head(&chain_id2)?.is_none());

        // set for chain_id1
        index.set_current_head(&chain_id1, block_1.clone())?;
        assert!(index.get_current_head(&chain_id1)?.is_some());
        assert_eq!(
            index.get_current_head(&chain_id1)?.unwrap().block_hash(),
            block_1.block_hash()
        );
        assert!(index.get_current_head(&chain_id2)?.is_none());

        // set for chain_id2
        index.set_current_head(&chain_id2, block_2.clone())?;
        assert!(index.get_current_head(&chain_id1)?.is_some());
        assert_eq!(
            index.get_current_head(&chain_id1)?.unwrap().block_hash(),
            block_1.block_hash()
        );
        assert!(index.get_current_head(&chain_id2)?.is_some());
        assert_eq!(
            index.get_current_head(&chain_id2)?.unwrap().block_hash(),
            block_2.block_hash()
        );

        // update for chain_id1
        index.set_current_head(&chain_id1, block_2.clone())?;
        assert!(index.get_current_head(&chain_id1)?.is_some());
        assert_eq!(
            index.get_current_head(&chain_id1)?.unwrap().block_hash(),
            block_2.block_hash()
        );
        assert!(index.get_current_head(&chain_id2)?.is_some());
        assert_eq!(
            index.get_current_head(&chain_id2)?.unwrap().block_hash(),
            block_2.block_hash()
        );

        Ok(())
    }

    #[test]
    fn test_caboose() -> Result<(), Error> {
        let tmp_storage = TmpStorage::create_to_out_dir("__test_caboose")?;
        let index = ChainMetaStorage::new(tmp_storage.storage());

        let chain_id1 = "NetXgtSLGNJvNye".try_into()?;
        let block_1 = Head::new(
            "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe".try_into()?,
            1,
            vec![],
        );

        let chain_id2 = "NetXjD3HPJJjmcd".try_into()?;
        let block_2 = Head::new(
            "BLockGenesisGenesisGenesisGenesisGenesisd6f5afWyME7".try_into()?,
            2,
            vec![],
        );

        // no current heads
        assert!(index.get_caboose(&chain_id1)?.is_none());
        assert!(index.get_caboose(&chain_id2)?.is_none());

        // set for chain_id1
        index.set_caboose(&chain_id1, block_1.clone())?;
        assert!(index.get_caboose(&chain_id1)?.is_some());
        assert_eq!(
            index.get_caboose(&chain_id1)?.unwrap().block_hash(),
            block_1.block_hash()
        );
        assert!(index.get_caboose(&chain_id2)?.is_none());

        // set for chain_id2
        index.set_caboose(&chain_id2, block_2.clone())?;
        assert!(index.get_caboose(&chain_id1)?.is_some());
        assert_eq!(
            index.get_caboose(&chain_id1)?.unwrap().block_hash(),
            block_1.block_hash()
        );
        assert!(index.get_caboose(&chain_id2)?.is_some());
        assert_eq!(
            index.get_caboose(&chain_id2)?.unwrap().block_hash(),
            block_2.block_hash()
        );

        // update for chain_id1
        index.set_caboose(&chain_id1, block_2.clone())?;
        assert!(index.get_caboose(&chain_id1)?.is_some());
        assert_eq!(
            index.get_caboose(&chain_id1)?.unwrap().block_hash(),
            block_2.block_hash()
        );
        assert!(index.get_caboose(&chain_id2)?.is_some());
        assert_eq!(
            index.get_caboose(&chain_id2)?.unwrap().block_hash(),
            block_2.block_hash()
        );

        Ok(())
    }

    #[test]
    fn test_genesis() -> Result<(), Error> {
        let tmp_storage = TmpStorage::create_to_out_dir("__test_genesis")?;
        let index = ChainMetaStorage::new(tmp_storage.storage());

        let chain_id1 = "NetXgtSLGNJvNye".try_into()?;
        let block_1 = Head::new(
            "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe".try_into()?,
            1,
            vec![],
        );

        let chain_id2 = "NetXjD3HPJJjmcd".try_into()?;
        let block_2 = Head::new(
            "BLockGenesisGenesisGenesisGenesisGenesisd6f5afWyME7".try_into()?,
            2,
            vec![],
        );

        // no current heads
        assert!(index.get_genesis(&chain_id1)?.is_none());
        assert!(index.get_genesis(&chain_id2)?.is_none());

        // set for chain_id1
        index.set_genesis(&chain_id1, block_1.clone())?;
        assert!(index.get_genesis(&chain_id1)?.is_some());
        assert_eq!(
            index.get_genesis(&chain_id1)?.unwrap().block_hash(),
            block_1.block_hash()
        );
        assert!(index.get_genesis(&chain_id2)?.is_none());

        // set for chain_id2
        index.set_genesis(&chain_id2, block_2.clone())?;
        assert!(index.get_genesis(&chain_id1)?.is_some());
        assert_eq!(
            index.get_genesis(&chain_id1)?.unwrap().block_hash(),
            block_1.block_hash()
        );
        assert!(index.get_genesis(&chain_id2)?.is_some());
        assert_eq!(
            index.get_genesis(&chain_id2)?.unwrap().block_hash(),
            block_2.block_hash()
        );

        // update for chain_id1
        index.set_genesis(&chain_id1, block_2.clone())?;
        assert!(index.get_genesis(&chain_id1)?.is_some());
        assert_eq!(
            index.get_genesis(&chain_id1)?.unwrap().block_hash(),
            block_2.block_hash()
        );
        assert!(index.get_genesis(&chain_id2)?.is_some());
        assert_eq!(
            index.get_genesis(&chain_id2)?.unwrap().block_hash(),
            block_2.block_hash()
        );

        Ok(())
    }

    #[test]
    fn test_test_chain_id() -> Result<(), Error> {
        let tmp_storage = TmpStorage::create_to_out_dir("__test_test_chain_id")?;
        let index = ChainMetaStorage::new(tmp_storage.storage());

        let chain_id1 = "NetXgtSLGNJvNye".try_into()?;
        let chain_id2 = "NetXjD3HPJJjmcd".try_into()?;
        let chain_id3 = "NetXjD3HPJJjmcd".try_into()?;

        assert!(index.get_test_chain_id(&chain_id1)?.is_none());
        assert!(index.get_test_chain_id(&chain_id2)?.is_none());

        // update for chain_id1
        index.set_test_chain_id(&chain_id1, &chain_id3)?;
        assert!(index.get_test_chain_id(&chain_id1)?.is_some());
        assert_eq!(index.get_test_chain_id(&chain_id1)?.unwrap(), chain_id3);
        assert!(index.get_test_chain_id(&chain_id2)?.is_none());

        // update for chain_id2
        index.set_test_chain_id(&chain_id2, &chain_id3)?;
        assert!(index.get_test_chain_id(&chain_id1)?.is_some());
        assert_eq!(index.get_test_chain_id(&chain_id1)?.unwrap(), chain_id3);
        assert!(index.get_test_chain_id(&chain_id2)?.is_some());
        assert_eq!(index.get_test_chain_id(&chain_id2)?.unwrap(), chain_id3);

        // update for chain_id1
        index.set_test_chain_id(&chain_id1, &chain_id2)?;
        assert!(index.get_test_chain_id(&chain_id1)?.is_some());
        assert_eq!(index.get_test_chain_id(&chain_id1)?.unwrap(), chain_id2);
        assert!(index.get_test_chain_id(&chain_id2)?.is_some());
        assert_eq!(index.get_test_chain_id(&chain_id2)?.unwrap(), chain_id3);

        // remove for chain_id1
        index.remove_test_chain_id(&chain_id1)?;
        assert!(index.get_test_chain_id(&chain_id1)?.is_none());
        assert!(index.get_test_chain_id(&chain_id2)?.is_some());
        assert_eq!(index.get_test_chain_id(&chain_id2)?.unwrap(), chain_id3);

        Ok(())
    }
}
