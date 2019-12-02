// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::Arc;

use tezos_encoding::hash::BlockHash;

use crate::{BlockHeaderWithHash, Direction, IteratorMode, StorageError};
use crate::persistent::{CommitLogs, CommitLogSchema, CommitLogWithSchema, DatabaseWithSchema, KeyValueSchema, Location};
use crate::persistent::database::IteratorWithSchema;

pub type BlockStorageCommitLog = dyn CommitLogWithSchema<BlockStorage> + Sync + Send;

pub trait BlockStorageReader: Sync + Send {
    fn get(&self, block_hash: &BlockHash) -> Result<Option<BlockHeaderWithHash>, StorageError>;

    fn get_blocks(&self, block_hash: &BlockHash, limit: usize) -> Result<Vec<BlockHeaderWithHash>, StorageError>;

    fn contains(&self, block_hash: &BlockHash) -> Result<bool, StorageError>;
}

#[derive(Clone)]
pub struct BlockStorage {
    block_index: BlockIndex,
    block_by_level_index: BlockByLevelIndex,
    clog: Arc<BlockStorageCommitLog>,
}

impl BlockStorage {
    pub fn new(db: Arc<rocksdb::DB>, clog: Arc<CommitLogs>) -> Self {
        Self {
            block_index: BlockIndex::new(db.clone()),
            block_by_level_index: BlockByLevelIndex::new(db),
            clog
        }
    }

    #[inline]
    pub fn put_block_header(&mut self, block: &BlockHeaderWithHash) -> Result<(), StorageError> {
            self.clog.append(block).map_err(StorageError::from)
                .and_then(|location| self.block_index.put(&block.hash, &location).and(self.block_by_level_index.put(block.header.level(), &location)))
    }

    #[inline]
    fn get_by_location(&self, location: &Location) -> Result<BlockHeaderWithHash, StorageError> {
        self.clog.get(location).map_err(StorageError::from)
    }
}

impl BlockStorageReader for BlockStorage {
    #[inline]
    fn get(&self, block_hash: &BlockHash) -> Result<Option<BlockHeaderWithHash>, StorageError> {
        self.block_index.get(block_hash)?
            .map(|location| self.clog.get(&location))
            .transpose()
            .map_err(StorageError::from)
    }

    fn get_blocks(&self, block_hash: &BlockHash, limit: usize) -> Result<Vec<BlockHeaderWithHash>, StorageError> {
        self.get(block_hash)?
            .map_or_else(|| Ok(Vec::new()), |block| self.block_by_level_index.get_blocks(block.header.level(), limit))?
            .iter()
            .map(|location| self.get_by_location(location))
            .collect()
    }

    #[inline]
    fn contains(&self, block_hash: &BlockHash) -> Result<bool, StorageError> {
        self.block_index.contains(block_hash)
    }
}

impl CommitLogSchema for BlockStorage {
    type Value = BlockHeaderWithHash;

    #[inline]
    fn name() -> &'static str {
        "block_storage"
    }
}


pub type BlockIndexDatabase = dyn DatabaseWithSchema<BlockIndex> + Sync + Send;

#[derive(Clone)]
pub struct BlockIndex {
    db: Arc<BlockIndexDatabase>,
}

/// Store block header data in a key-value store and into commit log.
/// The value is fist inserted into commit log, which returns a location of the newly inserted value.
/// That location is then stored as a value in the key-value store.
impl BlockIndex {
    fn new(db: Arc<BlockIndexDatabase>) -> Self {
        Self { db }
    }

    #[inline]
    fn put(&mut self, block_hash: &BlockHash, location: &Location) -> Result<(), StorageError> {
        self.db.put(block_hash, &location)
            .map_err(StorageError::from)
    }

    #[inline]
    fn get(&self, block_hash: &BlockHash) -> Result<Option<Location>, StorageError> {
        self.db.get(block_hash)
            .map_err(StorageError::from)
    }

    #[inline]
    fn contains(&self, block_hash: &BlockHash) -> Result<bool, StorageError> {
        self.db.contains(block_hash)
            .map_err(StorageError::from)
    }
}

impl KeyValueSchema for BlockIndex {
    type Key = BlockHash;
    type Value = Location;

    #[inline]
    fn name() -> &'static str {
        "block_storage"
    }
}



pub type BlockByLevelIndexDatabase = dyn DatabaseWithSchema<BlockByLevelIndex> + Sync + Send;
pub type BlockLevel = i32;

#[derive(Clone)]
pub struct BlockByLevelIndex {
    db: Arc<BlockByLevelIndexDatabase>,
}

impl BlockByLevelIndex {
    fn new(db: Arc<BlockByLevelIndexDatabase>) -> Self {
        Self { db }
    }

    fn put(&self, level: BlockLevel, location: &Location) -> Result<(), StorageError> {
        self.db.put(&level, location).map_err(StorageError::from)
    }

    #[inline]
    pub fn iter(&self, mode: IteratorMode<Self>) -> Result<IteratorWithSchema<Self>, StorageError> {
        self.db.iterator(mode)
            .map_err(StorageError::from)
    }

    fn get_blocks(&self, from_level: BlockLevel, limit: usize) -> Result<Vec<Location>, StorageError> {
        self.db.iterator(IteratorMode::From(&from_level, Direction::Forward))?.take(limit).map(|(_, location)| location.map_err(StorageError::from)).collect()
    }
}

impl KeyValueSchema for BlockByLevelIndex {
    type Key = BlockLevel;
    type Value = Location;

    #[inline]
    fn name() -> &'static str {
        "block_by_level_storage"
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use failure::Error;
    use rocksdb::DB;
    use rocksdb::Options;

    use tezos_messages::p2p::binary_message::BinaryMessage;
    use tezos_messages::p2p::encoding::prelude::*;

    use crate::persistent::CommitLogs;

    use super::*;

    #[test]
    fn block_storage_read_write() -> Result<(), Error> {
        let path = "__block_basictest";
        if Path::new(path).exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        {
            let db = DB::open_cf_descriptors(&opts, path, vec![BlockIndex::descriptor(), BlockByLevelIndex::descriptor()]).unwrap();
            let clog = CommitLogs::new(path, &vec![BlockStorage::descriptor()])?;
            let mut storage = BlockStorage::new(Arc::new(db), Arc::new(clog));

            let message_bytes = hex::decode("00006d6e0102dd00defaf70c53e180ea148b349a6feb4795610b2abc7b07fe91ce50a90814000000005c1276780432bc1d3a28df9a67b363aa1638f807214bb8987e5f9c0abcbd69531facffd1c80000001100000001000000000800000000000c15ef15a6f54021cb353780e2847fb9c546f1d72c1dc17c3db510f45553ce501ce1de000000000003c762c7df00a856b8bfcaf0676f069f825ca75f37f2bee9fe55ba109cec3d1d041d8c03519626c0c0faa557e778cb09d2e0c729e8556ed6a7a518c84982d1f2682bc6aa753f")?;
            let block_header = BlockHeaderWithHash::new(BlockHeader::from_bytes(message_bytes)?)?;

            storage.put_block_header(&block_header)?;
            let block_header_res = storage.get(&block_header.hash)?.unwrap();
            assert_eq!(block_header_res, block_header);
        }
        Ok(assert!(DB::destroy(&opts, path).is_ok()))
    }
}