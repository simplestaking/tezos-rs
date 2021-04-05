use crate::block_meta_storage;
use crate::database::backend::{
    BackendIterator, BackendIteratorMode, TezedgeDatabaseBackendStore,
    TezedgeDatabaseBackendStoreIterator,
};
use crate::database::error::Error;
use crate::database::tezedge_database::{KVStoreKeyValueSchema, TezdegeDatabaseBackendKV};
use crate::operations_meta_storage;
use crate::{BlockMetaStorage, Direction, OperationsMetaStorage};
use sled::{Config, IVec, Tree};
use std::path::Path;

pub struct SledDBBackend {
    db: sled::Db,
}

impl SledDBBackend {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = Config::default()
            .path(path)
            .compression_factor(2)
            .open()
            .map_err(Error::from)?;
        Ok(Self { db })
    }
    pub fn get_tree(&self, name: &'static str) -> Result<Tree, Error> {
        let tree = self.db.open_tree(name).map_err(Error::from)?;

        if name == OperationsMetaStorage::column_name() {
            tree.set_merge_operator(operations_meta_storage::merge_meta_value)
        }
        if name == BlockMetaStorage::column_name() {
            tree.set_merge_operator(block_meta_storage::merge_meta_value)
        }

        Ok(tree)
    }
}

impl TezedgeDatabaseBackendStore for SledDBBackend {
    fn put(&self, column: &'static str, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let tree = self.get_tree(column)?;
        let _ = tree.insert(key, value).map_err(Error::from)?;
        Ok(())
    }

    fn delete(&self, column: &'static str, key: Vec<u8>) -> Result<(), Error> {
        let tree = self.get_tree(column)?;
        let _ = tree.remove(key).map_err(Error::from)?;
        Ok(())
    }

    fn merge(&self, column: &'static str, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let tree = self.get_tree(column)?;
        let _ = tree.merge(key, value).map_err(Error::from)?;
        Ok(())
    }

    fn get(&self, column: &'static str, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        let tree = self.get_tree(column)?;
        tree.get(key)
            .map(|value| {
                if let Some(value) = value {
                    Some(value.to_vec())
                } else {
                    None
                }
            })
            .map_err(Error::from)
    }

    fn contains(&self, column: &'static str, key: Vec<u8>) -> Result<bool, Error> {
        let tree = self.get_tree(column)?;
        tree.contains_key(key).map_err(Error::from)
    }

    fn write_batch(
        &self,
        column: &'static str,
        batch: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), Error> {
        let mut sled_batch = sled::Batch::default();
        let tree = self.get_tree(column)?;
        for (k, v) in batch {
            sled_batch.insert(k, v)
        }
        tree.apply_batch(sled_batch).map_err(Error::from)
    }

    fn flush(&self) -> Result<usize, Error> {
        self.db.flush().map_err(Error::from)
    }
}

impl TezdegeDatabaseBackendKV for SledDBBackend {}

impl TezedgeDatabaseBackendStoreIterator for SledDBBackend {
    fn iterator(
        &self,
        column: &'static str,
        mode: BackendIteratorMode,
    ) -> Result<Box<BackendIterator>, Error> {
        let tree = self.get_tree(column)?;
        let iter = match mode {
            BackendIteratorMode::Start => SledDBIterator::new(SledDBIteratorMode::Start, tree),
            BackendIteratorMode::End => SledDBIterator::new(SledDBIteratorMode::End, tree),
            BackendIteratorMode::From(key, direction) => {
                SledDBIterator::new(SledDBIteratorMode::From(IVec::from(key), direction), tree)
            }
        };
        Ok(Box::new(iter))
    }

    fn prefix_iterator(
        &self,
        column: &'static str,
        key: &Vec<u8>,
        max_key_len: usize,
    ) -> Result<Box<BackendIterator>, Error> {
        let tree = self.get_tree(column)?;
        let prefix_key = key[..max_key_len].to_vec();
        let iter = SledDBIterator::new(SledDBIteratorMode::Prefix(IVec::from(prefix_key)), tree);
        Ok(Box::new(iter))
    }
}

//sled iterator

#[derive(Clone)]
pub enum SledDBIteratorMode {
    Start,
    End,
    From(IVec, Direction),
    Prefix(IVec),
}

pub struct SledDBIterator {
    mode: SledDBIteratorMode,
    iter: sled::Iter,
}

impl SledDBIterator {
    fn new(mode: SledDBIteratorMode, tree: Tree) -> Self {
        match mode.clone() {
            SledDBIteratorMode::Start => Self {
                mode,
                iter: tree.iter(),
            },
            SledDBIteratorMode::End => Self {
                mode,
                iter: tree.iter(),
            },
            SledDBIteratorMode::From(key, direction) => {
                let iter = match direction {
                    Direction::Forward => tree.range(key..),
                    Direction::Reverse => tree.range(..=key),
                };

                Self { mode, iter }
            }
            SledDBIteratorMode::Prefix(key) => Self {
                mode,
                iter: tree.scan_prefix(key),
            },
        }
    }
}

fn convert_next(
    item: Option<Result<(IVec, IVec), sled::Error>>,
) -> Option<Result<(Vec<u8>, Vec<u8>), Error>> {
    match item {
        None => None,
        Some(item) => match item {
            Ok((k, v)) => Some(Ok((k.to_vec(), v.to_vec()))),
            Err(error) => Some(Err(Error::SledDBError { error })),
        },
    }
}

// implementing BackendIterator
impl Iterator for SledDBIterator {
    type Item = Result<(Vec<u8>, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.mode {
            SledDBIteratorMode::Start => convert_next(self.iter.next()),
            SledDBIteratorMode::End => convert_next(self.iter.next_back()),
            SledDBIteratorMode::From(_, direction) => match direction {
                Direction::Forward => convert_next(self.iter.next()),
                Direction::Reverse => convert_next(self.iter.next_back()),
            },
            SledDBIteratorMode::Prefix(_) => convert_next(self.iter.next()),
        }
    }
}
