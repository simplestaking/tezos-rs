use crate::database::error::Error;
use crate::database::{
    DBSubtreeKeyValueSchema, KVDBIteratorWithSchema, KVDBStoreWithSchema, KVDatabase,
    KVDatabaseWithSchemaIterator, SledIteratorWrapper, SledIteratorWrapperMode,
};
use crate::persistent::{Decoder, Encoder, KeyValueSchema};
use crate::{
    block_meta_storage, operations_meta_storage, BlockMetaStorage, IteratorMode,
    OperationsMetaStorage,
};
use sled::{IVec, Tree};
use std::alloc::Global;
use std::path::Path;
use std::sync::Arc;

use std::collections::HashSet;
use std::marker::PhantomData;

pub struct MainDB {
    db: sled::Db,
    trees: Arc<HashSet<String>>,
}

impl MainDB {
    pub fn initialize<P: AsRef<Path>>(
        db_path: P,
        trees: Vec<String>,
        is_temporary: bool,
    ) -> Result<Self, Error> {
        let db = sled::Config::new()
            .path(db_path)
            .temporary(is_temporary)
            .use_compression(true)
            .mode(sled::Mode::LowSpace)
            .compression_factor(10)
            .open()
            .map_err(Error::from)?;

        let mut tree_set = HashSet::new();

        for name in trees {
            tree_set.insert(name);
        }
        Ok(MainDB {
            db,
            trees: Arc::new(tree_set),
        })
    }
    pub fn get_tree(&self, name: &'static str) -> Result<Tree, Error> {
        if !self.trees.contains(name) {
            return Err(Error::MissingSubTree {
                error: name.to_string(),
            });
        }

        let tree = self.db.open_tree(name).map_err(|_| Error::MissingSubTree {
            error: name.to_owned(),
        })?;

        if name == OperationsMetaStorage::sub_tree_name() {
            tree.set_merge_operator(operations_meta_storage::merge_meta_value)
        }

        if name == BlockMetaStorage::sub_tree_name() {
            tree.set_merge_operator(block_meta_storage::merge_meta_value)
        }
        Ok(tree)
    }
    pub fn flush(&self) -> Result<(), Error> {
        match self.db.flush() {
            Ok(_) => {}
            Err(error) => {
                println!("Flush failed {}", error)
            }
        }
        Ok(())
    }
}

impl<S: DBSubtreeKeyValueSchema> KVDatabase<S> for MainDB {
    fn put(&self, key: &S::Key, value: &S::Value) -> Result<(), Error> {
        let key = key.encode()?;
        let value = value.encode()?;
        let tree = self.get_tree(S::sub_tree_name())?;
        let _ = tree.insert(key, value).map_err(Error::from)?;
        Ok(())
    }

    fn delete(&self, key: &S::Key) -> Result<(), Error> {
        let key = key.encode()?;
        let tree = self.get_tree(S::sub_tree_name())?;
        let _ = tree.remove(key).map_err(Error::from)?;
        Ok(())
    }

    fn merge(&self, key: &S::Key, value: &S::Value) -> Result<(), Error> {
        let key = key.encode()?;
        let value = value.encode()?;
        let tree_name = S::sub_tree_name();
        let tree = self.get_tree(tree_name)?;
        tree.merge(key, value).map_err(Error::from)?;
        Ok(())
    }

    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, Error> {
        let key = key.encode()?;
        let tree = self.get_tree(S::sub_tree_name())?;
        tree.get(key)
            .map_err(Error::from)?
            .map(|value| S::Value::decode(value.as_ref()))
            .transpose()
            .map_err(Error::from)
    }

    fn contains(&self, key: &S::Key) -> Result<bool, Error> {
        let key = key.encode()?;
        let tree = self.get_tree(S::sub_tree_name())?;
        let value = tree.get(key).map_err(Error::from)?;
        Ok(value.is_some())
    }

    fn write_batch(&self, batch: Vec<(S::Key, S::Value), Global>) -> Result<(), Error> {
        let tree = self.get_tree(S::sub_tree_name())?;
        let mut sled_batch = sled::Batch::default();

        for (k, v) in batch.iter() {
            let key = k.encode()?;
            let value = v.encode()?;
            sled_batch.insert(key, value);
        }

        let _ = tree.apply_batch(sled_batch).map_err(Error::from)?;
        Ok(())
    }
}
impl<S: DBSubtreeKeyValueSchema> KVDBStoreWithSchema<S> for MainDB {}

impl<S: DBSubtreeKeyValueSchema> KVDatabaseWithSchemaIterator<S> for MainDB {
    fn iterator(&self, mode: IteratorMode<S>) -> Result<KVDBIteratorWithSchema<S>, Error> {
        let tree = self.get_tree(S::sub_tree_name())?;

        let iter = match mode {
            IteratorMode::Start => SledIteratorWrapper::new(SledIteratorWrapperMode::Start, tree),
            IteratorMode::End => SledIteratorWrapper::new(SledIteratorWrapperMode::End, tree),
            IteratorMode::From(key, direction) => SledIteratorWrapper::new(
                SledIteratorWrapperMode::From(IVec::from(key.encode()?), direction),
                tree,
            ),
        };
        Ok(KVDBIteratorWithSchema(iter, PhantomData))
    }

    fn prefix_iterator(
        &self,
        key: &<S as KeyValueSchema>::Key,
        max_key_len: usize,
    ) -> Result<KVDBIteratorWithSchema<S>, Error> {
        let tree = self.get_tree(S::sub_tree_name())?;
        let key = key.encode()?;
        let prefix_key = key[..max_key_len].to_vec();
        let iter = SledIteratorWrapper::new(
            SledIteratorWrapperMode::Prefix(IVec::from(prefix_key)),
            tree,
        );
        Ok(KVDBIteratorWithSchema(iter, PhantomData))
    }
}
