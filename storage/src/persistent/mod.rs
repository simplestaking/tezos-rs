// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::path::Path;

use derive_builder::Builder;

pub use codec::{BincodeEncoded, Codec, Decoder, Encoder, SchemaError};
pub use commit_log::{CommitLogError, CommitLogRef, CommitLogWithSchema, CommitLogs, Location};
pub use database::{DBError, KeyValueStoreWithSchema, KeyValueStoreWithSchemaIterator};
pub use schema::{CommitLogDescriptor, CommitLogSchema};

pub mod codec;
pub mod commit_log;
pub mod database;
pub mod schema;
pub mod sequence;

/// Rocksdb database system configuration
/// - [max_num_of_threads] - if not set, num of cpus is used
#[derive(Builder, Debug, Clone)]
pub struct DbConfiguration {
    #[builder(default = "None")]
    pub max_threads: Option<usize>,
}

impl Default for DbConfiguration {
    fn default() -> Self {
        DbConfigurationBuilder::default().build().unwrap()
    }
}

/// Open commit log at a given path.
pub fn open_cl<P, I>(path: P, cfs: I) -> Result<CommitLogs, CommitLogError>
where
    P: AsRef<Path>,
    I: IntoIterator<Item = CommitLogDescriptor>,
{
    CommitLogs::new(path, cfs)
}

/// This trait extends basic column family by introducing Codec types safety and enforcement
pub trait KeyValueSchema {
    type Key: Codec;
    type Value: Codec;
}

pub trait Flushable {
    fn flush(&self) -> Result<(), anyhow::Error>;
}

pub trait Persistable {
    fn is_persistent(&self) -> bool;
}

/// Provides information if backend can be opened for multi-instance access
pub trait MultiInstanceable {
    fn supports_multiple_opened_instances(&self) -> bool;

    // TODO: TE-150 - real support mutliprocess
    fn sync_with_primary(&self) -> Result<(), MultiInstanceableSyncError> {
        if self.supports_multiple_opened_instances() {
            Err(MultiInstanceableSyncError::new(
                "Not implemented yet, please implement correctly!".to_string(),
            ))
        } else {
            Err(MultiInstanceableSyncError::new(
                "Not supported - supports_multiple_opened_instances is false".to_string(),
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultiInstanceableSyncError(String);

impl MultiInstanceableSyncError {
    pub fn new(error_message: String) -> Self {
        MultiInstanceableSyncError(error_message)
    }
}

/// Custom trait to unify any kv-store schema access
pub trait KeyValueStoreBackend<S: KeyValueSchema> {
    /// Insert new key value pair into the database.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn put(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    /// Delete existing value associated with given key from the database.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    fn delete(&self, key: &S::Key) -> Result<(), DBError>;

    /// Delete existing value associated with given key from the database.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    fn try_delete(&self, key: &S::Key) -> Result<Option<S::Value>, DBError> {
        let v = self.get(key)?;
        if v.is_some() {
            self.delete(key)?;
        }
        Ok(v)
    }

    /// Insert key value pair into the database, overriding existing value if exists.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn merge(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    /// Read value associated with given key, if exists.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, DBError>;

    /// Check, if database contains given key
    ///
    /// # Arguments
    /// * `key` - Key (specified by schema), to be checked for existence
    fn contains(&self, key: &S::Key) -> Result<bool, DBError>;

    /// Removes every element that predicate(elem) evaluates to false
    ///
    /// # Arguments
    /// * `predicate` - functor used for assessment
    fn retain(&self, predicate: &dyn Fn(&S::Key) -> bool) -> Result<(), DBError>;

    /// Write batch into DB atomically
    ///
    /// # Arguments
    /// * `batch` - WriteBatch containing all batched writes to be written to DB
    fn write_batch(&self, batch: Vec<(S::Key, S::Value)>) -> Result<(), DBError>;

    /// Return memory usage statistics
    ///
    fn total_get_mem_usage(&self) -> Result<usize, DBError>;
}
