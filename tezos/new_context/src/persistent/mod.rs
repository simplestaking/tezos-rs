// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crypto::{base58::FromBase58CheckError, hash::FromBytesError};
use thiserror::Error;

pub use codec::{BincodeEncoded, Codec, Decoder, Encoder, SchemaError};
pub use database::DBError;

pub mod codec;
pub mod database;

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

/// Possible errors for storage
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Database error: {error:?}")]
    DBError { error: DBError },
    #[error("Error constructing hash: {error}")]
    HashError { error: FromBytesError },
    #[error("Error decoding hash: {error}")]
    HashDecodeError { error: FromBase58CheckError },
}

impl From<DBError> for StorageError {
    fn from(error: DBError) -> Self {
        StorageError::DBError { error }
    }
}

impl From<SchemaError> for StorageError {
    fn from(error: SchemaError) -> Self {
        StorageError::DBError {
            error: error.into(),
        }
    }
}

impl From<FromBytesError> for StorageError {
    fn from(error: FromBytesError) -> Self {
        StorageError::HashError { error }
    }
}

impl From<FromBase58CheckError> for StorageError {
    fn from(error: FromBase58CheckError) -> Self {
        StorageError::HashDecodeError { error }
    }
}

impl slog::Value for StorageError {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}
