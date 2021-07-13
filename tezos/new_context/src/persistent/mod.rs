// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{borrow::Cow, sync::Arc};

use crypto::{
    base58::FromBase58CheckError,
    hash::{ContextHash, FromBytesError},
};
use failure::Fail;

pub use codec::{Codec, Decoder, Encoder, SchemaError};
pub use database::DBError;

use crate::{
    kv_store::{in_memory::RepositoryMemoryUsage, HashId, VacantEntryHash},
    EntryHash,
};

pub mod codec;
pub mod database;

/// This trait extends basic column family by introducing Codec types safety and enforcement
pub trait KeyValueSchema {
    type Key: Codec;
    type Value: Codec;
}

pub trait Flushable {
    fn flush(&self) -> Result<(), failure::Error>;
}

pub trait Persistable {
    fn is_persistent(&self) -> bool;
}

pub trait KeyValueStoreBackend {
    /// Write batch into DB atomically
    ///
    /// # Arguments
    /// * `batch` - WriteBatch containing all batched writes to be written to DB
    fn write_batch(&mut self, batch: Vec<(HashId, Arc<[u8]>)>) -> Result<(), DBError>;
    /// Check if database contains given hash id
    ///
    /// # Arguments
    /// * `hash_id` - HashId, to be checked for existence
    fn contains(&self, hash_id: HashId) -> Result<bool, DBError>;
    /// Mark the HashId as a ContextHash
    ///
    /// # Arguments
    /// * `hash_id` - HashId to mark
    fn put_context_hash(&mut self, hash_id: HashId) -> Result<(), DBError>;
    /// Get the HashId corresponding to the ContextHash
    ///
    /// # Arguments
    /// * `context_hash` - ContextHash to find the HashId
    fn get_context_hash(&self, context_hash: &ContextHash) -> Result<Option<HashId>, DBError>;
    /// Read hash associated with given HashId, if exists.
    ///
    /// # Arguments
    /// * `hash_id` - HashId of the EntryHash
    fn get_hash(&self, hash_id: HashId) -> Result<Option<Cow<EntryHash>>, DBError>;
    /// Read value associated with given HashId, if exists.
    ///
    /// # Arguments
    /// * `hash_id` - HashId of the value
    fn get_value(&self, hash_id: HashId) -> Result<Option<Cow<[u8]>>, DBError>;
    /// Find an entry to insert a new EntryHash
    /// Return the entry
    fn get_vacant_entry_hash(&mut self) -> Result<VacantEntryHash, DBError>;
    /// Manually clear the entries, this should be a no-operation if the implementation
    /// has its own garbage collection
    fn clear_entries(&mut self) -> Result<(), DBError>;
    /// Memory usage
    fn memory_usage(&self) -> RepositoryMemoryUsage;
}

/// Possible errors for storage
#[derive(Debug, Fail)]
pub enum StorageError {
    #[fail(display = "Database error: {}", error)]
    DBError { error: DBError },
    #[fail(display = "Error constructing hash: {}", error)]
    HashError { error: FromBytesError },
    #[fail(display = "Error decoding hash: {}", error)]
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
