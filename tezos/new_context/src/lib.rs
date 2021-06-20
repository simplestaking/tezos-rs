// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

#![feature(hash_set_entry)]
// #![forbid(unsafe_code)]

//! Implementation of the TezEdge context for the Tezos economic protocol

// TODO: docs

pub mod gc;
pub mod hash;
pub mod working_tree;

pub mod ffi;
pub mod from_ocaml;
pub mod initializer;
pub mod timings;

pub fn force_libtezos_linking() {
    tezos_sys::force_libtezos_linking();
}

use std::collections::BTreeMap;
use std::num::TryFromIntError;
use std::sync::PoisonError;
use std::{array::TryFromSliceError, collections::HashSet};

use gc::GarbageCollectionError;
use persistent::DBError;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

pub use actions::ActionRecorder;
pub use hash::EntryHash;
pub use tezedge_context::PatchContextFunction;
pub use tezedge_context::TezedgeContext;
pub use tezedge_context::TezedgeIndex;
use working_tree::{
    working_tree::{FoldDepth, TreeWalker, WorkingTree},
    KeyFragment,
};

use crate::gc::GarbageCollector;
use crate::working_tree::working_tree::MerkleError;
use crate::working_tree::working_tree_stats::MerkleStoragePerfReport;
use crypto::hash::{ContextHash, FromBytesError};

mod persistent;

use crate::persistent::{Flushable, KeyValueSchema, KeyValueStoreBackend, Persistable};

pub mod actions;
pub mod kv_store;
pub mod tezedge_context;

pub type ContextKey<'a> = [&'a str];
pub type ContextKeyOwned = Vec<String>;
pub type ContextValue = Vec<u8>;

/// An unique tree identifier during a block application
pub type TreeId = i32;

/// Tree in String form needed for JSON RPCs
pub type StringTreeMap = BTreeMap<String, StringTreeEntry>;

/// Tree in String form needed for JSON RPCs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringTreeEntry {
    Tree(StringTreeMap),
    Blob(String),
    Null,
}

/// Context API used by the Protocol to manipulate the working tree
pub trait ProtocolContextApi
where
    Self: Sized,
{
    // set key-value
    fn add(&self, key: &ContextKey, value: ContextValue) -> Result<Self, ContextError>;
    // delete key-value
    fn delete(&self, key_prefix_to_delete: &ContextKey) -> Result<Self, ContextError>;
    // TODO: remove `copy`, not part of the API anymore (replaced by `find_tree` + `add_tree`)
    // copies subtree under 'from_key' to new subtree under 'to_key', returns None if from_key doesn't exist
    fn copy(
        &self,
        from_key: &ContextKey,
        to_key: &ContextKey,
    ) -> Result<Option<Self>, ContextError>;
    // find value for key
    fn find(&self, key: &ContextKey) -> Result<Option<ContextValue>, ContextError>;
    // mem - check if value exists
    fn mem(&self, key: &ContextKey) -> Result<bool, ContextError>;
    // mem_tree - check if directory exists
    fn mem_tree(&self, key: &ContextKey) -> bool;
    fn find_tree(&self, key: &ContextKey) -> Result<Option<WorkingTree>, ContextError>;
    fn add_tree(&self, key: &ContextKey, tree: &WorkingTree) -> Result<Self, ContextError>;
    fn empty(&self) -> Self;
    fn list(
        &self,
        offset: Option<usize>,
        length: Option<usize>,
        key: &ContextKey,
    ) -> Result<Vec<(KeyFragment, WorkingTree)>, ContextError>;
    fn fold_iter(
        &self,
        depth: Option<FoldDepth>,
        key: &ContextKey,
    ) -> Result<TreeWalker, ContextError>;

    fn get_merkle_root(&self) -> Result<EntryHash, ContextError>;
}

/// Index API used by the Shell
pub trait IndexApi<T: ShellContextApi + ProtocolContextApi> {
    // checks if a commit exists in the repository
    fn exists(&self, context_hash: &ContextHash) -> Result<bool, ContextError>;
    // checkout context for hash
    fn checkout(&self, context_hash: &ContextHash) -> Result<Option<T>, ContextError>;
    // called after a block is applied
    fn block_applied(
        &self,
        referenced_older_entries: HashSet<EntryHash>,
    ) -> Result<(), ContextError>;
    // called when a new cycle starts
    fn cycle_started(&mut self) -> Result<(), ContextError>;
    // get value for key from a point in history indicated by context hash
    fn get_key_from_history(
        &self,
        context_hash: &ContextHash,
        key: &ContextKey,
    ) -> Result<Option<ContextValue>, ContextError>;
    // get a list of all key-values under a certain key prefix
    fn get_key_values_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: &ContextKey,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, ContextError>;
    // get entire context tree in string form for JSON RPC
    fn get_context_tree_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: &ContextKey,
        depth: Option<usize>,
    ) -> Result<StringTreeEntry, ContextError>;
}

/// Context API used by the Shell
pub trait ShellContextApi
where
    Self: Sized,
{
    // commit current context diff to storage
    fn commit(
        &self,
        author: String,
        message: String,
        date: i64,
    ) -> Result<ContextHash, ContextError>;
    fn hash(&self, author: String, message: String, date: i64)
        -> Result<ContextHash, ContextError>;

    // get currently checked out hash
    fn get_last_commit_hash(&self) -> Result<Option<Vec<u8>>, ContextError>;
    // get stats from merkle storage
    fn get_merkle_stats(&self) -> Result<MerkleStoragePerfReport, ContextError>;

    fn get_memory_usage(&self) -> Result<usize, ContextError>;
}
/// Possible errors for context
#[derive(Debug, Error)]
pub enum ContextError {
    #[error("Unknown context_hash: {context_hash:?} - {entry_hash:?}")]
    UnknownContextHashAndEntryError {
        context_hash: String,
        entry_hash: String,
    },
    #[error("Failed operation on Merkle storage: {error}")]
    MerkleStorageError { error: MerkleError },
    #[error("Invalid commit date: {error}")]
    InvalidCommitDate { error: TryFromIntError },
    #[error("Failed to convert hash to array: {error}")]
    HashConversionError { error: TryFromSliceError },
    #[error("Conversion from bytes error: {error}")]
    HashError { error: FromBytesError },
    #[error("Garbage Collection error {error:?}")]
    GarbageCollectionError { error: GarbageCollectionError },
    #[error("Database error error {error:?}")]
    DBError { error: DBError },
    #[error("Serialization error: {error:?}")]
    SerializationError { error: bincode::Error },
    #[error("Found wrong structure. Was looking for {sought}, but found {found}")]
    FoundUnexpectedStructure { sought: String, found: String },
    #[error("Mutex/lock error, reason: {reason:?}")]
    LockError { reason: String },
}

impl From<MerkleError> for ContextError {
    fn from(error: MerkleError) -> Self {
        ContextError::MerkleStorageError { error }
    }
}

impl From<TryFromIntError> for ContextError {
    fn from(error: TryFromIntError) -> Self {
        ContextError::InvalidCommitDate { error }
    }
}

impl From<TryFromSliceError> for ContextError {
    fn from(error: TryFromSliceError) -> Self {
        ContextError::HashConversionError { error }
    }
}

impl From<FromBytesError> for ContextError {
    fn from(error: FromBytesError) -> Self {
        ContextError::HashError { error }
    }
}

impl From<GarbageCollectionError> for ContextError {
    fn from(error: GarbageCollectionError) -> Self {
        ContextError::GarbageCollectionError { error }
    }
}

impl From<DBError> for ContextError {
    fn from(error: DBError) -> Self {
        ContextError::DBError { error }
    }
}

impl From<bincode::Error> for ContextError {
    fn from(error: bincode::Error) -> Self {
        Self::SerializationError { error }
    }
}

impl<T> From<PoisonError<T>> for ContextError {
    fn from(pe: PoisonError<T>) -> Self {
        Self::LockError {
            reason: format!("{}", pe),
        }
    }
}

// keys is hash of Entry
pub type ContextKeyValueStoreSchemaKeyType = EntryHash;
// Entry (serialized) - watch out, this is not the same as ContextValue
pub type MerkleKeyValueStoreSchemaValueType = Vec<u8>;

/// Common serialization prescript for K-V
pub struct ContextKeyValueStoreSchema;

impl KeyValueSchema for ContextKeyValueStoreSchema {
    type Key = ContextKeyValueStoreSchemaKeyType;
    type Value = MerkleKeyValueStoreSchemaValueType;
}

/// Base trait for kv-store to be used with merkle
pub type ContextKeyValueStore = dyn ContextKeyValueStoreWithGargbageCollection + Sync + Send;

pub trait ContextKeyValueStoreWithGargbageCollection:
    KeyValueStoreBackend<ContextKeyValueStoreSchema> + GarbageCollector + Flushable + Persistable
{
}

impl<
        T: KeyValueStoreBackend<ContextKeyValueStoreSchema>
            + GarbageCollector
            + Flushable
            + Persistable,
    > ContextKeyValueStoreWithGargbageCollection for T
{
}

/// Marco that simplifies and unificates ContextKey creation
///
/// Common usage:
///
/// `context_key!("protocol")`
/// `context_key!("data/votes/listings")`
/// `context_key!("data/rolls/owner/snapshot/{}/{}", cycle, snapshot)`
/// `context_key!("{}/{}/{}", "data", "votes", "listings")`
///
#[macro_export]
macro_rules! context_key {
    ($key:expr) => {{
        $key.split('/').collect::<Vec<&str>>()

    }};
    ($($arg:tt)*) => {{
        context_key!(format!($($arg)*))
    }};
}

// Like [`context_key`] but produces ann owned key.
#[macro_export]
macro_rules! context_key_owned {
    ($key:expr) => {{
        $key.split('/').map(String::from).collect::<Vec<String>>()
    }};
    ($($arg:tt)*) => {{
        context_key_owned!(format!($($arg)*))
    }};
}

#[cfg(test)]
mod tests {
    use crate::context_key;

    #[test]
    fn test_context_key_simple() {
        assert_eq!(context_key!("protocol"), vec!["protocol".to_string()],);
    }

    #[test]
    fn test_context_key_mutliple() {
        assert_eq!(
            context_key!("data/votes/listings"),
            vec![
                "data".to_string(),
                "votes".to_string(),
                "listings".to_string()
            ],
        );
    }

    #[test]
    fn test_context_key_format() {
        let cycle: i64 = 5;
        let snapshot: i16 = 9;
        assert_eq!(
            context_key!("data/rolls/owner/snapshot/{}/{}", cycle, snapshot),
            vec![
                "data".to_string(),
                "rolls".to_string(),
                "owner".to_string(),
                "snapshot".to_string(),
                "5".to_string(),
                "9".to_string()
            ],
        );
    }
}
