// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This module provides wrapper on RocksDB database.
//! Everything related to RocksDB should be placed here.

use std::io;
use std::sync::PoisonError;

use thiserror::Error;

use crypto::hash::FromBytesError;

use crate::kv_store::readonly_ipc::ContextServiceError;
use crate::persistent::codec::SchemaError;

/// Possible errors for schema
#[derive(Debug, Error)]
pub enum DBError {
    #[error("Schema error: {error}")]
    SchemaError { error: SchemaError },
    #[error("Column family {name} is missing")]
    MissingColumnFamily { name: &'static str },
    #[error("Database incompatibility {name}")]
    DatabaseIncompatibility { name: String },
    #[error("Value already exists {key}")]
    ValueExists { key: String },
    #[error("Found wrong structure. Was looking for {sought}, but found {found}")]
    FoundUnexpectedStructure { sought: String, found: String },
    #[error("Guard Poison {error}")]
    GuardPoison { error: String },
    #[error("Serialization error: {error:?}")]
    SerializationError { error: bincode::Error },
    #[error("Hash encode error : {error}")]
    HashEncodeError { error: FromBytesError },
    #[error("Mutex/lock lock error! Reason: {reason}")]
    LockError { reason: String },
    #[error("I/O error {error}")]
    IOError { error: io::Error },
    #[error("MemoryStatisticsOverflow")]
    MemoryStatisticsOverflow,
    #[error("IPC Context access error: {reason:?}")]
    IpcAccessError { reason: ContextServiceError },
}

impl From<SchemaError> for DBError {
    fn from(error: SchemaError) -> Self {
        DBError::SchemaError { error }
    }
}

impl From<FromBytesError> for DBError {
    fn from(error: FromBytesError) -> Self {
        DBError::HashEncodeError { error }
    }
}

impl From<bincode::Error> for DBError {
    fn from(error: bincode::Error) -> Self {
        Self::SerializationError { error }
    }
}

impl slog::Value for DBError {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}

impl<T> From<PoisonError<T>> for DBError {
    fn from(pe: PoisonError<T>) -> Self {
        DBError::LockError {
            reason: format!("{}", pe),
        }
    }
}

impl From<io::Error> for DBError {
    fn from(error: io::Error) -> Self {
        DBError::IOError { error }
    }
}
