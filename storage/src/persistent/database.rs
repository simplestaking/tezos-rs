// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This module provides wrapper on RocksDB database.
//! Everything related to RocksDB should be placed here.

use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::PoisonError;

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBIterator, Error, Options, WriteBatch,
    WriteOptions, DB,
};
use serde::Serialize;
use thiserror::Error;

use crypto::hash::FromBytesError;

use crate::persistent::codec::{Decoder, Encoder, SchemaError};
use crate::persistent::{DbConfiguration, KeyValueSchema, KeyValueStoreBackend};

/// Open RocksDB database at given path with specified Column Family configurations
///
/// # Arguments
/// * `path` - Path to open RocksDB
/// * `cfs` - Iterator of Column Family descriptors
pub fn open_kv<P, I>(path: P, cfs: I, cfg: &DbConfiguration) -> Result<DB, DBError>
where
    P: AsRef<Path>,
    I: IntoIterator<Item = ColumnFamilyDescriptor>,
{
    DB::open_cf_descriptors(&default_kv_options(cfg), path, cfs).map_err(DBError::from)
}

/// Create default database configuration options,
/// based on recommended setting: https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
fn default_kv_options(cfg: &DbConfiguration) -> Options {
    // default db options
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
    db_opts.set_bytes_per_sync(1048576);
    db_opts.set_level_compaction_dynamic_level_bytes(true);
    db_opts.set_max_background_jobs(6);
    db_opts.enable_statistics();
    db_opts.set_report_bg_io_stats(true);

    // resolve thread count to use
    let num_of_threads = match cfg.max_threads {
        Some(num) => std::cmp::min(num, num_cpus::get()),
        None => num_cpus::get(),
    };
    // rocksdb default is 1, so we increase only, if above 1
    if num_of_threads > 1 {
        db_opts.increase_parallelism(num_of_threads as i32);
    }

    db_opts
}

/// Create default database configuration options,
/// based on recommended setting:
///     https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
///     https://rocksdb.org/blog/2019/03/08/format-version-4.html
pub fn default_table_options(_cache: &Cache) -> Options {
    // default db options
    let mut db_opts = Options::default();

    // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
    db_opts.set_level_compaction_dynamic_level_bytes(false);
    db_opts.set_write_buffer_size(32 * 1024 * 1024);

    // block table options
    let mut table_options = BlockBasedOptions::default();
    // table_options.set_block_cache(cache);
    // table_options.set_block_size(16 * 1024);
    // table_options.set_cache_index_and_filter_blocks(true);
    // table_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

    // set format_version 4 https://rocksdb.org/blog/2019/03/08/format-version-4.html
    table_options.set_format_version(4);
    // table_options.set_index_block_restart_interval(16);

    db_opts.set_block_based_table_factory(&table_options);

    db_opts
}

#[derive(Serialize, Debug, Clone)]
pub struct RocksDBStats {
    pub mem_table_total: u64,
    pub mem_table_unflushed: u64,
    pub mem_table_readers_total: u64,
    pub cache_total: u64,
}

/// Possible errors for schema
#[derive(Debug, Error)]
pub enum DBError {
    #[error("Schema error: {error}")]
    SchemaError { error: SchemaError },
    #[error("RocksDB error: {error}")]
    RocksDBError { error: Error },
    #[error("Column family {name} is missing")]
    MissingColumnFamily { name: &'static str },
    #[error("Database incompatibility {name}")]
    DatabaseIncompatibility { name: String },
    #[error("Value already exists {key}")]
    ValueExists { key: String },
    #[error("Guard Poison {error}")]
    GuardPoison { error: String },
    #[error("SledDB error: {error}")]
    SledDBError { error: sled::Error },
    #[error("Hash encode error : {error}")]
    HashEncodeError { error: FromBytesError },
    #[error("Mutex/lock lock error! Reason: {reason}")]
    LockError { reason: String },
    #[error("I/O error {error}")]
    IOError { error: io::Error },
    #[error("MemoryStatisticsOverflow")]
    MemoryStatisticsOverflow,
}

impl From<SchemaError> for DBError {
    fn from(error: SchemaError) -> Self {
        DBError::SchemaError { error }
    }
}

impl From<Error> for DBError {
    fn from(error: Error) -> Self {
        DBError::RocksDBError { error }
    }
}

impl From<FromBytesError> for DBError {
    fn from(error: FromBytesError) -> Self {
        DBError::HashEncodeError { error }
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

impl From<sled::Error> for DBError {
    fn from(error: sled::Error) -> Self {
        DBError::SledDBError { error }
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

pub trait RocksDbKeyValueSchema: KeyValueSchema {
    fn descriptor(cache: &Cache) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(Self::name(), default_table_options(cache))
    }

    fn name() -> &'static str;
}

pub trait KeyValueStoreWithSchemaIterator<S: RocksDbKeyValueSchema> {
    /// Read all entries in database.
    ///
    /// # Arguments
    /// * `mode` - Reading mode, specified by RocksDB, From start to end, from end to start, or from
    /// arbitrary position to end.
    fn iterator(&self, mode: IteratorMode<S>) -> Result<IteratorWithSchema<S>, DBError>;

    /// Starting from given key, read all entries to the end.
    ///
    /// # Arguments
    /// * `key` - Key (specified by schema), from which to start reading entries
    fn prefix_iterator(&self, key: &S::Key) -> Result<IteratorWithSchema<S>, DBError>;
}

pub trait KeyValueStoreWithSchema<S: RocksDbKeyValueSchema>:
    KeyValueStoreBackend<S> + KeyValueStoreWithSchemaIterator<S>
{
}

impl<S: RocksDbKeyValueSchema> KeyValueStoreWithSchemaIterator<S> for DB {
    fn iterator(&self, mode: IteratorMode<S>) -> Result<IteratorWithSchema<S>, DBError> {
        let cf = self
            .cf_handle(S::name())
            .ok_or(DBError::MissingColumnFamily { name: S::name() })?;

        let iter = match mode {
            IteratorMode::Start => self.iterator_cf(cf, rocksdb::IteratorMode::Start),
            IteratorMode::End => self.iterator_cf(cf, rocksdb::IteratorMode::End),
            IteratorMode::From(key, direction) => self.iterator_cf(
                cf,
                rocksdb::IteratorMode::From(&key.encode()?, direction.into()),
            ),
        };

        Ok(IteratorWithSchema(iter, PhantomData))
    }

    fn prefix_iterator(&self, key: &S::Key) -> Result<IteratorWithSchema<S>, DBError> {
        let key = key.encode()?;
        let cf = self
            .cf_handle(S::name())
            .ok_or(DBError::MissingColumnFamily { name: S::name() })?;

        Ok(IteratorWithSchema(
            self.prefix_iterator_cf(cf, key),
            PhantomData,
        ))
    }
}

impl<S: RocksDbKeyValueSchema> KeyValueStoreWithSchema<S> for DB {}

impl<S: RocksDbKeyValueSchema> KeyValueStoreBackend<S> for DB {
    fn put(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        let cf = self
            .cf_handle(S::name())
            .ok_or(DBError::MissingColumnFamily { name: S::name() })?;

        self.put_cf_opt(cf, &key, &value, &default_write_options())
            .map_err(DBError::from)
    }

    fn delete(&self, key: &S::Key) -> Result<(), DBError> {
        let key = key.encode()?;
        let cf = self
            .cf_handle(S::name())
            .ok_or(DBError::MissingColumnFamily { name: S::name() })?;

        self.delete_cf_opt(cf, &key, &default_write_options())
            .map_err(DBError::from)
    }

    fn merge(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        let cf = self
            .cf_handle(S::name())
            .ok_or(DBError::MissingColumnFamily { name: S::name() })?;

        self.merge_cf_opt(cf, &key, &value, &default_write_options())
            .map_err(DBError::from)
    }

    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, DBError> {
        let key = key.encode()?;
        let cf = self
            .cf_handle(S::name())
            .ok_or(DBError::MissingColumnFamily { name: S::name() })?;

        self.get_cf(cf, &key)
            .map_err(DBError::from)?
            .map(|value| S::Value::decode(&value))
            .transpose()
            .map_err(DBError::from)
    }

    fn contains(&self, key: &S::Key) -> Result<bool, DBError> {
        let key = key.encode()?;
        let cf = self
            .cf_handle(S::name())
            .ok_or(DBError::MissingColumnFamily { name: S::name() })?;

        let val = self.get_pinned_cf(cf, &key)?;
        Ok(val.is_some())
    }

    fn write_batch(&self, batch: Vec<(S::Key, S::Value)>) -> Result<(), DBError> {
        let mut rocksb_batch = WriteBatch::default(); // batch containing DB key values to persist

        for (k, v) in batch.iter() {
            let key = k.encode()?;
            let value = v.encode()?;
            let cf = self
                .cf_handle(S::name())
                .ok_or(DBError::MissingColumnFamily { name: S::name() })?;
            rocksb_batch.put_cf(cf, &key, &value);
        }

        self.write_opt(rocksb_batch, &default_write_options())?;
        Ok(())
    }

    fn total_get_mem_usage(&self) -> Result<usize, DBError> {
        let memory_usage_stats = rocksdb::perf::get_memory_usage_stats(Some(&[&self]), None)?;
        let mut usage: usize = 0;

        usage = usage
            .checked_add(memory_usage_stats.mem_table_total as usize)
            .ok_or(DBError::MemoryStatisticsOverflow)?;
        usage = usage
            .checked_add(memory_usage_stats.mem_table_unflushed as usize)
            .ok_or(DBError::MemoryStatisticsOverflow)?;
        usage = usage
            .checked_add(memory_usage_stats.mem_table_readers_total as usize)
            .ok_or(DBError::MemoryStatisticsOverflow)?;
        usage = usage
            .checked_add(memory_usage_stats.cache_total as usize)
            .ok_or(DBError::MemoryStatisticsOverflow)?;
        Ok(usage)
    }

    fn retain(&self, predicate: &dyn Fn(&S::Key) -> bool) -> Result<(), DBError> {
        let garbage: Vec<_> = (self as &dyn KeyValueStoreWithSchemaIterator<S>)
            .iterator(IteratorMode::Start)?
            .filter_map(|(k, _)| match k {
                Err(_) => None,
                Ok(value) => {
                    if !predicate(&value) {
                        Some(value)
                    } else {
                        None
                    }
                }
            })
            .collect();

        for i in garbage {
            (self as &dyn KeyValueStoreBackend<S>).delete(&i)?;
        }
        Ok(())
    }
}

fn default_write_options() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(false);
    opts
}

/// Database iterator extended by specific schema
pub struct IteratorWithSchema<'a, S: KeyValueSchema>(DBIterator<'a>, PhantomData<S>);

impl<'a, S: KeyValueSchema> Iterator for IteratorWithSchema<'a, S> {
    type Item = (Result<S::Key, SchemaError>, Result<S::Value, SchemaError>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(k, v)| (S::Key::decode(&k), S::Value::decode(&v)))
    }
}

/// Database iterator direction
pub enum Direction {
    Forward,
    Reverse,
}

impl From<Direction> for rocksdb::Direction {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::Forward => rocksdb::Direction::Forward,
            Direction::Reverse => rocksdb::Direction::Reverse,
        }
    }
}

/// Database iterator with schema mode, from start to end, from end to start or from specific key to end/start
pub enum IteratorMode<'a, S: KeyValueSchema> {
    Start,
    End,
    From(&'a S::Key, Direction),
}
