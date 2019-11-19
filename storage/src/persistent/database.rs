// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::marker::PhantomData;

use failure::Fail;
use rocksdb::{DB, DBIterator, Error, WriteOptions, DBRawIterator};

use crate::persistent::schema::{Codec, Schema, SchemaError};

/// Possible errors for schema
#[derive(Debug, Fail)]
pub enum DBError {
    #[fail(display = "Schema error: {}", error)]
    SchemaError {
        error: SchemaError
    },
    #[fail(display = "RocksDB error: {}", error)]
    RocksDBError {
        error: Error
    },
    #[fail(display = "Column family {} is missing", name)]
    MissingColumnFamily {
        name: &'static str
    },
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

/// Custom trait extending RocksDB to better handle and enforce database schema
pub trait DatabaseWithSchema<S: Schema> {
    /// Insert new key value pair into the database. If key already exists, method will fail
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn put(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

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

    /// Check, if database contains given key
    ///
    /// # Arguments
    /// * `key` - Key (specified by schema), to be checked for existence
    fn contains(&self, key: &S::Key) -> Result<bool, DBError>;
}

impl<S: Schema> DatabaseWithSchema<S> for DB {
    fn put(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        let cf = self.cf_handle(S::COLUMN_FAMILY_NAME)
            .ok_or(DBError::MissingColumnFamily { name: S::COLUMN_FAMILY_NAME })?;

        self.put_cf_opt(cf, &key, &value, &default_write_options())
            .map_err(DBError::from)
    }

    fn merge(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        let cf = self.cf_handle(S::COLUMN_FAMILY_NAME)
            .ok_or(DBError::MissingColumnFamily { name: S::COLUMN_FAMILY_NAME })?;

        self.merge_cf_opt(cf, &key, &value, &default_write_options())
            .map_err(DBError::from)
    }

    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, DBError> {
        let key = key.encode()?;
        let cf = self.cf_handle(S::COLUMN_FAMILY_NAME)
            .ok_or(DBError::MissingColumnFamily { name: S::COLUMN_FAMILY_NAME })?;

        self.get_cf(cf, &key)
            .map_err(DBError::from)?
            .map(|value| S::Value::decode(&value))
            .transpose()
            .map_err(DBError::from)
    }

    fn iterator(&self, mode: IteratorMode<S>) -> Result<IteratorWithSchema<S>, DBError> {
        let cf = self.cf_handle(S::COLUMN_FAMILY_NAME)
            .ok_or(DBError::MissingColumnFamily { name: S::COLUMN_FAMILY_NAME })?;

        let iter = match mode {
            IteratorMode::Start => self.iterator_cf(cf, rocksdb::IteratorMode::Start),
            IteratorMode::End => self.iterator_cf(cf, rocksdb::IteratorMode::End),
            IteratorMode::From(key, direction) => self.iterator_cf(cf, rocksdb::IteratorMode::From(&key.encode()?, direction.into()))
        };

        Ok(IteratorWithSchema(iter?, PhantomData))
    }

    fn prefix_iterator(&self, key: &S::Key) -> Result<IteratorWithSchema<S>, DBError> {
        let key = key.encode()?;
        let cf = self.cf_handle(S::COLUMN_FAMILY_NAME)
            .ok_or(DBError::MissingColumnFamily { name: S::COLUMN_FAMILY_NAME })?;

        Ok(IteratorWithSchema(self.prefix_iterator_cf(cf, key)?, PhantomData))
    }

    fn contains(&self, key: &S::Key) -> Result<bool, DBError> {
        let cf = self.cf_handle(S::COLUMN_FAMILY_NAME)
            .ok_or(DBError::MissingColumnFamily { name: S::COLUMN_FAMILY_NAME })?;

        let key = key.encode()?;
        let iter = self.iterator_cf(cf, rocksdb::IteratorMode::From(&key, rocksdb::Direction::Forward))?;
        let contains = if iter.valid() {
            let iter: DBRawIterator = iter.into();
            match iter.key() {
                Some(key_from_db) => key_from_db == key,
                None => false
            }
        } else {
            false
        };

        Ok(contains)
    }
}

fn default_write_options() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(false);
    opts
}

/// Database iterator extended by specific schema
pub struct IteratorWithSchema<'a, S: Schema>(DBIterator<'a>, PhantomData<S>);

impl<'a, S: Schema> Iterator for IteratorWithSchema<'a, S>
{
    type Item = (Result<S::Key, SchemaError>, Result<S::Value, SchemaError>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
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
pub enum IteratorMode<'a, S: Schema> {
    Start,
    End,
    From(&'a S::Key, Direction),
}

