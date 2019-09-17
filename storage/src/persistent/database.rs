// Copyright (c) SimpleStaking and Tezos-RS Contributors
// SPDX-License-Identifier: MIT

use failure::Fail;
use rocksdb::{DB, Error, WriteOptions};

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
    }
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


pub trait DatabaseWithSchema<S: Schema> {
    fn put(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    fn merge(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, DBError>;
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
}

fn default_write_options() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(false);
    opts
}
