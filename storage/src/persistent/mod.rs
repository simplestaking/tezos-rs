// Copyright (c) SimpleStaking and Tezos-RS Contributors
// SPDX-License-Identifier: MIT

use std::path::Path;

use rocksdb::{ColumnFamilyDescriptor, DB, Options};

pub use database::{DatabaseWithSchema, DBError};
pub use schema::{Codec, Schema, SchemaError};

pub mod schema;
pub mod database;

pub fn open_db<P, I>(path: P, cfs: I) -> Result<DB, DBError>
where
    P: AsRef<Path>,
    I: IntoIterator<Item = ColumnFamilyDescriptor>,
{
    DB::open_cf_descriptors(&default_db_options(), path, cfs)
        .map_err(DBError::from)
}

fn default_db_options() -> Options {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    db_opts
}