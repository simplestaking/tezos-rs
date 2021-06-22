// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This sub module provides different KV alternatives for context persistence

use std::convert::{TryFrom, TryInto};
use std::{num::NonZeroU32, str::FromStr};

use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use crate::EntryHash;

pub mod entries;
pub mod in_memory;
pub mod readonly_ipc;
pub mod stats;

pub const INMEM: &str = "inmem";

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HashId(NonZeroU32); // NonZeroU32 so that `Option<HashId>` is 8 bytes

pub struct HashIdError;

impl TryInto<usize> for HashId {
    type Error = HashIdError;

    fn try_into(self) -> Result<usize, Self::Error> {
        let a = self.0.get().checked_sub(1).ok_or(HashIdError)?;
        Ok(a as usize)
    }
}

impl TryFrom<usize> for HashId {
    type Error = HashIdError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let value: u32 = value.try_into().map_err(|_| HashIdError)?;

        value
            .checked_add(1)
            .and_then(NonZeroU32::new)
            .map(HashId)
            .ok_or(HashIdError)
    }
}

const SHIFT: usize = (std::mem::size_of::<u32>() * 8) - 1;
const READONLY: u32 = 1 << SHIFT;

impl HashId {
    pub fn new(value: u32) -> Option<Self> {
        Some(HashId(NonZeroU32::new(value)?))
    }

    pub fn as_u32(&self) -> u32 {
        self.0.get()
    }

    fn set_readonly_runner(&mut self) -> Result<(), HashIdError> {
        let hash_id = self.0.get();

        self.0 = NonZeroU32::new(hash_id | READONLY).ok_or(HashIdError)?;

        Ok(())
    }

    fn get_readonly_id(self) -> Result<Option<HashId>, HashIdError> {
        let hash_id = self.0.get();
        if hash_id & READONLY != 0 {
            Ok(Some(HashId(
                NonZeroU32::new(hash_id & !READONLY).ok_or(HashIdError)?,
            )))
        } else {
            Ok(None)
        }
    }
}

pub struct VacantEntryHash<'a> {
    entry: Option<&'a mut EntryHash>,
    hash_id: HashId,
}

impl<'a> VacantEntryHash<'a> {
    pub(crate) fn write_with<F>(self, fun: F) -> HashId
    where
        F: FnOnce(&mut EntryHash),
    {
        if let Some(entry) = self.entry {
            fun(entry)
        };
        self.hash_id
    }

    pub(crate) fn set_readonly_runner(mut self) -> Result<Self, HashIdError> {
        self.hash_id.set_readonly_runner()?;
        Ok(self)
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, EnumIter)]
pub enum SupportedContextKeyValueStore {
    InMem,
}

impl SupportedContextKeyValueStore {
    pub fn possible_values() -> Vec<&'static str> {
        let mut possible_values = Vec::new();
        for sp in SupportedContextKeyValueStore::iter() {
            possible_values.extend(sp.supported_values());
        }
        possible_values
    }

    fn supported_values(&self) -> Vec<&'static str> {
        match self {
            SupportedContextKeyValueStore::InMem => vec!["inmem"],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseKeyValueStoreBackendError(String);

impl FromStr for SupportedContextKeyValueStore {
    type Err = ParseKeyValueStoreBackendError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        for sp in SupportedContextKeyValueStore::iter() {
            if sp.supported_values().contains(&s.as_str()) {
                return Ok(sp);
            }
        }

        Err(ParseKeyValueStoreBackendError(format!(
            "Invalid variant name: {}",
            s
        )))
    }
}
