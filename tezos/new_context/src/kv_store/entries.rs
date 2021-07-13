// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    convert::{TryFrom, TryInto},
    marker::PhantomData,
    slice::SliceIndex,
};

#[derive(Debug)]
pub struct Entries<K, V> {
    entries: Vec<V>,
    _phantom: PhantomData<K>,
}

impl<K, V> Default for Entries<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Entries<K, V> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            _phantom: PhantomData,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Vec::with_capacity(cap),
            _phantom: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub fn get_index<I>(&self, index: I) -> Option<&<I as SliceIndex<[V]>>::Output>
    where
        I: SliceIndex<[V]>,
    {
        self.entries.get(index)
    }

    pub fn as_slice(&self) -> &[V] {
        self.entries.as_slice()
    }

    pub fn clear(&mut self) {
        // unsafe {
        //     self.entries.set_len(0);
        // }
        self.entries.clear();
    }
}

impl<K, V> Entries<K, V>
where
    K: TryInto<usize>,
{
    pub fn set(&mut self, key: K, value: V) -> Result<V, K::Error> {
        Ok(std::mem::replace(&mut self.entries[key.try_into()?], value))
    }

    pub fn get(&self, key: K) -> Result<Option<&V>, K::Error> {
        Ok(self.entries.get(key.try_into()?))
    }

    pub fn get_mut(&mut self, key: K) -> Result<Option<&mut V>, K::Error> {
        Ok(self.entries.get_mut(key.try_into()?))
    }
}

impl<K, V> Entries<K, V>
where
    K: TryFrom<usize>,
{
    pub fn push(&mut self, value: V) -> Result<K, <K as TryFrom<usize>>::Error> {
        let current = self.entries.len();
        self.entries.push(value);
        K::try_from(current)
    }
}

impl<K, V> Entries<K, V>
where
    K: TryInto<usize>,
    K: TryFrom<usize>,
    V: Default,
{
    pub fn get_vacant_entry(&mut self) -> Result<(K, &mut V), <K as TryFrom<usize>>::Error> {
        let current = self.entries.len();
        self.entries.push(Default::default());
        Ok((K::try_from(current)?, &mut self.entries[current]))
    }

    pub fn insert_at(&mut self, key: K, value: V) -> Result<V, <K as TryInto<usize>>::Error> {
        let index: usize = key.try_into()?;

        if index >= self.entries.len() {
            self.entries.resize_with(index + 1, V::default);
        }

        Ok(std::mem::replace(&mut self.entries[index], value))
    }
}
