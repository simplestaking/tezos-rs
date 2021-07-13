// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{borrow::Cow, cell::Cell};

use crate::hash::{hash_entry, EntryHash, HashingError};
use crate::{kv_store::HashId, ContextKeyValueStore};

use self::{
    storage::{BlobStorageId, Storage, TreeStorageId},
    working_tree::MerkleError,
};

pub mod serializer;
pub mod storage;
pub mod string_interner;
pub mod working_tree;
pub mod working_tree_stats; // TODO - TE-261 remove or reimplement

pub type Tree = TreeStorageId;

use modular_bitfield::prelude::*;
use static_assertions::assert_eq_size;

#[derive(BitfieldSpecifier)]
#[bits = 1]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum NodeKind {
    Leaf,
    NonLeaf,
}

#[bitfield]
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct NodeInner {
    #[skip]
    __: B6,
    node_kind: NodeKind,
    commited: bool,
    entry_hash_id: B32,
    entry_available: bool,
    entry_id: B63,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    pub(crate) inner: Cell<NodeInner>,
}

assert_eq_size!([u8; 13], Node);

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct Commit {
    pub(crate) parent_commit_hash: Option<HashId>,
    pub(crate) root_hash: HashId,
    pub(crate) time: u64,
    pub(crate) author: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Entry {
    Tree(Tree),
    Blob(BlobStorageId),
    Commit(Box<Commit>),
}

impl Node {
    pub fn is_commited(&self) -> bool {
        self.inner.get().commited()
    }

    pub fn set_commited(&self, commited: bool) {
        let inner = self.inner.get().with_commited(commited);
        self.inner.set(inner);
    }

    pub fn node_kind(&self) -> NodeKind {
        self.inner.get().node_kind()
    }

    pub fn hash_id(&self) -> Option<HashId> {
        let id = self.inner.get().entry_hash_id();
        HashId::new(id)
    }

    pub fn get_entry(&self) -> Option<Entry> {
        let inner = self.inner.get();

        if !inner.entry_available() {
            return None;
        }

        let entry_id: u64 = inner.entry_id();
        match self.node_kind() {
            NodeKind::NonLeaf => Some(Entry::Tree(TreeStorageId::from(entry_id))),
            NodeKind::Leaf => Some(Entry::Blob(BlobStorageId::from(entry_id))),
        }
    }

    pub fn entry_hash<'a>(
        &self,
        store: &'a mut ContextKeyValueStore,
        tree_storage: &Storage,
    ) -> Result<Cow<'a, EntryHash>, HashingError> {
        let hash_id = self
            .entry_hash_id(store, tree_storage)?
            .ok_or(HashingError::HashIdEmpty)?;
        Ok(store
            .get_hash(hash_id)?
            .ok_or_else(|| HashingError::HashIdNotFound { hash_id })?)
    }

    pub fn entry_hash_id(
        &self,
        store: &mut ContextKeyValueStore,
        tree_storage: &Storage,
    ) -> Result<Option<HashId>, HashingError> {
        match self.hash_id() {
            Some(hash_id) => Ok(Some(hash_id)),
            None => {
                let hash_id = hash_entry(
                    self.get_entry()
                        .as_ref()
                        .ok_or(HashingError::MissingEntry)?,
                    store,
                    tree_storage,
                )?;
                if let Some(hash_id) = hash_id {
                    let mut inner = self.inner.get();
                    inner.set_entry_hash_id(hash_id.as_u32());
                    self.inner.set(inner);
                };
                Ok(hash_id)
            }
        }
    }

    pub fn new(node_kind: NodeKind, entry: Entry) -> Self {
        Node {
            inner: Cell::new(
                NodeInner::new()
                    .with_commited(false)
                    .with_node_kind(node_kind)
                    .with_entry_hash_id(0)
                    .with_entry_available(true)
                    .with_entry_id(match entry {
                        Entry::Tree(tree_id) => tree_id.into(),
                        Entry::Blob(blob_id) => blob_id.into(),
                        Entry::Commit(_) => panic!(),
                    }),
            ),
        }
    }

    pub fn new_commited(
        node_kind: NodeKind,
        hash_id: Option<HashId>,
        entry: Option<Entry>,
    ) -> Self {
        Node {
            inner: Cell::new(
                NodeInner::new()
                    .with_commited(true)
                    .with_node_kind(node_kind)
                    .with_entry_hash_id(hash_id.map(|h| h.as_u32()).unwrap_or(0))
                    .with_entry_available(entry.is_some())
                    .with_entry_id(match entry {
                        Some(Entry::Tree(tree_id)) => tree_id.into(),
                        Some(Entry::Blob(blob_id)) => blob_id.into(),
                        Some(Entry::Commit(_)) => panic!(),
                        None => 0,
                    }),
            ),
        }
    }

    pub fn get_hash_id(&self) -> Result<HashId, MerkleError> {
        self.hash_id()
            .ok_or(MerkleError::InvalidState("Missing entry hash"))
    }

    pub fn set_entry(&self, entry: &Entry) -> Result<(), MerkleError> {
        let mut inner = self.inner.get();

        match entry {
            Entry::Tree(tree_id) => {
                let tree_id: u64 = (*tree_id).into();
                if tree_id != inner.entry_id() || !inner.entry_available() {
                    inner.set_entry_available(true);
                    inner.set_node_kind(NodeKind::NonLeaf);
                    inner.set_entry_id(tree_id);
                }
            }
            Entry::Blob(blob_id) => {
                let blob_id: u64 = (*blob_id).into();
                if blob_id != inner.entry_id() || !inner.entry_available() {
                    inner.set_entry_available(true);
                    inner.set_node_kind(NodeKind::Leaf);
                    inner.set_entry_id(blob_id);
                }
            }
            Entry::Commit(_) => {
                panic!()
            }
        };

        self.inner.set(inner);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::working_tree::storage::NodeId;
    use crate::working_tree::string_interner::StringId;

    use super::*;

    #[test]
    fn test_node() {
        assert!(!std::mem::needs_drop::<Node>());
        assert!(!std::mem::needs_drop::<NodeId>());
        assert!(!std::mem::needs_drop::<(StringId, NodeId)>());
    }
}
