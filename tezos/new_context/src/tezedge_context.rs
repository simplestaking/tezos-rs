// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    cell::{Ref, RefCell},
    collections::HashSet,
    convert::TryInto,
};
use std::{convert::TryFrom, rc::Rc};

use crypto::hash::ContextHash;
use ocaml_interop::BoxRoot;

use crate::{
    hash::EntryHash,
    working_tree::{Commit, Entry, Tree},
};
use crate::{
    working_tree::working_tree::{MerkleError, WorkingTree},
    IndexApi,
};
use crate::{
    working_tree::{working_tree_stats::MerkleStoragePerfReport, KeyFragment},
    ContextKeyOwned,
};
use crate::{
    ContextError, ContextKey, ContextValue, ProtocolContextApi, ShellContextApi, StringTreeEntry,
    TreeId,
};

use crate::ContextKeyValueStore;

// Represents the patch_context function passed from the OCaml side
// It is opaque to rust, we don't care about it's actual type
// because it is not used on Rust, but we need a type to represent it.
pub struct PatchContextFunction {}

#[derive(Clone)]
pub struct TezedgeIndex {
    pub repository: Rc<RefCell<ContextKeyValueStore>>,
    pub patch_context: Rc<Option<BoxRoot<PatchContextFunction>>>,
    pub strings: Rc<RefCell<StringInterner>>,
}

impl TezedgeIndex {
    pub fn new(
        repository: Rc<RefCell<ContextKeyValueStore>>,
        patch_context: Option<BoxRoot<PatchContextFunction>>,
    ) -> Self {
        let patch_context = Rc::new(patch_context);
        Self {
            repository,
            patch_context,
            strings: Default::default(),
        }
    }

    pub fn get_str(&self, s: &str) -> Rc<str> {
        let mut strings = self.strings.borrow_mut();
        strings.get_str(s)
    }
}

impl IndexApi<TezedgeContext> for TezedgeIndex {
    fn exists(&self, context_hash: &ContextHash) -> Result<bool, ContextError> {
        let context_hash_arr: EntryHash = context_hash.as_ref().as_slice().try_into()?;
        if let Some(Entry::Commit(_)) = db_get_entry(self.repository.borrow(), &context_hash_arr)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn checkout(&self, context_hash: &ContextHash) -> Result<Option<TezedgeContext>, ContextError> {
        let context_hash_arr: EntryHash = context_hash.as_ref().as_slice().try_into()?;

        if let Some(commit) = db_get_commit(self.repository.borrow(), &context_hash_arr)? {
            if let Some(tree) = db_get_tree(self.repository.borrow(), &commit.root_hash)? {
                let tree = WorkingTree::new_with_tree(self.clone(), tree);

                Ok(Some(TezedgeContext::new(
                    self.clone(),
                    Some(context_hash_arr),
                    Some(Rc::new(tree)),
                )))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn block_applied(&mut self, last_commit_hash: ContextHash) -> Result<(), ContextError> {
        let commit_hash_arr: EntryHash = last_commit_hash.as_ref().as_slice().try_into()?;
        Ok(self
            .repository
            .borrow_mut()
            .block_applied(commit_hash_arr)?)
    }

    fn cycle_started(&mut self) -> Result<(), ContextError> {
        Ok(self.repository.borrow_mut().new_cycle_started()?)
    }
}

#[derive(Default)]
pub struct StringInterner {
    strings: HashSet<Rc<str>>,
}

impl StringInterner {
    fn get_str(&mut self, s: &str) -> Rc<str> {
        if s.len() >= 30 {
            return Rc::from(s);
        }

        self.strings.get_or_insert_with(s, |s| Rc::from(s)).clone()
    }
}

// context implementation using merkle-tree-like storage
#[derive(Clone)]
pub struct TezedgeContext {
    pub index: TezedgeIndex,
    pub parent_commit_hash: Option<EntryHash>, // TODO: Rc instead?
    pub tree_id: TreeId,
    tree_id_generator: Rc<RefCell<TreeIdGenerator>>,
    pub tree: Rc<WorkingTree>,
}

impl ProtocolContextApi for TezedgeContext {
    fn add(&self, key: &ContextKey, value: ContextValue) -> Result<Self, ContextError> {
        let tree = self.tree.add(key, value)?;

        Ok(self.with_tree(tree))
    }

    fn delete(&self, key_prefix_to_delete: &ContextKey) -> Result<Self, ContextError> {
        let tree = self.tree.delete(key_prefix_to_delete)?;

        Ok(self.with_tree(tree))
    }

    fn copy(
        &self,
        from_key: &ContextKey,
        to_key: &ContextKey,
    ) -> Result<Option<Self>, ContextError> {
        if let Some(tree) = self.tree.copy(from_key, to_key)? {
            Ok(Some(self.with_tree(tree)))
        } else {
            Ok(None)
        }
    }

    fn find(&self, key: &ContextKey) -> Result<Option<ContextValue>, ContextError> {
        Ok(self.tree.find(key)?)
    }

    fn mem(&self, key: &ContextKey) -> Result<bool, ContextError> {
        Ok(self.tree.mem(key)?)
    }

    fn mem_tree(&self, key: &ContextKey) -> bool {
        self.tree.mem_tree(key)
    }

    fn find_tree(&self, key: &ContextKey) -> Result<Option<WorkingTree>, ContextError> {
        self.tree.find_tree(key).map_err(Into::into)
    }

    fn add_tree(&self, key: &ContextKey, tree: &WorkingTree) -> Result<Self, ContextError> {
        Ok(self.with_tree(self.tree.add_tree(key, tree)?))
    }

    fn empty(&self) -> Self {
        self.with_tree(self.tree.empty())
    }

    fn list(
        &self,
        offset: Option<usize>,
        length: Option<usize>,
        key: &ContextKey,
    ) -> Result<Vec<(KeyFragment, WorkingTree)>, ContextError> {
        self.tree.list(offset, length, key).map_err(Into::into)
    }

    fn fold(&self, key: &ContextKey) {
        self.tree.fold(key)
    }

    fn get_merkle_root(&self) -> Result<EntryHash, ContextError> {
        Ok(self.tree.get_working_tree_root_hash()?)
    }
}

impl ShellContextApi for TezedgeContext {
    fn commit(
        &self,
        author: String,
        message: String,
        date: i64,
    ) -> Result<ContextHash, ContextError> {
        // Entries to be inserted are obtained from the commit call and written here
        let date: u64 = date.try_into()?;
        let (commit_hash, batch) =
            self.tree
                .prepare_commit(date, author, message, self.parent_commit_hash)?;
        // FIXME: only write entries if there are any, empty commits should not produce anything
        self.index.repository.borrow_mut().write_batch(batch)?;
        let commit_hash = ContextHash::try_from(&commit_hash[..])?;

        Ok(commit_hash)
    }

    fn hash(
        &self,
        author: String,
        message: String,
        date: i64,
    ) -> Result<ContextHash, ContextError> {
        // FIXME: does more work than needed, just calculate the hash, no batch
        let date: u64 = date.try_into()?;
        let (commit_hash, _batch) =
            self.tree
                .prepare_commit(date, author, message, self.parent_commit_hash)?;
        let commit_hash = ContextHash::try_from(&commit_hash[..])?;

        Ok(commit_hash)
    }

    fn get_key_from_history(
        &self,
        context_hash: &ContextHash,
        key: &ContextKey,
    ) -> Result<Option<ContextValue>, ContextError> {
        let context_hash_arr: EntryHash = context_hash.as_ref().as_slice().try_into()?;
        match self.tree.get_history(&context_hash_arr, key) {
            Err(MerkleError::ValueNotFound { key: _ }) => Ok(None),
            Err(MerkleError::EntryNotFound { hash: _ }) => {
                Err(ContextError::UnknownContextHashError {
                    context_hash: context_hash.to_base58_check(),
                })
            }
            Err(err) => Err(ContextError::MerkleStorageError { error: err }),
            Ok(val) => Ok(Some(val)),
        }
    }

    fn get_key_values_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: &ContextKey,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, ContextError> {
        let context_hash_arr: EntryHash = context_hash.as_ref().as_slice().try_into()?;
        self.tree
            .get_key_values_by_prefix(&context_hash_arr, prefix)
            .map_err(ContextError::from)
    }

    fn get_context_tree_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: &ContextKey,
        depth: Option<usize>,
    ) -> Result<StringTreeEntry, ContextError> {
        let context_hash_arr: EntryHash = context_hash.as_ref().as_slice().try_into()?;
        self.tree
            .get_context_tree_by_prefix(&context_hash_arr, prefix, depth)
            .map_err(ContextError::from)
    }

    fn get_last_commit_hash(&self) -> Result<Option<Vec<u8>>, ContextError> {
        Ok(self.parent_commit_hash.map(|x| x.to_vec()))
    }

    fn get_merkle_stats(&self) -> Result<MerkleStoragePerfReport, ContextError> {
        Ok(self.tree.get_merkle_stats()?)
    }

    fn get_memory_usage(&self) -> Result<usize, ContextError> {
        Ok(self.index.repository.borrow().total_get_mem_usage()?)
    }
}

/// Generator of Tree IDs which are used to simulate pointers when they are not available.
///
/// During a regular use of the context API, contexts that are still in use are kept
/// alive by pointers to them. This is not available when for example, running the context
/// actions replayer tool. To solve that, we generate a tree id for each versionf of the
/// working tree that is produced while applying a block, so that actions can be associated
/// to the tree to which they are applied.
pub struct TreeIdGenerator(TreeId);

impl TreeIdGenerator {
    fn new() -> Self {
        Self(0)
    }

    fn next(&mut self) -> TreeId {
        self.0 += 1;
        self.0
    }
}

impl TezedgeContext {
    // NOTE: only used to start from scratch, otherwise checkout should be used
    pub fn new(
        index: TezedgeIndex,
        parent_commit_hash: Option<EntryHash>,
        tree: Option<Rc<WorkingTree>>,
    ) -> Self {
        let tree = if let Some(tree) = tree {
            tree
        } else {
            Rc::new(WorkingTree::new(index.clone()))
        };
        let tree_id_generator = Rc::new(RefCell::new(TreeIdGenerator::new()));
        let tree_id = tree_id_generator.borrow_mut().next();
        Self {
            index,
            parent_commit_hash,
            tree_id,
            tree_id_generator,
            tree,
        }
    }

    /// Produce a new copy of the context, replacing the tree (and if different, with a new tree id)
    pub fn with_tree(&self, tree: WorkingTree) -> Self {
        // TODO: only generate a new id if tree changes? Either that
        // or generate a new one every time for Irmin even if the tree doesn't change
        let tree_id = self.tree_id_generator.borrow_mut().next();
        let tree = Rc::new(tree);
        Self {
            tree,
            tree_id,
            tree_id_generator: Rc::clone(&self.tree_id_generator),
            index: self.index.clone(),
            ..*self
        }
    }
}

fn db_get_entry(
    db: Ref<ContextKeyValueStore>,
    hash: &EntryHash,
) -> Result<Option<Entry>, ContextError> {
    match db.get(hash)? {
        None => Ok(None),
        Some(entry_bytes) => Ok(Some(bincode::deserialize(&entry_bytes)?)),
    }
}

fn db_get_commit(
    db: Ref<ContextKeyValueStore>,
    hash: &EntryHash,
) -> Result<Option<Commit>, ContextError> {
    match db_get_entry(db, hash)? {
        Some(Entry::Commit(commit)) => Ok(Some(commit)),
        Some(Entry::Tree(_)) => Err(ContextError::FoundUnexpectedStructure {
            sought: "commit".to_string(),
            found: "tree".to_string(),
        }),
        Some(Entry::Blob(_)) => Err(ContextError::FoundUnexpectedStructure {
            sought: "commit".to_string(),
            found: "blob".to_string(),
        }),
        None => Ok(None),
    }
}

fn db_get_tree(
    db: Ref<ContextKeyValueStore>,
    hash: &EntryHash,
) -> Result<Option<Tree>, ContextError> {
    match db_get_entry(db, hash)? {
        Some(Entry::Tree(tree)) => Ok(Some(tree)),
        Some(Entry::Blob(_)) => Err(ContextError::FoundUnexpectedStructure {
            sought: "tree".to_string(),
            found: "blob".to_string(),
        }),
        Some(Entry::Commit { .. }) => Err(ContextError::FoundUnexpectedStructure {
            sought: "tree".to_string(),
            found: "commit".to_string(),
        }),
        None => Ok(None),
    }
}
