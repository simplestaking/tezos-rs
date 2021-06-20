// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

// TODO: these comments are not entirely up to date now (and haven't been for a while), update.

//! # WorkingTree
//!
//! Abstract merkle storage with git-like semantics and history which can be used with different K-V stores.
//!
//! # Data Structure
//! A storage with just one key `a/b/c` and its corresponding value `8` is represented like this:
//!
//! ``
//! [commit] ---> [tree1] --a--> [tree2] --b--> [tree3] --c--> [blob_8]
//! ``
//!
//! The db then contains the following:
//! ```no_compile
//! <hash_of_blob; blob8>
//! <hash_of_tree3, tree3>, where tree3 is a map {c: hash_blob8}
//! <hash_of_tree2, tree2>, where tree2 is a map {b: hash_of_tree3}
//! <hash_of_tree2, tree2>, where tree1 is a map {a: hash_of_tree2}
//! <hash_of_commit>; commit>, where commit points to the root tree (tree1)
//! ```
//!
//! Then, when looking for a path a/b/c in a spcific commit, we first get the hash of the root tree
//! from the commit, then get the tree from the database, get the hash of "a", look it up in the db,
//! get the hash of "b" from that tree, load from db, then get the hash of "c" and retrieve the
//! final value.
//!
//!
//! Now, let's assume we want to add a path `X` also referencing the value `8`. That creates a new
//! tree that reuses the previous subtree for `a/b/c` and branches away from root for `X`:
//!
//! ```no_compile
//! [tree1] --a--> [tree2] --b--> [tree3] --c--> [blob_8]
//!                   ^                             ^
//!                   |                             |
//! [tree_X]----a-----                              |
//!     |                                           |
//!      ----------------------X--------------------
//! ```
//!
//! The following is added to the database:
//! ``
//! <hash_of_tree_X; tree_X>, where tree_X is a map {a: hash_of_tree2, X: hash_of_blob8}
//! ``
//!
//! Reference: https://git-scm.com/book/en/v2/Git-Internals-Git-Objects
use std::{
    array::TryFromSliceError,
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::HashSet,
    rc::Rc,
};

use thiserror::Error;

use crypto::hash::FromBytesError;

use crate::hash::{hash_commit, hash_tree, HashingError};
use crate::persistent;
use crate::working_tree::{Commit, Entry, Node, NodeKind, Tree};
use crate::{gc::GarbageCollectionError, tezedge_context::TezedgeIndex};
use crate::{hash::EntryHash, ContextKeyOwned};
use crate::{ContextKey, ContextValue};

use super::KeyFragment;

// The 'working tree' can be either a Tree or a Value
#[derive(Clone)]
enum WorkingTreeValue {
    Tree(Tree),
    Value(ContextValue),
}

#[derive(Clone)]
pub struct WorkingTree {
    value: WorkingTreeValue,
    index: TezedgeIndex,
}

#[derive(Clone, Copy)]
pub enum FoldDepth {
    Eq(i64), // folds over nodes and contents of depth exactly [d].
    Lt(i64), // folds over nodes and contents of depth strictly less than [d].
    Le(i64), // folds over nodes and contents of depth less than or equal to [d].
    Gt(i64), // folds over nodes and contents of depth strictly more than [d].
    Ge(i64), // folds over nodes and contents of depth more than or equal to [d].
}

impl FoldDepth {
    /// `true` if for this depth the fold function should be applied
    fn should_apply(self, depth: i64) -> bool {
        match self {
            Self::Eq(n) => depth == n,
            Self::Lt(n) => depth < n,
            Self::Le(n) => depth <= n,
            Self::Gt(n) => depth > n,
            Self::Ge(n) => depth >= n,
        }
    }

    /// `true` if for this depth we should continue scanning the children
    fn should_continue(self, depth: i64) -> bool {
        match self {
            Self::Eq(n) => depth < n,
            Self::Lt(n) => depth < n - 1,
            Self::Le(n) => depth < n,
            Self::Gt(_) => true,
            Self::Ge(_) => true,
        }
    }
}

struct TreeWalkerLevel {
    key: ContextKeyOwned,
    root: WorkingTree,
    current_depth: i64,
    yield_self: bool,
    children_iter: Option<im_rc::ordmap::ConsumingIter<(KeyFragment, Rc<Node>)>>,
}

impl TreeWalkerLevel {
    fn new(
        key: ContextKeyOwned,
        root: WorkingTree,
        current_depth: i64,
        depth: &Option<FoldDepth>,
    ) -> Self {
        let should_continue = depth
            .map(|d| d.should_continue(current_depth))
            .unwrap_or(true);

        let children_iter = if should_continue {
            if let WorkingTreeValue::Tree(tree) = &root.value {
                // TODO: can this clone be avoided?
                Some(tree.clone().into_iter())
            } else {
                None
            }
        } else {
            None
        };

        Self {
            key,
            root,
            current_depth,
            yield_self: depth.map(|d| d.should_apply(current_depth)).unwrap_or(true),
            children_iter,
        }
    }
}

pub struct TreeWalker {
    depth: Option<FoldDepth>,
    stack: Vec<TreeWalkerLevel>,
}

impl TreeWalker {
    fn new(key: ContextKeyOwned, root: WorkingTree, depth: Option<FoldDepth>) -> Self {
        Self {
            depth,
            stack: vec![TreeWalkerLevel::new(key, root, 0, &depth)],
        }
    }

    fn empty() -> Self {
        Self {
            depth: None,
            stack: vec![],
        }
    }
}

impl Iterator for TreeWalker {
    type Item = (ContextKeyOwned, WorkingTree);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current_level) = self.stack.last_mut() {
                if current_level.yield_self {
                    current_level.yield_self = false;
                    return Some((current_level.key.clone(), current_level.root.clone()));
                }

                if let Some(iter) = &mut current_level.children_iter {
                    let current_depth = current_level.current_depth + 1;

                    if let Some((k, node)) = iter.next() {
                        // TODO: what to do with errors here?
                        if let Ok(root) = current_level.root.node_tree(&node) {
                            // TODO: this is not very efficient, maybe we need to improve the key representation
                            let mut key = current_level.key.clone();
                            key.push(k.to_string());

                            self.stack.push(TreeWalkerLevel::new(
                                key,
                                root,
                                current_depth,
                                &self.depth,
                            ));
                        }

                        continue;
                    }
                }

                self.stack.pop();
            } else {
                // Nothing left, we are done iterating
                return None;
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum MerkleError {
    /// External libs errors
    #[error("RocksDB error: {error:?}")]
    DBError {
        error: persistent::database::DBError,
    },
    #[error("Serialization error: {error:?}")]
    SerializationError { error: bincode::Error },
    #[error("Backend error: {error:?}")]
    GarbageCollectionError { error: GarbageCollectionError },

    /// Internal unrecoverable bugs that should never occur
    #[error("There is a commit or three under key {key:?}, but not a value!")]
    ValueIsNotABlob { key: String },
    #[error("Found wrong structure. Was looking for {sought}, but found {found}")]
    FoundUnexpectedStructure { sought: String, found: String },
    #[error("Entry not found! Hash={hash}")]
    EntryNotFound { hash: String },

    /// Wrong user input errors
    #[error("No value under key {key:?}.")]
    ValueNotFound { key: String },
    #[error("Cannot search for an empty key.")]
    KeyEmpty,
    #[error("Failed to convert hash into array: {error}")]
    HashToArrayError { error: TryFromSliceError },
    #[error("Failed to convert hash into string: {error}")]
    HashToStringError { error: FromBytesError },
    #[error("Failed to encode hash: {error}")]
    HashingError { error: HashingError },
    #[error("Expected value instead of `None` for {0}")]
    ValueExpected(&'static str),
    #[error("Invalid state: {0}")]
    InvalidState(&'static str),
}

impl From<persistent::database::DBError> for MerkleError {
    fn from(error: persistent::database::DBError) -> Self {
        MerkleError::DBError { error }
    }
}

impl From<HashingError> for MerkleError {
    fn from(error: HashingError) -> Self {
        Self::HashingError { error }
    }
}

impl From<GarbageCollectionError> for MerkleError {
    fn from(error: GarbageCollectionError) -> Self {
        Self::GarbageCollectionError { error }
    }
}

impl From<bincode::Error> for MerkleError {
    fn from(error: bincode::Error) -> Self {
        Self::SerializationError { error }
    }
}

impl From<TryFromSliceError> for MerkleError {
    fn from(error: TryFromSliceError) -> Self {
        Self::HashToArrayError { error }
    }
}

impl From<FromBytesError> for MerkleError {
    fn from(error: FromBytesError) -> Self {
        Self::HashToStringError { error }
    }
}

#[derive(Debug, Error)]
pub enum CheckEntryHashError {
    #[error("MerkleError error: {error:?}")]
    MerkleError { error: MerkleError },
    #[error("Calculated hash for {entry_type} not matching expected hash: expected {expected:?}, calculated {calculated:?}")]
    InvalidHashError {
        entry_type: String,
        calculated: String,
        expected: String,
    },
}

impl WorkingTree {
    pub fn new(index: TezedgeIndex) -> Self {
        Self::new_with_tree(index, Tree::new())
    }

    pub fn new_with_tree(index: TezedgeIndex, tree: Tree) -> Self {
        WorkingTree {
            index,
            value: WorkingTreeValue::Tree(tree),
        }
    }

    pub fn new_with_value(index: TezedgeIndex, value: ContextValue) -> Self {
        WorkingTree {
            index,
            value: WorkingTreeValue::Value(value),
        }
    }

    pub fn get_value(&self) -> Option<ContextValue> {
        match &self.value {
            WorkingTreeValue::Tree(_) => None,
            WorkingTreeValue::Value(value) => Some(value.clone()),
        }
    }

    pub fn find_tree(&self, key: &ContextKey) -> Result<Option<Self>, MerkleError> {
        let root = self.get_working_tree_root_ref();
        let tree = self.find_raw_tree(root.as_ref(), key)?;

        if tree.is_empty() {
            return Ok(None);
        }

        Ok(Some(Self::new_with_tree(self.index.clone(), tree)))
    }

    pub fn add_tree(&self, key: &ContextKey, tree: &Self) -> Result<Self, MerkleError> {
        let node = match tree.value.clone() {
            WorkingTreeValue::Tree(tree) => Self::get_non_leaf(Entry::Tree(tree)),
            WorkingTreeValue::Value(value) => Self::get_leaf(Entry::Blob(value)),
        };

        let entry = &self._add(key, node)?;
        let tree = self.entry_tree(entry)?.clone();

        Ok(self.with_new_root(tree))
    }

    pub fn equal(&self, other: &Self) -> Result<bool, MerkleError> {
        // TODO: ok for now, but perform an actual compare instead of hashing here
        Ok(self.hash()? == other.hash()?)
    }

    pub fn hash(&self) -> Result<EntryHash, MerkleError> {
        self.get_working_tree_root_hash()
    }

    pub fn kind(&self) -> NodeKind {
        match &self.value {
            WorkingTreeValue::Tree(_) => NodeKind::NonLeaf,
            WorkingTreeValue::Value(_) => NodeKind::Leaf,
        }
    }

    pub fn empty(&self) -> Self {
        Self::new(self.index.clone())
    }

    pub fn is_empty(&self) -> bool {
        match &self.value {
            WorkingTreeValue::Tree(tree) => tree.is_empty(),
            WorkingTreeValue::Value(_) => true,
        }
    }

    pub fn list(
        &self,
        offset: Option<usize>,
        length: Option<usize>,
        key: &ContextKey,
    ) -> Result<Vec<(KeyFragment, WorkingTree)>, MerkleError> {
        let root = self.get_working_tree_root_ref();
        let node = self.find_raw_tree(root.as_ref(), key)?;

        let node_length = node.len();
        let length = length.unwrap_or(node_length).min(node_length);
        let offset = offset.unwrap_or(0);

        let mut children = Vec::with_capacity(length);

        for (key, value) in node.iter().skip(offset).take(length) {
            let value = match self.node_entry(value)? {
                Entry::Tree(tree) => Self::new_with_tree(self.index.clone(), tree),
                Entry::Blob(value) => Self::new_with_value(self.index.clone(), value),
                Entry::Commit(_) => continue,
            };

            children.push((key.clone(), value));
        }

        Ok(children)
    }

    fn node_tree(&self, node: &Node) -> Result<Self, MerkleError> {
        let entry = self.node_entry(&node)?;
        let tree = match node.node_kind {
            NodeKind::NonLeaf => WorkingTree {
                index: self.index.clone(),
                value: WorkingTreeValue::Tree(self.entry_tree(&entry)?.clone()),
            },
            NodeKind::Leaf => WorkingTree {
                index: self.index.clone(),
                value: WorkingTreeValue::Value(self.entry_value(&entry)?.clone()),
            },
        };
        Ok(tree)
    }

    // From OCaml:
    /*
      [fold ?depth t root ~init ~f] recursively folds over the trees
      and values of [t]. The [f] callbacks are called with a key relative
      to [root]. [f] is never called with an empty key for values; i.e.,
      folding over a value is a no-op.

      Elements are traversed in lexical order of keys.

      The depth is 0-indexed. If [depth] is set (by default it is not), then [f]
      is only called when the conditions described by the parameter is true:

      - [Eq d] folds over nodes and contents of depth exactly [d].
      - [Lt d] folds over nodes and contents of depth strictly less than [d].
      - [Le d] folds over nodes and contents of depth less than or equal to [d].
      - [Gt d] folds over nodes and contents of depth strictly more than [d].
      - [Ge d] folds over nodes and contents of depth more than or equal to [d].
    */
    pub fn fold_iter(
        &self,
        depth: Option<FoldDepth>,
        key: &ContextKey,
    ) -> Result<TreeWalker, MerkleError> {
        if let Some(root) = self.find_tree(key)? {
            Ok(TreeWalker::new(
                vec![], // Key is relative to the root
                root,
                depth,
            ))
        } else {
            Ok(TreeWalker::empty())
        }
    }

    /// Get value from current working tree
    pub fn find(&self, key: &ContextKey) -> Result<Option<ContextValue>, MerkleError> {
        let root = self.get_working_tree_root_ref();
        let rv = match self.get_from_tree(root.as_ref(), key) {
            Ok(value) => Ok(Some(value)),
            Err(MerkleError::ValueNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        };

        rv
    }

    /// Check if value exists in current working tree
    pub fn mem(&self, key: &ContextKey) -> Result<bool, MerkleError> {
        let root = self.get_working_tree_root_ref();
        let rv = self.value_exists(root.as_ref(), key);

        rv
    }

    /// Check if directory exists in current staged root
    pub fn mem_tree(&self, key: &ContextKey) -> bool {
        let root = self.get_working_tree_root_ref();
        let rv = self.directory_exists(root.as_ref(), key);

        rv
    }

    fn value_exists(&self, tree: &Tree, key: &ContextKey) -> Result<bool, MerkleError> {
        let (file, path) = key.split_last().ok_or(MerkleError::KeyEmpty)?;

        // find tree by path
        self.find_raw_tree(&tree, &path)
            .map(|node| node.get(*file).is_some())
            .or(Ok(false))
    }

    fn directory_exists(&self, root: &Tree, key: &ContextKey) -> bool {
        // find tree by path
        self.find_raw_tree(root, &key)
            .map(|node| !node.is_empty())
            .unwrap_or(false)
    }

    fn get_from_tree(&self, root: &Tree, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let (file, path) = key.split_last().ok_or(MerkleError::KeyEmpty)?;
        let node = self.find_raw_tree(&root, &path)?;

        // get file node from tree
        let node = node.get(*file).ok_or_else(|| MerkleError::ValueNotFound {
            key: self.key_to_string(key),
        })?;

        // get blob
        match self.node_entry(&node)? {
            Entry::Blob(blob) => Ok(blob),
            _ => Err(MerkleError::ValueIsNotABlob {
                key: self.key_to_string(key),
            }),
        }
    }

    // TODO: recursion is risky (stack overflow) and inefficient, try to do it iteratively..
    fn get_key_values_from_tree_recursively(
        &self,
        path: &str,
        entry: &Entry,
        entries: &mut Vec<(ContextKeyOwned, ContextValue)>,
    ) -> Result<(), MerkleError> {
        match entry {
            Entry::Blob(blob) => {
                // push key-value pair
                entries.push((self.string_to_key(path), blob.clone()));
                Ok(())
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                tree.iter()
                    .map(|(key, child_node)| {
                        let fullpath = path.to_owned() + "/" + key;

                        match self.node_entry(&child_node) {
                            Err(_) => Ok(()),
                            Ok(entry) => self
                                .get_key_values_from_tree_recursively(&fullpath, &entry, entries),
                        }
                    })
                    .find_map(|res| match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    })
                    .unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => match self.get_entry_from_hash(&commit.root_hash) {
                Err(err) => Err(err),
                Ok(entry) => self.get_key_values_from_tree_recursively(path, &entry, entries),
            },
        }
    }

    /// Construct Vec of all context key-values under given prefix
    pub fn get_key_values_by_prefix(
        &self,
        context_hash: &EntryHash,
        prefix: &ContextKey,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, MerkleError> {
        let commit = self.get_commit(context_hash)?;
        let entry = self.get_entry_from_hash(&commit.root_hash)?;
        let root_tree = self.entry_tree(&entry)?;
        let rv = self._get_key_values_by_prefix(root_tree, prefix);

        rv
    }

    fn _get_key_values_by_prefix(
        &self,
        root_tree: &Tree,
        prefix: &ContextKey,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, MerkleError> {
        let prefixed_tree = self.find_raw_tree(root_tree, prefix)?;
        let mut keyvalues: Vec<(ContextKeyOwned, ContextValue)> = Vec::new();
        let delimiter = if prefix.is_empty() { "" } else { "/" };

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.node_entry(&child_node)?;

            // construct full path as Tree key is only one chunk of it
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            self.get_key_values_from_tree_recursively(&fullpath, &entry, &mut keyvalues)?;
        }

        if keyvalues.is_empty() {
            Ok(None)
        } else {
            Ok(Some(keyvalues))
        }
    }

    /// Take the current changes in the staging area, create a commit and persist all changes
    /// to database under the new commit. Return last commit if there are no changes, that is
    /// empty commits are not allowed.
    // FIXME: we don't check for empty commits.
    pub fn prepare_commit(
        &self,
        time: u64,
        author: String,
        message: String,
        parent_commit_hash: Option<EntryHash>,
    ) -> Result<
        (
            EntryHash,
            Vec<(EntryHash, ContextValue)>,
            HashSet<EntryHash>,
        ),
        MerkleError,
    > {
        let root_hash = self.get_working_tree_root_hash()?;
        let root = self.get_working_tree_root_ref();

        let new_commit = Commit {
            root_hash,
            parent_commit_hash,
            time,
            author,
            message,
        };
        let entry = Entry::Commit(new_commit.clone());
        let commit_hash = hash_commit(&new_commit)?;

        // produce entries to be persisted to storage
        let mut batch: Vec<(EntryHash, ContextValue)> = Vec::new();
        let mut referenced_older_entries: HashSet<EntryHash> = HashSet::new();
        self.get_entries_recursively(
            &entry,
            commit_hash,
            Some(root.as_ref()),
            &mut batch,
            &mut referenced_older_entries,
        )?;

        Ok((commit_hash, batch, referenced_older_entries))
    }

    /// Returns a new version of the WorkingTree with the tree replaced
    pub fn with_new_root(&self, tree: Tree) -> Self {
        let index = self.index.clone();
        Self {
            value: WorkingTreeValue::Tree(tree),
            index,
        }
    }

    /// Set key/val to the working tree.
    pub fn add(&self, key: &ContextKey, value: ContextValue) -> Result<Self, MerkleError> {
        let node = Self::get_leaf(Entry::Blob(value));
        let entry = &self._add(key, node)?;
        let tree = self.entry_tree(entry)?.clone();

        Ok(self.with_new_root(tree))
    }

    fn _add(&self, key: &ContextKey, node: Node) -> Result<Entry, MerkleError> {
        self.compute_new_root_with_change(&key, Some(node))
    }

    /// Delete an item from the staging area.
    pub fn delete(&self, key: &ContextKey) -> Result<Self, MerkleError> {
        let new_root_entry = &self._delete(key)?;
        let tree = self.entry_tree(new_root_entry)?.clone();
        Ok(self.with_new_root(tree))
    }

    fn _delete(&self, key: &ContextKey) -> Result<Entry, MerkleError> {
        let root = self.get_working_tree_root_ref();

        if key.is_empty() {
            return Ok(Entry::Tree(root.as_ref().clone()));
        }
        self.compute_new_root_with_change(&key, None)
    }

    /// Copy subtree under a new path.
    ///
    /// Returns a new tree if the source path exists, or None otherwise.
    pub fn copy(
        &self,
        from_key: &ContextKey,
        to_key: &ContextKey,
    ) -> Result<Option<Self>, MerkleError> {
        if let Some(new_root_entry) = &self._copy(from_key, to_key)? {
            let tree = self.entry_tree(new_root_entry)?.clone();
            Ok(Some(self.with_new_root(tree)))
        } else {
            Ok(None)
        }
    }

    fn _copy(
        &self,
        from_key: &ContextKey,
        to_key: &ContextKey,
    ) -> Result<Option<Entry>, MerkleError> {
        let root = self.get_working_tree_root_ref();

        let source_tree = match self.find_raw_tree(root.as_ref(), &from_key) {
            Ok(tree) => tree,
            Err(MerkleError::EntryNotFound { .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        Ok(Some(self.compute_new_root_with_change(
            &to_key,
            Some(Self::get_non_leaf(Entry::Tree(source_tree))),
        )?))
    }

    /// Get a new tree with `new_node` put under given `key`.
    /// Walk down the tree to find key, set new value and walk back up recalculating hashes -
    /// return new top hash of tree. Note: no writes to DB yet
    ///
    /// # Arguments
    ///
    /// * `root` - Tree to modify
    /// * `key` - path under which the changes takes place
    /// * `new_node` - None for deletion, Some for inserting a hash under the key.
    fn compute_new_root_with_change(
        &self,
        key: &[&str],
        new_node: Option<Node>,
    ) -> Result<Entry, MerkleError> {
        let last = match key.last() {
            Some(last) => *last,
            None => match new_node {
                Some(n) => {
                    // if there is a value we want to assigin - just
                    // assigin it
                    return n
                        .entry
                        .try_borrow()
                        .map_err(|_| MerkleError::InvalidState("Entry borrowed twice"))?
                        .clone()
                        .ok_or(MerkleError::InvalidState("Missing entry value"));
                }
                None => {
                    // if key is empty and there is new_node == None
                    // that means that we just removed whole tree
                    // so set merkle storage root to empty dir and place
                    // it in staging area
                    return Ok(Entry::Tree(Tree::new()));
                }
            },
        };

        let path = &key[..key.len() - 1];
        let root = self.get_working_tree_root_ref();
        let mut tree = self.find_raw_tree(root.as_ref(), path)?;

        match new_node {
            None => tree.remove(last),
            Some(new_node) => {
                let last = self.index.get_str(last);
                tree.insert(last.into(), Rc::new(new_node))
            }
        };

        if tree.is_empty() {
            self.compute_new_root_with_change(path, None)
        } else {
            self.compute_new_root_with_change(path, Some(Self::get_non_leaf(Entry::Tree(tree))))
        }
    }

    fn find_raw_tree(&self, root: &Tree, key: &[&str]) -> Result<Tree, MerkleError> {
        self.index.find_raw_tree(root, key)
    }

    pub fn get_working_tree_root_hash(&self) -> Result<EntryHash, MerkleError> {
        // TOOD: unnecessery recalculation, should be one when set_staged_root
        let root = self.get_working_tree_root_ref();
        hash_tree(root.as_ref()).map_err(MerkleError::from)
    }

    /// Builds vector of entries to be persisted to DB, recursively
    fn get_entries_recursively(
        &self,
        entry: &Entry,
        entry_hash: EntryHash,
        root: Option<&Tree>,
        batch: &mut Vec<(EntryHash, ContextValue)>,
        referenced_older_entries: &mut HashSet<EntryHash>,
    ) -> Result<(), MerkleError> {
        // add entry to batch
        batch.push((entry_hash, bincode::serialize(entry)?));

        match entry {
            Entry::Blob(_) => Ok(()),
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                tree.iter()
                    .map(|(_, child_node)| {
                        if child_node.commited.get() {
                            referenced_older_entries.insert(child_node.entry_hash()?);
                            return Ok(());
                        }
                        child_node.commited.set(true);

                        match child_node
                            .entry
                            .try_borrow()
                            .map_err(|_| MerkleError::InvalidState("Entry borrowed twice"))?
                            .as_ref()
                        {
                            None => Ok(()),
                            Some(entry) => self.get_entries_recursively(
                                entry,
                                child_node.entry_hash()?,
                                None,
                                batch,
                                referenced_older_entries,
                            ),
                        }
                    })
                    .find_map(|res| match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    })
                    .unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => {
                let entry = match root {
                    Some(root) => Entry::Tree(root.clone()),
                    None => self.get_entry_from_hash(&commit.root_hash)?,
                };
                self.get_entries_recursively(
                    &entry,
                    commit.root_hash,
                    None,
                    batch,
                    referenced_older_entries,
                )
            }
        }
    }

    fn entry_tree<'e>(&self, entry: &'e Entry) -> Result<&'e Tree, MerkleError> {
        match entry {
            Entry::Tree(tree) => Ok(tree),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "blob".to_string(),
            }),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    fn entry_value<'e>(&self, entry: &'e Entry) -> Result<&'e ContextValue, MerkleError> {
        match entry {
            Entry::Blob(blob) => Ok(blob),
            Entry::Tree(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "blob".to_string(),
                found: "tree".to_string(),
            }),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "blob".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    fn get_commit(&self, hash: &EntryHash) -> Result<Commit, MerkleError> {
        self.index.get_commit(hash)
    }

    fn node_entry(&self, node: &Node) -> Result<Entry, MerkleError> {
        self.index.node_entry(node)
    }

    fn get_entry_from_hash(&self, hash: &EntryHash) -> Result<Entry, MerkleError> {
        self.index.get_entry(hash)
    }

    fn get_non_leaf(entry: Entry) -> Node {
        Node {
            node_kind: NodeKind::NonLeaf,
            entry_hash: RefCell::new(None),
            entry: RefCell::new(Some(entry)),
            commited: Cell::new(false),
        }
    }

    pub fn get_leaf(entry: Entry) -> Node {
        Node {
            node_kind: NodeKind::Leaf,
            entry_hash: RefCell::new(None),
            entry: RefCell::new(Some(entry)),
            commited: Cell::new(false),
        }
    }

    /// Convert key in array form to string form
    fn key_to_string(&self, key: &ContextKey) -> String {
        self.index.key_to_string(key)
    }

    /// Convert key in string form to array form
    fn string_to_key(&self, string: &str) -> ContextKeyOwned {
        self.index.string_to_key(string)
    }

    fn get_working_tree_root_ref(&self) -> Cow<Tree> {
        match &self.value {
            WorkingTreeValue::Tree(tree) => Cow::Borrowed(tree),
            WorkingTreeValue::Value(_) => Cow::Owned(Tree::new()),
        }
    }

    // TODO: only used in tests on this file
    //    pub fn get_staged_entries(&self) -> Result<std::string::String, MerkleError> {
    //        let mut result = String::new();
    //        for (hash, entry) in self.staged_cache.get_mut().cached() {
    //            match entry {
    //                Entry::Blob(blob) => {
    //                    result += &format!("{}: Value {:?}, \n", hex::encode(&hash[0..3]), blob);
    //                }
    //
    //                Entry::Tree(tree) => {
    //                    if tree.is_empty() {
    //                        continue;
    //                    }
    //                    let tree_hash = &hash_tree(tree)?[0..3];
    //                    result += &format!("{}: Tree {{", hex::encode(tree_hash));
    //
    //                    for (path, val) in tree {
    //                        let kind = if let NodeKind::NonLeaf = val.node_kind {
    //                            "Tree"
    //                        } else {
    //                            "Value/Leaf"
    //                        };
    //                        result += &format!(
    //                            "{}: {}({:?}), ",
    //                            path,
    //                            kind,
    //                            hex::encode(&val.entry_hash[0..3])
    //                        );
    //                    }
    //                    result += "}}\n";
    //                }
    //
    //                Entry::Commit(_) => {
    //                    return Err(MerkleError::InvalidState(
    //                        "commits must not occur in staged area",
    //                    ));
    //                }
    //            }
    //        }
    //        Ok(result)
    //    }
}

//* /// Merkle storage predefined tests with abstraction for underlaying kv_store for context
//*#[cfg(test)]
//*mod tests {
//*    use std::env;
//*    use std::path::PathBuf;
//*
//*    use assert_json_diff::assert_json_eq;
//*
//*    use crate::context::kv_store::test_support::TestContextKvStoreFactoryInstance;
//*    use crate::context::kv_store::SupportedContextKeyValueStore;
//*    use crate::context::ContextValue;
//*
//*    use super::*;
//*
//*    fn get_short_hash(hash: &EntryHash) -> String {
//*        hex::encode(&hash[0..3])
//*    }
//*
//*    fn get_staged_root_short_hash(storage: &mut WorkingTree) -> Result<String, MerkleError> {
//*        let hash = storage.get_staged_root_hash()?;
//*        Ok(get_short_hash(&hash))
//*    }
//*
//*    fn test_duplicate_entry_in_staging(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(
//*            kv_store_factory
//*                .create("test_duplicate_entry_in_staging")
//*                .unwrap(),
//*        );
//*
//*        let a_foo: &ContextKey = &vec!["a".to_string(), "foo".to_string()];
//*        let c_foo: &ContextKey = &vec!["c".to_string(), "foo".to_string()];
//*        storage
//*            .set(1, &vec!["a".to_string(), "foo".to_string()], vec![97, 98])
//*            .unwrap();
//*        storage
//*            .set(2, &vec!["c".to_string(), "zoo".to_string()], vec![1, 2])
//*            .unwrap();
//*        storage
//*            .set(3, &vec!["c".to_string(), "foo".to_string()], vec![97, 98])
//*            .unwrap();
//*        storage
//*            .delete(4, &vec!["c".to_string(), "zoo".to_string()])
//*            .unwrap();
//*        // now c/ is the same tree as a/ - which means there are two references to single entry in staging area
//*        // modify the tree and check that the other one was kept intact
//*        storage
//*            .set(5, &vec!["c".to_string(), "foo".to_string()], vec![3, 4])
//*            .unwrap();
//*        let commit = storage
//*            .commit(0, "Tezos".to_string(), "Genesis".to_string())
//*            .unwrap();
//*        assert_eq!(storage.get_history(&commit, a_foo).unwrap(), vec![97, 98]);
//*        assert_eq!(storage.get_history(&commit, c_foo).unwrap(), vec![3, 4]);
//*    }
//*
//*    fn test_tree_hash(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(kv_store_factory.create("test_tree_hash").unwrap());
//*
//*        storage
//*            .set(
//*                1,
//*                &vec!["a".to_string(), "foo".to_string()],
//*                vec![97, 98, 99],
//*            )
//*            .unwrap(); // abc
//*        storage
//*            .set(2, &vec!["b".to_string(), "boo".to_string()], vec![97, 98])
//*            .unwrap();
//*        storage
//*            .set(
//*                3,
//*                &vec!["a".to_string(), "aaa".to_string()],
//*                vec![97, 98, 99, 100],
//*            )
//*            .unwrap();
//*        storage.set(4, &vec!["x".to_string()], vec![97]).unwrap();
//*        storage
//*            .set(
//*                5,
//*                &vec!["one".to_string(), "two".to_string(), "three".to_string()],
//*                vec![97],
//*            )
//*            .unwrap();
//*        storage
//*            .commit(0, "Tezos".to_string(), "Genesis".to_string())
//*            .unwrap();
//*
//*        let hash = storage.get_staged_root_hash().unwrap();
//*
//*        assert_eq!([0xDB, 0xAE, 0xD7, 0xB6], hash[0..4]);
//*    }
//*
//*    fn test_commit_hash(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(kv_store_factory.create("test_commit_hash").unwrap());
//*
//*        storage
//*            .set(1, &vec!["a".to_string()], vec![97, 98, 99])
//*            .unwrap();
//*
//*        let commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string());
//*
//*        assert_eq!([0xCF, 0x95, 0x18, 0x33], commit.unwrap()[0..4]);
//*
//*        storage
//*            .set(1, &vec!["data".to_string(), "x".to_string()], vec![97])
//*            .unwrap();
//*        let commit = storage.commit(0, "Tezos".to_string(), "".to_string());
//*
//*        assert_eq!([0xCA, 0x7B, 0xC7, 0x02], commit.unwrap()[0..4]);
//*        // full irmin hash: ca7bc7022ffbd35acc97f7defb00c486bb7f4d19a2d62790d5949775eb74f3c8
//*    }
//*
//*    fn test_examples_from_article_about_storage(
//*        kv_store_factory: &TestContextKvStoreFactoryInstance,
//*    ) {
//*        let mut storage = WorkingTree::new(
//*            kv_store_factory
//*                .create("test_examples_from_article_about_storage")
//*                .unwrap(),
//*        );
//*
//*        storage.set(1, &vec!["a".to_string()], vec![1]).unwrap();
//*        let root = get_staged_root_short_hash(&mut storage).expect("hash error");
//*        println!("SET [a] = 1\nROOT: {}", root);
//*        println!("CONTENT {}", storage.get_staged_entries().unwrap());
//*        assert_eq!(root, "d49a53".to_string());
//*
//*        storage
//*            .set(2, &vec!["b".to_string(), "c".to_string()], vec![1])
//*            .unwrap();
//*        let root = get_staged_root_short_hash(&mut storage).expect("hash error");
//*        println!("\nSET [b,c] = 1\nROOT: {}", root);
//*        print!("{}", storage.get_staged_entries().unwrap());
//*        assert_eq!(root, "ed8adf".to_string());
//*
//*        storage
//*            .set(3, &vec!["b".to_string(), "d".to_string()], vec![2])
//*            .unwrap();
//*        let root = get_staged_root_short_hash(&mut storage).expect("hash error");
//*        println!("\nSET [b,d] = 2\nROOT: {}", root);
//*        print!("{}", storage.get_staged_entries().unwrap());
//*        assert_eq!(root, "437186".to_string());
//*
//*        storage.set(4, &vec!["a".to_string()], vec![2]).unwrap();
//*        let root = get_staged_root_short_hash(&mut storage).expect("hash error");
//*        println!("\nSET [a] = 2\nROOT: {}", root);
//*        print!("{}", storage.get_staged_entries().unwrap());
//*        assert_eq!(root, "0d78b3".to_string());
//*
//*        let entries = storage.get_staged_entries().unwrap();
//*        let commit_hash = storage
//*            .commit(0, "Tezedge".to_string(), "persist changes".to_string())
//*            .unwrap();
//*        println!("\nCOMMIT time:0 author:'tezedge' message:'persist'");
//*        println!("ROOT: {}", get_short_hash(&commit_hash));
//*        if let Entry::Commit(c) = storage.get_entry(&commit_hash).unwrap() {
//*            println!("{} : Commit{{time:{}, message:{}, author:{}, root_hash:{}, parent_commit_hash: None}}", get_short_hash(&commit_hash), c.time, c.message, c.author, get_short_hash(&c.root_hash));
//*        }
//*        print!("{}", entries);
//*        assert_eq!("e6de3f", get_short_hash(&commit_hash))
//*    }
//*
//*    fn test_multiple_commit_hash(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(
//*            kv_store_factory
//*                .create("test_multiple_commit_hash")
//*                .unwrap(),
//*        );
//*
//*        let _commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string());
//*
//*        storage
//*            .set(
//*                1,
//*                &vec!["data".to_string(), "a".to_string(), "x".to_string()],
//*                vec![97],
//*            )
//*            .unwrap();
//*        storage
//*            .copy(
//*                2,
//*                &vec!["data".to_string(), "a".to_string()],
//*                &vec!["data".to_string(), "b".to_string()],
//*            )
//*            .unwrap();
//*        storage
//*            .delete(
//*                3,
//*                &vec!["data".to_string(), "b".to_string(), "x".to_string()],
//*            )
//*            .unwrap();
//*        let commit = storage.commit(0, "Tezos".to_string(), "".to_string());
//*
//*        assert_eq!([0x9B, 0xB0, 0x0D, 0x6E], commit.unwrap()[0..4]);
//*    }
//*
//*    fn test_get(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let db_name = "test_get";
//*
//*        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
//*        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
//*        let key_eab: &ContextKey = &vec!["e".to_string(), "a".to_string(), "b".to_string()];
//*        let key_az: &ContextKey = &vec!["a".to_string(), "z".to_string()];
//*        let key_d: &ContextKey = &vec!["d".to_string()];
//*
//*        let kv_store = kv_store_factory.create(db_name).unwrap();
//*        let mut storage = WorkingTree::new(kv_store);
//*
//*        let res = storage.get(&vec![]);
//*        assert_eq!(res.unwrap().is_empty(), true);
//*        let res = storage.get(&vec!["a".to_string()]);
//*        assert_eq!(res.unwrap().is_empty(), true);
//*
//*        storage.set(1, key_abc, vec![1u8, 2u8]).unwrap();
//*        storage.set(2, key_abx, vec![3u8]).unwrap();
//*        assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8, 2u8]);
//*        assert_eq!(storage.get(&key_abx).unwrap(), vec![3u8]);
//*        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*
//*        storage.set(3, key_az, vec![4u8]).unwrap();
//*        storage.set(4, key_abx, vec![5u8]).unwrap();
//*        storage.set(5, key_d, vec![6u8]).unwrap();
//*        storage.set(6, key_eab, vec![7u8]).unwrap();
//*        assert_eq!(storage.get(key_az).unwrap(), vec![4u8]);
//*        assert_eq!(storage.get(key_abx).unwrap(), vec![5u8]);
//*        assert_eq!(storage.get(key_d).unwrap(), vec![6u8]);
//*        assert_eq!(storage.get(key_eab).unwrap(), vec![7u8]);
//*        let commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*
//*        assert_eq!(
//*            storage.get_history(&commit1, key_abc).unwrap(),
//*            vec![1u8, 2u8]
//*        );
//*        assert_eq!(storage.get_history(&commit1, key_abx).unwrap(), vec![3u8]);
//*        assert_eq!(storage.get_history(&commit2, key_abx).unwrap(), vec![5u8]);
//*        assert_eq!(storage.get_history(&commit2, key_az).unwrap(), vec![4u8]);
//*        assert_eq!(storage.get_history(&commit2, key_d).unwrap(), vec![6u8]);
//*        assert_eq!(storage.get_history(&commit2, key_eab).unwrap(), vec![7u8]);
//*    }
//*
//*    fn test_mem(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(kv_store_factory.create("test_mem").unwrap());
//*
//*        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
//*        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
//*
//*        assert_eq!(storage.mem(&key_abc).unwrap(), false);
//*        assert_eq!(storage.mem(&key_abx).unwrap(), false);
//*        storage.set(1, key_abc, vec![1u8, 2u8]).unwrap();
//*        assert_eq!(storage.mem(&key_abc).unwrap(), true);
//*        assert_eq!(storage.mem(&key_abx).unwrap(), false);
//*        storage.set(2, key_abx, vec![3u8]).unwrap();
//*        assert_eq!(storage.mem(&key_abc).unwrap(), true);
//*        assert_eq!(storage.mem(&key_abx).unwrap(), true);
//*        storage.delete(3, key_abx).unwrap();
//*        assert_eq!(storage.mem(&key_abc).unwrap(), true);
//*        assert_eq!(storage.mem(&key_abx).unwrap(), false);
//*    }
//*
//*    fn test_dirmem(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(kv_store_factory.create("test_dirmem").unwrap());
//*
//*        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
//*        let key_ab: &ContextKey = &vec!["a".to_string(), "b".to_string()];
//*        let key_a: &ContextKey = &vec!["a".to_string()];
//*
//*        assert_eq!(storage.dirmem(&key_a).unwrap(), false);
//*        assert_eq!(storage.dirmem(&key_ab).unwrap(), false);
//*        assert_eq!(storage.dirmem(&key_abc).unwrap(), false);
//*        storage.set(1, key_abc, vec![1u8, 2u8]).unwrap();
//*        assert_eq!(storage.dirmem(&key_a).unwrap(), true);
//*        assert_eq!(storage.dirmem(&key_ab).unwrap(), true);
//*        assert_eq!(storage.dirmem(&key_abc).unwrap(), false);
//*        storage.delete(2, key_abc).unwrap();
//*        assert_eq!(storage.dirmem(&key_a).unwrap(), false);
//*        assert_eq!(storage.dirmem(&key_ab).unwrap(), false);
//*        assert_eq!(storage.dirmem(&key_abc).unwrap(), false);
//*    }
//*
//*    fn test_copy(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(kv_store_factory.create("test_copy").unwrap());
//*
//*        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
//*        storage.set(1, key_abc, vec![1_u8]).unwrap();
//*        storage
//*            .copy(2, &vec!["a".to_string()], &vec!["z".to_string()])
//*            .unwrap();
//*
//*        assert_eq!(
//*            vec![1_u8],
//*            storage
//*                .get(&vec!["z".to_string(), "b".to_string(), "c".to_string()])
//*                .unwrap()
//*        );
//*        // TODO test copy over commits
//*    }
//*
//*    fn test_delete(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(kv_store_factory.create("test_delete").unwrap());
//*
//*        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
//*        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
//*        storage.set(1, key_abc, vec![2_u8]).unwrap();
//*        storage.set(2, key_abx, vec![3_u8]).unwrap();
//*        storage.delete(3, key_abx).unwrap();
//*        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*
//*        assert!(storage.get_history(&commit1, &key_abx).is_err());
//*    }
//*
//*    fn test_deleted_entry_available(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(
//*            kv_store_factory
//*                .create("test_deleted_entry_available")
//*                .unwrap(),
//*        );
//*
//*        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
//*        storage.set(1, key_abc, vec![2_u8]).unwrap();
//*        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*        storage.delete(2, key_abc).unwrap();
//*        let _commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*
//*        assert_eq!(vec![2_u8], storage.get_history(&commit1, &key_abc).unwrap());
//*    }
//*
//*    fn test_delete_in_separate_commit(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(
//*            kv_store_factory
//*                .create("test_delete_in_separate_commit")
//*                .unwrap(),
//*        );
//*
//*        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
//*        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
//*        storage.set(1, key_abc, vec![2_u8]).unwrap();
//*        storage.set(2, key_abx, vec![3_u8]).unwrap();
//*        storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*
//*        storage.delete(1, key_abx).unwrap();
//*        let commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*
//*        assert!(storage.get_history(&commit2, &key_abx).is_err());
//*    }
//*
//*    fn test_checkout(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
//*        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
//*
//*        let mut storage = WorkingTree::new(kv_store_factory.create("test_checkout").unwrap());
//*
//*        storage.set(1, key_abc, vec![1u8]).unwrap();
//*        storage.set(2, key_abx, vec![2u8]).unwrap();
//*        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*
//*        storage.set(1, key_abc, vec![3u8]).unwrap();
//*        storage.set(2, key_abx, vec![4u8]).unwrap();
//*        let commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
//*
//*        storage.checkout(&commit1).unwrap();
//*        assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8]);
//*        assert_eq!(storage.get(&key_abx).unwrap(), vec![2u8]);
//*        // this set be wiped by checkout
//*        storage.set(1, key_abc, vec![8u8]).unwrap();
//*
//*        storage.checkout(&commit2).unwrap();
//*        assert_eq!(storage.get(&key_abc).unwrap(), vec![3u8]);
//*        assert_eq!(storage.get(&key_abx).unwrap(), vec![4u8]);
//*    }
//*
//*    /// Test getting entire tree in string format for JSON RPC
//*    fn test_get_context_tree_by_prefix(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(
//*            kv_store_factory
//*                .create("test_get_context_tree_by_prefix")
//*                .unwrap(),
//*        );
//*
//*        let all_json = serde_json::json!(
//*            {
//*                "adata": {
//*                    "b": {
//*                            "x": {
//*                                    "y":"090a"
//*                            }
//*                    }
//*                },
//*                "data": {
//*                    "a": {
//*                            "x": {
//*                                    "y":"0506"
//*                            }
//*                    },
//*                    "b": {
//*                            "x": {
//*                                    "y":"0708"
//*                            }
//*                    },
//*                    "c":"0102"
//*                }
//*            }
//*        );
//*        let data_json = serde_json::json!(
//*            {
//*                "a": {
//*                        "x": {
//*                                "y":"0506"
//*                        }
//*                },
//*                "b": {
//*                        "x": {
//*                                "y":"0708"
//*                        }
//*                },
//*                "c":"0102"
//*            }
//*        );
//*
//*        let _commit = storage
//*            .commit(0, "Tezos".to_string(), "Genesis".to_string())
//*            .unwrap();
//*
//*        storage
//*            .set(
//*                1,
//*                &vec!["data".to_string(), "a".to_string(), "x".to_string()],
//*                vec![3, 4],
//*            )
//*            .unwrap();
//*        storage
//*            .set(2, &vec!["data".to_string(), "a".to_string()], vec![1, 2])
//*            .unwrap();
//*        storage
//*            .set(
//*                3,
//*                &vec![
//*                    "data".to_string(),
//*                    "a".to_string(),
//*                    "x".to_string(),
//*                    "y".to_string(),
//*                ],
//*                vec![5, 6],
//*            )
//*            .unwrap();
//*        storage
//*            .set(
//*                4,
//*                &vec![
//*                    "data".to_string(),
//*                    "b".to_string(),
//*                    "x".to_string(),
//*                    "y".to_string(),
//*                ],
//*                vec![7, 8],
//*            )
//*            .unwrap();
//*        storage
//*            .set(5, &vec!["data".to_string(), "c".to_string()], vec![1, 2])
//*            .unwrap();
//*        storage
//*            .set(
//*                6,
//*                &vec![
//*                    "adata".to_string(),
//*                    "b".to_string(),
//*                    "x".to_string(),
//*                    "y".to_string(),
//*                ],
//*                vec![9, 10],
//*            )
//*            .unwrap();
//*        //data-a[1,2]
//*        //data-a-x[3,4]
//*        //data-a-x-y[5,6]
//*        //data-b-x-y[7,8]
//*        //data-c[1,2]
//*        //adata-b-x-y[9,10]
//*        let commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string());
//*
//*        // without depth
//*        let rv_all = storage
//*            .get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec![], None)
//*            .unwrap();
//*        assert_json_eq!(all_json, serde_json::to_value(&rv_all).unwrap());
//*
//*        let rv_data = storage
//*            .get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec!["data".to_string()], None)
//*            .unwrap();
//*        assert_json_eq!(data_json, serde_json::to_value(&rv_data).unwrap());
//*
//*        // with depth 0
//*        assert_json_eq!(
//*            serde_json::json!(null),
//*            serde_json::to_value(
//*                storage
//*                    .get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec![], Some(0))
//*                    .unwrap()
//*            )
//*            .unwrap()
//*        );
//*
//*        // with depth 1
//*        assert_json_eq!(
//*            serde_json::json!(
//*                {
//*                    "adata": null,
//*                    "data": null
//*                }
//*            ),
//*            serde_json::to_value(
//*                storage
//*                    .get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec![], Some(1))
//*                    .unwrap()
//*            )
//*            .unwrap()
//*        );
//*        // with depth 2
//*        assert_json_eq!(
//*            serde_json::json!(
//*                {
//*                    "adata": {
//*                        "b" : null
//*                    },
//*                    "data": {
//*                        "a" : null,
//*                        "b" : null,
//*                        "c" : null,
//*                    },
//*                }
//*            ),
//*            serde_json::to_value(
//*                storage
//*                    .get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec![], Some(2))
//*                    .unwrap()
//*            )
//*            .unwrap()
//*        );
//*    }
//*
//*    fn test_backtracking_on_set(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage =
//*            WorkingTree::new(kv_store_factory.create("test_backtracking_on_set").unwrap());
//*
//*        let dummy_key = &vec!["a".to_string()];
//*        storage.set(1, dummy_key, vec![1u8]).unwrap();
//*        storage.set(2, dummy_key, vec![2u8]).unwrap();
//*
//*        // get recent value
//*        assert_eq!(storage.get(dummy_key).unwrap(), vec![2u8]);
//*
//*        // checkout previous stage state
//*        storage.stage_checkout(1).unwrap();
//*        assert_eq!(storage.get(dummy_key).unwrap(), vec![1u8]);
//*
//*        // checkout newest stage state
//*        storage.stage_checkout(2).unwrap();
//*        assert_eq!(storage.get(dummy_key).unwrap(), vec![2u8]);
//*    }
//*
//*    fn test_backtracking_on_delete(kv_store_factory: &TestContextKvStoreFactoryInstance) {
//*        let mut storage = WorkingTree::new(
//*            kv_store_factory
//*                .create("test_backtracking_on_delete")
//*                .unwrap(),
//*        );
//*
//*        let key = &vec!["a".to_string()];
//*        let value = vec![1u8];
//*        let empty_response: ContextValue = Vec::new();
//*
//*        storage.set(1, key, value.clone()).unwrap();
//*        storage.delete(2, key).unwrap();
//*
//*        assert_eq!(storage.get(key).unwrap(), empty_response);
//*
//*        // // checkout previous stage state
//*        storage.stage_checkout(1).unwrap();
//*        assert_eq!(storage.get(key).unwrap(), value);
//*
//*        // checkout latest stage state
//*        storage.stage_checkout(2).unwrap();
//*        assert_eq!(storage.get(key).unwrap(), empty_response);
//*    }
//*
//*    // Currently we don't perform a cleanup after each COMMIT
//*    // That will happen during the next CHECKOUT, this test is to ensure that
//*    fn test_checkout_stage_from_before_commit(
//*        kv_store_factory: &TestContextKvStoreFactoryInstance,
//*    ) {
//*        let mut storage = WorkingTree::new(
//*            kv_store_factory
//*                .create("test_checkout_stage_from_before_commit")
//*                .unwrap(),
//*        );
//*
//*        let key = &vec!["a".to_string()];
//*        storage.set(1, key, vec![1u8]).unwrap();
//*        storage.set(2, key, vec![2u8]).unwrap();
//*        storage
//*            .commit(0, "author".to_string(), "message".to_string())
//*            .unwrap();
//*
//*        assert_eq!(storage.staged.is_empty(), false);
//*        assert_eq!(storage.stage_checkout(1).is_err(), false);
//*    }
//*
//*    macro_rules! tests_with_storage {
//*        ($storage_tests_name:ident, $kv_store_factory:expr) => {
//*            mod $storage_tests_name {
//*                #[test]
//*                fn test_tree_hash() {
//*                    super::test_tree_hash($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_duplicate_entry_in_staging() {
//*                    super::test_duplicate_entry_in_staging($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_commit_hash() {
//*                    super::test_commit_hash($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_examples_from_article_about_storage() {
//*                    super::test_examples_from_article_about_storage($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_multiple_commit_hash() {
//*                    super::test_multiple_commit_hash($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_get() {
//*                    super::test_get($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_mem() {
//*                    super::test_mem($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_dirmem() {
//*                    super::test_dirmem($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_copy() {
//*                    super::test_copy($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_delete() {
//*                    super::test_delete($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_deleted_entry_available() {
//*                    super::test_deleted_entry_available($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_delete_in_separate_commit() {
//*                    super::test_delete_in_separate_commit($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_checkout() {
//*                    super::test_checkout($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_get_context_tree_by_prefix() {
//*                    super::test_get_context_tree_by_prefix($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_backtracking_on_set() {
//*                    super::test_backtracking_on_set($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_backtracking_on_delete() {
//*                    super::test_backtracking_on_delete($kv_store_factory)
//*                }
//*                #[test]
//*                fn test_fail_to_checkout_stage_from_before_commit() {
//*                    super::test_checkout_stage_from_before_commit($kv_store_factory)
//*                }
//*            }
//*        };
//*    }
//*
//*    lazy_static::lazy_static! {
//*        static ref SUPPORTED_KV_STORES: std::collections::HashMap<SupportedContextKeyValueStore, TestContextKvStoreFactoryInstance> = crate::context::kv_store::test_support::all_kv_stores(out_dir_path());
//*    }
//*
//*    fn out_dir_path() -> PathBuf {
//*        let out_dir = env::var("OUT_DIR").expect(
//*            "OUT_DIR is not defined - please add build.rs to root or set env variable OUT_DIR",
//*        );
//*        out_dir.as_str().into()
//*    }
//*
//*    macro_rules! tests_with_all_kv_stores {
//*        () => {
//*            tests_with_storage!(
//*                kv_store_inmemory_tests,
//*                super::SUPPORTED_KV_STORES
//*                    .get(&crate::context::kv_store::SupportedContextKeyValueStore::InMem)
//*                    .unwrap()
//*            );
//*            tests_with_storage!(
//*                kv_store_btree_tests,
//*                super::SUPPORTED_KV_STORES
//*                    .get(&crate::context::kv_store::SupportedContextKeyValueStore::BTreeMap)
//*                    .unwrap()
//*            );
//*            tests_with_storage!(
//*                kv_store_sled_tests,
//*                super::SUPPORTED_KV_STORES
//*                    .get(
//*                        &crate::context::kv_store::SupportedContextKeyValueStore::Sled {
//*                            path: super::out_dir_path()
//*                        }
//*                    )
//*                    .unwrap()
//*            );
//*        };
//*    }
//*
//*    tests_with_all_kv_stores!();
//*}
