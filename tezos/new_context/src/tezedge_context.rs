// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{
    cell::RefCell,
    convert::TryInto,
    sync::{Arc, RwLock},
};
use std::{convert::TryFrom, rc::Rc};

use crypto::hash::ContextHash;
use ocaml_interop::BoxRoot;

use crate::{
    hash::EntryHash,
    kv_store::{in_memory::RepositoryMemoryUsage, HashId},
    persistent::DBError,
    working_tree::{
        serializer::deserialize,
        storage::{BlobStorageId, NodeId, Storage, StorageMemoryUsage},
        working_tree::MerkleError,
        working_tree_stats::MerkleStoragePerfReport,
        Commit, Entry, Tree,
    },
    ContextKeyValueStore, StringTreeMap,
};
use crate::{working_tree::working_tree::WorkingTree, IndexApi};
use crate::{
    working_tree::working_tree::{FoldDepth, TreeWalker},
    ContextKeyOwned,
};
use crate::{
    ContextError, ContextKey, ContextValue, ProtocolContextApi, ShellContextApi, StringTreeEntry,
    TreeId,
};

// Represents the patch_context function passed from the OCaml side
// It is opaque to rust, we don't care about it's actual type
// because it is not used on Rust, but we need a type to represent it.
pub struct PatchContextFunction {}

#[derive(Clone)]
pub struct TezedgeIndex {
    pub repository: Arc<RwLock<ContextKeyValueStore>>,
    pub patch_context: Rc<Option<BoxRoot<PatchContextFunction>>>,
    pub storage: Rc<RefCell<Storage>>,
}

// TODO: some of the utility methods here (and in `WorkingTree`) should probably be
// standalone functions defined in a separate module that take the `index`
// as an argument.
// Also revise all errors defined, some may be obsolete now.
impl TezedgeIndex {
    pub fn new(
        repository: Arc<RwLock<ContextKeyValueStore>>,
        patch_context: Option<BoxRoot<PatchContextFunction>>,
    ) -> Self {
        let patch_context = Rc::new(patch_context);
        Self {
            patch_context,
            repository,
            storage: Default::default(),
        }
    }

    pub fn find_entry_bytes(&self, hash: HashId) -> Result<Option<Vec<u8>>, DBError> {
        let repo = self.repository.read()?;
        Ok(repo.get_value(hash)?.map(|v| v.to_vec()))
    }

    pub fn find_entry(
        &self,
        hash: HashId,
        storage: &mut Storage,
    ) -> Result<Option<Entry>, DBError> {
        match self.find_entry_bytes(hash)? {
            None => Ok(None),
            Some(entry_bytes) => Ok(Some(deserialize(entry_bytes.as_ref(), storage)?)),
        }
    }

    pub fn get_hash(&self, hash_id: HashId) -> Result<Option<EntryHash>, DBError> {
        Ok(self
            .repository
            .read()?
            .get_hash(hash_id)?
            .map(|h| h.into_owned()))
    }

    pub fn get_entry(&self, hash: HashId, storage: &mut Storage) -> Result<Entry, MerkleError> {
        match self.find_entry(hash, storage)? {
            None => Err(MerkleError::EntryNotFound { hash_id: hash }),
            Some(entry) => Ok(entry),
        }
    }

    pub fn find_commit(
        &self,
        hash: HashId,
        storage: &mut Storage,
    ) -> Result<Option<Commit>, DBError> {
        match self.find_entry(hash, storage)? {
            Some(Entry::Commit(commit)) => Ok(Some(*commit)),
            Some(Entry::Tree(_)) => Err(DBError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "tree".to_string(),
            }),
            Some(Entry::Blob(_)) => Err(DBError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "blob".to_string(),
            }),
            None => Ok(None),
        }
    }

    pub fn get_commit(&self, hash: HashId, storage: &mut Storage) -> Result<Commit, MerkleError> {
        match self.find_commit(hash, storage)? {
            None => Err(MerkleError::EntryNotFound { hash_id: hash }),
            Some(entry) => Ok(entry),
        }
    }

    pub fn find_tree(&self, hash: HashId, storage: &mut Storage) -> Result<Option<Tree>, DBError> {
        match self.find_entry(hash, storage)? {
            Some(Entry::Tree(tree)) => Ok(Some(tree)),
            Some(Entry::Blob(_)) => Err(DBError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "blob".to_string(),
            }),
            Some(Entry::Commit { .. }) => Err(DBError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            }),
            None => Ok(None),
        }
    }

    pub fn get_tree(&self, hash: HashId, storage: &mut Storage) -> Result<Tree, MerkleError> {
        match self.find_tree(hash, storage)? {
            None => Err(MerkleError::EntryNotFound { hash_id: hash }),
            Some(entry) => Ok(entry),
        }
    }

    pub fn contains(&self, hash: HashId) -> Result<bool, DBError> {
        let db = self.repository.read()?;
        Ok(db.contains(hash)?)
    }

    pub fn get_context_hash_id(
        &self,
        context_hash: &ContextHash,
    ) -> Result<Option<HashId>, MerkleError> {
        let db = self.repository.read()?;
        Ok(db.get_context_hash(context_hash)?)
    }

    /// Convert key in array form to string form
    pub fn key_to_string(&self, key: &ContextKey) -> String {
        key.join("/")
    }

    /// Convert key in string form to array form
    pub fn string_to_key(&self, string: &str) -> ContextKeyOwned {
        string.split('/').map(str::to_string).collect()
    }

    pub fn node_entry(&self, node_id: NodeId, storage: &mut Storage) -> Result<Entry, MerkleError> {
        let node = storage.get_node(node_id).ok_or(MerkleError::NodeNotFound)?;

        if let Some(e) = node.get_entry() {
            return Ok(e);
        };
        let hash = node.get_hash_id()?;
        std::mem::drop(node);

        let entry = self.get_entry(hash, storage)?;
        let node = storage.get_node(node_id).ok_or(MerkleError::NodeNotFound)?;
        node.set_entry(&entry)?;

        Ok(entry)
    }

    /// Get context tree under given prefix in string form (for JSON)
    /// depth - None returns full tree
    pub fn _get_context_tree_by_prefix(
        &self,
        context_hash: HashId,
        prefix: &ContextKey,
        depth: Option<usize>,
        storage: &mut Storage,
    ) -> Result<StringTreeEntry, MerkleError> {
        if let Some(0) = depth {
            return Ok(StringTreeEntry::Null);
        }

        let mut out = StringTreeMap::new();
        let commit = self.get_commit(context_hash, storage)?;

        let root_tree = self.get_tree(commit.root_hash, storage)?;
        let prefixed_tree = self.find_raw_tree(root_tree, prefix, storage)?;
        let delimiter = if prefix.is_empty() { "" } else { "/" };

        let prefixed_tree = storage
            .get_tree(prefixed_tree)
            .ok_or(MerkleError::TreeNotFound)?
            .to_vec();

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.node_entry(*child_node, storage)?;

            let key = storage.get_str(*key)?;

            // construct full path as Tree key is only one chunk of it
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            let rdepth = depth.map(|d| d - 1);
            let key_str = key.to_string();

            std::mem::drop(key);

            out.insert(
                key_str,
                self.get_context_recursive(&fullpath, &entry, rdepth, storage)?,
            );
        }

        //stat_updater.update_execution_stats(&mut self.stats);
        Ok(StringTreeEntry::Tree(out))
    }

    /// Go recursively down the tree from Entry, build string tree and return it
    /// (or return hex value if Blob)
    fn get_context_recursive(
        &self,
        path: &str,
        entry: &Entry,
        depth: Option<usize>,
        storage: &mut Storage,
    ) -> Result<StringTreeEntry, MerkleError> {
        if let Some(0) = depth {
            return Ok(StringTreeEntry::Null);
        }

        match entry {
            Entry::Blob(blob_id) => {
                let blob = storage
                    .get_blob(*blob_id)
                    .ok_or(MerkleError::BlobNotFound)?;
                Ok(StringTreeEntry::Blob(hex::encode(blob)))
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                let mut new_tree = StringTreeMap::new();

                let tree = storage
                    .get_tree(*tree)
                    .ok_or(MerkleError::TreeNotFound)?
                    .to_vec();

                for (key, child_node) in tree.iter() {
                    let key = storage.get_str(*key)?;
                    let fullpath = path.to_owned() + "/" + key;
                    let key_str = key.to_string();
                    std::mem::drop(key);

                    let entry = self.node_entry(*child_node, storage)?;
                    let rdepth = depth.map(|d| d - 1);

                    new_tree.insert(
                        key_str,
                        self.get_context_recursive(&fullpath, &entry, rdepth, storage)?,
                    );
                }
                Ok(StringTreeEntry::Tree(new_tree))
            }
            Entry::Commit(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "Tree/Blob".to_string(),
                found: "Commit".to_string(),
            }),
        }
    }

    /// Find tree by path and return a copy. Return an empty tree if no tree under this path exists or if a blob
    /// (= value) is encountered along the way.
    ///
    /// # Arguments
    ///
    /// * `root` - reference to a tree in which we search
    /// * `key` - sought path
    pub fn find_raw_tree(
        &self,
        root: Tree,
        key: &[&str],
        storage: &mut Storage,
    ) -> Result<Tree, MerkleError> {
        let first = match key.first() {
            Some(first) => *first,
            None => {
                // terminate recursion if end of path was reached
                return Ok(root.clone());
            }
        };

        // first get node at key
        let child_node_id = match storage.get_tree_node_id(root, first) {
            Some(hash) => hash,
            None => {
                return Ok(Tree::empty());
            }
        };

        // get entry (from working tree)
        let child_node = storage
            .get_node(child_node_id)
            .ok_or(MerkleError::NodeNotFound)?;
        if let Some(entry) = child_node.get_entry() {
            match entry {
                Entry::Tree(tree) => {
                    return self.find_raw_tree(tree, &key[1..], storage);
                }
                Entry::Blob(_) => return Ok(Tree::empty()),
                Entry::Commit { .. } => {
                    return Err(MerkleError::FoundUnexpectedStructure {
                        sought: "Tree/Blob".to_string(),
                        found: "commit".to_string(),
                    })
                }
            }
        }

        // get entry by hash (from DB)
        let hash = child_node.get_hash_id()?;
        std::mem::drop(child_node);

        let entry = self.get_entry(hash, storage)?;
        let child_node = storage
            .get_node(child_node_id)
            .ok_or(MerkleError::NodeNotFound)?;
        child_node.set_entry(&entry)?;

        match entry {
            Entry::Tree(tree) => self.find_raw_tree(tree, &key[1..], storage),
            Entry::Blob(_) => Ok(Tree::empty()),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "Tree/Blob".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    /// Get value from historical context identified by commit hash.
    pub fn get_history(
        &self,
        commit_hash: HashId,
        key: &ContextKey,
    ) -> Result<ContextValue, MerkleError> {
        let mut storage = (&*self.storage).borrow_mut();

        let commit = self.get_commit(commit_hash, &mut storage)?;
        let tree = self.get_tree(commit.root_hash, &mut storage)?;

        let blob_id = self.get_from_tree(tree, key, &mut storage)?;
        let blob = storage.get_blob(blob_id).ok_or(MerkleError::BlobNotFound)?;

        Ok(blob.to_vec())
    }

    fn get_from_tree(
        &self,
        root: Tree,
        key: &ContextKey,
        storage: &mut Storage,
    ) -> Result<BlobStorageId, MerkleError> {
        let (file, path) = key.split_last().ok_or(MerkleError::KeyEmpty)?;

        let node = self.find_raw_tree(root, &path, storage)?;

        // get file node from tree
        let node_id =
            storage
                .get_tree_node_id(node, *file)
                .ok_or_else(|| MerkleError::ValueNotFound {
                    key: self.key_to_string(key),
                })?;

        // get blob
        match self.node_entry(node_id, storage)? {
            Entry::Blob(blob) => Ok(blob),
            _ => Err(MerkleError::ValueIsNotABlob {
                key: self.key_to_string(key),
            }),
        }
    }

    /// Construct Vec of all context key-values under given prefix
    pub fn get_context_key_values_by_prefix(
        &self,
        context_hash: HashId,
        prefix: &ContextKey,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, MerkleError> {
        let mut storage = (&*self.storage).borrow_mut();

        let commit = self.get_commit(context_hash, &mut storage)?;
        let root_tree = self.get_tree(commit.root_hash, &mut storage)?;
        let rv = self._get_context_key_values_by_prefix(root_tree, prefix, &mut storage);

        rv
    }

    fn _get_context_key_values_by_prefix(
        &self,
        root_tree: Tree,
        prefix: &ContextKey,
        storage: &mut Storage,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, MerkleError> {
        let prefixed_tree = self.find_raw_tree(root_tree, prefix, storage)?;
        let mut keyvalues: Vec<(ContextKeyOwned, ContextValue)> = Vec::new();
        let delimiter = if prefix.is_empty() { "" } else { "/" };

        let prefixed_tree = storage
            .get_tree(prefixed_tree)
            .ok_or(MerkleError::TreeNotFound)?
            .to_vec();

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.node_entry(*child_node, storage)?;

            let key = storage.get_str(*key)?;
            // construct full path as Tree key is only one chunk of it
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            std::mem::drop(key);

            self.get_key_values_from_tree_recursively(&fullpath, &entry, &mut keyvalues, storage)?;
        }

        if keyvalues.is_empty() {
            Ok(None)
        } else {
            Ok(Some(keyvalues))
        }
    }

    // TODO: can we get rid of the recursion?
    fn get_key_values_from_tree_recursively(
        &self,
        path: &str,
        entry: &Entry,
        entries: &mut Vec<(ContextKeyOwned, ContextValue)>,
        storage: &mut Storage,
    ) -> Result<(), MerkleError> {
        match entry {
            Entry::Blob(blob_id) => {
                // push key-value pair
                let blob = storage
                    .get_blob(*blob_id)
                    .ok_or(MerkleError::BlobNotFound)?;
                entries.push((self.string_to_key(path), blob.to_vec()));
                Ok(())
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                // let storage = (&*self.trees).borrow();
                let tree = storage
                    .get_tree(*tree)
                    .ok_or(MerkleError::TreeNotFound)?
                    .to_vec();

                tree.iter()
                    .map(|(key, child_node)| {
                        let key = storage.get_str(*key)?;
                        let fullpath = path.to_owned() + "/" + key;
                        std::mem::drop(key);

                        match self.node_entry(*child_node, storage) {
                            Err(_) => Ok(()),
                            Ok(entry) => self.get_key_values_from_tree_recursively(
                                &fullpath, &entry, entries, storage,
                            ),
                        }
                    })
                    .find_map(|res| match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    })
                    .unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => match self.get_entry(commit.root_hash, storage) {
                Err(err) => Err(err),
                Ok(entry) => {
                    self.get_key_values_from_tree_recursively(path, &entry, entries, storage)
                }
            },
        }
    }
}

impl IndexApi<TezedgeContext> for TezedgeIndex {
    fn exists(&self, context_hash: &ContextHash) -> Result<bool, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;

            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => return Ok(false),
            }
        };

        let mut storage = self.storage.borrow_mut();

        if let Some(Entry::Commit(_)) = self.find_entry(hash_id, &mut storage)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn checkout(&self, context_hash: &ContextHash) -> Result<Option<TezedgeContext>, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;

            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => return Ok(None),
            }
        };

        let mut storage = self.storage.borrow_mut();
        storage.clear();

        if let Some(commit) = self.find_commit(hash_id, &mut storage)? {
            if let Some(tree) = self.find_tree(commit.root_hash, &mut storage)? {
                let tree = WorkingTree::new_with_tree(self.clone(), tree);

                Ok(Some(TezedgeContext::new(
                    self.clone(),
                    Some(hash_id),
                    Some(Rc::new(tree)),
                )))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn block_applied(&self, referenced_older_entries: Vec<HashId>) -> Result<(), ContextError> {
        Ok(self
            .repository
            .write()?
            .block_applied(referenced_older_entries)?)
    }

    fn cycle_started(&mut self) -> Result<(), ContextError> {
        Ok(self.repository.write()?.new_cycle_started()?)
    }

    fn get_key_from_history(
        &self,
        context_hash: &ContextHash,
        key: &ContextKey,
    ) -> Result<Option<ContextValue>, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;

            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => {
                    return Err(ContextError::UnknownContextHashError {
                        context_hash: context_hash.to_base58_check(),
                    })
                }
            }
        };

        match self.get_history(hash_id, key) {
            Err(MerkleError::ValueNotFound { key: _ }) => Ok(None),
            Err(MerkleError::EntryNotFound { hash_id: _ }) => Ok(None),
            Err(err) => Err(ContextError::MerkleStorageError { error: err }),
            Ok(val) => Ok(Some(val)),
        }
    }

    fn get_key_values_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: &ContextKey,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;
            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => {
                    return Err(ContextError::UnknownContextHashError {
                        context_hash: context_hash.to_base58_check(),
                    })
                }
            }
        };

        let context = match self.checkout(context_hash)? {
            Some(context) => context,
            None => return Ok(None),
        };

        let repository = self.repository.read()?;
        context
            .tree
            .get_key_values_by_prefix(hash_id, prefix, &*repository)
            .map_err(ContextError::from)
    }

    fn get_context_tree_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: &ContextKey,
        depth: Option<usize>,
    ) -> Result<StringTreeEntry, ContextError> {
        let hash_id = {
            let repository = self.repository.read()?;
            match repository.get_context_hash(context_hash)? {
                Some(hash_id) => hash_id,
                None => {
                    return Err(ContextError::UnknownContextHashError {
                        context_hash: context_hash.to_base58_check(),
                    })
                }
            }
        };

        let mut storage = self.storage.borrow_mut();

        self._get_context_tree_by_prefix(hash_id, prefix, depth, &mut storage)
            .map_err(ContextError::from)
    }
}

// context implementation using merkle-tree-like storage
#[derive(Clone)]
pub struct TezedgeContext {
    pub index: TezedgeIndex,
    pub parent_commit_hash: Option<HashId>,
    pub tree_id: TreeId,
    tree_id_generator: Rc<RefCell<TreeIdGenerator>>,
    pub tree: Rc<WorkingTree>,
}

impl ProtocolContextApi for TezedgeContext {
    fn add(&self, key: &ContextKey, value: &[u8]) -> Result<Self, ContextError> {
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
    ) -> Result<Vec<(String, WorkingTree)>, ContextError> {
        self.tree.list(offset, length, key).map_err(Into::into)
    }

    fn fold_iter(
        &self,
        depth: Option<FoldDepth>,
        key: &ContextKey,
    ) -> Result<TreeWalker, ContextError> {
        Ok(self.tree.fold_iter(depth, key)?)
    }

    fn get_merkle_root(&self) -> Result<EntryHash, ContextError> {
        self.tree.hash().map_err(Into::into)
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
        let mut repository = self.index.repository.write()?;
        let (commit_hash_id, batch, referenced_older_entries) = self.tree.prepare_commit(
            date,
            author,
            message,
            self.parent_commit_hash,
            &mut *repository,
            true,
        )?;
        // FIXME: only write entries if there are any, empty commits should not produce anything
        repository.write_batch(batch)?;
        repository.put_context_hash(commit_hash_id)?;
        repository.block_applied(referenced_older_entries)?;

        let commit_hash = self.get_commit_hash(commit_hash_id, &*repository)?;
        repository.clear_entries()?;

        std::mem::drop(repository);
        // let time = self.index.time_clear.load(std::sync::atomic::Ordering::Relaxed);
        // println!("TIME_CLEAR={:?}", std::time::Duration::from_nanos(time.try_into().unwrap()));
        self.get_memory_usage();

        // println!("COMMIT={:?}", commit_hash.to_base58_check());

        Ok(commit_hash)
    }

    fn hash(
        &self,
        author: String,
        message: String,
        date: i64,
    ) -> Result<ContextHash, ContextError> {
        let date: u64 = date.try_into()?;
        let mut repository = self.index.repository.write()?;

        let (commit_hash_id, _, _) = self.tree.prepare_commit(
            date,
            author,
            message,
            self.parent_commit_hash,
            &mut *repository,
            false,
        )?;

        let commit_hash = self.get_commit_hash(commit_hash_id, &*repository)?;
        repository.clear_entries()?;
        Ok(commit_hash)
    }

    fn get_last_commit_hash(&self) -> Result<Option<Vec<u8>>, ContextError> {
        let repository = self.index.repository.read()?;

        let value = match self.parent_commit_hash {
            Some(hash_id) => repository.get_value(hash_id)?,
            None => return Ok(None),
        };

        Ok(value.map(|v| v.to_vec()))
    }

    fn get_merkle_stats(&self) -> Result<MerkleStoragePerfReport, ContextError> {
        Ok(MerkleStoragePerfReport::default())
    }

    fn get_memory_usage(&self) -> Result<ContextMemoryUsage, ContextError> {
        let repository = self.index.repository.read()?;
        let storage = (&*self.index.storage).borrow();

        let usage = ContextMemoryUsage {
            repo: repository.memory_usage(),
            storage: storage.memory_usage(),
        };

        println!("usage={:?}", usage);

        Ok(usage)

        // println!("REPO={:?}", repository);
        //println!("STORAGE={:?}", );

        // todo!()

        // Ok(ContextMemoryUsage {
        //     storage: repository.memory_usage(),
        //     strings: strings.memory_usage(),
        // })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ContextMemoryUsage {
    repo: RepositoryMemoryUsage,
    storage: StorageMemoryUsage,
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
        parent_commit_hash: Option<HashId>,
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

    fn get_commit_hash(
        &self,
        commit_hash_id: HashId,
        repo: &ContextKeyValueStore,
    ) -> Result<ContextHash, ContextError> {
        let commit_hash = match repo.get_hash(commit_hash_id)? {
            Some(hash) => hash,
            None => {
                return Err(MerkleError::EntryNotFound {
                    hash_id: commit_hash_id,
                }
                .into())
            }
        };
        let commit_hash = ContextHash::try_from(&commit_hash[..])?;
        Ok(commit_hash)
    }
}

#[cfg(test)]
mod tests {
    use tezos_api::ffi::{ContextKvStoreConfiguration, TezosContextTezEdgeStorageConfiguration};

    use super::*;
    use crate::initializer::initialize_tezedge_context;

    #[test]
    fn init_context() {
        let context = initialize_tezedge_context(&TezosContextTezEdgeStorageConfiguration {
            backend: ContextKvStoreConfiguration::InMem,
            ipc_socket_path: None,
        })
        .unwrap();

        // Context is immutable so on any modification, the methods return the new tree
        let context = context.add(&["a", "b", "c"], &[1, 2, 3]).unwrap();
        let context = context.add(&["m", "n", "o"], &[4, 5, 6]).unwrap();
        assert_eq!(context.find(&["a", "b", "c"]).unwrap().unwrap(), &[1, 2, 3]);

        let context2 = context.delete(&["m", "n", "o"]).unwrap();
        assert!(context.mem(&["m", "n", "o"]).unwrap());
        assert!(context2.mem(&["m", "n", "o"]).unwrap() == false);

        assert!(context.mem_tree(&["a"]));

        let tree_a = context.find_tree(&["a"]).unwrap().unwrap();
        let context = context.add_tree(&["z"], &tree_a).unwrap();

        assert_eq!(
            context.find(&["a", "b", "c"]).unwrap().unwrap(),
            context.find(&["z", "b", "c"]).unwrap().unwrap(),
        );

        let context = context.add(&["a", "b1", "c"], &[10, 20, 30]).unwrap();
        let list = context.list(None, None, &["a"]).unwrap();

        assert_eq!(&*list[0].0, "b");
        assert_eq!(&*list[1].0, "b1");

        assert_eq!(
            context.get_merkle_root().unwrap(),
            [
                1, 217, 94, 141, 166, 51, 65, 3, 104, 220, 208, 35, 122, 106, 131, 147, 183, 133,
                81, 239, 195, 111, 25, 29, 88, 1, 46, 251, 25, 205, 202, 229
            ]
        );
    }
}
