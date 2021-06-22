// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This module contains an implementation of Irmin's context hashes.
//!
//! A document describing the algorithm can be found [here](https://github.com/tarides/tezos-context-hash).

use std::{array::TryFromSliceError, io};

use blake2::digest::{InvalidOutputSize, Update, VariableOutput};
use blake2::VarBlake2b;
use failure::Fail;

use ocaml::ocaml_hash_string;

use crate::{
    kv_store::HashId,
    persistent::DBError,
    working_tree::{
        storage::{Blob, BlobStorageId, NodeId, Storage},
        string_interner::StringId,
        Commit, Entry, NodeKind, Tree,
    },
    ContextKeyValueStore,
};

mod ocaml;

pub const ENTRY_HASH_LEN: usize = 32;

pub type EntryHash = [u8; ENTRY_HASH_LEN];

#[derive(Debug, Fail)]
pub enum HashingError {
    #[fail(display = "Failed to encode LEB128 value: {}", error)]
    Leb128EncodeFailure { error: io::Error },
    #[fail(display = "Invalid output size")]
    InvalidOutputSize,
    #[fail(display = "Failed to convert hash to array: {}", error)]
    ConversionError { error: TryFromSliceError },
    #[fail(display = "Expected value instead of `None` for {}", _0)]
    ValueExpected(&'static str),
    #[fail(display = "Got an unexpected empty inode")]
    UnexpectedEmptyInode,
    #[fail(display = "Invalid hash value, reason: {}", _0)]
    InvalidHash(String),
    #[fail(display = "Missing Entry")]
    MissingEntry,
    #[fail(display = "The Entry is borrowed more than once")]
    EntryBorrow,
    #[fail(display = "Database error error {:?}", error)]
    DBError { error: DBError },
    #[fail(display = "HashId not found: {:?}", hash_id)]
    HashIdNotFound { hash_id: HashId },
}

impl From<DBError> for HashingError {
    fn from(error: DBError) -> Self {
        HashingError::DBError { error }
    }
}

impl From<InvalidOutputSize> for HashingError {
    fn from(_error: InvalidOutputSize) -> Self {
        Self::InvalidOutputSize
    }
}

impl From<TryFromSliceError> for HashingError {
    fn from(error: TryFromSliceError) -> Self {
        HashingError::ConversionError { error }
    }
}

impl From<io::Error> for HashingError {
    fn from(error: io::Error) -> Self {
        Self::Leb128EncodeFailure { error }
    }
}

/// Inode representation used for hashing directories with >256 entries.
enum Inode {
    Empty,
    Value(Vec<(StringId, NodeId)>),
    Tree {
        depth: u32,
        children: usize,
        pointers: Vec<(u8, HashId)>,
    },
}

fn encode_irmin_node_kind(kind: &NodeKind) -> [u8; 8] {
    match kind {
        NodeKind::NonLeaf => [0, 0, 0, 0, 0, 0, 0, 0],
        NodeKind::Leaf => [255, 0, 0, 0, 0, 0, 0, 0],
    }
}

fn index(depth: u32, name: &str) -> u32 {
    ocaml_hash_string(depth, name.as_bytes()) % 32
}

// IMPORTANT: entries must be sorted in lexicographic order of the name
// Because we use `OrdMap`, this holds true when we iterate the items, but this is
// something to keep in mind if the representation of `Tree` changes.
fn partition_entries<'a>(
    depth: u32,
    entries: &[(StringId, NodeId)],
    store: &mut ContextKeyValueStore,
    storage: &Storage,
) -> Result<Inode, HashingError> {
    if entries.is_empty() {
        Ok(Inode::Empty)
    } else if entries.len() <= 32 {
        Ok(Inode::Value(entries.to_vec()))
    } else {
        let children = entries.len();
        let mut pointers = Vec::with_capacity(32);

        // pointers = {p(i) | i <- [0..31], t(i) != Empty}
        for i in 0..=31 {
            let entries_at_depth_and_index_i: Vec<(StringId, NodeId)> = entries
                .iter()
                .filter(|(name, _)| {
                    let name = storage.get_str(*name);
                    index(depth, name) == i
                })
                .cloned()
                .collect();
            let ti = partition_entries(depth + 1, &entries_at_depth_and_index_i, store, storage)?;

            match ti {
                Inode::Empty => (),
                non_empty => pointers.push((i as u8, hash_long_inode(&non_empty, store, storage)?)),
            }
        }

        Ok(Inode::Tree {
            depth,
            children,
            pointers,
        })
    }
}

fn hash_long_inode(
    inode: &Inode,
    store: &mut ContextKeyValueStore,
    storage: &Storage,
) -> Result<HashId, HashingError> {
    let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN)?;

    match inode {
        Inode::Empty => return Err(HashingError::UnexpectedEmptyInode),
        Inode::Value(entries) => {
            // Inode value:
            //
            // |   1   |   1  |     n_1      |  ...  |      n_k      |
            // +-------+------+--------------+-------+---------------+
            // | \000  |  \n  | prehash(e_1) |  ...  | prehash(e_k)  |
            //
            // where n_i = len(prehash(e_i))

            hasher.update(&[0u8]); // type tag
            hasher.update(&[entries.len() as u8]);

            // Inode value entry:
            //
            // |   (LEB128)  |  len(name)   |   1    |   32   |
            // +-------------+--------------+--------+--------+
            // | \len(name)  |     name     |  kind  |  hash  |

            for (name, node_id) in entries {
                let name = storage.get_str(*name);

                leb128::write::unsigned(&mut hasher, name.len() as u64)?;
                hasher.update(name.as_bytes());

                // \000 for nodes, and \001 for contents.
                let node = storage.get_node(*node_id).unwrap();
                match node.node_kind() {
                    NodeKind::Leaf => hasher.update(&[1u8]),
                    NodeKind::NonLeaf => hasher.update(&[0u8]),
                };
                hasher.update(node.entry_hash(store, storage)?.as_ref());
            }
        }
        Inode::Tree {
            depth,
            children,
            pointers,
        } => {
            // Inode tree:
            //
            // |   1    | (LEB128) |   (LEB128)    |    1   |  33  | ... |  33  |
            // +--------+----------+---------------+--------+------+-----+------+
            // |  \001  |  depth   | len(children) |   \k   | s_1  | ... | s_k  |

            hasher.update(&[1u8]); // type tag
            leb128::write::unsigned(&mut hasher, *depth as u64)?;
            leb128::write::unsigned(&mut hasher, *children as u64)?;
            hasher.update(&[pointers.len() as u8]);

            // Inode pointer:
            //
            // |    1    |   32   |
            // +---------+--------+
            // |  index  |  hash  |

            for (index, hash) in pointers {
                hasher.update(&[*index]);
                let hash = store
                    .get_hash(*hash)?
                    .ok_or_else(|| HashingError::HashIdNotFound { hash_id: *hash })?;
                hasher.update(hash.as_ref());
            }
        }
    }

    let hash_id = store
        .get_vacant_entry_hash()?
        .write_with(|entry| hasher.finalize_variable(|r| entry.copy_from_slice(r)));

    Ok(hash_id)
}

// hash is calculated as:
// <number of child nodes (8 bytes)><CHILD NODE>
// where:
// - CHILD NODE - <NODE TYPE><length of string (1 byte)><string/path bytes><length of hash (8bytes)><hash bytes>
// - NODE TYPE - leaf node(0xff0000000000000000) or internal node (0x0000000000000000)
fn hash_short_inode(
    tree: Tree,
    store: &mut ContextKeyValueStore,
    storage: &Storage,
) -> Result<HashId, HashingError> {
    let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN)?;

    // Node list:
    //
    // |    8   |     n_1      | ... |      n_k     |
    // +--------+--------------+-----+--------------+
    // |   \k   | prehash(e_1) | ... | prehash(e_k) |

    let tree = storage.get_tree(tree).unwrap();
    hasher.update(&(tree.len() as u64).to_be_bytes());

    // Node entry:
    //
    // |   8   |   (LEB128)   |  len(name)  |   8   |   32   |
    // +-------+--------------+-------------+-------+--------+
    // | kind  |  \len(name)  |    name     |  \32  |  hash  |

    for (k, v) in tree {
        let v = storage.get_node(*v).unwrap();
        hasher.update(encode_irmin_node_kind(&v.node_kind()));
        // Key length is written in LEB128 encoding

        let k = storage.get_str(*k);
        leb128::write::unsigned(&mut hasher, k.len() as u64)?;
        hasher.update(k.as_bytes());
        hasher.update(&(ENTRY_HASH_LEN as u64).to_be_bytes());

        let blob_inlined = v.get_entry().and_then(|entry| match entry {
            Entry::Blob(blob_id) if blob_id.is_inline() => storage.get_blob(blob_id),
            _ => None,
        });

        if let Some(blob) = blob_inlined {
            hasher.update(&hash_inlined_blob(blob)?);
        } else {
            hasher.update(v.entry_hash(store, storage)?.as_ref());
        }
    }

    let hash_id = store
        .get_vacant_entry_hash()?
        .write_with(|entry| hasher.finalize_variable(|r| entry.copy_from_slice(r)));

    Ok(hash_id)
}

// Calculates hash of tree
// uses BLAKE2 binary 256 length hash function
pub(crate) fn hash_tree(
    tree_id: Tree,
    store: &mut ContextKeyValueStore,
    storage: &Storage,
) -> Result<HashId, HashingError> {
    // If there are >256 entries, we need to partition the tree and hash the resulting inode
    let tree = storage.get_tree(tree_id).unwrap();

    if tree.len() > 256 {
        let inode = partition_entries(0, &tree, store, storage)?;
        hash_long_inode(&inode, store, storage)
    } else {
        hash_short_inode(tree_id, store, storage)
    }
}

// Calculates hash of BLOB
// uses BLAKE2 binary 256 length hash function
// hash is calculated as <length of data (8 bytes)><data>
pub(crate) fn hash_blob(
    blob_id: BlobStorageId,
    store: &mut ContextKeyValueStore,
    storage: &Storage,
) -> Result<Option<HashId>, HashingError> {
    if blob_id.is_inline() {
        return Ok(None);
    }

    let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN)?;

    let blob = storage.get_blob(blob_id).unwrap();
    hasher.update(&(blob.len() as u64).to_be_bytes());
    hasher.update(blob);

    let hash_id = store
        .get_vacant_entry_hash()?
        .write_with(|entry| hasher.finalize_variable(|r| entry.copy_from_slice(r)));

    Ok(Some(hash_id))
}

// Calculates hash of BLOB
// uses BLAKE2 binary 256 length hash function
// hash is calculated as <length of data (8 bytes)><data>
pub(crate) fn hash_inlined_blob(blob: Blob) -> Result<EntryHash, HashingError> {
    let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN)?;

    hasher.update(&(blob.len() as u64).to_be_bytes());
    hasher.update(blob);

    let mut entry_hash: EntryHash = Default::default();

    hasher.finalize_variable(|r| entry_hash.copy_from_slice(r));

    Ok(entry_hash)
}

// Calculates hash of commit
// uses BLAKE2 binary 256 length hash function
// hash is calculated as:
// <hash length (8 bytes)><tree hash bytes>
// <length of parent hash (8bytes)><parent hash bytes>
// <time in epoch format (8bytes)
// <commit author name length (8bytes)><commit author name bytes>
// <commit message length (8bytes)><commit message bytes>
pub(crate) fn hash_commit(
    commit: &Commit,
    store: &mut ContextKeyValueStore,
) -> Result<HashId, HashingError> {
    let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN)?;
    hasher.update(&(ENTRY_HASH_LEN as u64).to_be_bytes());

    let root_hash = store
        .get_hash(commit.root_hash)?
        .ok_or(HashingError::ValueExpected("root_hash"))?;
    hasher.update(root_hash.as_ref());

    if let Some(parent) = commit.parent_commit_hash {
        let parent_commit_hash = store
            .get_hash(parent)?
            .ok_or(HashingError::ValueExpected("parent_commit_hash"))?;
        hasher.update(&(1_u64).to_be_bytes()); // # of parents; we support only 1
        hasher.update(&(parent_commit_hash.len() as u64).to_be_bytes());
        hasher.update(&parent_commit_hash.as_ref());
    } else {
        hasher.update(&(0_u64).to_be_bytes());
    }

    hasher.update(&(commit.time as u64).to_be_bytes());
    hasher.update(&(commit.author.len() as u64).to_be_bytes());
    hasher.update(&commit.author.clone().into_bytes());
    hasher.update(&(commit.message.len() as u64).to_be_bytes());
    hasher.update(&commit.message.clone().into_bytes());

    let hash_id = store
        .get_vacant_entry_hash()?
        .write_with(|entry| hasher.finalize_variable(|r| entry.copy_from_slice(r)));

    Ok(hash_id)
}

pub(crate) fn hash_entry(
    entry: &Entry,
    store: &mut ContextKeyValueStore,
    storage: &Storage,
) -> Result<Option<HashId>, HashingError> {
    match entry {
        Entry::Commit(commit) => hash_commit(commit, store).map(Some),
        Entry::Tree(tree) => hash_tree(*tree, store, storage).map(Some),
        Entry::Blob(blob_id) => hash_blob(*blob_id, store, storage),
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod tests {
    use std::{convert::TryInto, env, fs::File, io::Read, path::Path};

    use flate2::read::GzDecoder;

    use crypto::hash::{ContextHash, HashTrait};

    use crate::{
        kv_store::in_memory::InMemory,
        working_tree::{Node, NodeKind, Tree},
    };

    use super::*;

    #[test]
    fn test_hash_of_commit() {
        let mut repo = InMemory::new();

        // Calculates hash of commit
        // uses BLAKE2 binary 256 length hash function
        // hash is calculated as:
        // <hash length (8 bytes)><tree hash bytes>
        // <length of parent hash (8bytes)><parent hash bytes>
        // <time in epoch format (8bytes)
        // <commit author name length (8bytes)><commit author name bytes>
        // <commit message length (8bytes)><commit message bytes>
        let expected_commit_hash =
            "e6de3fd37b1dc2b3c9d072ea67c2c5be1b55eeed9f5377b2bfc1228e6f9cb69b";

        let hash_id = repo.put_entry_hash(
            hex::decode("0d78b30e959c2a079e8ccb4ca19d428c95d29b2f02a35c1c58ef9c8972bc26aa")
                .unwrap()
                .try_into()
                .unwrap(),
        );

        let dummy_commit = Commit {
            parent_commit_hash: None,
            root_hash: hash_id,
            time: 0,
            author: "Tezedge".to_string(),
            message: "persist changes".to_string(),
        };

        // hexademical representation of above commit:
        //
        // hash length (8 bytes)           ->  00 00 00 00 00 00 00 20
        // tree hash bytes                 ->  0d78b30e959c2a079e8ccb4ca19d428c95d29b2f02a35c1c58ef9c8972bc26aa
        // parents count                   ->  00 00 00 00 00 00 00 00  (0)
        // commit time                     ->  00 00 00 00 00 00 00 00  (0)
        // commit author name length       ->  00 00 00 00 00 00 00 07  (7)
        // commit author name ('Tezedge')  ->  54 65 7a 65 64 67 65     (Tezedge)
        // commit message length           ->  00 00 00 00 00 00 00 0xf (15)

        let mut bytes = String::new();
        let hash_length = "0000000000000020"; // 32
        let tree_hash = "0d78b30e959c2a079e8ccb4ca19d428c95d29b2f02a35c1c58ef9c8972bc26aa"; // tree hash bytes
        let parents_count = "0000000000000000"; // 0
        let commit_time = "0000000000000000"; // 0
        let commit_author_name_length = "0000000000000007"; // 7
        let commit_author_name = "54657a65646765"; // 'Tezedge'
        let commit_message_length = "000000000000000f"; // 15
        let commit_message = "70657273697374206368616e676573"; // 'persist changes'

        println!("calculating hash of commit: \n\t{:?}\n", dummy_commit);

        println!("[hex] hash_length : {}", hash_length);
        println!("[hex] tree_hash : {}", tree_hash);
        println!("[hex] parents_count : {}", parents_count);
        println!("[hex] commit_time : {}", commit_time);
        println!(
            "[hex] commit_author_name_length : {}",
            commit_author_name_length
        );
        println!("[hex] commit_author_name : {}", commit_author_name);
        println!("[hex] commit_message_length : {}", commit_message_length);
        println!("[hex] commit_message : {}", commit_message);

        bytes += &hash_length;
        bytes += &tree_hash;
        bytes += &parents_count;
        bytes += &commit_time;
        bytes += &commit_author_name_length;
        bytes += &commit_author_name;
        bytes += &commit_message_length;
        bytes += &commit_message;

        println!(
            "manually calculated haxedemical representation of commit: {}",
            bytes
        );

        let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN).unwrap();
        hasher.update(hex::decode(bytes).unwrap());
        let calculated_commit_hash = hasher.finalize_boxed();

        println!(
            "calculated hash of the commit: {}",
            hex::encode(calculated_commit_hash.as_ref())
        );

        let hash_id = hash_commit(&dummy_commit, &mut repo).unwrap();

        assert_eq!(
            calculated_commit_hash.as_ref(),
            repo.get_hash(hash_id).unwrap().unwrap()
        );
        assert_eq!(
            expected_commit_hash,
            hex::encode(calculated_commit_hash.as_ref())
        );
    }

    #[test]
    fn test_hash_of_small_tree() {
        // Calculates hash of tree
        // uses BLAKE2 binary 256 length hash function
        // hash is calculated as:
        // <number of child nodes (8 bytes)><CHILD NODE>
        // where:
        // - CHILD NODE - <NODE TYPE><length of string (1 byte)><string/path bytes><length of hash (8bytes)><hash bytes>
        // - NODE TYPE - leaf node(0xff00000000000000) or internal node (0x0000000000000000)
        let mut repo = InMemory::new();
        let expected_tree_hash = "d49a53323107f2ae40b01eaa4e9bec4d02801daf60bab82dc2529e40d40fa917";
        let dummy_tree = Tree::empty();

        let mut storage = Storage::new();

        let blob_id = storage.add_blob_by_ref(&[1]);

        let node = Node::new(NodeKind::Leaf, Entry::Blob(blob_id));

        let dummy_tree = storage.insert(dummy_tree, "a", node);

        // hexademical representation of above tree:
        //
        // number of child nodes           ->  00 00 00 00 00 00 00 01  (1)
        // node type                       ->  ff 00 00 00 00 00 00 00  (leaf node)
        // length of string                ->  01                       (1)
        // string                          ->  61                       ('a')
        // length of hash                  ->  00 00 00 00 00 00 00 20  (32)
        // hash                            ->  407f958990678e2e9fb06758bc6520dae46d838d39948a4c51a5b19bd079293d

        let mut bytes = String::new();
        let child_nodes = "0000000000000001";
        let leaf_node = "ff00000000000000";
        let string_length = "01";
        let string_value = "61";
        let hash_length = "0000000000000020";
        let hash = "407f958990678e2e9fb06758bc6520dae46d838d39948a4c51a5b19bd079293d";

        println!("calculating hash of tree: \n\t{:?}\n", dummy_tree);
        println!("[hex] child nodes count: {}", child_nodes);
        println!("[hex] leaf_node        : {}", leaf_node);
        println!("[hex] string_length    : {}", string_length);
        println!("[hex] string_value     : {}", string_value);
        println!("[hex] hash_length      : {}", hash_length);
        println!("[hex] hash             : {}", hash);

        bytes += &child_nodes;
        bytes += &leaf_node;
        bytes += &string_length;
        bytes += &string_value;
        bytes += &hash_length;
        bytes += &hash;

        println!(
            "manually calculated haxedemical representation of tree: {}",
            bytes
        );

        let mut hasher = VarBlake2b::new(ENTRY_HASH_LEN).unwrap();
        hasher.update(hex::decode(bytes).unwrap());
        let calculated_tree_hash = hasher.finalize_boxed();

        println!(
            "calculated hash of the tree: {}",
            hex::encode(calculated_tree_hash.as_ref())
        );

        let hash_id = hash_tree(dummy_tree, &mut repo, &mut storage).unwrap();

        assert_eq!(
            calculated_tree_hash.as_ref(),
            repo.get_hash(hash_id).unwrap().unwrap()
        );
        assert_eq!(
            calculated_tree_hash.as_ref(),
            hex::decode(expected_tree_hash).unwrap()
        );
    }

    // Tests from Tarides json dataset

    #[derive(serde::Deserialize)]
    struct NodeHashTest {
        hash: String,
        bindings: Vec<NodeHashBinding>,
    }

    #[derive(serde::Deserialize)]
    struct NodeHashBinding {
        name: String,
        kind: String,
        hash: String,
    }

    #[test]
    fn test_node_hashes() {
        test_type_hashes("nodes.json.gz");
    }

    #[test]
    fn test_inode_hashes() {
        test_type_hashes("inodes.json.gz");
    }

    fn test_type_hashes(json_gz_file_name: &str) {
        let mut json_file = open_hashes_json_gz(json_gz_file_name);
        let mut bytes = Vec::new();

        let mut repo = InMemory::new();
        let mut storage = Storage::new();

        // NOTE: reading from a stream is very slow with serde, thats why
        // the whole file is being read here before parsing.
        // See: https://github.com/serde-rs/json/issues/160#issuecomment-253446892
        json_file.read_to_end(&mut bytes).unwrap();

        let test_cases: Vec<NodeHashTest> = serde_json::from_slice(&bytes).unwrap();

        for test_case in test_cases {
            let bindings_count = test_case.bindings.len();
            let mut tree = Tree::empty();

            for binding in test_case.bindings {
                let node_kind = match binding.kind.as_str() {
                    "Tree" => NodeKind::NonLeaf,
                    "Contents" => NodeKind::Leaf,
                    other => panic!("Got unexpected binding kind: {}", other),
                };
                let entry_hash = ContextHash::from_base58_check(&binding.hash).unwrap();

                let hash_id =
                    repo.put_entry_hash(entry_hash.as_ref().as_slice().try_into().unwrap());

                let node = Node::new_commited(node_kind, Some(hash_id), None);

                tree = storage.insert(tree, binding.name.as_str(), node);
            }

            let expected_hash = ContextHash::from_base58_check(&test_case.hash).unwrap();
            let computed_hash = hash_tree(tree, &mut repo, &mut storage).unwrap();
            let computed_hash = repo.get_hash(computed_hash).unwrap().unwrap();
            let computed_hash = ContextHash::try_from_bytes(computed_hash).unwrap();

            assert_eq!(
                expected_hash.to_base58_check(),
                computed_hash.to_base58_check(),
                "Expected hash {} but got {} (bindings: {})",
                expected_hash.to_base58_check(),
                computed_hash.to_base58_check(),
                bindings_count
            );
        }
    }

    fn open_hashes_json_gz(file_name: &str) -> GzDecoder<File> {
        let path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("tests")
            .join("resources")
            .join(file_name);
        let file = File::open(path)
            .unwrap_or_else(|_| panic!("Couldn't open file: tests/resources/{}", file_name));
        GzDecoder::new(file)
    }
}
