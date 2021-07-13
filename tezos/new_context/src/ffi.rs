// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

#![allow(clippy::type_complexity)]

//! Functions exposed to be called from OCaml

use core::borrow::Borrow;
use lazy_static::lazy_static;
use std::{
    cell::RefCell,
    convert::TryFrom,
    rc::Rc,
    sync::{Arc, RwLock},
};

use ocaml_interop::*;

use crypto::hash::ContextHash;

use crate::{
    initializer::initialize_tezedge_index,
    timings,
    working_tree::{
        working_tree::{FoldDepth, TreeWalker, WorkingTree},
        NodeKind, Tree,
    },
    ContextKeyValueStore, IndexApi, PatchContextFunction, ProtocolContextApi, ShellContextApi,
    TezedgeContext, TezedgeIndex,
};
use tezos_api::ffi::TezosContextTezEdgeStorageConfiguration;
use tezos_api::ocaml_conv::{OCamlBlockHash, OCamlContextHash, OCamlOperationHash};

// TODO: instead of converting errors into strings, it may be useful to pass
// them around using custom pointers so that they can be recovered later.
// OCaml code will not do anything with the errors, just raise an exception,
// but once we catch it on Rust, having the original error value may be useful.

// TODO: see if some of the cloning can be avoided.

enum TreeKind {
    Tree,
    Value,
}

impl_to_ocaml_polymorphic_variant! {
    TreeKind {
        TreeKind::Tree,
        TreeKind::Value
    }
}

type WorkingTreeFFI = WorkingTree;
#[derive(Clone)]
struct TreeWalkerFFI(Rc<RefCell<TreeWalker>>);
#[derive(Clone)]
struct TezedgeIndexFFI(RefCell<TezedgeIndex>);
#[derive(Clone)]
struct TezedgeContextFFI(RefCell<TezedgeContext>);

impl TreeWalkerFFI {
    fn new(walker: TreeWalker) -> Self {
        Self(Rc::new(RefCell::new(walker)))
    }
}

impl From<TreeWalker> for TreeWalkerFFI {
    fn from(walker: TreeWalker) -> Self {
        Self::new(walker)
    }
}

impl TezedgeIndexFFI {
    fn new(index: TezedgeIndex) -> Self {
        Self(RefCell::new(index))
    }
}

impl From<TezedgeIndex> for TezedgeIndexFFI {
    fn from(index: TezedgeIndex) -> Self {
        Self::new(index)
    }
}

impl TezedgeContextFFI {
    fn new(index: TezedgeContext) -> Self {
        Self(RefCell::new(index))
    }
}

impl From<TezedgeContext> for TezedgeContextFFI {
    fn from(index: TezedgeContext) -> Self {
        Self::new(index)
    }
}

fn make_key<'a>(rt: &'a OCamlRuntime, key: OCamlRef<OCamlList<String>>) -> Vec<&'a str> {
    let mut key = rt.get(key);
    let mut vector: Vec<&str> = Vec::with_capacity(128);

    while let Some((head, tail)) = key.uncons() {
        vector.push(unsafe { head.as_str_unchecked() });
        key = tail;
    }

    vector
}

// TODO: move this static and its accessors to a better place
lazy_static! {
    pub static ref TEZEDGE_CONTEXT_REPOSITORY: RwLock<Option<Arc<RwLock<ContextKeyValueStore>>>> =
        RwLock::new(None);
}

#[derive(Debug)]
pub enum TezedgeIndexError {
    LockPoisonError,
}

// Set the repository so that it can be accessed by the context IPC
fn set_context_index(index: &TezedgeIndex) -> Result<(), TezedgeIndexError> {
    TEZEDGE_CONTEXT_REPOSITORY
        .write()
        .map_err(|_| TezedgeIndexError::LockPoisonError)?
        .replace(Arc::clone(&index.repository));
    Ok(())
}

// Obtain the context index (for context IPC access)
pub fn get_context_index() -> Result<Option<TezedgeIndex>, TezedgeIndexError> {
    Ok(TEZEDGE_CONTEXT_REPOSITORY
        .read()
        .map_err(|_| TezedgeIndexError::LockPoisonError)?
        .as_ref()
        .map(|repository| TezedgeIndex::new(Arc::clone(repository), None)))
}

ocaml_export! {
    // Index API

    fn tezedge_index_init(
        rt,
        configuration: OCamlRef<TezosContextTezEdgeStorageConfiguration>,
        patch_context: OCamlRef<Option<PatchContextFunction>>,
    ) -> OCaml<Result<DynBox<TezedgeIndexFFI>, String>> {
        let patch_context = rt.get(patch_context).to_option().map(BoxRoot::new);
        let configuration: TezosContextTezEdgeStorageConfiguration = configuration.to_rust(rt);
        let result = initialize_tezedge_index(&configuration, patch_context)
            .map_err(|err| format!("{:?}", err));

        if let Ok(index) = &result {
            if let Err(err) = set_context_index(index) {
                Err::<BoxRoot<DynBox<TezedgeIndexFFI>>, String>(format!("{:?}", err)).to_ocaml(rt)
            } else {
                result.map(|index| OCaml::box_value(rt, index.into()).root()).to_ocaml(rt)
            }
        } else {
            result.map(|index| OCaml::box_value(rt, index.into()).root()).to_ocaml(rt)
        }
    }

    fn tezedge_index_close(
        rt,
        _index: OCamlRef<DynBox<TezedgeIndexFFI>>,
    ) {
        OCaml::unit()
    }

    fn tezedge_index_patch_context_get(
        rt,
        index: OCamlRef<DynBox<TezedgeIndexFFI>>,
    ) -> OCaml<Option<PatchContextFunction>> {
        let ocaml_index = rt.get(index);
        let index: &TezedgeIndexFFI = ocaml_index.borrow();
        let index = index.0.borrow().clone();
        index.patch_context.to_ocaml(rt)
    }

    // OCaml = val exists : index -> Context_hash.t -> bool Lwt.t
    fn tezedge_index_exists(
        rt,
        index: OCamlRef<DynBox<TezedgeIndexFFI>>,
        context_hash: OCamlRef<OCamlContextHash>,
    ) -> OCaml<Result<bool, String>> {
        let ocaml_index = rt.get(index);
        let index: &TezedgeIndexFFI = ocaml_index.borrow();
        let index = index.0.borrow().clone();
        let context_hash: ContextHash = context_hash.to_rust(rt);

        let result = index.exists(&context_hash)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val checkout : index -> Context_hash.t -> context option Lwt.t
    fn tezedge_index_checkout(
        rt,
        index: OCamlRef<DynBox<TezedgeIndexFFI>>,
        context_hash: OCamlRef<OCamlContextHash>,
    ) -> OCaml<Result<Option<DynBox<TezedgeContextFFI>>, String>> {
        let ocaml_index = rt.get(index);
        let index: &TezedgeIndexFFI = ocaml_index.borrow();
        let index = index.0.borrow().clone();
        let context_hash: ContextHash = context_hash.to_rust(rt);

        let result = index.checkout(&context_hash)
            .map_err(|err| format!("{:?}", err))
            .map(|opt| opt.map(TezedgeContextFFI::new));

        result.to_ocaml(rt)
    }

    fn tezedge_index_block_applied(
        rt,
        index: OCamlRef<DynBox<TezedgeIndexFFI>>,
        _context_hash: OCamlRef<Option<OCamlContextHash>>,
        cycle_position: OCamlRef<Option<OCamlInt>>,
    ) -> OCaml<Result<(), String>> {
        let ocaml_index = rt.get(index);
        let index: &TezedgeIndexFFI = ocaml_index.borrow();
        let mut index = index.0.borrow().clone();
        let cycle_position: Option<i64> = cycle_position.to_rust(rt);

        let result = if let Some(0) = cycle_position {
            index.cycle_started().map_err(|err| format!("BlockApplied->CycleStarted: {:?}", err))
        } else {
            Ok(())
        };

        result.to_ocaml(rt)
    }

    // Context API

    // OCaml = val commit : time:Time.Protocol.t -> ?message:string -> context -> Context_hash.t Lwt.t
    fn tezedge_context_commit(
        rt,
        date: OCamlRef<OCamlInt64>,
        message: OCamlRef<String>,
        author: OCamlRef<String>,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
    ) -> OCaml<Result<OCamlContextHash, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let message = message.to_rust(rt);
        let date = date.to_rust(rt);
        let author = author.to_rust(rt);

        // TODO: return commit value instead of hash?
        let result = context.commit(author, message, date)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val hash : time:Time.Protocol.t -> ?message:string -> context -> Context_hash.t
    fn tezedge_context_hash(
        rt,
        date: OCamlRef<OCamlInt64>,
        message: OCamlRef<String>,
        author: OCamlRef<String>,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
    ) -> OCaml<Result<OCamlContextHash, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let message = message.to_rust(rt);
        let date = date.to_rust(rt);
        let author = author.to_rust(rt);

        let result = context.hash(author, message, date)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val mem : context -> key -> bool Lwt.t
    fn tezedge_context_mem(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<bool, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let key = make_key(rt, key);

        let result = context.mem(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    fn tezedge_context_empty(
        rt,
        index: OCamlRef<DynBox<TezedgeIndexFFI>>,
    ) -> OCaml<DynBox<TezedgeContextFFI>> {
        let ocaml_index = rt.get(index);
        let index: &TezedgeIndexFFI = ocaml_index.borrow();
        let index = index.0.borrow().clone();
        let empty_context = TezedgeContext::new(index, None, None);

        OCaml::box_value(rt, empty_context.into())
    }

    // OCaml = val mem_tree : context -> key -> bool Lwt.t
    fn tezedge_context_mem_tree(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<bool> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let key = make_key(rt, key);

        let result = context.mem_tree(&key);

        result.to_ocaml(rt)
    }

    // OCaml = val find : context -> key -> value option Lwt.t
    fn tezedge_context_find(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<Option<OCamlBytes>, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let key = make_key(rt, key);

        let result = context.find(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val find_tree : context -> key -> tree option Lwt.t
    fn tezedge_context_find_tree(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<Option<DynBox<WorkingTreeFFI>>, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let key = make_key(rt, key);

        let result = context.find_tree(&key)
            .map_err(|err| format!("{:?}", err));
            //.map(|tree_opt| tree_opt.map(|tree| OCamlToRustPointer::alloc_custom(rt, Rc::new(tree))));

        result.to_ocaml(rt)
    }

    // OCaml = val add : context -> key -> value -> t Lwt.t
    fn tezedge_context_add(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        key: OCamlRef<OCamlList<String>>,
        value: OCamlRef<OCamlBytes>,
    ) -> OCaml<Result<DynBox<TezedgeContextFFI>, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let key = make_key(rt, key);

        let value_ref = rt.get(value);
        let value = value_ref.as_bytes();

        let result = context.add(&key, value)
            .map_err(|err| format!("{:?}", err))
            .map(TezedgeContextFFI::new);

        result.to_ocaml(rt)
    }

    // OCaml = val add_tree : context -> key -> tree -> t Lwt.t
    fn tezedge_context_add_tree(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        key: OCamlRef<OCamlList<String>>,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
    ) -> OCaml<Result<DynBox<TezedgeContextFFI>, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let key = make_key(rt, key);
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();

        let result =  context.add_tree(&key, tree)
            .map_err(|err| format!("{:?}", err))
            .map(TezedgeContextFFI::new);

         result.to_ocaml(rt)
    }

    // OCaml = val remove_ : context -> key -> t Lwt.t
    fn tezedge_context_remove(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<DynBox<TezedgeContextFFI>, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let key = make_key(rt, key);

        let result = context.delete(&key)
            .map_err(|err| format!("{:?}", err))
            .map(TezedgeContextFFI::new);

        result.to_ocaml(rt)
    }

    // OCaml = val list : context -> ?offset:int -> ?length:int -> key -> (string * tree) list Lwt.t
    fn tezedge_context_list(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        offset: OCamlRef<Option<OCamlInt>>,
        length: OCamlRef<Option<OCamlInt>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<OCamlList<(String, DynBox<WorkingTreeFFI>)>, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let offset: Option<i64> = offset.to_rust(rt);
        let offset = offset.map(|n| n as usize);
        let length: Option<i64> = length.to_rust(rt);
        let length = length.map(|n| n as usize);
        let key = make_key(rt, key);

        let result = context
            .list(offset, length, &key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    fn tezedge_context_get_tree(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
    ) -> OCaml<DynBox<WorkingTreeFFI>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let tree = Rc::clone(&context.0.borrow().tree);

        tree.to_ocaml(rt)
    }

    fn tezedge_context_set_tree(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
    ) -> OCaml<DynBox<TezedgeContextFFI>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let mut context = context.0.borrow().clone();
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();

        context.tree = Rc::new(tree.clone());

        let result = TezedgeContextFFI::new(context);

        result.to_ocaml(rt)
    }

    // Tree API

    // OCaml = val hash : tree -> Context_hash.t
    fn tezedge_tree_hash(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
    ) -> OCaml<Result<OCamlContextHash, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();

        let result = tree
            .hash()
            .map_err(|err| format!("{:?}", err))
            .map(|h| {
                ContextHash::try_from(&h[..]).map_err(|err| format!("{:?}", err))
            })
            .unwrap_or_else(Err);

        result.to_ocaml(rt)
    }

    // OCaml = val mem : tree -> key -> bool Lwt.t
    fn tezedge_tree_mem(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<bool, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let key = make_key(rt, key);

        let result = tree.mem(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val empty : context -> tree
    fn tezedge_tree_empty(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
    ) -> OCaml<DynBox<WorkingTreeFFI>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let empty_tree = WorkingTree::new_with_tree(context.index, Tree::empty());

        empty_tree.to_ocaml(rt)
    }

    // OCaml = val to_value : tree -> value option Lwt.t
    fn tezedge_tree_to_value(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
    ) -> OCaml<Option<OCamlBytes>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();

        let result = tree.get_value();

        result.to_ocaml(rt)
    }

    // OCaml = val of_value : context -> value -> tree Lwt.t
    fn tezedge_tree_of_value(
        rt,
        context: OCamlRef<DynBox<TezedgeContextFFI>>,
        value: OCamlRef<OCamlBytes>
    ) -> OCaml<Result<DynBox<WorkingTreeFFI>, String>> {
        let ocaml_context = rt.get(context);
        let context: &TezedgeContextFFI = ocaml_context.borrow();
        let context = context.0.borrow().clone();
        let value = rt.get(value);

        let value_id = {
            let mut storage = context.index.storage.borrow_mut();
            storage.add_blob_by_ref(value.as_bytes())
        };

        let tree = value_id.map(|v| {
            WorkingTree::new_with_value(context.index, v)
        }).map_err(|err| {
            format!("{:?}", err)
        });

        tree.to_ocaml(rt)
    }

    // OCaml = val is_empty : tree -> bool
    fn tezedge_tree_is_empty(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
    ) -> OCaml<bool> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();

        let result = tree.is_empty();

        result.to_ocaml(rt)
    }

    // OCaml = val equal : tree -> tree -> bool
    fn tezedge_tree_equal(
        rt,
        tree1: OCamlRef<DynBox<WorkingTreeFFI>>,
        tree2: OCamlRef<DynBox<WorkingTreeFFI>>,
    ) -> OCaml<Result<bool, String>> {
        let ocaml_tree1 = rt.get(tree1);
        let tree1: &WorkingTreeFFI = ocaml_tree1.borrow();
        let ocaml_tree2 = rt.get(tree2);
        let tree2: &WorkingTreeFFI = ocaml_tree2.borrow();
        let result = tree1.equal(tree2)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val kind : tree -> [`Value | `Tree]
    fn tezedge_tree_kind(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
    ) -> OCaml<TreeKind> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();

        let result = match tree.kind() {
            NodeKind::Leaf => TreeKind::Value,
            NodeKind::NonLeaf => TreeKind::Tree,
        };

        result.to_ocaml(rt)
    }

    // OCaml = val mem_tree : tree -> key -> bool Lwt.t
    fn tezedge_tree_mem_tree(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<bool> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let key = make_key(rt, key);

        let result = tree.mem_tree(&key);

        result.to_ocaml(rt)
    }

    // OCaml = val find : tree -> key -> value option Lwt.t
    fn tezedge_tree_find(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<Option<OCamlBytes>, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let key = make_key(rt, key);

        let result = tree.find(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val find_tree : tree -> key -> tree option Lwt.t
    fn tezedge_tree_find_tree(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<Option<DynBox<WorkingTreeFFI>>, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let key = make_key(rt, key);

        let result = tree.find_tree(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val add : tree -> key -> value -> t Lwt.t
    fn tezedge_tree_add(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        key: OCamlRef<OCamlList<String>>,
        value: OCamlRef<OCamlBytes>,
    ) -> OCaml<Result<DynBox<WorkingTreeFFI>, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let key = make_key(rt, key);

        let value_ref = rt.get(value);
        let value = value_ref.as_bytes();

        let result =  tree.add(&key, value)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val add_tree : tree -> key -> tree -> t Lwt.t
    fn tezedge_tree_add_tree(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        key: OCamlRef<OCamlList<String>>,
        new_tree: OCamlRef<DynBox<WorkingTreeFFI>>,
    ) -> OCaml<Result<DynBox<WorkingTreeFFI>, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let key = make_key(rt, key);
        let ocaml_new_tree = rt.get(new_tree);
        let new_tree: &WorkingTreeFFI = ocaml_new_tree.borrow();

        let result =  tree.add_tree(&key, new_tree)
            .map_err(|err| format!("{:?}", err));

         result.to_ocaml(rt)
    }

    // OCaml = val remove : tree -> key -> t Lwt.t
    fn tezedge_tree_remove(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<DynBox<WorkingTreeFFI>, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let key = make_key(rt, key);

        let result = tree.delete(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val list : tree -> ?offset:int -> ?length:int -> key -> (string * tree) list Lwt.t
    fn tezedge_tree_list(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        offset: OCamlRef<Option<OCamlInt>>,
        length: OCamlRef<Option<OCamlInt>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<OCamlList<(String, DynBox<WorkingTreeFFI>)>, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let offset: Option<i64> = offset.to_rust(rt);
        let offset = offset.map(|n| n as usize);
        let length: Option<i64> = length.to_rust(rt);
        let length = length.map(|n| n as usize);
        let key = make_key(rt, key);

        let result = tree
            .list(offset, length, &key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml =
    //  val fold :
    //    ?depth:[`Eq of int | `Le of int | `Lt of int | `Ge of int | `Gt of int] ->
    //    t ->
    //    key ->
    //    init:'a ->
    //    f:(key -> tree -> 'a -> 'a Lwt.t) ->
    //    'a Lwt.t
    fn tezedge_tree_walker_make(
        rt,
        tree: OCamlRef<DynBox<WorkingTreeFFI>>,
        depth: OCamlRef<Option<FoldDepth>>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<DynBox<TreeWalkerFFI>, String>> {
        let ocaml_tree = rt.get(tree);
        let tree: &WorkingTreeFFI = ocaml_tree.borrow();
        let key = make_key(rt, key);
        let depth: Option<FoldDepth> = depth.to_rust(rt);
        let result = tree.fold_iter(depth, &key)
            .map_err(|err| format!("{:?}", err))
            .map(TreeWalkerFFI::new);

        result.to_ocaml(rt)
    }

    fn tezedge_tree_walker_next(
        rt,
        walker: OCamlRef<DynBox<TreeWalkerFFI>>,
    ) -> OCaml<Option<(OCamlList<String>, DynBox<WorkingTreeFFI>)>> {
        let ocaml_walker = rt.get(walker);
        let walker: &TreeWalkerFFI = ocaml_walker.borrow();
        let result = walker.0.borrow_mut().next();

        result.to_ocaml(rt)
    }

    // Timings

    fn tezedge_timing_set_block(
        rt,
        block_hash: OCamlRef<Option<OCamlBlockHash>>,
    ) {
        timings::set_block(rt, block_hash);
        OCaml::unit()
    }

    fn tezedge_timing_checkout(
        rt,
        context_hash: OCamlRef<OCamlContextHash>,
        irmin_time: f64,
        tezedge_time: f64,
    ) {
        timings::checkout(rt, context_hash, irmin_time, tezedge_time);
        OCaml::unit()
    }

    fn tezedge_timing_set_operation(
        rt,
        operation_hash: OCamlRef<Option<OCamlOperationHash>>,
    ) {
        timings::set_operation(rt, operation_hash);
        OCaml::unit()
    }

    fn tezedge_timing_commit(
        rt,
        new_context_hash: OCamlRef<OCamlContextHash>,
        irmin_time: f64,
        tezedge_time: f64,
    ) {
        timings::commit(rt, new_context_hash, irmin_time, tezedge_time);
        OCaml::unit()
    }

    fn tezedge_timing_context_action(
        rt,
        action_name: OCamlRef<String>,
        key: OCamlRef<OCamlList<String>>,
        irmin_time: f64,
        tezedge_time: f64,
    ) {
        timings::context_action(rt, action_name, key, irmin_time, tezedge_time);
        OCaml::unit()
    }

    fn tezedge_timing_init(
        rt,
        db_path: OCamlRef<String>,
    ) {
        let db_path: String = db_path.to_rust(rt);

        timings::init_timing(db_path);
        OCaml::unit()
    }
}

unsafe impl ToOCaml<DynBox<WorkingTreeFFI>> for WorkingTreeFFI {
    fn to_ocaml<'gc>(&self, rt: &'gc mut OCamlRuntime) -> OCaml<'gc, DynBox<WorkingTreeFFI>> {
        OCaml::box_value(rt, self.clone())
    }
}

unsafe impl ToOCaml<DynBox<TreeWalkerFFI>> for TreeWalkerFFI {
    fn to_ocaml<'gc>(&self, rt: &'gc mut OCamlRuntime) -> OCaml<'gc, DynBox<TreeWalkerFFI>> {
        OCaml::box_value(rt, self.clone())
    }
}

unsafe impl ToOCaml<DynBox<TezedgeContextFFI>> for TezedgeContextFFI {
    fn to_ocaml<'gc>(&self, rt: &'gc mut OCamlRuntime) -> OCaml<'gc, DynBox<TezedgeContextFFI>> {
        OCaml::box_value(rt, self.clone())
    }
}

unsafe impl ToOCaml<DynBox<TezedgeIndexFFI>> for TezedgeIndexFFI {
    fn to_ocaml<'gc>(&self, rt: &'gc mut OCamlRuntime) -> OCaml<'gc, DynBox<TezedgeIndexFFI>> {
        OCaml::box_value(rt, self.clone())
    }
}

use tezos_sys::{
    initialize_tezedge_context_callbacks, initialize_tezedge_index_callbacks,
    initialize_tezedge_timing_callbacks, initialize_tezedge_tree_callbacks,
};

pub fn initialize_callbacks() {
    unsafe {
        initialize_tezedge_context_callbacks(
            tezedge_context_commit,
            tezedge_context_hash,
            tezedge_context_find_tree,
            tezedge_context_add_tree,
            tezedge_context_remove,
            tezedge_context_add,
            tezedge_context_find,
            tezedge_context_mem_tree,
            tezedge_context_mem,
            tezedge_context_list,
            tezedge_context_get_tree,
            tezedge_context_set_tree,
            tezedge_context_empty,
        );
        initialize_tezedge_tree_callbacks(
            tezedge_tree_hash,
            tezedge_tree_find_tree,
            tezedge_tree_add_tree,
            tezedge_tree_remove,
            tezedge_tree_add,
            tezedge_tree_find,
            tezedge_tree_mem_tree,
            tezedge_tree_mem,
            tezedge_tree_list,
            tezedge_tree_walker_make,
            tezedge_tree_walker_next,
            tezedge_tree_empty,
            tezedge_tree_to_value,
            tezedge_tree_of_value,
            tezedge_tree_is_empty,
            tezedge_tree_equal,
            tezedge_tree_kind,
        );
        initialize_tezedge_index_callbacks(
            tezedge_index_patch_context_get,
            tezedge_index_checkout,
            tezedge_index_exists,
            tezedge_index_close,
            tezedge_index_block_applied,
            tezedge_index_init,
        );
        initialize_tezedge_timing_callbacks(
            tezedge_timing_set_block,
            tezedge_timing_checkout,
            tezedge_timing_set_operation,
            tezedge_timing_commit,
            tezedge_timing_context_action,
            tezedge_timing_init,
        );
    }
}
