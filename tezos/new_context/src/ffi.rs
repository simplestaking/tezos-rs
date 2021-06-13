// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Functions exposed to be called from OCaml

// TODO: init function

use std::{marker::PhantomData, rc::Rc};

use ocaml_interop::*;

use crypto::hash::ContextHash;

use crate::{
    initializer::initialize_tezedge_index, initializer::ContextKvStoreConfiguration,
    working_tree::working_tree::WorkingTree, ContextKey, ContextValue, IndexApi,
    ProtocolContextApi, ShellContextApi, TezedgeContext, TezedgeIndex,
};
use tezos_api::ocaml_conv::OCamlContextHash;

// TODO: instead of converting errors into strings, it may be useful to pass
// them around using custom pointers so that they can be recovered later.
// OCaml code will not do anything with the errors, just raice an exception,
// but once we catch it on Rust, having the original error value may be useful.

ocaml_export! {
    fn tezedge_index_init(
        rt,
        _unit: OCamlRef<()>,
    ) -> OCaml<TezedgeIndex> {
        let index = initialize_tezedge_index(&ContextKvStoreConfiguration::InMem);
        let index = OCamlToRustPointer::alloc_custom(rt, index);
        index.to_ocaml(rt)
    }

    fn tezedge_index_close(
        rt,
        _index: OCamlRef<TezedgeIndex>,
    ) {
        OCaml::unit()
    }

    // OCaml = val exists : index -> Context_hash.t -> bool Lwt.t
    fn tezedge_index_exists(
        rt,
        index: OCamlRef<TezedgeIndex>,
        context_hash: OCamlRef<OCamlContextHash>,
    ) -> OCaml<Result<bool, String>> {
        let index_ptr: OCamlToRustPointer<TezedgeIndex> = index.to_rust(rt);
        let index = index_ptr.as_ref();
        let context_hash: ContextHash = context_hash.to_rust(rt);

        let result = index.exists(&context_hash)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val checkout : index -> Context_hash.t -> context option Lwt.t
    fn tezedge_index_checkout(
        rt,
        index: OCamlRef<TezedgeIndex>,
        context_hash: OCamlRef<OCamlContextHash>,
    ) -> OCaml<Result<Option<TezedgeContext>, String>> {
        let index_ptr: OCamlToRustPointer<TezedgeIndex> = index.to_rust(rt);
        let index = index_ptr.as_ref();
        let context_hash: ContextHash = context_hash.to_rust(rt);

        let result = index.checkout(&context_hash)
            .map_err(|err| format!("{:?}", err))
            .map(|ok| ok.map(|context| OCamlToRustPointer::alloc_custom(rt, context)));

        result.to_ocaml(rt)
    }

    // OCaml = val commit : time:Time.Protocol.t -> ?message:string -> context -> Context_hash.t Lwt.t
    fn tezedge_context_commit(
        rt,
        date: OCamlRef<OCamlInt64>,
        message: OCamlRef<String>,
        author: OCamlRef<String>,
        context: OCamlRef<TezedgeContext>,
    ) -> OCaml<Result<OCamlContextHash, String>> {
        let mut context_ptr: OCamlToRustPointer<TezedgeContext> = context.to_rust(rt);
        let context = context_ptr.as_mut();
        let message = message.to_rust(rt);
        let date = date.to_rust(rt);
        let author = author.to_rust(rt);

        // TODO: commit value instead of hash
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
        context: OCamlRef<TezedgeContext>,
    ) -> OCaml<Result<OCamlContextHash, String>> {
        let context_ptr: OCamlToRustPointer<TezedgeContext> = context.to_rust(rt);
        let context = context_ptr.as_ref();
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
        context: OCamlRef<TezedgeContext>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<bool, String>> {
        let context_ptr: OCamlToRustPointer<TezedgeContext> = context.to_rust(rt);
        let context = context_ptr.as_ref();
        let key: ContextKey = key.to_rust(rt);

        let result = context.mem(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // TODO: implement
    fn tezedge_context_empty(
        rt,
        _unit: OCamlRef<()>,
    ) {
        OCaml::unit()
    }

    // TODO: implement
    fn tezedge_index_patch_context_get(
        rt,
        _unit: OCamlRef<()>,
    ) {
        OCaml::unit()
    }

    // OCaml = val dir_mem : context -> key -> bool Lwt.t
    fn tezedge_context_mem_tree(
        rt,
        context: OCamlRef<TezedgeContext>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<bool, String>> {
        let context_ptr: OCamlToRustPointer<TezedgeContext> = context.to_rust(rt);
        let context = context_ptr.as_ref();
        let key: ContextKey = key.to_rust(rt);

        let result = context.dirmem(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)
    }

    // OCaml = val get : context -> key -> value option Lwt.t
    fn tezedge_context_find(
        rt,
        context: OCamlRef<TezedgeContext>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<OCamlBytes, String>> {
        let context_ptr: OCamlToRustPointer<TezedgeContext> = context.to_rust(rt);
        let context = context_ptr.as_ref();
        let key: ContextKey = key.to_rust(rt);

        let result = context.get(&key)
            .map_err(|err| format!("{:?}", err));

        result.to_ocaml(rt)

    }

    // OCaml = val set : context -> key -> value -> t Lwt.t
    fn tezedge_context_add(
        rt,
        context: OCamlRef<TezedgeContext>,
        key: OCamlRef<OCamlList<String>>,
        value: OCamlRef<OCamlBytes>,
    ) -> OCaml<Result<TezedgeContext, String>> {
        let context_ptr: OCamlToRustPointer<TezedgeContext> = context.to_rust(rt);
        let context = context_ptr.as_ref();
        let key: ContextKey = key.to_rust(rt);
        let value: ContextValue = value.to_rust(rt);

        let result =  context.set(&key, value)
            .map_err(|err| format!("{:?}", err))
            .map(|context| OCamlToRustPointer::alloc_custom(rt, context));

        result.to_ocaml(rt)
    }

    // OCaml = val remove_rec : context -> key -> t Lwt.t
    fn tezedge_context_remove(
        rt,
        context: OCamlRef<TezedgeContext>,
        key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<TezedgeContext, String>> {
        let context_ptr: OCamlToRustPointer<TezedgeContext> = context.to_rust(rt);
        let context = context_ptr.as_ref();
        let key: ContextKey = key.to_rust(rt);

        let result = context.delete(&key)
            .map_err(|err| format!("{:?}", err))
            .map(|context| OCamlToRustPointer::alloc_custom(rt, context));

        result.to_ocaml(rt)
    }

    // (** [copy] returns None if the [from] key is not bound *)
    // OCaml = val copy : context -> from:key -> to_:key -> context option Lwt.t
    fn tezedge_context_copy(
        rt,
        context: OCamlRef<TezedgeContext>,
        from_key: OCamlRef<OCamlList<String>>,
        to_key: OCamlRef<OCamlList<String>>,
    ) -> OCaml<Result<Option<TezedgeContext>, String>> {
        let context_ptr: OCamlToRustPointer<TezedgeContext> = context.to_rust(rt);
        let context = context_ptr.as_ref();
        let from_key: ContextKey = from_key.to_rust(rt);
        let to_key: ContextKey = to_key.to_rust(rt);

        let result = context.copy(&from_key, &to_key)
            .map_err(|err| format!("{:?}", err))
            .map(|ok| ok.map(|context| OCamlToRustPointer::alloc_custom(rt, context)));

        result.to_ocaml(rt)
    }

    // TODO: fold
    // type key_or_dir = [`Key of key | `Dir of key]
    //
    // (** [fold] iterates over elements under a path (not recursive). Iteration order
    //     is nondeterministic. *)
    // val fold :
    //   context -> key -> init:'a -> f:(key_or_dir -> 'a -> 'a Lwt.t) -> 'a Lwt.t
}

use tezos_sys::initialize_tezedge_context_callbacks;

pub fn initialize_callbacks() {
    unsafe {
        initialize_tezedge_context_callbacks(
            tezedge_context_commit,
            tezedge_context_hash,
            tezedge_context_copy,
            tezedge_context_remove,
            tezedge_context_add,
            tezedge_context_find,
            tezedge_context_mem_tree,
            tezedge_context_mem,
            tezedge_context_empty,
            tezedge_index_patch_context_get,
            tezedge_index_checkout,
            tezedge_index_exists,
            tezedge_index_close,
            tezedge_index_init,
        )
    }
}

ocaml_export! {}

// Custom pointers from OCaml's heap to Rust's heap

// TODO: reimplement all this directly into ocaml-interop

pub const DEFAULT_CUSTOM_OPS: CustomOps = CustomOps {
    identifier: core::ptr::null(),
    fixed_length: core::ptr::null_mut(),
    compare: None,
    compare_ext: None,
    deserialize: None,
    finalize: None,
    hash: None,
    serialize: None,
};

#[derive(Clone)]
#[repr(C)]
#[allow(missing_docs)]
pub struct CustomOps {
    pub identifier: *const ocaml_sys::Char,
    pub finalize: Option<unsafe extern "C" fn(v: RawOCaml)>,
    pub compare: Option<unsafe extern "C" fn(v1: RawOCaml, v2: RawOCaml) -> i32>,
    pub hash: Option<unsafe extern "C" fn(v: RawOCaml) -> OCamlInt>,

    pub serialize: Option<
        unsafe extern "C" fn(
            v: RawOCaml,
            bsize_32: *mut ocaml_sys::Uintnat,
            bsize_64: *mut ocaml_sys::Uintnat,
        ),
    >,
    pub deserialize:
        Option<unsafe extern "C" fn(dst: *mut core::ffi::c_void) -> ocaml_sys::Uintnat>,
    pub compare_ext: Option<unsafe extern "C" fn(v1: RawOCaml, v2: RawOCaml) -> i32>,
    pub fixed_length: *const ocaml_sys::custom_fixed_length,
}

pub trait CustomOCamlPointer {
    const NAME: &'static str;
    const FIXED_LENGTH: Option<ocaml_sys::custom_fixed_length> = None;
    const OPS: CustomOps;
    const USED: usize = 0;
    const MAX: usize = 1;

    fn ops() -> &'static CustomOps {
        &Self::OPS
    }
}

// NOTE: the block is not initialized, a pointer must be written to it immediately
// before anything else happens
unsafe fn alloc_custom<T>(_rt: &mut OCamlRuntime) -> RawOCaml
where
    T: CustomOCamlPointer,
{
    ocaml_sys::caml_alloc_custom(
        &T::ops() as *const _ as *const ocaml_sys::custom_operations,
        ::core::mem::size_of::<T>(),
        T::USED,
        T::MAX,
    )
}

// OCamlToRustPointer is an allocated OCaml custom block which contains
// a Rust value.

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct OCamlToRustPointer<T: 'static>(pub RawOCaml, PhantomData<T>);

impl<T> OCamlToRustPointer<T> {
    pub fn alloc_custom(rt: &mut OCamlRuntime, x: T) -> Self
    where
        T: CustomOCamlPointer,
    {
        unsafe {
            let mut ptr = Self(alloc_custom::<T>(rt), PhantomData);
            ptr.set(x);
            ptr
        }
    }

    pub fn set(&mut self, x: T) {
        unsafe {
            ::core::ptr::write_unaligned(self.as_mut_ptr(), x);
        }
    }

    pub fn as_ptr(&self) -> *const T {
        unsafe { ocaml_sys::field(self.0, 1) as *const T }
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        unsafe { ocaml_sys::field(self.0, 1) as *mut T }
    }
}

impl<'a, T> AsRef<T> for OCamlToRustPointer<T> {
    fn as_ref(&self) -> &T {
        unsafe { &*self.as_ptr() }
    }
}

impl<'a, T> AsMut<T> for OCamlToRustPointer<T> {
    fn as_mut(&mut self) -> &mut T {
        unsafe { &mut *self.as_mut_ptr() }
    }
}

// Fake conversion from OCamlToRustPointer<T> into OCaml<T>.
// Doesn't need to allocate anything, just reuse the pointer,

unsafe impl<T: CustomOCamlPointer> ToOCaml<T> for OCamlToRustPointer<T> {
    fn to_ocaml<'gc>(&self, rt: &'gc mut OCamlRuntime) -> OCaml<'gc, T> {
        unsafe { OCaml::new(rt, self.0) }
    }
}

unsafe impl<T: CustomOCamlPointer> FromOCaml<T> for OCamlToRustPointer<T> {
    fn from_ocaml(value: OCaml<T>) -> Self {
        OCamlToRustPointer(unsafe { value.raw() }, PhantomData)
    }
}

#[macro_export]
macro_rules! impl_custom_ocaml_pointer {
    ($name:ident $(<$t:tt>)? $({$($k:ident : $v:expr),* $(,)? })?) => {
        impl $(<$t>)? CustomOCamlPointer for $name $(<$t>)? {
            impl_custom_ocaml_pointer! {
                name: concat!("rust.", stringify!($name))
                $(, $($k: $v),*)?
            }
        }
    };
    {name : $name:expr $(, fixed_length: $fl:expr)? $(, $($k:ident : $v:expr),*)? $(,)? } => {
        const NAME: &'static str = concat!($name, "\0");

        const OPS: CustomOps = CustomOps {
            identifier: Self::NAME.as_ptr() as *const ocaml_sys::Char,
            $($($k: Some($v),)*)?
            .. DEFAULT_CUSTOM_OPS
        };
    };
}

// Concrete implementations of custom pointers used by the API

impl_custom_ocaml_pointer!(TezedgeIndex {
    finalize: tezedge_drop_tezedge_index,
});

impl_custom_ocaml_pointer!(TezedgeContext {
    finalize: tezedge_drop_tezedge_context,
});

type WorkingTreeRc = Rc<WorkingTree>;

impl_custom_ocaml_pointer!(WorkingTreeRc {
    finalize: tezedge_drop_working_tree_rc,
});

extern "C" fn tezedge_drop_tezedge_index(v: RawOCaml) {
    unsafe {
        let ptr = ocaml_sys::field(v, 1) as *mut TezedgeIndex;
        std::ptr::drop_in_place(ptr);
    }
}

extern "C" fn tezedge_drop_tezedge_context(v: RawOCaml) {
    unsafe {
        let ptr = ocaml_sys::field(v, 1) as *mut TezedgeContext;
        std::ptr::drop_in_place(ptr);
    }
}

extern "C" fn tezedge_drop_working_tree_rc(v: RawOCaml) {
    unsafe {
        let ptr = ocaml_sys::field(v, 1) as *mut WorkingTreeRc;
        std::ptr::drop_in_place(ptr);
    }
}
