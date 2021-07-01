// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crypto::hash::Hash;
use tezos_messages::p2p::encoding::block_header::{Fitness, Level};

// FFI Wrappers:
// Defined to be able to implement ToOCaml/FromOCaml traits for
// structs that are defined in another crate and/or do not map directly
// to the matching OCaml struct.

pub struct FfiPathRight {
    pub left: Hash,
    pub path: FfiPath,
}

pub struct FfiPathLeft {
    pub path: FfiPath,
    pub right: Hash,
}

pub enum FfiPath {
    Right(Box<FfiPathRight>),
    Left(Box<FfiPathLeft>),
    Op,
}

pub struct FfiBlockHeader<'a> {
    shell: FfiBlockHeaderShellHeader<'a>,
    protocol_data: &'a Vec<u8>,
}
pub struct FfiBlockHeaderShellHeader<'a> {
    level: Level,
    proto_level: i32,
    predecessor: TaggedHash<'a>,
    timestamp: i64,
    validation_passes: i32,
    operations_hash: TaggedHash<'a>,
    fitness: &'a Fitness,
    context: TaggedHash<'a>,
}
pub struct FfiOperation<'a> {
    shell: FfiOperationShellHeader<'a>,
    data: &'a Vec<u8>,
}
pub struct FfiOperationShellHeader<'a> {
    pub branch: TaggedHash<'a>,
}

// A hash that is represented as a tagged block in OCaml (borrows form a `Hash`)
pub enum TaggedHash<'a> {
    Hash(&'a Hash),
}

// Hashes
pub struct OCamlHash {}
pub struct OCamlOperationListListHash {}
pub struct OCamlOperationHash {}
pub struct OCamlBlockHash {}
pub struct OCamlContextHash {}
pub struct OCamlProtocolHash {}
pub struct OCamlBlockMetadataHash {}
pub struct OCamlOperationMetadataHash {}
pub struct OCamlOperationMetadataListListHash {}
pub struct OCamlChainId {}

pub mod from_ocaml;
pub mod to_ocaml;

pub mod ffi_error_ids {
    pub const APPLY_ERROR: &str = "ffi.apply_error";
    pub const CALL_ERROR: &str = "ffi.call_error";
    pub const CALL_EXCEPTION: &str = "ffi.call_exception";
    pub const INCONSISTENT_OPERATIONS_HASH: &str = "ffi.inconsistent_operations_hash";
    pub const PREDECESSOR_MISMATCH: &str = "ffi.predecessor_mismatch";
    pub const UNAVAILABLE_PROTOCOL: &str = "ffi.unavailable_protocol";
    pub const UNKNOWN_CONTEXT: &str = "ffi.unknown_context";
    pub const UNKNOWN_PREDECESSOR_CONTEXT: &str = "ffi.unknown_predecessor_context";
}
