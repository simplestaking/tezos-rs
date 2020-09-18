// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crypto::hash::BlockHash;
use tezos_messages::p2p::encoding::{
    block_header::BlockHeader, operation::Operation, operations_for_blocks::Path,
};

// FFI Wrappers:
// Defined to be able to implement ToOCaml/FromOCaml traits for
// structs that are defined in another crate and/or do not map directly
// to the matching OCaml struct.
pub struct FfiPath(pub Path);
pub struct FfiBlockHeader(pub BlockHeader);
pub struct FfiBlockHeaderShellHeader<'a>(pub &'a FfiBlockHeader);
pub struct FfiOperation(pub Operation);
pub struct FfiOperationShellHeader<'a> {
    pub branch: &'a BlockHash,
}

// Hashes
struct OCamlHash {}
pub struct OCamlOperationListListHash {}
pub struct OCamlOperationHash {}
pub struct OCamlBlockHash {}
pub struct OCamlContextHash {}
pub struct OCamlProtocolHash {}

pub mod from_ocaml;
pub mod to_ocaml;
