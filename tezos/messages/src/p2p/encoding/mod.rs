// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Provides definitions of p2p messages.

pub mod ack;
pub mod advertise;
pub mod block_header;
pub mod connection;
pub mod current_branch;
pub mod current_head;
pub mod deactivate;
pub mod limits;
pub mod mempool;
pub mod metadata;
pub mod operation;
pub mod operations_for_blocks;
pub mod peer;
pub mod protocol;
pub mod swap;
pub mod version;

pub mod prelude {
    pub use super::ack::AckMessage;
    pub use super::advertise::AdvertiseMessage;
    pub use super::block_header::{
        BlockHeader, BlockHeaderBuilder, BlockHeaderMessage, GetBlockHeadersMessage,
    };
    pub use super::connection::ConnectionMessage;
    pub use super::current_branch::{CurrentBranch, CurrentBranchMessage, GetCurrentBranchMessage};
    pub use super::current_head::{CurrentHeadMessage, GetCurrentHeadMessage};
    pub use super::deactivate::DeactivateMessage;
    pub use super::mempool::Mempool;
    pub use super::metadata::MetadataMessage;
    pub use super::operation::{GetOperationsMessage, Operation, OperationMessage};
    pub use super::operations_for_blocks::MAX_PASS_MERKLE_DEPTH;
    pub use super::operations_for_blocks::{
        GetOperationsForBlocksMessage, OperationsForBlock, OperationsForBlocksMessage, Path,
        PathLeft, PathRight,
    };
    pub use super::peer::{PeerMessage, PeerMessageResponse};
    pub use super::protocol::{Component, GetProtocolsMessage, Protocol, ProtocolMessage};
    pub use super::swap::SwapMessage;
    pub use super::version::NetworkVersion;
}

use prelude::*;

/// All possible p2p messages.
#[derive(Debug, Clone)]
pub enum Message {
    Ack(AckMessage),
    Advertise(AdvertiseMessage),
    BlockHeader(BlockHeaderMessage),
    GetBlockHeaders(GetBlockHeadersMessage),
    Connection(ConnectionMessage),
    CurrentBranch(CurrentBranchMessage),
    GetCurrentBranch(GetCurrentBranchMessage),
    CurrentHead(CurrentHeadMessage),
    GetCurrentHead(GetCurrentHeadMessage),
    Deactivate(DeactivateMessage),
    Metadata(MetadataMessage),
    GetOperations(GetOperationsMessage),
    Operation(OperationMessage),
    GetOperationsForBlock(GetOperationsForBlocksMessage),
    OperationsForBlock(OperationsForBlocksMessage),
    GetProtocols(GetProtocolsMessage),
    Protocol(ProtocolMessage),
    Swap(SwapMessage),
}
