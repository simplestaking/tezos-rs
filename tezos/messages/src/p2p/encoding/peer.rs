// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use getset::Getters;
use serde::{Deserialize, Serialize};

use crate::p2p::{binary_message::SizeFromChunk, encoding::prelude::*, peer_message_size};
use tezos_encoding::encoding::HasEncoding;
use tezos_encoding::nom::NomReader;

use super::limits::MESSAGE_MAX_SIZE;

#[derive(Serialize, Deserialize, Debug, Clone, HasEncoding, NomReader)]
#[encoding(tags = "u16", ignore_unknown)]
pub enum PeerMessage {
    #[encoding(tag = 0x01)]
    Disconnect,
    #[encoding(tag = 0x03)]
    Advertise(AdvertiseMessage),
    #[encoding(tag = 0x04)]
    SwapRequest(SwapMessage),
    #[encoding(tag = 0x05)]
    SwapAck(SwapMessage),
    #[encoding(tag = 0x02)]
    Bootstrap,
    #[encoding(tag = 0x10)]
    GetCurrentBranch(GetCurrentBranchMessage),
    #[encoding(tag = 0x11)]
    CurrentBranch(CurrentBranchMessage),
    #[encoding(tag = 0x12)]
    Deactivate(DeactivateMessage),
    #[encoding(tag = 0x13)]
    GetCurrentHead(GetCurrentHeadMessage),
    #[encoding(tag = 0x14)]
    CurrentHead(CurrentHeadMessage),
    #[encoding(tag = 0x20)]
    GetBlockHeaders(GetBlockHeadersMessage),
    #[encoding(tag = 0x21)]
    BlockHeader(BlockHeaderMessage),
    #[encoding(tag = 0x30)]
    GetOperations(GetOperationsMessage),
    #[encoding(tag = 0x31)]
    Operation(OperationMessage),
    #[encoding(tag = 0x40)]
    GetProtocols(GetProtocolsMessage),
    #[encoding(tag = 0x41)]
    Protocol(ProtocolMessage),
    #[encoding(tag = 0x60)]
    GetOperationsForBlocks(GetOperationsForBlocksMessage),
    #[encoding(tag = 0x61)]
    OperationsForBlocks(OperationsForBlocksMessage),
}

#[derive(Clone, Serialize, Deserialize, Debug, Getters, HasEncoding, NomReader)]
#[encoding(dynamic = "MESSAGE_MAX_SIZE")]
pub struct PeerMessageResponse {
    #[get = "pub"]
    pub message: PeerMessage,
}

impl From<PeerMessage> for PeerMessageResponse {
    fn from(message: PeerMessage) -> Self {
        PeerMessageResponse { message }
    }
}

macro_rules! into_peer_message {
    ($m:ident,$v:ident) => {
        impl From<$m> for PeerMessageResponse {
            fn from(msg: $m) -> Self {
                PeerMessage::$v(msg).into()
            }
        }

        impl From<$m> for std::sync::Arc<PeerMessageResponse> {
            fn from(msg: $m) -> Self {
                std::sync::Arc::new(PeerMessage::$v(msg).into())
            }
        }

        impl From<$m> for PeerMessage {
            fn from(msg: $m) -> Self {
                PeerMessage::$v(msg)
            }
        }
    };
}

into_peer_message!(AdvertiseMessage, Advertise);
into_peer_message!(GetCurrentBranchMessage, GetCurrentBranch);
into_peer_message!(CurrentBranchMessage, CurrentBranch);
into_peer_message!(GetBlockHeadersMessage, GetBlockHeaders);
into_peer_message!(BlockHeaderMessage, BlockHeader);
into_peer_message!(GetCurrentHeadMessage, GetCurrentHead);
into_peer_message!(CurrentHeadMessage, CurrentHead);
into_peer_message!(GetOperationsForBlocksMessage, GetOperationsForBlocks);
into_peer_message!(OperationsForBlocksMessage, OperationsForBlocks);
into_peer_message!(GetOperationsMessage, GetOperations);
into_peer_message!(OperationMessage, Operation);

impl SizeFromChunk for PeerMessageResponse {
    fn size_from_chunk(
        bytes: impl AsRef<[u8]>,
    ) -> Result<usize, tezos_encoding::binary_reader::BinaryReaderError> {
        peer_message_size(bytes.as_ref()).map(|s| s + 4)
    }
}
