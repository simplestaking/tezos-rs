// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::mem::size_of;

use getset::Getters;
use serde::{Deserialize, Serialize};

use tezos_encoding::encoding::{Encoding, Field, HasEncoding, Tag, TagMap};
use tezos_encoding::has_encoding;

use crate::cached_data;
use crate::p2p::binary_message::cache::BinaryDataCache;
use crate::p2p::encoding::prelude::*;

use super::limits::MESSAGE_MAX_SIZE;

#[cfg(feature = "arbitrary-fuzz")]
use arbitrary::Arbitrary;

#[cfg_attr(feature = "arbitrary-fuzz", derive(Arbitrary))]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PeerMessage {
    Disconnect,
    Advertise(AdvertiseMessage),
    SwapRequest(SwapMessage),
    SwapAck(SwapMessage),
    Bootstrap,
    GetCurrentBranch(GetCurrentBranchMessage),
    CurrentBranch(CurrentBranchMessage),
    Deactivate(DeactivateMessage),
    GetCurrentHead(GetCurrentHeadMessage),
    CurrentHead(CurrentHeadMessage),
    GetBlockHeaders(GetBlockHeadersMessage),
    BlockHeader(BlockHeaderMessage),
    GetOperations(GetOperationsMessage),
    Operation(OperationMessage),
    GetProtocols(GetProtocolsMessage),
    Protocol(ProtocolMessage),
    GetOperationsForBlocks(GetOperationsForBlocksMessage),
    OperationsForBlocks(OperationsForBlocksMessage),
}

#[cfg_attr(feature = "arbitrary-fuzz", derive(Arbitrary))]
#[derive(Serialize, Deserialize, Debug, Getters)]
pub struct PeerMessageResponse {
    #[get = "pub"]
    message: PeerMessage,
    #[serde(skip_serializing)]
    body: BinaryDataCache,
}

cached_data!(PeerMessageResponse, body);
has_encoding!(PeerMessageResponse, PEER_MESSAGE_RESPONSE_ENCODING, {
    Encoding::bounded_dynamic(
        MESSAGE_MAX_SIZE,
        Encoding::Obj(
            "PeerMessageResponse",
            vec![Field::new(
                "message",
                Encoding::Tags(
                    size_of::<u16>(),
                    TagMap::new(vec![
                        Tag::new(0x01, "Disconnect", Encoding::Unit),
                        Tag::new(0x02, "Bootstrap", Encoding::Unit),
                        Tag::new(0x03, "Advertise", AdvertiseMessage::encoding().clone()),
                        Tag::new(0x04, "SwapRequest", SwapMessage::encoding().clone()),
                        Tag::new(0x05, "SwapAck", SwapMessage::encoding().clone()),
                        Tag::new(
                            0x10,
                            "GetCurrentBranch",
                            GetCurrentBranchMessage::encoding().clone(),
                        ),
                        Tag::new(
                            0x11,
                            "CurrentBranch",
                            CurrentBranchMessage::encoding().clone(),
                        ),
                        Tag::new(0x12, "Deactivate", DeactivateMessage::encoding().clone()),
                        Tag::new(
                            0x13,
                            "GetCurrentHead",
                            GetCurrentHeadMessage::encoding().clone(),
                        ),
                        Tag::new(0x14, "CurrentHead", CurrentHeadMessage::encoding().clone()),
                        Tag::new(
                            0x20,
                            "GetBlockHeaders",
                            GetBlockHeadersMessage::encoding().clone(),
                        ),
                        Tag::new(0x21, "BlockHeader", BlockHeaderMessage::encoding().clone()),
                        Tag::new(
                            0x30,
                            "GetOperations",
                            GetOperationsMessage::encoding().clone(),
                        ),
                        Tag::new(0x31, "Operation", OperationMessage::encoding().clone()),
                        Tag::new(
                            0x40,
                            "GetProtocols",
                            GetProtocolsMessage::encoding().clone(),
                        ),
                        Tag::new(0x41, "Protocol", ProtocolMessage::encoding().clone()),
                        Tag::new(
                            0x60,
                            "GetOperationsForBlocks",
                            GetOperationsForBlocksMessage::encoding().clone(),
                        ),
                        Tag::new(
                            0x61,
                            "OperationsForBlocks",
                            OperationsForBlocksMessage::encoding().clone(),
                        ),
                    ]),
                ),
            )],
        ),
    )
});

impl From<PeerMessage> for PeerMessageResponse {
    fn from(message: PeerMessage) -> Self {
        PeerMessageResponse {
            message,
            body: Default::default(),
        }
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
