// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Shell channel is used to transmit high level shell messages.

use std::sync::Arc;

use riker::actors::*;

use crypto::hash::{BlockHash, ChainId};
use storage::BlockHeaderWithHash;
use tezos_messages::p2p::encoding::prelude::{Mempool, Operation, Path};
use tezos_messages::Head;

use crate::state::synchronization_state::PeerBranchSynchronizationDone;
use crate::utils::CondvarResult;

/// Notify actors that system is about to shut down
#[derive(Clone, Debug)]
pub struct ShuttingDown;

/// Request peers to send their current heads
#[derive(Clone, Debug)]
pub struct RequestCurrentHead;

/// Message informing actors about receiving block header
#[derive(Clone, Debug)]
pub struct BlockReceived {
    pub hash: BlockHash,
    pub level: i32,
}

/// Message informing actors about receiving all operations for a specific block
#[derive(Clone, Debug)]
pub struct AllBlockOperationsReceived {
    pub level: i32,
}

#[derive(Clone, Debug)]
pub struct InjectBlock {
    pub chain_id: Arc<ChainId>,
    pub block_header: Arc<BlockHeaderWithHash>,
    pub operations: Option<Vec<Vec<Operation>>>,
    pub operation_paths: Option<Vec<Path>>,
}

/// Shell channel event message.
#[derive(Clone, Debug)]
pub enum ShellChannelMsg {
    /// Events
    /// If chain_manager resolved new current head for chain
    NewCurrentHead(Head, Arc<BlockHeaderWithHash>),
    BlockReceived(BlockReceived),
    BlockApplied(Arc<BlockHash>),
    AllBlockOperationsReceived(AllBlockOperationsReceived),

    /// Commands
    AdvertiseToP2pNewCurrentBranch(Arc<ChainId>, Arc<BlockHash>),
    AdvertiseToP2pNewCurrentHead(Arc<ChainId>, Arc<BlockHash>),
    AdvertiseToP2pNewMempool(Arc<ChainId>, Arc<BlockHash>, Arc<Mempool>),
    InjectBlock(InjectBlock, Option<CondvarResult<(), anyhow::Error>>),
    RequestCurrentHead(RequestCurrentHead),
    PeerBranchSynchronizationDone(PeerBranchSynchronizationDone),
    ShuttingDown(ShuttingDown),
}

impl From<BlockReceived> for ShellChannelMsg {
    fn from(msg: BlockReceived) -> Self {
        ShellChannelMsg::BlockReceived(msg)
    }
}

impl From<AllBlockOperationsReceived> for ShellChannelMsg {
    fn from(msg: AllBlockOperationsReceived) -> Self {
        ShellChannelMsg::AllBlockOperationsReceived(msg)
    }
}

impl From<ShuttingDown> for ShellChannelMsg {
    fn from(msg: ShuttingDown) -> Self {
        ShellChannelMsg::ShuttingDown(msg)
    }
}

impl From<RequestCurrentHead> for ShellChannelMsg {
    fn from(msg: RequestCurrentHead) -> Self {
        ShellChannelMsg::RequestCurrentHead(msg)
    }
}

/// Represents various topics
pub enum ShellChannelTopic {
    /// Ordinary events generated from shell layer
    ShellEvents,

    /// Dedicated channel for new current head
    ShellNewCurrentHead,

    /// Control event
    ShellCommands,

    /// Shutdown event
    ShellShutdown,
}

impl From<ShellChannelTopic> for Topic {
    fn from(evt: ShellChannelTopic) -> Self {
        match evt {
            ShellChannelTopic::ShellEvents => Topic::from("shell.events"),
            ShellChannelTopic::ShellNewCurrentHead => Topic::from("shell.new_current_head"),
            ShellChannelTopic::ShellCommands => Topic::from("shell.commands"),
            ShellChannelTopic::ShellShutdown => Topic::from("shell.shutdown"),
        }
    }
}

/// This struct represents shell event bus where all shell events must be published.
pub struct ShellChannel(Channel<ShellChannelMsg>);

pub type ShellChannelRef = ChannelRef<ShellChannelMsg>;

impl ShellChannel {
    pub fn actor(fact: &impl ActorRefFactory) -> Result<ShellChannelRef, CreateError> {
        fact.actor_of::<ShellChannel>(ShellChannel::name())
    }

    fn name() -> &'static str {
        "shell-event-channel"
    }
}

type ChannelCtx<Msg> = Context<ChannelMsg<Msg>>;

impl ActorFactory for ShellChannel {
    fn create() -> Self {
        ShellChannel(Channel::default())
    }
}

impl Actor for ShellChannel {
    type Msg = ChannelMsg<ShellChannelMsg>;

    fn pre_start(&mut self, ctx: &ChannelCtx<ShellChannelMsg>) {
        self.0.pre_start(ctx);
    }

    fn recv(
        &mut self,
        ctx: &ChannelCtx<ShellChannelMsg>,
        msg: ChannelMsg<ShellChannelMsg>,
        sender: Sender,
    ) {
        self.0.receive(ctx, msg, sender);
    }
}
