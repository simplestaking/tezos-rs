// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This channel is used to transmit message to [`MempoolPrevalidator`]

use std::sync::Arc;

use riker::actors::*;

use crypto::hash::OperationHash;
use storage::mempool_storage::MempoolOperationType;
use storage::BlockHeaderWithHash;

use crate::utils::CondvarResult;

/// Notify actors that operations should by validated by mempool
#[derive(Clone, Debug)]
pub struct MempoolOperationReceived {
    pub operation_hash: OperationHash,
    pub operation_type: MempoolOperationType,
    pub result_callback: Option<CondvarResult<(), anyhow::Error>>,
}

/// Channel event message.
#[derive(Clone, Debug)]
pub enum MempoolChannelMsg {
    ResetMempool(Arc<BlockHeaderWithHash>),
    ValidateOperation(MempoolOperationReceived),
}

impl From<MempoolOperationReceived> for MempoolChannelMsg {
    fn from(msg: MempoolOperationReceived) -> Self {
        MempoolChannelMsg::ValidateOperation(msg)
    }
}

/// Represents various topics
pub struct MempoolChannelTopic;

impl From<MempoolChannelTopic> for Topic {
    fn from(_: MempoolChannelTopic) -> Self {
        Topic::from("mempool")
    }
}

/// This struct represents shell event bus where all shell events must be published.
pub struct MempoolChannel(Channel<MempoolChannelMsg>);

pub type MempoolChannelRef = ChannelRef<MempoolChannelMsg>;

impl MempoolChannel {
    pub fn actor(fact: &impl ActorRefFactory) -> Result<MempoolChannelRef, CreateError> {
        fact.actor_of::<MempoolChannel>(MempoolChannel::name())
    }

    fn name() -> &'static str {
        "mempool-channel"
    }
}

type ChannelCtx<Msg> = Context<ChannelMsg<Msg>>;

impl ActorFactory for MempoolChannel {
    fn create() -> Self {
        MempoolChannel(Channel::default())
    }
}

impl Actor for MempoolChannel {
    type Msg = ChannelMsg<MempoolChannelMsg>;

    fn pre_start(&mut self, ctx: &ChannelCtx<MempoolChannelMsg>) {
        self.0.pre_start(ctx);
    }

    fn recv(
        &mut self,
        ctx: &ChannelCtx<MempoolChannelMsg>,
        msg: ChannelMsg<MempoolChannelMsg>,
        sender: Sender,
    ) {
        self.0.receive(ctx, msg, sender);
    }
}

#[inline]
pub fn subscribe_to_mempool_channel<M, E>(channel: &ChannelRef<E>, myself: ActorRef<M>)
where
    M: Message,
    E: Message + Into<M>,
{
    channel.tell(
        Subscribe {
            actor: Box::new(myself),
            topic: MempoolChannelTopic.into(),
        },
        None,
    );
}
