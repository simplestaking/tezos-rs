use riker::actors::*;
use super::peer::PeerRef;
use std::sync::Arc;
use crate::p2p::encoding::peer::PeerMessageResponse;

pub const DEFAULT_TOPIC: &str = "network";

/// Peer has been created. This event does indicate
/// only creation of the peer and is not indicative if
/// bootstrap is going to be successful or not.
#[derive(Clone, Debug)]
pub struct PeerCreated {
    pub peer: PeerRef,
}

/// The peer has disconnected.
#[derive(Clone, Debug)]
pub struct PeerDisconnected;

/// Peer has been bootstrapped.
#[derive(Clone, Debug)]
pub struct PeerBootstrapped {
    pub peer: PeerRef,
}

/// We have received message from another peer
#[derive(Clone, Debug)]
pub struct PeerMessageReceived {
    pub peer: PeerRef,
    pub message: Arc<PeerMessageResponse>
}

/// Network channel event message.
#[derive(Clone, Debug)]
pub enum NetworkChannelMsg {
    PeerCreated(PeerCreated),
    PeerDisconnected(PeerDisconnected),
    PeerBootstrapped(PeerBootstrapped),
    PeerMessageReceived(PeerMessageReceived)
}

impl From<PeerCreated> for NetworkChannelMsg {
    fn from(msg: PeerCreated) -> Self {
        NetworkChannelMsg::PeerCreated(msg)
    }
}

impl From<PeerDisconnected> for NetworkChannelMsg {
    fn from(msg: PeerDisconnected) -> Self {
        NetworkChannelMsg::PeerDisconnected(msg)
    }
}

impl From<PeerBootstrapped> for NetworkChannelMsg {
    fn from(msg: PeerBootstrapped) -> Self {
        NetworkChannelMsg::PeerBootstrapped(msg)
    }
}

impl From<PeerMessageReceived> for NetworkChannelMsg {
    fn from(msg: PeerMessageReceived) -> Self {
        NetworkChannelMsg::PeerMessageReceived(msg)
    }
}

/// Represents various topics
pub enum NetworkChannelTopic {
    /// Events generated from networking layer
    NetworkEvents
}

impl From<NetworkChannelTopic> for Topic {
    fn from(evt: NetworkChannelTopic) -> Self {
        match evt {
            NetworkChannelTopic::NetworkEvents => Topic::from("network.events")
        }
    }
}

/// This struct represents network bus where all network events must be published.
pub struct NetworkChannel(Channel<NetworkChannelMsg>);

impl NetworkChannel {

    pub fn actor(fact: &impl ActorRefFactory) -> Result<ChannelRef<NetworkChannelMsg>, CreateError> {
        fact.actor_of(Props::new(NetworkChannel::new), NetworkChannel::name())
    }

    fn name() -> &'static str {
        "network-event-channel"
    }

    fn new() -> Self {
        NetworkChannel(Channel::new())
    }
}

type ChannelCtx<Msg> = Context<ChannelMsg<Msg>>;

impl Actor for NetworkChannel {
    type Msg = ChannelMsg<NetworkChannelMsg>;

    fn pre_start(&mut self, ctx: &ChannelCtx<NetworkChannelMsg>) {
        self.0.pre_start(ctx);
    }

    fn recv(
        &mut self,
        ctx: &ChannelCtx<NetworkChannelMsg>,
        msg: ChannelMsg<NetworkChannelMsg>,
        sender: Sender,
    ) {
        self.0.receive(ctx, msg, sender);
    }
}
