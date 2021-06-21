// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::time::Duration;

use networking::PeerAddress;
use riker::{actor::*, actors::SystemMsg, system::SystemEvent, system::Timer};
use slog::{debug, info, warn, Logger};

use crypto::hash::ChainId;
use networking::p2p::network_channel::{NetworkChannelMsg, NetworkChannelRef, PeerMessageReceived};
use shell::shell_channel::{ShellChannelMsg, ShellChannelRef};
use shell::subscription::{
    subscribe_to_actor_terminated, subscribe_to_network_events, subscribe_to_shell_events,
    subscribe_to_shell_new_current_head,
};
use storage::chain_meta_storage::ChainMetaStorageReader;
use storage::PersistentStorage;
use storage::{BlockStorage, BlockStorageReader, ChainMetaStorage, OperationsMetaStorage};
use tezos_messages::p2p::binary_message::BinaryWrite;

use crate::websocket::handler_messages::HandlerMessage;
use crate::{
    monitors::*, websocket::handler_messages::PeerConnectionStatus, websocket::WebsocketHandlerMsg,
};

/// How often to print stats in logs
const LOG_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub enum BroadcastSignal {
    PublishPeerStatistics,
    PublishBlocksStatistics,
    PeerUpdate(PeerConnectionStatus),
}

#[derive(Clone, Debug)]
pub struct LogStats;

pub type MonitorRef = ActorRef<MonitorMsg>;

#[actor(
    BroadcastSignal,
    NetworkChannelMsg,
    ShellChannelMsg,
    LogStats
)]
pub struct Monitor {
    network_channel: NetworkChannelRef,
    shell_channel: ShellChannelRef,
    msg_channel: ActorRef<WebsocketHandlerMsg>,
    /// Monitors
    peer_monitors: HashMap<PeerAddress, PeerMonitor>,
    bootstrap_monitor: BootstrapMonitor,
    blocks_monitor: BlocksMonitor,
    block_application_monitor: ApplicationMonitor,
    chain_monitor: ChainMonitor,

    /// Count of received messages from the last log
    actor_received_messages_count: usize,
}

impl Monitor {
    fn name() -> &'static str {
        "monitor-manager"
    }

    pub fn actor(
        sys: &impl ActorRefFactory,
        event_channel: NetworkChannelRef,
        msg_channel: ActorRef<WebsocketHandlerMsg>,
        shell_channel: ShellChannelRef,
        persistent_storage: PersistentStorage,
        main_chain_id: ChainId,
    ) -> Result<MonitorRef, CreateError> {
        sys.actor_of_props::<Monitor>(
            Self::name(),
            Props::new_args((
                event_channel,
                msg_channel,
                shell_channel,
                persistent_storage,
                main_chain_id,
            )),
        )
    }

    fn process_peer_message(&mut self, msg: PeerMessageReceived, log: &Logger) {
        use std::mem::size_of_val;
        use tezos_messages::p2p::encoding::peer::PeerMessage;

        if let PeerMessage::CurrentBranch(msg) = msg.message.message() {
            if msg.current_branch().current_head().level() > 0 {
                self.bootstrap_monitor
                    .set_level(msg.current_branch().current_head().level() as usize);
            }
        }

        if let Some(monitor) = self.peer_monitors.get_mut(&msg.peer_address) {
            // TODO: TE-190 - reimplement correctly, now not all messages are counted in (Ack, Metadata, ConnectionMessage is not involved)
            let size = if let Ok(msg) = msg.message.as_bytes() {
                msg.len()
            } else {
                size_of_val(&msg.message)
            };
            monitor.incoming_bytes(size);
        } else {
            debug!(log, "Missing monitor for peer"; "peer" => msg.peer_address.to_string());
        }
    }

    fn get_and_clear_actor_received_messages_count(&mut self) -> usize {
        std::mem::replace(&mut self.actor_received_messages_count, 0)
    }
}

impl
    ActorFactoryArgs<(
        NetworkChannelRef,
        ActorRef<WebsocketHandlerMsg>,
        ShellChannelRef,
        PersistentStorage,
        ChainId,
    )> for Monitor
{
    fn create_args(
        (event_channel, msg_channel, shell_channel, persistent_storage, main_chain_id): (
            NetworkChannelRef,
            ActorRef<WebsocketHandlerMsg>,
            ShellChannelRef,
            PersistentStorage,
            ChainId,
        ),
    ) -> Self {
        let (chain_monitor, blocks_monitor, bootstrap_monitor) =
            initialize_monitors(&persistent_storage, &main_chain_id);

        Self {
            network_channel: event_channel,
            shell_channel,
            msg_channel,
            peer_monitors: HashMap::new(),
            bootstrap_monitor,
            blocks_monitor,
            block_application_monitor: ApplicationMonitor::new(),
            chain_monitor,
            actor_received_messages_count: 0,
        }
    }
}

impl Actor for Monitor {
    type Msg = MonitorMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        subscribe_to_shell_events(&self.shell_channel, ctx.myself());
        subscribe_to_shell_new_current_head(&self.shell_channel, ctx.myself());
        subscribe_to_network_events(&self.network_channel, ctx.myself());

        // Every second, send yourself a message to broadcast the monitoring to all connected clients
        ctx.schedule(
            Duration::from_secs(1),
            Duration::from_secs(1),
            ctx.myself(),
            None,
            BroadcastSignal::PublishPeerStatistics,
        );
        ctx.schedule::<Self::Msg, _>(
            LOG_INTERVAL / 2,
            LOG_INTERVAL,
            ctx.myself(),
            None,
            LogStats.into(),
        );
        ctx.schedule(
            Duration::from_secs_f32(1.5),
            Duration::from_secs(1),
            ctx.myself(),
            None,
            BroadcastSignal::PublishBlocksStatistics,
        );
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Option<BasicActorRef>) {
        self.actor_received_messages_count += 1;
        self.receive(ctx, msg, sender);
    }
}

impl Receive<BroadcastSignal> for Monitor {
    type Msg = MonitorMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: BroadcastSignal, _sender: Sender) {
        match msg {
            BroadcastSignal::PublishPeerStatistics => {
                let peer_stats: HandlerMessage = self.peer_monitors.values_mut().collect();
                self.msg_channel.tell(peer_stats, ctx.myself().into());
            }
            BroadcastSignal::PublishBlocksStatistics => {
                let bootstrap_stats: HandlerMessage = self.bootstrap_monitor.snapshot().into();
                self.msg_channel.tell(bootstrap_stats, ctx.myself().into());

                let payload = self.blocks_monitor.snapshot();
                self.msg_channel
                    .tell(HandlerMessage::BlockStatus { payload }, ctx.myself().into());

                let payload = self.block_application_monitor.snapshot();
                self.msg_channel.tell(
                    HandlerMessage::BlockApplicationStatus { payload },
                    ctx.myself().into(),
                );

                let payload = self.chain_monitor.snapshot();
                self.msg_channel
                    .tell(HandlerMessage::ChainStatus { payload }, ctx.myself().into());
            }
            BroadcastSignal::PeerUpdate(msg) => {
                let msg: HandlerMessage = msg.into();
                self.msg_channel.tell(msg, ctx.myself().into())
            }
        }
    }
}

impl Receive<NetworkChannelMsg> for Monitor {
    type Msg = MonitorMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: NetworkChannelMsg, _: Sender) {
        match msg {
            NetworkChannelMsg::PeerBootstrapped(peer_id, _, _) => {
                let previous = self.peer_monitors.insert(
                    peer_id.address.clone(),
                    PeerMonitor::new(
                        peer_id.address.into(),
                        peer_id.public_key_hash.clone(),
                    ),
                );
                if let Some(previous) = previous {
                    warn!(ctx.system.log(), "Duplicate monitor found for peer"; "key" => peer_id.address.to_string());
                } else {
                    ctx.myself.tell(
                        BroadcastSignal::PeerUpdate(PeerConnectionStatus::connected(
                            peer_id.address.to_string(),
                        )),
                        None,
                    );
                }
            }
            NetworkChannelMsg::PeerMessageReceived(msg) => {
                self.process_peer_message(msg, &ctx.system.log())
            }
            NetworkChannelMsg::PeerDisconnected(peer) | NetworkChannelMsg::PeerBlacklisted(peer) => {
                if let Some(_) = self.peer_monitors.remove(&peer) {
                    ctx.myself.tell(
                        BroadcastSignal::PeerUpdate(PeerConnectionStatus::disconnected(
                            peer.to_string(),
                        )),
                        None,
                    );
                }
            }
            _ => (),
        }
    }
}

impl Receive<ShellChannelMsg> for Monitor {
    type Msg = MonitorMsg;

    fn receive(&mut self, _ctx: &Context<Self::Msg>, msg: ShellChannelMsg, _sender: Sender) {
        match msg {
            ShellChannelMsg::BlockReceived(msg) => {
                // Update current max block count
                self.bootstrap_monitor.set_level(msg.level as usize);

                // Start tracking it in the blocks monitor
                self.blocks_monitor.accept_block();
                self.bootstrap_monitor.increase_headers_count();

                // update stats for block header
                self.chain_monitor.process_block_header(msg.level);
            }
            ShellChannelMsg::NewCurrentHead(head, ..) => {
                // update stats for block applications
                self.chain_monitor.process_block_application(*head.level());

                self.blocks_monitor.block_was_applied_by_protocol();
                self.block_application_monitor.block_was_applied(head);
            }
            ShellChannelMsg::AllBlockOperationsReceived(msg) => {
                self.bootstrap_monitor.increase_block_count();
                self.blocks_monitor.block_finished_downloading_operations();

                // update stats for block operations
                self.chain_monitor.process_block_operations(msg.level);
            }
            _ => (),
        }
    }
}

fn initialize_monitors(
    persistent_storage: &PersistentStorage,
    main_chain_id: &ChainId,
) -> (ChainMonitor, BlocksMonitor, BootstrapMonitor) {
    let mut chain_monitor = ChainMonitor::new();

    let block_storage = BlockStorage::new(&persistent_storage);
    let operations_meta_storage = OperationsMetaStorage::new(&persistent_storage);

    let mut downloaded_headers = 0;
    let mut downloaded_blocks = 0;

    // populate the monitors with the data from storage
    if let Ok(iter) = block_storage.iterator() {
        for key in iter {
            if let Ok(Some(header_with_hash)) = block_storage.get(&key) {
                let block_level = header_with_hash.header.level();
                chain_monitor.process_block_header(block_level);
                downloaded_headers += 1;

                if let Ok(is_complete) = operations_meta_storage.is_complete(&header_with_hash.hash)
                {
                    if is_complete {
                        chain_monitor.process_block_operations(block_level);
                        downloaded_blocks += 1;
                    }
                }
            }
        }
    }

    let current_head_level = if let Ok(Some(head)) =
        ChainMetaStorage::new(&persistent_storage).get_current_head(&main_chain_id)
    {
        *head.level()
    } else {
        0
    };

    for level in 0..current_head_level {
        chain_monitor.process_block_application(level)
    }

    let bootstrap_monitor = BootstrapMonitor::initialize(downloaded_blocks, downloaded_headers);
    let block_monitor = BlocksMonitor::new(4096, downloaded_blocks);

    (chain_monitor, block_monitor, bootstrap_monitor)
}

impl Receive<LogStats> for Monitor {
    type Msg = MonitorMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, _: LogStats, _: Sender) {
        info!(ctx.system.log(), "Monitoring info";
                   "actor_received_messages_count" => self.get_and_clear_actor_received_messages_count(),
                   "peers_count" => self.peer_monitors.len(),
        );
    }
}
