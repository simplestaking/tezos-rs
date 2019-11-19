use networking::p2p::network_channel::{NetworkChannelMsg, NetworkChannelTopic, NetworkChannelRef};
use shell::shell_channel::{ShellChannelRef, ShellChannelMsg, ShellChannelTopic, BlockApplied};
use riker::{
    actors::*,
};
use crate::{
    server::{spawn_server, control_msg::*},
};
use slog::warn;
use std::net::SocketAddr;
use tokio::runtime::Runtime;
use std::sync::Arc;
use storage::{BlockStorageReader, BlockHeaderWithHash};
use tezos_encoding::hash::{ChainId, ProtocolHash, HashEncoding, HashType};
use crate::helpers::FullBlockInfo;

pub type RpcServerRef = ActorRef<RpcServerMsg>;

/// Actor responsible for managing HTTP REST API and server, and to share parts of inner actor
/// system with the server.
#[actor(NetworkChannelMsg, ShellChannelMsg, GetCurrentHead, GetFullCurrentHead)]
pub struct RpcServer {
    network_channel: NetworkChannelRef,
    shell_channel: ShellChannelRef,
    // Stats
    chain_id: ChainId,
    _supported_protocols: Vec<ProtocolHash>,
    current_head: Option<BlockApplied>,
    db: Arc<rocksdb::DB>,
}

impl RpcServer {
    pub fn name() -> &'static str { "rpc-server" }

    fn new((network_channel, shell_channel, db, chain_id, supported_protocols): (NetworkChannelRef, ShellChannelRef, Arc<rocksdb::DB>, ChainId, Vec<ProtocolHash>)) -> Self {
        let current_head = if let Some(h) = Self::load_current_head(db.clone()) {
            Some(BlockApplied {
                hash: h.hash,
                level: h.header.level(),
                header: h.header,
                block_header_proto_info: Default::default(),
                block_header_info: None,
            })
        } else {
            None
        };

        Self {
            network_channel,
            shell_channel,
            chain_id,
            _supported_protocols: supported_protocols,
            current_head,
            db,
        }
    }

    pub fn actor(sys: &ActorSystem, network_channel: NetworkChannelRef, shell_channel: ShellChannelRef, addr: SocketAddr, runtime: &Runtime, db: Arc<rocksdb::DB>, chain_id: ChainId, protocols: Vec<ProtocolHash>) -> Result<RpcServerRef, CreateError> {
        let ret = sys.actor_of(
            Props::new_args(Self::new, (network_channel, shell_channel, db, chain_id, protocols)),
            Self::name(),
        )?;

        let server = spawn_server(&addr, sys.clone(), ret.clone());
        let inner_log = sys.log();
        runtime.spawn(async move {
            if let Err(e) = server.await {
                warn!(inner_log, "HTTP Server encountered failure"; "error" => format!("{}", e));
            }
        });
        Ok(ret)
    }

    /// Load local head (block with highest level) from dedicated storage
    fn load_current_head(db: Arc<rocksdb::DB>) -> Option<BlockHeaderWithHash> {
        use storage::{BlockMetaStorage, BlockStorage, IteratorMode};
        use tezos_encoding::hash::BlockHash as RawBlockHash;

        let meta_storage = BlockMetaStorage::new(db.clone());
        let mut head: Option<RawBlockHash> = None;
        if let Ok(iter) = meta_storage.iter(IteratorMode::End) {
            let cur_level = -1;
            for (key, value) in iter {
                if let Ok(value) = value {
                    if cur_level < value.level {
                        head = Some(key.unwrap())
                    }
                }
            }
            if let Some(head) = head {
                let block_storage = BlockStorage::new(db.clone());
                if let Ok(Some(head)) = block_storage.get(&head) {
                    return Some(head);
                }
            }
        }
        None
    }
}

impl Actor for RpcServer {
    type Msg = RpcServerMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        self.network_channel.tell(Subscribe {
            actor: Box::new(ctx.myself()),
            topic: NetworkChannelTopic::NetworkEvents.into(),
        }, ctx.myself().into());

        self.shell_channel.tell(Subscribe {
            actor: Box::new(ctx.myself()),
            topic: ShellChannelTopic::ShellEvents.into(),
        }, ctx.myself().into());
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Option<BasicActorRef>) {
        self.receive(ctx, msg, sender);
    }
}

impl Receive<NetworkChannelMsg> for RpcServer {
    type Msg = RpcServerMsg;

    fn receive(&mut self, _ctx: &Context<Self::Msg>, _msg: NetworkChannelMsg, _sender: Sender) {
        /* Not yet implemented, do nothing */
    }
}

impl Receive<ShellChannelMsg> for RpcServer {
    type Msg = RpcServerMsg;

    fn receive(&mut self, _ctx: &Context<Self::Msg>, msg: ShellChannelMsg, _sender: Sender) {
        match msg {
            ShellChannelMsg::BlockApplied(data) => {
                if let Some(ref current_head) = self.current_head {
                    if current_head.level < data.level {
                        self.current_head = Some(data);
                    }
                } else {
                    self.current_head = Some(data);
                }
            }
            _ => (/* Not yet implemented, do nothing */),
        }
    }
}

impl Receive<GetCurrentHead> for RpcServer {
    type Msg = RpcServerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: GetCurrentHead, sender: Sender) {
        if let GetCurrentHead::Request = msg {
            if let Some(sender) = sender {
                let me: Option<BasicActorRef> = ctx.myself().into();
                if sender.try_tell(GetCurrentHead::Response(self.current_head.clone()), me).is_err() {
                    warn!(ctx.system.log(), "Failed to send response for GetCurrentHead");
                }
            }
        }
    }
}

impl Receive<GetFullCurrentHead> for RpcServer {
    type Msg = RpcServerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: GetFullCurrentHead, sender: Sender) {
        use storage::{OperationsStorage, operations_storage::OperationsStorageReader};

        if let GetFullCurrentHead::Request = msg {
            if let Some(sender) = sender {
                let me: Option<BasicActorRef> = ctx.myself().into();
                let current_head = self.current_head.clone();
                let resp = GetFullCurrentHead::Response(if let Some(head) = current_head {
                    let ops_storage = OperationsStorage::new(self.db.clone());
                    let _ops = ops_storage.get_operations(&head.hash).unwrap_or_default();
                    let mut head: FullBlockInfo = head.into();
                    head.chain_id = HashEncoding::new(HashType::ChainId).bytes_to_string(&self.chain_id);
                    Some(head)
                } else {
                    None
                });

                if sender.try_tell(resp, me).is_err() {
                    warn!(ctx.system.log(), "Failed to send response for GetFullCurrentHead");
                }
            }
        }
    }
}