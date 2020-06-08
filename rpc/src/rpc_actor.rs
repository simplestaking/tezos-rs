// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use getset::Getters;
use riker::actors::*;
use slog::{Logger, warn};
use tokio::runtime::Handle;

use crypto::hash::ChainId;
use shell::shell_channel::{BlockApplied, ShellChannelMsg, ShellChannelRef, ShellChannelTopic};
use storage::persistent::PersistentStorage;
use storage::StorageInitInfo;

use crate::server::{RpcServiceEnvironment, spawn_server};

pub type RpcServerRef = ActorRef<RpcServerMsg>;

/// Thread safe reference to a shared RPC state
pub type RpcCollectedStateRef = Arc<RwLock<RpcCollectedState>>;

/// Represents various collected information about
/// internal state of the node.
#[derive(Getters)]
pub struct RpcCollectedState {
    #[get = "pub(crate)"]
    current_head: Option<BlockApplied>,
    #[get = "pub(crate)"]
    chain_id: ChainId,
}

/// Actor responsible for managing HTTP REST API and server, and to share parts of inner actor
/// system with the server.
#[actor(ShellChannelMsg)]
pub struct RpcServer {
    shell_channel: ShellChannelRef,
    state: RpcCollectedStateRef,
}

impl RpcServer {
    pub fn name() -> &'static str { "rpc-server" }

    pub fn actor(
        sys: &ActorSystem,
        shell_channel: ShellChannelRef,
        rpc_listen_address: SocketAddr,
        tokio_executor: &Handle,
        persistent_storage: &PersistentStorage,
        init_storage_data: &StorageInitInfo) -> Result<RpcServerRef, CreateError> {

        // TODO: refactor - call load_current_head in pre_start
        let shared_state = Arc::new(RwLock::new(RpcCollectedState {
            current_head: load_current_head(persistent_storage, sys.log()),
            chain_id: init_storage_data.chain_id.clone(),
        }));
        let actor_ref = sys.actor_of_props::<RpcServer>(
            Self::name(),
            Props::new_args((shell_channel, shared_state.clone())),
        )?;

        // spawn RPC JSON server
        {
            let env = RpcServiceEnvironment::new(sys.clone(), actor_ref.clone(), persistent_storage, &init_storage_data.genesis_block_header_hash, shared_state, sys.log());
            let inner_log = sys.log();

            tokio_executor.spawn(async move {
                if let Err(e) = spawn_server(&rpc_listen_address, env).await {
                    warn!(inner_log, "HTTP Server encountered failure"; "error" => format!("{}", e));
                }
            });
        }

        Ok(actor_ref)
    }
}

impl ActorFactoryArgs<(ShellChannelRef, RpcCollectedStateRef)> for RpcServer {
    fn create_args((shell_channel, state): (ShellChannelRef, RpcCollectedStateRef)) -> Self {
        Self { shell_channel, state }
    }
}

impl Actor for RpcServer {
    type Msg = RpcServerMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        self.shell_channel.tell(Subscribe {
            actor: Box::new(ctx.myself()),
            topic: ShellChannelTopic::ShellEvents.into(),
        }, ctx.myself().into());
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Option<BasicActorRef>) {
        self.receive(ctx, msg, sender);
    }
}

impl Receive<ShellChannelMsg> for RpcServer {
    type Msg = RpcServerMsg;

    fn receive(&mut self, _ctx: &Context<Self::Msg>, msg: ShellChannelMsg, _sender: Sender) {
        match msg {
            ShellChannelMsg::BlockApplied(block) => {
                let current_head_ref = &mut *self.state.write().unwrap();
                match &mut current_head_ref.current_head {
                    Some(current_head) => {
                        if current_head.header().header.level() < block.header().header.level() {
                            *current_head = block;
                        }
                    }
                    None => current_head_ref.current_head = Some(block)
                }
            }
            _ => (/* Not yet implemented, do nothing */),
        }
    }
}

/// Load local head (block with highest level) from dedicated storage
fn load_current_head(persistent_storage: &PersistentStorage, log: Logger) -> Option<BlockApplied> {
    use storage::{BlockStorage, BlockStorageReader, BlockMetaStorage, BlockMetaStorageReader, StorageError};

    let block_meta_storage = BlockMetaStorage::new(persistent_storage);
    match block_meta_storage.load_current_head() {
        Ok(Some((block_hash, ..))) => {
            let block_applied = BlockStorage::new(persistent_storage)
                .get_with_json_data(&block_hash)
                .and_then(|data| data.map(|(block, json)| BlockApplied::new(block, json)).ok_or(StorageError::MissingKey));
            match block_applied {
                Ok(block) => Some(block),
                Err(e) => {
                    warn!(log, "Error reading current head detail from database."; "reason" => format!("{}", e));
                    None
                }
            }
        },
        Ok(None) => None,
        Err(e) => {
            warn!(log, "Error reading current head from database."; "reason" => format!("{}", e));
            None
        }
    }
}