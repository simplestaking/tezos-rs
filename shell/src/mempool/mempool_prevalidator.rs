// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This actor responsibility is to take care of Mempool/MempoolState,
//! which means, to validate operations which are not yet injected in any block.
//!
//! This actor listens on shell events (see [process_shell_channel_message]) and schedules it to internal queue/channel for validation processing.
//!
//! Actor validates received operations and result of validate as a new MempoolState is send back to shell channel, where:
//!     - is used by rpc_actor to show current mempool state - pending_operations
//!     - is used by chain_manager to send new current head with current mempool to inform other peers throught P2P

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver as QueueReceiver, Sender as QueueSender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::thread::JoinHandle;

use anyhow::{format_err, Error};
use riker::actors::*;
use slog::{debug, info, trace, warn, Logger};
use thiserror::Error;

use crypto::hash::{BlockHash, ChainId, OperationHash};
use storage::chain_meta_storage::{ChainMetaStorage, ChainMetaStorageReader};
use storage::mempool_storage::MempoolOperationType;
use storage::{BlockHeaderWithHash, PersistentStorage};
use storage::{BlockStorage, BlockStorageReader, MempoolStorage, StorageError};
use tezos_api::ffi::{
    Applied, BeginConstructionRequest, PrevalidatorWrapper, ValidateOperationRequest,
};
use tezos_messages::p2p::encoding::block_header::BlockHeader;
use tezos_wrapper::service::{
    handle_protocol_service_error, ProtocolController, ProtocolServiceError,
};
use tezos_wrapper::TezosApiConnectionPool;

use crate::mempool::mempool_channel::{
    subscribe_to_mempool_channel, MempoolChannelMsg, MempoolChannelRef, MempoolOperationReceived,
};
use crate::mempool::mempool_state::collect_mempool;
use crate::mempool::CurrentMempoolStateStorageRef;
use crate::shell_channel::{ShellChannelMsg, ShellChannelRef, ShellChannelTopic};
use crate::subscription::subscribe_to_shell_shutdown;
use crate::utils::{dispatch_condvar_result, CondvarResult};

type SharedJoinHandle = Arc<Mutex<Option<JoinHandle<Result<(), Error>>>>>;

/// Feeds blocks and operations to the tezos protocol (ocaml code).
#[actor(ShellChannelMsg, MempoolChannelMsg)]
pub struct MempoolPrevalidator {
    shell_channel: ShellChannelRef,
    mempool_channel: MempoolChannelRef,

    validator_event_sender: Arc<Mutex<QueueSender<Event>>>,
    validator_run: Arc<AtomicBool>,
    validator_thread: SharedJoinHandle,
}

enum Event {
    NewHead(Arc<BlockHeaderWithHash>),
    ValidateOperation(
        OperationHash,
        MempoolOperationType,
        Option<CondvarResult<(), anyhow::Error>>,
    ),
    ShuttingDown,
}

/// Reference to [chain feeder](ChainFeeder) actor
pub type MempoolPrevalidatorRef = ActorRef<MempoolPrevalidatorMsg>;

impl MempoolPrevalidator {
    pub fn actor(
        sys: &impl ActorRefFactory,
        shell_channel: ShellChannelRef,
        mempool_channel: MempoolChannelRef,
        persistent_storage: &PersistentStorage,
        current_mempool_state_storage: CurrentMempoolStateStorageRef,
        chain_id: ChainId,
        tezos_readonly_api: Arc<TezosApiConnectionPool>,
        log: Logger,
    ) -> Result<MempoolPrevalidatorRef, CreateError> {
        // spawn thread which processes event
        let (validator_event_sender, mut validator_event_receiver) = channel();
        let validator_run = Arc::new(AtomicBool::new(true));
        let validator_thread = {
            let persistent_storage = persistent_storage.clone();
            let shell_channel = shell_channel.clone();
            let validator_run = validator_run.clone();

            thread::spawn(move || {
                let block_storage = BlockStorage::new(&persistent_storage);
                let chain_meta_storage = ChainMetaStorage::new(&persistent_storage);
                let mempool_storage = MempoolStorage::new(&persistent_storage);

                while validator_run.load(Ordering::Acquire) {
                    match tezos_readonly_api.pool.get() {
                        Ok(mut protocol_controller) => match process_prevalidation(
                            &block_storage,
                            &chain_meta_storage,
                            &mempool_storage,
                            current_mempool_state_storage.clone(),
                            &chain_id,
                            &validator_run,
                            &shell_channel,
                            &protocol_controller.api,
                            &mut validator_event_receiver,
                            &log,
                        ) {
                            Ok(()) => {
                                protocol_controller.set_release_on_return_to_pool();
                                info!(log, "Mempool - prevalidation process finished")
                            }
                            Err(err) => {
                                protocol_controller.set_release_on_return_to_pool();
                                if validator_run.load(Ordering::Acquire) {
                                    warn!(log, "Mempool - error while process prevalidation"; "reason" => format!("{:?}", err));
                                }
                            }
                        },
                        Err(err) => {
                            warn!(log, "Mempool - no protocol runner connection available (try next turn)!"; "pool_name" => tezos_readonly_api.pool_name.clone(), "reason" => format!("{:?}", err))
                        }
                    }
                }

                info!(log, "Mempool prevalidator thread finished");
                Ok(())
            })
        };

        // create actor
        let myself = sys.actor_of_props::<MempoolPrevalidator>(
            MempoolPrevalidator::name(),
            Props::new_args((
                shell_channel,
                mempool_channel,
                validator_run,
                Arc::new(Mutex::new(Some(validator_thread))),
                Arc::new(Mutex::new(validator_event_sender)),
            )),
        )?;

        Ok(myself)
    }

    /// The `MempoolPrevalidator` is intended to serve as a singleton actor so that's why
    /// we won't support multiple names per instance.
    pub fn name() -> &'static str {
        "mempool-prevalidator"
    }

    fn process_shell_channel_message(
        &mut self,
        _: &Context<MempoolPrevalidatorMsg>,
        msg: ShellChannelMsg,
    ) -> Result<(), Error> {
        if let ShellChannelMsg::ShuttingDown(_) = msg {
            self.validator_event_sender
                .lock()
                .map_err(|e| format_err!("Failed to obtain the lock: {:?}", e))?
                .send(Event::ShuttingDown)?;
        }
        Ok(())
    }

    fn process_mempool_channel_message(
        &mut self,
        _: &Context<MempoolPrevalidatorMsg>,
        msg: MempoolChannelMsg,
    ) -> Result<(), Error> {
        match msg {
            MempoolChannelMsg::ResetMempool(block) => {
                // add NewHead to queue
                self.validator_event_sender
                    .lock()
                    .map_err(|e| format_err!("Failed to obtain the lock: {:?}", e))?
                    .send(Event::NewHead(block))?;
            }
            MempoolChannelMsg::ValidateOperation(MempoolOperationReceived {
                operation_hash,
                operation_type,
                result_callback,
            }) => {
                // add operation to queue for validation
                self.validator_event_sender
                    .lock()
                    .map_err(|e| format_err!("Failed to obtain the lock: {:?}", e))?
                    .send(Event::ValidateOperation(
                        operation_hash,
                        operation_type,
                        result_callback,
                    ))?;
            }
        }

        Ok(())
    }
}

impl
    ActorFactoryArgs<(
        ShellChannelRef,
        MempoolChannelRef,
        Arc<AtomicBool>,
        SharedJoinHandle,
        Arc<Mutex<QueueSender<Event>>>,
    )> for MempoolPrevalidator
{
    fn create_args(
        (shell_channel, mempool_channel, validator_run, validator_thread, validator_event_sender): (
            ShellChannelRef,
            MempoolChannelRef,
            Arc<AtomicBool>,
            SharedJoinHandle,
            Arc<Mutex<QueueSender<Event>>>,
        ),
    ) -> Self {
        MempoolPrevalidator {
            shell_channel,
            mempool_channel,
            validator_run,
            validator_thread,
            validator_event_sender,
        }
    }
}

impl Actor for MempoolPrevalidator {
    type Msg = MempoolPrevalidatorMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        subscribe_to_shell_shutdown(&self.shell_channel, ctx.myself());
        subscribe_to_mempool_channel(&self.mempool_channel, ctx.myself());
    }

    fn post_stop(&mut self) {
        self.validator_run.store(false, Ordering::Release);

        let join_handle = self
            .validator_thread
            .lock()
            .unwrap()
            .take()
            .expect("Thread join handle is missing");
        join_handle.thread().unpark();
        let _ = join_handle
            .join()
            .expect("Failed to join block applier thread");
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        self.receive(ctx, msg, sender);
    }
}

impl Receive<ShellChannelMsg> for MempoolPrevalidator {
    type Msg = MempoolPrevalidatorMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: ShellChannelMsg, _sender: Sender) {
        match self.process_shell_channel_message(ctx, msg) {
            Ok(_) => (),
            Err(e) => {
                warn!(ctx.system.log(), "Mempool - failed to process shell channel message"; "reason" => format!("{:?}", e))
            }
        }
    }
}

impl Receive<MempoolChannelMsg> for MempoolPrevalidator {
    type Msg = MempoolPrevalidatorMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: MempoolChannelMsg, _: Sender) {
        match self.process_mempool_channel_message(ctx, msg) {
            Ok(_) => (),
            Err(e) => {
                warn!(ctx.system.log(), "Mempool - failed to process mempool channel message"; "reason" => format!("{:?}", e))
            }
        }
    }
}

/// Possible errors for prevalidation
#[derive(Debug, Error)]
pub enum PrevalidationError {
    #[error("Storage read/write error, reason: {error:?}")]
    StorageError { error: StorageError },
    #[error("Protocol service error, reason: {error:?}")]
    ProtocolServiceError { error: ProtocolServiceError },
    #[error("Current mempool storage lock error, reason: {reason:?}")]
    CurrentMempoolStorageLockError { reason: String },
}

impl From<ProtocolServiceError> for PrevalidationError {
    fn from(error: ProtocolServiceError) -> Self {
        PrevalidationError::ProtocolServiceError { error }
    }
}

impl From<StorageError> for PrevalidationError {
    fn from(error: StorageError) -> Self {
        PrevalidationError::StorageError { error }
    }
}

impl<T> From<PoisonError<T>> for PrevalidationError {
    fn from(pe: PoisonError<T>) -> Self {
        PrevalidationError::CurrentMempoolStorageLockError {
            reason: format!("{}", pe),
        }
    }
}

fn process_prevalidation(
    block_storage: &BlockStorage,
    chain_meta_storage: &ChainMetaStorage,
    mempool_storage: &MempoolStorage,
    current_mempool_state_storage: CurrentMempoolStateStorageRef,
    chain_id: &ChainId,
    validator_run: &AtomicBool,
    shell_channel: &ShellChannelRef,
    api: &ProtocolController,
    validator_event_receiver: &mut QueueReceiver<Event>,
    log: &Logger,
) -> Result<(), PrevalidationError> {
    info!(log, "Mempool prevalidator started processing");

    // hydrate state
    hydrate_state(
        &shell_channel,
        block_storage,
        chain_meta_storage,
        mempool_storage,
        current_mempool_state_storage.clone(),
        &api,
        &chain_id,
        &log,
    )?;

    // start receiving event
    while validator_run.load(Ordering::Acquire) {
        // 1. at first let's handle event
        if let Ok(event) = validator_event_receiver.recv() {
            match event {
                Event::NewHead(header) => {
                    // we dont want to reset mempool if header is not changed
                    let process_new_head = match current_mempool_state_storage.read()?.head() {
                        Some(mempool_head) => mempool_head.ne(&header.hash),
                        None => true,
                    };

                    if process_new_head {
                        debug!(log, "Mempool - new head received, so begin construction a new context"; "received_block_hash" => header.hash.to_base58_check());

                        // try to begin construction new context
                        let (prevalidator, head) = begin_construction(
                            &api,
                            &chain_id,
                            header.hash.clone(),
                            header.header.clone(),
                            &log,
                        )?;

                        // reinitialize state for new prevalidator and head
                        let operations_to_delete = current_mempool_state_storage
                            .write()?
                            .reinit(prevalidator, head);

                        // clear unneeded operations from mempool storage
                        operations_to_delete
                            .iter()
                            .for_each(|oph| {
                                if let Err(err) = mempool_storage.delete(&oph) {
                                    warn!(log, "Mempool - delete operation failed"; "hash" => oph.to_base58_check(), "error" => format!("{:?}", err))
                                }
                            });
                    } else {
                        debug!(log, "Mempool - new head received, but was ignored"; "received_block_hash" => header.hash.to_base58_check());
                    }
                }
                Event::ValidateOperation(oph, mempool_operation_type, result_callback) => {
                    // TODO: handling when operation not exists - can happen?
                    if let Some(operation) =
                        mempool_storage.get(mempool_operation_type, oph.clone())?
                    {
                        // TODO: handle and validate pre_filter with operation?

                        // try to add to pendings
                        // let mut state = current_mempool_state_storage.write()?;
                        let was_added_to_pending = current_mempool_state_storage
                            .write()?
                            .add_to_pending(&oph, operation.into());
                        if !was_added_to_pending {
                            debug!(log, "Mempool - received validate operation event - operation already validated"; "hash" => oph.to_base58_check());
                            if let Err(e) = dispatch_condvar_result(
                                result_callback,
                                || {
                                    Err(format_err!("Mempool - received validate operation event - operation already validated, hash: {}", oph.to_base58_check()))
                                },
                                true,
                            ) {
                                warn!(log, "Failed to dispatch result to condvar"; "reason" => format!("{}", e));
                            }
                        } else {
                            if let Err(e) =
                                dispatch_condvar_result(result_callback, || Ok(()), true)
                            {
                                warn!(log, "Failed to dispatch result to condvar"; "reason" => format!("{}", e));
                            }
                        }
                    } else {
                        debug!(log, "Mempool - received validate operation event - operations was previously validated and removed from mempool storage"; "hash" => oph.to_base58_check());
                        if let Err(e) = dispatch_condvar_result(
                            result_callback,
                            || {
                                Err(format_err!("Mempool - received validate operation event - operations was previously validated and removed from mempool storage, hash: {}", oph.to_base58_check()))
                            },
                            true,
                        ) {
                            warn!(log, "Failed to dispatch result to condvar"; "reason" => format!("{}", e));
                        }
                    }
                }
                Event::ShuttingDown => {
                    validator_run.store(false, Ordering::Release);
                }
            }
        }

        // 2. lets handle pending operations (if any)
        handle_pending_operations(
            &shell_channel,
            &api,
            current_mempool_state_storage.clone(),
            &log,
        )?;
    }

    Ok(())
}

fn hydrate_state(
    shell_channel: &ShellChannelRef,
    block_storage: &BlockStorage,
    chain_meta_storage: &ChainMetaStorage,
    mempool_storage: &MempoolStorage,
    current_mempool_state_storage: CurrentMempoolStateStorageRef,
    api: &ProtocolController,
    chain_id: &ChainId,
    log: &Logger,
) -> Result<(), PrevalidationError> {
    // load current head
    let current_head = match chain_meta_storage.get_current_head(&chain_id)? {
        Some(head) => block_storage
            .get(head.block_hash())?
            .map(|header| (head, header.header)),
        None => None,
    };

    // begin construction for a current head
    let (prevalidator, head) = match current_head {
        Some((head, header)) => begin_construction(api, &chain_id, head.into(), header, &log)?,
        None => (None, None),
    };

    // read from Mempool_storage (just pending) -> add to queue for validation -> pending
    let pending = mempool_storage.iter()?;

    // initialize internal mempool state (write lock)
    let mut state = current_mempool_state_storage.write()?;

    // reinit + add old unprocessed pendings
    let _ = state.reinit(prevalidator, head);
    for (oph, op) in pending {
        let _ = state.add_to_pending(&oph, op.into());
    }
    // drop write lock
    drop(state);

    // and process it immediatly on startup, before any event received to clean old stored unprocessed operations
    handle_pending_operations(&shell_channel, &api, current_mempool_state_storage, &log)?;

    Ok(())
}

fn begin_construction(
    api: &ProtocolController,
    chain_id: &ChainId,
    block_hash: BlockHash,
    block_header: Arc<BlockHeader>,
    log: &Logger,
) -> Result<(Option<PrevalidatorWrapper>, Option<BlockHash>), PrevalidationError> {
    // try to begin construction
    let result = match api.begin_construction(BeginConstructionRequest {
        chain_id: chain_id.clone(),
        predecessor: block_header.as_ref().clone(),
        protocol_data: None,
    }) {
        Ok(prevalidator) => (Some(prevalidator), Some(block_hash)),
        Err(pse) => {
            handle_protocol_service_error(
                pse,
                |e| warn!(log, "Mempool - failed to begin construction"; "block_hash" => block_hash.to_base58_check(), "error" => format!("{:?}", e)),
            )?;
            (None, None)
        }
    };
    Ok(result)
}

fn handle_pending_operations(
    shell_channel: &ShellChannelRef,
    api: &ProtocolController,
    current_mempool_state_storage: CurrentMempoolStateStorageRef,
    log: &Logger,
) -> Result<(), PrevalidationError> {
    // check if we can handle something
    let mut state = current_mempool_state_storage.write()?;

    // this destruct mempool_state to be modified under write lock
    let (prevalidator, head, pendings, operations, validation_result) =
        match state.can_handle_pending() {
            Some((prevalidator, head, pendings, operations, validation_result)) => {
                debug!(log, "Mempool - handle_pending_operations"; "pendings" => pendings.len());
                (prevalidator, head, pendings, operations, validation_result)
            }
            None => {
                trace!(
                    log,
                    "Mempool - handle_pending_operations - nothing to handle or no prevalidator"
                );
                return Ok(());
            }
        };

    // lets iterate pendings and validate them
    for pending_op in pendings.drain().into_iter() {
        // handle validation
        match operations.get(&pending_op) {
            Some(operation) => {
                trace!(log, "Mempool - lets validate "; "hash" => pending_op.to_base58_check());

                // lets validate throught protocol
                match api.validate_operation(ValidateOperationRequest {
                    prevalidator: prevalidator.clone(),
                    operation: operation.clone(),
                }) {
                    Ok(response) => {
                        debug!(log, "Mempool - validate operation response finished with success"; "hash" => pending_op.to_base58_check(), "result" => format!("{:?}", response.result));

                        // merge new result with existing one
                        let _ = validation_result.merge(response.result);

                        // TODO: handle Duplicate/ Outdated - if result is empty
                        // TODO: handle result like ocaml - branch_delayed (is_endorsement) add back to pending and so on - check handle_unprocessed
                    }
                    Err(pse) => {
                        handle_protocol_service_error(
                            pse,
                            |e| warn!(log, "Mempool - failed to validate operation message"; "hash" => pending_op.to_base58_check(), "error" => format!("{:?}", e)),
                        )?

                        // TODO: create custom error and add to refused or just revalidate (retry algorithm?)
                    }
                }
            }
            None => {
                warn!(log, "Mempool - missing operation in mempool state (should not happen)"; "hash" => pending_op.to_base58_check())
            }
        }
    }

    advertise_new_mempool(
        &shell_channel,
        prevalidator,
        head,
        (&validation_result.applied, &pendings),
    );

    Ok(())
}

/// Notify other actors that mempool state changed
fn advertise_new_mempool(
    shell_channel: &ShellChannelRef,
    prevalidator: &PrevalidatorWrapper,
    head: &BlockHash,
    (applied, pending): (&Vec<Applied>, &HashSet<OperationHash>),
) {
    // we advertise new mempool, only if we have new applied operations
    if applied.is_empty() {
        return;
    }

    shell_channel.tell(
        Publish {
            msg: ShellChannelMsg::AdvertiseToP2pNewMempool(
                Arc::new(prevalidator.chain_id.clone()),
                Arc::new(head.clone()),
                Arc::new(collect_mempool(applied, pending)),
            ),
            topic: ShellChannelTopic::ShellCommands.into(),
        },
        None,
    );
}
