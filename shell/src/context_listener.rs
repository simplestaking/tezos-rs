// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;

use failure::Error;
use riker::actors::*;
use slog::{crit, debug, Logger, warn};

use crypto::hash::HashType;
use storage::{BlockStorage, ContextStorage};
use storage::persistent::{ContextList, ContextMap, PersistentStorage};
use storage::skip_list::Bucket;
use tezos_context::channel::ContextAction;
use tezos_wrapper::service::IpcEvtServer;

type SharedJoinHandle = Arc<Mutex<Option<JoinHandle<Result<(), Error>>>>>;

#[actor]
pub struct ContextListener {
    /// Thread where blocks are applied will run until this is set to `false`
    listener_run: Arc<AtomicBool>,
    /// Context event listener thread
    listener_thread: SharedJoinHandle,
}

pub type ContextListenerRef = ActorRef<ContextListenerMsg>;

impl ContextListener {
    pub fn actor(sys: &impl ActorRefFactory, persistent_storage: &PersistentStorage, mut event_server: IpcEvtServer, log: Logger) -> Result<ContextListenerRef, CreateError> {
        let storage = persistent_storage.context_storage();
        let listener_run = Arc::new(AtomicBool::new(true));
        let block_applier_thread = {
            let listener_run = listener_run.clone();
            let persistent_storage = persistent_storage.clone();

            thread::spawn(move || {
                let mut context_storage = ContextStorage::new(&persistent_storage);
                let mut block_storage = BlockStorage::new(&persistent_storage);
                while listener_run.load(Ordering::Acquire) {
                    match listen_protocol_events(
                        &listener_run,
                        &mut event_server,
                        &mut context_storage,
                        &mut block_storage,
                        storage.clone(),
                        &log,
                    ) {
                        Ok(()) => debug!(log, "Context listener finished"),
                        Err(err) => {
                            if listener_run.load(Ordering::Acquire) {
                                warn!(log, "Timeout while waiting for context event connection"; "reason" => format!("{:?}", err))
                            }
                        }
                    }
                }

                Ok(())
            })
        };

        let myself = sys.actor_of(
            Props::new_args(ContextListener::new, (listener_run, Arc::new(Mutex::new(Some(block_applier_thread))))),
            ContextListener::name())?;

        Ok(myself)
    }

    /// The `ContextListener` is intended to serve as a singleton actor so that's why
    /// we won't support multiple names per instance.
    fn name() -> &'static str {
        "context-listener"
    }

    fn new((block_applier_run, block_applier_thread): (Arc<AtomicBool>, SharedJoinHandle)) -> Self {
        ContextListener {
            listener_run: block_applier_run,
            listener_thread: block_applier_thread,
        }
    }
}

impl Actor for ContextListener {
    type Msg = ContextListenerMsg;

    fn post_stop(&mut self) {
        self.listener_run.store(false, Ordering::Release);

        let _ = self.listener_thread.lock().unwrap()
            .take().expect("Thread join handle is missing")
            .join().expect("Failed to join context listener thread");
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        self.receive(ctx, msg, sender);
    }
}

fn listen_protocol_events(
    apply_block_run: &AtomicBool,
    event_server: &mut IpcEvtServer,
    context_storage: &mut ContextStorage,
    block_storage: &mut BlockStorage,
    storage: ContextList,
    log: &Logger,
) -> Result<(), Error> {
    debug!(log, "Waiting for connection from protocol runner");
    let mut rx = event_server.accept()?;
    debug!(log, "Received connection from protocol runner. Starting to process context events.");

    let mut event_count = 0;
    let mut state: ContextMap = Default::default();
    let mut blocks: HashMap<Vec<u8>, ContextMap> = Default::default();
    while apply_block_run.load(Ordering::Acquire) {
        match rx.receive() {
            Ok(ContextAction::Shutdown) => break,
            Ok(msg) => {
                if event_count % 100 == 0 {
                    debug!(log, "Received protocol event"; "count" => event_count);
                }
                event_count += 1;

                match &msg {
                    ContextAction::Set { block_hash: Some(block_hash), key, value, .. } => {
                        let state = get_default(&mut blocks, block_hash.clone());
                        state.insert(key.join("/"), Bucket::Exists(value.clone()));

                        context_storage.put_action(&block_hash.clone(), msg)?;
                    }
                    ContextAction::Copy { block_hash: Some(block_hash), to_key: key, from_key, .. } => {
                        let partial_state = get_default(&mut blocks, block_hash.clone());

                        let from_key = from_key.join("/");
                        let to_key = key.join("/");
                        if let Some(value) = partial_state.get(&from_key) {
                            let value = value.clone();
                            state.insert(to_key, value);
                        } else if let Some(value) = state.get(&from_key) {
                            let value = value.clone();
                            state.insert(to_key, value);
                        } else {
                            warn!(log, "Trying to copy from non-existent location"; "from" => &from_key, "to" => &to_key);
                            state.insert(to_key, Bucket::Invalid);
                        }

                        context_storage.put_action(&block_hash.clone(), msg)?;
                    }
                    ContextAction::Delete { block_hash: Some(block_hash), key, .. }
                    | ContextAction::RemoveRecord { block_hash: Some(block_hash), key, .. } => {
                        let state = get_default(&mut blocks, block_hash.clone());
                        state.insert(key.join("/"), Bucket::Deleted);

                        context_storage.put_action(&block_hash.clone(), msg)?;
                    }
                    ContextAction::Commit { new_context_hash, block_hash: Some(block_hash), .. } => {
                        if let Some(block) = blocks.get(block_hash) {
                            let mut writer = storage.write().expect("lock poisoning");
                            writer.push(block.clone())?;
                        } else {
                            crit!(log, "Trying to commit non-existent block"; "block" => HashType::BlockHash.bytes_to_string(block_hash));
                        }
                        blocks.clear();
                        block_storage.assign_to_context(block_hash, new_context_hash)?
                    }
                    ContextAction::Checkout { context_hash: _, .. } => {
                        /**/
                    }
                    ContextAction::Mem { block_hash: Some(block_hash), .. }
                    | ContextAction::DirMem { block_hash: Some(block_hash), .. }
                    | ContextAction::Get { block_hash: Some(block_hash), .. }
                    | ContextAction::Fold { block_hash: Some(block_hash), .. } => {
                        let key = block_hash.clone();
                        context_storage.put_action(&key, msg)?;
                    }
                    _ => (),
                };
            }
            Err(err) => {
                warn!(log, "Failed to receive event from protocol runner"; "reason" => format!("{:?}", err));
                break;
            }
        }
    }

    Ok(())
}

/// Get value contained in the dictionary, or create new entry associated with given key
/// populated with default value
fn get_default(state: &mut HashMap<Vec<u8>, ContextMap>, block_hash: Vec<u8>) -> &mut ContextMap {
    state.entry(block_hash).or_insert(Default::default())
}