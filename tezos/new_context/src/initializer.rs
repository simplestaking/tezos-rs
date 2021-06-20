// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::{Arc, RwLock};

use ipc::IpcError;
use ocaml_interop::BoxRoot;
pub use tezos_api::ffi::ContextKvStoreConfiguration;
use tezos_api::ffi::TezosContextTezEdgeStorageConfiguration;
use thiserror::Error;

use crate::gc::mark_move_gced::MarkMoveGCed;
use crate::kv_store::readonly_ipc::ReadonlyIpcBackend;
use crate::kv_store::{btree_map::BTreeMapBackend, in_memory_backend::InMemoryBackend};
use crate::{PatchContextFunction, TezedgeContext, TezedgeIndex};

// TODO: should this be here?
const PRESERVE_CYCLE_COUNT: usize = 7;

/// IPC communication errors
#[derive(Debug, Error)]
pub enum IndexInitializationError {
    #[error("Failure when initializing IPC context: {reason}")]
    IpcError { reason: IpcError },
    #[error("Attempted to initialize an IPC context without a socket path")]
    IpcSocketPathMissing,
}

impl From<IpcError> for IndexInitializationError {
    fn from(error: IpcError) -> Self {
        Self::IpcError { reason: error }
    }
}

pub fn initialize_tezedge_index(
    configuration: &TezosContextTezEdgeStorageConfiguration,
    patch_context: Option<BoxRoot<PatchContextFunction>>,
) -> Result<TezedgeIndex, IndexInitializationError> {
    Ok(TezedgeIndex::new(
        match configuration.backend {
            ContextKvStoreConfiguration::ReadOnlyIpc => {
                match configuration.ipc_socket_path.clone() {
                    None => return Err(IndexInitializationError::IpcSocketPathMissing),
                    Some(ipc_socket_path) => Arc::new(RwLock::new(
                        ReadonlyIpcBackend::try_connect(ipc_socket_path)?,
                    )),
                }
            }
            ContextKvStoreConfiguration::InMem => Arc::new(RwLock::new(InMemoryBackend::new())),
            ContextKvStoreConfiguration::BTreeMap => Arc::new(RwLock::new(BTreeMapBackend::new())),
            ContextKvStoreConfiguration::InMemGC => {
                Arc::new(RwLock::new(MarkMoveGCed::<InMemoryBackend>::new(
                    PRESERVE_CYCLE_COUNT,
                )))
            }
        },
        patch_context,
    ))
}

pub fn initialize_tezedge_context(
    configuration: &TezosContextTezEdgeStorageConfiguration,
) -> Result<TezedgeContext, IndexInitializationError> {
    let index = initialize_tezedge_index(configuration, None)?;
    Ok(TezedgeContext::new(index, None, None))
}
