// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::Arc;

use crate::TezosApiConnectionPool;
use crypto::hash::ContextHash;
use tezos_new_context::{ContextError, ContextKeyOwned, ContextValue, StringTreeEntry};
use thiserror::Error;

#[derive(Clone)]
pub struct TezedgeContextClient {
    tezos_readonly_api: Arc<TezosApiConnectionPool>,
}

#[derive(Debug, Error)]
pub enum TezedgeContextClientError {
    #[error("Context error: {error:?}")]
    ContextError { error: ContextError },
    #[error("Pool error: {error:?}")]
    PoolError { error: crate::InternalPoolError },
    #[error("Protocol service error: {error:?}")]
    ProtocolServiceError {
        error: crate::service::ProtocolServiceError,
    },
}

impl From<ContextError> for TezedgeContextClientError {
    fn from(error: ContextError) -> Self {
        TezedgeContextClientError::ContextError { error }
    }
}

impl From<crate::InternalPoolError> for TezedgeContextClientError {
    fn from(error: crate::InternalPoolError) -> Self {
        TezedgeContextClientError::PoolError { error }
    }
}

impl From<crate::service::ProtocolServiceError> for TezedgeContextClientError {
    fn from(error: crate::service::ProtocolServiceError) -> Self {
        TezedgeContextClientError::ProtocolServiceError { error }
    }
}

impl TezedgeContextClient {
    pub fn new(tezos_readonly_api: Arc<TezosApiConnectionPool>) -> Self {
        Self { tezos_readonly_api }
    }

    pub fn get_key_from_history(
        &self,
        context_hash: &ContextHash,
        key: ContextKeyOwned,
    ) -> Result<Option<ContextValue>, TezedgeContextClientError> {
        Ok(self
            .tezos_readonly_api
            .pool
            .get()?
            .api
            .get_context_key_from_history(context_hash, key)?)
    }

    pub fn get_key_values_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: ContextKeyOwned,
    ) -> Result<Option<Vec<(ContextKeyOwned, ContextValue)>>, TezedgeContextClientError> {
        Ok(self
            .tezos_readonly_api
            .pool
            .get()?
            .api
            .get_context_key_values_by_prefix(context_hash, prefix)?)
    }

    pub fn get_context_tree_by_prefix(
        &self,
        context_hash: &ContextHash,
        prefix: ContextKeyOwned,
        depth: Option<usize>,
    ) -> Result<StringTreeEntry, TezedgeContextClientError> {
        Ok(self
            .tezos_readonly_api
            .pool
            .get()?
            .api
            .get_context_tree_by_prefix(context_hash, prefix, depth)?)
    }
}
