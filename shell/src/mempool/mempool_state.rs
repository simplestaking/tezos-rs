// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::{HashMap, HashSet};

use crypto::hash::{BlockHash, OperationHash};
use tezos_api::ffi::{Applied, PrevalidatorWrapper, ValidateOperationResult};
use tezos_messages::p2p::encoding::prelude::{Mempool, Operation};

/// Mempool state is defined with mempool and validation_result attributes, which are in sync:
/// - `validation_result`
///     - contains results of all validated operations
///     - also contains `known_valid` operations, which where validated as `applied`
/// - `pending`
///     - operations, which where not validated yet or endorsements (`branch_refused`, `branched_delay`, `refused`?)
///     - are being processed sequentially, after validation, they are moved to `validation_result`
/// - `operations`
///     - kind of cache, contains operation data
#[derive(Debug, Default)]
pub struct MempoolState {
    /// Original tezos prevalidator has prevalidator.fitness which is used for set_head comparision
    /// So, we keep it in-memory here
    prevalidator: Option<PrevalidatorWrapper>,
    predecessor: Option<BlockHash>,

    /// Actual cumulated operation results
    validation_result: ValidateOperationResult,

    /// In-memory store of actual operations
    operations: HashMap<OperationHash, Operation>,
    // TODO: pendings limit
    // TODO: pendings as vec and order
    pending: HashSet<OperationHash>,
}

impl MempoolState {
    /// Reinitialize state for new prevalidator and head, returns unneeded operation hashes
    pub(crate) fn reinit(
        &mut self,
        prevalidator: Option<PrevalidatorWrapper>,
        predecessor: Option<BlockHash>,
    ) -> Vec<OperationHash> {
        // we want to validate pending operations with new prevalidator, so other "already_validated" can be removed
        let unneeded_operations: Vec<OperationHash> = self
            .operations
            .keys()
            .filter(|&key| !self.pending.contains(key))
            .cloned()
            .collect();

        // remove unneeded
        for oph in &unneeded_operations {
            self.operations.remove(oph);
        }
        self.predecessor = predecessor;
        self.prevalidator = prevalidator;
        self.validation_result = ValidateOperationResult::default();

        unneeded_operations
    }

    /// Tries to add operation to pendings.
    /// Returns true - if added, false - if operation was already validated
    pub(crate) fn add_to_pending(
        &mut self,
        operation_hash: &OperationHash,
        operation: Operation,
    ) -> bool {
        if self.is_already_validated(&operation_hash) {
            return false;
        }

        if self.pending.contains(operation_hash) {
            false
        } else {
            self.operations.insert(operation_hash.clone(), operation);
            self.pending.insert(operation_hash.clone())
        }
    }

    /// Removes operation from mempool
    pub fn remove_operation(&mut self, oph: OperationHash) {
        // remove from applied
        if let Some(pos) = self
            .validation_result
            .applied
            .iter()
            .position(|x| oph.eq(&x.hash))
        {
            self.validation_result.applied.remove(pos);
            self.operations.remove(&oph);
        }
        // remove from branch_delayed
        if let Some(pos) = self
            .validation_result
            .branch_delayed
            .iter()
            .position(|x| oph.eq(&x.hash))
        {
            self.validation_result.branch_delayed.remove(pos);
            self.operations.remove(&oph);
        }
        // remove from branch_refused
        if let Some(pos) = self
            .validation_result
            .branch_refused
            .iter()
            .position(|x| oph.eq(&x.hash))
        {
            self.validation_result.branch_refused.remove(pos);
            self.operations.remove(&oph);
        }
        // remove from refused
        if let Some(pos) = self
            .validation_result
            .refused
            .iter()
            .position(|x| oph.eq(&x.hash))
        {
            self.validation_result.refused.remove(pos);
            self.operations.remove(&oph);
        }
        // remove from pending
        if self.pending.contains(&oph) {
            self.pending.remove(&oph);
            self.operations.remove(&oph);
        }
    }

    /// Indicates, that pending operations can be handled
    /// Returns - None, if nothing can be done, or Some(prevalidator, head, pendings, operations) to handle
    pub(crate) fn can_handle_pending(
        &mut self,
    ) -> Option<(
        &PrevalidatorWrapper,
        &BlockHash,
        &mut HashSet<OperationHash>,
        &HashMap<OperationHash, Operation>,
        &mut ValidateOperationResult,
    )> {
        if self.pending.is_empty() {
            return None;
        }

        match self.prevalidator.as_ref() {
            Some(prevalidator) => match self.predecessor.as_ref() {
                Some(head) => Some((
                    &prevalidator,
                    &head,
                    &mut self.pending,
                    &self.operations,
                    &mut self.validation_result,
                )),
                None => None,
            },
            None => None,
        }
    }

    /// Indicates, that the operation was already validated and is in the mempool
    fn is_already_validated(&self, operation_hash: &OperationHash) -> bool {
        if self
            .validation_result
            .applied
            .iter()
            .any(|op| op.hash.eq(operation_hash))
        {
            return true;
        }
        if self
            .validation_result
            .branch_delayed
            .iter()
            .any(|op| op.hash.eq(operation_hash))
        {
            return true;
        }
        if self
            .validation_result
            .branch_refused
            .iter()
            .any(|op| op.hash.eq(operation_hash))
        {
            return true;
        }
        if self
            .validation_result
            .refused
            .iter()
            .any(|op| op.hash.eq(operation_hash))
        {
            return true;
        }
        false
    }

    pub fn is_already_in_mempool(&self, operation_hash: &OperationHash) -> bool {
        self.pending.contains(operation_hash) || self.is_already_validated(operation_hash)
    }

    pub fn prevalidator(&self) -> Option<&PrevalidatorWrapper> {
        self.prevalidator.as_ref()
    }

    pub fn head(&self) -> Option<&BlockHash> {
        self.predecessor.as_ref()
    }

    pub fn result(&self) -> &ValidateOperationResult {
        &self.validation_result
    }

    pub fn operations(&self) -> &HashMap<OperationHash, Operation> {
        &self.operations
    }
}

pub(crate) fn collect_mempool(applied: &Vec<Applied>, pending: &HashSet<OperationHash>) -> Mempool {
    let known_valid = applied
        .iter()
        .cloned()
        .map(|a| a.hash)
        .collect::<Vec<OperationHash>>();

    let pending = pending.iter().cloned().collect::<Vec<OperationHash>>();

    Mempool::new(known_valid, pending)
}

impl From<&MempoolState> for Mempool {
    fn from(mempool_state: &MempoolState) -> Mempool {
        collect_mempool(
            &mempool_state.validation_result.applied,
            &mempool_state.pending,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use tezos_api::ffi::PrevalidatorWrapper;
    use tezos_messages::p2p::binary_message::BinaryRead;
    use tezos_messages::p2p::encoding::prelude::Operation;

    use crate::mempool::MempoolState;

    #[test]
    fn test_state_reinit() -> Result<(), anyhow::Error> {
        let op_hash1 = "opJ4FdKumPfykAP9ZqwY7rNB8y1SiMupt44RqBDMWL7cmb4xbNr".try_into()?;
        let op_hash2 = "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ".try_into()?;

        // init state with two pendings
        let mut state = MempoolState::default();
        state.add_to_pending(
            &op_hash1,
            Operation::from_bytes(hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08")?)?,
        );
        state.add_to_pending(
            &op_hash2,
            Operation::from_bytes(hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08")?)?,
        );
        assert_eq!(2, state.pending.len());
        assert_eq!(2, state.operations.len());

        // no prevalidator/ no head, means nothing to handle
        assert!(state.can_handle_pending().is_none());

        // add header/prevalidator
        let _ = state.reinit(
            Some(PrevalidatorWrapper {
                chain_id: "NetXgtSLGNJvNye".try_into()?,
                protocol: "PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb".try_into()?,
                context_fitness: Some(vec![vec![0, 1], vec![0, 0, 1, 2, 3, 4, 5]]),
            }),
            Some("BLFQ2JjYWHC95Db21cRZC4cgyA1mcXmx1Eg6jKywWy9b8xLzyK9".try_into()?),
        );

        // remove from pending
        let handle_pendings = state.can_handle_pending();
        assert!(handle_pendings.is_some());
        let (_, _head, pendings, ..) = handle_pendings.unwrap();
        assert!(pendings.remove(&op_hash1));

        // reinit state
        let unneeded = state.reinit(None, None);
        assert_eq!(1, state.pending.len());
        assert_eq!(1, state.operations.len());
        assert!(state.pending.contains(&op_hash2));
        assert!(unneeded.contains(&op_hash1));

        // remove operation
        state.remove_operation(op_hash2);
        assert!(state.pending.is_empty());
        assert!(state.operations.is_empty());

        Ok(())
    }
}
