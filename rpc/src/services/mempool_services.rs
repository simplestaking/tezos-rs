// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, format_err};
use riker::actors::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slog::info;

use crypto::hash::{ChainId, OperationHash, ProtocolHash};
use shell::mempool::mempool_channel::{
    MempoolChannelRef, MempoolChannelTopic, MempoolOperationReceived,
};
use shell::mempool::CurrentMempoolStateStorageRef;
use shell::shell_channel::{
    InjectBlock, RequestCurrentHead, ShellChannelMsg, ShellChannelRef, ShellChannelTopic,
};
use shell::utils::try_wait_for_condvar_result;
use shell::validation;
use storage::mempool_storage::MempoolOperationType;
use storage::{
    BlockHeaderWithHash, BlockMetaStorage, BlockMetaStorageReader, BlockStorage,
    BlockStorageReader, MempoolStorage,
};
use tezos_api::ffi::{Applied, Errored};
use tezos_messages::p2p::binary_message::{BinaryRead, MessageHash};
use tezos_messages::p2p::encoding::operation::DecodedOperation;
use tezos_messages::p2p::encoding::prelude::{BlockHeader, Operation};

use crate::helpers::get_prevalidators;
use crate::server::RpcServiceEnvironment;

const INJECT_BLOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const INJECT_OPERATION_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MempoolOperations {
    pub applied: Vec<HashMap<String, Value>>,
    pub refused: Vec<Value>,
    pub branch_refused: Vec<Value>,
    pub branch_delayed: Vec<Value>,
    // TODO: unprocessed - we dont have protocol data, because we can get it just from ffi now
    pub unprocessed: Vec<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct InjectedBlockWithOperations {
    pub data: String,
    pub operations: Vec<Vec<DecodedOperation>>,
}

pub fn get_pending_operations(
    _chain_id: &ChainId,
    current_mempool_state_storage: CurrentMempoolStateStorageRef,
) -> Result<(MempoolOperations, Option<ProtocolHash>), anyhow::Error> {
    // get actual known state of mempool
    let current_mempool_state = current_mempool_state_storage
        .read()
        .map_err(|e| format_err!("Failed to obtain read lock, reson: {}", e))?;

    // convert to rpc data - we need protocol_hash
    let (mempool_operations, mempool_prevalidator_protocol) = match current_mempool_state
        .prevalidator()
    {
        Some(prevalidator) => {
            let result = current_mempool_state.result();
            let operations = current_mempool_state.operations();
            (
                MempoolOperations {
                    applied: convert_applied(&result.applied, &operations)?,
                    refused: convert_errored(&result.refused, &operations, &prevalidator.protocol)?,
                    branch_refused: convert_errored(
                        &result.branch_refused,
                        &operations,
                        &prevalidator.protocol,
                    )?,
                    branch_delayed: convert_errored(
                        &result.branch_delayed,
                        &operations,
                        &prevalidator.protocol,
                    )?,
                    unprocessed: vec![],
                },
                Some(prevalidator.protocol.clone()),
            )
        }
        None => (MempoolOperations::default(), None),
    };

    Ok((mempool_operations, mempool_prevalidator_protocol))
}

fn convert_applied(
    applied: &Vec<Applied>,
    operations: &HashMap<OperationHash, Operation>,
) -> Result<Vec<HashMap<String, Value>>, anyhow::Error> {
    let mut result: Vec<HashMap<String, Value>> = Vec::new();
    for a in applied {
        let operation_hash = a.hash.to_base58_check();
        let protocol_data: HashMap<String, Value> = serde_json::from_str(&a.protocol_data_json)?;
        let operation = match operations.get(&a.hash) {
            Some(b) => b,
            None => {
                return Err(format_err!(
                    "missing operation data for operation_hash: {}",
                    &operation_hash
                ));
            }
        };

        let mut m = HashMap::new();
        m.insert(String::from("hash"), Value::String(operation_hash));
        m.insert(
            String::from("branch"),
            Value::String(operation.branch().to_base58_check()),
        );
        m.extend(protocol_data);
        result.push(m);
    }

    Ok(result)
}

fn convert_errored(
    errored: &Vec<Errored>,
    operations: &HashMap<OperationHash, Operation>,
    protocol: &ProtocolHash,
) -> Result<Vec<Value>, anyhow::Error> {
    let mut result: Vec<Value> = Vec::new();
    let protocol = protocol.to_base58_check();

    for e in errored {
        let operation_hash = e.hash.to_base58_check();
        let operation = match operations.get(&e.hash) {
            Some(b) => b,
            None => {
                return Err(format_err!(
                    "missing operation data for operation_hash: {}",
                    &operation_hash
                ));
            }
        };

        let protocol_data: HashMap<String, Value> = if e
            .protocol_data_json_with_error_json
            .protocol_data_json
            .is_empty()
        {
            HashMap::new()
        } else {
            serde_json::from_str(&e.protocol_data_json_with_error_json.protocol_data_json)?
        };

        let error = if e.protocol_data_json_with_error_json.error_json.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&e.protocol_data_json_with_error_json.error_json)?
        };

        let mut m = HashMap::new();
        m.insert(String::from("protocol"), Value::String(protocol.clone()));
        m.insert(
            String::from("branch"),
            Value::String(operation.branch().to_base58_check()),
        );
        m.extend(protocol_data);
        m.insert(String::from("error"), error);

        result.push(Value::Array(vec![
            Value::String(operation_hash),
            serde_json::to_value(m)?,
        ]));
    }

    Ok(result)
}

pub fn inject_operation(
    is_async: bool,
    chain_id: ChainId,
    operation_data: &str,
    env: &RpcServiceEnvironment,
    mempool_channel: &MempoolChannelRef,
) -> Result<String, anyhow::Error> {
    info!(env.log(),
          "Operation injection requested";
          "chain_id" => chain_id.to_base58_check(),
          "is_async" => is_async,
          "operation_data" => operation_data,
    );

    let start_request = Instant::now();

    let persistent_storage = env.persistent_storage();
    let block_storage: Box<dyn BlockStorageReader> =
        Box::new(BlockStorage::new(persistent_storage));
    let block_meta_storage: Box<dyn BlockMetaStorageReader> =
        Box::new(BlockMetaStorage::new(persistent_storage));

    // find prevalidator for chain_id, if not found, then stop
    if get_prevalidators(env, Some(&chain_id))?.is_empty() {
        bail!("Prevalidator is not running, cannot inject the operation.");
    }

    // parse operation data
    let operation: Operation = Operation::from_bytes(hex::decode(operation_data)?)?;
    let operation_hash = operation.message_typed_hash()?;

    // do prevalidation before add the operation to mempool
    let result = validation::prevalidate_operation(
        &chain_id,
        &operation_hash,
        &operation,
        env.current_mempool_state_storage().clone(),
        &env.tezos_readonly_prevalidation_api().pool.get()?.api,
        &block_storage,
        &block_meta_storage,
    )?;

    // can accpect operation ?
    if !validation::can_accept_operation_from_rpc(&operation_hash, &result) {
        return Err(format_err!(
            "Operation from rpc ({}) was not added to mempool. Reason: {:?}",
            operation_hash.to_base58_check(),
            result
        ));
    }

    // store operation in mempool storage
    let mut mempool_storage = MempoolStorage::new(persistent_storage);
    let operation_hash_b58check_string = operation_hash.to_base58_check();
    mempool_storage.put(MempoolOperationType::Pending, operation.into())?;

    // callback will wait all the asynchonous processing to finish, and then returns rpc response
    let result_callback = if is_async {
        // if async no wait
        None
    } else {
        // if not async, means sync and we wait
        Some(Arc::new((Mutex::new(None), Condvar::new())))
    };

    let start_async = Instant::now();

    // ping mempool with new operation for mempool validation
    mempool_channel.tell(
        Publish {
            msg: MempoolOperationReceived {
                operation_hash,
                operation_type: MempoolOperationType::Pending,
                result_callback: result_callback.clone(),
            }
            .into(),
            topic: MempoolChannelTopic.into(),
        },
        None,
    );

    // wait for result
    if let Some(result_callback) = result_callback {
        let wait_result =
            try_wait_for_condvar_result(result_callback, INJECT_OPERATION_WAIT_TIMEOUT).map_err(
                |e| {
                    format_err!(
                        "Operation injection - failed to inject, operation_hash: {}, reason: {:?}!",
                        &operation_hash_b58check_string,
                        e
                    )
                },
            )?;
        match wait_result {
            Ok(_) => {
                info!(env.log(),
                      "Operation injected";
                      "operation_hash" => &operation_hash_b58check_string,
                      "elapsed" => format!("{:?}", start_request.elapsed()),
                      "elapsed_async" => format!("{:?}", start_async.elapsed()),
                );
            }
            Err(e) => {
                return Err(format_err!(
                    "Operation injection - error received, operation_hash: {}, reason: {}!",
                    &operation_hash_b58check_string,
                    e
                ));
            }
        };
    }

    Ok(operation_hash_b58check_string)
}

pub fn inject_block(
    is_async: bool,
    chain_id: ChainId,
    injection_data: &str,
    env: &RpcServiceEnvironment,
    shell_channel: &ShellChannelRef,
) -> Result<String, anyhow::Error> {
    let block_with_op: InjectedBlockWithOperations = serde_json::from_str(injection_data)?;
    let chain_id = Arc::new(chain_id);

    let start_request = Instant::now();

    let header: BlockHeaderWithHash =
        BlockHeader::from_bytes(hex::decode(block_with_op.data)?)?.try_into()?;
    let block_hash_b58check_string = header.hash.to_base58_check();
    info!(env.log(),
          "Block injection requested";
          "block_hash" => block_hash_b58check_string.clone(),
          "chain_id" => chain_id.to_base58_check(),
          "is_async" => is_async,
    );

    // special case for block on level 1 - has 0 validation passes
    let validation_passes: Option<Vec<Vec<Operation>>> = if header.header.validation_pass() > 0 {
        Some(
            block_with_op
                .operations
                .into_iter()
                .map(|validation_pass| {
                    validation_pass
                        .into_iter()
                        .map(|op| op.try_into())
                        .collect::<Result<_, _>>()
                })
                .collect::<Result<_, _>>()?,
        )
    } else {
        None
    };

    // clean actual mempool_state - just applied should be enough
    if let Some(validation_passes) = &validation_passes {
        let mut current_mempool_state = env
            .current_mempool_state_storage()
            .write()
            .map_err(|e| format_err!("Failed to obtain write lock, reason: {}", e))?;

        for vps in validation_passes {
            for vp in vps {
                let oph: OperationHash = vp.message_typed_hash()?;
                current_mempool_state.remove_operation(oph);
            }
        }
    }

    // compute the paths for each validation passes
    let paths = if let Some(vps) = validation_passes.as_ref() {
        let response = env
            .tezos_without_context_api()
            .pool
            .get()?
            .api
            .compute_path(vps.try_into()?)?;
        Some(response.operations_hashes_path)
    } else {
        None
    };

    // callback will wait all the asynchonous processing to finish, and then returns rpc response
    let result_callback = if is_async {
        // if async no wait
        None
    } else {
        // if not async, means sync and we wait
        Some(Arc::new((Mutex::new(None), Condvar::new())))
    };

    let start_async = Instant::now();

    // notify other actors, that a block was injected
    shell_channel.tell(
        Publish {
            msg: ShellChannelMsg::InjectBlock(
                InjectBlock {
                    chain_id: chain_id.clone(),
                    block_header: Arc::new(header),
                    operations: validation_passes,
                    operation_paths: paths,
                },
                result_callback.clone(),
            ),
            topic: ShellChannelTopic::ShellCommands.into(),
        },
        None,
    );

    // wait for result
    if let Some(result_callback) = result_callback {
        let wait_result = try_wait_for_condvar_result(result_callback, INJECT_BLOCK_WAIT_TIMEOUT)
            .map_err(|e| {
            format_err!(
                "Block injection - failed to inject, block_hash: {}, reason: {:?}!",
                &block_hash_b58check_string,
                e
            )
        })?;
        match wait_result {
            Ok(_) => {
                info!(env.log(),
                      "Block injected";
                      "block_hash" => block_hash_b58check_string.clone(),
                      "chain_id" => chain_id.to_base58_check(),
                      "elapsed" => format!("{:?}", start_request.elapsed()),
                      "elapsed_async" => format!("{:?}", start_async.elapsed()),
                );
            }
            Err(e) => {
                return Err(format_err!(
                    "Block injection - error received, block_hash: {}, reason: {}!",
                    &block_hash_b58check_string,
                    e
                ));
            }
        };
    }

    // return the block hash to the caller
    Ok(block_hash_b58check_string)
}

pub fn request_operations(shell_channel: ShellChannelRef) -> Result<(), anyhow::Error> {
    // request current head from the peers
    shell_channel.tell(
        Publish {
            msg: RequestCurrentHead.into(),
            topic: ShellChannelTopic::ShellCommands.into(),
        },
        None,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::TryInto};

    use assert_json_diff::assert_json_eq;
    use serde_json::json;

    use tezos_api::ffi::{Applied, Errored, OperationProtocolDataJsonWithErrorListJson};
    use tezos_messages::p2p::binary_message::BinaryRead;
    use tezos_messages::p2p::encoding::prelude::Operation;

    use crate::services::mempool_services::{convert_applied, convert_errored};

    #[test]
    fn test_convert_applied() -> Result<(), anyhow::Error> {
        let data = vec![
            Applied {
                hash: "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ".try_into()?,
                protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
            }
        ];

        let mut operations = HashMap::new();
        // operation with branch=BKqTKfGwK3zHnVXX33X5PPHy1FDTnbkajj3eFtCXGFyfimQhT1H
        operations.insert(
            "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ".try_into()?,
            Operation::from_bytes(hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08")?)?,
        );

        let expected_json = json!(
            [
                {
                    "hash" : "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ",
                    "branch" : "BKqTKfGwK3zHnVXX33X5PPHy1FDTnbkajj3eFtCXGFyfimQhT1H",
                    "contents": [{ "kind": "endorsement", "level": 459020 } ],
                    "signature": "siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9"
                }
            ]
        );

        // convert
        let result = convert_applied(&data, &operations)?;
        assert_json_eq!(
            serde_json::to_value(result)?,
            serde_json::to_value(expected_json)?
        );

        Ok(())
    }

    #[test]
    fn test_convert_errored() -> Result<(), anyhow::Error> {
        let data = vec![
            Errored {
                hash: "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ".try_into()?,
                is_endorsement: None,
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "{ \"contents\": [ { \"kind\": \"endorsement\", \"level\": 459020 } ],\n  \"signature\":\n    \"siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9\" }".to_string(),
                    error_json: "[ { \"kind\": \"temporary\",\n    \"id\": \"proto.005-PsBabyM1.operation.wrong_endorsement_predecessor\",\n    \"expected\": \"BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL\",\n    \"provided\": \"BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4\" } ]".to_string(),
                },
            }
        ];

        let mut operations = HashMap::new();
        // operation with branch=BKqTKfGwK3zHnVXX33X5PPHy1FDTnbkajj3eFtCXGFyfimQhT1H
        operations.insert(
            "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ".try_into()?,
            Operation::from_bytes(hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08")?)?,
        );
        let protocol = "PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb".try_into()?;

        let expected_json = json!(
                [
                    [
                        "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ",
                        {
                            "protocol" : "PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb",
                            "branch" : "BKqTKfGwK3zHnVXX33X5PPHy1FDTnbkajj3eFtCXGFyfimQhT1H",
                            "contents": [{ "kind": "endorsement", "level": 459020}],
                            "signature": "siguKbKFVDkXo2m1DqZyftSGg7GZRq43EVLSutfX5yRLXXfWYG5fegXsDT6EUUqawYpjYE1GkyCVHfc2kr3hcaDAvWSAhnV9",
                            "error" : [ { "kind": "temporary", "id": "proto.005-PsBabyM1.operation.wrong_endorsement_predecessor", "expected": "BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL", "provided": "BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4" } ]
                        }
                    ]
                ]
        );

        // convert
        let result = convert_errored(&data, &operations, &protocol)?;
        assert_json_eq!(
            serde_json::to_value(result)?,
            serde_json::to_value(expected_json)?
        );

        Ok(())
    }

    #[test]
    fn test_convert_errored_missing_protocol_data() -> Result<(), anyhow::Error> {
        let data = vec![
            Errored {
                hash: "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ".try_into()?,
                is_endorsement: Some(true),
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "".to_string(),
                    error_json: "[ { \"kind\": \"temporary\",\n    \"id\": \"proto.005-PsBabyM1.operation.wrong_endorsement_predecessor\",\n    \"expected\": \"BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL\",\n    \"provided\": \"BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4\" } ]".to_string(),
                },
            }
        ];

        let mut operations = HashMap::new();
        // operation with branch=BKqTKfGwK3zHnVXX33X5PPHy1FDTnbkajj3eFtCXGFyfimQhT1H
        operations.insert(
            "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ".try_into()?,
            Operation::from_bytes(hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08")?)?,
        );
        let protocol = "PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb".try_into()?;

        let expected_json = json!(
                [
                    [
                        "onvN8U6QJ6DGJKVYkHXYRtFm3tgBJScj9P5bbPjSZUuFaGzwFuJ",
                        {
                            "protocol" : "PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb",
                            "branch" : "BKqTKfGwK3zHnVXX33X5PPHy1FDTnbkajj3eFtCXGFyfimQhT1H",
                            "error" : [ { "kind": "temporary", "id": "proto.005-PsBabyM1.operation.wrong_endorsement_predecessor", "expected": "BMDb9PfcJmiibDDEbd6bEEDj4XNG4C7QACG6TWqz29c9FxNgDLL", "provided": "BLd8dLs4X5Ve6a8B37kUu7iJkRycWzfSF5MrskY4z8YaideQAp4" } ]
                        }
                    ]
                ]
        );

        // convert
        let result = convert_errored(&data, &operations, &protocol)?;
        assert_json_eq!(
            serde_json::to_value(result)?,
            serde_json::to_value(expected_json)?
        );

        Ok(())
    }
}
