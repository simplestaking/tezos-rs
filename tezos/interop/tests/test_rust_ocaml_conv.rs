// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]
extern crate test;

use std::convert::{TryFrom, TryInto};

use crypto::hash::{BlockHash, ChainId, HashType, OperationHash, ProtocolHash};
use ocaml_interop::{ocaml_call, ocaml_frame, to_ocaml, OCaml, ToOCaml, ToRust};
use serial_test::serial;

use tezos_api::{
    ffi::BeginConstructionRequest,
    ffi::PrevalidatorWrapper,
    ffi::ProtocolRpcRequest,
    ffi::RpcMethod,
    ffi::RpcRequest,
    ffi::RustBytes,
    ffi::ValidateOperationRequest,
    ffi::{ApplyBlockRequest, ApplyBlockRequestBuilder},
    ocaml_conv::FfiBlockHeader,
    ocaml_conv::FfiOperation,
};
use tezos_interop::runtime;
use tezos_messages::p2p::{
    binary_message::BinaryMessage, encoding::block_header::BlockHeader,
    encoding::operation::Operation, encoding::operations_for_blocks::OperationsForBlock,
    encoding::operations_for_blocks::OperationsForBlocksMessage,
    encoding::operations_for_blocks::Path,
};

const CHAIN_ID: &str = "8eceda2f";
const HEADER: &str = "0000000301a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000005c017ed604dfcb6b41e91650bb908618b2740a6167d9072c3230e388b24feeef04c98dc27f000000110000000100000000080000000000000005f06879947f3d9959090f27054062ed23dbf9f7bd4b3c8a6e86008daabb07913e000c00000003e5445371002b9745d767d7f164a39e7f373a0f25166794cba491010ab92b0e281b570057efc78120758ff26a33301870f361d780594911549bcb7debbacd8a142e0b76a605";
const HEADER_HASH: &str = "61e687e852460b28f0f9540ccecf8f6cf87a5ad472c814612f0179caf4b9f673";
const OPERATION_HASH: &str = "7e73e3da041ea251037af062b7bc04b37a5ee38bc7e229e7e20737071ed73af4";
const OPERATION: &str = "a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000000236663bacdca76094fdb73150092659d463fec94eda44ba4db10973a1ad057ef53a5b3239a1b9c383af803fc275465bd28057d68f3cab46adfd5b2452e863ff0a";
const MAX_OPERATIONS_TTL: i32 = 5;

mod tezos_ffi {
    use ocaml_interop::{ocaml, OCamlBytes, OCamlInt, OCamlInt32, OCamlInt64, OCamlList};

    use tezos_api::{
        ffi::ApplyBlockRequest,
        ffi::BeginConstructionRequest,
        ffi::PrevalidatorWrapper,
        ffi::ProtocolRpcRequest,
        ffi::RpcMethod,
        ffi::RpcRequest,
        ffi::ValidateOperationRequest,
        ocaml_conv::OCamlBlockHash,
        ocaml_conv::OCamlContextHash,
        ocaml_conv::OCamlOperationHash,
        ocaml_conv::OCamlOperationListListHash,
        ocaml_conv::{OCamlChainId, OCamlProtocolHash},
    };
    use tezos_messages::p2p::encoding::prelude::{BlockHeader, Operation};

    ocaml! {
        pub fn construct_and_compare_hash(operation_hash: OCamlOperationHash, hash_bytes: OCamlBytes) -> bool;
        pub fn construct_and_compare_block_header(
            block_header: BlockHeader,
            shell_params1: (
                /* level: */ OCamlInt32,
                /* proto_level: */ OCamlInt,
                /* validation_passes: */ OCamlInt,
                /* timestamp: */ OCamlInt64,
            ),
            predecessor:  OCamlBlockHash,
            operations_hash: OCamlOperationListListHash,
            fitness: OCamlList<OCamlBytes>,
            context: OCamlContextHash,
            protocol_data: OCamlBytes,
        ) -> bool;
        pub fn construct_and_compare_apply_block_request(
            apply_block_request: ApplyBlockRequest,
            chain_id: OCamlChainId,
            block_header: BlockHeader,
            pred_header: BlockHeader,
            max_operations_ttl: OCamlInt,
            operations: OCamlList<OCamlList<Operation>>
        ) -> bool;
        pub fn construct_and_compare_begin_construction_request(
            begin_construction_request: BeginConstructionRequest,
            chain_id: OCamlChainId,
            predecessor: BlockHeader,
            protocol_data: Option<OCamlBytes>,
        ) -> bool;
        pub fn construct_and_compare_validate_operation_request(
            validate_operation_request: ValidateOperationRequest,
            prevalidator: PrevalidatorWrapper,
            operation: Operation,
        ) -> bool;
        pub fn construct_and_compare_rpc_request(
            rpc_request: RpcRequest,
            body: OCamlBytes,
            context_path: OCamlBytes,
            meth: RpcMethod,
            content_type: Option<OCamlBytes>,
            accept: Option<OCamlBytes>,
        ) -> bool;
        pub fn construct_and_compare_protocol_rpc_request(
            protocol_rpc_request: ProtocolRpcRequest,
            block_header: BlockHeader,
            chain_id: OCamlChainId,
            chain_arg: OCamlBytes,
            request: RpcRequest,
        ) -> bool;
        pub fn construct_and_compare_operation(
            operation: Operation,
            branch: OCamlBlockHash,
            proto: OCamlBytes,
        ) -> bool;
        pub fn construct_and_compare_prevalidator_wrapper(
            prevalidator_wrapper: PrevalidatorWrapper,
            chain_id: OCamlChainId,
            protocol: OCamlProtocolHash,
            context_fitness: Option<OCamlList<OCamlBytes>>,
        ) -> bool;
    }
}

fn block_operations_from_hex(
    block_hash: &str,
    hex_operations: Vec<Vec<RustBytes>>,
) -> Vec<OperationsForBlocksMessage> {
    hex_operations
        .into_iter()
        .map(|bo| {
            let ops = bo
                .into_iter()
                .map(|op| Operation::from_bytes(op).unwrap())
                .collect();
            OperationsForBlocksMessage::new(
                OperationsForBlock::new(
                    BlockHash::try_from(hex::decode(block_hash).unwrap()).unwrap(),
                    4,
                ),
                Path::Op,
                ops,
            )
        })
        .collect()
}

fn sample_operations_for_request_decoded() -> Vec<Vec<RustBytes>> {
    vec![
        vec![hex::decode(OPERATION).unwrap()],
        vec![],
        vec![],
        vec![hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08").unwrap()],
        vec![]
    ]
}

#[test]
#[serial]
fn test_hash_conv() {
    let operation_hash = OperationHash::try_from(hex::decode(OPERATION_HASH).unwrap()).unwrap();

    let result: bool = runtime::execute(move || {
        ocaml_frame!(gc(hash_root), {
            let hash = to_ocaml!(gc, operation_hash, hash_root);
            let hash_bytes = to_ocaml!(gc, operation_hash.as_ref());
            ocaml_call!(tezos_ffi::construct_and_compare_hash(
                gc,
                gc.get(&hash),
                hash_bytes
            ))
            .unwrap()
            .to_rust()
        })
    })
    .unwrap();

    assert!(result, "OperationHash conversion failed")
}

#[test]
#[serial]
fn test_block_header_conv() {
    let block_header = BlockHeader::from_bytes(hex::decode(HEADER).unwrap()).unwrap();

    let result: bool = runtime::execute(move || {
        ocaml_frame!(
            gc(
                shell_params1_root,
                predecessor_root,
                operations_hash_root,
                fitness_root,
                context_root,
                protocol_data_root
            ),
            {
                // Bundle a bunch of the params because of the GC frame keep limit
                let shell_params1 = (
                    block_header.level(),
                    block_header.proto() as i32,
                    block_header.validation_pass() as i32,
                    block_header.timestamp(),
                );
                let shell_params1 = to_ocaml!(gc, shell_params1, shell_params1_root);
                let predecessor = to_ocaml!(gc, block_header.predecessor(), predecessor_root);
                let operations_hash =
                    to_ocaml!(gc, block_header.operations_hash(), operations_hash_root);
                let fitness = to_ocaml!(gc, block_header.fitness(), fitness_root);
                let context = to_ocaml!(gc, block_header.context(), context_root);
                let protocol_data = to_ocaml!(gc, block_header.protocol_data(), protocol_data_root);
                let block_header = to_ocaml!(gc, FfiBlockHeader::from(&block_header));

                ocaml_call!(tezos_ffi::construct_and_compare_block_header(
                    gc,
                    block_header,
                    gc.get(&shell_params1),
                    gc.get(&predecessor),
                    gc.get(&operations_hash),
                    gc.get(&fitness),
                    gc.get(&context),
                    gc.get(&protocol_data),
                ))
                .unwrap()
                .to_rust()
            }
        )
    })
    .unwrap();

    assert!(result, "BlockHeader conversion failed")
}

#[test]
#[serial]
fn test_apply_block_request_conv() {
    let request: ApplyBlockRequest = ApplyBlockRequestBuilder::default()
        .chain_id(ChainId::try_from(hex::decode(CHAIN_ID).unwrap()).unwrap())
        .block_header(BlockHeader::from_bytes(hex::decode(HEADER).unwrap()).unwrap())
        .pred_header(BlockHeader::from_bytes(hex::decode(HEADER).unwrap()).unwrap())
        .max_operations_ttl(MAX_OPERATIONS_TTL)
        .operations(ApplyBlockRequest::convert_operations(
            block_operations_from_hex(HEADER_HASH, sample_operations_for_request_decoded()),
        ))
        .predecessor_block_metadata_hash(None)
        .predecessor_ops_metadata_hash(None)
        .build()
        .unwrap();

    let result: bool = runtime::execute(move || {
        let ffi_operations: Vec<Vec<FfiOperation>> = request
            .operations
            .iter()
            .map(|ops| ops.iter().map(FfiOperation::from).collect())
            .collect();

        ocaml_frame!(
            gc(
                apply_block_request_root,
                chain_id_root,
                block_header_root,
                pred_header_root
            ),
            {
                let apply_block_request = to_ocaml!(gc, request, apply_block_request_root);
                let chain_id = to_ocaml!(gc, request.chain_id, chain_id_root);
                let block_header = to_ocaml!(
                    gc,
                    FfiBlockHeader::from(&request.block_header),
                    block_header_root
                );
                let pred_header = to_ocaml!(
                    gc,
                    FfiBlockHeader::from(&request.pred_header),
                    pred_header_root
                );
                let max_operations_ttl = OCaml::of_i32(request.max_operations_ttl);
                let operations = to_ocaml!(gc, ffi_operations);

                ocaml_call!(tezos_ffi::construct_and_compare_apply_block_request(
                    gc,
                    gc.get(&apply_block_request),
                    gc.get(&chain_id),
                    gc.get(&block_header),
                    gc.get(&pred_header),
                    max_operations_ttl,
                    operations,
                ))
                .unwrap()
                .to_rust()
            }
        )
    })
    .unwrap();

    assert!(result, "ApplyBlockRequest conversion failed")
}

#[test]
#[serial]
fn test_begin_construction_request_conv() {
    let begin_construction_request = BeginConstructionRequest {
        chain_id: ChainId::try_from(hex::decode(CHAIN_ID).unwrap()).unwrap(),
        predecessor: BlockHeader::from_bytes(hex::decode(HEADER).unwrap()).unwrap(),
        protocol_data: Some(vec![1, 2, 3, 4, 5, 6, 7, 8]),
    };

    let result: bool = runtime::execute(move || {
        ocaml_frame!(gc(chain_id_root, predecesor_root, protocol_data_root), {
            let chain_id = to_ocaml!(gc, begin_construction_request.chain_id, chain_id_root);
            let predecesor = to_ocaml!(
                gc,
                FfiBlockHeader::from(&begin_construction_request.predecessor),
                predecesor_root
            );
            let protocol_data = to_ocaml!(
                gc,
                begin_construction_request.protocol_data,
                protocol_data_root
            );
            let begin_construction_request = to_ocaml!(gc, begin_construction_request);
            ocaml_call!(tezos_ffi::construct_and_compare_begin_construction_request(
                gc,
                begin_construction_request,
                gc.get(&chain_id),
                gc.get(&predecesor),
                gc.get(&protocol_data),
            ))
            .unwrap()
            .to_rust()
        })
    })
    .unwrap();

    assert!(result, "BeginConstructionRequest conversion failed")
}

fn get_protocol_hash(prefix: &[u8]) -> ProtocolHash {
    let mut vec = prefix.to_vec();
    vec.extend(std::iter::repeat(0).take(HashType::ProtocolHash.size() - prefix.len()));
    vec.try_into().unwrap()
}

#[test]
#[serial]
fn test_validate_operation_request_conv() {
    let prevalidator = PrevalidatorWrapper {
        chain_id: ChainId::try_from(hex::decode(CHAIN_ID).unwrap()).unwrap(),
        protocol: get_protocol_hash(&[1, 2, 3, 4, 5, 6, 7, 8, 9]),
        context_fitness: Some(vec![vec![0, 1], vec![0, 0, 1, 2, 3, 4, 5]]),
    };
    let operations = ApplyBlockRequest::convert_operations(block_operations_from_hex(
        HEADER_HASH,
        sample_operations_for_request_decoded(),
    ));
    let operation = operations[0][0].clone();
    let validate_operation_request = ValidateOperationRequest {
        prevalidator,
        operation,
    };

    let result: bool = runtime::execute(move || {
        ocaml_frame!(gc(prevalidator_root, operation_root), {
            let prevalidator = to_ocaml!(
                gc,
                validate_operation_request.prevalidator,
                prevalidator_root
            );
            let operation = to_ocaml!(
                gc,
                FfiOperation::from(&validate_operation_request.operation),
                operation_root
            );
            let validate_operation_request = to_ocaml!(gc, validate_operation_request);
            ocaml_call!(tezos_ffi::construct_and_compare_validate_operation_request(
                gc,
                validate_operation_request,
                gc.get(&prevalidator),
                gc.get(&operation),
            ))
            .unwrap()
            .to_rust()
        })
    })
    .unwrap();

    assert!(result, "ValidateOperationRequest conversion failed")
}

#[test]
#[serial]
fn test_validate_rpc_request_conv() {
    let rpc_request = RpcRequest {
        body: "body of request".to_owned(),
        context_path: "/context/path/string".to_owned(),
        meth: RpcMethod::GET,
        content_type: None,
        accept: None,
    };
    let result: bool = runtime::execute(move || {
        ocaml_frame!(
            gc(
                body_root,
                context_path_root,
                meth_root,
                content_type_root,
                accept_root
            ),
            {
                let body = to_ocaml!(gc, rpc_request.body, body_root);
                let context_path = to_ocaml!(gc, rpc_request.context_path, context_path_root);
                let meth = to_ocaml!(gc, rpc_request.meth, meth_root);
                let content_type = to_ocaml!(gc, rpc_request.content_type, content_type_root);
                let accept = to_ocaml!(gc, rpc_request.accept, accept_root);
                let rpc_request = to_ocaml!(gc, rpc_request);
                ocaml_call!(tezos_ffi::construct_and_compare_rpc_request(
                    gc,
                    rpc_request,
                    gc.get(&body),
                    gc.get(&context_path),
                    gc.get(&meth),
                    gc.get(&content_type),
                    gc.get(&accept),
                ))
                .unwrap()
                .to_rust()
            }
        )
    })
    .unwrap();

    assert!(result, "RpcRequest conversion failed")
}

#[test]
#[serial]
fn test_validate_protocol_rpc_request_conv() {
    let rpc_request = RpcRequest {
        body: "body of request".to_owned(),
        context_path: "/context/path/string".to_owned(),
        meth: RpcMethod::GET,
        content_type: None,
        accept: None,
    };
    let protocol_rpc_request = ProtocolRpcRequest {
        block_header: BlockHeader::from_bytes(hex::decode(HEADER).unwrap()).unwrap(),
        chain_arg: "some chain arg".to_owned(),
        chain_id: ChainId::try_from(hex::decode(CHAIN_ID).unwrap()).unwrap(),
        request: rpc_request,
    };
    let result: bool = runtime::execute(move || {
        ocaml_frame!(
            gc(
                block_header_root,
                chain_arg_root,
                chain_id_root,
                request_root,
                ffi_service_root
            ),
            {
                let ref block_header = to_ocaml!(
                    gc,
                    FfiBlockHeader::from(&protocol_rpc_request.block_header),
                    block_header_root
                );
                let ref chain_arg = to_ocaml!(gc, protocol_rpc_request.chain_arg, chain_arg_root);
                let ref chain_id = to_ocaml!(gc, protocol_rpc_request.chain_id, chain_id_root);
                let ref request = to_ocaml!(gc, protocol_rpc_request.request, request_root);
                let protocol_rpc_request = to_ocaml!(gc, protocol_rpc_request);
                ocaml_call!(tezos_ffi::construct_and_compare_protocol_rpc_request(
                    gc,
                    protocol_rpc_request,
                    gc.get(&block_header),
                    gc.get(&chain_id),
                    gc.get(&chain_arg),
                    gc.get(&request),
                ))
                .unwrap()
                .to_rust()
            }
        )
    })
    .unwrap();

    assert!(result, "ProtocolRpcRequest conversion failed")
}

#[test]
#[serial]
fn test_validate_operation_conv() {
    let operations = ApplyBlockRequest::convert_operations(block_operations_from_hex(
        HEADER_HASH,
        sample_operations_for_request_decoded(),
    ));
    let operation = operations[0][0].clone();

    let result: bool = runtime::execute(move || {
        ocaml_frame!(gc(branch_root, proto_root), {
            let branch = to_ocaml!(gc, operation.branch(), branch_root);
            let proto = to_ocaml!(gc, operation.data(), proto_root);
            let operation = to_ocaml!(gc, FfiOperation::from(&operation));
            ocaml_call!(tezos_ffi::construct_and_compare_operation(
                gc,
                operation,
                gc.get(&branch),
                gc.get(&proto),
            ))
            .unwrap()
            .to_rust()
        })
    })
    .unwrap();

    assert!(result, "Operation conversion failed")
}

#[test]
#[serial]
fn test_validate_prevalidator_wrapper_conv() {
    let prevalidator_wrapper = PrevalidatorWrapper {
        chain_id: ChainId::try_from(hex::decode(CHAIN_ID).unwrap()).unwrap(),
        protocol: get_protocol_hash(&[1, 2, 3, 4, 5, 6, 7, 8, 9]),
        context_fitness: Some(vec![vec![0, 0], vec![0, 0, 1, 2, 3, 4, 5]]),
    };

    let result: bool = runtime::execute(move || {
        ocaml_frame!(gc(chain_id_root, protocol_root, context_fitness_root), {
            let chain_id = to_ocaml!(gc, prevalidator_wrapper.chain_id, chain_id_root);
            let protocol = to_ocaml!(gc, prevalidator_wrapper.protocol, protocol_root);
            let context_fitness = to_ocaml!(
                gc,
                prevalidator_wrapper.context_fitness,
                context_fitness_root
            );
            let prevalidator_wrapper = to_ocaml!(gc, prevalidator_wrapper);
            ocaml_call!(tezos_ffi::construct_and_compare_prevalidator_wrapper(
                gc,
                prevalidator_wrapper,
                gc.get(&chain_id),
                gc.get(&protocol),
                gc.get(&context_fitness),
            ))
            .unwrap()
            .to_rust()
        })
    })
    .unwrap();

    assert!(result, "PrevalidatorWrapper conversion failed")
}
