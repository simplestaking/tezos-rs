// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]
extern crate test;

use std::{env, thread};

use znfe::{ocaml_frame, OCaml, OCamlBytes, OCamlList, OCamlFn1, FromOCaml, ToOCaml, ocaml_alloc, ocaml_call};
use serial_test::serial;

use crypto::hash::HashType;
use tezos_api::ffi::{
    Applied, ApplyBlockRequest, ApplyBlockRequestBuilder, ApplyBlockResponse,
    BeginConstructionRequest, BeginConstructionRequestBuilder, Errored, FfiMessage, ForkingTestchainData, OperationProtocolDataJsonWithErrorListJson, PrevalidatorWrapper, RustBytes,
    TezosRuntimeConfiguration,
    ValidateOperationResponse, ValidateOperationResult, ValidateOperationResultBuilder,
};
use tezos_context::channel::{context_receive, ContextAction, enable_context_channel};
use tezos_interop::ffi;
use tezos_interop::runtime;
use tezos_interop::runtime::OcamlError;
use tezos_messages::p2p::binary_message::BinaryMessage;
use tezos_messages::p2p::encoding::prelude::*;

const CHAIN_ID: &str = "8eceda2f";
const CONTEXT_HASH: &str = "2f358bab4d28c4ee733ad7f2b01dcf116b33474b8c3a6cb40cccda2bdddd6d72";
const HEADER_HASH: &str = "61e687e852460b28f0f9540ccecf8f6cf87a5ad472c814612f0179caf4b9f673";
const HEADER: &str = "0000000301a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000005c017ed604dfcb6b41e91650bb908618b2740a6167d9072c3230e388b24feeef04c98dc27f000000110000000100000000080000000000000005f06879947f3d9959090f27054062ed23dbf9f7bd4b3c8a6e86008daabb07913e000c00000003e5445371002b9745d767d7f164a39e7f373a0f25166794cba491010ab92b0e281b570057efc78120758ff26a33301870f361d780594911549bcb7debbacd8a142e0b76a605";
const OPERATION: &str = "a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000000236663bacdca76094fdb73150092659d463fec94eda44ba4db10973a1ad057ef53a5b3239a1b9c383af803fc275465bd28057d68f3cab46adfd5b2452e863ff0a";
const OPERATION_HASH: &str = "7e73e3da041ea251037af062b7bc04b37a5ee38bc7e229e7e20737071ed73af4";
const MAX_OPERATIONS_TTL: i32 = 5;

mod tezos_ffi {
    use znfe::{ocaml, OCamlBytes, OCamlList, OCamlInt};
    ocaml!{
        pub fn apply_block_params_roundtrip(chain_id: OCamlBytes, block_header: OCamlBytes, operations: OCamlList<OCamlList<OCamlBytes>>) -> (OCamlBytes, OCamlBytes, OCamlList<OCamlList<OCamlBytes>>);
        pub fn apply_block_request_roundtrip(data: OCamlBytes) -> OCamlBytes;
        pub fn apply_block_response_roundtrip(data: OCamlBytes) -> OCamlBytes;
        pub fn validate_operation_response_roundtrip(data: OCamlBytes) -> OCamlBytes;
        pub fn begin_construction_request_roundtrip(data: OCamlBytes) -> OCamlBytes;
        pub fn context_callback_roundtrip(count: OCamlInt, context_hash: OCamlBytes, header_hash: OCamlBytes, operation_hash: OCamlBytes, key: OCamlList<OCamlBytes>, data: OCamlBytes) -> ();
        pub fn operation_roundtrip(operation: OCamlBytes) -> OCamlBytes;
        pub fn operations_list_list_roundtrip(operations_list_list: OCamlList<OCamlList<OCamlBytes>>) -> OCamlList<OCamlList<OCamlBytes>>;
        pub fn chain_id_roundtrip(chain_id: OCamlBytes) -> OCamlBytes;
        pub fn block_header_roundtrip(header: OCamlBytes) -> (OCamlBytes, OCamlBytes);
        pub fn block_header_with_hash_roundtrip(header_hash: OCamlBytes, hash: OCamlBytes) -> (OCamlBytes, OCamlBytes);
    }
}

fn init_test_runtime() {
    // init runtime and turn on/off ocaml logging
    ffi::change_runtime_configuration(
        TezosRuntimeConfiguration {
            debug_mode: false,
            log_enabled: is_ocaml_log_enabled(),
            no_of_ffi_calls_treshold_for_gc: no_of_ffi_calls_treshold_for_gc(),
        }
    ).unwrap().unwrap();
}

macro_rules! roundtrip_test {
        ($test_name:ident, $test_fn:expr, $counts:expr) => {
            #[test]
            #[serial]
            fn $test_name() {
                init_test_runtime();

                for i in 0..$counts {
                    let result = $test_fn(i);
                    match result {
                        Err(e) => {
                            panic!("roundtrip number {} failed! error: {:?}", i, e)
                        },
                        Ok(_) => ()
                    }
                }
            }
        };
}

type RoundtripFn = OCamlFn1<OCamlBytes, OCamlBytes>;

fn bytes_encoding_roundtrip<MSG: FfiMessage + 'static>(orig_data: MSG, ffi_roundtrip_fn: RoundtripFn) -> Result<(), OcamlError> {
    runtime::execute(move || {
        ocaml_frame!(gc, {
            // sent bytes to ocaml
            let data_as_rust_bytes: RustBytes = orig_data.as_rust_bytes().expect("failed to convert");
            let data = ocaml_alloc!(data_as_rust_bytes.to_ocaml(gc));

            let result = ocaml_call!(ffi_roundtrip_fn(gc, data)).expect("failed to call ffi");

            // check
            let result = RustBytes::from_ocaml(result);

            let result = MSG::from_rust_bytes(result).expect("failed to convert in ffi");
            assert_eq!(orig_data, result);

            ()
        })
    })
}

roundtrip_test!(test_chain_id_roundtrip_calls, test_chain_id_roundtrip, 1);

fn test_chain_id_roundtrip(iteration: i32) -> Result<(), failure::Error> {
    let chain_id: RustBytes = hex::decode(CHAIN_ID).unwrap();

    let result = runtime::execute(move || {
        ocaml_frame!(gc, {
            // sent bytes to ocaml
            let chain_id: OCaml<OCamlBytes> = ocaml_alloc!(chain_id.to_ocaml(gc));
            let result = ocaml_call!(tezos_ffi::chain_id_roundtrip(gc, chain_id));
            let result = String::from_ocaml(result.unwrap());
            assert_eq_hash(CHAIN_ID, result);
            ()
        })
    });

    Ok(
        assert!(
            result.is_ok(),
            format!("test_chain_id_roundtrip roundtrip iteration: {} failed!", iteration)
        )
    )
}

roundtrip_test!(test_block_header_roundtrip_calls, test_block_header_roundtrip, 1);

fn test_block_header_roundtrip(iteration: i32) -> Result<(), failure::Error> {
    let header: RustBytes = hex::decode(HEADER).unwrap();

    let result = runtime::execute(move || {
        ocaml_frame!(gc, {
            // sent bytes to ocaml
            let header: OCaml<OCamlBytes> = ocaml_alloc!(header.to_ocaml(gc));
            let result = ocaml_call!(tezos_ffi::block_header_roundtrip(gc, header));
            let result = <(String, String)>::from_ocaml(result.unwrap());
            assert_eq_hash_and_header(HEADER_HASH, HEADER, result);
            ()
        })
    });

    Ok(
        assert!(
            result.is_ok(),
            format!("test_block_header_roundtrip roundtrip iteration: {} failed!", iteration)
        )
    )
}

roundtrip_test!(test_block_header_with_hash_roundtrip_calls, test_block_header_with_hash_roundtrip, 1);

fn test_block_header_with_hash_roundtrip(iteration: i32) -> Result<(), failure::Error> {
    let header_hash: RustBytes = hex::decode(HEADER_HASH).unwrap();
    let header: RustBytes = hex::decode(HEADER).unwrap();

    let result = runtime::execute(move || {
        ocaml_frame!(gc, {
            // sent bytes to ocaml
            let header_hash: OCaml<OCamlBytes> = ocaml_alloc!(header_hash.to_ocaml(gc));
            let ref header_hash_ref = gc.keep(header_hash);
            let header: OCaml<OCamlBytes> = ocaml_alloc!(header.to_ocaml(gc));

            let result = ocaml_call!(tezos_ffi::block_header_with_hash_roundtrip(
                gc,
                gc.get(header_hash_ref),
                header
            ));
            let result = <(String, String)>::from_ocaml(result.unwrap());
            assert_eq_hash_and_header(HEADER_HASH, HEADER, result);
            ()
        })
    });

    Ok(
        assert!(
            result.is_ok(),
            format!("test_block_header_with_hash_roundtrip roundtrip iteration: {} failed!", iteration)
        )
    )
}

roundtrip_test!(test_operation_roundtrip_calls, test_operation_roundtrip, 1);

fn test_operation_roundtrip(iteration: i32) -> Result<(), failure::Error> {
    let operation: RustBytes = hex::decode(OPERATION).unwrap();

    let result = runtime::execute(move || {
        ocaml_frame!(gc, {
            // sent bytes to ocaml
            let operation: OCaml<OCamlBytes> = ocaml_alloc!(operation.to_ocaml(gc));
            let result = ocaml_call!(tezos_ffi::operation_roundtrip(gc, operation));

            // check
            let result = String::from_ocaml(result.unwrap());
            assert_eq!(OPERATION, hex::encode(result).as_str());

            ()
        })
    });

    Ok(
        assert!(
            result.is_ok(),
            format!("test_operation_roundtrip roundtrip iteration: {} failed!", iteration)
        )
    )
}

#[test]
#[serial]
fn test_operations_list_list_roundtrip_one() {
    init_test_runtime();

    assert!(test_operations_list_list_roundtrip(1, sample_operations(), 4, 1).is_ok());
    assert!(test_operations_list_list_roundtrip(1, sample_operations2(), 4, 0).is_ok());
    assert!(test_operations_list_list_roundtrip(1, sample_operations3(), 5, 1).is_ok());
}

fn test_operations_list_list_roundtrip_for_bench(iteration: i32) -> Result<(), failure::Error> {
    Ok(assert!(test_operations_list_list_roundtrip(iteration, sample_operations(), 4, 1).is_ok()))
}

fn test_operations_list_list_roundtrip(iteration: i32, operations: Vec<Option<Vec<RustBytes>>>, expected_list_count: usize, expected_list_0_count: usize) -> Result<(), failure::Error> {
    let result = runtime::execute(move || {
        ocaml_frame!(gc, {
            let empty_vec = vec![];
            let operations: Vec<_> = operations.iter().map(|op| op.as_ref().unwrap_or(&empty_vec)).collect();
            let operations_list_list_ocaml: OCaml<OCamlList<OCamlList<OCamlBytes>>> = ocaml_alloc!(operations.to_ocaml(gc));

            // sent bytes to ocaml
            let result = ocaml_call!(tezos_ffi::operations_list_list_roundtrip(gc, operations_list_list_ocaml));

            // check
            let result = <Vec<Vec<String>>>::from_ocaml(result.unwrap());
            assert_eq_operations(result, expected_list_count, expected_list_0_count);

            ()
        })
    });

    Ok(
        assert!(
            result.is_ok(),
            format!("test_operations_list_list_roundtrip roundtrip iteration: {} failed!", iteration)
        )
    )
}

#[test]
#[serial]
fn test_operations_list_list_roundtrip_times() {
    init_test_runtime();

    for i in 0..10000 {
        let result = test_operations_list_list_roundtrip(i, sample_operations(), 4, 1);
        if result.is_err() {
            println!("test_operations_list_list_roundtrip_times roundtrip number {} failed!", i);
        }
        assert!(result.is_ok())
    }
}

roundtrip_test!(test_apply_block_params_roundtrip_calls, test_apply_block_params_roundtrip, 10000);

fn test_apply_block_params_roundtrip(iteration: i32) -> Result<(), failure::Error> {
    let chain_id = hex::decode(CHAIN_ID).unwrap();
    let block_header = hex::decode(HEADER).unwrap();
    let operations = sample_operations();

    Ok(
        assert!(
            apply_block_params_roundtrip(chain_id, block_header, operations).is_ok(),
            format!("test_apply_block_params_roundtrip roundtrip iteration: {} failed!", iteration)
        )
    )
}

fn apply_block_params_roundtrip(chain_id: RustBytes,
                                block_header: RustBytes,
                                operations: Vec<Option<Vec<RustBytes>>>) -> Result<(), OcamlError> {
    runtime::execute(move || {
        ocaml_frame!(gc, {
            // sent bytes to ocaml
            let chain_id = ocaml_alloc!(chain_id.to_ocaml(gc));
            let ref chain_id_ref = gc.keep(chain_id);
            let block_header = ocaml_alloc!(block_header.to_ocaml(gc));
            let ref block_header_ref = gc.keep(block_header);
            let empty_vec = vec![];
            let operations: Vec<_> = operations.iter().map(|op| op.as_ref().unwrap_or(&empty_vec)).collect();
            let operations: OCaml<OCamlList<OCamlList<OCamlBytes>>> = ocaml_alloc!(operations.to_ocaml(gc));

            let result = ocaml_call!(tezos_ffi::apply_block_params_roundtrip(
                gc,
                gc.get(chain_id_ref),
                gc.get(block_header_ref),
                operations));

            // check
            let result = <(String, String, Vec<Vec<String>>)>::from_ocaml(result.unwrap());
            //assert_eq!(3, result.len());

            // check chain_id
            assert_eq_hash(CHAIN_ID, result.0);

            // check header
            assert_eq_bytes(HEADER, result.1);

            // check operations
            assert_eq_operations(result.2, 4, 1);

            ()
        })
    })
}

roundtrip_test!(test_apply_block_request_roundtrip_calls, test_apply_block_request_roundtrip, 10000);

fn test_apply_block_request_roundtrip(iteration: i32) -> Result<(), failure::Error> {
    // request
    let request: ApplyBlockRequest = ApplyBlockRequestBuilder::default()
        .chain_id(hex::decode(CHAIN_ID).unwrap())
        .block_header(BlockHeader::from_bytes(hex::decode(HEADER).unwrap()).unwrap())
        .pred_header(BlockHeader::from_bytes(hex::decode(HEADER).unwrap()).unwrap())
        .max_operations_ttl(MAX_OPERATIONS_TTL)
        .operations(ApplyBlockRequest::convert_operations(block_operations_from_hex(HEADER_HASH, sample_operations_for_request())))
        .build().unwrap();

    Ok(
        assert!(
            bytes_encoding_roundtrip(request, tezos_ffi::apply_block_request_roundtrip).is_ok(),
            format!("test_apply_block_params_roundtrip roundtrip iteration: {} failed!", iteration)
        )
    )
}

roundtrip_test!(test_apply_block_response_roundtrip_calls, test_apply_block_response_roundtrip, 10000);

fn test_apply_block_response_roundtrip(iteration: i32) -> Result<(), failure::Error> {
    // request - test data - None
    let response_with_none_forking_data: ApplyBlockResponse = ApplyBlockResponse {
        validation_result_message: "validation_result_message".to_string(),
        context_hash: HashType::ContextHash.string_to_bytes("CoV16kW8WgL51SpcftQKdeqc94D6ekghMgPMmEn7TSZzFA697PeE").expect("failed to convert"),
        block_header_proto_json: "block_header_proto_json".to_string(),
        block_header_proto_metadata_json: "block_header_proto_metadata_json".to_string(),
        operations_proto_metadata_json: "operations_proto_metadata_json".to_string(),
        max_operations_ttl: 6,
        last_allowed_fork_level: 8,
        forking_testchain: true,
        forking_testchain_data: None,
    };

    assert!(
        bytes_encoding_roundtrip(response_with_none_forking_data.clone(), tezos_ffi::apply_block_response_roundtrip).is_ok(),
        format!("test_apply_block_response_roundtrip (forking data - none) roundtrip iteration: {} failed!", iteration)
    );

    // request - test data - None
    let response_with_some_forking_data: ApplyBlockResponse = ApplyBlockResponse {
        validation_result_message: "validation_result_message".to_string(),
        context_hash: HashType::ContextHash.string_to_bytes("CoV16kW8WgL51SpcftQKdeqc94D6ekghMgPMmEn7TSZzFA697PeE").expect("failed to convert"),
        block_header_proto_json: "block_header_proto_json".to_string(),
        block_header_proto_metadata_json: "block_header_proto_metadata_json".to_string(),
        operations_proto_metadata_json: "operations_proto_metadata_json".to_string(),
        max_operations_ttl: 6,
        last_allowed_fork_level: 8,
        forking_testchain: true,
        forking_testchain_data: Some(ForkingTestchainData {
            test_chain_id: HashType::ChainId.string_to_bytes("NetXgtSLGNJvNye")?,
            forking_block_hash: HashType::BlockHash.string_to_bytes("BKyQ9EofHrgaZKENioHyP4FZNsTmiSEcVmcghgzCC9cGhE7oCET")?,
        }),
    };

    assert!(
        bytes_encoding_roundtrip(response_with_some_forking_data, tezos_ffi::apply_block_response_roundtrip).is_ok(),
        format!("test_apply_block_response_roundtrip (forking data - some) roundtrip iteration: {} failed!", iteration)
    );

    Ok(())
}

roundtrip_test!(test_validate_operation_response_roundtrip_calls, test_validate_operation_response_roundtrip, 10000);

fn test_validate_operation_response_roundtrip(iteration: i32) -> Result<(), failure::Error> {
    let operation_hash = HashType::OperationHash.string_to_bytes("oobuLSAcTvomrF2dFPCp5mLxABSD8o65v6wL79ihJY75dkUkhdd")?;

    // validation result
    let result: ValidateOperationResult = ValidateOperationResultBuilder::default()
        .applied(vec![
            Applied {
                hash: operation_hash.clone(),
                protocol_data_json: "hello".to_string(),
            }
        ])
        .refused(vec![])
        .branch_refused(vec![
            Errored {
                hash: operation_hash.clone(),
                is_endorsement: None,
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "protocol_data_json".to_string(),
                    error_json: "error_list_json".to_string(),
                },
            }
        ])
        .branch_delayed(vec![
            Errored {
                hash: operation_hash.clone(),
                is_endorsement: Some(true),
                protocol_data_json_with_error_json: OperationProtocolDataJsonWithErrorListJson {
                    protocol_data_json: "".to_string(),
                    error_json: "error".to_string(),
                },
            }
        ])
        .build().unwrap();

    // prevalidator
    let prevalidator = PrevalidatorWrapper {
        chain_id: HashType::ChainId.string_to_bytes("NetXgtSLGNJvNye")?,
        protocol: HashType::ProtocolHash.string_to_bytes("PsddFKi32cMJ2qPjf43Qv5GDWLDPZb3T3bF6fLKiF5HtvHNU7aP")?,
    };

    // response
    let orig_validation_response: ValidateOperationResponse = ValidateOperationResponse {
        result,
        prevalidator,
    };


    Ok(
        assert!(
            bytes_encoding_roundtrip(orig_validation_response, tezos_ffi::validate_operation_response_roundtrip).is_ok(),
            format!("test_validate_operation_response_roundtrip roundtrip iteration: {} failed!", iteration)
        )
    )
}

roundtrip_test!(test_begin_construction_request_roundtrip_calls, test_begin_construction_request_roundtrip, 10000);

fn test_begin_construction_request_roundtrip(iteration: i32) -> Result<(), failure::Error> {

    // validation result - no protocol data
    let request: BeginConstructionRequest = BeginConstructionRequestBuilder::default()
        .chain_id(HashType::ChainId.string_to_bytes("NetXgtSLGNJvNye")?)
        .predecessor(BlockHeader::from_bytes(hex::decode(HEADER)?)?)
        .protocol_data(None)
        .build().unwrap();

    assert!(
        bytes_encoding_roundtrip(request, tezos_ffi::begin_construction_request_roundtrip).is_ok(),
        format!("test_begin_construction_request_roundtrip (none protocol data) roundtrip iteration: {} failed!", iteration)
    );

    // validation result - some protocol data
    let request: BeginConstructionRequest = BeginConstructionRequestBuilder::default()
        .chain_id(HashType::ChainId.string_to_bytes("NetXgtSLGNJvNye")?)
        .predecessor(BlockHeader::from_bytes(hex::decode(HEADER)?)?)
        .protocol_data(Some(vec![1, 2, 3, 4, 5, 6]))
        .build().unwrap();

    assert!(
        bytes_encoding_roundtrip(request, tezos_ffi::begin_construction_request_roundtrip).is_ok(),
        format!("test_begin_construction_request_roundtrip (some protocol data) roundtrip iteration: {} failed!", iteration)
    );

    Ok(())
}

#[test]
#[serial]
fn test_context_callback() {
    init_test_runtime();

    let expected_count = 10000;
    let expected_context_hash = hex::decode(CONTEXT_HASH).unwrap();
    let expected_header_hash = hex::decode(HEADER_HASH).unwrap();
    let expected_operation_hash = hex::decode(OPERATION_HASH).unwrap();
    let expected_key: Vec<String> = vec!["data".to_string(), "contracts".to_string(), "contract".to_string(), "amount".to_string()];
    let expected_data = hex::decode(HEADER).unwrap();

    let expected_context_hash_cloned = expected_context_hash.clone();
    let expected_header_hash_cloned = expected_header_hash.clone();
    let expected_operation_hash_cloned = expected_operation_hash.clone();
    let expected_key_cloned = expected_key.clone();
    let expected_data_cloned = expected_data.clone();

    enable_context_channel();
    let handle = thread::spawn(move || {
        let mut received = 0;
        for _ in 0..expected_count {
            let action = context_receive().unwrap();
            received += 1;

            match action {
                ContextAction::Set {
                    context_hash,
                    block_hash,
                    operation_hash,
                    key,
                    value,
                    ..
                } => {
                    assert!(context_hash.is_some());
                    assert_eq!(expected_context_hash_cloned, context_hash.unwrap());

                    assert!(block_hash.is_some());
                    assert_eq!(expected_header_hash_cloned, block_hash.unwrap());

                    assert!(operation_hash.is_some());
                    assert_eq!(expected_operation_hash_cloned, operation_hash.unwrap());

                    assert_eq!(expected_key_cloned.clone(), key);
                    assert_eq!(expected_data_cloned.clone(), value);
                }
                ContextAction::Checkout {
                    context_hash,
                    ..
                } => {
                    assert_eq!(expected_context_hash_cloned, context_hash);
                }
                _ => panic!("test failed - waiting just 'Set' action!")
            }
        }
        received
    });

    call_to_send_context_events(
        expected_count,
        expected_context_hash,
        expected_header_hash,
        expected_operation_hash,
        expected_key,
        expected_data,
    );

    let received = handle.join().unwrap();
    assert_eq!(expected_count, received)
}

fn call_to_send_context_events(
    count: i32,
    context_hash: RustBytes,
    block_header_hash: RustBytes,
    operation_hash: RustBytes,
    key: Vec<String>,
    data: RustBytes) {
    runtime::execute(move || {
        ocaml_frame!(gc, {
            // sent bytes to ocaml
            let count = OCaml::of_int(count as i64);
            let context_hash: OCaml<OCamlBytes> = ocaml_alloc!(context_hash.to_ocaml(gc));
            let ref context_hash_ref = gc.keep(context_hash);
            let block_header_hash: OCaml<OCamlBytes> = ocaml_alloc!(block_header_hash.to_ocaml(gc));
            let ref block_header_hash_ref = gc.keep(block_header_hash);
            let operation_hash: OCaml<OCamlBytes> = ocaml_alloc!(operation_hash.to_ocaml(gc));
            let ref operation_hash_ref = gc.keep(operation_hash);
            let key: OCaml<OCamlList<OCamlBytes>> = ocaml_alloc!(key.to_ocaml(gc));
            let ref key_ref = gc.keep(key);
            let data: OCaml<OCamlBytes> = ocaml_alloc!(data.to_ocaml(gc));

            let result = ocaml_call!(tezos_ffi::context_callback_roundtrip(
                gc,
                count,
                gc.get(context_hash_ref),
                gc.get(block_header_hash_ref),
                gc.get(operation_hash_ref),
                gc.get(key_ref),
                data));

            // check
            assert!(result.is_ok());

            ()
        })
    }).unwrap()
}

fn block_operations_from_hex(block_hash: &str, hex_operations: Vec<Vec<RustBytes>>) -> Vec<OperationsForBlocksMessage> {
    hex_operations
        .into_iter()
        .map(|bo| {
            let ops = bo
                .into_iter()
                .map(|op| Operation::from_bytes(op).unwrap())
                .collect();
            OperationsForBlocksMessage::new(OperationsForBlock::new(hex::decode(block_hash).unwrap(), 4), Path::Op, ops)
        })
        .collect()
}

fn sample_operations() -> Vec<Option<Vec<RustBytes>>> {
    vec![
        Some(vec![hex::decode(OPERATION).unwrap()]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![])
    ]
}

fn sample_operations_for_request() -> Vec<Vec<RustBytes>> {
    vec![
        vec![hex::decode(OPERATION).unwrap()],
        vec![],
        vec![],
        vec![hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08").unwrap()],
        vec![]
    ]
}

fn sample_operations2() -> Vec<Option<Vec<RustBytes>>> {
    vec![
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![])
    ]
}

fn sample_operations3() -> Vec<Option<Vec<RustBytes>>> {
    vec![
        Some(vec![hex::decode(OPERATION).unwrap()]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08").unwrap()]),
        Some(vec![])
    ]
}

fn assert_eq_bytes(expected: &str, bytes: String) {
    assert!(!bytes.is_empty());
    let bytes_ocaml = hex::encode(bytes);
    assert_eq!(expected, bytes_ocaml.as_str());
}

fn assert_eq_hash(expected: &str, hash: String) {
    assert!(!hash.is_empty());
    let hash_ocaml = hex::encode(hash);
    assert_eq!(expected, hash_ocaml.as_str());
}

fn is_ocaml_log_enabled() -> bool {
    env::var("OCAML_LOG_ENABLED")
        .unwrap_or("false".to_string())
        .parse::<bool>().unwrap()
}

fn no_of_ffi_calls_treshold_for_gc() -> i32 {
    env::var("OCAML_CALLS_GC")
        .unwrap_or("1000".to_string())
        .parse::<i32>().unwrap()
}

fn assert_eq_hash_and_header(expected_hash: &str, expected_header: &str, header_tuple: (String, String)) {
    let (hash, header) = header_tuple;
    assert_eq_hash(expected_hash, hash);
    assert_eq_bytes(expected_header, header);
}

fn assert_eq_operations(result: Vec<Vec<String>>, expected_list_count: usize, expected_list_0_count: usize) {
    assert_eq!(expected_list_count, result.len());

    let ref operations_list_0 = result[0];
    assert_eq!(expected_list_0_count, operations_list_0.len());

    if expected_list_0_count > 0 {
        let ref operation_0_0 = operations_list_0[0];
        assert_eq!(OPERATION, hex::encode(operation_0_0));
    }
}

// run as: cargo bench --tests --nocapture
mod benches {
    use test::Bencher;

    use crate::*;

    macro_rules! bench_test {
        ($test_name:ident, $f:expr) => {
            #[bench]
            fn $test_name(b: &mut Bencher) {
                init_test_runtime();

                let mut counter = 0;
                let mut counter_failed = 0;
                b.iter(|| {
                    counter += 1;
                    let result: Result<(), failure::Error> = $f(counter);
                    if let Err(_) = result {
                        counter_failed += 1;
                    }
                });
                assert_eq!(0, counter_failed);
            }
        };
    }

    bench_test!(bench_test_chain_id_roundtrip, test_chain_id_roundtrip);
    bench_test!(bench_test_block_header_roundtrip, test_block_header_roundtrip);
    bench_test!(bench_test_block_header_with_hash_roundtrip, test_block_header_with_hash_roundtrip);
    bench_test!(bench_test_operations_list_list_roundtrip_one, test_operations_list_list_roundtrip_for_bench);
    bench_test!(bench_test_apply_block_params_roundtrip_one, test_apply_block_params_roundtrip);
    bench_test!(bench_test_apply_block_request_roundtrip_one, test_apply_block_request_roundtrip);
    bench_test!(bench_test_apply_block_response_roundtrip_one, test_apply_block_response_roundtrip);
    bench_test!(bench_test_begin_construction_request_roundtrip_one, test_begin_construction_request_roundtrip);
    bench_test!(bench_test_validate_operation_response_roundtrip_one, test_validate_operation_response_roundtrip);
}
