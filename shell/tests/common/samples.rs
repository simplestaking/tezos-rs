// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::fs::File;
use std::io::BufRead;
use std::path::Path;
use std::{collections::HashMap, convert::TryFrom};
use std::{env, io};

use itertools::Itertools;
use lazy_static::lazy_static;

use crypto::hash::{BlockHash, ChainId, HashType};
use tezos_api::environment::TezosEnvironment;
use tezos_api::ffi::ApplyBlockRequest;
use tezos_encoding::{
    encoding::{Encoding, Field, HasEncoding},
    nom::{field, NomReader},
};
use tezos_messages::p2p::binary_message::BinaryRead;
use tezos_messages::p2p::encoding::prelude::{BlockHeader, Operation, OperationsForBlocksMessage};

lazy_static! {
    pub static ref APPLY_BLOCK_REQUEST_ENCODING: Encoding = Encoding::Obj(
        "ApplyBlockRequest",
        vec![
            Field::new("chain_id", Encoding::Hash(HashType::ChainId)),
            Field::new(
                "block_header",
                Encoding::dynamic(BlockHeader::encoding().clone())
            ),
            Field::new(
                "pred_header",
                Encoding::dynamic(BlockHeader::encoding().clone())
            ),
            Field::new("max_operations_ttl", Encoding::Int31),
            Field::new(
                "operations",
                Encoding::dynamic(Encoding::list(Encoding::dynamic(Encoding::list(
                    Encoding::dynamic(Operation::encoding().clone())
                ))))
            ),
        ]
    );
}

/// Create new struct from bytes.
#[inline]
pub fn from_captured_bytes(request: &str) -> Result<ApplyBlockRequest, anyhow::Error> {
    struct Request(ApplyBlockRequest);
    impl NomReader for Request {
        fn nom_read(bytes: &[u8]) -> tezos_encoding::nom::NomResult<Self> {
            use nom::combinator::map;
            use nom::number::complete::be_i32;
            use nom::sequence::tuple;
            use tezos_encoding::nom::{dynamic, list};
            map(
                tuple((
                    field("chain_id", ChainId::nom_read),
                    field("block_header", dynamic(BlockHeader::nom_read)),
                    field("pred_header", dynamic(BlockHeader::nom_read)),
                    field("max_operations_ttl", be_i32),
                    field(
                        "operations",
                        dynamic(list(dynamic(list(dynamic(Operation::nom_read))))),
                    ),
                )),
                |(chain_id, block_header, pred_header, max_operations_ttl, operations)| {
                    Request(ApplyBlockRequest {
                        chain_id,
                        block_header,
                        pred_header,
                        max_operations_ttl,
                        operations,
                        predecessor_block_metadata_hash: None,
                        predecessor_ops_metadata_hash: None,
                    })
                },
            )(bytes)
        }
    }
    let bytes = hex::decode(request)?;
    let value = <Request as BinaryRead>::from_bytes(bytes)?;
    Ok(value.0)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct OperationsForBlocksMessageKey {
    block_hash: String,
    validation_pass: i8,
}

impl OperationsForBlocksMessageKey {
    pub fn new(block_hash: BlockHash, validation_pass: i8) -> Self {
        OperationsForBlocksMessageKey {
            block_hash: block_hash.to_base58_check(),
            validation_pass,
        }
    }
}

pub fn read_data_apply_block_request_until_1326() -> (
    Vec<String>,
    HashMap<OperationsForBlocksMessageKey, OperationsForBlocksMessage>,
    TezosEnvironment,
) {
    read_data_zip(
        "apply_block_request_until_1326.zip",
        TezosEnvironment::Carthagenet,
    )
}

/// Expected zip structure:
/// - files with tezos_encoding encoded ApplyBlockRequest as hex, ordered:
///   apply_block_request_001.bytes
///   apply_block_request_002.bytes
///   ...
///   apply_block_request_XYZ.bytes
/// - stored operations in file OperationsForBlocksMessage
pub fn read_data_zip(
    zip_file_name: &str,
    tezos_env: TezosEnvironment,
) -> (
    Vec<String>,
    HashMap<OperationsForBlocksMessageKey, OperationsForBlocksMessage>,
    TezosEnvironment,
) {
    let path = Path::new(&env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is not set"))
        .join("tests")
        .join("resources")
        .join(zip_file_name);
    let file = File::open(path)
        .unwrap_or_else(|_| panic!("Couldn't open file: tests/resources/{}", zip_file_name));
    let mut archive = zip::ZipArchive::new(file).unwrap();

    // 1. get requests from files sorted by name
    let requests = archive
        .file_names()
        .filter(|file_name| file_name.starts_with("apply_block_request_"))
        .map(String::from)
        .sorted()
        .map(|file_name| {
            let mut file = archive.by_name(&file_name).unwrap();
            let mut writer: Vec<u8> = vec![];
            io::copy(&mut file, &mut writer).unwrap();
            String::from_utf8(writer).expect(&format!("error reading entry {}", file_name))
        })
        .collect_vec();

    // 2. get operations
    let operations_file = archive.by_name("OperationsForBlocksMessage").unwrap();
    let mut operations: HashMap<OperationsForBlocksMessageKey, OperationsForBlocksMessage> =
        HashMap::new();

    // read file by lines
    let reader = io::BufReader::new(operations_file);
    let lines = reader.lines();
    for line in lines {
        if let Ok(mut line) = line {
            let _ = line.remove(0);
            let split = line.split('|').collect_vec();
            assert_eq!(3, split.len());

            let block_hash = BlockHash::try_from(split[0]).expect("Failed to parse block_hash");
            let validation_pass = split[1]
                .parse::<i8>()
                .expect("Failed to parse validation_pass");

            let operations_for_blocks_message =
                hex::decode(split[2]).expect("Failed to parse operations_for_blocks_message");
            let operations_for_blocks_message =
                OperationsForBlocksMessage::from_bytes(operations_for_blocks_message)
                    .expect("Failed to readed bytes for operations_for_blocks_message");

            operations.insert(
                OperationsForBlocksMessageKey::new(block_hash, validation_pass),
                operations_for_blocks_message,
            );
        }
    }

    (requests, operations, tezos_env)
}

#[allow(dead_code)]
pub(crate) fn read_carthagenet_context_json(file_name: &str) -> Option<String> {
    let path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("tests")
        .join("resources")
        .join("ocaml_context_jsons.zip");
    let file =
        File::open(path).expect("Couldn't open file: tests/resources/ocaml_context_jsons.zip");
    let mut archive = zip::ZipArchive::new(file).unwrap();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).unwrap();
        if file.name().eq(file_name) {
            let mut writer: Vec<u8> = vec![];
            io::copy(&mut file, &mut writer).unwrap();
            return Some(String::from_utf8(writer).expect("error"));
        }
    }

    None
}
