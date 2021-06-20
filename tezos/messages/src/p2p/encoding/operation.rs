// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryFrom;

use getset::Getters;
use hex::FromHexError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crypto::{
    base58::FromBase58CheckError,
    hash::{BlockHash, OperationHash},
};
use tezos_encoding::encoding::HasEncoding;
use tezos_encoding::nom::NomReader;

use super::limits::{GET_OPERATIONS_MAX_LENGTH, OPERATION_MAX_SIZE};

#[derive(Serialize, Deserialize, PartialEq, Debug, Getters, Clone, HasEncoding, NomReader)]
pub struct OperationMessage {
    #[get = "pub"]
    operation: Operation,
}

impl From<Operation> for OperationMessage {
    fn from(operation: Operation) -> Self {
        Self { operation }
    }
}

impl From<OperationMessage> for Operation {
    fn from(msg: OperationMessage) -> Self {
        msg.operation
    }
}

// -----------------------------------------------------------------------------------------------
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, HasEncoding, NomReader)]
pub struct Operation {
    branch: BlockHash,
    #[encoding(list = "OPERATION_MAX_SIZE")]
    data: Vec<u8>,
}

impl Operation {
    pub fn branch(&self) -> &BlockHash {
        &self.branch
    }

    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }
}

#[derive(Error, Debug)]
pub enum FromDecodedOperationError {
    #[error("Failed to decode from base58 string: {0}")]
    Base58(FromBase58CheckError),
    #[error("Failed to decode from hex string: {0}")]
    Hex(FromHexError),
}

impl From<FromBase58CheckError> for FromDecodedOperationError {
    fn from(source: FromBase58CheckError) -> Self {
        Self::Base58(source)
    }
}

impl From<FromHexError> for FromDecodedOperationError {
    fn from(source: FromHexError) -> Self {
        Self::Hex(source)
    }
}

impl TryFrom<DecodedOperation> for Operation {
    type Error = FromDecodedOperationError;
    fn try_from(dop: DecodedOperation) -> Result<Operation, FromDecodedOperationError> {
        Ok(Operation {
            branch: BlockHash::from_base58_check(&dop.branch)?,
            data: hex::decode(&dop.data)?,
        })
    }
}

// -----------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct DecodedOperation {
    branch: String,
    data: String,
}

impl From<Operation> for DecodedOperation {
    fn from(op: Operation) -> DecodedOperation {
        DecodedOperation {
            branch: op.branch().to_base58_check(),
            data: hex::encode(op.data()),
        }
    }
}

// -----------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Getters, Clone, HasEncoding, NomReader)]
pub struct GetOperationsMessage {
    #[get = "pub"]
    #[encoding(dynamic, list = "GET_OPERATIONS_MAX_LENGTH")]
    get_operations: Vec<OperationHash>,
}

impl GetOperationsMessage {
    pub fn new(operations: Vec<OperationHash>) -> Self {
        Self {
            get_operations: operations,
        }
    }
}
