// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![forbid(unsafe_code)]

use thiserror::Error;

#[macro_use]
pub mod blake2b;
pub mod base58;
pub mod crypto_box;
pub mod nonce;
pub mod proof_of_work;
pub mod seeded_step;
#[macro_use]
pub mod hash;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("Invalid crypto key, reason: {reason}")]
    InvalidKey { reason: String },
    #[error("Invalid crypto key size - expected: {expected}, actual: {actual}")]
    InvalidKeySize { expected: usize, actual: usize },
    #[error("Invalid nonce size - expected: {expected}, actual: {actual}")]
    InvalidNonceSize { expected: usize, actual: usize },
    #[error("Failed to decrypt")]
    FailedToDecrypt,
}
