// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![forbid(unsafe_code)]

use failure::Fail;

#[macro_use]
pub mod blake2b;
pub mod base58;
pub mod crypto_box;
pub mod nonce;
pub mod proof_of_work;
pub mod seeded_step;
#[macro_use]
pub mod hash;

#[derive(Debug, Fail)]
pub enum CryptoError {
    #[fail(display = "Invalid crypto key, reason: {}", reason)]
    InvalidKey { reason: String },
    #[fail(
        display = "Invalid crypto key size - expected: {}, actual: {}",
        expected, actual
    )]
    InvalidKeySize { expected: usize, actual: usize },
    #[fail(
        display = "Invalid nonce size - expected: {}, actual: {}",
        expected, actual
    )]
    InvalidNonceSize { expected: usize, actual: usize },
    #[fail(display = "Failed to decrypt")]
    FailedToDecrypt,
}
