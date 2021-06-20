// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This crate provides functions for manipulation with 'public key hash'.
//! Tezos uses this kinds: tz1(ed25519), tz2 (secp256k1), tz3(p256)

use std::convert::TryInto;

use hex::FromHexError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crypto::blake2b;
use crypto::hash::FromBytesError;
use crypto::hash::{ContractTz1Hash, ContractTz2Hash, ContractTz3Hash};
use crypto::{base58::FromBase58CheckError, blake2b::Blake2bError};

#[derive(Debug, Error, PartialEq)]
pub enum ConversionError {
    #[error("Conversion from invalid public key")]
    InvalidPublicKey,

    #[error("Invalid hash: {hash}")]
    InvalidHash { hash: String },

    #[error("Invalid curve tag: {curve_tag}")]
    InvalidCurveTag { curve_tag: String },

    #[error("Blake2b digest error")]
    Blake2bError,
}

impl From<hex::FromHexError> for ConversionError {
    fn from(error: FromHexError) -> Self {
        ConversionError::InvalidHash {
            hash: error.to_string(),
        }
    }
}

impl From<FromBase58CheckError> for ConversionError {
    fn from(error: FromBase58CheckError) -> Self {
        ConversionError::InvalidHash {
            hash: error.to_string(),
        }
    }
}

impl From<FromBytesError> for ConversionError {
    fn from(error: FromBytesError) -> Self {
        ConversionError::InvalidHash {
            hash: error.to_string(),
        }
    }
}

impl From<Blake2bError> for ConversionError {
    fn from(_: Blake2bError) -> Self {
        ConversionError::Blake2bError
    }
}

/// This is a wrapper for Signature.PublicKeyHash, which tezos uses with different curves: tz1(ed25519), tz2 (secp256k1), tz3(p256).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SignaturePublicKeyHash {
    Ed25519(ContractTz1Hash),
    Secp256k1(ContractTz2Hash),
    P256(ContractTz3Hash),
}

impl SignaturePublicKeyHash {
    #[inline]
    pub fn to_string_representation(&self) -> String {
        match self {
            SignaturePublicKeyHash::Ed25519(h) => h.to_base58_check(),
            SignaturePublicKeyHash::Secp256k1(h) => h.to_base58_check(),
            SignaturePublicKeyHash::P256(h) => h.to_base58_check(),
        }
    }

    #[inline]
    pub fn from_hex_hash_and_curve(
        hash: &str,
        curve: &str,
    ) -> Result<SignaturePublicKeyHash, ConversionError> {
        if hash.len() == 40 {
            let hash = hex::decode(hash)?;
            let public_hash_key = match curve {
                "ed25519" => SignaturePublicKeyHash::Ed25519(hash.try_into()?),
                "secp256k1" => SignaturePublicKeyHash::Secp256k1(hash.try_into()?),
                "p256" => SignaturePublicKeyHash::P256(hash.try_into()?),
                _ => {
                    return Err(ConversionError::InvalidCurveTag {
                        curve_tag: curve.to_string(),
                    })
                }
            };
            Ok(public_hash_key)
        } else {
            Err(ConversionError::InvalidHash {
                hash: hash.to_string(),
            })
        }
    }

    #[inline]
    pub fn from_b58_hash(b58_hash: &str) -> Result<SignaturePublicKeyHash, ConversionError> {
        if b58_hash.len() > 3 {
            match &b58_hash[0..3] {
                "tz1" => Ok(SignaturePublicKeyHash::Ed25519(
                    ContractTz1Hash::from_base58_check(b58_hash)?,
                )),
                "tz2" => Ok(SignaturePublicKeyHash::Secp256k1(
                    ContractTz2Hash::from_base58_check(b58_hash)?,
                )),
                "tz3" => Ok(SignaturePublicKeyHash::P256(
                    ContractTz3Hash::from_base58_check(b58_hash)?,
                )),
                _ => Err(ConversionError::InvalidCurveTag {
                    curve_tag: String::from(&b58_hash[0..3]),
                }),
            }
        } else {
            Err(ConversionError::InvalidHash {
                hash: b58_hash.to_string(),
            })
        }
    }

    /// convert public key byte string to contract id, e.g. data from context can be in this format
    ///
    /// 1 byte tag and - 32 bytes for ed25519 (tz1)
    ///                 - 33 bytes for secp256k1 (tz2) and p256 (tz3)
    ///
    /// # Arguments
    ///
    /// * `pk` - public key in byte string format
    #[inline]
    pub fn from_tagged_bytes(pk: Vec<u8>) -> Result<SignaturePublicKeyHash, ConversionError> {
        if pk.len() == 33 || pk.len() == 34 {
            let tag = pk[0];
            let hash = blake2b::digest_160(&pk[1..])?;

            match tag {
                0 => Self::from_hex_hash_and_curve(&hex::encode(hash), "ed25519"),
                1 => Self::from_hex_hash_and_curve(&hex::encode(hash), "secp256k1"),
                2 => Self::from_hex_hash_and_curve(&hex::encode(hash), "p256"),
                _ => Err(ConversionError::InvalidPublicKey),
            }
        } else {
            Err(ConversionError::InvalidPublicKey)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::base::signature_public_key_hash::{ConversionError, SignaturePublicKeyHash};

    #[test]
    fn test_ed25519_from_bytes() -> Result<(), anyhow::Error> {
        let valid_pk = vec![
            0, 3, 65, 14, 206, 174, 244, 127, 36, 48, 150, 156, 243, 27, 213, 139, 41, 30, 231,
            173, 127, 97, 192, 177, 142, 31, 107, 197, 219, 246, 111, 155, 121,
        ];
        let short_pk = vec![
            0, 3, 65, 14, 206, 174, 244, 127, 36, 48, 150, 156, 243, 27, 213, 139, 41, 30, 231,
            173, 127, 97, 192, 177, 142, 31, 107, 197, 219,
        ];
        let wrong_tag_pk = vec![
            4, 3, 65, 14, 206, 174, 244, 127, 36, 48, 150, 156, 243, 27, 213, 139, 41, 30, 231,
            173, 127, 97, 192, 177, 142, 31, 107, 197, 219, 246, 111, 155, 121,
        ];

        let decoded = SignaturePublicKeyHash::from_tagged_bytes(valid_pk)?;
        let decoded = match decoded {
            SignaturePublicKeyHash::Ed25519(hash) => Some(hash),
            _ => None,
        };
        assert!(decoded.is_some());

        let decoded = decoded.map(|h| h.to_base58_check()).unwrap();
        assert_eq!("tz1PirboZKFVqkfE45hVLpkpXaZtLk3mqC17", decoded);

        let result = SignaturePublicKeyHash::from_tagged_bytes(short_pk);
        assert_eq!(result, Err(ConversionError::InvalidPublicKey));

        let result = SignaturePublicKeyHash::from_tagged_bytes(wrong_tag_pk);
        assert_eq!(result, Err(ConversionError::InvalidPublicKey));

        Ok(())
    }

    #[test]
    fn test_ed25519_from_b58_hash() -> Result<(), anyhow::Error> {
        let decoded =
            SignaturePublicKeyHash::from_b58_hash("tz1PirboZKFVqkfE45hVLpkpXaZtLk3mqC17")?;
        let decoded = match decoded {
            SignaturePublicKeyHash::Ed25519(hash) => Some(hash),
            _ => None,
        };
        assert!(decoded.is_some());

        let expected_valid_pk = hex::decode("2cca28ab019ae2d8c26f4ce4924cad67a2dc6618")?;
        let decoded = decoded.unwrap();
        assert_eq!(decoded.as_ref(), &expected_valid_pk);
        Ok(())
    }

    #[test]
    fn test_from_hex_hash_and_curve() -> Result<(), anyhow::Error> {
        let result = SignaturePublicKeyHash::from_hex_hash_and_curve(
            &"2cca28ab019ae2d8c26f4ce4924cad67a2dc6618",
            &"ed25519",
        )?;
        assert_eq!(
            result.to_string_representation().as_str(),
            "tz1PirboZKFVqkfE45hVLpkpXaZtLk3mqC17"
        );

        let result = SignaturePublicKeyHash::from_hex_hash_and_curve(
            &"20262e6195b91181f1713c4237c8195096b8adc9",
            &"secp256k1",
        )?;
        assert_eq!(
            result.to_string_representation().as_str(),
            "tz2BFE2MEHhphgcR7demCGQP2k1zG1iMj1oj"
        );

        let result = SignaturePublicKeyHash::from_hex_hash_and_curve(
            &"6fde46af0356a0476dae4e4600172dc9309b3aa4",
            &"p256",
        )?;
        assert_eq!(
            result.to_string_representation().as_str(),
            "tz3WXYtyDUNL91qfiCJtVUX746QpNv5i5ve5"
        );

        let result = SignaturePublicKeyHash::from_hex_hash_and_curve(
            &"2cca28ab019ae2d8c26f4ce4924cad67a2dc6618",
            &"invalidcurvetag",
        );
        assert_eq!(
            result.unwrap_err(),
            ConversionError::InvalidCurveTag {
                curve_tag: "invalidcurvetag".to_string()
            }
        );

        let result = SignaturePublicKeyHash::from_hex_hash_and_curve(
            &"2cca28a6f4ce4924cad67a2dc6618",
            &"ed25519",
        );
        assert_eq!(
            result.unwrap_err(),
            ConversionError::InvalidHash {
                hash: "2cca28a6f4ce4924cad67a2dc6618".to_string()
            }
        );

        let result = SignaturePublicKeyHash::from_hex_hash_and_curve(
            &"56a0476dae4e4600172dc9309b3aa4",
            &"p256",
        );
        assert_eq!(
            result.unwrap_err(),
            ConversionError::InvalidHash {
                hash: "56a0476dae4e4600172dc9309b3aa4".to_string()
            }
        );

        Ok(())
    }
}
