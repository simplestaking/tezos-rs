// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This crate provides functions for manipulation with 'public key'.
//! Tezos uses this kinds: edpk(ed25519), sppk(secp256k1), p2pk(p256)

use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use crypto::hash::{PublicKeyEd25519, PublicKeyP256, PublicKeySecp256k1};

use crate::base::ConversionError;

/// This is a wrapper for Signature.PublicKey, which tezos uses with different curves: edpk(ed25519), sppk(secp256k1), p2pk(p256) and smart contracts
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SignaturePublicKey {
    Ed25519(PublicKeyEd25519),
    Secp256k1(PublicKeySecp256k1),
    P256(PublicKeyP256),
}

impl SignaturePublicKey {
    #[inline]
    pub fn to_string_representation(&self) -> String {
        match self {
            SignaturePublicKey::Ed25519(h) => h.to_base58_check(),
            SignaturePublicKey::Secp256k1(h) => h.to_base58_check(),
            SignaturePublicKey::P256(h) => h.to_base58_check(),
        }
    }

    #[inline]
    pub fn from_b58_hash(b58_hash: &str) -> Result<SignaturePublicKey, ConversionError> {
        if b58_hash.len() > 4 {
            match &b58_hash[0..4] {
                "edpk" => Ok(SignaturePublicKey::Ed25519(b58_hash.try_into()?)),
                "sppk" => Ok(SignaturePublicKey::Secp256k1(b58_hash.try_into()?)),
                "p2pk" => Ok(SignaturePublicKey::P256(b58_hash.try_into()?)),
                _ => Err(ConversionError::InvalidCurveTag {
                    curve_tag: String::from(&b58_hash[0..4]),
                }),
            }
        } else {
            Err(ConversionError::InvalidHash {
                hash: b58_hash.to_string(),
            })
        }
    }

    #[inline]
    pub fn from_hex_hash_and_curve(
        hash: &str,
        curve: &str,
    ) -> Result<SignaturePublicKey, ConversionError> {
        if hash.len() == 66 || hash.len() == 64 {
            let hash = hex::decode(hash)?;
            let public_hash_key = match curve {
                "ed25519" => SignaturePublicKey::Ed25519(hash.try_into()?),
                "secp256k1" => SignaturePublicKey::Secp256k1(hash.try_into()?),
                "p256" => SignaturePublicKey::P256(hash.try_into()?),
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
    //

    /// convert public key byte string to contract id, e.g. data from context can be in this format
    ///
    /// 1 byte tag and - 32 bytes for ed25519 (tz1)
    ///                - 33 bytes for secp256k1 (tz2) and p256 (tz3)
    ///
    /// # Arguments
    ///
    /// * `pk` - public key in byte string format
    #[inline]
    pub fn from_tagged_bytes(pk: Vec<u8>) -> Result<SignaturePublicKey, ConversionError> {
        if pk.len() == 33 || pk.len() == 34 {
            let tag = pk[1];

            match tag {
                0 => Self::from_hex_hash_and_curve(&hex::encode(&pk[2..]), "ed25519"),
                1 => Self::from_hex_hash_and_curve(&hex::encode(&pk[2..]), "secp256k1"),
                2 => Self::from_hex_hash_and_curve(&hex::encode(&pk[2..]), "p256"),
                _ => Err(ConversionError::InvalidPublicKey),
            }
        } else {
            Err(ConversionError::InvalidPublicKey)
        }
    }
}

#[cfg(test)]
mod tests {
    use crypto::hash::{PublicKeyEd25519, PublicKeyP256, PublicKeySecp256k1};

    use crate::base::signature_public_key::SignaturePublicKey;

    //tz1TEZtYnuLiZLdA6c7JysAUJcHMrogu4Cpr - edpkv2CiwuithtFAYEvH3QKfrJkq4JZuL4YS7i9W1vaKFfHZHLP2JP
    //tz2TSvNTh2epDMhZHrw73nV9piBX7kLZ9K9m - sppk7bn9MKAWDUFwqowcxA1zJgp12yn2kEnMQJP3WmqSZ4W8WQhLqJN
    //tz3bEQoFCZEEfZMskefZ8q8e4eiHH1pssRax - p2pk66G3vbHoscNYJdgQU72xSkrCWzoXNnFwroADcRTUtrHDvwnUNyW

    #[test]
    fn test_from_b58_hash() -> Result<(), anyhow::Error> {
        let decoded = decode_pk("edpkv2CiwuithtFAYEvH3QKfrJkq4JZuL4YS7i9W1vaKFfHZHLP2JP");
        assert!(decoded.is_some());

        let expected_valid_bytes = [
            181, 154, 48, 170, 159, 163, 206, 35, 84, 17, 234, 205, 0, 80, 66, 141, 114, 204, 77,
            76, 204, 108, 83, 76, 39, 206, 128, 206, 247, 170, 72, 113,
        ];
        let decoded = decoded.unwrap();
        assert_eq!(decoded, expected_valid_bytes);
        println!("{}", hex::encode(&expected_valid_bytes));

        let decoded = decode_pk("sppk7bn9MKAWDUFwqowcxA1zJgp12yn2kEnMQJP3WmqSZ4W8WQhLqJN");
        assert!(decoded.is_some());

        let expected_valid_bytes = vec![
            3, 69, 218, 140, 119, 94, 111, 160, 6, 57, 131, 202, 89, 148, 129, 168, 50, 167, 185,
            155, 207, 103, 33, 252, 0, 241, 225, 115, 122, 207, 237, 248, 197,
        ];
        let decoded = decoded.unwrap();
        assert_eq!(decoded, expected_valid_bytes);
        println!("{}", hex::encode(&expected_valid_bytes));

        let decoded = decode_pk("p2pk66G3vbHoscNYJdgQU72xSkrCWzoXNnFwroADcRTUtrHDvwnUNyW");
        assert!(decoded.is_some());

        let expected_valid_bytes = vec![
            2, 222, 68, 156, 232, 235, 225, 129, 250, 81, 252, 118, 231, 35, 189, 211, 21, 80, 11,
            178, 189, 66, 159, 204, 141, 166, 10, 113, 79, 176, 188, 68, 228,
        ];
        let decoded = decoded.unwrap();
        assert_eq!(decoded, expected_valid_bytes);
        println!("{}", hex::encode(&expected_valid_bytes));

        Ok(())
    }

    #[test]
    fn test_from_hex_hash_and_curve() -> Result<(), anyhow::Error> {
        let result = SignaturePublicKey::from_hex_hash_and_curve(
            &"b59a30aa9fa3ce235411eacd0050428d72cc4d4ccc6c534c27ce80cef7aa4871",
            &"ed25519",
        )?;
        assert_eq!(
            result.to_string_representation().as_str(),
            "edpkv2CiwuithtFAYEvH3QKfrJkq4JZuL4YS7i9W1vaKFfHZHLP2JP"
        );

        let result = SignaturePublicKey::from_hex_hash_and_curve(
            &"0345da8c775e6fa0063983ca599481a832a7b99bcf6721fc00f1e1737acfedf8c5",
            &"secp256k1",
        )?;
        assert_eq!(
            result.to_string_representation().as_str(),
            "sppk7bn9MKAWDUFwqowcxA1zJgp12yn2kEnMQJP3WmqSZ4W8WQhLqJN"
        );

        let result = SignaturePublicKey::from_hex_hash_and_curve(
            &"02de449ce8ebe181fa51fc76e723bdd315500bb2bd429fcc8da60a714fb0bc44e4",
            &"p256",
        )?;
        assert_eq!(
            result.to_string_representation().as_str(),
            "p2pk66G3vbHoscNYJdgQU72xSkrCWzoXNnFwroADcRTUtrHDvwnUNyW"
        );

        Ok(())
    }

    fn decode_pk(pk: &str) -> Option<Vec<u8>> {
        let decoded = SignaturePublicKey::from_b58_hash(pk).unwrap();
        Some(match decoded {
            SignaturePublicKey::Ed25519(PublicKeyEd25519(hash))
            | SignaturePublicKey::P256(PublicKeyP256(hash))
            | SignaturePublicKey::Secp256k1(PublicKeySecp256k1(hash)) => hash,
        })
    }
}
