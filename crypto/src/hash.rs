// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryFrom;

use crate::{
    base58::{FromBase58Check, FromBase58CheckError, ToBase58Check},
    blake2b::Blake2bError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod prefix_bytes {
    pub const CHAIN_ID: [u8; 3] = [87, 82, 0];
    pub const BLOCK_HASH: [u8; 2] = [1, 52];
    pub const BLOCK_METADATA_HASH: [u8; 2] = [234, 249];
    pub const CONTEXT_HASH: [u8; 2] = [79, 199];
    pub const OPERATION_HASH: [u8; 2] = [5, 116];
    pub const OPERATION_LIST_LIST_HASH: [u8; 3] = [29, 159, 109];
    pub const OPERATION_METADATA_HASH: [u8; 2] = [5, 183];
    pub const OPERATION_METADATA_LIST_LIST_HASH: [u8; 3] = [29, 159, 182];
    pub const PROTOCOL_HASH: [u8; 2] = [2, 170];
    pub const CRYPTOBOX_PUBLIC_KEY_HASH: [u8; 2] = [153, 103];
    pub const CONTRACT_KT1_HASH: [u8; 3] = [2, 90, 121];
    pub const CONTRACT_TZ1_HASH: [u8; 3] = [6, 161, 159];
    pub const CONTRACT_TZ2_HASH: [u8; 3] = [6, 161, 161];
    pub const CONTRACT_TZ3_HASH: [u8; 3] = [6, 161, 164];
    pub const PUBLIC_KEY_ED25519: [u8; 4] = [13, 15, 37, 217];
    pub const PUBLIC_KEY_SECP256K1: [u8; 4] = [3, 254, 226, 86];
    pub const PUBLIC_KEY_P256: [u8; 4] = [3, 178, 139, 127];
}

pub type Hash = Vec<u8>;

pub trait HashTrait: Into<Hash> + AsRef<Hash> {
    /// Returns this hash type.
    fn hash_type() -> HashType;

    /// Returns the size of this hash.
    fn hash_size() -> usize {
        Self::hash_type().size()
    }

    /// Tries to create this hash from the `bytes`.
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, FromBytesError>;
}

/// Error creating hash from bytes
#[derive(Debug, Error, PartialEq)]
pub enum FromBytesError {
    /// Invalid data size
    #[error("invalid hash size")]
    InvalidSize,
}

macro_rules! define_hash {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            Serialize,
            Deserialize,
            std::cmp::PartialOrd,
            std::cmp::Ord,
            std::hash::Hash,
        )]
        pub struct $name(pub Hash);

        impl $name {
            pub fn from_base58_check(data: &str) -> Result<Self, FromBase58CheckError> {
                HashType::$name.b58check_to_hash(data).map(Self)
            }

            fn from_bytes(data: &[u8]) -> Result<Self, FromBytesError> {
                if data.len() == HashType::$name.size() {
                    Ok($name(data.into()))
                } else {
                    Err(FromBytesError::InvalidSize)
                }
            }

            fn from_vec(hash: Vec<u8>) -> Result<Self, FromBytesError> {
                if hash.len() == HashType::$name.size() {
                    Ok($name(hash))
                } else {
                    Err(FromBytesError::InvalidSize)
                }
            }

            pub fn to_base58_check(&self) -> String {
                // TODO: Fixing TE-373 will allow to get rid of this `unreachable`
                HashType::$name
                    .hash_to_b58check(&self.0)
                    .unwrap_or_else(|_| {
                        unreachable!("Typed hash should always be representable in base58")
                    })
            }
        }

        impl std::str::FromStr for $name {
            type Err = FromBase58CheckError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::from_base58_check(s)
            }
        }

        impl HashTrait for $name {
            fn hash_type() -> HashType {
                HashType::$name
            }

            fn try_from_bytes(bytes: &[u8]) -> Result<Self, FromBytesError> {
                $name::try_from(bytes)
            }
        }

        impl std::convert::AsRef<Hash> for $name {
            fn as_ref(&self) -> &Hash {
                &self.0
            }
        }

        impl std::convert::Into<Hash> for $name {
            fn into(self) -> Hash {
                self.0
            }
        }

        impl std::convert::TryFrom<&[u8]> for $name {
            type Error = FromBytesError;
            fn try_from(h: &[u8]) -> Result<Self, Self::Error> {
                Self::from_bytes(h)
            }
        }

        impl std::convert::TryFrom<Hash> for $name {
            type Error = FromBytesError;
            fn try_from(h: Hash) -> Result<Self, Self::Error> {
                Self::from_vec(h)
            }
        }

        impl std::convert::TryFrom<&str> for $name {
            type Error = FromBase58CheckError;
            fn try_from(encoded: &str) -> Result<Self, Self::Error> {
                Self::from_base58_check(encoded)
            }
        }
    };
}

define_hash!(ChainId);
define_hash!(BlockHash);
define_hash!(BlockMetadataHash);
define_hash!(OperationHash);
define_hash!(OperationListListHash);
define_hash!(OperationMetadataHash);
define_hash!(OperationMetadataListListHash);
define_hash!(ContextHash);
define_hash!(ProtocolHash);
define_hash!(ContractKt1Hash);
define_hash!(ContractTz1Hash);
define_hash!(ContractTz2Hash);
define_hash!(ContractTz3Hash);
define_hash!(CryptoboxPublicKeyHash);
define_hash!(PublicKeyEd25519);
define_hash!(PublicKeySecp256k1);
define_hash!(PublicKeyP256);

/// Note: see Tezos ocaml lib_crypto/base58.ml
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum HashType {
    // "\087\082\000" (* Net(15) *)
    ChainId,
    // "\001\052" (* B(51) *)
    BlockHash,
    // "\234\249" (* bm(52) *)
    BlockMetadataHash,
    // "\002\170" (* P(51) *)
    ProtocolHash,
    // "\079\199" (* Co(52) *)
    ContextHash,
    // "\005\116" (* o(51) *)
    OperationHash,
    // "\029\159\109" (* LLo(53) *)
    OperationListListHash,
    // "\005\183" (* r(51) *)
    OperationMetadataHash,
    // "\029\159\182" (* LLr(53) *)
    OperationMetadataListListHash,
    // "\153\103" (* id(30) *)
    CryptoboxPublicKeyHash,
    // "\002\090\121" (* KT1(36) *)
    ContractKt1Hash,
    // "\006\161\159" (* tz1(36) *)
    ContractTz1Hash,
    // "\006\161\161" (* tz2(36) *)
    ContractTz2Hash,
    // "\006\161\164" (* tz3(36) *)
    ContractTz3Hash,
    // "\013\015\037\217" (* edpk(54) *)
    PublicKeyEd25519,
    // "\003\254\226\086" (* sppk(55) *)
    PublicKeySecp256k1,
    // "\003\178\139\127" (* p2pk(55) *)
    PublicKeyP256,
}

impl HashType {
    #[inline]
    pub fn base58check_prefix(&self) -> &'static [u8] {
        use prefix_bytes::*;
        match self {
            HashType::ChainId => &CHAIN_ID,
            HashType::BlockHash => &BLOCK_HASH,
            HashType::BlockMetadataHash => &BLOCK_METADATA_HASH,
            HashType::ContextHash => &CONTEXT_HASH,
            HashType::ProtocolHash => &PROTOCOL_HASH,
            HashType::OperationHash => &OPERATION_HASH,
            HashType::OperationListListHash => &OPERATION_LIST_LIST_HASH,
            HashType::OperationMetadataHash => &OPERATION_METADATA_HASH,
            HashType::OperationMetadataListListHash => &OPERATION_METADATA_LIST_LIST_HASH,
            HashType::CryptoboxPublicKeyHash => &CRYPTOBOX_PUBLIC_KEY_HASH,
            HashType::ContractKt1Hash => &CONTRACT_KT1_HASH,
            HashType::ContractTz1Hash => &CONTRACT_TZ1_HASH,
            HashType::ContractTz2Hash => &CONTRACT_TZ2_HASH,
            HashType::ContractTz3Hash => &CONTRACT_TZ3_HASH,
            HashType::PublicKeyEd25519 => &PUBLIC_KEY_ED25519,
            HashType::PublicKeySecp256k1 => &PUBLIC_KEY_SECP256K1,
            HashType::PublicKeyP256 => &PUBLIC_KEY_P256,
        }
    }

    /// Size of hash in bytes
    pub const fn size(&self) -> usize {
        match self {
            HashType::ChainId => 4,
            HashType::BlockHash
            | HashType::BlockMetadataHash
            | HashType::ContextHash
            | HashType::ProtocolHash
            | HashType::OperationHash
            | HashType::OperationListListHash
            | HashType::OperationMetadataHash
            | HashType::OperationMetadataListListHash
            | HashType::PublicKeyEd25519 => 32,
            HashType::CryptoboxPublicKeyHash => 16,
            HashType::ContractKt1Hash
            | HashType::ContractTz1Hash
            | HashType::ContractTz2Hash
            | HashType::ContractTz3Hash => 20,
            HashType::PublicKeySecp256k1 | HashType::PublicKeyP256 => 33,
        }
    }

    /// Convert hash byte representation into string.
    pub fn hash_to_b58check(&self, data: &[u8]) -> Result<String, FromBytesError> {
        if self.size() != data.len() {
            Err(FromBytesError::InvalidSize)
        } else {
            let mut hash = Vec::with_capacity(self.base58check_prefix().len() + data.len());
            hash.extend(self.base58check_prefix());
            hash.extend(data);
            hash.to_base58check()
                // currently the error is returned if the input lenght exceeds 128 bytes
                // that is the limitation of base58 crate, and there are no hash types
                // exceeding that limit.
                // TODO: remove this when TE-373 is implemented
                .map_err(|_| unreachable!("Hash size should not exceed allowed 128 bytes"))
        }
    }

    /// Convert string representation of the hash to bytes form.
    pub fn b58check_to_hash(&self, data: &str) -> Result<Hash, FromBase58CheckError> {
        let mut hash = data.from_base58check()?;
        let expected_len = self.size() + self.base58check_prefix().len();
        if expected_len != hash.len() {
            return Err(FromBase58CheckError::MismatchedLength {
                expected: expected_len,
                actual: hash.len(),
            });
        }
        // prefix is not present in a binary representation
        hash.drain(0..self.base58check_prefix().len());
        Ok(hash)
    }
}

/// Implementation of chain_id.ml -> of_block_hash
#[inline]
pub fn chain_id_from_block_hash(block_hash: &BlockHash) -> Result<ChainId, Blake2bError> {
    let result = crate::blake2b::digest_256(&block_hash.0)?;
    Ok(ChainId::from_bytes(&result[0..HashType::ChainId.size()])
        .unwrap_or_else(|_| unreachable!("ChainId is created from slice of correct size")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto_box::PublicKey;
    use hex::FromHex;

    #[test]
    fn test_encode_chain_id() -> Result<(), anyhow::Error> {
        let decoded = HashType::ChainId.hash_to_b58check(&hex::decode("8eceda2f")?)?;
        let expected = "NetXgtSLGNJvNye";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_chain_id_to_b58_string() -> Result<(), anyhow::Error> {
        let encoded = &ChainId::from_bytes(&hex::decode("8eceda2f")?)?.to_base58_check();
        let expected = "NetXgtSLGNJvNye";
        assert_eq!(expected, encoded);

        Ok(())
    }

    #[test]
    fn test_chain_id_to_base58_check() -> Result<(), anyhow::Error> {
        let encoded = ChainId::from_bytes(&hex::decode("8eceda2f")?)?.to_base58_check();
        let expected = "NetXgtSLGNJvNye";
        assert_eq!(expected, encoded);

        Ok(())
    }

    #[test]
    fn test_chain_id_from_block_hash() -> Result<(), anyhow::Error> {
        let decoded_chain_id: ChainId = chain_id_from_block_hash(&BlockHash::from_base58_check(
            "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe",
        )?)?;
        let decoded_chain_id: &str = &decoded_chain_id.to_base58_check();
        let expected_chain_id = "NetXgtSLGNJvNye";
        assert_eq!(expected_chain_id, decoded_chain_id);

        let decoded_chain_id: ChainId = chain_id_from_block_hash(&BlockHash::from_base58_check(
            "BLockGenesisGenesisGenesisGenesisGenesisd6f5afWyME7",
        )?)?;
        let decoded_chain_id: &str = &decoded_chain_id.to_base58_check();
        let expected_chain_id = "NetXjD3HPJJjmcd";
        assert_eq!(expected_chain_id, decoded_chain_id);

        Ok(())
    }

    #[test]
    fn test_encode_block_header_genesis() -> Result<(), anyhow::Error> {
        let encoded = HashType::BlockHash.hash_to_b58check(&hex::decode(
            "8fcf233671b6a04fcf679d2a381c2544ea6c1ea29ba6157776ed8424affa610d",
        )?)?;
        let expected = "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe";
        assert_eq!(expected, encoded);

        Ok(())
    }

    #[test]
    fn test_encode_block_header_genesis_new() -> Result<(), anyhow::Error> {
        let encoded = BlockHash::from_bytes(&hex::decode(
            "8fcf233671b6a04fcf679d2a381c2544ea6c1ea29ba6157776ed8424affa610d",
        )?)?
        .to_base58_check();
        let expected = "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe";
        assert_eq!(expected, encoded);

        Ok(())
    }

    #[test]
    fn test_encode_block_header() -> Result<(), anyhow::Error> {
        let encoded = HashType::BlockHash.hash_to_b58check(&hex::decode(
            "46a6aefde9243ae18b191a8d010b7237d5130b3530ce5d1f60457411b2fa632d",
        )?)?;
        let expected = "BLFQ2JjYWHC95Db21cRZC4cgyA1mcXmx1Eg6jKywWy9b8xLzyK9";
        assert_eq!(expected, encoded);

        Ok(())
    }

    #[test]
    fn test_encode_block_header_new() -> Result<(), anyhow::Error> {
        let encoded = BlockHash::from_bytes(&hex::decode(
            "46a6aefde9243ae18b191a8d010b7237d5130b3530ce5d1f60457411b2fa632d",
        )?)?
        .to_base58_check();
        let expected = "BLFQ2JjYWHC95Db21cRZC4cgyA1mcXmx1Eg6jKywWy9b8xLzyK9";
        assert_eq!(expected, encoded);

        Ok(())
    }

    #[test]
    fn test_encode_context() -> Result<(), anyhow::Error> {
        let decoded = HashType::ContextHash.hash_to_b58check(&hex::decode(
            "934484026d24be9ad40c98341c20e51092dd62bbf470bb9ff85061fa981ebbd9",
        )?)?;
        let expected = "CoVmAcMV64uAQo8XvfLr9VDuz7HVZLT4cgK1w1qYmTjQNbGwQwDd";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_encode_context_new() -> Result<(), anyhow::Error> {
        let encoded = ContextHash::from_bytes(&hex::decode(
            "934484026d24be9ad40c98341c20e51092dd62bbf470bb9ff85061fa981ebbd9",
        )?)?
        .to_base58_check();
        let expected = "CoVmAcMV64uAQo8XvfLr9VDuz7HVZLT4cgK1w1qYmTjQNbGwQwDd";
        assert_eq!(expected, encoded);

        Ok(())
    }

    #[test]
    fn test_encode_operations_hash() -> Result<(), anyhow::Error> {
        let decoded = HashType::OperationListListHash.hash_to_b58check(&hex::decode(
            "acecbfac449678f1d68b90c7b7a86c9280fd373d872e072f3fb1b395681e7149",
        )?)?;
        let expected = "LLoads9N8uB8v659hpNhpbrLzuzLdUCjz5euiR6Lm2hd7C6sS2Vep";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_encode_operations_hash_new() -> Result<(), anyhow::Error> {
        let encoded = OperationListListHash::from_bytes(&hex::decode(
            "acecbfac449678f1d68b90c7b7a86c9280fd373d872e072f3fb1b395681e7149",
        )?)?
        .to_base58_check();
        let expected = "LLoads9N8uB8v659hpNhpbrLzuzLdUCjz5euiR6Lm2hd7C6sS2Vep";
        assert_eq!(expected, encoded);

        Ok(())
    }

    #[test]
    fn test_encode_public_key_hash() -> Result<(), anyhow::Error> {
        let pk = PublicKey::from_hex(
            "2cc1b580f4b8b1f6dbd0aa1d9cde2655c2081c07d7e61249aad8b11d954fb01a",
        )?;
        let pk_hash = pk.public_key_hash()?;
        let decoded = HashType::CryptoboxPublicKeyHash.hash_to_b58check(pk_hash.as_ref())?;
        let expected = "idsg2wkkDDv2cbEMK4zH49fjgyn7XT";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_encode_public_key_hash_new() -> Result<(), anyhow::Error> {
        let pk = PublicKey::from_hex(
            "2cc1b580f4b8b1f6dbd0aa1d9cde2655c2081c07d7e61249aad8b11d954fb01a",
        )?;
        let pk_hash = pk.public_key_hash()?;
        let decoded = CryptoboxPublicKeyHash::from_bytes(pk_hash.as_ref())?.to_base58_check();
        let expected = "idsg2wkkDDv2cbEMK4zH49fjgyn7XT";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_encode_contract_tz1() -> Result<(), anyhow::Error> {
        let decoded = HashType::ContractTz1Hash
            .hash_to_b58check(&hex::decode("83846eddd5d3c5ed96e962506253958649c84a74")?)?;
        let expected = "tz1XdRrrqrMfsFKA8iuw53xHzug9ipr6MuHq";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_encode_contract_tz1_new() -> Result<(), anyhow::Error> {
        let decoded =
            ContractTz1Hash::from_bytes(&hex::decode("83846eddd5d3c5ed96e962506253958649c84a74")?)?
                .to_base58_check();
        let expected = "tz1XdRrrqrMfsFKA8iuw53xHzug9ipr6MuHq";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_encode_contract_tz2() -> Result<(), anyhow::Error> {
        let decoded = HashType::ContractTz2Hash
            .hash_to_b58check(&hex::decode("2fcb1d9307f0b1f94c048ff586c09f46614c7e90")?)?;
        let expected = "tz2Cfwk4ortcaqAGcVJKSxLiAdcFxXBLBoyY";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_encode_contract_tz3() -> Result<(), anyhow::Error> {
        let decoded = HashType::ContractTz3Hash
            .hash_to_b58check(&hex::decode("193b2b3f6b8f8e1e6b39b4d442fc2b432f6427a8")?)?;
        let expected = "tz3NdTPb3Ax2rVW2Kq9QEdzfYFkRwhrQRPhX";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_encode_contract_kt1() -> Result<(), anyhow::Error> {
        let decoded = HashType::ContractKt1Hash
            .hash_to_b58check(&hex::decode("42b419240509ddacd12839700b7f720b4aa55e4e")?)?;
        let expected = "KT1EfTusMLoeCAAGd9MZJn5yKzFr6kJU5U91";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_decode_block_header_hash() -> Result<(), anyhow::Error> {
        let decoded = HashType::BlockHash
            .b58check_to_hash("BKyQ9EofHrgaZKENioHyP4FZNsTmiSEcVmcghgzCC9cGhE7oCET")?;
        let decoded = hex::encode(&decoded);
        let expected = "2253698f0c94788689fb95ca35eb1535ec3a8b7c613a97e6683f8007d7959e4b";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_decode_operations_hash() -> Result<(), anyhow::Error> {
        let decoded = HashType::OperationListListHash
            .b58check_to_hash("LLoaGLRPRx3Zf8kB4ACtgku8F4feeBiskeb41J1ciwfcXB3KzHKXc")?;
        let decoded = hex::encode(&decoded);
        let expected = "7c09f7c4d76ace86e1a7e1c7dc0a0c7edcaa8b284949320081131976a87760c3";
        assert_eq!(expected, decoded);

        Ok(())
    }

    #[test]
    fn test_decode_protocol_hash() -> Result<(), anyhow::Error> {
        let decoded =
            ProtocolHash::from_base58_check("PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb")?;
        let decoded = hex::encode(decoded.as_ref());
        let expected = "3e5e3a606afab74a59ca09e333633e2770b6492c5e594455b71e9a2f0ea92afb";
        assert_eq!(expected, decoded);

        assert_eq!(
            "PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb",
            HashType::ProtocolHash.hash_to_b58check(&hex::decode(decoded)?)?
        );
        Ok(())
    }

    #[test]
    fn test_decode_block_metadata_hash() -> Result<(), anyhow::Error> {
        let decoded = HashType::BlockMetadataHash
            .b58check_to_hash("bm2gU1qwmoPNsXzFKydPDHWX37es6C5Z4nHyuesW8YxbkZ1339cN")?;
        let decoded = hex::encode(&decoded);
        let expected = "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8";
        assert_eq!(expected, decoded);

        assert_eq!(
            "bm2gU1qwmoPNsXzFKydPDHWX37es6C5Z4nHyuesW8YxbkZ1339cN",
            HashType::BlockMetadataHash.hash_to_b58check(&hex::decode(decoded)?)?
        );
        Ok(())
    }

    #[test]
    fn test_decode_operation_metadata_list_list_hash() -> Result<(), anyhow::Error> {
        let decoded = HashType::OperationMetadataListListHash
            .b58check_to_hash("LLr283rR7AWhepNeHcP9msa2VeAurWtodBLrnSjwaxpNyiyfhYcKX")?;
        let decoded = hex::encode(&decoded);
        let expected = "761223dee6643bb9f28acf45f2a44ae1a3e2fd68bfc1a00e3f539cc2dc637632";
        assert_eq!(expected, decoded);

        assert_eq!(
            "LLr283rR7AWhepNeHcP9msa2VeAurWtodBLrnSjwaxpNyiyfhYcKX",
            HashType::OperationMetadataListListHash.hash_to_b58check(&hex::decode(decoded)?)?
        );
        Ok(())
    }

    #[test]
    fn test_decode_operation_metadata_hash() -> Result<(), anyhow::Error> {
        let decoded = HashType::OperationMetadataHash
            .b58check_to_hash("r3E9xb2QxUeG56eujC66B56CV8mpwjwfdVmEpYu3FRtuEx9tyfG")?;
        let decoded = hex::encode(&decoded);
        let expected = "2d905a5c4fefad1f1ab8a5436f26b15290f2fe2bea111c85ee4626156dc6b4da";
        assert_eq!(expected, decoded);

        assert_eq!(
            "r3E9xb2QxUeG56eujC66B56CV8mpwjwfdVmEpYu3FRtuEx9tyfG",
            HashType::OperationMetadataHash.hash_to_b58check(&hex::decode(decoded)?)?
        );
        Ok(())
    }

    #[test]
    fn test_b58_to_hash_mismatched_lenght() -> Result<(), anyhow::Error> {
        let b58 = HashType::ChainId.hash_to_b58check(&[0, 0, 0, 0])?;
        let result = HashType::BlockHash.b58check_to_hash(&b58);
        assert!(matches!(
            result,
            Err(FromBase58CheckError::MismatchedLength {
                expected: _,
                actual: _
            })
        ));
        Ok(())
    }
}
