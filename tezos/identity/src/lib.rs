// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::{collections::HashMap, convert::TryFrom};
use std::{fs, io};

use hex::{FromHex, FromHexError};
use serde_json::Value;
use thiserror::Error;

use crypto::{crypto_box::PublicKeyError, hash::CryptoboxPublicKeyHash};
use crypto::{
    crypto_box::{random_keypair, PublicKey, SecretKey},
    proof_of_work::ProofOfWork,
};

#[derive(Error, Debug)]
pub enum IdentityError {
    #[error("I/O error: {reason}")]
    IoError { reason: io::Error },

    #[error("Serde error, reason: {reason}")]
    IdentitySerdeError { reason: serde_json::Error },

    #[error("Invalid field error, reason: {reason}")]
    IdentityFieldError { reason: String },

    #[error("Invalid public key, reason: {reason}")]
    InvalidPublicKeyError { reason: FromHexError },

    #[error("Identity invalid peer_id check")]
    InvalidPeerIdError,

    #[error("Public key error: {0}")]
    PublicKeyError(PublicKeyError),
}

impl From<PublicKeyError> for IdentityError {
    fn from(source: PublicKeyError) -> Self {
        Self::PublicKeyError(source)
    }
}

/// This node identity information compatible with Tezos
#[derive(Clone, Debug)]
pub struct Identity {
    /// Peer_id is calculated hash of public_key [`crypto_box::PublicKey`]
    pub peer_id: CryptoboxPublicKeyHash,

    /// Hex encoded public key: [`crypto_box::PublicKey`]
    pub public_key: PublicKey,
    /// Hex encoded secret key: [`crypto_box::SecretKey`]
    pub secret_key: SecretKey,
    /// Hex encoded pow: [`crypto::ProofOfWork`]
    pub proof_of_work_stamp: ProofOfWork,
}

impl Identity {
    pub fn generate(expected_pow: f64) -> Result<Self, PublicKeyError> {
        let (sk, pk, peer_id) = random_keypair()?;
        let pow = ProofOfWork::generate(&pk, expected_pow);
        Ok(Identity {
            peer_id,
            public_key: pk,
            secret_key: sk,
            proof_of_work_stamp: pow,
        })
    }

    pub fn check_peer_id(&self) -> Result<(), IdentityError> {
        if self.peer_id == self.public_key.public_key_hash()? {
            Ok(())
        } else {
            Err(IdentityError::InvalidPeerIdError)
        }
    }

    pub fn from_json(json: &str) -> Result<Identity, IdentityError> {
        let identity: HashMap<String, Value> = serde_json::from_str(json)
            .map_err(|e| IdentityError::IdentitySerdeError { reason: e })?;

        let peer_id_str = identity
            .get("peer_id")
            .ok_or(IdentityError::IdentityFieldError {
                reason: "Missing 'peer_id'".to_string(),
            })?
            .as_str()
            .ok_or(IdentityError::IdentityFieldError {
                reason: "Missing valid 'peer_id'".to_string(),
            })?;
        let peer_id = CryptoboxPublicKeyHash::try_from(peer_id_str).map_err(|e| {
            IdentityError::IdentityFieldError {
                reason: format!("Missing valid 'peer_id': {}", e),
            }
        })?;

        let public_key_str = identity
            .get("public_key")
            .ok_or(IdentityError::IdentityFieldError {
                reason: "Missing 'public_key'".to_string(),
            })?
            .as_str()
            .ok_or(IdentityError::IdentityFieldError {
                reason: "Missing valid 'public_key'".to_string(),
            })?;
        let public_key =
            PublicKey::from_hex(public_key_str).map_err(|e| IdentityError::IdentityFieldError {
                reason: format!("Missing valid 'public_key': {}", e),
            })?;

        let secret_key_str = identity
            .get("secret_key")
            .ok_or(IdentityError::IdentityFieldError {
                reason: "Missing 'secret_key'".to_string(),
            })?
            .as_str()
            .ok_or(IdentityError::IdentityFieldError {
                reason: "Missing valid 'secret_key'".to_string(),
            })?;
        let secret_key =
            SecretKey::from_hex(secret_key_str).map_err(|e| IdentityError::IdentityFieldError {
                reason: format!("Missing valid 'secret_key': {}", e),
            })?;

        let proof_of_work_stamp_str = identity
            .get("proof_of_work_stamp")
            .ok_or(IdentityError::IdentityFieldError {
                reason: "Missing 'proof_of_work_stamp'".to_string(),
            })?
            .as_str()
            .ok_or(IdentityError::IdentityFieldError {
                reason: "Missing valid 'proof_of_work_stamp'".to_string(),
            })?;
        let proof_of_work_stamp = ProofOfWork::from_hex(proof_of_work_stamp_str).map_err(|e| {
            IdentityError::IdentityFieldError {
                reason: format!("Missing valid 'proof_of_work_stamp': {}", e),
            }
        })?;

        Ok(Identity {
            peer_id,
            public_key,
            secret_key,
            proof_of_work_stamp,
        })
    }

    pub fn as_json(&self) -> Result<String, IdentityError> {
        let mut identity: HashMap<&'static str, String> = Default::default();
        identity.insert("peer_id", self.peer_id.to_base58_check());
        identity.insert("public_key", hex::encode(self.public_key.as_ref()));
        identity.insert("secret_key", hex::encode(self.secret_key.as_ref()));
        identity.insert(
            "proof_of_work_stamp",
            hex::encode(self.proof_of_work_stamp.as_ref()),
        );
        serde_json::to_string(&identity)
            .map_err(|e| IdentityError::IdentitySerdeError { reason: e })
    }

    pub fn peer_id(&self) -> CryptoboxPublicKeyHash {
        self.peer_id.clone()
    }
}

/// Load identity from tezos configuration file.
pub fn load_identity<P: AsRef<Path>>(
    identity_json_file_path: P,
) -> Result<Identity, IdentityError> {
    fs::read_to_string(identity_json_file_path)
        .map(|contents| Identity::from_json(&contents))
        .map_err(|e| IdentityError::IoError { reason: e })?
}

/// Stores provided identity into the file specified by path
pub fn store_identity(path: &PathBuf, identity: &Identity) -> Result<(), IdentityError> {
    let identity_json = identity.as_json()?;
    fs::write(&path, &identity_json).map_err(|e| IdentityError::IoError { reason: e })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_generate() -> Result<(), anyhow::Error> {
        // generate
        let identity = Identity::generate(16f64)?;

        // check
        assert!(identity.check_peer_id().is_ok());

        // convert json and back
        let converted = identity.as_json()?;
        let converted = Identity::from_json(&converted)?;

        assert!(identity.check_peer_id().is_ok());
        assert_eq!(identity.peer_id, converted.peer_id);
        assert_eq!(identity.public_key, converted.public_key);
        assert_eq!(identity.secret_key, converted.secret_key);
        assert_eq!(identity.proof_of_work_stamp, converted.proof_of_work_stamp);

        Ok(())
    }

    #[test]
    fn test_identity_json_serde_generated_by_tezos() -> Result<(), anyhow::Error> {
        let expected_json = serde_json::json!(
            {
              "peer_id": "idtqxHUjbjbCfaDn4jczoPGsnhacKX",
              "public_key": "a072c7b3e477142689cadee638078b377df5e5793e3cea529d0b718cde59f212",
              "secret_key": "d37c77a8643c7f7fce9219e9769ed4dd23bc542265da47a64a2613bd199ad74e",
              "proof_of_work_stamp": "0cfe810d9b4591f0f50721b6811f2981a4274e9d0593bbd0"
            }
        );

        let converted = Identity::from_json(serde_json::to_string(&expected_json)?.as_str())?;
        assert!(converted.check_peer_id().is_ok());

        let converted = converted.as_json()?;
        let converted = Identity::from_json(&converted)?;
        assert!(converted.check_peer_id().is_ok());
        let converted = converted.as_json()?;

        // check
        assert_json_diff::assert_json_eq!(
            serde_json::from_str::<serde_json::Value>(&converted)?,
            expected_json
        );
        Ok(())
    }
}
