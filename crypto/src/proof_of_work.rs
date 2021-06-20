use std::convert::TryFrom;

use hex::FromHex;
use num_bigint::BigUint;
use sodiumoxide::randombytes::randombytes;
use thiserror::Error;

use crate::{blake2b::Blake2bError, CryptoError};

use super::{
    blake2b,
    crypto_box::{PublicKey, CRYPTO_KEY_SIZE},
    nonce::NONCE_SIZE,
};

pub const POW_SIZE: usize = NONCE_SIZE;

#[derive(Debug, Error)]
pub enum PowError {
    #[error("Proof-of-work check failed")]
    CheckFailed,
    #[error("Proof-of-work blake2b error: {0}")]
    Blake2b(Blake2bError),
}

impl From<Blake2bError> for PowError {
    fn from(error: Blake2bError) -> Self {
        PowError::Blake2b(error)
    }
}

pub type PowResult = Result<(), PowError>;

#[derive(Debug, Clone, PartialEq)]
pub struct ProofOfWork([u8; POW_SIZE]);

impl AsRef<[u8]> for ProofOfWork {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl FromHex for ProofOfWork {
    type Error = CryptoError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let bytes = hex::decode(hex)?;

        if bytes.len() != POW_SIZE {
            return Err(CryptoError::InvalidKeySize {
                expected: POW_SIZE,
                actual: bytes.len(),
            });
        }

        let mut arr = [0u8; POW_SIZE];
        arr.copy_from_slice(&bytes);
        Ok(ProofOfWork(arr))
    }
}

impl ProofOfWork {
    pub const DEFAULT_TARGET: f64 = 24.0;

    pub fn generate(public_key: &PublicKey, target: f64) -> Self {
        let mut data = [0; CRYPTO_KEY_SIZE + POW_SIZE];
        data[..CRYPTO_KEY_SIZE].clone_from_slice(public_key.as_ref().as_ref());
        data[CRYPTO_KEY_SIZE..].clone_from_slice(randombytes(POW_SIZE).as_ref());

        let target_number = make_target(target);
        loop {
            if let Ok(()) = check_proof_of_work_inner(data.as_ref(), &target_number) {
                let mut nonce = [0; POW_SIZE];
                nonce.clone_from_slice(&data[CRYPTO_KEY_SIZE..]);
                return ProofOfWork(nonce);
            } else {
                // the code might look obscure,
                // but it just treat `data[CRYPTO_KEY_SIZE..]` as an 192-bit integer and increment it

                let mut c = u64::from_be_bytes(<[u8; 8]>::try_from(&data[0x30..0x38]).unwrap());
                if c == u64::MAX {
                    let mut b = u64::from_be_bytes(<[u8; 8]>::try_from(&data[0x28..0x30]).unwrap());
                    if b == u64::MAX {
                        let mut a =
                            u64::from_be_bytes(<[u8; 8]>::try_from(&data[0x20..0x28]).unwrap());
                        if a == u64::MAX {
                            a = 0;
                            b = 0;
                            c = 0;
                        } else {
                            a += 1;
                            b = 0;
                            c = 0;
                        }
                        data[0x20..0x28].clone_from_slice(a.to_be_bytes().as_ref());
                    } else {
                        b += 1;
                        c = 0;
                    }
                    data[0x28..0x30].clone_from_slice(b.to_be_bytes().as_ref());
                } else {
                    c += 1;
                }
                data[0x30..0x38].clone_from_slice(c.to_be_bytes().as_ref());
            }
        }
    }

    pub fn check(&self, pk: &PublicKey, target: f64) -> PowResult {
        let mut data = [0; CRYPTO_KEY_SIZE + POW_SIZE];
        data[..CRYPTO_KEY_SIZE].clone_from_slice(pk.as_ref().as_ref());
        data[CRYPTO_KEY_SIZE..].clone_from_slice(self.as_ref());
        check_proof_of_work(data.as_ref(), target)
    }
}

// Check without deserializing connection message.
// Will know proof is valid once receive first 60 bytes.
// 2 chunk length + 2 port + 32 public key + 24 nonce = 60,
// `check_proof_of_work(&received_raw_data[4..60], target)`
pub fn check_proof_of_work(data: &[u8], target: f64) -> PowResult {
    let target_number = make_target(target);
    check_proof_of_work_inner(data, &target_number)
}

fn check_proof_of_work_inner(data: &[u8], target_number: &BigUint) -> PowResult {
    let hash = blake2b::digest_256(data)?;
    let hash_number = BigUint::from_bytes_le(hash.as_ref());
    if hash_number.le(target_number) {
        Ok(())
    } else {
        Err(PowError::CheckFailed)
    }
}

fn make_target(target: f64) -> BigUint {
    assert!((0.0..256.0).contains(&target));
    let (frac, shift) = (target.fract(), target.floor() as u64);
    let m = if frac.abs() < std::f64::EPSILON {
        (1 << 54) - 1
    } else {
        2.0f64.powf(54.0 - frac) as u64
    };
    let m = BigUint::from(m);
    if shift < 202 {
        (m << (202 - shift)) | ((BigUint::from(1u64) << (202 - shift)) - BigUint::from(1u64))
    } else {
        m >> (shift - 202)
    }
}

#[cfg(test)]
mod tests {
    use hex::FromHex;
    use num_bigint::BigUint;

    use crate::crypto_box::PublicKey;

    use super::{check_proof_of_work, ProofOfWork};

    // `BigUint::from_bytes_le` is the same as `Z.of_bits`
    #[test]
    fn check_binary_format() {
        let hex_string = "65813cba342745fb8870cf192efd7abf5a7f7c0bb4852d33bcb8e8a521c88561";
        let dec_string = "\
            44110718228612227164362334473137928594922343768065507925100594771156402995557\
        ";
        let x = BigUint::from_bytes_le(hex::decode(hex_string).unwrap().as_ref());
        assert_eq!(x.to_string(), dec_string);

        let hex_string = "6a9b7e0243f052c67124d54abd23991734e7dad8a53ab7d82fd96b4e0b000000";
        let dec_string = "\
            304818138341606080779209476504996542811599673553028925663939963820906\
        ";
        let x = BigUint::from_bytes_le(hex::decode(hex_string).unwrap().as_ref());
        assert_eq!(x.to_string(), dec_string);
    }

    #[test]
    fn simple_check() {
        let data = hex::decode(
            "\
            d8246d13d0270cbfff4046b6d94b05ab19920bc5ad9fb77f3e945c40b340e874\
            d1d0ebd55784bc92852d913dbf0fb5152d505b567d930fb2\
        ",
        )
        .unwrap();
        check_proof_of_work(data.as_ref(), ProofOfWork::DEFAULT_TARGET).unwrap();
    }

    #[test]
    fn simple_generate() {
        let pk =
            PublicKey::from_hex("d8246d13d0270cbfff4046b6d94b05ab19920bc5ad9fb77f3e945c40b340e874")
                .expect("Failed to generate public key");
        let pow = ProofOfWork::generate(&pk, 3.5);
        assert!(pow.check(&pk, 3.5).is_ok());
    }
}
