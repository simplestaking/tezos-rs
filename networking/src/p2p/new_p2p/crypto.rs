use crypto::CryptoError;
use crypto::crypto_box::PrecomputedKey;
use crypto::nonce::{Nonce, NoncePair};

#[derive(Debug, Clone)]
pub struct Crypto {
    /// Precomputed key is created from merge of peer public key and our secret key.
    /// It's used to speedup of crypto operations.
    precomputed_key: PrecomputedKey,
    /// Nonce used to encrypt outgoing messages
    nonce_pair: NoncePair,
}

impl Crypto {
    #[inline]
    pub fn new(precomputed_key: PrecomputedKey, nonce_pair: NoncePair) -> Self {
        Self {
            precomputed_key,
            nonce_pair,
        }
    }

    #[inline]
    fn local_nonce_fetch_increment(&mut self) -> Nonce {
        let nonce = self.nonce_pair.local.increment();
        std::mem::replace(&mut self.nonce_pair.local, nonce)
    }

    #[inline]
    fn remote_nonce_fetch_increment(&mut self) -> Nonce {
        let nonce = self.nonce_pair.remote.increment();
        std::mem::replace(&mut self.nonce_pair.remote, nonce)
    }

    #[inline]
    pub fn encrypt<T: AsRef<[u8]>>(&mut self, data: &T) -> Result<Vec<u8>, CryptoError> {
        let nonce = self.local_nonce_fetch_increment();
        self.precomputed_key.encrypt(data.as_ref(), &nonce)
    }

    #[inline]
    pub fn decrypt<T: AsRef<[u8]>>(&mut self, data: &T) -> Result<Vec<u8>, CryptoError> {
        let nonce = self.remote_nonce_fetch_increment();
        self.precomputed_key.decrypt(data.as_ref(), &nonce)
    }
}
