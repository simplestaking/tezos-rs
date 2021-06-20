// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::cmp::Ordering as Ord;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use thiserror::Error;

use crate::persistent::database::RocksDbKeyValueSchema;
use crate::persistent::{DBError, KeyValueSchema, KeyValueStoreWithSchema};

/// Provider a system wide unique sequence generators backed by a permanent RocksDB storage.
/// This struct can be safely shared by a multiple threads.
/// Because sequence number is stored into eventually consistent key-value store it is not
/// safe to create multiple instances of this struct.
/// One exception is when only a single and unique generator is created in call to `Sequences::generator()` function.
#[derive(Clone)]
pub struct Sequences {
    /// Persistent storage
    db: Arc<SequencerDatabase>,
    /// Represents how many sequence numbers will be pre-allocated in a single batch.
    seq_batch_size: u16,
    /// Map of all loaded generators
    generators: Arc<Mutex<HashMap<String, Arc<SequenceGenerator>>>>,
}

pub type SequenceNumber = u64;
pub type SequencerDatabase = dyn KeyValueStoreWithSchema<Sequences> + Sync + Send;

impl Sequences {
    pub fn new(db: Arc<SequencerDatabase>, seq_batch_size: u16) -> Self {
        assert_ne!(seq_batch_size, 0, "Batch size must be a positive number");

        Self {
            db,
            seq_batch_size,
            generators: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Retrieve a sequence generator by it's unique name. If generator does not exist it is created.
    pub fn generator(&self, name: &str) -> Arc<SequenceGenerator> {
        let mut generators = self.generators.lock().unwrap();
        match generators.get(name) {
            Some(generator) => generator.clone(),
            None => {
                let generator = Arc::new(SequenceGenerator::new(
                    name.to_owned(),
                    self.seq_batch_size,
                    self.db.clone(),
                ));
                generators.insert(name.into(), generator.clone());
                generator
            }
        }
    }
}

impl KeyValueSchema for Sequences {
    type Key = String;
    type Value = SequenceNumber;
}

impl RocksDbKeyValueSchema for Sequences {
    fn name() -> &'static str {
        "sequence"
    }
}

pub struct SequenceGenerator {
    /// Database
    db: Arc<SequencerDatabase>,
    /// Current value of the sequence
    seq_cur: AtomicU64,
    /// This value represents an offset from the base
    seq_available: AtomicI32,
    /// Represents how many sequence numbers will be pre-allocated in a single batch.
    seq_batch_size: u16,
    /// unique identifier of the sequence
    seq_name: String,
    /// Guarding write access to a database
    guard: (Mutex<()>, Condvar),
}

impl SequenceGenerator {
    fn new(seq_name: String, seq_batch_size: u16, db: Arc<SequencerDatabase>) -> Self {
        Self {
            seq_cur: AtomicU64::new(db.get(&seq_name).unwrap_or_default().unwrap_or(0)),
            seq_available: AtomicI32::new(0),
            guard: (Mutex::new(()), Condvar::new()),
            db,
            seq_name,
            seq_batch_size,
        }
    }

    /// Get next unique sequence number. Value by this function is positive and always increasing.
    pub fn next(&self) -> Result<SequenceNumber, SequenceError> {
        let seq = loop {
            let available = self.seq_available.fetch_add(-1, Ordering::SeqCst);

            match available.cmp(&0) {
                Ord::Greater => {
                    // no need to allocate new sequence numbers yet
                    let seq = self.seq_cur.fetch_add(1, Ordering::SeqCst);
                    break seq;
                }
                Ord::Equal => {
                    // last pre-allocated sequence numbers was allocated now, we have to perform a new allocation
                    let seq = self.seq_cur.fetch_add(1, Ordering::SeqCst);

                    // obtain mutex lock to ensure exclusive access to the database
                    let _allocated = self.guard.0.lock()?;

                    // pre-allocate sequence numbers
                    let seq_prev = self.db.get(&self.seq_name)?.unwrap_or(0);
                    let seq_new = seq_prev + u64::from(self.seq_batch_size);
                    self.db.put(&self.seq_name, &seq_new)?;

                    // reset available counter
                    self.seq_available
                        .store(i32::from(self.seq_batch_size) - 1, Ordering::SeqCst);

                    // notify waiting threads
                    self.guard.1.notify_all();

                    break seq;
                }
                Ord::Less => {
                    // wait until seq_available is positive number again
                    let _lock = self.guard.1.wait_while(self.guard.0.lock()?, |_| {
                        self.seq_available.load(Ordering::SeqCst) <= 0
                    })?;
                }
            }
        };

        Ok(seq)
    }
}

#[derive(Debug, Error)]
pub enum SequenceError {
    #[error("Persistent storage error: {error}")]
    PersistentStorageError { error: DBError },
    #[error("Thread synchronization error")]
    SynchronizationError,
}

impl From<DBError> for SequenceError {
    fn from(error: DBError) -> Self {
        SequenceError::PersistentStorageError { error }
    }
}

impl<T> From<PoisonError<T>> for SequenceError {
    fn from(_: PoisonError<T>) -> Self {
        SequenceError::SynchronizationError
    }
}
