// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use thiserror::Error;

// TODO: TE-386 - remove not needed
// pub mod collections;

/// Simple condvar synchronized result callback
pub type CondvarResult<T, E> = Arc<(Mutex<Option<Result<T, E>>>, Condvar)>;

#[derive(Error, Debug)]
pub enum WaitCondvarResultError {
    #[error("Timeout exceeded: {duration:?}")]
    TimeoutExceeded { duration: Duration },

    #[error("No result received")]
    NoResultReceived,

    #[error("Mutex/lock poison error, reason: {reason}")]
    PoisonedLock { reason: String },
}

#[derive(Error, Debug)]
pub enum DispatchCondvarResultError {
    #[error("Failed to set result, reason: {reason}")]
    DispatchResultError { reason: String },
}

pub fn dispatch_condvar_result<T, E, RC>(
    result_callback: Option<CondvarResult<T, E>>,
    result: RC,
    notify_condvar_on_lock_error: bool,
) -> Result<(), DispatchCondvarResultError>
where
    RC: FnOnce() -> Result<T, E>,
{
    if let Some(result_callback) = result_callback {
        let &(ref lock, ref cvar) = &*result_callback;
        match lock.lock() {
            Ok(mut result_guard) => {
                *result_guard = Some(result());
                cvar.notify_all();
                Ok(())
            }
            Err(e) => {
                if notify_condvar_on_lock_error {
                    cvar.notify_all();
                }
                Err(DispatchCondvarResultError::DispatchResultError {
                    reason: format!("{}", e),
                })
            }
        }
    } else {
        Ok(())
    }
}

pub fn try_wait_for_condvar_result<T, E>(
    result_callback: CondvarResult<T, E>,
    duration: Duration,
) -> Result<Result<T, E>, WaitCondvarResultError> {
    // get lock
    let &(ref lock, ref cvar) = &*result_callback;
    let lock = lock
        .lock()
        .map_err(|e| WaitCondvarResultError::PoisonedLock {
            reason: format!("{}", e),
        })?;

    // wait for condvar and handle
    match cvar.wait_timeout(lock, duration) {
        Ok((mut result, timeout)) => {
            // process timeout
            if timeout.timed_out() {
                return Err(WaitCondvarResultError::TimeoutExceeded { duration });
            }

            // process result
            match result.take() {
                Some(result) => Ok(result),
                None => Err(WaitCondvarResultError::NoResultReceived),
            }
        }
        Err(e) => Err(WaitCondvarResultError::PoisonedLock {
            reason: format!("{}", e),
        }),
    }
}

/// Simple try_lock without Mutex/RwLock
///
/// Inspired by: https://morestina.net/blog/749/exploring-lock-free-rust-2-atomics
pub struct AtomicTryLock(Arc<AtomicBool>);

impl AtomicTryLock {
    pub fn create() -> AtomicTryLock {
        AtomicTryLock(Arc::new(AtomicBool::new(false)))
    }

    pub fn try_lock(&self) -> Option<AtomicTryLockGuard> {
        let was_locked = self.0.swap(true, Ordering::Acquire);
        if was_locked {
            None
        } else {
            Some(AtomicTryLockGuard {
                lock: self.0.clone(),
            })
        }
    }

    pub fn is_available(&self) -> bool {
        !self.0.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct AtomicTryLockGuard {
    lock: Arc<AtomicBool>,
}

impl Drop for AtomicTryLockGuard {
    fn drop(&mut self) {
        self.lock.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;

    use crate::utils::{
        dispatch_condvar_result, try_wait_for_condvar_result, AtomicTryLock, CondvarResult,
    };

    #[test]
    fn test_wait_and_dispatch() -> Result<(), anyhow::Error> {
        let condvar_result: CondvarResult<(), anyhow::Error> =
            Arc::new((Mutex::new(None), Condvar::new()));

        // run async
        {
            let result = condvar_result.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_secs(2));
                assert!(dispatch_condvar_result(Some(result), || Ok(()), true).is_ok());
            });
        }

        // wait
        assert!(try_wait_for_condvar_result(condvar_result, Duration::from_secs(4))?.is_ok());

        Ok(())
    }

    #[test]
    fn test_atomic_try_lock() {
        let lock = AtomicTryLock::create();
        assert!(lock.is_available());

        // get lock
        let lock_guard = lock.try_lock();
        assert!(lock_guard.is_some());
        assert!(!lock.is_available());

        // get next lock
        assert!(lock.try_lock().is_none());
        assert!(lock.try_lock().is_none());
        assert!(lock.try_lock().is_none());
        assert!(!lock.is_available());

        // release lock
        drop(lock_guard);
        assert!(lock.is_available());

        // try next lock
        assert!(lock.try_lock().is_some());
        assert!(lock.is_available());
        assert!(lock.try_lock().is_some());
        assert!(lock.is_available());
    }
}
