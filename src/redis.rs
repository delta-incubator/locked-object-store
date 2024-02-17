//!
//! The redis module uses redis hash functions to implement a locking mechanism

use crate::errors::LockedObjectStoreError;
use crate::{LockClient, LockItem};
use serde::Serialize;

///
/// A Redis-based lock, which relies on the key functionality in redis
///
/// ```rust
/// # use locking_object_store::redis::*;
/// let manager = rslock::LockManager::new(vec!["redis://127.0.0.1"]);
/// let lock = RedisLock::with(manager);
/// ````
#[derive(Clone, Debug)]
pub struct RedisLock {
    manager: rslock::LockManager,
}

impl RedisLock {
    pub fn with(manager: rslock::LockManager) -> Self {
        Self { manager }
    }
}

#[async_trait::async_trait]
impl<T: Send + Serialize> LockClient<T> for RedisLock {
    async fn try_acquire_lock(&self, data: T) -> Result<Option<LockItem<T>>, LockedObjectStoreError>
    where
        T: 'async_trait,
    {
        let item = LockItem::from(data);
        Ok(Some(item))
    }

    /// Returns current lock for data (if any).
    // the original implementation of this was returning the top lock in the system, since our lock
    // is unique based on data, we require that same piece of data in order to return the current lock
    // again though, this is just lock information - it does not mean you're safe to act on the lock
    async fn get_lock(&self, data: T) -> Result<Option<LockItem<T>>, LockedObjectStoreError>
    where
        T: 'async_trait,
    {
        todo!()
    }

    /// Update data in the upstream lock of the current user still has it.
    /// The returned lock will have a new `rvn` so it'll increase the lease duration
    /// as this method is usually called when the work with a lock is extended.
    async fn update_data(&self, lock: &LockItem<T>) -> Result<LockItem<T>, LockedObjectStoreError>
    where
        T: 'async_trait,
    {
        todo!()
    }

    /// Releases the given lock if the current user still has it, returning true if the lock was
    /// successfully released, and false if someone else already stole the lock
    async fn release_lock(&self, lock: &LockItem<T>) -> Result<bool, LockedObjectStoreError>
    where
        T: 'async_trait,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "integration-test")]
    mod integration_tests {
        use super::*;

        #[tokio::test]
        async fn test_construct() -> Result<(), crate::errors::LockedObjectStoreError> {
            let manager = rslock::LockManager::new(vec!["redis://127.0.0.1/"]);
            let lock = RedisLock::with(manager);
            let data = "hello rust";
            let result = lock.try_acquire_lock(data).await?;
            assert!(result.is_some(), "Should receive a valid lock item");

            Ok(())
        }
    }
}
