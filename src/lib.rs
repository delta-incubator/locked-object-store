//! Simple lock initially backed by PostgreSQL.
mod errors;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;

use crate::errors::LockedObjectStoreError;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Debug;
use std::sync::Arc;

///
/// The `LockedObjectStore` provides the wrapper needed to lock on specific [ObjectStore]
/// operations
///
#[derive(Clone, Debug)]
pub struct LockedObjectStore<T> {
    inner: Arc<dyn ObjectStore>,
    lock: Arc<dyn LockClient<T>>,
}

///
/// Definitions of operations on [LockedObjectStore] Which can be locked
///
#[derive(Debug)]
pub enum LockOperations {
    Copy,
    CopyIfNotExists,
    Delete,
    Put,
    Rename,
    RenameIfNotExists,
}

/// A lock that has been successfully acquired
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LockItem<T> {
    /// The name of the owner that owns this lock.
    pub owner_name: String,
    /// The amount of time (in seconds) that the owner has this lock for.
    /// If lease_duration is None then the lock is non-expirable.
    pub lease_duration: Option<u64>,
    /// Tells whether or not the lock was marked as released when loaded from DynamoDB.
    pub is_released: bool,
    /// Optional data associated with this lock, must be able to be serialized/deserialized and send safe
    pub data: Option<T>,
    /// The last time this lock was updated or retrieved.
    pub lookup_time: u128,
    /// Tells whether this lock was acquired by expiring existing one.
    pub acquired_expired_lock: bool,
    /// If true then this lock could not be acquired.
    pub is_non_acquirable: bool,
}

impl<T> Default for LockItem<T> {
    fn default() -> Self {
        let owner_name = env::var("LOCK_OWNER_NAME").unwrap_or("locking-object-store".into());
        let lease_duration = env::var("LOCK_LEASE_SECONDS").unwrap_or("20".into());

        Self {
            owner_name,
            acquired_expired_lock: false,
            is_non_acquirable: false,
            is_released: false,
            // TODO bring lease_duration through
            lease_duration: None,
            data: None,
            lookup_time: 0,
        }
    }
}

impl<T> LockItem<T> {
    ///
    /// Create a standard [LockItem] with default parameters set by environment variables
    fn from(data: T) -> Self {
        let mut item = LockItem::default();
        item.data = Some(data);
        item
    }
}

/// Abstraction over a distributive lock provider
#[async_trait::async_trait]
pub trait LockClient<T: Serialize + Send>: Send + Sync + Debug {
    /// Attempts to acquire lock for data. If successful, returns the lock.
    /// Otherwise returns [`Option::None`] which is retryable action.
    /// Visit implementation docs for more details.
    async fn try_acquire_lock(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError>
    where
        T: 'async_trait;

    /// Returns current lock for data (if any).
    // the original implementation of this was returning the top lock in the system, since our lock
    // is unique based on data, we require that same piece of data in order to return the current lock
    // again though, this is just lock information - it does not mean you're safe to act on the lock
    async fn get_lock(&self, data: T) -> Result<Option<LockItem<T>>, LockedObjectStoreError>
    where
        T: 'async_trait;

    /// Update data in the upstream lock of the current user still has it.
    /// The returned lock will have a new `rvn` so it'll increase the lease duration
    /// as this method is usually called when the work with a lock is extended.
    async fn update_data(&self, lock: &LockItem<T>) -> Result<LockItem<T>, LockedObjectStoreError>
    where
        T: 'async_trait;

    /// Releases the given lock if the current user still has it, returning true if the lock was
    /// successfully released, and false if someone else already stole the lock
    async fn release_lock(&self, lock: &LockItem<T>) -> Result<bool, LockedObjectStoreError>
    where
        T: 'async_trait;
}

pub const DEFAULT_MAX_RETRY_ACQUIRE_LOCK_ATTEMPTS: u32 = 100;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_item_detault() {
        let _item = LockItem::<&str>::default();
    }
}
