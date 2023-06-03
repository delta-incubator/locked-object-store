//! Simple lock initially backed by PostgreSQL.
mod errors;

#[cfg(feature = "postgres")]
pub mod postgres;

use crate::errors::LockedObjectStoreError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// A lock that has been successfully acquired
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LockItem<T> {
    /// The name of the owner that owns this lock.
    pub owner_name: String,
    /// Current version number of the lock in DynamoDB. This is what tells the lock client
    /// when the lock is stale.
    pub record_version_number: String,
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

/// Abstraction over a distributive lock provider
#[async_trait]
pub trait LockClient: Send + Sync + Debug {
    /// Attempts to acquire lock for data. If successful, returns the lock.
    /// Otherwise returns [`Option::None`] which is retryable action.
    /// Visit implementation docs for more details.
    async fn try_acquire_lock<T: Serialize + Send>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError>;

    /// Returns current lock for data (if any).
    // the original implementation of this was returning the top lock in the system, since our lock
    // is unique based on data, we require that same piece of data in order to return the current lock
    // again though, this is just lock information - it does not mean you're safe to act on the lock
    async fn get_lock<T: Serialize + Send>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError>;

    /// Update data in the upstream lock of the current user still has it.
    /// The returned lock will have a new `rvn` so it'll increase the lease duration
    /// as this method is usually called when the work with a lock is extended.
    async fn update_data<T: Serialize>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<LockItem<T>, LockedObjectStoreError>;

    /// Releases the given lock if the current user still has it, returning true if the lock was
    /// successfully released, and false if someone else already stole the lock
    async fn release_lock<T: Serialize>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<bool, LockedObjectStoreError>;
}

pub const DEFAULT_MAX_RETRY_ACQUIRE_LOCK_ATTEMPTS: u32 = 100;
