//! Simple lock initially backed by PostgreSQL.
mod errors;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;
#[cfg(feature = "postgres")]
pub mod postgres;

use crate::errors::LockedObjectStoreError;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A lock that has been successfully acquired
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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

impl<T> LockItem<T> {
    /// REturn true of this [LockItem] has expired
    pub fn is_expired(&self) -> bool {
        if self.is_released {
            return true;
        }
        match self.lease_duration {
            None => false,
            Some(lease_duration) => {
                now_millis() - self.lookup_time > (lease_duration as u128) * 1000
            }
        }
    }
}

/// Abstraction over a distributive lock provider
#[async_trait]
pub trait LockClient: Send + Sync + Debug + 'static {
    /// Attempts to acquire lock for data. If successful, returns the lock.
    /// Otherwise returns [`Option::None`] which is retryable action.
    /// Visit implementation docs for more details.
    async fn try_acquire_lock<T: Serialize + Send + Sync>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError>
    where
        Self: Sized;

    /// Returns current lock for data (if any).
    // the original implementation of this was returning the top lock in the system, since our lock
    // is unique based on data, we require that same piece of data in order to return the current lock
    // again though, this is just lock information - it does not mean you're safe to act on the lock
    async fn get_lock<T: Serialize + Send + Sync>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError>
    where
        Self: Sized;

    /// Update data in the upstream lock of the current user still has it.
    /// The returned lock will have a new `rvn` so it'll increase the lease duration
    /// as this method is usually called when the work with a lock is extended.
    async fn update_data<T: Serialize + Send + Sync>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<LockItem<T>, LockedObjectStoreError>
    where
        Self: Sized;

    /// Releases the given lock if the current user still has it, returning true if the lock was
    /// successfully released, and false if someone else already stole the lock
    async fn release_lock<T: Serialize + Send + Sync>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<bool, LockedObjectStoreError>
    where
        Self: Sized;
}

pub const DEFAULT_MAX_RETRY_ACQUIRE_LOCK_ATTEMPTS: u32 = 100;

/// LockingModes dictate which methods the [LockingObjectStore] should provide a lock around
pub enum LockingModes {
    Delete,
    Put,
    Rename,
}

/// LockingObjectStore is a generic object store implementation which can take any [LockClient] and
/// provide locking behavior around the [ObjectStore] behavior.
#[derive(Clone, Debug)]
pub struct LockingObjectStore {
    lock_client: Arc<dyn LockClient>,
    inner: Arc<dyn ObjectStore>,
}

/// The [ObjectStore] impementation for DynamoLockingObjectStore.
///
/// This implementation passes through most calls to the provided [ObjectStore] except when
/// necessary to add additional locking semantics e.g. rename operations on S3
#[async_trait::async_trait]
impl ObjectStore for LockingObjectStore {
    /// Pass through to the provided [ObjectStore]'s implementation
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.inner.put(location, bytes).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner.get(location).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    /// Pass through to the provided [ObjectStore]'s implementation
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    /// rename
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    /// rename if exists
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

impl std::fmt::Display for LockingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DynamoLockingObjectStore: {}", self.inner)
    }
}

/// Simple function to return the current time in milliseconds
///
/// This is implemented to avoid needing to pull a dependency on chrono for just the one function
pub(crate) fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lockitem_is_expired_when_released() {
        let mut item: LockItem<bool> = LockItem::default();
        assert_eq!(
            false,
            item.is_expired(),
            "Default state should be not expired"
        );
        item.is_released = true;
        assert_eq!(true, item.is_expired());
    }

    #[test]
    fn lockitem_is_expired_when_expired() {
        let mut item: LockItem<bool> = LockItem::default();
        assert_eq!(
            false,
            item.is_expired(),
            "Default state should be not expired"
        );
        let now = now_millis();
        item.lookup_time = now - 2000;
        item.lease_duration = Some(1);
        assert_eq!(true, item.is_expired());
    }
}
