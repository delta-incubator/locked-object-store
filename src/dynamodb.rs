/*
 * This module defines the DynamoDB backed object_store
 */

use dynamodb_lock::DynamoDbLockClient;
pub use dynamodb_lock::DynamoDbOptions;
pub use dynamodb_lock::Region;

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};
use tokio::io::AsyncWrite;

use std::ops::Range;
use std::sync::Arc;

/**
 * DynamoLockBuilder creates a [DynamoLockingObjectStore] with the configured options
 *
 * ```rust
 * # use locking_object_store::dynamodb::*;
 * use object_store::aws::AmazonS3Builder;
 *
 * let s3store = AmazonS3Builder::new()
 *      .with_region("us-east-2")
 *      .with_bucket_name("example")
 *      .build()
 *      .unwrap();
 *
 * let store = DynamoLockBuilder::with_store(s3store).build();
 * ```
 */
#[derive(Clone, Debug)]
pub struct DynamoLockBuilder {
    store: Arc<dyn ObjectStore>,
    options: Option<DynamoDbOptions>,
    region: Option<Region>,
}

impl DynamoLockBuilder {
    pub fn with_store(store: impl ObjectStore) -> Self {
        Self {
            store: Arc::new(store),
            options: None,
            region: None,
        }
    }

    pub fn with_store_ref(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            options: None,
            region: None,
        }
    }

    pub fn with_options(mut self, options: DynamoDbOptions) -> Self {
        self.options = Some(options);
        self
    }

    pub fn with_region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    pub fn build(mut self) -> Result<DynamoLockingObjectStore> {
        let store = DynamoLockingObjectStore::default();
        Ok(store)
    }

    /**
     * Retrieve the inner, unlocked, [ObjectStore].
     *
     * This should be used only very carefully! The `ObjectStore` was wrapped with locking for a
     * reason.
     */
    pub fn get_inner(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }
}

/**
 * The DynamoLockingObjectStore can not be constructed directly, instead use [DynamoLockBuilder]
 */
#[derive(Debug)]
pub struct DynamoLockingObjectStore {
    inner: Arc<dyn ObjectStore>,
    lock_client: DynamoDbLockClient,
}

impl DynamoLockingObjectStore {
    /**
     * Construct a new DynamoLockingObjectStore with another given [ObjectStore]
     *
     * This function expects an [Arc] wrapping, to delegate ownership of the [ObjectStore] to
     * DynamoLockingObjectStore, use the `from` method
     */
    pub fn new(inner: Arc<dyn ObjectStore>, lock_client: DynamoDbLockClient) -> Self {
        Self { inner, lock_client }
    }
}

impl Default for DynamoLockingObjectStore {
    /**
     * The Default trait implementation for DynamoLockingObjectStore will create a stub in-memory
     * ObjectStore for testing purposes only
     *
     * ```rust
     * # use locking_object_store::dynamodb::DynamoLockingObjectStore;
     * let store = DynamoLockingObjectStore::default();
     * ```
     */
    fn default() -> Self {
        let store = object_store::memory::InMemory::new();
        let lock_client = DynamoDbLockClient::default();
        Self {
            inner: Arc::new(store),
            lock_client,
        }
    }
}

/*
 * Simple Display implementation which delegates to the inner ObjectStore
 */
impl std::fmt::Display for DynamoLockingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DynamoLockingObjectStore: {}", self.inner)
    }
}

/// The [ObjectStore] impementation for DynamoLockingObjectStore.
///
/// This implementation passes through most calls to the provided [ObjectStore] except when
/// necessary to add additional locking semantics e.g. rename operations on S3
#[async_trait::async_trait]
impl ObjectStore for DynamoLockingObjectStore {
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
        todo!();
    }

    /// rename if exists
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_dynamodb_store() {
        let _store = DynamoLockingObjectStore::default();
    }

    #[test]
    fn builder_with_store() {
        let inner = object_store::memory::InMemory::new();
        let store = DynamoLockBuilder::with_store(inner);
    }

    #[test]
    fn builder_with_store_ref() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = DynamoLockBuilder::with_store_ref(inner);
    }

    #[test]
    fn builder_with_options() {
        let inner = object_store::memory::InMemory::new();
        let region = Region::UsEast2;
        let store = DynamoLockBuilder::with_store(inner)
            .with_region(region)
            .with_options(DynamoDbOptions::default())
            .build();
        assert!(store.is_ok());
    }
}
