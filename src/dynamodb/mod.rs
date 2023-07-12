/*
 * This module defines the DynamoDB backed object_store
 */
use crate::errors::*;
use crate::{now_millis, LockClient, LockItem};

use bytes::Bytes;
use futures::stream::BoxStream;
use maplit::hashmap;
use object_store::path::Path;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};
/// Re-export of [rusuto_core::Region] for convenience
pub use rusoto_core::Region;
use rusoto_core::RusotoError;

#[cfg(feature = "sts")]
use rusoto_credential::{AutoRefreshingProvider, CredentialsError};
use rusoto_dynamodb::*;
#[cfg(feature = "sts")]
use rusoto_sts::WebIdentityProvider;
use tokio::io::AsyncWrite;
use uuid::Uuid;

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
 * let store = DynamoLockBuilder::with_store(s3store)
 *              .with_region(Region::UsEast2)
 *              .build();
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

    /// Create a [DynamoLockingObjectStore] with the configured options, if there are no options
    /// set on the builder then the default region and DynamoDb options will be used
    pub fn build(mut self) -> Result<DynamoLockingObjectStore> {
        let region = self.region.unwrap_or(Region::default());
        let options = self.options.unwrap_or(DynamoDbOptions::default());

        let lock_client = DynamoDbLockClient::for_region(region).with_options(options);

        Ok(DynamoLockingObjectStore {
            inner: self.store.clone(),
            lock_client,
        })
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

/**
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
        self.inner.rename(from, to).await
    }

    /// rename if exists
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

/// DynamoDb option keys to use when creating DynamoDbOptions.
/// The same key should be used whether passing a key in the hashmap or setting it as an environment variable.
pub mod dynamo_lock_options {
    /// Used as the partition key for DynamoDb writes.
    /// This should be the same for all writers writing against the same S3 table.
    pub const DYNAMO_LOCK_PARTITION_KEY_VALUE: &str = "DYNAMO_LOCK_PARTITION_KEY_VALUE";
    /// The DynamoDb table where locks are stored. Must be the same between clients that require the same lock.
    pub const DYNAMO_LOCK_TABLE_NAME: &str = "DYNAMO_LOCK_TABLE_NAME";
    /// Name of the task that owns the DynamoDb lock. If not provided, defaults to a UUID that represents the process performing the write.
    pub const DYNAMO_LOCK_OWNER_NAME: &str = "DYNAMO_LOCK_OWNER_NAME";
    /// Amount of time to lease a lock. If not provided, defaults to 20 seconds.
    pub const DYNAMO_LOCK_LEASE_DURATION: &str = "DYNAMO_LOCK_LEASE_DURATION";
    /// Amount of time to wait before trying to acquire a lock.
    /// If not provided, defaults to 1000 millis.
    pub const DYNAMO_LOCK_REFRESH_PERIOD_MILLIS: &str = "DYNAMO_LOCK_REFRESH_PERIOD_MILLIS";
    /// Timeout for lock acquisition.
    /// In practice, this is used to allow acquiring the lock in case it cannot be acquired immediately when a check after `LEASE_DURATION` is performed.
    /// If not provided, defaults to 1000 millis.
    pub const DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS: &str =
        "DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS";
}

/// Configuration options for [`DynamoDbLockClient`].
///
/// Available options are described in [dynamo_lock_options].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DynamoDbOptions {
    /// Partition key value of DynamoDB table,
    /// Should be the same among the clients which work with the lock.
    pub partition_key_value: String,
    /// The DynamoDB table name, should be the same among the clients which work with the lock.
    /// The table has to be created if it not exists before using it with DynamoDB locking API.
    pub table_name: String,
    /// Owner name, should be unique among the clients which work with the lock.
    pub owner_name: String,
    /// The amount of time (in seconds) that the owner has for the acquired lock.
    pub lease_duration: u64,
    /// The amount of time to wait before trying to get the lock again in milliseconds. Defaults to 1000ms (1 second).
    pub refresh_period: Duration,
    /// The amount of time to wait in addition to `lease_duration`.
    pub additional_time_to_wait_for_lock: Duration,
}

impl Default for DynamoDbOptions {
    fn default() -> Self {
        Self::from_map(HashMap::new())
    }
}

impl DynamoDbOptions {
    /// Creates a new DynamoDb options from the given map.
    /// Keys not present in the map are taken from the environment variable.
    pub fn from_map(options: HashMap<String, String>) -> Self {
        let refresh_period = Duration::from_millis(Self::u64_opt(
            &options,
            dynamo_lock_options::DYNAMO_LOCK_REFRESH_PERIOD_MILLIS,
            1000,
        ));
        let additional_time_to_wait_for_lock = Duration::from_millis(Self::u64_opt(
            &options,
            dynamo_lock_options::DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS,
            1000,
        ));

        Self {
            partition_key_value: Self::str_opt(
                &options,
                dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE,
                "delta-rs".to_string(),
            ),
            table_name: Self::str_opt(
                &options,
                dynamo_lock_options::DYNAMO_LOCK_TABLE_NAME,
                "delta_rs_lock_table".to_string(),
            ),
            owner_name: Self::str_opt(
                &options,
                dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME,
                Uuid::new_v4().to_string(),
            ),
            lease_duration: Self::u64_opt(
                &options,
                dynamo_lock_options::DYNAMO_LOCK_LEASE_DURATION,
                20,
            ),
            refresh_period,
            additional_time_to_wait_for_lock,
        }
    }

    fn str_opt(map: &HashMap<String, String>, key: &str, default: String) -> String {
        map.get(key)
            .map(|v| v.to_owned())
            .unwrap_or_else(|| std::env::var(key).unwrap_or(default))
    }

    fn u64_opt(map: &HashMap<String, String>, key: &str, default: u64) -> u64 {
        map.get(key)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or_else(|| {
                std::env::var(key)
                    .ok()
                    .and_then(|e| e.parse::<u64>().ok())
                    .unwrap_or(default)
            })
    }
}

/// Error returned by the [`DynamoDbLockClient`] API.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum DynamoError {
    #[cfg(feature = "sts")]
    /// Error of the underlying authentication mechanism
    #[error("Failed to authenticate: {0}")]
    AuthenticationError(CredentialsError),

    /// Error caused by the DynamoDB table not being created.
    #[error("Dynamo table not found")]
    TableNotFound,

    /// Error that indicates the condition in the DynamoDB operation could not be evaluated.
    /// Mostly used by [`DynamoDbLockClient::acquire_lock`] to handle unsuccessful retries
    /// of acquiring the lock.
    #[error("Conditional check failed")]
    ConditionalCheckFailed,

    /// The required field of [`LockItem`] is missing in DynamoDB record or has incompatible type.
    #[error("DynamoDB item has invalid schema")]
    InvalidItemSchema,

    /// Error returned by [`DynamoDbLockClient::acquire_lock`] which indicates that the lock could
    /// not be acquired for more that returned number of seconds.
    #[error("Could not acquire lock for {0} sec")]
    TimedOut(u64),

    /// Error returned by [`DynamoDbLockClient::acquire_lock`] which indicates that the lock could
    /// not be acquired because the `is_non_acquirable` is set to `true`.
    /// Usually this is done intentionally outside of [`DynamoDbLockClient`].
    ///
    /// The example could be the dropping of a table. For example external service acquires the lock
    /// to drop (or drop/create etc., something that modifies the delta log completely) a table.
    /// The dangerous part here is that the concurrent delta workers will still perform the write
    /// whenever the lock is available, because it effectively locks the rename operation. However
    /// if the `is_non_acquirable` is set, then the `NonAcquirableLock` is returned which prohibits
    /// the delta-rs to continue the write.
    #[error("The existing lock in dynamodb is non-acquirable")]
    NonAcquirableLock,

    /// Error that caused by the dynamodb request exceeded maximum allowed provisioned throughput
    /// for the table.
    #[error("Maximum allowed provisioned throughput for the table exceeded")]
    ProvisionedThroughputExceeded,

    /// Error caused by the [`DynamoDbClient::put_item`] request.
    #[error("Put item error: {0}")]
    PutItemError(RusotoError<PutItemError>),

    /// Error caused by the [`DynamoDbClient::delete_item`] request.
    #[error("Delete item error: {0}")]
    DeleteItemError(#[from] RusotoError<DeleteItemError>),

    /// Error caused by the [`DynamoDbClient::get_item`] request.
    #[error("Get item error: {0}")]
    GetItemError(RusotoError<GetItemError>),
}

impl From<DynamoError> for LockedObjectStoreError {
    fn from(e: DynamoError) -> LockedObjectStoreError {
        LockedObjectStoreError::LockProviderError(Box::new(e))
    }
}

impl From<RusotoError<PutItemError>> for DynamoError {
    fn from(error: RusotoError<PutItemError>) -> Self {
        match error {
            RusotoError::Service(PutItemError::ConditionalCheckFailed(_)) => {
                DynamoError::ConditionalCheckFailed
            }
            RusotoError::Service(PutItemError::ProvisionedThroughputExceeded(_)) => {
                DynamoError::ProvisionedThroughputExceeded
            }
            _ => DynamoError::PutItemError(error),
        }
    }
}

#[cfg(feature = "sts")]
impl From<CredentialsError> for DynamoError {
    fn from(error: CredentialsError) -> Self {
        DynamoError::AuthenticationError(error)
    }
}

impl From<RusotoError<GetItemError>> for DynamoError {
    fn from(error: RusotoError<GetItemError>) -> Self {
        match error {
            RusotoError::Service(GetItemError::ResourceNotFound(_)) => DynamoError::TableNotFound,
            RusotoError::Service(GetItemError::ProvisionedThroughputExceeded(_)) => {
                DynamoError::ProvisionedThroughputExceeded
            }
            _ => DynamoError::GetItemError(error),
        }
    }
}

/// The partition key field name in DynamoDB
pub const PARTITION_KEY_NAME: &str = "key";
/// The field name of `owner_name` in DynamoDB
pub const OWNER_NAME: &str = "ownerName";
/// The field name of `record_version_number` in DynamoDB
pub const RECORD_VERSION_NUMBER: &str = "recordVersionNumber";
/// The field name of `is_released` in DynamoDB
pub const IS_RELEASED: &str = "isReleased";
/// The field name of `lease_duration` in DynamoDB
pub const LEASE_DURATION: &str = "leaseDuration";
/// The field name of `is_non_acquirable` in DynamoDB
pub const IS_NON_ACQUIRABLE: &str = "isNonAcquirable";
/// The field name of `data` in DynamoDB
pub const DATA: &str = "data";
/// The field name of `data.source` in DynamoDB
pub const DATA_SOURCE: &str = "src";
/// The field name of `data.destination` in DynamoDB
pub const DATA_DESTINATION: &str = "dst";

mod expressions {
    /// The expression that checks whether the lock record does not exists.
    pub const ACQUIRE_LOCK_THAT_DOESNT_EXIST: &str = "attribute_not_exists(#pk)";

    /// The expression that checks whether the lock record exists and it is marked as released.
    pub const PK_EXISTS_AND_IS_RELEASED: &str = "attribute_exists(#pk) AND #ir = :ir";

    /// The expression that checks whether the lock record exists
    /// and its record version number matches with the given one.
    pub const PK_EXISTS_AND_RVN_MATCHES: &str = "attribute_exists(#pk) AND #rvn = :rvn";

    /// The expression that checks whether the lock record exists,
    /// its record version number matches with the given one
    /// and its owner name matches with the given one.
    pub const PK_EXISTS_AND_OWNER_RVN_MATCHES: &str =
        "attribute_exists(#pk) AND #rvn = :rvn AND #on = :on";
}

mod vars {
    pub const PK_PATH: &str = "#pk";
    pub const RVN_PATH: &str = "#rvn";
    pub const RVN_VALUE: &str = ":rvn";
    pub const IS_RELEASED_PATH: &str = "#ir";
    pub const IS_RELEASED_VALUE: &str = ":ir";
    pub const OWNER_NAME_PATH: &str = "#on";
    pub const OWNER_NAME_VALUE: &str = ":on";
}

/**
 * Provides a simple library for using DynamoDB's consistent read/write feature to use it for
 * managing distributed locks.
 *
 * ```rust
 * use locking_object_store::dynamodb::*;
 *
 * let lock = DynamoDbLockClient::for_region(Region::UsEast2);
 * ```
 */
pub struct DynamoDbLockClient {
    client: DynamoDbClient,
    opts: DynamoDbOptions,
}

impl std::fmt::Debug for DynamoDbLockClient {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "DynamoDbLockClient")
    }
}

impl Default for DynamoDbLockClient {
    fn default() -> Self {
        Self::for_region(Region::UsEast1)
    }
}

#[async_trait::async_trait]
impl LockClient for DynamoDbLockClient {
    async fn try_acquire_lock<T>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError>
    where
        T: Send + Sync,
    {
        Ok(self.try_acquire_lock(Some(data)).await?)
    }

    async fn get_lock<T>(&self, data: T) -> Result<Option<LockItem<T>>, LockedObjectStoreError>
    where
        T: Send + Sync,
    {
        Ok(self.get_lock(data).await?)
    }

    async fn update_data<T>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<LockItem<T>, LockedObjectStoreError>
    where
        T: Send + Sync,
    {
        Ok(self.update_data(lock).await?)
    }

    async fn release_lock<T>(&self, lock: &LockItem<T>) -> Result<bool, LockedObjectStoreError>
    where
        T: Send + Sync,
    {
        Ok(self.release_lock(lock).await?)
    }
}

impl DynamoDbLockClient {
    /// construct a new DynamoDbLockClient for the given region
    pub fn for_region(region: Region) -> Self {
        Self::new(DynamoDbClient::new(region), DynamoDbOptions::default())
    }

    /// Provide the given DynamoDbLockClient with a fully custom [rusoto_dynamodb::DynamoDbClient]
    pub fn with_client(mut self, client: DynamoDbClient) -> Self {
        self.client = client;
        self
    }

    /// Add the given [DynamoDbOptions]
    pub fn with_options(mut self, options: DynamoDbOptions) -> Self {
        self.opts = options;
        self
    }

    /// Creates new DynamoDB lock client
    fn new(client: DynamoDbClient, opts: DynamoDbOptions) -> Self {
        Self { client, opts }
    }

    /// Attempts to acquire lock. If successful, returns the lock.
    /// Otherwise returns [`Option::None`] when the lock is stolen by someone else or max
    /// provisioned throughput for a table is exceeded. Both are retryable actions.
    ///
    /// For more details on behavior,  please see [`DynamoDbLockClient::acquire_lock`].
    pub async fn try_acquire_lock<T>(
        &self,
        data: Option<T>,
    ) -> Result<Option<LockItem<T>>, DynamoError> {
        match self.acquire_lock(data).await {
            Ok(lock) => Ok(Some(lock)),
            Err(DynamoError::TimedOut(_)) => Ok(None),
            Err(DynamoError::ProvisionedThroughputExceeded) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Attempts to acquire a lock until it either acquires the lock or a specified
    /// `additional_time_to_wait_for_lock` is reached. This function will poll DynamoDB based
    /// on the `refresh_period`. If it does not see the lock in DynamoDB, it will immediately
    /// return the lock to the caller. If it does see the lock, it will note the lease
    /// expiration on the lock. If the lock is deemed stale, then this will acquire and return it.
    /// Otherwise, if it waits for as long as `additional_time_to_wait_for_lock` without acquiring
    /// the lock, then it will a [`DynamoError::TimedOut].
    ///
    /// Note that this method will wait for at least as long as the `lease_duration` in order
    /// to acquire a lock that already exists. If the lock is not acquired in that time,
    /// it will wait an additional amount of time specified in `additional_time_to_wait_for_lock`
    /// before giving up.
    pub async fn acquire_lock<T>(&self, data: Option<T>) -> Result<LockItem<T>, DynamoError> {
        let mut state = AcquireLockState {
            client: self,
            cached_lock: None,
            started: Instant::now(),
            timeout_in: self.opts.additional_time_to_wait_for_lock,
        };

        loop {
            match state.try_acquire_lock(data).await {
                Ok(lock) => return Ok(lock),
                Err(DynamoError::ConditionalCheckFailed) => {
                    if state.has_timed_out() {
                        return Err(DynamoError::TimedOut(state.started.elapsed().as_secs()));
                    }
                    tokio::time::sleep(self.opts.refresh_period).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Returns current lock from DynamoDB (if any).
    pub async fn get_lock<T>(&self, _data: T) -> Result<Option<LockItem<T>>, DynamoError> {
        let output = self
            .client
            .get_item(GetItemInput {
                consistent_read: Some(true),
                table_name: self.opts.table_name.clone(),
                key: hashmap! {
                    PARTITION_KEY_NAME.to_string() => attr(self.opts.partition_key_value.clone())
                },
                ..Default::default()
            })
            .await?;

        if let Some(item) = output.item {
            let lease_duration = {
                match item.get(LEASE_DURATION).and_then(|v| v.s.clone()) {
                    None => None,
                    Some(v) => Some(
                        v.parse::<u64>()
                            .map_err(|_| DynamoError::InvalidItemSchema)?,
                    ),
                }
            };

            let data = item.get(DATA).and_then(|r| r.s.clone());

            return Ok(Some(LockItem {
                owner_name: get_string(item.get(OWNER_NAME))?,
                record_version_number: get_string(item.get(RECORD_VERSION_NUMBER))?,
                lease_duration,
                is_released: item.contains_key(IS_RELEASED),
                data: data.into(),
                lookup_time: now_millis(),
                acquired_expired_lock: false,
                is_non_acquirable: item.contains_key(IS_NON_ACQUIRABLE),
            }));
        }

        Ok(None)
    }

    /// Update data in the upstream lock of the current user still has it.
    /// The returned lock will have a new `rvn` so it'll increase the lease duration
    /// as this method is usually called when the work with a lock is extended.
    pub async fn update_data<T>(&self, lock: &LockItem<T>) -> Result<LockItem<T>, DynamoError> {
        self.upsert_item(
            lock.data.as_deref(),
            false,
            Some(expressions::PK_EXISTS_AND_OWNER_RVN_MATCHES.to_string()),
            Some(hashmap! {
                vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
                vars::RVN_PATH.to_string() => RECORD_VERSION_NUMBER.to_string(),
                vars::OWNER_NAME_PATH.to_string() => OWNER_NAME.to_string(),
            }),
            Some(hashmap! {
                vars::RVN_VALUE.to_string() => attr(&lock.record_version_number),
                vars::OWNER_NAME_VALUE.to_string() => attr(&lock.owner_name),
            }),
        )
        .await
    }

    /// Releases the given lock if the current user still has it, returning true if the lock was
    /// successfully released, and false if someone else already stole the lock
    pub async fn release_lock<T>(&self, lock: &LockItem<T>) -> Result<bool, DynamoError> {
        if lock.owner_name != self.opts.owner_name {
            return Ok(false);
        }
        self.delete_lock(lock).await
    }

    /// Deletes the given lock from dynamodb if given rvn and owner is still matching. This is
    /// dangerous call and allows every owner to delete the active lock
    pub async fn delete_lock<T>(&self, lock: &LockItem<T>) -> Result<bool, DynamoError> {
        self.delete_item(&lock.record_version_number, &lock.owner_name)
            .await
    }

    async fn upsert_item<T>(
        &self,
        data: Option<T>,
        acquired_expired_lock: bool,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Result<LockItem<T>, DynamoError> {
        let rvn = Uuid::new_v4().to_string();

        let mut item = hashmap! {
            PARTITION_KEY_NAME.to_string() => attr(self.opts.partition_key_value.clone()),
            OWNER_NAME.to_string() => attr(&self.opts.owner_name),
            RECORD_VERSION_NUMBER.to_string() => attr(&rvn),
            LEASE_DURATION.to_string() => attr(self.opts.lease_duration),
        };

        if let Some(d) = data {
            item.insert(DATA.to_string(), attr(d));
        }

        self.client
            .put_item(PutItemInput {
                table_name: self.opts.table_name.clone(),
                item,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                ..Default::default()
            })
            .await?;

        Ok(LockItem {
            owner_name: self.opts.owner_name.clone(),
            record_version_number: rvn,
            lease_duration: Some(self.opts.lease_duration),
            is_released: false,
            data,
            lookup_time: now_millis(),
            acquired_expired_lock,
            is_non_acquirable: false,
        })
    }

    async fn delete_item(&self, rvn: &str, owner: &str) -> Result<bool, DynamoError> {
        let result = self.client.delete_item(DeleteItemInput {
            table_name: self.opts.table_name.clone(),
            key: hashmap! {
                PARTITION_KEY_NAME.to_string() => attr(self.opts.partition_key_value.clone())
            },
            condition_expression: Some(expressions::PK_EXISTS_AND_OWNER_RVN_MATCHES.to_string()),
            expression_attribute_names: Some(hashmap! {
                vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
                vars::RVN_PATH.to_string() => RECORD_VERSION_NUMBER.to_string(),
                vars::OWNER_NAME_PATH.to_string() => OWNER_NAME.to_string(),
            }),
            expression_attribute_values: Some(hashmap! {
                vars::RVN_VALUE.to_string() => attr(rvn),
                vars::OWNER_NAME_VALUE.to_string() => attr(owner),
            }),
            ..Default::default()
        });

        match result.await {
            Ok(_) => Ok(true),
            Err(RusotoError::Service(DeleteItemError::ConditionalCheckFailed(_))) => Ok(false),
            Err(e) => Err(DynamoError::DeleteItemError(e)),
        }
    }
}

/// Converts Rust String into DynamoDB string AttributeValue
fn attr<T: ToString>(s: T) -> AttributeValue {
    AttributeValue {
        s: Some(s.to_string()),
        ..Default::default()
    }
}

fn get_string(attr: Option<&AttributeValue>) -> Result<String, DynamoError> {
    Ok(attr
        .and_then(|r| r.s.as_ref())
        .ok_or(DynamoError::InvalidItemSchema)?
        .clone())
}

struct AcquireLockState<'a, T> {
    client: &'a DynamoDbLockClient,
    cached_lock: Option<LockItem<T>>,
    started: Instant,
    timeout_in: Duration,
}

impl<'a, T> AcquireLockState<'a, T> {
    /// If lock is expirable (lease_duration is set) then this function returns `true`
    /// if the elapsed time sine `started` is reached `timeout_in`.
    fn has_timed_out(&self) -> bool {
        self.started.elapsed() > self.timeout_in && {
            let non_expirable = if let Some(ref cached_lock) = self.cached_lock {
                cached_lock.lease_duration.is_none()
            } else {
                false
            };
            !non_expirable
        }
    }

    async fn try_acquire_lock(&mut self, data: T) -> Result<LockItem<T>, DynamoError> {
        match self.client.get_lock(data).await? {
            None => {
                // there's no lock, we good to acquire it
                Ok(self.upsert_new_lock(Some(data)).await?)
            }
            Some(existing) if existing.is_non_acquirable => Err(DynamoError::NonAcquirableLock),
            Some(existing) if existing.is_released => {
                // lock is released by a caller, we good to acquire it
                Ok(self.upsert_released_lock(Some(data)).await?)
            }
            Some(existing) => {
                let cached = match self.cached_lock.as_ref() {
                    // there's existing lock and it's out first attempt to acquire it
                    // first we store it, extend timeout period and try again later
                    None => {
                        // if lease_duration is None then the existing lock cannot be expired,
                        // but we still extends the timeout so the writer will wait until it's released
                        let lease_duration = existing
                            .lease_duration
                            .unwrap_or(self.client.opts.lease_duration);

                        self.timeout_in =
                            Duration::from_secs(self.timeout_in.as_secs() + lease_duration);
                        self.cached_lock = Some(existing);

                        return Err(DynamoError::ConditionalCheckFailed);
                    }
                    Some(cached) => cached,
                };
                // there's existing lock and we've already tried to acquire it, let's try again
                let cached_rvn = &cached.record_version_number;

                // let's check store rvn against current lock from dynamo
                if cached_rvn == &existing.record_version_number {
                    // rvn matches
                    if cached.is_expired() {
                        // the lock is expired and we're safe to try to acquire it
                        self.upsert_expired_lock(cached_rvn, existing.data.as_deref())
                            .await
                    } else {
                        // the lock is not yet expired, try again later
                        Err(DynamoError::ConditionalCheckFailed)
                    }
                } else {
                    // rvn doesn't match, meaning that other worker acquire it before us
                    // let's change cached lock with new one and extend timeout period
                    self.cached_lock = Some(existing);
                    Err(DynamoError::ConditionalCheckFailed)
                }
            }
        }
    }

    async fn upsert_new_lock(&self, data: Option<T>) -> Result<LockItem<T>, DynamoError> {
        self.client
            .upsert_item(
                data,
                false,
                Some(expressions::ACQUIRE_LOCK_THAT_DOESNT_EXIST.to_string()),
                Some(hashmap! {
                    vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
                }),
                None,
            )
            .await
    }

    async fn upsert_released_lock(&self, data: Option<T>) -> Result<LockItem<T>, DynamoError> {
        self.client
            .upsert_item(
                data,
                false,
                Some(expressions::PK_EXISTS_AND_IS_RELEASED.to_string()),
                Some(hashmap! {
                    vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
                    vars::IS_RELEASED_PATH.to_string() => IS_RELEASED.to_string(),
                }),
                Some(hashmap! {
                    vars::IS_RELEASED_VALUE.to_string() => attr("1")
                }),
            )
            .await
    }

    async fn upsert_expired_lock(
        &self,
        existing_rvn: &str,
        data: Option<T>,
    ) -> Result<LockItem<T>, DynamoError> {
        self.client
            .upsert_item(
                data,
                true,
                Some(expressions::PK_EXISTS_AND_RVN_MATCHES.to_string()),
                Some(hashmap! {
                  vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
                  vars::RVN_PATH.to_string() => RECORD_VERSION_NUMBER.to_string(),
                }),
                Some(hashmap! {
                    vars::RVN_VALUE.to_string() => attr(existing_rvn)
                }),
            )
            .await
    }
}

#[cfg(feature = "sts")]
fn get_web_identity_provider() -> Result<AutoRefreshingProvider<WebIdentityProvider>, DynamoError> {
    let provider = WebIdentityProvider::from_k8s_env();
    Ok(AutoRefreshingProvider::new(provider)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use maplit::hashmap;

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
    fn builder_with_string_region() {
        let inner = object_store::memory::InMemory::new();
        let store = DynamoLockBuilder::with_store(inner)
            .with_region(Region::UsEast2)
            .build();
        assert!(store.is_ok());
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

    #[test]
    fn lock_options_default_test() {
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_TABLE_NAME, "some_table");
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME, "some_owner");
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE,
            "some_pk",
        );
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_LEASE_DURATION, "40");
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_REFRESH_PERIOD_MILLIS,
            "2000",
        );
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS,
            "3000",
        );

        let options = DynamoDbOptions::default();

        assert_eq!(
            DynamoDbOptions {
                partition_key_value: "some_pk".to_string(),
                table_name: "some_table".to_string(),
                owner_name: "some_owner".to_string(),
                lease_duration: 40,
                refresh_period: Duration::from_millis(2000),
                additional_time_to_wait_for_lock: Duration::from_millis(3000),
            },
            options
        );
    }

    #[test]
    fn lock_options_from_map_test() {
        let options = DynamoDbOptions::from_map(hashmap! {
            dynamo_lock_options::DYNAMO_LOCK_TABLE_NAME.to_string() => "a_table".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME.to_string() => "an_owner".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE.to_string() => "a_pk".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_LEASE_DURATION.to_string() => "60".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_REFRESH_PERIOD_MILLIS.to_string() => "4000".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS.to_string() => "5000".to_string(),
        });

        assert_eq!(
            DynamoDbOptions {
                partition_key_value: "a_pk".to_string(),
                table_name: "a_table".to_string(),
                owner_name: "an_owner".to_string(),
                lease_duration: 60,
                refresh_period: Duration::from_millis(4000),
                additional_time_to_wait_for_lock: Duration::from_millis(5000),
            },
            options
        );
    }

    #[test]
    fn lock_options_mixed_test() {
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_TABLE_NAME, "some_table");
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME, "some_owner");
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE,
            "some_pk",
        );
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_LEASE_DURATION, "40");
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_REFRESH_PERIOD_MILLIS,
            "2000",
        );
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS,
            "3000",
        );

        let options = DynamoDbOptions::from_map(hashmap! {
            dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE.to_string() => "overridden_key".to_string()
        });

        assert_eq!(
            DynamoDbOptions {
                partition_key_value: "overridden_key".to_string(),
                table_name: "some_table".to_string(),
                owner_name: "some_owner".to_string(),
                lease_duration: 40,
                refresh_period: Duration::from_millis(2000),
                additional_time_to_wait_for_lock: Duration::from_millis(3000),
            },
            options
        );
    }
}
