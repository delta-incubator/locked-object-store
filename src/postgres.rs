use crate::errors::LockedObjectStoreError;
use crate::{LockClient, LockItem};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{Executor, FromRow, PgPool, Pool, Postgres};
use std::ops::{Add, Deref};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug)]
pub struct PostgresLockClient {
    options: PostgresOptions,
    db: Pool<Postgres>,
}

#[derive(Debug)]
pub struct PostgresOptions {
    /// Owner name, should be unique among the clients which work with the lock.
    pub owner_name: String,
    /// The amount of time (in seconds) that the owner has for the acquired lock.
    pub lease_duration: i64,
    /// The amount of time to wait before trying to get the lock again.
    pub refresh_period: Duration,
    /// The amount of time to wait in addition to `lease_duration`.
    pub additional_time_to_wait_for_lock: Duration,
}

/// Postgres representation of the lock. You'll notice that we do not represent the inner data, we
/// don't really need to deserialize or inspect that payload once the equality check has taken place
/// so in order to get around having to constrain the impl to require Deserialize we've gone this
/// direction.
#[derive(Clone, Debug, Deserialize, Serialize, FromRow)]
pub struct PgLockItem {
    /// The name of the owner that owns this lock.
    pub owner_name: String,
    /// Current version number of the lock in Postgres.
    pub record_version_number: Uuid,
    /// The amount of time (in seconds) that the owner has this lock for.
    /// If lease_duration is None then the lock is non-expirable.
    pub lease_duration: Option<i64>,
    /// Tells whether or not the lock was marked as released when loaded from Postgres.
    pub is_released: bool,
    /// The last time this lock was updated or retrieved.
    pub lookup_time: chrono::NaiveDateTime,
    /// Whether or not this lock should be marked as acquirable
    pub is_non_acquirable: bool,
}

// we do the checking for expired acquire and whether or not its acquirable in the conversion from
// a PgLock to the standard Lock - since this flow should only ever be db -> PgLock -> Lock we
// shouldn't have any issues running the checks here vs somewhere else.
impl<T> From<PgLockItem> for LockItem<T> {
    fn from(value: PgLockItem) -> Self {
        // sqlx doesn't like u64s so we have to pull it out as an i64
        let lease_duration = value.lease_duration.map(|v| v as u64);

        let now = Utc::now().naive_utc();
        let expiration_time = value
            .lookup_time
            .add(chrono::Duration::seconds(lease_duration.unwrap_or(0) as i64));

        Self {
            owner_name: value.owner_name,
            record_version_number: value.record_version_number.to_string(),
            lease_duration,
            is_released: value.is_released,
            data: None,
            // as can be dangerous, but here it should be safe enough as we're doubling space
            lookup_time: value.lookup_time.timestamp_millis() as u128,
            acquired_expired_lock: now > expiration_time,
            is_non_acquirable: value.is_non_acquirable,
        }
    }
}

impl PostgresLockClient {
    //! `db_connection_string` options can be found here - https://docs.rs/sqlx/0.6.3/sqlx/postgres/struct.PgConnectOptions.html.
    //! The options will be parsed from the passed `&str` automatically. Migrations will be run automatically
    //! on the `locked_object_store` schema.  
    // fairly simple new function - options can be passed in through the connection string. I figured
    // this was going to be easier than creating a complicated facade that mirrored the options
    pub async fn new(
        db_connection_string: &str,
        options: PostgresOptions,
    ) -> Result<Self, LockedObjectStoreError> {
        let db = PgPool::connect(db_connection_string).await?;

        sqlx::migrate!().run(&db).await?;

        Ok(Self { db, options })
    }
}

#[async_trait::async_trait]
impl LockClient for PostgresLockClient {
    async fn try_acquire_lock<T: Serialize + Send + Copy>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError> {
        // we wrap this all in a transaction so that the SELECT FOR UPDATE doesn't block our own
        // updates if the lock exists and we can acquire it
        let mut transaction = self.db.begin().await?;
        let data = Json(data);

        let lock: Option<PgLockItem> = sqlx::query_as(
            r#"
        SELECT record_version_number,owner_name,lease_duration,is_released,lookup_time,is_non_acquirable 
        FROM locked_object_store.locks 
        WHERE data = $1 
        FOR UPDATE
        LIMIT 1
        "#,
        )
        .bind(data)
        .fetch_optional(&mut transaction)
        .await?;

        let lock = match lock {
            // no lock means we're good to acquire and insert
            None => {
                let l: PgLockItem = sqlx::query_as(
                    r#"
                INSERT INTO locked_object_store.locks(owner_name,lease_duration,data) 
                VALUES($1,$2,$3) RETURNING *"#,
                )
                .bind(self.options.owner_name.clone())
                .bind(self.options.lease_duration)
                .bind(data)
                .fetch_one(&mut transaction)
                .await?;

                // we have to put the data back into the lock even though we're not deserializing it
                // from storage - again doing this so we avoid having to constrain the impl with the
                // Deserialize trait as well as Serialize
                let mut l = LockItem::from(l);
                l.data = Some(data.0);

                transaction.commit().await?;
                return Ok(Some(l));
            }
            Some(l) => l,
        };

        let mut lock = LockItem::from(lock);
        lock.data = Some(data.0);

        // non-acquirable means that even if expired we cannot get this lock, so it's not a retryable
        // option - send the error back
        if lock.is_non_acquirable {
            transaction.commit().await?;
            return Err(LockedObjectStoreError::NonAcquirableLock);
        }

        // if we can acquire the lock, do so and change the version to indicate to anyone else holding
        // it that we're stale now
        if lock.is_released || lock.acquired_expired_lock {
            let lock: PgLockItem = sqlx::query_as(
                r#"
            UPDATE locked_object_store.locks 
            SET record_version_number = $1, lease_duration = $2, lookup_time = CLOCK_TIMESTAMP(), is_released = false
            WHERE data = $3 and record_version_number = $4::uuid
            RETURNING *;
            "#,
            )
            .bind(Uuid::new_v4())
            .bind(self.options.lease_duration)
            .bind(data)
            .bind(lock.record_version_number)
            .fetch_one(&mut transaction)
            .await?;

            // we have to put the data back into the lock even though we're not deserializing it
            // from storage - again doing this so we avoid having to constrain the impl with the
            // Deserialize trait as well as Serialize
            let mut lock = LockItem::from(lock);
            lock.data = Some(data.0);

            transaction.commit().await?;
            Ok(Some(lock))
        } else {
            transaction.commit().await?;
            Ok(None)
        }
    }

    async fn get_lock<T: Serialize + Send + Copy>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError> {
        let lock: Option<PgLockItem> = sqlx::query_as(
            r#"
        SELECT record_version_number,owner_name,lease_duration,is_released,lookup_time,is_non_acquirable 
        FROM locked_object_store.locks 
        WHERE data = $1 
        FOR UPDATE
        LIMIT 1
        "#,
        )
            .bind(Json(data))
            .fetch_optional(&self.db)
            .await?;

        match lock {
            None => Err(LockedObjectStoreError::NotExists),
            Some(l) => {
                let mut lock = LockItem::from(l);
                lock.data = Some(data);
                Ok(Some(lock))
            }
        }
    }

    async fn update_data<T: Serialize + Sync + Send + Copy>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<LockItem<T>, LockedObjectStoreError> {
        todo!()
    }

    async fn release_lock<T: Serialize + Sync + Send + Copy>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<bool, LockedObjectStoreError> {
        // we wrap this all in a transaction so that the SELECT FOR UPDATE doesn't block our own
        // updates if the lock exists and we can acquire it - we don't need the actual lock here
        // just the row lock FOR UPDATE gives us
        let mut transaction = self.db.begin().await?;
        sqlx::query(
            r#"
        SELECT record_version_number,owner_name,lease_duration,is_released,lookup_time,is_non_acquirable 
        FROM locked_object_store.locks 
        WHERE data = $1
        AND record_version_number = $2::uuid
        FOR UPDATE
        LIMIT 1
        "#,
        )
            .bind(Json(lock.data))
            .bind(lock.record_version_number.clone())
            .fetch_optional(&mut transaction)
            .await?;

        transaction
            .execute(
                sqlx::query(
                    r#"
        UPDATE locked_object_store.locks SET is_released = true 
        WHERE record_version_number = $1::uuid AND data = $2
        "#,
                )
                .bind(lock.record_version_number.clone())
                .bind(Json(lock.data)),
            )
            .await?;

        transaction.commit().await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::TestError;
    use crate::postgres::{PostgresLockClient, PostgresOptions};
    use crate::LockClient;
    use serde::Serialize;
    use std::time::Duration;

    #[derive(Serialize, Copy, Clone)]
    struct TestData {
        id: u32,
    }

    #[tokio::test]
    async fn test_basic_acquire() -> Result<(), TestError> {
        let db_connection_string = option_env!("DATABASE_URL")
            .ok_or(TestError::RequiredEnvVar("DATABASE_URL".to_string()))?;

        let lock_client = PostgresLockClient::new(
            db_connection_string,
            PostgresOptions {
                owner_name: "test suite".to_string(),
                lease_duration: 10, // long lease duration so our test suite can check locks
                refresh_period: Duration::from_secs(1),
                additional_time_to_wait_for_lock: Duration::from_millis(200),
            },
        )
        .await?;

        let lock = lock_client
            .try_acquire_lock(TestData { id: 0 })
            .await?
            .ok_or(TestError::UnwrapOption("initial lock return".to_string()))?;

        assert!(lock.lease_duration.is_some());
        // just a sanity check that our lease matches our options
        assert_eq!(lock.lease_duration.unwrap(), 10);
        assert!(!lock.is_non_acquirable);
        assert!(!lock.acquired_expired_lock);

        // we should not be able to acquire the the lock immediately after locking and before release
        let lock2 = lock_client.try_acquire_lock(TestData { id: 0 }).await?;
        assert!(lock2.is_none());

        let result = lock_client.release_lock(&lock).await?;
        assert!(result);
        Ok(())
    }

    #[tokio::test]
    async fn test_get() -> Result<(), TestError> {
        let db_connection_string = option_env!("DATABASE_URL")
            .ok_or(TestError::RequiredEnvVar("DATABASE_URL".to_string()))?;

        let lock_client = PostgresLockClient::new(
            db_connection_string,
            PostgresOptions {
                owner_name: "test suite".to_string(),
                lease_duration: 10, // long lease duration so our test suite can check locks
                refresh_period: Duration::from_secs(1),
                additional_time_to_wait_for_lock: Duration::from_millis(200),
            },
        )
        .await?;

        lock_client
            .try_acquire_lock(TestData { id: 1 })
            .await?
            .ok_or(TestError::UnwrapOption("lock return".to_string()))?;

        let lock = lock_client.get_lock(TestData { id: 1 }).await?;

        assert!(lock.is_some());
        let lock = lock.unwrap();
        assert!(lock.lease_duration.is_some());
        // just a sanity check that our lease matches our options
        assert_eq!(lock.lease_duration.unwrap(), 10);
        assert!(!lock.is_non_acquirable);
        assert!(!lock.acquired_expired_lock);

        let result = lock_client.release_lock(&lock).await?;
        assert!(result);
        Ok(())
    }
}
