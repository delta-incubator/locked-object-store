use crate::errors::LockedObjectStoreError;
use crate::{LockClient, LockItem};
use serde::Serialize;
use sqlx::{Pool, Postgres};

#[derive(Debug)]
pub struct PostgresLockClient {
    db: Pool<Postgres>,
}

#[async_trait::async_trait]
impl LockClient for PostgresLockClient {
    async fn try_acquire_lock<T: Serialize + Send>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError> {
        todo!()
    }

    async fn get_lock<T: Serialize + Send>(
        &self,
        data: T,
    ) -> Result<Option<LockItem<T>>, LockedObjectStoreError> {
        todo!()
    }

    async fn update_data<T: Serialize>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<LockItem<T>, LockedObjectStoreError> {
        todo!()
    }

    async fn release_lock<T: Serialize>(
        &self,
        lock: &LockItem<T>,
    ) -> Result<bool, LockedObjectStoreError> {
        todo!()
    }
}
