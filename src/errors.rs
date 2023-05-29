use thiserror::Error;

#[derive(Error, Debug)]
pub enum LockedObjectStoreError {
    /// Error returned by `acquire_lock` which indicates that the lock could
    /// not be acquired for more that returned number of seconds.
    #[error("Could not acquire lock for {0} sec")]
    TimedOut(u64),

    /// Error returned which indicates that the lock could
    /// not be acquired because the `is_non_acquirable` is set to `true`.
    /// Usually this is done intentionally outside of a locking client.
    ///
    /// The example could be the dropping of a table. For example external service acquires the lock
    /// to drop (or drop/create etc., something that modifies the delta log completely) a table.
    /// The dangerous part here is that the concurrent delta workers will still perform the write
    /// whenever the lock is available, because it effectively locks the rename operation. However
    /// if the `is_non_acquirable` is set, then the `NonAcquirableLock` is returned which prohibits
    /// the delta-rs to continue the write.
    #[error("The existing lock is non-acquirable")]
    NonAcquirableLock,
}
