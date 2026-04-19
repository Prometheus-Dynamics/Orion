use std::sync::{LockResult, RwLockReadGuard, RwLockWriteGuard};

use tracing::error;

pub(crate) fn read_rwlock<'a, T>(
    lock: LockResult<RwLockReadGuard<'a, T>>,
    name: &'static str,
) -> RwLockReadGuard<'a, T> {
    match lock {
        Ok(guard) => guard,
        Err(err) => {
            error!(lock = name, "poisoned read lock; recovering inner value");
            err.into_inner()
        }
    }
}

pub(crate) fn write_rwlock<'a, T>(
    lock: LockResult<RwLockWriteGuard<'a, T>>,
    name: &'static str,
) -> RwLockWriteGuard<'a, T> {
    match lock {
        Ok(guard) => guard,
        Err(err) => {
            error!(lock = name, "poisoned write lock; recovering inner value");
            err.into_inner()
        }
    }
}
