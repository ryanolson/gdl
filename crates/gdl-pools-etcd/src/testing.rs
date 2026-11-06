//! Hooks for the race cases of the test target that runs against a real
//! etcd (`tests/etcd`): built only with the `etcd-local` feature, which no
//! deployment enables.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, PoisonError},
};

use crate::EtcdDirectory;

/// Where a class's watch task awaits the hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    /// Between two pages of a snapshot, once the first fixed its revision.
    BetweenPages,
    /// Just before the task creates the store's watch.
    BeforeWatch,
    /// Just before the task reads the class's newest key, for watches that
    /// joined it.
    BeforeBarrier,
}

/// A hook the watch task awaits at each [`Point`].
pub type Hook = Arc<dyn Fn(Point) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

impl EtcdDirectory {
    /// Await `hook` at each [`Point`] of every watch task of this handle
    /// and its clones.
    pub fn set_hook(&self, hook: Hook) {
        *self
            .shared
            .hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(hook);
    }
}
