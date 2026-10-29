use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::StreamExt;
use gdl_pools::{
    DirectoryError, MemoryDirectory, MemoryLimits, PoolDirectory, PoolEvent, PoolRecord,
    ResumeToken, Withdrawal, conformance::Backend,
};

use crate::{Events, failure};

const LIMITS: MemoryLimits = MemoryLimits {
    buffer: 8,
    history: 5,
};

/// Wraps the memory directory, whose watches pass through `rewrite`.
#[derive(Clone)]
struct Wrapped<F> {
    inner: MemoryDirectory,
    // Records a refused publish stored anyway, by class and pool.
    refused: Arc<Mutex<HashMap<(String, String), PoolRecord>>>,
    store_refused: bool,
    rewrite: F,
}

impl<F> PoolDirectory for Wrapped<F>
where
    F: Fn(&str, Option<ResumeToken>) -> Option<ResumeToken> + Clone + Send + Sync + 'static,
{
    type Watch = Events;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        let result = self.inner.publish(record.clone()).await;
        let key = (record.class.clone(), record.name.clone());
        let mut refused = self.refused.lock().expect("lock");
        match result {
            Err(DirectoryError::NotNewer { .. }) if self.store_refused => {
                refused.insert(key, record);
            }
            // A write that succeeds overwrites what was stored, as in a
            // store that wrote the refused record.
            Ok(()) => {
                refused.remove(&key);
            }
            _ => {}
        }
        result
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        let key = (withdrawal.class.clone(), withdrawal.pool.clone());
        let result = self.inner.withdraw(withdrawal).await;
        if result.is_ok() {
            self.refused.lock().expect("lock").remove(&key);
        }
        result
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Events {
        let refused = Arc::clone(&self.refused);
        let inner = self.inner.watch(class, (self.rewrite)(class, from));
        Box::pin(inner.map(move |event| match event {
            Ok(PoolEvent::Current(record)) => {
                let key = (record.class.clone(), record.name.clone());
                let stored = refused.lock().expect("lock").get(&key).cloned();
                Ok(PoolEvent::Current(stored.unwrap_or(record)))
            }
            other => other,
        }))
    }
}

struct WrappedBackend<F> {
    store_refused: bool,
    rewrite: F,
}

impl<F> Backend for WrappedBackend<F>
where
    F: Fn(&str, Option<ResumeToken>) -> Option<ResumeToken> + Clone + Send + Sync + 'static,
{
    type Directory = Wrapped<F>;

    async fn fresh<const N: usize>(&self) -> [Wrapped<F>; N] {
        let directory = Wrapped {
            inner: MemoryDirectory::with_limits(LIMITS),
            refused: Arc::default(),
            store_refused: self.store_refused,
            rewrite: self.rewrite.clone(),
        };
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        LIMITS.history
    }

    fn watchers(&self, directory: &Wrapped<F>, class: &str) -> usize {
        directory.inner.watchers(class)
    }

    // The case of each wrapping is not about a restart.
    async fn restart(&self, directory: Wrapped<F>) -> Wrapped<F> {
        directory
    }
}

/// A backend that stores a publish it refuses as not newer must fail the
/// ordering case: a snapshot right after the refusal shows it.
#[tokio::test]
async fn a_backend_that_stores_a_refused_publish_fails() {
    let backend = WrappedBackend {
        store_refused: true,
        rewrite: |_: &str, from| from,
    };
    let why = failure(&backend).await;
    assert!(
        why.ends_with("failed: writes_are_ordered_by_epoch_and_seq"),
        "{why}"
    );
}

/// A backend that serves a token in another class's changes must fail the
/// resume case, even when that class has changes at the token's position.
#[tokio::test]
async fn a_backend_that_ignores_a_tokens_class_fails() {
    let backend = WrappedBackend {
        store_refused: false,
        rewrite: |class: &str, from: Option<ResumeToken>| {
            from.map(|token| ResumeToken::new(class, token.position()))
        },
    };
    let why = failure(&backend).await;
    assert!(
        why.ends_with("failed: a_watch_resumes_after_a_token"),
        "{why}"
    );
}
