//! A backend may fold several writes of one pool into the latest, as the
//! `dir` backend does between two polls: rule 3 allows it, so the rule
//! cases must too.

use std::pin::Pin;

use futures::{FutureExt, Stream, StreamExt, stream};
use gdl_pools::{
    DirectoryError, MemoryDirectory, MemoryLimits, PoolDirectory, PoolEvent, PoolRecord,
    ResumeToken, Withdrawal,
    conformance::{self, Backend, Rule},
};

type Events = Pin<Box<dyn Stream<Item = Result<PoolEvent, DirectoryError>> + Send>>;

const LIMITS: MemoryLimits = MemoryLimits {
    buffer: 8,
    history: 5,
};

/// A memory directory whose watches drop a put when the next event, ready
/// at once, is a put of the same pool.
#[derive(Clone)]
struct Folding(MemoryDirectory);

impl PoolDirectory for Folding {
    type Watch = Events;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.0.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.0.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Events {
        let inner: Events = Box::pin(self.0.watch(class, from));
        Box::pin(stream::unfold(
            (inner, None::<Result<PoolEvent, DirectoryError>>),
            |(mut inner, held)| async move {
                let mut event = match held {
                    Some(event) => event,
                    None => inner.next().await?,
                };
                loop {
                    let Ok(PoolEvent::Put { record, .. }) = &event else {
                        return Some((event, (inner, None)));
                    };
                    let pool = record.name.clone();
                    match inner.next().now_or_never() {
                        Some(Some(Ok(PoolEvent::Put { record, token }))) if record.name == pool => {
                            event = Ok(PoolEvent::Put { record, token });
                        }
                        Some(Some(next)) => return Some((event, (inner, Some(next)))),
                        _ => return Some((event, (inner, None))),
                    }
                }
            },
        ))
    }
}

struct Folds;

impl Backend for Folds {
    type Directory = Folding;

    async fn fresh<const N: usize>(&self) -> [Folding; N] {
        let directory = Folding(MemoryDirectory::with_limits(LIMITS));
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        LIMITS.buffer
    }

    fn history(&self) -> usize {
        LIMITS.history
    }

    fn watchers(&self, directory: &Folding, class: &str) -> usize {
        directory.0.watchers(class)
    }

    async fn restart(&self, _: Folding) -> Folding {
        Folding(MemoryDirectory::with_limits(LIMITS))
    }

    fn keeps(&self, rule: Rule) -> bool {
        rule != Rule::Kept
    }
}

/// A folding backend breaks no rule, and passes the suite.
#[tokio::test]
async fn a_backend_that_folds_writes_of_one_pool_conforms() {
    conformance::run(&Folds).await;
}
