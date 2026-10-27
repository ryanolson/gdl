//! A backend may request a new snapshot instead of keeping a replay log.

use std::{panic::AssertUnwindSafe, pin::Pin};

use futures::{FutureExt, Stream, StreamExt, stream};
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, DirectoryError, MemoryDirectory, MemoryLimits,
    ResumeToken, Withdrawal,
    conformance::{self, Backend, Rule},
};

type Events = Pin<Box<dyn Stream<Item = Result<CellEvent, DirectoryError>> + Send>>;

/// A memory directory that requests a new snapshot for every resume token.
#[derive(Clone)]
struct NoReplay(MemoryDirectory);

impl CellDirectory for NoReplay {
    type Watch = Events;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        self.0.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.0.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Events {
        match from {
            Some(_) => Box::pin(stream::iter([Ok(CellEvent::Resnapshot)])),
            None => Box::pin(self.0.watch(class, None)),
        }
    }
}

impl Backend for NoReplay {
    type Directory = Self;

    async fn fresh<const N: usize>(&self) -> [Self; N] {
        let directory = Self(MemoryDirectory::with_limits(MemoryLimits {
            buffer: 8,
            history: 0,
        }));
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        8
    }

    fn history(&self) -> usize {
        0
    }

    fn watchers(&self, directory: &Self, class: &str) -> usize {
        directory.0.watchers(class)
    }

    async fn restart(&self, _: Self) -> Self {
        self.fresh::<1>().await.into_iter().next().expect("one")
    }

    fn keeps(&self, rule: Rule) -> bool {
        rule != Rule::Kept
    }
}

/// A backend that keeps no changes to replay keeps the rule of changes
/// by saying `Resnapshot` for every token.
#[tokio::test]
async fn a_backend_without_replay_conforms_by_saying_resnapshot() {
    conformance::run(&NoReplay(MemoryDirectory::new())).await;
}

/// Treats another class's resume token as a request for a fresh snapshot.
#[derive(Clone)]
struct ForeignTokens(NoReplay);

impl CellDirectory for ForeignTokens {
    type Watch = Events;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        self.0.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.0.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Events {
        self.0
            .watch(class, from.filter(|token| token.class() == class))
    }
}

impl Backend for ForeignTokens {
    type Directory = Self;

    async fn fresh<const N: usize>(&self) -> [Self; N] {
        self.0.fresh().await.map(Self)
    }

    fn buffer(&self) -> usize {
        self.0.buffer()
    }

    fn history(&self) -> usize {
        0
    }

    fn watchers(&self, directory: &Self, class: &str) -> usize {
        self.0.watchers(&directory.0, class)
    }

    async fn restart(&self, directory: Self) -> Self {
        Self(self.0.restart(directory.0).await)
    }

    fn keeps(&self, rule: Rule) -> bool {
        self.0.keeps(rule)
    }
}

/// A backend that never resumes must still refuse another class's
/// token, and the suite checks it.
#[tokio::test(start_paused = true)]
async fn a_backend_without_replay_must_reject_a_foreign_token() {
    let backend = ForeignTokens(NoReplay(MemoryDirectory::new()));
    let [directory] = backend.fresh().await;
    let mut invalid = directory.watch("model", Some(ResumeToken::new("global", 0)));
    assert!(matches!(
        invalid.next().await,
        Some(Ok(CellEvent::Synced { snapshot: true, .. }))
    ));
    drop((directory, invalid));

    let outcome = AssertUnwindSafe(conformance::run(&backend))
        .catch_unwind()
        .await;
    assert!(outcome.is_err(), "the suite accepted another class's token");
}
