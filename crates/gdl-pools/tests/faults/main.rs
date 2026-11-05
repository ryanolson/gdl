//! The conformance suite rejects broken backends: each backend here breaks
//! one rule, and the suite must fail, naming the case that holds it,
//! within its bounds.

mod endless;
mod rules;
mod wrapped;

use std::{panic::AssertUnwindSafe, pin::Pin, time::Duration};

use futures::{FutureExt, Stream};
use gdl_pools::{
    DirectoryError, PoolEvent,
    conformance::{self, Backend, Rule},
};

type Events = Pin<Box<dyn Stream<Item = Result<PoolEvent, DirectoryError>> + Send>>;

/// `backend`, saying it keeps every rule but `rule`, whatever it says of
/// itself. The suite then passes it only if every other case passes and
/// each case of `rule` fails on the rule itself: so a broken backend that
/// passes the suite this way is caught by the rule's cases, and by
/// nothing else.
struct Disowns<'a, B> {
    backend: &'a B,
    rule: Rule,
}

impl<B: Backend> Backend for Disowns<'_, B> {
    type Directory = B::Directory;

    async fn fresh<const N: usize>(&self) -> [B::Directory; N] {
        self.backend.fresh().await
    }

    fn buffer(&self) -> usize {
        self.backend.buffer()
    }

    fn history(&self) -> usize {
        self.backend.history()
    }

    fn watchers(&self, directory: &B::Directory, class: &str) -> usize {
        self.backend.watchers(directory, class)
    }

    async fn restart(&self, directory: B::Directory) -> B::Directory {
        self.backend.restart(directory).await
    }

    fn keeps(&self, rule: Rule) -> bool {
        rule != self.rule
    }
}

/// Run the suite against `backend` and return its failure message.
async fn failure<B: Backend>(backend: &B) -> String {
    let run = AssertUnwindSafe(conformance::run(backend)).catch_unwind();
    // On a paused clock this is virtual time: it stops a suite that would
    // run forever, without waiting.
    let outcome = tokio::time::timeout(Duration::from_secs(600), run)
        .await
        .expect("the suite did not finish");
    let Err(panic) = outcome else {
        panic!("the suite passed a broken backend");
    };
    panic
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "a panic without a message".into())
}

/// The suite fails `backend` on `rule` alone: run as a backend that
/// disowns the rule ([`Disowns`]), it passes.
async fn fails_only_on<B: Backend>(backend: &B, rule: Rule) {
    let disowns = Disowns { backend, rule };
    tokio::time::timeout(Duration::from_secs(600), conformance::run(&disowns))
        .await
        .expect("the suite did not finish");
}
