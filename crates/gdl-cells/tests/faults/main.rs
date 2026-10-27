//! The conformance suite rejects broken backends: each backend here breaks
//! one rule, and the suite must fail, naming the case that holds it,
//! within its bounds.

mod endless;
mod rules;
mod wrapped;

use std::{panic::AssertUnwindSafe, pin::Pin, time::Duration};

use futures::{FutureExt, Stream};
use gdl_cells::{
    CellEvent, DirectoryError,
    conformance::{self, Backend},
};

type Events = Pin<Box<dyn Stream<Item = Result<CellEvent, DirectoryError>> + Send>>;
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
