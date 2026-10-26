//! Cancellation keeps priority when another future spends the task's budget.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures::poll;
use gdl::{Cx, Error, ErrorKind, Hop, Items, Next, Result, Sink, SinkStage};

struct FailsAfterPending(Arc<AtomicUsize>);

impl Sink for FailsAfterPending {
    type Req = ();
    type Item = ();

    async fn generate(&self, _: Cx, _: ()) -> Result<Items<()>> {
        std::future::poll_fn(|_| {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Poll::Pending
            } else {
                Poll::Ready(Err(Error::node("producer failed")))
            }
        })
        .await
    }
}

#[tokio::test]
async fn a_cancelled_pending_attempt_is_not_polled_with_an_exhausted_budget() {
    let root = Cx::new().with_timeout(Duration::from_secs(30));
    let polls = Arc::new(AtomicUsize::new(0));
    let hop = Hop::new(
        (),
        SinkStage::new(Arc::new(FailsAfterPending(Arc::clone(&polls)))),
        (),
    );
    let mut call = std::pin::pin!(hop.generate(root.clone(), ()));
    assert!(poll!(call.as_mut()).is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    root.cancel();

    // Another branch of the same task can spend its budget before this poll.
    while tokio::task::coop::has_budget_remaining() {
        tokio::task::coop::consume_budget().await;
    }
    let result = match poll!(call.as_mut()) {
        Poll::Ready(result) => result,
        Poll::Pending => {
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            tokio::task::yield_now().await;
            call.await
        }
    };
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}
