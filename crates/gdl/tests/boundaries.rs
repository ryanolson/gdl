//! Stream boundaries cancel their scopes even when a producer's destructor panics.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::Stream;
use gdl::{Cx, Items, Result, items};

struct Producer {
    cx: Cx,
    dropped: Arc<AtomicBool>,
    panics: bool,
}

impl Stream for Producer {
    type Item = Result<()>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        assert!(
            !self.cx.is_cancelled(),
            "the producer drops before cancellation"
        );
        self.dropped.store(true, Ordering::SeqCst);
        if self.panics {
            panic!("producer destructor failed");
        }
    }
}

fn dropping_cancels_even_when_the_producer_panics(boundary: impl Fn(&Cx, Items<()>) -> Items<()>) {
    for panics in [false, true] {
        let cx = Cx::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let out = boundary(
            &cx,
            items(Producer {
                cx: cx.clone(),
                dropped: Arc::clone(&dropped),
                panics,
            }),
        );
        let result = catch_unwind(AssertUnwindSafe(|| drop(out)));
        assert_eq!(result.is_err(), panics);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(
            cx.is_cancelled(),
            "the scope must be cancelled after producer drop, panics={panics}"
        );
    }
}

#[test]
fn dropping_a_node_boundary_cancels_when_its_producer_destructor_panics() {
    dropping_cancels_even_when_the_producer_panics(|cx, inner| gdl::__runtime::fuse(inner, cx));
}

#[test]
fn dropping_an_attempt_boundary_cancels_when_its_producer_destructor_panics() {
    dropping_cancels_even_when_the_producer_panics(|cx, inner| {
        gdl::__runtime::bound_measured(cx, cx.cancel_on_drop(), inner, None)
    });
}
