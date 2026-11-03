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

#[tokio::test]
async fn a_clean_end_reports_cancellation_from_producer_cleanup() {
    use futures::StreamExt;
    use gdl::{Backward, ErrorKind, Forward, Hop, Next, Sink, SinkStage};

    struct CarryAttempt;
    impl Forward for CarryAttempt {
        type In = ();
        type Out = Cx;

        async fn forward(&self, cx: &Cx, (): ()) -> Result<Cx> {
            Ok(cx.clone())
        }
    }

    struct Cleanup(Cx);
    impl Stream for Cleanup {
        type Item = Result<()>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }

    struct Ends;
    impl Sink for Ends {
        type Req = Cx;
        type Item = ();

        async fn generate(&self, _cx: Cx, attempt: Cx) -> Result<Items<()>> {
            Ok(items(Cleanup(attempt)))
        }
    }

    struct Keep;
    impl Backward for Keep {
        type In = ();
        type Out = ();

        fn backward(&self, _cx: &Cx, input: Items<()>) -> Items<()> {
            input
        }
    }

    let with_backward = Hop::new(
        (Arc::new(CarryAttempt), ()),
        SinkStage::new(Arc::new(Ends)),
        (Arc::new(Keep), ()),
    );
    let mut control = with_backward.generate(Cx::new(), ()).await.expect("stream");
    assert_eq!(
        control.next().await.map(|r| r.map_err(|e| e.kind())),
        Some(Err(ErrorKind::Cancelled)),
        "cleanup cancellation is visible through separate boundaries"
    );
    assert!(control.next().await.is_none());

    let bare = Hop::new(
        (Arc::new(CarryAttempt), ()),
        SinkStage::new(Arc::new(Ends)),
        (),
    );
    let mut out = bare.generate(Cx::new(), ()).await.expect("stream");
    assert_eq!(
        out.next().await.map(|r| r.map_err(|e| e.kind())),
        Some(Err(ErrorKind::Cancelled)),
        "combining boundaries must preserve cleanup cancellation"
    );
    assert!(out.next().await.is_none());
}

#[tokio::test]
async fn polling_after_a_producer_cleanup_panic_still_cancels_the_attempt() {
    use std::sync::OnceLock;

    use futures::{FutureExt, StreamExt};
    use gdl::{Backward, Error, Forward, Hop, Next, Sink, SinkStage};

    struct ObserveAttempt(Arc<OnceLock<Cx>>);
    impl Forward for ObserveAttempt {
        type In = ();
        type Out = ();

        async fn forward(&self, cx: &Cx, (): ()) -> Result<()> {
            self.0.set(cx.clone()).expect("one attempt");
            Ok(())
        }
    }

    struct Cleanup {
        cx: Cx,
        error: bool,
    }
    impl Stream for Cleanup {
        type Item = Result<()>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.error.then(|| Err(Error::node("producer failed"))))
        }
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            assert!(
                !self.cx.is_cancelled(),
                "the producer drops before its scope is cancelled"
            );
            panic!("producer cleanup failed");
        }
    }

    struct Ends {
        scope: Arc<OnceLock<Cx>>,
        error: bool,
    }
    impl Sink for Ends {
        type Req = ();
        type Item = ();

        async fn generate(&self, cx: Cx, (): ()) -> Result<Items<()>> {
            self.scope.set(cx.clone()).expect("one producer");
            Ok(items(Cleanup {
                cx,
                error: self.error,
            }))
        }
    }

    struct Keep;
    impl Backward for Keep {
        type In = ();
        type Out = ();

        fn backward(&self, _cx: &Cx, input: Items<()>) -> Items<()> {
            input
        }
    }

    let mut uncancelled = Vec::new();
    for error in [false, true] {
        for has_backward in [true, false] {
            let attempt = Arc::new(OnceLock::new());
            let scope = Arc::new(OnceLock::new());
            let forward = (Arc::new(ObserveAttempt(Arc::clone(&attempt))), ());
            let sink = SinkStage::new(Arc::new(Ends {
                scope: Arc::clone(&scope),
                error,
            }));
            let mut out = if has_backward {
                Hop::new(forward, sink, (Arc::new(Keep), ()))
                    .generate(Cx::new(), ())
                    .await
            } else {
                Hop::new(forward, sink, ()).generate(Cx::new(), ()).await
            }
            .expect("stream");
            let attempt = attempt.get().expect("attempt started");

            assert!(AssertUnwindSafe(out.next()).catch_unwind().await.is_err());
            assert!(scope.get().expect("producer started").is_cancelled());
            assert!(!attempt.is_cancelled());
            assert!(out.next().await.is_none());
            if has_backward {
                assert!(
                    attempt.is_cancelled(),
                    "separate boundaries end the attempt"
                );
            } else if !attempt.is_cancelled() {
                uncancelled.push(error);
            }
            // Keep the ended stream until the cancellation check above.
            drop(out);
            assert!(attempt.is_cancelled());
        }
    }
    assert!(
        uncancelled.is_empty(),
        "ended attempts remain live after a cleanup panic: error={uncancelled:?}"
    );
}
