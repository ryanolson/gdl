//! An attempt boundary over its producer's node boundary behaves the same
//! whether the node boundary is boxed (`Items`, a hop with a backward node's
//! shape) or held unboxed (`Fuse`, a hop with none). Each case below runs
//! the same producer and the same steps through both forms and records what
//! happens, in order: every poll's result, every waiter registration, the
//! producer's drop and what was cancelled when it ran, the wake of a watcher
//! on the producer's scope and on the attempt, and every panic. The two
//! records must be equal.
//!
//! The cases are every way the boundary ends: a clean end, an error, the
//! check every `CHECK_INTERVAL` items, the deadline and the cancel while
//! waiting, the yield every `YIELD_INTERVAL` items, a producer that cancels
//! its own scope, a drop before the first poll and while waiting, a poll
//! after the end, and a producer whose drop panics at an end or at the
//! boundary's drop.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use futures::Stream;

use super::{Items, Producer, bound};
use crate::Result;
use crate::cx::{Cx, REGISTERED};
use crate::error::Error;

type Log = Arc<Mutex<Vec<String>>>;

fn note(log: &Log, line: impl Into<String>) {
    log.lock().expect("log").push(line.into());
}

/// A waker that notes its wake.
struct Noting {
    log: Log,
    what: &'static str,
}

impl Wake for Noting {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        note(&self.log, format!("woken: {}", self.what));
    }
}

/// What the producer does on its `n`th poll.
type Script = fn(u32, &Cx) -> Poll<Option<Result<u32>>>;

struct Scripted {
    polls: u32,
    script: Script,
    scope: Cx,
    attempt: Cx,
    log: Log,
    panics: bool,
}

impl Stream for Scripted {
    type Item = Result<u32>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let n = self.polls;
        self.polls += 1;
        (self.script)(n, &self.scope)
    }
}

impl Drop for Scripted {
    fn drop(&mut self) {
        note(
            &self.log,
            format!(
                "producer dropped: scope cancelled {}, attempt cancelled {}",
                self.scope.is_cancelled(),
                self.attempt.is_cancelled()
            ),
        );
        if self.panics {
            panic!("producer drop");
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Form {
    Boxed,
    Unboxed,
}

#[derive(Clone, Copy)]
enum Step {
    Poll(u32),
    CancelAttempt,
    Advance(u64),
    Drop,
}

struct Case {
    name: &'static str,
    script: Script,
    panics: bool,
    deadline: bool,
    steps: &'static [Step],
}

fn shown(polled: &Poll<Option<Result<u32>>>) -> String {
    match polled {
        Poll::Pending => "pending".into(),
        Poll::Ready(None) => "end".into(),
        Poll::Ready(Some(Ok(n))) => format!("item {n}"),
        Poll::Ready(Some(Err(error))) => format!("error {:?}", error.kind()),
    }
}

async fn record(form: Form, case: &Case) -> Vec<String> {
    let log: Log = Arc::default();
    let root = if case.deadline {
        Cx::new().with_timeout(Duration::from_millis(5))
    } else {
        Cx::new()
    };
    let attempt = root.child();
    let guard = attempt.cancel_on_drop();
    let scope = attempt.child();

    // Watchers on the scope and the attempt, so that each cancel shows
    // where it falls in the record.
    let mut watchers: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();
    for (watched, what) in [(scope.clone(), "scope"), (attempt.clone(), "attempt")] {
        let mut watcher: Pin<Box<dyn Future<Output = ()> + Send>> =
            Box::pin(async move { watched.cancelled().await });
        let waker = Waker::from(Arc::new(Noting {
            log: Arc::clone(&log),
            what,
        }));
        assert!(
            watcher
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        watchers.push(watcher);
    }

    let producer = Producer::of(&scope);
    let inner: Items<u32> = Box::pin(Scripted {
        polls: 0,
        script: case.script,
        scope: scope.clone(),
        attempt: attempt.clone(),
        log: Arc::clone(&log),
        panics: case.panics,
    });
    let mut out = Some(match form {
        Form::Boxed => bound(&attempt, guard, producer.fuse(inner)),
        Form::Unboxed => bound(&attempt, guard, producer.fused(inner)),
    });

    let waker = Waker::from(Arc::new(Noting {
        log: Arc::clone(&log),
        what: "consumer",
    }));
    for step in case.steps {
        match *step {
            Step::Poll(times) => {
                for _ in 0..times {
                    let stream = out.as_mut().expect("the stream is held");
                    REGISTERED.with(|count| count.set(0));
                    let polled = catch_unwind(AssertUnwindSafe(|| {
                        stream.as_mut().poll_next(&mut Context::from_waker(&waker))
                    }));
                    let registered = REGISTERED.with(std::cell::Cell::get);
                    if registered > 0 {
                        note(&log, format!("registered {registered}"));
                    }
                    match polled {
                        Ok(polled) => note(&log, shown(&polled)),
                        Err(_) => note(&log, "poll panicked"),
                    }
                }
            }
            Step::CancelAttempt => {
                note(&log, "cancel the attempt");
                attempt.cancel();
            }
            Step::Advance(ms) => tokio::time::advance(Duration::from_millis(ms)).await,
            Step::Drop => {
                note(&log, "drop the stream");
                if catch_unwind(AssertUnwindSafe(|| drop(out.take()))).is_err() {
                    note(&log, "drop panicked");
                }
            }
        }
    }
    note(
        &log,
        format!(
            "at the end: scope cancelled {}, attempt cancelled {}",
            scope.is_cancelled(),
            attempt.is_cancelled()
        ),
    );
    drop(out);
    let lines = log.lock().expect("log").clone();
    // Repeated identical lines (a long run of items) are folded, so that a
    // failure prints a record a reader can compare.
    let mut folded: Vec<String> = Vec::new();
    let mut run = 0_usize;
    for line in lines {
        if folded.last() == Some(&line) {
            run += 1;
            continue;
        }
        if run > 0 {
            folded.push(format!("  (x{})", run + 1));
            run = 0;
        }
        folded.push(line);
    }
    if run > 0 {
        folded.push(format!("  (x{})", run + 1));
    }
    folded
}

fn ready_then_end(n: u32, _: &Cx) -> Poll<Option<Result<u32>>> {
    if n < 2 {
        Poll::Ready(Some(Ok(n)))
    } else {
        Poll::Ready(None)
    }
}

fn ready_then_error(n: u32, _: &Cx) -> Poll<Option<Result<u32>>> {
    if n < 2 {
        Poll::Ready(Some(Ok(n)))
    } else {
        Poll::Ready(Some(Err(Error::node("x"))))
    }
}

fn always_ready(_: u32, _: &Cx) -> Poll<Option<Result<u32>>> {
    Poll::Ready(Some(Ok(7)))
}

fn always_pending(_: u32, _: &Cx) -> Poll<Option<Result<u32>>> {
    Poll::Pending
}

fn pending_then_end(n: u32, _: &Cx) -> Poll<Option<Result<u32>>> {
    if n == 0 {
        Poll::Pending
    } else {
        Poll::Ready(None)
    }
}

fn cancels_itself_then_ends(n: u32, scope: &Cx) -> Poll<Option<Result<u32>>> {
    if n == 0 {
        scope.cancel();
        Poll::Ready(Some(Ok(0)))
    } else {
        Poll::Ready(None)
    }
}

const CASES: &[Case] = &[
    Case {
        name: "a clean end, then a poll after the end",
        script: ready_then_end,
        panics: false,
        deadline: false,
        steps: &[Step::Poll(4), Step::Drop],
    },
    Case {
        name: "an error, then a poll after the end",
        script: ready_then_error,
        panics: false,
        deadline: false,
        steps: &[Step::Poll(4), Step::Drop],
    },
    Case {
        name: "a cancel seen by the check every CHECK_INTERVAL items",
        script: always_ready,
        panics: false,
        deadline: false,
        steps: &[
            Step::Poll(1),
            Step::CancelAttempt,
            Step::Poll(70),
            Step::Drop,
        ],
    },
    Case {
        name: "the yield every YIELD_INTERVAL items",
        script: always_ready,
        panics: false,
        deadline: false,
        steps: &[Step::Poll(1030), Step::Drop],
    },
    Case {
        name: "a cancel while waiting",
        script: always_pending,
        panics: false,
        deadline: false,
        steps: &[
            Step::Poll(1),
            Step::CancelAttempt,
            Step::Poll(2),
            Step::Drop,
        ],
    },
    Case {
        name: "a clean end after a cancel while waiting",
        script: pending_then_end,
        panics: false,
        deadline: false,
        steps: &[
            Step::Poll(1),
            Step::CancelAttempt,
            Step::Poll(2),
            Step::Drop,
        ],
    },
    Case {
        name: "the deadline while waiting",
        script: always_pending,
        panics: false,
        deadline: true,
        steps: &[Step::Poll(1), Step::Advance(10), Step::Poll(2), Step::Drop],
    },
    Case {
        name: "a clean end after the deadline",
        script: ready_then_end,
        panics: false,
        deadline: true,
        steps: &[Step::Poll(1), Step::Advance(10), Step::Poll(3), Step::Drop],
    },
    Case {
        name: "a producer that cancels its own scope, then ends",
        script: cancels_itself_then_ends,
        panics: false,
        deadline: false,
        steps: &[Step::Poll(3), Step::Drop],
    },
    Case {
        name: "a drop before the first poll",
        script: always_ready,
        panics: false,
        deadline: false,
        steps: &[Step::Drop],
    },
    Case {
        name: "a drop while waiting, with the waiter registered",
        script: always_pending,
        panics: false,
        deadline: true,
        steps: &[Step::Poll(2), Step::Drop],
    },
    Case {
        name: "a producer whose drop panics at a clean end, then polls after it",
        script: ready_then_end,
        panics: true,
        deadline: false,
        steps: &[Step::Poll(4), Step::Drop],
    },
    Case {
        name: "a producer whose drop panics at an error",
        script: ready_then_error,
        panics: true,
        deadline: false,
        steps: &[Step::Poll(4), Step::Drop],
    },
    Case {
        name: "a producer whose drop panics at a cancel while waiting",
        script: always_pending,
        panics: true,
        deadline: false,
        steps: &[
            Step::Poll(1),
            Step::CancelAttempt,
            Step::Poll(2),
            Step::Drop,
        ],
    },
    Case {
        name: "a producer whose drop panics at the stream's drop",
        script: always_pending,
        panics: true,
        deadline: false,
        steps: &[Step::Poll(1), Step::Drop],
    },
];

#[tokio::test(start_paused = true)]
async fn the_unboxed_node_boundary_ends_and_drops_as_the_boxed_one_does() {
    for case in CASES {
        let boxed = record(Form::Boxed, case).await;
        let unboxed = record(Form::Unboxed, case).await;
        assert_eq!(
            unboxed, boxed,
            "{}: the unboxed form differs\nboxed: {boxed:#?}",
            case.name
        );
        // The record is not empty or trivially equal: the producer ran and
        // was dropped in every case.
        assert!(
            boxed
                .iter()
                .any(|line| line.starts_with("producer dropped")),
            "{}: {boxed:#?}",
            case.name
        );
    }
}

/// The record tells the forms apart when they differ: a boundary that
/// cancels the attempt before it drops the producer is caught.
#[tokio::test]
async fn the_record_shows_the_order_of_a_drop_and_a_cancel() {
    let case = &CASES[0];
    let boxed = record(Form::Boxed, case).await;
    let dropped = boxed
        .iter()
        .position(|line| line.starts_with("producer dropped"))
        .expect("the producer was dropped");
    let scope = boxed
        .iter()
        .position(|line| line == "woken: scope")
        .expect("the scope was cancelled");
    assert!(dropped < scope, "{boxed:#?}");
    assert_eq!(
        boxed[dropped],
        "producer dropped: scope cancelled false, attempt cancelled false"
    );
}
