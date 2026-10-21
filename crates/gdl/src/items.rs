use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use pin_project_lite::pin_project;
use tokio::time::{Instant, Sleep};
use tokio_util::sync::{CancellationToken, DropGuard, WaitForCancellationFutureOwned};

use crate::cx::Cx;
use crate::error::{Error, Result};
use crate::meter::{Reading, ended};

/// A stream of items: a response, or a streamed request's items. Every
/// response in gdl is a stream; a unary response is a stream of one item. An
/// `Err` item is terminal.
pub type Items<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send>>;

/// Box a stream as [`Items`].
pub fn items<T, S>(stream: S) -> Items<T>
where
    S: Stream<Item = Result<T>> + Send + 'static,
{
    Box::pin(stream)
}

/// A stream of one terminal error.
pub(crate) fn failed<T: Send + 'static>(error: Error) -> Items<T> {
    Box::pin(futures::stream::once(async move { Err(error) }))
}

/// The stream up to and including its first `Err`. The input is dropped at
/// that error, not when the result is: an input holding a remote anchor
/// releases it at once.
///
/// It yields every `YIELD_INTERVAL` consecutive ready items, as a node
/// boundary does. Its consumer is a node, and a node that consumes without
/// producing (a filter) loops over its input inside one poll; over remote
/// input that stays ready, that loop would never return to the pump that
/// forwards a cancel or a stop.
pub(crate) fn until_error<T: Send + 'static>(inner: Items<T>) -> Items<T> {
    Box::pin(UntilError {
        inner: Some(inner),
        ready_run: 0,
    })
}

struct UntilError<T> {
    inner: Option<Items<T>>,
    ready_run: u16,
}

impl<T> Stream for UntilError<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ready_run >= YIELD_INTERVAL {
            self.ready_run = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = inner.as_mut().poll_next(cx);
        match polled {
            Poll::Ready(Some(Ok(_))) => self.ready_run += 1,
            Poll::Ready(Some(Err(_)) | None) => self.inner = None,
            Poll::Pending => self.ready_run = 0,
        }
        polled
    }
}

/// A node boundary: the stream ends after its first `Err`, so the next node
/// never sees an item a terminal error should have stopped, and any ending
/// (or dropping the stream) cancels the producing node's own scope through
/// `guard`. Cancelling at the producer, not the attempt, matters: the next
/// node may still be holding the error, and must be able to deliver it.
/// Cancel and deadline are enforced once per attempt by [`bound`], not here.
///
/// It also yields every `YIELD_INTERVAL` consecutive ready items. A node
/// that consumes items without producing any (a filter, a batcher) polls its
/// input in a loop inside one call; over an always-ready producer that loop
/// would never return to the attempt boundary, which is where cancel and
/// deadline are checked.
///
/// `scope` is the producing node's own scope. A node that ends its stream
/// cleanly after its scope was cancelled (by itself, or by its attempt) or
/// after the deadline is reported as interrupted, not as success, just as at
/// an attempt boundary: whether a backward node follows must not change the
/// outcome.
pub(crate) fn fuse<T: Send + 'static>(inner: Items<T>, scope: &Cx) -> Items<T> {
    fuse_measured(inner, scope, None)
}

/// [`fuse`], ending `reading` with the outcome the boundary reports: its
/// first error, an interruption it reports for a clean end, or its end.
/// Measured here, a node's request costs no allocation of its own.
pub(crate) fn fuse_measured<T: Send + 'static>(
    inner: Items<T>,
    scope: &Cx,
    reading: Option<Reading>,
) -> Items<T> {
    Box::pin(Fuse {
        inner: Some(inner),
        guard: Some(scope.cancel_on_drop()),
        token: scope.cancel_token(),
        deadline_at: scope.deadline().map(|deadline| deadline.instant()),
        ready_run: 0,
        reading,
    })
}

/// Consecutive ready items a node boundary passes before it yields.
const YIELD_INTERVAL: u16 = 1024;

struct Fuse<T> {
    inner: Option<Items<T>>,
    guard: Option<DropGuard>,
    token: CancellationToken,
    deadline_at: Option<Instant>,
    ready_run: u16,
    /// The producing node's request, when the runtime records it; dropped
    /// with the stream, it ends as given up.
    reading: Option<Reading>,
}

impl<T> Stream for Fuse<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.inner.is_none() {
            return Poll::Ready(None);
        }
        if self.ready_run >= YIELD_INTERVAL {
            self.ready_run = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let this = &mut *self;
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = inner.as_mut().poll_next(cx);
        match &polled {
            Poll::Ready(Some(Ok(_))) => this.ready_run += 1,
            Poll::Ready(Some(Err(error))) => {
                ended(&mut this.reading, Some(error));
                end(&mut this.inner, &mut this.guard);
            }
            Poll::Ready(None) => {
                // Checked before ending: ending cancels the scope itself.
                let interruption = interrupted(&this.token, this.deadline_at);
                ended(&mut this.reading, interruption.as_ref());
                end(&mut this.inner, &mut this.guard);
                return Poll::Ready(interruption.map(Err));
            }
            Poll::Pending => this.ready_run = 0,
        }
        polled
    }
}

/// An attempt boundary: the outermost stream of one attempt.
///
/// It ends the stream after the first `Err`, with `Cancelled` when `cx` is
/// cancelled, and with `DeadlineExceeded` when `cx`'s deadline passes. Any
/// ending, including a normal one, drops `guard`, which cancels the attempt:
/// a producer still running after its stream has ended (for example, a sink
/// waiting after it reported an error) must stop even while the consumer
/// keeps the ended stream. Dropping the stream drops `guard` too.
pub(crate) fn bound<T: Send + 'static>(cx: &Cx, guard: DropGuard, inner: Items<T>) -> Items<T> {
    bound_measured(cx, guard, inner, None)
}

/// [`bound`], ending `reading` with the outcome the boundary reports, as
/// [`fuse_measured`] does.
pub(crate) fn bound_measured<T: Send + 'static>(
    cx: &Cx,
    guard: DropGuard,
    inner: Items<T>,
    reading: Option<Reading>,
) -> Items<T> {
    let token = cx.cancel_token();
    let deadline_at = cx.deadline().map(|deadline| deadline.instant());
    Box::pin(Bounded {
        inner: Some(inner),
        guard: Some(guard),
        cancelled: token.clone().cancelled_owned(),
        token,
        deadline: deadline_at.map(tokio::time::sleep_until),
        deadline_at,
        since_check: 0,
        ready_run: 0,
        reading,
    })
}

/// How many ready items may pass between checks of the cancel flag and the
/// clock. Both reads cost more than an item should (`CancellationToken::
/// is_cancelled` takes a mutex; a clock read is a syscall-class call), and a
/// producer that never returns `Pending` never lets the cancel or deadline
/// futures be polled (an unpolled `Sleep` never even registers). So the
/// ready path checks every this many items: a ready producer can overrun a
/// cancel or a deadline by at most this many items, and a pending producer
/// is woken by the futures at once.
pub(crate) const CHECK_INTERVAL: u8 = 64;

pin_project! {
    struct Bounded<T> {
        // `None` once the stream has ended; dropping the inner stream drops
        // the attempt's node streams.
        inner: Option<Items<T>>,
        // `None` once the stream has ended; dropping it cancels the attempt.
        guard: Option<DropGuard>,
        #[pin]
        cancelled: WaitForCancellationFutureOwned,
        token: CancellationToken,
        #[pin]
        deadline: Option<Sleep>,
        deadline_at: Option<Instant>,
        since_check: u8,
        // Consecutive ready items; the boundary yields every
        // `YIELD_INTERVAL` so a consumer draining an always-ready attempt
        // cannot starve the task that would cancel it.
        ready_run: u16,
        // The source's request, when the runtime records it.
        reading: Option<Reading>,
    }
}

impl<T> Stream for Bounded<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        if *this.ready_run >= YIELD_INTERVAL {
            *this.ready_run = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        *this.since_check += 1;
        if *this.since_check >= CHECK_INTERVAL {
            *this.since_check = 0;
            if let Some(error) = interrupted(this.token, *this.deadline_at) {
                ended(this.reading, Some(&error));
                end(this.inner, this.guard);
                return Poll::Ready(Some(Err(error)));
            }
        }

        match inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                *this.ready_run += 1;
                return Poll::Ready(Some(Ok(item)));
            }
            Poll::Ready(Some(Err(error))) => {
                ended(this.reading, Some(&error));
                end(this.inner, this.guard);
                return Poll::Ready(Some(Err(error)));
            }
            Poll::Ready(None) => {
                // A producer that honors its cancel or deadline often ends
                // cleanly; the end must still be reported as the interruption
                // it was, not as success. One check per stream, off the
                // per-item path.
                let interruption = interrupted(this.token, *this.deadline_at);
                ended(this.reading, interruption.as_ref());
                end(this.inner, this.guard);
                return Poll::Ready(interruption.map(Err));
            }
            Poll::Pending => *this.ready_run = 0,
        }

        // The deadline is polled first: when both have happened it is the
        // cause worth reporting, as everywhere else (see `Cx::check`).
        if let Some(deadline) = this.deadline.as_pin_mut()
            && deadline.poll(cx).is_ready()
        {
            let error = Error::deadline_exceeded();
            ended(this.reading, Some(&error));
            end(this.inner, this.guard);
            return Poll::Ready(Some(Err(error)));
        }
        if this.cancelled.poll(cx).is_ready() {
            // The deadline timer fires on a whole-millisecond tick, and an
            // exhausted cooperative budget can hold it back; a deadline that
            // has passed is the cause to report even if its timer has not
            // fired yet.
            let error = interrupted(this.token, *this.deadline_at).unwrap_or_else(Error::cancelled);
            ended(this.reading, Some(&error));
            end(this.inner, this.guard);
            return Poll::Ready(Some(Err(error)));
        }
        Poll::Pending
    }
}

fn interrupted(token: &CancellationToken, deadline_at: Option<Instant>) -> Option<Error> {
    if deadline_at.is_some_and(|at| Instant::now() >= at) {
        Some(Error::deadline_exceeded())
    } else if token.is_cancelled() {
        Some(Error::cancelled())
    } else {
        None
    }
}

fn end<T>(inner: &mut Option<Items<T>>, guard: &mut Option<DropGuard>) {
    *inner = None;
    drop(guard.take());
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;

    use super::*;
    use crate::ErrorKind;

    fn kinds(items: Vec<Result<u32>>) -> Vec<Result<u32, ErrorKind>> {
        items.into_iter().map(|r| r.map_err(|e| e.kind())).collect()
    }

    fn attempt_of(cx: &Cx) -> (Cx, DropGuard) {
        let attempt = cx.child();
        let guard = attempt.cancel_on_drop();
        (attempt, guard)
    }

    #[tokio::test]
    async fn a_fused_stream_ends_after_the_first_error_and_cancels_its_producer() {
        let attempt = Cx::new();
        let producer = attempt.child();
        let inner = items(futures::stream::iter([Ok(1), Err(Error::node("x")), Ok(2)]));
        let mut out = fuse(inner, &producer);
        assert_eq!(out.next().await.map(|r| r.ok()), Some(Some(1)));
        assert!(!producer.is_cancelled());
        assert!(out.next().await.is_some_and(|r| r.is_err()));
        assert!(
            producer.is_cancelled(),
            "the error ended the producer's scope"
        );
        assert!(!attempt.is_cancelled());
        assert!(out.next().await.is_none());
    }

    #[tokio::test]
    async fn a_bounded_stream_ends_after_the_first_error() {
        let (attempt, guard) = attempt_of(&Cx::new());
        let inner = items(futures::stream::iter([Ok(1), Err(Error::node("x")), Ok(2)]));
        let all: Vec<_> = bound(&attempt, guard, inner).collect().await;
        assert_eq!(kinds(all), vec![Ok(1), Err(ErrorKind::Node)]);
    }

    #[tokio::test]
    async fn ending_cancels_the_attempt_while_the_stream_is_held() {
        let parent = Cx::new();
        let (attempt, guard) = attempt_of(&parent);
        let inner = items(futures::stream::iter([Err::<u32, _>(Error::node("x"))]));
        let mut out = bound(&attempt, guard, inner);
        assert!(out.next().await.is_some_and(|r| r.is_err()));
        assert!(attempt.is_cancelled());
        assert!(!parent.is_cancelled());
        drop(out);
    }

    #[tokio::test]
    async fn an_always_ready_producer_cannot_outrun_a_cancel() {
        let (attempt, guard) = attempt_of(&Cx::new());
        let mut out = bound(
            &attempt,
            guard,
            items(futures::stream::repeat_with(|| Ok(7_u32))),
        );
        assert!(out.next().await.is_some_and(|r| r.is_ok()));
        attempt.cancel();
        let rest: Vec<_> = out.take(1_000).collect().await;
        let (last, before) = rest.split_last().expect("items");
        assert_eq!(
            last.as_ref().map_err(|e| e.kind()),
            Err(ErrorKind::Cancelled)
        );
        assert!(
            before.len() < usize::from(CHECK_INTERVAL),
            "{}",
            before.len()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_always_ready_producer_cannot_outrun_the_deadline() {
        let cx = Cx::new().with_timeout(Duration::from_millis(5));
        let (attempt, guard) = attempt_of(&cx);
        let mut out = bound(
            &attempt,
            guard,
            items(futures::stream::repeat_with(|| Ok(7_u32))),
        );
        assert!(out.next().await.is_some_and(|r| r.is_ok()));
        tokio::time::advance(Duration::from_millis(10)).await;
        let rest: Vec<_> = out.take(1_000).collect().await;
        let (last, before) = rest.split_last().expect("items");
        assert_eq!(
            last.as_ref().map_err(|e| e.kind()),
            Err(ErrorKind::DeadlineExceeded)
        );
        assert!(
            before.len() < usize::from(CHECK_INTERVAL),
            "{}",
            before.len()
        );
    }

    #[tokio::test]
    async fn a_pending_producer_ends_at_once_on_cancel() {
        let (attempt, guard) = attempt_of(&Cx::new());
        let mut out = bound(
            &attempt,
            guard,
            items(futures::stream::pending::<Result<u32>>()),
        );
        let canceller = attempt.clone();
        tokio::spawn(async move { canceller.cancel() });
        let first = tokio::time::timeout(Duration::from_secs(5), out.next())
            .await
            .expect("woken by the cancel");
        assert_eq!(
            first.map(|r| r.map_err(|e| e.kind())),
            Some(Err(ErrorKind::Cancelled))
        );
    }

    /// A producer that ends cleanly as soon as it sees its cancel.
    fn ends_on_cancel(attempt: &Cx) -> (Items<u32>, tokio::sync::oneshot::Receiver<()>) {
        let (ended_tx, ended_rx) = tokio::sync::oneshot::channel();
        let watched = attempt.clone();
        let producer = futures::stream::once(async move {
            watched.cancelled().await;
            let _ = ended_tx.send(());
        })
        .filter_map(|()| async { None::<Result<u32>> });
        (items(producer), ended_rx)
    }

    #[tokio::test]
    async fn a_cancel_is_not_reported_as_a_clean_end() {
        let (attempt, guard) = attempt_of(&Cx::new());
        let (producer, ended) = ends_on_cancel(&attempt);
        let mut out = bound(&attempt, guard, producer);
        // Register the consumer, then cancel and let the producer finish
        // before the consumer polls again.
        assert!(futures::poll!(out.next()).is_pending());
        attempt.cancel();
        // The producer only finishes when polled, so drive it through the
        // boundary's inner stream by polling once more after the cancel.
        let next = out.next().await;
        let _ = ended.await;
        assert_eq!(
            next.map(|r| r.map_err(|e| e.kind())),
            Some(Err(ErrorKind::Cancelled))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_passed_deadline_is_not_reported_as_a_clean_end() {
        let cx = Cx::new().with_timeout(Duration::from_millis(5));
        let (attempt, guard) = attempt_of(&cx);
        // A producer that ends cleanly once the deadline has passed.
        let deadline = cx.deadline().expect("deadline").instant();
        let producer = futures::stream::once(async move {
            tokio::time::sleep_until(deadline).await;
        })
        .filter_map(|()| async { None::<Result<u32>> });
        let mut out = bound(&attempt, guard, items(producer));
        tokio::time::advance(Duration::from_millis(10)).await;
        assert_eq!(
            out.next().await.map(|r| r.map_err(|e| e.kind())),
            Some(Err(ErrorKind::DeadlineExceeded))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_pending_producer_reports_the_deadline_ahead_of_a_cancel() {
        let cx = Cx::new().with_timeout(Duration::from_millis(5));
        let (attempt, guard) = attempt_of(&cx);
        let mut out = bound(
            &attempt,
            guard,
            items(futures::stream::pending::<Result<u32>>()),
        );
        tokio::time::advance(Duration::from_millis(10)).await;
        attempt.cancel();
        assert_eq!(
            out.next().await.map(|r| r.map_err(|e| e.kind())),
            Some(Err(ErrorKind::DeadlineExceeded))
        );
    }

    /// Tokio's timer fires on whole-millisecond ticks: a deadline 1.5 ms
    /// out has passed at 1.6 ms, but its timer has not fired.
    #[tokio::test(start_paused = true)]
    async fn a_passed_deadline_whose_timer_has_not_fired_is_still_reported_first() {
        let cx = Cx::new().with_timeout(Duration::from_micros(1_500));
        let (attempt, guard) = attempt_of(&cx);
        let mut out = bound(
            &attempt,
            guard,
            items(futures::stream::pending::<Result<u32>>()),
        );
        assert!(futures::poll!(out.next()).is_pending());
        tokio::time::advance(Duration::from_micros(1_600)).await;
        attempt.cancel();
        assert_eq!(
            out.next().await.map(|r| r.map_err(|e| e.kind())),
            Some(Err(ErrorKind::DeadlineExceeded))
        );
    }

    #[tokio::test]
    async fn dropping_the_stream_cancels_the_attempt() {
        let parent = Cx::new();
        let (attempt, guard) = attempt_of(&parent);
        let out = bound(
            &attempt,
            guard,
            items(futures::stream::pending::<Result<u32>>()),
        );
        drop(out);
        assert!(attempt.is_cancelled());
        assert!(!parent.is_cancelled());
    }
}
