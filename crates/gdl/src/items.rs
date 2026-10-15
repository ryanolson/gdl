use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use pin_project_lite::pin_project;
use tokio::time::{Instant, Sleep};
use tokio_util::sync::{CancellationToken, DropGuard, WaitForCancellationFutureOwned};

use crate::cx::Cx;
use crate::error::{Error, Result};

/// A response stream. Every response in gdl is a stream; a unary response is
/// a stream of one item. An `Err` item is terminal.
pub type Items<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send>>;

/// Box a stream as [`Items`].
pub fn items<T, S>(stream: S) -> Items<T>
where
    S: Stream<Item = Result<T>> + Send + 'static,
{
    Box::pin(stream)
}

/// A node boundary: the stream ends after its first `Err`, so the next node
/// never sees an item a terminal error should have stopped. That is all it
/// does; cancel and deadline are enforced once per attempt by [`bound`],
/// not at every node.
pub(crate) fn fuse<T: Send + 'static>(inner: Items<T>) -> Items<T> {
    Box::pin(Fuse { inner: Some(inner) })
}

struct Fuse<T> {
    inner: Option<Items<T>>,
}

impl<T> Stream for Fuse<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = inner.as_mut().poll_next(cx);
        if matches!(polled, Poll::Ready(Some(Err(_)) | None)) {
            self.inner = None;
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
    }
}

impl<T> Stream for Bounded<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };

        *this.since_check += 1;
        if *this.since_check >= CHECK_INTERVAL {
            *this.since_check = 0;
            let ended = if this.token.is_cancelled() {
                Some(Error::cancelled())
            } else if this.deadline_at.is_some_and(|at| Instant::now() >= at) {
                Some(Error::deadline_exceeded())
            } else {
                None
            };
            if let Some(error) = ended {
                end(this.inner, this.guard);
                return Poll::Ready(Some(Err(error)));
            }
        }

        match inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(error))) => {
                end(this.inner, this.guard);
                return Poll::Ready(Some(Err(error)));
            }
            Poll::Ready(None) => {
                end(this.inner, this.guard);
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        if this.cancelled.poll(cx).is_ready() {
            end(this.inner, this.guard);
            return Poll::Ready(Some(Err(Error::cancelled())));
        }
        if let Some(deadline) = this.deadline.as_pin_mut()
            && deadline.poll(cx).is_ready()
        {
            end(this.inner, this.guard);
            return Poll::Ready(Some(Err(Error::deadline_exceeded())));
        }
        Poll::Pending
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
    async fn a_fused_stream_ends_after_the_first_error() {
        let inner = items(futures::stream::iter([Ok(1), Err(Error::node("x")), Ok(2)]));
        let all: Vec<_> = fuse(inner).collect().await;
        assert_eq!(kinds(all), vec![Ok(1), Err(ErrorKind::Node)]);
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
