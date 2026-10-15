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

/// Wrap a node's output at a runtime boundary.
///
/// The wrapper makes the stream contract hold no matter what the node does:
/// it ends after the first `Err`, it ends with `Cancelled` when `cx` is
/// cancelled, and it ends with `DeadlineExceeded` when `cx`'s deadline
/// passes. Ending drops the inner stream, which cancels whatever produced it.
/// `guard`, when given, cancels the attempt that owns the stream when the
/// consumer drops it.
pub(crate) fn bound<T: Send + 'static>(
    cx: &Cx,
    guard: Option<DropGuard>,
    inner: Items<T>,
) -> Items<T> {
    let token = cx.cancel_token();
    let deadline_at = cx.deadline().map(|deadline| deadline.instant());
    Box::pin(Bounded {
        inner: Some(inner),
        cancelled: token.clone().cancelled_owned(),
        token,
        deadline: deadline_at.map(tokio::time::sleep_until),
        deadline_at,
        since_clock_check: 0,
        _guard: guard,
    })
}

/// How many ready items may pass between clock reads. A producer that never
/// returns `Pending` never lets the deadline timer be polled (an unpolled
/// `Sleep` never registers, so it never reports elapsed), so the clock is
/// read directly; reading it on every item would put a clock read on the
/// hottest path. A ready producer can overrun its deadline by at most this
/// many items.
const CLOCK_CHECK_INTERVAL: u8 = 64;

pin_project! {
    struct Bounded<T> {
        // `None` once the stream has ended; dropping the inner stream is what
        // cancels its producer.
        inner: Option<Items<T>>,
        #[pin]
        cancelled: WaitForCancellationFutureOwned,
        token: CancellationToken,
        #[pin]
        deadline: Option<Sleep>,
        deadline_at: Option<Instant>,
        since_clock_check: u8,
        _guard: Option<DropGuard>,
    }
}

impl<T> Stream for Bounded<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };

        // Items first: a ready producer is the hot path, and the cancel and
        // deadline futures only need polling (to register a wakeup) when it
        // is not ready. The token flag is a cheap read and the clock is read
        // every `CLOCK_CHECK_INTERVAL` items, so a producer that never returns
        // `Pending` still cannot outrun them.
        if this.token.is_cancelled() {
            *this.inner = None;
            return Poll::Ready(Some(Err(Error::cancelled())));
        }
        if let Some(at) = *this.deadline_at {
            *this.since_clock_check += 1;
            if *this.since_clock_check >= CLOCK_CHECK_INTERVAL {
                *this.since_clock_check = 0;
                if Instant::now() >= at {
                    *this.inner = None;
                    return Poll::Ready(Some(Err(Error::deadline_exceeded())));
                }
            }
        }
        match inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(error))) => {
                *this.inner = None;
                return Poll::Ready(Some(Err(error)));
            }
            Poll::Ready(None) => {
                *this.inner = None;
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        if this.cancelled.poll(cx).is_ready() {
            *this.inner = None;
            return Poll::Ready(Some(Err(Error::cancelled())));
        }
        if let Some(deadline) = this.deadline.as_pin_mut()
            && deadline.poll(cx).is_ready()
        {
            *this.inner = None;
            return Poll::Ready(Some(Err(Error::deadline_exceeded())));
        }
        Poll::Pending
    }
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

    #[tokio::test]
    async fn the_stream_ends_after_the_first_error() {
        let cx = Cx::new();
        let inner = items(futures::stream::iter([Ok(1), Err(Error::node("x")), Ok(2)]));
        let all: Vec<_> = bound(&cx, None, inner).collect().await;
        assert_eq!(kinds(all), vec![Ok(1), Err(ErrorKind::Node)]);
    }

    #[tokio::test]
    async fn an_always_ready_producer_cannot_outrun_a_cancel() {
        let cx = Cx::new();
        let mut out = bound(&cx, None, items(futures::stream::repeat_with(|| Ok(7_u32))));
        assert_eq!(out.next().await.map(|r| r.ok()), Some(Some(7)));
        cx.cancel();
        let rest: Vec<_> = out.collect().await;
        assert_eq!(kinds(rest), vec![Err(ErrorKind::Cancelled)]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_always_ready_producer_cannot_outrun_the_deadline() {
        let cx = Cx::new().with_timeout(Duration::from_millis(5));
        let mut out = bound(&cx, None, items(futures::stream::repeat_with(|| Ok(7_u32))));
        assert!(out.next().await.is_some_and(|r| r.is_ok()));
        tokio::time::advance(Duration::from_millis(10)).await;
        let rest: Vec<_> = out.take(1_000).collect().await;
        let (last, before) = rest.split_last().expect("items");
        assert_eq!(
            last.as_ref().map_err(|e| e.kind()),
            Err(ErrorKind::DeadlineExceeded)
        );
        assert!(
            before.len() < usize::from(CLOCK_CHECK_INTERVAL),
            "{}",
            before.len()
        );
    }

    #[tokio::test]
    async fn dropping_the_stream_fires_its_guard() {
        let cx = Cx::new();
        let attempt = cx.child();
        let out = bound(
            &attempt,
            Some(attempt.cancel_on_drop()),
            items(futures::stream::pending::<Result<u32>>()),
        );
        drop(out);
        assert!(attempt.is_cancelled());
        assert!(!cx.is_cancelled());
    }
}
