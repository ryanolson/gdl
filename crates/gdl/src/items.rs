use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::cx::{CancelOnDrop, Cancelled, Scope};
use futures::Stream;
use pin_project_lite::pin_project;
use tokio::time::{Instant, Sleep};

use crate::cx::Cx;
use crate::error::{Error, Result};
use crate::meter::{Ending, Owner, ended};

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
pub fn failed<T: Send + 'static>(error: Error) -> Items<T> {
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
pub fn until_error<T: Send + 'static>(inner: Items<T>) -> Items<T> {
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
        // As in `Fuse`: returned as it came, with no call while it is held.
        // Dropping the input is a call, so an ending takes the poll first.
        match polled {
            Poll::Ready(Some(Ok(_))) => self.ready_run += 1,
            Poll::Pending => self.ready_run = 0,
            Poll::Ready(Some(Err(_)) | None) => {
                let error = error_of(polled);
                self.inner = None;
                return Poll::Ready(error.map(Err));
            }
        }
        polled
    }
}

/// A node boundary: the stream ends after its first `Err`, so the next node
/// never sees an item a terminal error should have stopped, and any ending
/// (or dropping the stream) cancels the producing node's own scope through
/// `guard`. Cancelling at the producer, not the attempt, matters: the next
/// node may still be holding the error, and must be able to deliver it.
/// Cancel and deadline are enforced once per attempt by `bound`, not here.
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
pub fn fuse<T: Send + 'static>(inner: Items<T>, scope: &Cx) -> Items<T> {
    Producer::of(scope).fuse(inner)
}

/// What a node boundary keeps of its producer's scope. Taken before the
/// scope moves into the producer, it spares a clone of the whole context,
/// which copies the metadata, extensions, and visit handles too.
pub(crate) struct Producer {
    token: Scope,
    deadline_at: Option<Instant>,
}

impl Producer {
    pub(crate) fn of(scope: &Cx) -> Self {
        Self {
            token: scope.cancel_token(),
            deadline_at: scope.deadline().map(|deadline| deadline.instant()),
        }
    }

    /// The node boundary [`fuse`] describes, over the producer's stream.
    pub(crate) fn fuse<T: Send + 'static>(self, inner: Items<T>) -> Items<T> {
        Box::pin(self.fused(inner))
    }

    /// The same node boundary, not boxed: an attempt boundary with nothing
    /// between it and the producer holds it as its inner stream.
    pub(crate) fn fused<T>(self, inner: Items<T>) -> Fuse<T> {
        Fuse {
            inner: Some(inner),
            token: Some(self.token),
            deadline_at: self.deadline_at,
            ready_run: 0,
        }
    }
}

/// Consecutive ready items a node boundary passes before it yields.
const YIELD_INTERVAL: u16 = 1024;

/// The node boundary [`fuse`] describes. Public only because a hop's
/// backward chain names it as the stream its attempt boundary takes; it is
/// not reachable from outside the crate.
pub struct Fuse<T> {
    inner: Option<Items<T>>,
    // The producer's scope, cancelled at any ending or drop; `None` once
    // the stream has ended. One token does what a guard and a clone did.
    token: Option<Scope>,
    deadline_at: Option<Instant>,
    ready_run: u16,
}

impl<T> Drop for Fuse<T> {
    fn drop(&mut self) {
        end(&mut self.inner, &mut self.token);
    }
}

impl<T> Stream for Fuse<T> {
    type Item = Result<T>;

    // hot path: local-item-boundary
    // Forced into the attempt boundary that holds it unboxed: left to the
    // compiler, it stays a call there, which costs 14 Ir per item. Behind a
    // box it is called through the vtable as before.
    #[inline(always)]
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
        // Returned as it came, with no call while it is held: the compiler
        // then writes the item once, where the consumer reads it. An ending
        // takes the poll with it (docs/src/development/runtime-design.md).
        match polled {
            Poll::Ready(Some(Ok(_))) => this.ready_run += 1,
            Poll::Pending => this.ready_run = 0,
            Poll::Ready(Some(Err(_)) | None) => {
                // A clean end is checked before ending: ending cancels the
                // scope itself.
                let error =
                    error_of(polled).or_else(|| interrupted(this.token.as_ref(), this.deadline_at));
                end(&mut this.inner, &mut this.token);
                return Poll::Ready(error.map(Err));
            }
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
/// keeps the ended stream. Dropping the stream drops `guard` too. `guard`
/// is `cx`'s own ([`Cx::cancel_on_drop`]).
///
/// The boundary checks the deadline only when the stream is polled. A
/// caller that holds the stream unread keeps its local scopes past the
/// deadline until it reads or drops the stream. Watching the deadline
/// without a read would take a task per attempt.
///
/// `inner` is the attempt's last node's stream: boxed behind that node's
/// boundary, or, when nothing comes between the producer and the attempt
/// boundary, the producer's node boundary itself ([`Fuse`]), held here
/// unboxed. The attempt boundary then polls it directly, and drops it in
/// the same place a boxed one is dropped: every end, drop and panic path of
/// the node boundary is its own code either way.
pub(crate) fn bound<T, S>(cx: &Cx, guard: CancelOnDrop, inner: S) -> Items<T>
where
    T: Send + 'static,
    S: Stream<Item = Result<T>> + Unpin + Send + 'static,
{
    bounded(cx, guard, inner, ())
}

/// `bound` for the root's attempt, ending the visit `response` owns with
/// the outcome the boundary reports, and noting its first item out.
pub fn bound_measured<T: Send + 'static>(
    cx: &Cx,
    guard: CancelOnDrop,
    inner: Items<T>,
    response: Option<Owner>,
) -> Items<T> {
    let response = Measured {
        first_pending: response.is_some(),
        owner: response,
    };
    bounded(cx, guard, inner, response)
}

fn bounded<T, S, V>(cx: &Cx, guard: CancelOnDrop, inner: S, response: V) -> Items<T>
where
    T: Send + 'static,
    S: Stream<Item = Result<T>> + Unpin + Send + 'static,
    V: Response,
{
    // The fields are built in the order written here, and a panic while
    // building them (`sleep_until` on a runtime without timers) drops those
    // already built: `inner` first, so a node boundary in it ends its
    // producer. The guard is disarmed after the last call that can panic,
    // so an unwind then drops it still armed, and it cancels the attempt.
    let deadline_at = cx.deadline().map(|deadline| deadline.instant());
    Box::pin(Bounded {
        inner: Some(inner),
        cancelled: None,
        deadline: deadline_at.map(tokio::time::sleep_until),
        // The guard's token is `cx`'s: the stream cancels it itself, and
        // needs no clone of it to read it.
        token: Some(guard.disarm()),
        deadline_at,
        ready_run: 0,
        response,
    })
}

/// How many ready items may pass between checks of the cancel flag and the
/// clock. Both reads cost more than an item should (a cancel check reads a
/// flag on every scope up to the root; a clock read is a syscall-class
/// call), and a producer that never returns `Pending` never lets the cancel
/// or deadline futures be polled (an unpolled `Sleep` never even
/// registers). So the ready path checks every this many items: a ready
/// producer can overrun a cancel or a deadline by at most this many items,
/// and a pending producer is woken by the futures at once.
pub const CHECK_INTERVAL: u8 = 64;

// The attempt boundary checks on the last item of every `CHECK_INTERVAL`
// ready items, counted by the run it counts for its yield: a second
// counter cost 4 Ir per item. A wait starts the run again but keeps its
// place in the interval (`wait`), and a yield comes after a whole number
// of intervals.
const CHECK_MASK: u16 = CHECK_INTERVAL as u16 - 1;
const _: () = assert!(CHECK_INTERVAL.is_power_of_two());
const _: () = assert!(YIELD_INTERVAL.is_multiple_of(CHECK_INTERVAL as u16));

pin_project! {
    #[project = BoundedProj]
    struct Bounded<S, V> {
        // `None` once the stream has ended; dropping the inner stream drops
        // the attempt's node streams. Boxed (`Items`), or the producer's
        // node boundary unboxed (`Fuse`): either way the same field, so it
        // drops at the same point.
        inner: Option<S>,
        // The attempt's, cancelled at any ending or drop; `None` once the
        // stream has ended.
        token: Option<Scope>,
        // Made the first time the producer waits: a stream whose items are
        // all ready never pays for the clone and the waiter.
        #[pin]
        cancelled: Option<Cancelled>,
        #[pin]
        deadline: Option<Sleep>,
        deadline_at: Option<Instant>,
        // Ready items since the last yield, and, in its low bits, since the
        // last check. The boundary yields after at most `YIELD_INTERVAL` in
        // a row, so a consumer draining an always-ready attempt cannot
        // starve the task that would cancel it, and checks the cancel and
        // the deadline every `CHECK_INTERVAL`.
        ready_run: u16,
        // The root's visit (`Measured`), or nothing to report (`()`): a
        // type, so that an attempt that reports nothing tests no flag per
        // item. Dropped with the stream, a visit ends as given up.
        response: V,
    }

    impl<S, V> PinnedDrop for Bounded<S, V> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            end(this.inner, this.token);
        }
    }
}

impl<T, S: Stream<Item = Result<T>> + Unpin, V: Response> Stream for Bounded<S, V> {
    type Item = Result<T>;

    // hot path: local-item-boundary
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        if *this.ready_run >= YIELD_INTERVAL {
            *this.ready_run = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        if *this.ready_run & CHECK_MASK == CHECK_MASK
            && let Some(error) = interrupted(this.token.as_ref(), *this.deadline_at)
        {
            return Poll::Ready(Some(Err(interrupt(self, error))));
        }

        // Read before the poll: a call while the poll is held, even one that
        // returns a constant, keeps the compiler from writing the poll where
        // the caller reads it, and every item is then copied.
        let first_pending = this.response.first_pending();
        let polled = Pin::new(inner).poll_next(cx);
        // As in `Fuse`: returned as it came, with no call while it is held.
        match polled {
            Poll::Ready(Some(Ok(_))) if !first_pending => *this.ready_run += 1,
            Poll::Ready(Some(Ok(_))) => {
                let item = item_of(polled);
                *this.ready_run += 1;
                this.response.first_out();
                return Poll::Ready(item.map(Ok));
            }
            Poll::Ready(Some(Err(_)) | None) => return finish(self, error_of(polled)),
            Poll::Pending => {
                return match wait(self, cx, polled) {
                    Some(error) => Poll::Ready(Some(Err(error))),
                    None => Poll::Pending,
                };
            }
        }
        polled
    }
}

/// The check every `CHECK_INTERVAL` items found the attempt interrupted:
/// end with `error`. Out of line, as `wait` is, and for the same reason as
/// `finish`.
#[cold]
#[inline(never)]
fn interrupt<S, V: Response>(bounded: Pin<&mut Bounded<S, V>>, error: Error) -> Error {
    let this = bounded.project();
    this.response.ended(Ending::Failed(&error));
    end(this.inner, this.token);
    error
}

/// The inner stream ended, with `error` or cleanly: end the attempt, and
/// return its last poll. Out of line: inlined, the calls of an ending that
/// runs once per stream keep registers saved and restored on every poll.
/// It returns the poll, not the error: built in the caller, a poll as
/// large as its item is built aside and copied whole.
#[cold]
#[inline(never)]
fn finish<T, S, V: Response>(
    bounded: Pin<&mut Bounded<S, V>>,
    error: Option<Error>,
) -> Poll<Option<Result<T>>> {
    let this = bounded.project();
    // A producer that honors its cancel or deadline often ends cleanly;
    // the end must still be reported as the interruption it was, not as
    // success. One check per stream, off the per-item path.
    let error = error.or_else(|| interrupted(this.token.as_ref(), *this.deadline_at));
    let ending = error.as_ref().map_or(Ending::Finished, Ending::Failed);
    this.response.ended(ending);
    end(this.inner, this.token);
    Poll::Ready(error.map(Err))
}

/// The producer waits: wait on the deadline and the cancel too, and return
/// the error to end with if either has come. It takes the pending poll, so
/// that `Bounded::poll_next` holds none across these calls, and it returns
/// no poll, so that the caller's own poll can be its result. Out of line:
/// inlined, the poll would be held across its calls again. It takes the
/// boundary whole, not its projection: a projection passed out of line is a
/// struct of pointers that every poll would build first, 14 Ir per item.
#[inline(never)]
fn wait<T, S, V: Response>(
    bounded: Pin<&mut Bounded<S, V>>,
    cx: &mut Context<'_>,
    _pending: Poll<Option<Result<T>>>,
) -> Option<Error> {
    let this = bounded.project();
    // The run of ready items starts again, but not the count to the next
    // check: the timer fires on a whole-millisecond tick, so it can still
    // be pending past the deadline, and a producer that waits every few
    // items would otherwise never reach a check. The yield then comes at
    // most `CHECK_INTERVAL` items early.
    *this.ready_run &= CHECK_MASK;
    // The deadline is polled first: when both have happened it is the
    // cause worth reporting, as everywhere else (see `Cx::check`).
    if let Some(deadline) = this.deadline.as_pin_mut()
        && deadline.poll(cx).is_ready()
    {
        let error = Error::deadline_exceeded();
        this.response.ended(Ending::Failed(&error));
        end(this.inner, this.token);
        return Some(error);
    }
    let mut cancelled = this.cancelled;
    if cancelled.is_none()
        && let Some(token) = this.token.as_ref()
    {
        cancelled.set(Some(token.clone().cancelled_owned()));
    }
    if let Some(cancelled) = cancelled.as_pin_mut()
        && cancelled.poll(cx).is_ready()
    {
        // The deadline timer fires on a whole-millisecond tick, and an
        // exhausted cooperative budget can hold it back; a deadline that
        // has passed is the cause to report even if its timer has not
        // fired yet.
        let error =
            interrupted(this.token.as_ref(), *this.deadline_at).unwrap_or_else(Error::cancelled);
        this.response.ended(Ending::Failed(&error));
        end(this.inner, this.token);
        return Some(error);
    }
    None
}

/// What an attempt boundary reports of its stream to the root's visit.
trait Response: Send + 'static {
    /// Whether the item being passed is the stream's first.
    fn first_pending(&self) -> bool;
    /// The first item has passed.
    fn first_out(&mut self);
    /// The stream ends with `ending`.
    fn ended(&self, ending: Ending<'_>);
}

/// An attempt that reports to no visit: every attempt but the root's.
impl Response for () {
    #[inline(always)]
    fn first_pending(&self) -> bool {
        false
    }

    fn first_out(&mut self) {}

    fn ended(&self, _: Ending<'_>) {}
}

/// The root's attempt, reporting to the visit `owner` holds when the
/// runtime records it.
struct Measured {
    // Whether the first item is still to come: one flag per item, no
    // counter.
    first_pending: bool,
    owner: Option<Owner>,
}

impl Response for Measured {
    fn first_pending(&self) -> bool {
        self.first_pending
    }

    fn first_out(&mut self) {
        self.first_pending = false;
        if let Some(owner) = &self.owner {
            owner.visit().first_out();
        }
    }

    fn ended(&self, ending: Ending<'_>) {
        ended(self.owner.as_ref(), ending);
    }
}

/// The error a poll ends with, taking the whole poll: a part of it left
/// behind would be dropped after the caller's next calls, and held until
/// then.
fn error_of<T>(polled: Poll<Option<Result<T>>>) -> Option<Error> {
    match polled {
        Poll::Ready(Some(Err(error))) => Some(error),
        _ => None,
    }
}

/// The item a poll holds, taking the whole poll, as `error_of` does.
fn item_of<T>(polled: Poll<Option<Result<T>>>) -> Option<T> {
    match polled {
        Poll::Ready(Some(Ok(item))) => Some(item),
        _ => None,
    }
}

fn interrupted(token: Option<&Scope>, deadline_at: Option<Instant>) -> Option<Error> {
    if deadline_at.is_some_and(|at| Instant::now() >= at) {
        Some(Error::deadline_exceeded())
    } else if token.is_some_and(Scope::is_cancelled) {
        Some(Error::cancelled())
    } else {
        None
    }
}

/// Drop the producer's stream, then cancel its scope, in the order a
/// stream that held a drop guard used. The guard is made first, so the
/// cancel happens even when the stream's destructor panics.
fn end<S>(inner: &mut Option<S>, token: &mut Option<Scope>) {
    let _cancel = token.take().map(Scope::drop_guard);
    *inner = None;
}

#[cfg(test)]
mod order;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;

    use super::*;
    use crate::ErrorKind;

    fn kinds(items: Vec<Result<u32>>) -> Vec<Result<u32, ErrorKind>> {
        items.into_iter().map(|r| r.map_err(|e| e.kind())).collect()
    }

    fn attempt_of(cx: &Cx) -> (Cx, CancelOnDrop) {
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

    /// The input goes at its error or its end, before the consumer drops
    /// the stream: a remote input releases its anchor there.
    #[tokio::test]
    async fn a_stream_until_its_error_drops_its_input_at_the_error() {
        struct Dropped(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        for last in [Some(Error::node("x")), None] {
            let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = Dropped(std::sync::Arc::clone(&dropped));
            let input =
                futures::stream::iter([Ok(1)].into_iter().chain(last.map(Err))).map(move |item| {
                    let _ = &flag;
                    item
                });
            let mut out = until_error(items(input));
            assert_eq!(out.next().await.map(|r| r.ok()), Some(Some(1)));
            assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
            let _ = out.next().await;
            assert!(
                dropped.load(std::sync::atomic::Ordering::SeqCst),
                "the input outlived its ending"
            );
            assert!(out.next().await.is_none());
        }
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

    /// A cancel made after `cancel_at` items is seen within
    /// `CHECK_INTERVAL` items, wherever a wait falls in the run: before the
    /// cancel, so that the run of ready items starts again, or after it.
    #[tokio::test]
    async fn a_cancel_is_seen_within_the_check_interval_across_a_wait() {
        let interval = usize::from(CHECK_INTERVAL);
        for cancel_at in [1, 2, 63, 64, 65, 100, 127, 128, 200] {
            for wait_at in [0, 1, 30, 62, 63, 64, 65, 90, 128, 150, 250] {
                let (attempt, guard) = attempt_of(&Cx::new());
                let mut polls = 0_usize;
                let producer = futures::stream::poll_fn(move |task| {
                    polls += 1;
                    if polls == wait_at + 1 {
                        task.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Poll::Ready(Some(Ok(7_u32)))
                });
                let mut out = bound(&attempt, guard, items(producer));
                for _ in 0..cancel_at {
                    assert!(out.next().await.is_some_and(|r| r.is_ok()));
                }
                attempt.cancel();
                let mut after = 0;
                loop {
                    match out.next().await.expect("an ending") {
                        Ok(_) => after += 1,
                        Err(error) => {
                            assert_eq!(error.kind(), ErrorKind::Cancelled);
                            break;
                        }
                    }
                    assert!(
                        after < interval,
                        "cancel after {cancel_at}, wait after {wait_at}: {after} items passed"
                    );
                }
            }
        }
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

    /// A node boundary yields after 1,024 ready items in a row, and the
    /// attempt boundary above it then waits. It registers its waiter on its
    /// first wait and keeps it, so a stream that yields five times
    /// registers as often as one that yields twice: once on each node from
    /// the attempt up. The same holds with the node boundary boxed or held
    /// unboxed.
    #[tokio::test]
    async fn an_attempt_boundary_registers_once_per_stream() {
        async fn registrations(n: u64, boxed: bool) -> usize {
            let root = Cx::new();
            let (attempt, guard) = attempt_of(&root);
            let producer = Producer::of(&attempt.child());
            let inner = items(futures::stream::iter((0..n).map(Ok)));
            crate::cx::REGISTERED.with(|count| count.set(0));
            let count = if boxed {
                bound(&attempt, guard, producer.fuse(inner)).count().await
            } else {
                bound(&attempt, guard, producer.fused(inner)).count().await
            };
            assert_eq!(count, usize::try_from(n).expect("a count"));
            crate::cx::REGISTERED.with(std::cell::Cell::get)
        }
        for boxed in [true, false] {
            assert_eq!(registrations(1_000, boxed).await, 0, "no yield, no wait");
            assert_eq!(
                registrations(3_000, boxed).await,
                2,
                "the attempt and the root"
            );
            assert_eq!(
                registrations(6_000, boxed).await,
                2,
                "a boundary registered more than once: boxed={boxed}"
            );
        }
    }
}
