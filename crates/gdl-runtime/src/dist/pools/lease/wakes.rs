//! A request whose wait for room took a lease end's wake, then leaves
//! without offering, at its deadline or by a cancel, passes the wake to
//! the request that still waits ([`super::Wake`]). Each case has a control
//! in which the wake is never taken, and tokio's `Notified` passes it on.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Duration;

use gdl::{Cx, ErrorKind};

use super::{Leases, PoolLease};
use crate::dist::runtime::RuntimeConfig;

/// A re-queued request's wait, bounded as a request is: its deadline or
/// cancel first, then its room, then the check the re-queue makes.
async fn waiting(leases: &Arc<Leases>, request: &Cx, seen: u64) -> gdl::Result<()> {
    tokio::select! {
        biased;
        reason = request.done() => Err(reason),
        _woken = leases.room(
            seen,
            RuntimeConfig::DEFAULT_POOL_REQUEUE_BACKOFF,
            request.deadline().map(gdl::Deadline::instant),
        ) => request.check(),
    }
}

/// A waiter whose deadline passed before its timer's tick takes the wake
/// of a lease's end, then leaves: the live request is woken.
#[tokio::test(start_paused = true)]
async fn an_expired_waiter_passes_its_wake_to_a_live_request() {
    assert!(
        expired_waiter(false).await,
        "the expired first waiter consumed the only wake, leaving a live request asleep with room available"
    );
}

/// The control: the expired waiter is dropped before it takes the wake.
#[tokio::test(start_paused = true)]
async fn dropping_an_expired_waiter_passes_its_wake_to_a_live_request() {
    assert!(expired_waiter(true).await);
}

/// Two waits on one follow, the first with a deadline that passed before
/// its timer's tick; a lease ends. Whether the live request was served.
async fn expired_waiter(drop_before_poll: bool) -> bool {
    let leases = Arc::new(Leases::default());
    let held = PoolLease::take(&leases).expect("offered");
    let timeout = Duration::from_millis(10) + Duration::from_micros(1);
    let expired = Cx::new().with_timeout(timeout);
    let live = Cx::new().with_timeout(Duration::from_millis(20));
    let mut first = Box::pin(waiting(&leases, &expired, leases.ends()));
    let mut second = Box::pin(waiting(&leases, &live, leases.ends()));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 2);

    tokio::time::advance(timeout).await;
    assert_eq!(
        expired.check().expect_err("expired").kind(),
        ErrorKind::DeadlineExceeded
    );
    let mut timer = Box::pin(expired.done());
    assert!(
        timer.as_mut().poll(&mut cx).is_pending(),
        "the deadline timer has not reached its tick"
    );
    drop(held);
    assert_eq!(leases.held(), 0);
    if !drop_before_poll {
        let Poll::Ready(Err(error)) = first.as_mut().poll(&mut cx) else {
            panic!("the expired waiter must leave");
        };
        assert_eq!(error.kind(), ErrorKind::DeadlineExceeded);
    }
    drop(first);
    assert!(live.check().is_ok());
    let ready = match second.as_mut().poll(&mut cx) {
        Poll::Ready(result) => {
            result.expect("the live request can offer again");
            true
        }
        Poll::Pending => {
            tokio::time::advance(Duration::from_millis(20)).await;
            assert_eq!(
                second
                    .as_mut()
                    .await
                    .expect_err("the live request exhausts its deadline")
                    .kind(),
                ErrorKind::DeadlineExceeded
            );
            false
        }
    };
    drop(second);
    assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
    ready
}

/// The control: a cancel that the request's bound sees first.
#[tokio::test(start_paused = true)]
async fn cancelling_before_the_bound_is_polled_passes_the_wake() {
    assert!(cancelled_waiter(false).await);
}

/// A cancel that lands after the request's bound was polled, as the
/// lease ends: the waiter takes the wake, then leaves, and the live
/// request is woken.
#[tokio::test(start_paused = true)]
async fn cancelling_after_the_bound_is_polled_passes_the_wake() {
    assert!(
        cancelled_waiter(true).await,
        "the cancelled request consumed the only wake, leaving a live request asleep"
    );
}

/// Two waits on one follow; the first request is cancelled as a lease
/// ends. Whether the live request was served.
async fn cancelled_waiter(cancel_during_poll: bool) -> bool {
    use std::cell::{Cell, RefCell};

    let leases = Arc::new(Leases::default());
    let held = RefCell::new(PoolLease::take(&leases));
    let cancelled = Cx::new().with_timeout(Duration::from_secs(1));
    let live = Cx::new().with_timeout(Duration::from_millis(20));
    let interrupt = Cell::new(false);
    let seen = leases.ends();
    let mut first = Box::pin(async {
        tokio::select! {
            biased;
            reason = cancelled.done() => Err(reason),
            result = async {
                let room = leases.room(seen, RuntimeConfig::DEFAULT_POOL_REQUEUE_BACKOFF, cancelled.deadline().map(gdl::Deadline::instant));
                let mut room = std::pin::pin!(room);
                let _woken = std::future::poll_fn(|cx| {
                    // A concurrent cancel can arrive after the bound polls.
                    if interrupt.replace(false) {
                        cancelled.cancel();
                        drop(held.borrow_mut().take());
                    }
                    room.as_mut().poll(cx)
                }).await;
                cancelled.check()
            } => result,
        }
    });
    let mut second = Box::pin(waiting(&leases, &live, seen));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 2);
    if cancel_during_poll {
        interrupt.set(true);
    } else {
        cancelled.cancel();
        drop(held.borrow_mut().take());
    }
    let Poll::Ready(Err(error)) = first.as_mut().poll(&mut cx) else {
        panic!("the cancelled waiter must leave");
    };
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    drop(first);
    assert_eq!(leases.held(), 0);
    assert!(live.check().is_ok());
    let ready = match second.as_mut().poll(&mut cx) {
        Poll::Ready(result) => {
            result.expect("the live request can offer again");
            true
        }
        Poll::Pending => {
            tokio::time::advance(Duration::from_millis(20)).await;
            assert_eq!(
                second
                    .as_mut()
                    .await
                    .expect_err("the live request exhausts its deadline")
                    .kind(),
                ErrorKind::DeadlineExceeded
            );
            false
        }
    };
    drop(second);
    assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
    ready
}
