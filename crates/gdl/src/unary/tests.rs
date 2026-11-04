//! A local call through the client, which Miri runs with the slot's own
//! tests: the service's future is built at the first poll and answers
//! there, a call dropped before it is dropped with its request, and the
//! call future is `Send`, not `Sync`, and not `Unpin`.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

use super::*;
use crate::Error;

fn task() -> Context<'static> {
    Context::from_waker(Waker::noop())
}

fn drive<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut task()) {
            return out;
        }
    }
}

/// Pending at its first poll, ready at the next.
struct Yield(bool);

impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if std::mem::replace(&mut self.0, true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Adds its offset; with `pending`, after one pending poll, and holding a
/// borrow of its own local across it.
struct Add {
    offset: u64,
    pending: bool,
    calls: AtomicUsize,
}

impl Add {
    fn new(offset: u64, pending: bool) -> Arc<Self> {
        Arc::new(Self {
            offset,
            pending,
            calls: AtomicUsize::new(0),
        })
    }
}

impl Unary for Add {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, req: u64) -> Result<u64> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let local = [req, self.offset];
        let borrowed = &local;
        if self.pending {
            Yield(false).await;
        }
        Ok(borrowed[0] + borrowed[1])
    }
}

#[test]
fn a_local_call_answers_at_once_or_after_a_pending_poll() {
    let cx = Cx::new();
    for pending in [false, true] {
        let service = Add::new(2, pending);
        let client = UnaryClient::local(Arc::clone(&service));
        assert_eq!(drive(client.call(&cx, 40)).expect("an answer"), 42);
        let mut call = pin!(client.call(&cx, 1));
        assert_eq!(
            service.calls.load(Ordering::SeqCst),
            1,
            "built at the first poll"
        );
        let first = call.as_mut().poll(&mut task());
        assert_eq!(first.is_pending(), pending);
        assert_eq!(service.calls.load(Ordering::SeqCst), 2);
        if pending {
            let Poll::Ready(out) = call.as_mut().poll(&mut task()) else {
                panic!("ready at the second poll");
            };
            assert_eq!(out.expect("an answer"), 3);
        }
    }
}

#[test]
fn a_clone_calls_the_same_service() {
    let cx = Cx::new();
    let service = Add::new(1, false);
    let client = UnaryClient::local(Arc::clone(&service));
    let clone = client.clone();
    drop(client);
    assert_eq!(drive(clone.call(&cx, 1)).expect("an answer"), 2);
    assert_eq!(service.calls.load(Ordering::SeqCst), 1);
    assert_eq!(size_of::<UnaryClient<u64, u64>>(), 2 * size_of::<usize>());
}

/// Answers its request's length; its future is larger than the buffer.
struct Large;

impl Unary for Large {
    type Req = Vec<u8>;
    type Resp = usize;

    async fn call(&self, _cx: &Cx, req: Vec<u8>) -> Result<usize> {
        let local = [1u8; 512];
        let borrowed = &local;
        Yield(false).await;
        Ok(req.len() + usize::from(borrowed[511]))
    }
}

/// Answers with the service's error.
struct Fails;

impl Unary for Fails {
    type Req = Vec<u8>;
    type Resp = usize;

    async fn call(&self, _cx: &Cx, _req: Vec<u8>) -> Result<usize> {
        Err(Error::node("refused"))
    }
}

#[test]
fn every_call_future_has_one_size() {
    let cx = Cx::new();
    let large = UnaryClient::local(Arc::new(Large));
    let fails = UnaryClient::local(Arc::new(Fails));
    let (a, b) = (large.call(&cx, vec![0; 3]), fails.call(&cx, vec![]));
    assert_eq!(size_of_val(&a), size_of_val(&b));
    assert_eq!(drive(a).expect("an answer"), 4);
    let error = drive(b).expect_err("the service's error");
    assert!(error.to_string().contains("refused"), "{error}");
}

/// Counts its drops.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Panics before it makes a future, when asked to.
struct Builds;

impl Unary for Builds {
    type Req = (bool, Counted);
    type Resp = ();

    fn call(&self, _cx: &Cx, req: (bool, Counted)) -> impl Future<Output = Result<()>> + Send {
        assert!(!req.0, "asked to panic");
        async move {
            drop(req);
            Ok(())
        }
    }
}

#[test]
fn a_call_dropped_before_its_first_poll_drops_its_request_once() {
    let cx = Cx::new();
    let client = UnaryClient::local(Arc::new(Builds));
    let dropped = Arc::new(AtomicUsize::new(0));
    drop(client.call(&cx, (false, Counted(Arc::clone(&dropped)))));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn a_call_whose_service_panics_before_its_future_panics_when_polled_again() {
    let cx = Cx::new();
    let client = UnaryClient::local(Arc::new(Builds));
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut call = pin!(client.call(&cx, (true, Counted(Arc::clone(&dropped)))));
    let polled = catch_unwind(AssertUnwindSafe(|| call.as_mut().poll(&mut task())));
    assert!(polled.is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let again = catch_unwind(AssertUnwindSafe(|| call.as_mut().poll(&mut task())));
    assert!(again.is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn a_completed_call_panics_when_polled_again() {
    let cx = Cx::new();
    let client = UnaryClient::local(Add::new(0, false));
    let mut call = pin!(client.call(&cx, 1));
    assert!(call.as_mut().poll(&mut task()).is_ready());
    let again = catch_unwind(AssertUnwindSafe(|| call.as_mut().poll(&mut task())));
    assert!(again.is_err());
}

/// Records, when dropped, the value it borrows.
struct ReadsWhenDropped<'a>(&'a u64, &'a AtomicUsize);

impl Drop for ReadsWhenDropped<'_> {
    fn drop(&mut self) {
        self.1.store(*self.0 as usize, Ordering::SeqCst);
    }
}

/// Holds a borrow of its own local across a pending poll, which its
/// destructor reads.
struct Holds(AtomicUsize);

impl Unary for Holds {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, req: u64) -> Result<u64> {
        let local = req;
        let _reads = ReadsWhenDropped(&local, &self.0);
        Yield(false).await;
        Yield(false).await;
        Ok(local)
    }
}

/// Under Miri, the regressions of the slot's tests through the client: a
/// service future that borrows its own local reads it after a pending
/// poll, and its destructor reads it when the call is dropped pending.
#[test]
fn a_service_future_keeps_a_borrow_of_its_own_local() {
    let cx = Cx::new();
    let service = Arc::new(Holds(AtomicUsize::new(0)));
    let client = UnaryClient::local(Arc::clone(&service));
    assert_eq!(drive(client.call(&cx, 7)).expect("an answer"), 7);
    assert_eq!(service.0.load(Ordering::SeqCst), 7);
    {
        let mut call = pin!(client.call(&cx, 9));
        assert!(call.as_mut().poll(&mut task()).is_pending());
        assert!(call.as_mut().poll(&mut task()).is_pending());
    }
    assert_eq!(service.0.load(Ordering::SeqCst), 9);
}

/// Compiles only for a type that is not `Unpin`: for an `Unpin` type both
/// impls apply, and the call is ambiguous.
trait NotUnpin<A> {
    fn holds(&self) {}
}
impl<T: ?Sized> NotUnpin<()> for T {}
impl<T: ?Sized + Unpin> NotUnpin<u8> for T {}

/// Compiles only for a type that is not `Sync`, as [`NotUnpin`].
trait NotSync<A> {
    fn holds(&self) {}
}
impl<T: ?Sized> NotSync<()> for T {}
impl<T: ?Sized + Sync> NotSync<u8> for T {}

fn sends<T: Send>(_: &T) {}

#[test]
fn a_call_is_send_and_neither_sync_nor_unpin() {
    let cx = Cx::new();
    let client = UnaryClient::local(Add::new(0, false));
    let call = client.call(&cx, 1);
    sends(&call);
    NotSync::holds(&call);
    NotUnpin::holds(&call);
}
