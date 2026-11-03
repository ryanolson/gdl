//! The scope node under std threads. No tokio, so Miri runs these too.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc;
use std::sync::{Barrier, Mutex as StdMutex};
use std::task::Wake;
use std::time::Duration;

use super::*;

/// Rounds of each race; Miri runs a few.
const ROUNDS: usize = if cfg!(miri) { 10 } else { 2_000 };

/// Long enough that a missing wake fails the test, not the runner.
const BOUND: Duration = Duration::from_secs(5);

/// Counts its wakes.
#[derive(Default)]
struct Counted(AtomicUsize);

impl Counted {
    fn waker() -> (std::sync::Arc<Self>, Waker) {
        let counted = std::sync::Arc::new(Self::default());
        let waker = Waker::from(std::sync::Arc::clone(&counted));
        (counted, waker)
    }

    fn wakes(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Wake for Counted {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Sends on a channel when woken.
struct Signal(StdMutex<mpsc::Sender<()>>);

impl Signal {
    fn waker() -> (Waker, mpsc::Receiver<()>) {
        let (send, receive) = mpsc::channel();
        (
            Waker::from(std::sync::Arc::new(Self(StdMutex::new(send)))),
            receive,
        )
    }
}

impl Wake for Signal {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        let _ = self.0.lock().expect("the sender").send(());
    }
}

/// Panics on its first wake.
#[derive(Default)]
struct PanicsOnWake(AtomicBool);

impl Wake for PanicsOnWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        if !self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
            panic!("a waker that panics on wake");
        }
    }
}

/// Runs `then` when its last reference is dropped: a waker drop callback.
struct OnDrop(StdMutex<Option<Box<dyn FnOnce() + Send>>>);

impl OnDrop {
    fn waker(then: impl FnOnce() + Send + 'static) -> Waker {
        Waker::from(std::sync::Arc::new(Self(StdMutex::new(Some(Box::new(
            then,
        ))))))
    }
}

impl Wake for OnDrop {
    fn wake(self: std::sync::Arc<Self>) {}
}

impl Drop for OnDrop {
    fn drop(&mut self) {
        if let Some(then) = self.0.get_mut().expect("the callback").take() {
            then();
        }
    }
}

/// A chain of `depth` scopes below a root; `[0]` is the root.
fn chain(depth: usize) -> Vec<Scope> {
    let mut scopes = vec![Scope::root()];
    for _ in 0..depth {
        let below = scopes.last().expect("a scope").child();
        scopes.push(below);
    }
    scopes
}

fn listed(scopes: &[Scope]) -> Vec<usize> {
    scopes.iter().map(|scope| scope.0.listed()).collect()
}

/// A waiter at the leaf of a chain of depth 3 races a cancel of
/// `scopes[target]` on another thread. Whichever runs first, the waiter is
/// ready, at once or after a wake.
fn race_cancel(target: usize) {
    for _ in 0..ROUNDS {
        let scopes = chain(3);
        let leaf = scopes[3].clone();
        let cancelled = scopes[target].clone();
        let start = std::sync::Arc::new(Barrier::new(2));
        let canceller = std::thread::spawn({
            let start = std::sync::Arc::clone(&start);
            move || {
                start.wait();
                cancelled.cancel();
            }
        });
        let (waker, woken) = Signal::waker();
        let mut task = Context::from_waker(&waker);
        let mut waiter = Box::pin(leaf.wait(CANCEL));
        start.wait();
        if waiter.as_mut().poll(&mut task).is_pending() {
            woken
                .recv_timeout(BOUND)
                .expect("the cancel woke the waiter");
            assert!(waiter.as_mut().poll(&mut task).is_ready());
        }
        canceller.join().expect("the canceller");
        drop(waiter);
        assert_eq!(listed(&scopes), [0, 0, 0, 0]);
    }
}

#[test]
fn a_waiter_registering_while_an_ancestor_cancels_is_woken() {
    race_cancel(0);
    race_cancel(1);
}

#[test]
fn a_cancel_of_the_waiters_own_node_during_registration_is_not_lost() {
    race_cancel(3);
}

#[test]
fn a_waiter_dropped_during_a_cancel_leaves_no_entry() {
    for _ in 0..ROUNDS {
        let scopes = chain(2);
        let (_, waker) = Counted::waker();
        let mut waiter = Box::pin(scopes[2].wait(CANCEL));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let start = std::sync::Arc::new(Barrier::new(2));
        let canceller = std::thread::spawn({
            let start = std::sync::Arc::clone(&start);
            let middle = scopes[1].clone();
            move || {
                start.wait();
                middle.cancel();
            }
        });
        start.wait();
        drop(waiter);
        canceller.join().expect("the canceller");
        assert_eq!(listed(&scopes), [0, 0, 0]);
    }
}

#[test]
fn no_wakeup_is_lost_among_many_waiters() {
    let rounds = if cfg!(miri) { 2 } else { 200 };
    for _ in 0..rounds {
        let scopes = chain(3);
        let registered = std::sync::Arc::new(Barrier::new(9));
        let waiters: Vec<_> = (0..8)
            .map(|i| {
                let scope = scopes[i % 4].clone();
                let registered = std::sync::Arc::clone(&registered);
                std::thread::spawn(move || {
                    let (waker, woken) = Signal::waker();
                    let mut task = Context::from_waker(&waker);
                    let mut waiter = pin!(scope.wait(CANCEL));
                    assert!(waiter.as_mut().poll(&mut task).is_pending());
                    registered.wait();
                    woken
                        .recv_timeout(BOUND)
                        .expect("the cancel woke the waiter");
                    assert!(waiter.as_mut().poll(&mut task).is_ready());
                })
            })
            .collect();
        registered.wait();
        scopes[0].cancel();
        for waiter in waiters {
            waiter.join().expect("a waiter");
        }
        assert_eq!(listed(&scopes), [0, 0, 0, 0]);
    }
}

/// A cancel wakes the waiters below it and no others: an attempt's end
/// must not wake its siblings' waiters, as one root `Notify` would.
#[test]
fn a_cancel_wakes_nothing_outside_its_subtree() {
    let root = Scope::root();
    let (one, other) = (root.child(), root.child());
    let below_one = one.child();
    let (one_count, one_waker) = Counted::waker();
    let (other_count, other_waker) = Counted::waker();
    let mut at_one = pin!(below_one.wait(CANCEL));
    let mut at_other = pin!(other.wait(CANCEL));
    assert!(
        at_one
            .as_mut()
            .poll(&mut Context::from_waker(&one_waker))
            .is_pending()
    );
    assert!(
        at_other
            .as_mut()
            .poll(&mut Context::from_waker(&other_waker))
            .is_pending()
    );
    one.cancel();
    assert_eq!(one_count.wakes(), 1);
    assert_eq!(other_count.wakes(), 0, "a sibling's waiter was woken");
    assert!(
        at_other
            .as_mut()
            .poll(&mut Context::from_waker(&other_waker))
            .is_pending()
    );
}

#[test]
fn a_stop_leaves_cancel_waiters_registered() {
    let scope = Scope::root();
    let (count, waker) = Counted::waker();
    let mut waiter = pin!(scope.wait(CANCEL));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    scope.request_stop();
    assert_eq!(count.wakes(), 0);
    assert_eq!(scope.0.listed(), 1);
    scope.cancel();
    assert_eq!(count.wakes(), 1);
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
}

#[test]
fn a_dropped_waiter_leaves_no_entry_on_any_level() {
    let scopes = chain(2);
    let (_, waker) = Counted::waker();
    let mut waiter = Box::pin(scopes[2].wait(STOP | CANCEL));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(listed(&scopes), [1, 1, 1]);
    drop(waiter);
    assert_eq!(listed(&scopes), [0, 0, 0]);
}

#[test]
fn a_long_chain_drops_without_recursion() {
    let length = if cfg!(miri) { 1_000 } else { 1_000_000 };
    let mut scope = Scope::root();
    for _ in 0..length {
        scope = scope.child();
    }
    std::thread::Builder::new()
        .stack_size(64 * 1024)
        .spawn(move || drop(scope))
        .expect("a thread")
        .join()
        .expect("the chain dropped on a small stack");
}

/// A waker change finds an entry a cancel took, and the poll is ready: the
/// old waker was woken, and the new one would never be.
#[test]
fn a_waker_change_racing_a_cancel_is_not_lost() {
    for _ in 0..ROUNDS {
        let scopes = chain(2);
        let (_, first) = Counted::waker();
        let mut waiter = Box::pin(scopes[2].wait(CANCEL));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&first))
                .is_pending()
        );
        let start = std::sync::Arc::new(Barrier::new(2));
        let canceller = std::thread::spawn({
            let start = std::sync::Arc::clone(&start);
            let middle = scopes[1].clone();
            move || {
                start.wait();
                middle.cancel();
            }
        });
        let (second, woken) = Signal::waker();
        let mut task = Context::from_waker(&second);
        start.wait();
        if waiter.as_mut().poll(&mut task).is_pending() {
            woken.recv_timeout(BOUND).expect("the new waker was woken");
            assert!(waiter.as_mut().poll(&mut task).is_ready());
        }
        canceller.join().expect("the canceller");
    }
}

#[test]
fn a_stop_wakes_every_batch() {
    let scope = Scope::root();
    let mut counts = Vec::new();
    let mut waiters = Vec::new();
    for _ in 0..40 {
        let (count, waker) = Counted::waker();
        let mut waiter = Box::pin(scope.wait(STOP | CANCEL));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        counts.push(count);
        waiters.push(waiter);
    }
    let (cancel_count, cancel_waker) = Counted::waker();
    let mut cancel_only = Box::pin(scope.wait(CANCEL));
    assert!(
        cancel_only
            .as_mut()
            .poll(&mut Context::from_waker(&cancel_waker))
            .is_pending()
    );
    scope.request_stop();
    assert!(counts.iter().all(|count| count.wakes() == 1));
    assert_eq!(cancel_count.wakes(), 0);
    scope.request_stop();
    assert!(counts.iter().all(|count| count.wakes() == 1));
    scope.cancel();
    assert_eq!(cancel_count.wakes(), 1);
}

#[test]
fn a_panicking_waker_does_not_strand_a_stop_batch_or_a_cancel() {
    for bit in [STOP, CANCEL] {
        let scope = Scope::root();
        let mut counts = Vec::new();
        let mut waiters = Vec::new();
        for i in 0..40 {
            let waker = if i == 2 {
                Waker::from(std::sync::Arc::new(PanicsOnWake::default()))
            } else {
                let (count, waker) = Counted::waker();
                counts.push(count);
                waker
            };
            let mut waiter = Box::pin(scope.wait(STOP | CANCEL));
            assert!(
                waiter
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            waiters.push(waiter);
        }
        let set = catch_unwind(AssertUnwindSafe(|| scope.0.set(bit)));
        assert!(set.is_err(), "the waker panicked");
        assert!(
            counts.iter().all(|count| count.wakes() == 1),
            "every other waiter was woken once, bit {bit}"
        );
        assert_eq!(scope.0.listed(), 0);
    }
}

/// A waker's drop may reach the same node again, here through another
/// entry's removal. Every waker that leaves a list is dropped after the
/// lock, so none of these deadlocks.
#[test]
fn a_waker_dropped_after_its_entry_left_may_lock_its_node_again() {
    let (done, finished) = mpsc::channel();
    std::thread::spawn(move || {
        let scope = Scope::root();
        let reenter = |scope: &Scope| {
            let scope = scope.clone();
            OnDrop::waker(move || drop(scope.0.remove(usize::MAX)))
        };
        // Replaced by an update.
        assert!(scope.0.register(1, CANCEL, reenter(&scope)));
        let (_, other) = Counted::waker();
        assert!(scope.0.update(1, other));
        // Removed by a deregistration.
        assert!(scope.0.register(2, CANCEL, reenter(&scope)));
        drop(scope.0.remove(2));
        // Rejected by a registration that finds the bit set.
        scope.cancel();
        assert!(!scope.0.register(3, CANCEL, reenter(&scope)));
        drop(scope.0.remove(1));
        done.send(()).expect("the test");
    });
    finished
        .recv_timeout(BOUND)
        .expect("no waker drop deadlocked on its node's lock");
}

/// One panicking waker drop does not stop a waiter's removal part way:
/// the entries above it go too.
#[test]
fn a_panicking_waker_drop_still_unlinks_every_level() {
    let scopes = chain(2);
    let panics = OnDrop::waker(|| panic!("a waker drop that panics"));
    assert!(scopes[2].0.register(7, CANCEL, panics));
    let (_, above) = Counted::waker();
    assert!(scopes[1].0.register(7, CANCEL, above.clone()));
    assert!(scopes[0].0.register(7, CANCEL, above));
    let mut levels = 3;
    let unlinked = catch_unwind(AssertUnwindSafe(|| unlink(&scopes[2].0, 7, &mut levels)));
    assert!(unlinked.is_err(), "the drop panicked");
    assert_eq!(listed(&scopes), [0, 0, 0]);

    // The same through an update, whose old waker is dropped after it.
    let scope = Scope::root();
    let panics = OnDrop::waker(|| panic!("a waker drop that panics"));
    assert!(scope.0.register(8, CANCEL, panics));
    let (_, other) = Counted::waker();
    assert!(catch_unwind(AssertUnwindSafe(|| scope.0.update(8, other))).is_err());
    let mut levels = 1;
    unlink(&scope.0, 8, &mut levels);
    assert_eq!(scope.0.listed(), 0);
}
