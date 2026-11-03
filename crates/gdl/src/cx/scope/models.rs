//! Loom models of the scope node. Run with
//! `RUSTFLAGS="--cfg gdl_loom" cargo test -p gdl --release --lib cx::scope::models`.
//!
//! Each model has at most two spawned threads beside the main one, a chain
//! of at most three nodes, and at most four entries on a node. The
//! preemption bound is set here, so the run does not depend on the
//! environment.

use loom::sync::atomic::AtomicUsize;
use loom::thread;

use super::*;

/// Context switches loom may force in one execution.
const PREEMPTIONS: usize = 3;

fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(PREEMPTIONS);
    builder.check(f);
}

/// A waker that counts its wakes in a loom atomic.
struct Mock(AtomicUsize);

impl std::task::Wake for Mock {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn mock() -> (std::sync::Arc<Mock>, Waker) {
    let mock = std::sync::Arc::new(Mock(AtomicUsize::new(0)));
    (std::sync::Arc::clone(&mock), Waker::from(mock))
}

fn wakes(mock: &Mock) -> usize {
    mock.0.load(Ordering::SeqCst)
}

/// root, middle, leaf.
fn chain() -> [Scope; 3] {
    let root = Scope::root();
    let middle = root.child();
    let leaf = middle.child();
    [root, middle, leaf]
}

fn empty(scopes: &[Scope]) -> bool {
    scopes.iter().all(|scope| scope.0.listed() == 0)
}

/// A waiter registers at the leaf, the middle, and the root while
/// `target` is cancelled: it is ready at once, or it was woken.
fn cancel_during_registration(target: usize) {
    model(move || {
        let scopes = chain();
        let cancelled = scopes[target].clone();
        let canceller = thread::spawn(move || cancelled.cancel());
        let (count, waker) = mock();
        let mut waiter = Box::pin(scopes[2].clone().cancelled_owned());
        let ready = waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready();
        canceller.join().expect("the canceller");
        assert!(ready || wakes(&count) >= 1, "a cancel was lost");
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
        drop(waiter);
        assert!(empty(&scopes));
    });
}

/// M1.
#[test]
fn an_ancestor_cancels_during_registration() {
    cancel_during_registration(1);
}

/// M2.
#[test]
fn the_waiters_own_node_cancels_during_registration() {
    cancel_during_registration(2);
}

/// M3: a waiter dropped while the root cancels leaves no entry.
#[test]
fn a_drop_during_a_wait() {
    model(|| {
        let scopes = chain();
        let leaf = scopes[2].clone();
        let waiting = thread::spawn(move || {
            let (_, waker) = mock();
            let mut waiter = Box::pin(leaf.cancelled_owned());
            let _ = waiter.as_mut().poll(&mut Context::from_waker(&waker));
            drop(waiter);
        });
        scopes[0].cancel();
        waiting.join().expect("the waiter");
        assert!(empty(&scopes));
    });
}

/// M4: a stop that takes one entry per lock, a cancel, and a removal,
/// all at once, on one node.
#[test]
fn a_stop_and_a_cancel_drain_one_node_together() {
    model(|| {
        let scope = Scope::root();
        let (first, first_waker) = mock();
        let (second, second_waker) = mock();
        let (cancel_only, cancel_waker) = mock();
        assert!(scope.0.register(1, STOP | CANCEL, first_waker));
        assert!(scope.0.register(2, STOP | CANCEL, second_waker));
        assert!(scope.0.register(3, CANCEL, cancel_waker));
        let stopping = scope.clone();
        let stopper = thread::spawn(move || stopping.0.stop_in::<1>());
        let cancelling = scope.clone();
        let canceller = thread::spawn(move || cancelling.cancel());
        drop(scope.0.remove(2));
        stopper.join().expect("the stopper");
        canceller.join().expect("the canceller");
        assert_eq!(wakes(&first), 1);
        assert!(wakes(&second) <= 1);
        assert_eq!(wakes(&cancel_only), 1);
        assert_eq!(scope.0.listed(), 0);
    });
}

/// M5: the last two children of one parent drop at once; the parent is
/// freed once, which loom's `Arc` checks.
#[test]
fn two_last_children_drop_at_once() {
    model(|| {
        let parent = Scope::root();
        let one = parent.child();
        let other = parent.child();
        drop(parent);
        let dropper = thread::spawn(move || drop(one));
        drop(other);
        dropper.join().expect("the dropper");
    });
}

/// M6: a waiter polled with a new waker while the middle cancels is ready,
/// or its new waker was woken.
#[test]
fn a_waker_change_against_a_cancel() {
    model(|| {
        let scopes = chain();
        let (_, old) = mock();
        let mut waiter = Box::pin(scopes[2].clone().cancelled_owned());
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&old))
                .is_pending()
        );
        let middle = scopes[1].clone();
        let canceller = thread::spawn(move || middle.cancel());
        let (count, new) = mock();
        let ready = waiter
            .as_mut()
            .poll(&mut Context::from_waker(&new))
            .is_ready();
        canceller.join().expect("the canceller");
        assert!(ready || wakes(&count) >= 1, "the new waker was never woken");
        drop(waiter);
        assert!(empty(&scopes));
    });
}

/// M7: a stop drains past its first batch while entries register.
#[test]
fn a_stop_drain_past_a_full_batch() {
    model(|| {
        let scope = Scope::root();
        let (first, first_waker) = mock();
        let (second, second_waker) = mock();
        assert!(scope.0.register(1, STOP | CANCEL, first_waker));
        assert!(scope.0.register(2, STOP | CANCEL, second_waker));
        let stopping = scope.clone();
        let stopper = thread::spawn(move || stopping.0.stop_in::<1>());
        let (third, third_waker) = mock();
        let (cancel_only, cancel_waker) = mock();
        let third_listed = scope.0.register(3, STOP | CANCEL, third_waker);
        assert!(scope.0.register(4, CANCEL, cancel_waker));
        stopper.join().expect("the stopper");
        assert_eq!(wakes(&first), 1);
        assert_eq!(wakes(&second), 1);
        assert_eq!(wakes(&third), usize::from(third_listed));
        assert_eq!(wakes(&cancel_only), 0);
        assert_eq!(scope.0.listed(), 1, "only the cancel-only entry is left");
        drop(scope.0.remove(4));
    });
}

/// M8: the node's first waiter leaves while a second registers and the
/// node cancels. The second lands in the place held in the node, or in
/// the list after it. Each entry is woken or removed exactly once, and a
/// removal finds its entry unless the cancel took it first.
#[test]
fn a_second_waiter_registers_while_the_first_leaves() {
    model(|| {
        let scope = Scope::root();
        let (first, first_waker) = mock();
        assert!(scope.0.register(1, CANCEL, first_waker));
        let leaving = scope.clone();
        let leaver = thread::spawn(move || {
            let removed = leaving.0.remove(1).is_some();
            assert!(
                removed || leaving.is_cancelled(),
                "a listed entry was not found"
            );
            removed
        });
        let registering = scope.clone();
        let (second, second_waker) = mock();
        let registerer = thread::spawn(move || registering.0.register(2, CANCEL, second_waker));
        scope.cancel();
        let removed = leaver.join().expect("the leaver");
        let listed = registerer.join().expect("the registerer");
        assert_eq!(wakes(&first), usize::from(!removed));
        assert_eq!(wakes(&second), usize::from(listed));
        assert_eq!(scope.0.listed(), 0);
    });
}
