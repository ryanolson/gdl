//! A pool choice's lease: what keeping one allocates, and that the runtime's
//! paths tell and drop it as `PoolChoice::with_lease` says, once each, even
//! when the lease panics. The counts are the test thread's own, so tests
//! that run at the same time on other threads do not change them.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use gdl::__runtime::ChoiceHeld;
use gdl::{
    ChoiceLease, Cx, Encoded, NoPool, Pool, PoolChoice, PoolPolicy, PoolRequest, PoolState,
    RoundRobin,
};
use tokio::time::Instant;

struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED: Cell<u64> = const { Cell::new(0) };
    static FREED: Cell<u64> = const { Cell::new(0) };
    static WIDE_ALLOCATED: Cell<u64> = const { Cell::new(0) };
    static WIDE_FREED: Cell<u64> = const { Cell::new(0) };
}

/// Blocks aligned to this or more are counted apart: a lease aligned to it
/// is the only thing in a test that allocates one, while a panic, which
/// allocates on its own, never does.
const WIDE: usize = 64;

fn add(to: &'static std::thread::LocalKey<Cell<u64>>) {
    // `try_with`: the allocator also runs while the thread's locals are
    // torn down.
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        let _ = to.try_with(|n| n.set(n.get() + 1));
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        add(&ALLOCATED);
        if layout.align() >= WIDE {
            add(&WIDE_ALLOCATED);
        }
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        add(&FREED);
        if layout.align() >= WIDE {
            add(&WIDE_FREED);
        }
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        add(&ALLOCATED);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The allocations and frees this thread makes while `f` runs.
fn counted<R>(f: impl FnOnce() -> R) -> (R, u64, u64) {
    for n in [&ALLOCATED, &FREED, &WIDE_ALLOCATED, &WIDE_FREED] {
        n.with(|n| n.set(0));
    }
    COUNTING.with(|c| c.set(true));
    let out = f();
    COUNTING.with(|c| c.set(false));
    (out, ALLOCATED.with(Cell::get), FREED.with(Cell::get))
}

/// The blocks aligned to [`WIDE`] that the last [`counted`] allocated and
/// freed.
fn wide() -> (u64, u64) {
    (WIDE_ALLOCATED.with(Cell::get), WIDE_FREED.with(Cell::get))
}

/// How often a lease was told and dropped.
#[derive(Default)]
struct Seen {
    accepted: AtomicUsize,
    epoch: AtomicUsize,
    dropped: AtomicUsize,
}

impl Seen {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn dropped(&self) -> usize {
        self.dropped.load(Ordering::SeqCst)
    }
}

/// A lease the size of a set router's: it overrides `accepted` only.
struct Booked {
    seen: Arc<Seen>,
    _record: [u64; 14],
}

impl Booked {
    fn new(seen: &Arc<Seen>) -> Self {
        Self {
            seen: Arc::clone(seen),
            _record: [7; 14],
        }
    }
}

impl ChoiceLease for Booked {
    fn accepted(&mut self) {
        self.seen.accepted.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for Booked {
    fn drop(&mut self) {
        self.seen.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// `round_robin`, keeping a lease with each choice, as a set router does.
struct Leasing(Arc<Seen>);

impl PoolPolicy for Leasing {
    type Head = Encoded;

    fn choose(
        &self,
        request: PoolRequest<'_, Encoded>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        RoundRobin
            .choose(request, pools)
            .map(|choice| choice.with_lease(Booked::new(&self.0)))
    }
}

/// A leased pick allocates once: the lease, with whatever the choice needs
/// to call and drop it. Telling the lease and dropping it allocate nothing,
/// and the drop frees what the pick allocated.
#[test]
fn a_leased_pick_allocates_once() {
    let seen = Seen::new();
    let policy = Leasing(Arc::clone(&seen));
    let pools = [
        Pool::new("c", "a", "r", PoolState::Ready).with_entries(1),
        Pool::new("c", "b", "r", PoolState::Ready).with_entries(1),
    ];
    let cx = Cx::new();
    for turn in 0..4 {
        let request = PoolRequest::new(&cx, &Encoded, "r", Instant::now()).with_turn(turn);
        let (choice, allocated, freed) = counted(|| policy.choose(request, &pools));
        assert_eq!((allocated, freed), (1, 0), "the pick, turn {turn}");
        let ((), allocated, freed) = counted(|| {
            let (_, lease) = choice.expect("a pool").into_parts();
            lease.expect("a lease").accepted();
        });
        assert_eq!((allocated, freed), (0, 1), "the outcome, turn {turn}");
    }
    assert_eq!((seen.accepted(), seen.dropped()), (4, 4));
    // So does a lease of no size, and one aligned past the choice's header.
    assert_eq!(kept(Empty), (1, 1));
    let seen = Seen::new();
    assert_eq!(kept(Wide::new(&seen)), (1, 1));
}

/// What keeping `lease` with a choice allocates, and what telling it and
/// dropping it frees.
fn kept(lease: impl ChoiceLease) -> (u64, u64) {
    let (choice, allocated, _) = counted(|| PoolChoice::new(0).with_lease(lease));
    let ((), _, freed) = counted(|| choice.into_parts().1.expect("a lease").accepted());
    (allocated, freed)
}

/// A lease of no size. It has nowhere to keep its counts, so it keeps them
/// in its thread, and a test reads only what it added: tests run in any
/// order, on their own threads or on one.
struct Empty;

thread_local! {
    /// How often an `Empty` on this thread was told, and dropped.
    static EMPTY: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

impl Empty {
    /// How often an `Empty` was told and dropped while `f` ran.
    fn seen<R>(f: impl FnOnce() -> R) -> (R, (u64, u64)) {
        let before = EMPTY.with(Cell::get);
        let out = f();
        let after = EMPTY.with(Cell::get);
        (out, (after.0 - before.0, after.1 - before.1))
    }
}

impl ChoiceLease for Empty {
    fn accepted(&mut self) {
        EMPTY.with(|n| n.set((n.get().0 + 1, n.get().1)));
    }
}

impl Drop for Empty {
    fn drop(&mut self) {
        EMPTY.with(|n| n.set((n.get().0, n.get().1 + 1)));
    }
}

/// A lease aligned past a pointer, which checks that it is read where it
/// was written.
#[repr(align(64))]
struct Wide {
    seen: Arc<Seen>,
    mark: u8,
}

impl Wide {
    fn new(seen: &Arc<Seen>) -> Self {
        Self {
            seen: Arc::clone(seen),
            mark: 0xa5,
        }
    }
}

impl ChoiceLease for Wide {
    fn accepted(&mut self) {
        assert_eq!((self as *mut Self).addr() % 64, 0, "aligned");
        assert_eq!(self.mark, 0xa5);
        self.seen.accepted.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for Wide {
    fn drop(&mut self) {
        assert_eq!(self.mark, 0xa5);
        self.seen.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Each way the runtime ends a choice drops its lease once, and tells it
/// only when the pool accepted.
#[test]
fn every_path_drops_the_lease_once() {
    let told = |f: &dyn Fn(PoolChoice)| {
        let seen = Seen::new();
        f(PoolChoice::new(1).with_lease(Booked::new(&seen)));
        (seen.accepted(), seen.dropped())
    };
    // The choice dropped unused, as when the policy's pick is refused.
    assert_eq!(told(&drop), (0, 1));
    // Taken apart and dropped: the pool declined, or the outcome is unknown.
    assert_eq!(told(&|choice| drop(choice.into_parts())), (0, 1));
    // Accepted, then held and dropped.
    assert_eq!(
        told(&|choice| {
            let (at, lease) = choice.into_parts();
            assert_eq!(at, 1);
            let mut lease = lease.expect("a lease");
            lease.accepted_at(3);
            drop(lease);
        }),
        (1, 1)
    );
    // Accepted by `accepted`.
    assert_eq!(
        told(&|choice| choice.into_parts().1.expect("a lease").accepted()),
        (1, 1)
    );
}

/// A lease that overrides `accepted_at` is told through it, with the epoch,
/// and its `accepted` is not called by the runtime's `accepted_at`.
#[test]
fn a_lease_hears_the_epoch_it_was_accepted_at() {
    struct AtEpoch(Arc<Seen>);

    impl ChoiceLease for AtEpoch {
        fn accepted(&mut self) {
            self.0.accepted.fetch_add(1, Ordering::SeqCst);
        }

        fn accepted_at(&mut self, epoch: u64) {
            self.0.epoch.store(epoch as usize, Ordering::SeqCst);
        }
    }

    let seen = Seen::new();
    let (_, lease) = PoolChoice::new(0)
        .with_lease(AtEpoch(Arc::clone(&seen)))
        .into_parts();
    let mut lease = lease.expect("a lease");
    lease.accepted_at(42);
    assert_eq!(seen.epoch.load(Ordering::SeqCst), 42);
    assert_eq!(seen.accepted(), 0);
    lease.accepted();
    assert_eq!(seen.accepted(), 1);
}

/// A lease whose `accepted` panics is still dropped once, and its memory
/// freed, when the runtime lets it go.
#[test]
fn a_lease_that_panics_when_told_is_dropped_once() {
    struct Panics(Arc<Seen>);

    impl ChoiceLease for Panics {
        fn accepted(&mut self) {
            panic!("told");
        }
    }

    impl Drop for Panics {
        fn drop(&mut self) {
            self.0.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let seen = Seen::new();
    let (choice, allocated, _) =
        counted(|| PoolChoice::new(0).with_lease(Panics(Arc::clone(&seen))));
    let mut lease = choice.into_parts().1.expect("a lease");
    assert!(catch_unwind(AssertUnwindSafe(|| lease.accepted_at(1))).is_err());
    assert_eq!(seen.dropped(), 0);
    let ((), _, freed) = counted(|| drop(lease));
    assert_eq!(seen.dropped(), 1);
    assert_eq!(freed, allocated);
    assert_eq!(Arc::strong_count(&seen), 1);
}

/// A lease whose `Drop` panics runs it once, and the choice's memory and the
/// lease's fields are freed all the same. The lease is wide, so that its
/// block is told apart from what the panic allocates.
#[test]
fn a_lease_that_panics_when_dropped_is_freed_once() {
    #[repr(align(64))]
    struct PanicsOnDrop(Arc<Seen>);

    impl ChoiceLease for PanicsOnDrop {
        fn accepted(&mut self) {}
    }

    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            self.0.dropped.fetch_add(1, Ordering::SeqCst);
            panic!("dropped");
        }
    }

    for taken in [false, true] {
        let seen = Seen::new();
        let lease = PanicsOnDrop(Arc::clone(&seen));
        counted(|| {
            let choice = PoolChoice::new(0).with_lease(lease);
            let unwound = catch_unwind(AssertUnwindSafe(move || {
                if taken {
                    drop(choice.into_parts());
                } else {
                    drop(choice);
                }
            }));
            assert!(unwound.is_err());
        });
        assert_eq!(seen.dropped(), 1, "taken apart: {taken}");
        assert_eq!(wide(), (1, 1), "taken apart: {taken}");
        assert_eq!(Arc::strong_count(&seen), 1, "taken apart: {taken}");
    }
}

/// A lease of no size, and one aligned past a pointer, are told at their
/// own address and dropped once, and what keeping them allocates is freed.
#[test]
fn a_lease_of_any_size_and_alignment_is_dropped_once() {
    let ((allocated, freed), seen) = Empty::seen(|| kept(Empty));
    assert_eq!(freed, allocated);
    assert_eq!(seen, (1, 1));
    let seen = Seen::new();
    let (allocated, freed) = kept(Wide::new(&seen));
    assert_eq!(freed, allocated);
    assert_eq!((seen.accepted(), seen.dropped()), (1, 1));
}

/// The tests that keep an `Empty` pass in either order, on their own
/// threads or on one, as a runner may schedule them.
#[test]
fn zero_sized_checks_are_independent_of_test_order() {
    std::thread::spawn(a_leased_pick_allocates_once)
        .join()
        .expect("allocation check");
    std::thread::spawn(a_lease_of_any_size_and_alignment_is_dropped_once)
        .join()
        .expect("alignment check");
    a_leased_pick_allocates_once();
    a_lease_of_any_size_and_alignment_is_dropped_once();
}

/// A choice is two words, with or without a lease, and a policy's return is
/// three: no larger than an `Option` of a choice.
#[test]
fn a_choice_stays_two_words() {
    const WORD: usize = size_of::<usize>();
    assert_eq!(size_of::<PoolChoice>(), 2 * WORD);
    assert_eq!(size_of::<Option<ChoiceHeld>>(), WORD);
    assert_eq!(size_of::<Result<PoolChoice, NoPool>>(), 3 * WORD);
    assert_eq!(
        size_of::<Result<PoolChoice, NoPool>>(),
        size_of::<Option<PoolChoice>>()
    );
}

/// A choice and its lease move to another thread, and are not shared
/// between threads or across an unwind: a lease is `Send` and nothing more.
const _: () = {
    const fn sent<T: Send + Unpin>() {}
    sent::<PoolChoice>();
    sent::<ChoiceHeld>();

    // Each trait below makes the call ambiguous if the type implements it,
    // so the file does not compile.
    trait Shared<A> {
        const NOT: () = ();
    }
    impl<T: ?Sized> Shared<()> for T {}
    impl<T: ?Sized + Sync> Shared<u8> for T {}
    impl<T: ?Sized + std::panic::UnwindSafe> Shared<u16> for T {}
    impl<T: ?Sized + std::panic::RefUnwindSafe> Shared<u32> for T {}
    let () = <PoolChoice as Shared<_>>::NOT;
    let () = <ChoiceHeld as Shared<_>>::NOT;
};
