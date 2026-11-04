//! The slot's own tests, which Miri runs: each future is polled where it
//! was written, at its alignment, and dropped once on every path, also
//! when it panics; and a future that borrows itself keeps that borrow
//! across a pending poll and into its destructor.

use std::cell::Cell;
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::task::Waker;

use super::*;

/// The fill of a test future's bytes, which it checks whenever it runs.
const MARK: u8 = 0x5a;

thread_local! {
    /// What this thread's test futures saw.
    static SEEN: Cell<Seen> = const { Cell::new(Seen::new()) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    /// Pending polls still to answer before the next ready one.
    pending: u32,
    polls: u32,
    drops: u32,
    /// The address of the future at its last poll.
    at: usize,
}

impl Seen {
    const fn new() -> Self {
        Self {
            pending: 0,
            polls: 0,
            drops: 0,
            at: 0,
        }
    }
}

fn seen() -> Seen {
    SEEN.with(Cell::get)
}

/// Start a test: forget what earlier futures saw, and answer `pending`
/// polls with `Pending` before the first ready one.
fn reset(pending: u32) {
    SEEN.with(|s| {
        s.set(Seen {
            pending,
            ..Seen::new()
        })
    });
}

fn note(change: impl FnOnce(&mut Seen)) {
    SEEN.with(|s| {
        let mut seen = s.get();
        change(&mut seen);
        s.set(seen);
    });
}

/// Test futures of `N` bytes at each alignment. Each checks its bytes and
/// its alignment when it is polled and dropped, records its address, and
/// answers its size.
macro_rules! probes {
    ($($name:ident = $align:literal),* $(,)?) => {$(
        #[repr(C, align($align))]
        struct $name<const N: usize>([u8; N]);

        impl<const N: usize> $name<N> {
            fn new() -> Self {
                Self([MARK; N])
            }

            fn check(&self) -> usize {
                let at = (self as *const Self).addr();
                assert_eq!(at % align_of::<Self>(), 0, "a misaligned future");
                assert!(self.0.iter().all(|&b| b == MARK), "a future's bytes changed");
                at
            }
        }

        impl<const N: usize> Future for $name<N> {
            type Output = usize;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
                let at = self.check();
                let mut pending = false;
                note(|s| {
                    s.polls += 1;
                    s.at = at;
                    if s.pending > 0 {
                        s.pending -= 1;
                        pending = true;
                    }
                });
                if pending {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(size_of::<Self>())
            }
        }

        impl<const N: usize> Drop for $name<N> {
            fn drop(&mut self) {
                self.check();
                note(|s| s.drops += 1);
            }
        }
    )*};
}

probes!(
    Align1 = 1,
    Align8 = 8,
    Align16 = 16,
    Align64 = 64,
    Align512 = 512,
);

fn task() -> Context<'static> {
    Context::from_waker(Waker::noop())
}

/// Poll the slot's future until it is ready.
fn drive<T>(mut slot: Pin<&mut Slot<'_, T>>) -> T {
    loop {
        if let Poll::Ready(out) = slot.as_mut().poll(&mut task()) {
            return out;
        }
    }
}

/// Place `future`, pending `pending` times, run it to the end, and drop
/// the slot. Returns the slot's buffer and what the future saw.
fn run<F: Future<Output = usize> + Send>(future: F, pending: u32) -> (Range<usize>, Seen) {
    reset(pending);
    let buffer = {
        let mut slot = pin!(Slot::new());
        let buffer = slot.buffer();
        slot.as_mut().place(future);
        assert_eq!(drive(slot.as_mut()), size_of::<F>());
        // Dropped when it was ready, before the slot is.
        assert_eq!(seen().drops, 1);
        buffer
    };
    (buffer, seen())
}

/// Assert that the future `make` makes is built in the buffer, and polled
/// there.
fn in_place<F: Future<Output = usize> + Send>(make: impl Fn() -> F) {
    assert!(Slot::<usize>::fits::<F>());
    for pending in [0, 2] {
        let (buffer, seen) = run(make(), pending);
        assert!(buffer.contains(&seen.at), "{seen:?} outside {buffer:?}");
        assert_eq!((seen.polls, seen.drops), (pending + 1, 1));
    }
}

/// Assert that the future `make` makes is boxed: polled outside the
/// buffer, at its alignment.
fn boxed<F: Future<Output = usize> + Send>(make: impl Fn() -> F) {
    assert!(!Slot::<usize>::fits::<F>());
    for pending in [0, 2] {
        let (buffer, seen) = run(make(), pending);
        assert!(!buffer.contains(&seen.at), "{seen:?} in {buffer:?}");
        assert_eq!((seen.polls, seen.drops), (pending + 1, 1));
    }
}

#[test]
fn a_future_of_no_size_is_built_in_place() {
    assert_eq!(size_of::<Align1<0>>(), 0);
    in_place(Align1::<0>::new);
}

#[test]
fn a_future_of_256_bytes_is_built_in_place() {
    assert_eq!(size_of::<Align1<256>>(), 256);
    in_place(Align1::<256>::new);
    assert_eq!(size_of::<Align8<256>>(), 256);
    in_place(Align8::<256>::new);
}

#[test]
fn a_future_of_257_bytes_is_boxed() {
    assert_eq!(size_of::<Align1<257>>(), 257);
    boxed(Align1::<257>::new);
}

#[test]
fn a_future_aligned_to_512_is_boxed_at_its_alignment() {
    assert_eq!(align_of::<Align512<1>>(), 512);
    boxed(Align512::<1>::new);
}

#[test]
fn a_small_future_aligned_past_the_buffer_is_boxed_at_its_alignment() {
    assert_eq!(size_of::<Align16<32>>(), 32);
    boxed(Align16::<32>::new);
    assert_eq!(size_of::<Align64<32>>(), 64);
    boxed(Align64::<32>::new);
}

#[test]
fn a_future_never_polled_is_dropped_once_with_its_slot() {
    for boxed in [false, true] {
        reset(0);
        {
            let mut slot = pin!(Slot::new());
            if boxed {
                slot.as_mut().place(Align1::<300>::new());
            } else {
                slot.as_mut().place(Align8::<64>::new());
            }
            assert_eq!(seen().drops, 0);
        }
        assert_eq!((seen().polls, seen().drops), (0, 1));
    }
}

#[test]
fn a_pending_future_is_dropped_once_with_its_slot() {
    for boxed in [false, true] {
        reset(1);
        {
            let mut slot = pin!(Slot::new());
            if boxed {
                slot.as_mut().place(Align1::<300>::new());
            } else {
                slot.as_mut().place(Align8::<64>::new());
            }
            assert!(slot.as_mut().poll(&mut task()).is_pending());
            assert_eq!(seen().drops, 0);
        }
        assert_eq!((seen().polls, seen().drops), (1, 1));
    }
}

#[test]
fn a_second_future_drops_the_first() {
    reset(1);
    let mut slot = pin!(Slot::new());
    slot.as_mut().place(Align8::<64>::new());
    assert!(slot.as_mut().poll(&mut task()).is_pending());
    slot.as_mut().place(Align1::<300>::new());
    assert_eq!(seen().drops, 1);
    assert_eq!(drive(slot.as_mut()), 300);
    assert_eq!(seen().drops, 2);
}

#[test]
fn an_empty_slot_panics_when_polled() {
    let mut slot = pin!(Slot::<usize>::new());
    let polled = catch_unwind(AssertUnwindSafe(|| slot.as_mut().poll(&mut task())));
    assert!(polled.is_err());

    reset(0);
    slot.as_mut().place(Align8::<8>::new());
    assert_eq!(drive(slot.as_mut()), 8);
    let polled = catch_unwind(AssertUnwindSafe(|| slot.as_mut().poll(&mut task())));
    assert!(polled.is_err());
    assert_eq!(seen().drops, 1);
}

/// A future that panics when polled, and counts its drops.
struct PanicsWhenPolled<const N: usize>([u64; N]);

impl<const N: usize> Future for PanicsWhenPolled<N> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        panic!("polled");
    }
}

impl<const N: usize> Drop for PanicsWhenPolled<N> {
    fn drop(&mut self) {
        note(|s| s.drops += 1);
    }
}

#[test]
fn a_future_that_panics_when_polled_is_dropped_once() {
    fn case<const N: usize>() {
        reset(0);
        {
            let mut slot = pin!(Slot::new());
            slot.as_mut().place(PanicsWhenPolled([1; N]));
            let polled = catch_unwind(AssertUnwindSafe(|| slot.as_mut().poll(&mut task())));
            assert!(polled.is_err());
            assert_eq!(seen().drops, 0);
        }
        assert_eq!(seen().drops, 1);
    }
    case::<4>();
    // Boxed.
    case::<40>();
}

/// A future that panics when dropped, after it counts the drop. It is
/// ready at once, or never.
struct PanicsWhenDropped<const N: usize> {
    ready: bool,
    _bytes: [u64; N],
}

impl<const N: usize> Future for PanicsWhenDropped<N> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        if self.ready {
            Poll::Ready(N)
        } else {
            Poll::Pending
        }
    }
}

impl<const N: usize> Drop for PanicsWhenDropped<N> {
    fn drop(&mut self) {
        note(|s| s.drops += 1);
        panic!("dropped");
    }
}

#[test]
fn a_future_that_panics_when_dropped_is_dropped_once() {
    fn case<const N: usize>() {
        // Dropped with its slot, pending. Miri's leak check shows that a
        // boxed one is freed.
        reset(0);
        let mut slot = Box::pin(Slot::new());
        slot.as_mut().place(PanicsWhenDropped {
            ready: false,
            _bytes: [1; N],
        });
        assert!(slot.as_mut().poll(&mut task()).is_pending());
        assert!(catch_unwind(AssertUnwindSafe(move || drop(slot))).is_err());
        assert_eq!(seen().drops, 1);

        // Dropped when it is ready: the poll panics, and the slot does not
        // drop it again.
        reset(0);
        let mut slot = Box::pin(Slot::new());
        slot.as_mut().place(PanicsWhenDropped {
            ready: true,
            _bytes: [1; N],
        });
        let polled = catch_unwind(AssertUnwindSafe(|| slot.as_mut().poll(&mut task())));
        assert!(polled.is_err());
        assert_eq!(seen().drops, 1);
        drop(slot);
        assert_eq!(seen().drops, 1);
    }
    case::<4>();
    // Boxed.
    case::<40>();
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

/// Place `future`, which must fit.
fn place_in<'a, T, F: Future<Output = T> + Send + 'a>(slot: Pin<&mut Slot<'a, T>>, future: F) {
    assert!(Slot::<T>::fits::<F>());
    slot.place(future);
}

/// Under Miri, a `&mut` to the buffer in `poll` would invalidate the
/// borrow the future holds of its own local, and the read after the
/// pending poll would be undefined behavior.
#[test]
fn a_future_reads_a_borrow_of_its_own_local_after_a_pending_poll() {
    let mut slot = pin!(Slot::new());
    place_in(slot.as_mut(), async {
        let local = [7u64; 4];
        let borrowed = &local;
        Yield(false).await;
        borrowed.iter().sum::<u64>()
    });
    assert!(slot.as_mut().poll(&mut task()).is_pending());
    assert_eq!(drive(slot.as_mut()), 28);
}

/// Under Miri, a `&mut` to the buffer in the slot's drop would invalidate
/// the borrow the future holds of its own local, which its destructor
/// reads.
#[test]
fn a_future_dropped_pending_reads_a_borrow_of_its_own_local() {
    /// Records what it borrows when it is dropped.
    struct ReadsWhenDropped<'a>(&'a u64);

    impl Drop for ReadsWhenDropped<'_> {
        fn drop(&mut self) {
            let read = *self.0;
            note(|s| s.at = read as usize);
        }
    }

    reset(0);
    {
        let mut slot = pin!(Slot::new());
        place_in(slot.as_mut(), async {
            let local = 5u64;
            let _reads = ReadsWhenDropped(&local);
            Yield(false).await;
            0u64
        });
        assert!(slot.as_mut().poll(&mut task()).is_pending());
    }
    assert_eq!(seen().at, 5);
}

#[test]
fn a_pending_slot_moves_to_another_thread() {
    let mut slot = Box::pin(Slot::new());
    place_in(slot.as_mut(), async {
        let local = [3u64; 2];
        let borrowed = &local;
        Yield(false).await;
        borrowed.iter().sum::<u64>()
    });
    assert!(slot.as_mut().poll(&mut task()).is_pending());
    let out = std::thread::spawn(move || drive(slot.as_mut()))
        .join()
        .expect("joined");
    assert_eq!(out, 6);
}

/// Compiles only for a type that is not `Unpin`: for an `Unpin` type both
/// impls apply, and the constant is ambiguous.
trait NotUnpin<A> {
    const HOLDS: () = ();
}
impl<T: ?Sized> NotUnpin<()> for T {}
impl<T: ?Sized + Unpin> NotUnpin<u8> for T {}

/// Compiles only for a type that is not `Sync`, as [`NotUnpin`].
trait NotSync<A> {
    const HOLDS: () = ();
}
impl<T: ?Sized> NotSync<()> for T {}
impl<T: ?Sized + Sync> NotSync<u8> for T {}

const fn sends<T: Send>() {}

// A slot is `Send`, as the `Send` futures it takes are, whatever it
// answers; it is not `Sync`, and not `Unpin`.
const _: () = sends::<Slot<'static, std::rc::Rc<u8>>>();
const _: () = <Slot<'static, u8> as NotSync<_>>::HOLDS;
const _: () = <Slot<'static, u8> as NotUnpin<_>>::HOLDS;
