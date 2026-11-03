//! A choice's lease in one allocation behind one thin pointer: a header of
//! functions built for the lease's type, then the lease. `Box<dyn
//! ChoiceLease>` is one allocation, but a fat pointer, which made every
//! pick 2 to 4 ns slower, and boxing that box again made every leased pick
//! allocate twice (`docs/src/operations/feature-costs.md`).
//!
//! `Box` allocates and frees the block, so its layout, a lease of no size,
//! and a lease aligned past the header are `Box`'s to get right. The only
//! code here that `Box` does not check is reading the block as the type it
//! was made for, through the header.

#![allow(unsafe_code)]

use std::marker::PhantomData;
use std::ptr::NonNull;

use super::ChoiceLease;

/// A lease of any type, owned: it drops the lease and frees its block once,
/// when it is dropped.
pub(super) struct Lease {
    /// The block, a `Block<T>` for the `T` its header was made for.
    block: NonNull<Header>,
    /// The auto traits of the `Box<dyn ChoiceLease>` it stands for: `Send`
    /// (below), not `Sync`, and not unwind safe.
    _owns: PhantomData<Box<dyn ChoiceLease>>,
}

// SAFETY: `Lease` owns a `T: ChoiceLease`, which is `Send`, and nothing
// else that another thread could reach. Sending it sends that `T`.
unsafe impl Send for Lease {}

/// The functions for one lease type, each called with the block it heads.
struct Header {
    accepted: unsafe fn(NonNull<Header>),
    accepted_at: unsafe fn(NonNull<Header>, u64),
    drop: unsafe fn(NonNull<Header>),
}

/// A lease's block: `repr(C)`, so that the header is first and a pointer
/// to the block is a pointer to its header.
#[repr(C)]
struct Block<T> {
    header: &'static Header,
    lease: T,
}

impl Lease {
    /// Keep `lease`, in one allocation.
    #[inline]
    pub(super) fn new<T: ChoiceLease>(lease: T) -> Self {
        let header: &'static Header = &Header {
            accepted: accept::<T>,
            accepted_at: accept_at::<T>,
            drop: release::<T>,
        };
        let block = Box::new(Block { header, lease });
        Self {
            block: NonNull::from(Box::leak(block)).cast(),
            _owns: PhantomData,
        }
    }

    /// The header of this lease's type.
    #[inline]
    fn header(&self) -> &'static Header {
        // SAFETY: `block` points to a live `Block<T>`, `repr(C)` with its
        // `&'static Header` first, so reading that field as the block's
        // first field reads an initialized reference.
        unsafe { *self.block.cast::<&'static Header>().as_ptr() }
    }

    /// Tell the lease its pool accepted the offer.
    #[inline]
    pub(super) fn accepted(&mut self) {
        // SAFETY: the header was made for the block's `T`, and `&mut self`
        // makes this the only access to the block while it runs.
        unsafe { (self.header().accepted)(self.block) }
    }

    /// Tell the lease its pool accepted the offer, by its hub of `epoch`.
    #[inline]
    pub(super) fn accepted_at(&mut self, epoch: u64) {
        // SAFETY: as in `accepted`.
        unsafe { (self.header().accepted_at)(self.block, epoch) }
    }
}

impl Drop for Lease {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: the header was made for the block's `T`, and this is the
        // last use of the block: `Lease` gives it out nowhere else, and is
        // dropped once.
        unsafe { (self.header().drop)(self.block) }
    }
}

/// # Safety
///
/// `block` is a live `Block<T>`, and nothing else reads or writes it while
/// this runs.
unsafe fn accept<T: ChoiceLease>(block: NonNull<Header>) {
    // SAFETY: the caller's contract: a live `Block<T>` with no other access,
    // so a unique reference to its lease is sound.
    let lease = unsafe { &mut (*block.cast::<Block<T>>().as_ptr()).lease };
    lease.accepted();
}

/// # Safety
///
/// As [`accept`].
unsafe fn accept_at<T: ChoiceLease>(block: NonNull<Header>, epoch: u64) {
    // SAFETY: as in `accept`.
    let lease = unsafe { &mut (*block.cast::<Block<T>>().as_ptr()).lease };
    lease.accepted_at(epoch);
}

/// # Safety
///
/// `block` came from `Box::<Block<T>>::leak` in [`Lease::new`], is live,
/// and is not used after this call.
unsafe fn release<T: ChoiceLease>(block: NonNull<Header>) {
    // SAFETY: the caller's contract: the block is `Box`'s, of the type it
    // was made as, and this takes it back once. If the lease's `Drop`
    // panics, the `Box` still frees the block as it unwinds.
    drop(unsafe { Box::from_raw(block.cast::<Block<T>>().as_ptr()) });
}

#[cfg(test)]
mod tests {
    //! The block's own tests, which Miri runs: each lease is read where it
    //! was written, dropped and freed once, also when it panics.

    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// What a lease saw: `accepted` calls, the last epoch, and drops.
    #[derive(Default)]
    struct Seen {
        accepted: AtomicUsize,
        epoch: AtomicUsize,
        dropped: AtomicUsize,
    }

    impl Seen {
        fn get(&self) -> (usize, usize, usize) {
            (
                self.accepted.load(Ordering::SeqCst),
                self.epoch.load(Ordering::SeqCst),
                self.dropped.load(Ordering::SeqCst),
            )
        }
    }

    /// A lease with a field on either side of its counts, which it checks
    /// whenever it runs.
    struct Marked<const N: usize> {
        before: [u8; N],
        seen: Arc<Seen>,
        after: u64,
    }

    impl<const N: usize> Marked<N> {
        fn new(seen: &Arc<Seen>) -> Self {
            Self {
                before: [0x5a; N],
                seen: Arc::clone(seen),
                after: 0xfeed,
            }
        }

        fn check(&self) {
            assert!(self.before.iter().all(|&b| b == 0x5a));
            assert_eq!(self.after, 0xfeed);
        }
    }

    impl<const N: usize> ChoiceLease for Marked<N> {
        fn accepted(&mut self) {
            self.check();
            self.seen.accepted.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl<const N: usize> Drop for Marked<N> {
        fn drop(&mut self) {
            self.check();
            self.seen.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A lease that hears the epoch through its own `accepted_at`.
    struct AtEpoch(Arc<Seen>);

    impl ChoiceLease for AtEpoch {
        fn accepted(&mut self) {
            self.0.accepted.fetch_add(1, Ordering::SeqCst);
        }

        fn accepted_at(&mut self, epoch: u64) {
            self.0.epoch.store(epoch as usize, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_lease_is_told_and_dropped_once() {
        let seen = Arc::new(Seen::default());
        let mut lease = Lease::new(Marked::<3>::new(&seen));
        lease.accepted();
        // `Marked` keeps the default `accepted_at`, which calls `accepted`.
        lease.accepted_at(9);
        assert_eq!(seen.get(), (2, 0, 0));
        drop(lease);
        assert_eq!(seen.get(), (2, 0, 1));
        assert_eq!(Arc::strong_count(&seen), 1);

        let seen = Arc::new(Seen::default());
        drop(Lease::new(Marked::<0>::new(&seen)));
        assert_eq!(seen.get(), (0, 0, 1));
    }

    #[test]
    fn a_lease_hears_its_own_accepted_at() {
        let seen = Arc::new(Seen::default());
        let mut lease = Lease::new(AtEpoch(Arc::clone(&seen)));
        lease.accepted_at(7);
        assert_eq!(seen.get(), (0, 7, 0));
        lease.accepted();
        assert_eq!(seen.get(), (1, 7, 0));
        drop(lease);
        assert_eq!(Arc::strong_count(&seen), 1);
    }

    #[test]
    fn a_lease_of_no_size_is_told_and_dropped() {
        thread_local! {
            static TOLD: Cell<(u32, u32)> = const { Cell::new((0, 0)) };
        }

        struct Empty;

        impl ChoiceLease for Empty {
            fn accepted(&mut self) {
                TOLD.with(|t| t.set((t.get().0 + 1, t.get().1)));
            }
        }

        impl Drop for Empty {
            fn drop(&mut self) {
                TOLD.with(|t| t.set((t.get().0, t.get().1 + 1)));
            }
        }

        let mut lease = Lease::new(Empty);
        lease.accepted();
        drop(lease);
        assert_eq!(TOLD.with(Cell::get), (1, 1));
    }

    #[test]
    fn a_lease_aligned_past_the_header_is_read_where_it_was_written() {
        #[repr(align(64))]
        struct Wide(Marked<5>);

        impl ChoiceLease for Wide {
            fn accepted(&mut self) {
                assert_eq!((self as *mut Self).addr() % 64, 0);
                self.0.accepted();
            }
        }

        #[repr(align(4096))]
        struct Paged(Marked<1>);

        impl ChoiceLease for Paged {
            fn accepted(&mut self) {
                assert_eq!((self as *mut Self).addr() % 4096, 0);
                self.0.accepted();
            }
        }

        let seen = Arc::new(Seen::default());
        let mut wide = Lease::new(Wide(Marked::new(&seen)));
        let mut paged = Lease::new(Paged(Marked::new(&seen)));
        wide.accepted();
        paged.accepted_at(1);
        drop((wide, paged));
        assert_eq!(seen.get(), (2, 0, 2));
        assert_eq!(Arc::strong_count(&seen), 1);
    }

    /// A lease that panics when told is still owned, and dropped once later.
    #[test]
    fn a_lease_that_panics_when_told_is_dropped_once() {
        struct Panics(Arc<AtomicUsize>);

        impl ChoiceLease for Panics {
            fn accepted(&mut self) {
                panic!("told");
            }
        }

        impl Drop for Panics {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let mut lease = Lease::new(Panics(Arc::clone(&dropped)));
        assert!(catch_unwind(AssertUnwindSafe(|| lease.accepted())).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| lease.accepted_at(2))).is_err());
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(lease);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&dropped), 1);
    }

    /// A lease whose `Drop` panics is dropped once, and its block and its
    /// other fields are freed as the panic unwinds. Miri's leak check is
    /// what proves the block is freed.
    #[test]
    fn a_lease_that_panics_when_dropped_is_freed() {
        struct PanicsOnDrop {
            dropped: Arc<AtomicUsize>,
            _field: Box<[u64; 4]>,
        }

        impl ChoiceLease for PanicsOnDrop {
            fn accepted(&mut self) {}
        }

        impl Drop for PanicsOnDrop {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
                panic!("dropped");
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let lease = Lease::new(PanicsOnDrop {
            dropped: Arc::clone(&dropped),
            _field: Box::new([1; 4]),
        });
        assert!(catch_unwind(AssertUnwindSafe(move || drop(lease))).is_err());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&dropped), 1);
    }

    #[test]
    fn a_lease_moves_to_another_thread() {
        let seen = Arc::new(Seen::default());
        let mut lease = Lease::new(Marked::<2>::new(&seen));
        std::thread::spawn(move || {
            lease.accepted();
            drop(lease);
        })
        .join()
        .expect("joined");
        assert_eq!(seen.get(), (1, 0, 1));
    }

    #[test]
    fn a_lease_is_one_pointer() {
        assert_eq!(size_of::<Lease>(), size_of::<usize>());
        assert_eq!(size_of::<Option<Lease>>(), size_of::<usize>());
    }
}
