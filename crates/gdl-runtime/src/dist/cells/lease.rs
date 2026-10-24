//! The offers a chooser instance holds to one follow of a cell, counted so
//! that it knows when none is left: the follow closes only once every
//! chooser says so, since gdl bounds no request's lifetime and no wait of
//! fixed length would know that an offer has ended.
//!
//! One word per follow holds both the count and whether the follow is
//! offered, so that taking a lease and marking the follow not offered are
//! ordered by the word alone: a lease taken after the mark sees it and is
//! given back, and the mark sees every lease taken before it. A request's
//! path touches the word once when it makes an offer and once when the
//! offer ends, as the admission ledger does; nothing is kept per request.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The bit of a follow's word that says it is not offered; the rest count
/// the leases.
const UNOFFERED: usize = 1 << (usize::BITS - 1);

/// One follow's word, shared by every table its follow makes.
#[derive(Debug, Default)]
pub(crate) struct Leases(AtomicUsize);

impl Leases {
    /// Mark the follow not offered: no lease is taken from now on.
    pub(crate) fn unoffer(&self) {
        self.0.fetch_or(UNOFFERED, Ordering::AcqRel);
    }

    /// Mark the follow offered again.
    pub(crate) fn offer(&self) {
        self.0.fetch_and(!UNOFFERED, Ordering::AcqRel);
    }

    /// Whether the follow is not offered and no lease on it is held: no
    /// offer this instance made to it can still be under way.
    pub(crate) fn idle(&self) -> bool {
        self.0.load(Ordering::Acquire) == UNOFFERED
    }

    /// How many leases are held.
    pub(crate) fn held(&self) -> usize {
        self.0.load(Ordering::Acquire) & !UNOFFERED
    }
}

/// One offer to a follow, held for as long as the chooser holds the
/// request: until the response stream ends or is dropped, as a selector's
/// lease is.
#[derive(Debug)]
pub(crate) struct CellLease(Arc<Leases>);

impl CellLease {
    /// A lease on the follow `leases` counts for, unless it is not
    /// offered: then the offer must not be made, and nothing is held.
    pub(crate) fn take(leases: &Arc<Leases>) -> Option<Self> {
        let before = leases.0.fetch_add(1, Ordering::AcqRel);
        if before & UNOFFERED != 0 {
            leases.0.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Self(Arc::clone(leases)))
    }
}

impl Drop for CellLease {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{CellLease, Leases};

    /// A follow is idle once it is not offered and its last lease is let
    /// go; a lease asked for once it is not offered is refused and counts
    /// for nothing, and one asked for once it is offered again is taken.
    #[test]
    fn a_follow_is_idle_once_not_offered_and_its_last_lease_is_let_go() {
        let leases = Arc::new(Leases::default());
        let held = CellLease::take(&leases).expect("offered");
        assert_eq!(leases.held(), 1);
        leases.unoffer();
        assert!(!leases.idle(), "idle with a lease held");
        assert!(CellLease::take(&leases).is_none(), "taken once not offered");
        assert_eq!(leases.held(), 1);
        drop(held);
        assert!(leases.idle());
        leases.offer();
        assert!(!leases.idle(), "idle while offered");
        let again = CellLease::take(&leases).expect("offered again");
        assert_eq!(leases.held(), 1);
        drop(again);
        assert_eq!(leases.held(), 0);
    }

    /// Leases taken and let go on many threads while the follow is marked
    /// not offered: once every thread is done, the follow is idle, and no
    /// lease is taken after it.
    #[test]
    fn a_mark_and_leases_on_many_threads_end_idle() {
        let leases = Arc::new(Leases::default());
        let start = Arc::new(std::sync::Barrier::new(5));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let (leases, start) = (Arc::clone(&leases), Arc::clone(&start));
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..10_000 {
                        drop(CellLease::take(&leases));
                    }
                })
            })
            .collect();
        start.wait();
        leases.unoffer();
        for thread in threads {
            thread.join().expect("a thread");
        }
        assert!(leases.idle());
        assert!(CellLease::take(&leases).is_none());
    }
}
