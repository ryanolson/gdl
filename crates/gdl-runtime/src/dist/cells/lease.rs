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

use gdl::__runtime::ChoiceHeld;

use super::meter::CellFigures;

/// The bit of a follow's word that says it is not offered; the rest count
/// the leases.
const UNOFFERED: usize = 1 << (usize::BITS - 1);

/// One follow's word, shared by every table its follow makes, with the
/// counts of its cell's offers, if the chooser records them: the same for
/// every follow of the cell.
#[derive(Debug, Default)]
pub(crate) struct Leases(AtomicUsize, Option<&'static CellFigures>);

impl Leases {
    /// The word of a follow whose cell's offers count into `figures`.
    pub(crate) fn counted(figures: Option<&'static CellFigures>) -> Self {
        Self(AtomicUsize::new(0), figures)
    }

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
/// lease is. It also holds the cell policy's lease on the choice, if the
/// policy gave one, which lives as long.
pub(crate) struct CellLease {
    follow: Arc<Leases>,
    choice: Option<ChoiceHeld>,
}

impl CellLease {
    /// A lease on the follow `leases` counts for, unless it is not
    /// offered: then the offer must not be made, and nothing is held.
    pub(crate) fn take(leases: &Arc<Leases>) -> Option<Self> {
        let before = leases.0.fetch_add(1, Ordering::AcqRel);
        if before & UNOFFERED != 0 {
            leases.0.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Self {
            follow: Arc::clone(leases),
            choice: None,
        })
    }

    /// The lease, holding the cell policy's lease on the choice it was
    /// taken for.
    #[inline]
    pub(crate) fn with_choice(mut self, choice: Option<ChoiceHeld>) -> Self {
        self.choice = choice;
        self
    }

    /// Whether this lease is on the follow `table` shows: the leases of a
    /// follow are shared by every table of it, and a newer generation has
    /// its own.
    pub(crate) fn is_of(&self, table: &super::CellTable) -> bool {
        Arc::ptr_eq(&self.follow, &table.leases)
    }

    /// The counts of the offers to the cell, if the chooser records them.
    #[inline]
    pub(crate) fn figures(&self) -> Option<&'static CellFigures> {
        self.follow.1
    }

    /// The cell accepted the offer: tell the cell policy's lease, if it
    /// gave one.
    #[inline]
    pub(crate) fn accepted(&mut self) {
        if let Some(choice) = &mut self.choice {
            choice.accepted();
        }
    }
}

impl Drop for CellLease {
    fn drop(&mut self) {
        self.follow.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl std::fmt::Debug for CellLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellLease")
            .field("follow", &self.follow)
            .field("choice", &self.choice.is_some())
            .finish()
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
