//! The offers a chooser instance holds to one follow of a pool, counted so
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
//!
//! The word's follow also wakes the requests that wait for room in its
//! pool ([`Leases::room`]). It counts the leases that ended, other than
//! those whose offer the pool declined: a generation of ends. A request
//! reads the generation before each offer, and a wait returns at once
//! when it has moved since; otherwise a lease's end wakes every waiter.
//! A lease's end adds one to the generation and reads the waiter count,
//! and takes the waiters' lock only when some wait.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use gdl::__runtime::ChoiceHeld;
use tokio::sync::Notify;
use tokio::time::Instant;

use super::meter::PoolFigures;

/// The bit of a follow's word that says it is not offered; the rest count
/// the leases.
const UNOFFERED: usize = 1 << (usize::BITS - 1);

/// One follow's word, shared by every table its follow makes, with the
/// counts of its pool's offers, if the chooser records them: the same for
/// every follow of the pool. With them, the requests that wait for room
/// in the pool.
#[derive(Debug, Default)]
pub(crate) struct Leases(AtomicUsize, Option<&'static PoolFigures>, Room);

/// The requests that wait for room in a follow's pool, and the
/// generation of the follow's lease ends they wait for to move.
#[derive(Debug, Default)]
struct Room {
    /// The leases on the follow that ended, other than declined ones.
    ends: AtomicU64,
    /// The waits under way: only for a lease's end to skip
    /// `Notify::notify_waiters`, which takes the waiters' lock even when
    /// none waits (tokio 1.54, `sync/notify.rs`, `notify_waiters`).
    waiting: AtomicUsize,
    woken: Notify,
}

impl Leases {
    /// The word of a follow whose pool's offers count into `figures`.
    pub(crate) fn counted(figures: Option<&'static PoolFigures>) -> Self {
        Self(AtomicUsize::new(0), figures, Room::default())
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

    /// The generation of the follow's lease ends: read before an offer,
    /// so that a wait after it sees each end since ([`Leases::room`]).
    pub(crate) fn ends(&self) -> u64 {
        self.2.ends.load(Ordering::SeqCst)
    }

    /// Wait for room in the follow's pool, for a request that read the
    /// generation of lease ends `seen` before its last offer: return at
    /// once if a lease ended since, else when one ends, or after
    /// `backoff`, or at `deadline`, whichever comes first. Every lease end
    /// wakes every waiter, which then offers again, so no end is lost
    /// however many end at once or however waits and ends interleave. The
    /// backoff sees room that another chooser frees. Allocates nothing:
    /// the waiter is held in this future, and the timer is the one sleep.
    pub(crate) async fn room(&self, seen: u64, backoff: Duration, deadline: Option<Instant>) {
        let room = &self.2;
        // A store, then a load, on each side: the wait counts itself, then
        // reads the generation; an end adds to the generation, then reads
        // the count. Sequentially consistent on all four, at least one side
        // sees the other's write: the end notifies, or the wait sees the
        // generation move. Acquire and release would not order a store
        // before a later load. A SeqCst load costs what an acquire does on
        // aarch64 and x86.
        room.waiting.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting(&room.waiting);
        #[cfg(test)]
        tests::ENABLING.with_borrow_mut(|hook| {
            if let Some(hook) = hook.take() {
                hook();
            }
        });
        // `notify_waiters` wakes every `Notified` made before it, polled or
        // not, so one made before the generation is read misses no end
        // after the read.
        let woken = room.woken.notified();
        if room.ends.load(Ordering::SeqCst) != seen {
            return;
        }
        let after = Instant::now().checked_add(backoff);
        let until = match (after, deadline) {
            (Some(after), Some(deadline)) => Some(after.min(deadline)),
            (after, deadline) => after.or(deadline),
        };
        match until {
            Some(until) => {
                tokio::select! {
                    () = woken => {}
                    () = tokio::time::sleep_until(until) => {}
                }
            }
            None => woken.await,
        }
    }

    /// One lease ended: add to the generation of ends, and wake every
    /// waiter, if some wait.
    fn end(&self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
        let room = &self.2;
        room.ends.fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        tests::ENDING.with_borrow_mut(|hook| {
            if let Some(hook) = hook.take() {
                hook();
            }
        });
        if room.waiting.load(Ordering::SeqCst) > 0 {
            room.woken.notify_waiters();
        }
    }
}

/// One request's place among a follow's waiters, given up when its wait
/// ends however it ends.
struct Waiting<'a>(&'a AtomicUsize);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// What a re-queued request saw of its pool before its last offer: the
/// follow the offer was made under, and that follow's generation of
/// lease ends then. Made only on a miss.
pub(crate) struct Seen {
    follow: Option<Arc<Leases>>,
    ends: u64,
}

impl Seen {
    /// The follow that `lease` is on, which read `ends` before its offer.
    pub(crate) fn of(lease: Option<&PoolLease>, ends: u64) -> Self {
        Self {
            follow: lease.and_then(|lease| lease.follow.clone()),
            ends,
        }
    }

    /// Wait for room under `current`, the pool's follow now
    /// ([`Leases::room`]). A follow that replaced the one the offer was
    /// made under has a generation of its own, which the one read may
    /// match by chance, while the offers made under the old one end there:
    /// so a new follow returns at once, and the request offers again.
    pub(crate) async fn room(
        &self,
        current: &Arc<Leases>,
        backoff: Duration,
        deadline: Option<Instant>,
    ) {
        if !self
            .follow
            .as_ref()
            .is_some_and(|follow| Arc::ptr_eq(follow, current))
        {
            return;
        }
        current.room(self.ends, backoff, deadline).await;
    }
}

/// One offer to a follow, held for as long as the chooser holds the
/// request: until the response stream ends or is dropped, as a selector's
/// lease is. It also holds the pool policy's lease on the choice, if the
/// policy gave one, which lives as long. `follow` is `None` only once the
/// lease ended without waking a waiter ([`PoolLease::declined`]): an
/// `Option` of an `Arc` keeps the lease two words.
pub(crate) struct PoolLease {
    follow: Option<Arc<Leases>>,
    choice: Option<ChoiceHeld>,
}

impl PoolLease {
    /// A lease on the follow `leases` counts for, unless it is not
    /// offered: then the offer must not be made, and nothing is held.
    pub(crate) fn take(leases: &Arc<Leases>) -> Option<Self> {
        let before = leases.0.fetch_add(1, Ordering::AcqRel);
        if before & UNOFFERED != 0 {
            leases.0.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Self {
            follow: Some(Arc::clone(leases)),
            choice: None,
        })
    }

    /// The lease, holding the pool policy's lease on the choice it was
    /// taken for.
    #[inline]
    pub(crate) fn with_choice(mut self, choice: Option<ChoiceHeld>) -> Self {
        self.choice = choice;
        self
    }

    /// Whether this lease is on the follow `table` shows: the leases of a
    /// follow are shared by every table of it, and a newer generation has
    /// its own.
    pub(crate) fn is_of(&self, table: &super::PoolTable) -> bool {
        self.follow
            .as_ref()
            .is_some_and(|follow| Arc::ptr_eq(follow, &table.leases))
    }

    /// The generation of lease ends on this lease's follow
    /// ([`Leases::ends`]): read just before the offer it is held for.
    #[inline]
    pub(crate) fn ends(&self) -> u64 {
        self.follow.as_ref().map_or(0, |follow| follow.ends())
    }

    /// The counts of the offers to the pool, if the chooser records them.
    #[inline]
    pub(crate) fn figures(&self) -> Option<&'static PoolFigures> {
        self.follow.as_ref().and_then(|follow| follow.1)
    }

    /// End the lease of an offer the pool did not take, with no change to
    /// the generation of ends and no wake: a declined offer frees no room,
    /// and waking the waiters for it would have them offer a full pool in
    /// turn, each decline waking them all again. Gives back the pool
    /// policy's lease on the choice.
    pub(crate) fn declined(mut self) -> Option<ChoiceHeld> {
        if let Some(follow) = self.follow.take() {
            follow.0.fetch_sub(1, Ordering::AcqRel);
        }
        self.choice.take()
    }

    /// The pool accepted the offer: tell the pool policy's lease, if it
    /// gave one.
    #[inline]
    pub(crate) fn accepted(&mut self) {
        if let Some(choice) = &mut self.choice {
            choice.accepted();
        }
    }
}

impl Drop for PoolLease {
    fn drop(&mut self) {
        if let Some(follow) = &self.follow {
            follow.end();
        }
    }
}

impl std::fmt::Debug for PoolLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolLease")
            .field("follow", &self.follow)
            .field("choice", &self.choice.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures::task::{ArcWake, waker};

    use super::{Leases, PoolLease};
    use crate::dist::counting::{counted, counting};

    thread_local! {
        /// Run once inside a wait's registration, after it counts itself
        /// and before it registers its waiter: where a lease that another
        /// thread ends lands between the two.
        pub(super) static ENABLING: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
        /// Run once inside a lease's end, after it moves the generation and
        /// before it reads the waiter count: where a wait that another
        /// thread starts lands between the two.
        pub(super) static ENDING: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    /// The control of the next test: two waits that registered before
    /// either of two leases ended are both served.
    #[tokio::test(start_paused = true)]
    async fn an_end_paused_after_both_waits_keeps_both_releases() {
        assert_eq!(staggered_releases(false).await, 2);
    }

    /// A lease's end pauses after it moved the generation and before it
    /// read the waiter count; a second wait starts meanwhile, and a second
    /// lease ends. Both waits are served with no time passed: the second
    /// sees the generation move, and the end that resumes reads the count.
    #[tokio::test(start_paused = true)]
    async fn an_end_paused_before_the_second_wait_keeps_both_releases() {
        assert_eq!(
            staggered_releases(true).await,
            2,
            "two lease ends leave one of two waits asleep until its deadline"
        );
    }

    /// Two waits, a deadline of 1 s and a backoff of an hour, and two
    /// leases that end, the first paused inside its end; the second wait
    /// starts during that pause if `joins_during_end`. How many waits were
    /// served with no time passed. A wait that was not served must end at
    /// its deadline.
    async fn staggered_releases(joins_during_end: bool) -> usize {
        use std::sync::mpsc;

        let leases = Arc::new(Leases::default());
        let first = PoolLease::take(&leases).expect("offered");
        let second = PoolLease::take(&leases).expect("offered");
        let request = gdl::Cx::new().with_timeout(Duration::from_secs(1));
        let deadline = request.deadline().expect("a deadline").instant();
        let mut waits = [
            Box::pin(leases.room(leases.ends(), Duration::from_secs(3600), Some(deadline))),
            Box::pin(leases.room(leases.ends(), Duration::from_secs(3600), Some(deadline))),
        ];
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut ready = [false; 2];
        assert!(waits[0].as_mut().poll(&mut cx).is_pending());
        if !joins_during_end {
            assert!(waits[1].as_mut().poll(&mut cx).is_pending());
        }

        let (read, reads) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            ENDING.with_borrow_mut(|hook| {
                *hook = Some(Box::new(move || {
                    read.send(()).expect("the observer");
                    resumed
                        .recv_timeout(Duration::from_secs(5))
                        .expect("the second release completes");
                }));
            });
            drop(first);
        });
        reads
            .recv_timeout(Duration::from_secs(5))
            .expect("the first end moved the generation");
        assert_eq!(leases.ends(), 1);
        if joins_during_end {
            // It read the generation before the end, as before its offer,
            // so it sees the end at once.
            ready[1] = waits[1].as_mut().poll(&mut cx).is_ready();
        }
        drop(second);
        assert_eq!(leases.ends(), 2, "both leases ended");
        resume.send(()).expect("the first end resumes");
        worker.join().expect("the first end completes");
        assert_eq!(leases.held(), 0, "both offers ended");

        for (wait, ready) in waits.iter_mut().zip(&mut ready) {
            if !*ready {
                *ready = wait.as_mut().poll(&mut cx).is_ready();
            }
        }
        let served = ready.into_iter().filter(|ready| *ready).count();
        assert!(request.check().is_ok(), "no time passed");
        if served != 2 {
            tokio::time::advance(Duration::from_secs(1)).await;
            for (wait, ready) in waits.iter_mut().zip(ready) {
                if !ready {
                    wait.await;
                }
            }
            assert_eq!(
                request.check().expect_err("the deadline passed").kind(),
                gdl::ErrorKind::DeadlineExceeded,
            );
        }
        drop(waits);
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
        served
    }

    /// A lease that ends while a wait registers ends that wait: the wait
    /// reads the generation after it registers, and sees the end. The
    /// control: a lease that ends after the registration wakes it.
    #[tokio::test]
    async fn a_lease_ending_during_wait_registration_is_not_lost() {
        let leases = Arc::new(Leases::default());
        let hour = Duration::from_secs(3600);
        let mut cx = Context::from_waker(std::task::Waker::noop());

        // A release after registration completes wakes the same wait.
        let held = PoolLease::take(&leases).expect("offered");
        let mut control = Box::pin(leases.room(leases.ends(), hour, None));
        assert!(control.as_mut().poll(&mut cx).is_pending());
        drop(held);
        assert!(control.as_mut().poll(&mut cx).is_ready());
        drop(control);
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);

        let held = PoolLease::take(&leases).expect("offered");
        ENABLING.with_borrow_mut(|hook| *hook = Some(Box::new(move || drop(held))));
        let mut waiting = Box::pin(leases.room(leases.ends(), hour, None));
        let polled = waiting.as_mut().poll(&mut cx);
        assert_eq!(leases.held(), 0, "the pool's last offer ended");
        assert!(
            polled.is_ready(),
            "the lease ended after Notify registration, but the request waits for the backoff"
        );
    }

    /// The control: two leases that end once two waits registered wake
    /// both.
    #[tokio::test(start_paused = true)]
    async fn registered_waits_keep_both_lease_releases() {
        let leases = Arc::new(Leases::default());
        let held = [
            PoolLease::take(&leases).expect("offered"),
            PoolLease::take(&leases).expect("offered"),
        ];
        let mut control = [
            Box::pin(leases.room(leases.ends(), Duration::from_secs(3600), None)),
            Box::pin(leases.room(leases.ends(), Duration::from_secs(3600), None)),
        ];
        let mut cx = Context::from_waker(std::task::Waker::noop());
        for wait in &mut control {
            assert!(wait.as_mut().poll(&mut cx).is_pending());
        }
        drop(held);
        for wait in &mut control {
            assert!(wait.as_mut().poll(&mut cx).is_ready());
        }
        drop(control);
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
    }

    /// Two leases that end while two waits are counted but not yet
    /// registered end both waits, which read the generation after they
    /// register: `notify_one` alone kept one permit for both.
    #[test]
    fn concurrent_registrations_keep_both_lease_releases() {
        assert_eq!(
            early_releases(2),
            2,
            "two rooms freed for two counted waits"
        );
    }

    /// Four waits, and four leases that end before any wait registers: all
    /// four are served at once, none on its backoff or deadline.
    #[test]
    fn every_lease_that_ends_before_the_waits_register_serves_one() {
        assert_eq!(
            early_releases(4),
            4,
            "four rooms freed for four counted waits"
        );
    }

    /// Start `n` waits, each on its own thread and paused clock, with a
    /// deadline of 1 s and a backoff of an hour; hold each between its
    /// count and its registration while `n` leases end; then let them
    /// register. How many were served at their first poll, with no time
    /// passed. A wait that was not served must end at its deadline.
    fn early_releases(n: usize) -> usize {
        use std::sync::mpsc;

        let leases = Arc::new(Leases::default());
        let held: Vec<PoolLease> = (0..n)
            .map(|_| PoolLease::take(&leases).expect("offered"))
            .collect();
        let (counted, counts) = mpsc::channel();
        let mut resumes = Vec::new();
        let mut workers = Vec::new();
        for _ in 0..n {
            let leases = Arc::clone(&leases);
            let counted = counted.clone();
            let (resume, resumed) = mpsc::channel();
            resumes.push(resume);
            workers.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .start_paused(true)
                    .build()
                    .expect("a runtime");
                ENABLING.with_borrow_mut(|hook| {
                    *hook = Some(Box::new(move || {
                        counted.send(()).expect("the observer");
                        resumed
                            .recv_timeout(Duration::from_secs(5))
                            .expect("the releases precede registration");
                    }));
                });
                runtime.block_on(async {
                    let cx = gdl::Cx::new().with_timeout(Duration::from_secs(1));
                    let deadline = cx.deadline().expect("a deadline").instant();
                    let mut waiting = Box::pin(leases.room(
                        leases.ends(),
                        Duration::from_secs(3600),
                        Some(deadline),
                    ));
                    let ready = waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(std::task::Waker::noop()))
                        .is_ready();
                    if ready {
                        assert!(cx.check().is_ok());
                    } else {
                        tokio::time::advance(Duration::from_secs(1)).await;
                        waiting.await;
                        assert_eq!(
                            cx.check()
                                .expect_err("the wait exhausted its deadline")
                                .kind(),
                            gdl::ErrorKind::DeadlineExceeded,
                        );
                    }
                    ready
                })
            }));
        }
        for _ in 0..n {
            counts
                .recv_timeout(Duration::from_secs(5))
                .expect("every wait counted itself");
        }
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), n);
        drop(held);
        assert_eq!(leases.held(), 0);
        for resume in resumes {
            resume.send(()).expect("a registering waiter");
        }
        let ready = workers
            .into_iter()
            .map(|worker| usize::from(worker.join().expect("a waiter")))
            .sum::<usize>();
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
        ready
    }

    /// A wait that ends by a drop or its deadline uncounts itself, a wake
    /// that reached a dropped wait passes to the next, and no wait holds a
    /// follow open.
    #[tokio::test(start_paused = true)]
    async fn dropped_and_expired_waits_release_their_counts_and_wakes() {
        let leases = Arc::new(Leases::default());
        let hour = Duration::from_secs(3600);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let held = PoolLease::take(&leases).expect("offered");
        let mut first = Box::pin(leases.room(leases.ends(), hour, None));
        let mut second = Box::pin(leases.room(leases.ends(), hour, None));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 2);
        drop(held);
        drop(first);
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 1);
        assert!(
            second.as_mut().poll(&mut cx).is_ready(),
            "a cancelled waiter kept the notification"
        );
        drop(second);
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
        leases.room(leases.ends(), hour, Some(deadline)).await;
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
        let mut cancelled = Box::pin(leases.room(leases.ends(), hour, None));
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        drop(cancelled);
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
        leases.unoffer();
        assert!(leases.idle(), "a waiter held the drain open");
    }

    /// A waker that notes that it was woken.
    #[derive(Default)]
    struct Noted(AtomicBool);

    impl ArcWake for Noted {
        fn wake_by_ref(noted: &Arc<Self>) {
            noted.0.store(true, Ordering::SeqCst);
        }
    }

    impl Noted {
        /// Whether it was woken since it was last asked.
        fn woken(&self) -> bool {
            self.0.swap(false, Ordering::SeqCst)
        }
    }

    /// A lease that ends wakes every request that waits for room in its
    /// pool, and moves the generation of ends; a lease whose offer was
    /// declined wakes none and leaves the generation as it was. A wait
    /// allocates nothing. Each wait's backoff is an hour, so only a lease's
    /// end can wake it, and the waits are polled by hand, so that nothing
    /// here depends on time.
    #[tokio::test]
    async fn a_lease_that_ends_wakes_every_waiting_request() {
        let leases = Arc::new(Leases::default());
        let held: Vec<PoolLease> = (0..2)
            .map(|_| PoolLease::take(&leases).expect("offered"))
            .collect();
        let hour = Duration::from_secs(3600);
        let (first, second) = (Arc::new(Noted::default()), Arc::new(Noted::default()));
        let seen = leases.ends();
        let mut waits = [
            Box::pin(leases.room(seen, hour, None)),
            Box::pin(leases.room(seen, hour, None)),
        ];
        let (one, two) = (waker(Arc::clone(&first)), waker(Arc::clone(&second)));
        let mut contexts = [Context::from_waker(&one), Context::from_waker(&two)];
        let before = counted();
        counting(true);
        for (wait, cx) in waits.iter_mut().zip(&mut contexts) {
            assert!(wait.as_mut().poll(cx).is_pending());
        }
        counting(false);
        assert_eq!(counted() - before, 0, "a wait allocated");

        let mut held = held.into_iter();
        let declined = held.next().expect("a lease");
        assert!(declined.declined().is_none());
        assert!(!first.woken() && !second.woken(), "a declined lease woke");
        assert_eq!(leases.ends(), seen, "a declined lease moved the generation");
        assert_eq!(leases.held(), 1);

        drop(held.next());
        assert_eq!(leases.ends(), seen + 1);
        assert!(first.woken() && second.woken(), "not every waiter woke");
        for (wait, cx) in waits.iter_mut().zip(&mut contexts) {
            assert_eq!(wait.as_mut().poll(cx), Poll::Ready(()));
        }
        assert_eq!(leases.held(), 0);
        assert_eq!(
            leases.2.waiting.load(Ordering::SeqCst),
            0,
            "a wait that ended is still counted"
        );
    }

    /// A wait whose request saw an older generation than the follow's now
    /// returns at once: a lease ended after the request read it, before
    /// its offer, so there may be room. A wait for another follow than the
    /// one the offer was made under returns at once, though its generation
    /// matches by chance, since the offers it would wait for end on the
    /// old follow.
    #[tokio::test]
    async fn a_wait_returns_at_once_for_an_end_since_its_offer_or_a_new_follow() {
        let hour = Duration::from_secs(3600);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let old = Arc::new(Leases::default());
        let lease = PoolLease::take(&old).expect("offered");
        let seen = super::Seen::of(Some(&lease), old.ends());
        drop(PoolLease::take(&old).expect("offered"));
        let mut moved = Box::pin(seen.room(&old, hour, None));
        assert!(
            moved.as_mut().poll(&mut cx).is_ready(),
            "an end since the offer"
        );

        let seen = super::Seen::of(Some(&lease), old.ends());
        let mut same = Box::pin(seen.room(&old, hour, None));
        assert!(same.as_mut().poll(&mut cx).is_pending(), "the control");
        let new = Arc::new(Leases::default());
        assert_eq!(new.ends(), 0);
        let replaced = super::Seen::of(Some(&lease), 0);
        let mut follow = Box::pin(replaced.room(&new, hour, None));
        assert!(follow.as_mut().poll(&mut cx).is_ready(), "a new follow");
    }

    /// `N` requests wait for room in a full pool, and the pool's `N` leases
    /// end one at a time, each freeing one room. A request offers again
    /// each time its wait returns, reading the generation before each
    /// offer, and an offer takes a free room if one is left. Every request
    /// is served, with no time passed: each backoff is an hour, so none
    /// was served by its backoff. Each end wakes every waiter, and each
    /// waiter offers once per wake, so the end that finds `k` requests
    /// waiting causes `k` offers, of which one takes the room: with each
    /// request's first offer, `N + N(N + 1) / 2` offers in all, 14 for 4.
    #[tokio::test(start_paused = true)]
    async fn every_waiting_request_is_served_with_bounded_offers() {
        const N: usize = 4;
        let leases = Arc::new(Leases::default());
        let (rooms, offers) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let held: Vec<PoolLease> = (0..N)
            .map(|_| PoolLease::take(&leases).expect("offered"))
            .collect();
        let start = tokio::time::Instant::now();
        let requests: Vec<_> = (0..N)
            .map(|_| {
                let (leases, rooms, offers) =
                    (Arc::clone(&leases), Arc::clone(&rooms), Arc::clone(&offers));
                tokio::spawn(async move {
                    loop {
                        let seen = leases.ends();
                        offers.fetch_add(1, Ordering::SeqCst);
                        let took = rooms
                            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |r| r.checked_sub(1))
                            .is_ok();
                        if took {
                            return;
                        }
                        leases.room(seen, Duration::from_secs(3600), None).await;
                    }
                })
            })
            .collect();
        let settle = || async {
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
        };
        settle().await;
        assert_eq!(
            leases.2.waiting.load(Ordering::SeqCst),
            N,
            "every request waits"
        );
        for lease in held {
            rooms.fetch_add(1, Ordering::SeqCst);
            drop(lease);
            settle().await;
        }
        for request in requests {
            request.await.expect("a request");
        }
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "a request waited for its backoff"
        );
        let offers = offers.load(Ordering::SeqCst);
        println!("{N} waiting requests, {N} lease ends: {offers} offers");
        assert!(offers <= N + N * (N + 1) / 2, "{offers} offers");
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
    }

    /// Take and end `GDL_LEASES` leases on one follow, with no request
    /// waiting: what every offer into another class pays for its lease,
    /// for an instruction count under callgrind at two counts
    /// (`docs/src/operations/feature-costs.md`). Ignored: it measures, and
    /// asserts nothing.
    #[test]
    #[ignore = "a measurement, not a check"]
    fn cost_of_a_lease() {
        let count: usize = std::env::var("GDL_LEASES").map_or(1, |n| n.parse().expect("a count"));
        let leases = Arc::new(Leases::default());
        for _ in 0..count {
            std::hint::black_box(PoolLease::take(std::hint::black_box(&leases)));
        }
    }

    /// A wait ends at its backoff, or at the request's deadline when that
    /// comes first, with no lease ending; with no backoff left it ends at
    /// once.
    #[tokio::test(start_paused = true)]
    async fn a_wait_ends_at_its_backoff_or_the_deadline() {
        let leases = Leases::default();
        let start = tokio::time::Instant::now();
        leases
            .room(leases.ends(), Duration::from_millis(50), None)
            .await;
        assert_eq!(start.elapsed(), Duration::from_millis(50));
        let start = tokio::time::Instant::now();
        let deadline = start + Duration::from_millis(20);
        leases
            .room(leases.ends(), Duration::from_secs(60), Some(deadline))
            .await;
        assert_eq!(start.elapsed(), Duration::from_millis(20));
    }

    /// A follow is idle once it is not offered and its last lease is let
    /// go; a lease asked for once it is not offered is refused and counts
    /// for nothing, and one asked for once it is offered again is taken.
    #[test]
    fn a_follow_is_idle_once_not_offered_and_its_last_lease_is_let_go() {
        let leases = Arc::new(Leases::default());
        let held = PoolLease::take(&leases).expect("offered");
        assert_eq!(leases.held(), 1);
        leases.unoffer();
        assert!(!leases.idle(), "idle with a lease held");
        assert!(PoolLease::take(&leases).is_none(), "taken once not offered");
        assert_eq!(leases.held(), 1);
        drop(held);
        assert!(leases.idle());
        leases.offer();
        assert!(!leases.idle(), "idle while offered");
        let again = PoolLease::take(&leases).expect("offered again");
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
                        drop(PoolLease::take(&leases));
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
        assert!(PoolLease::take(&leases).is_none());
    }
}
