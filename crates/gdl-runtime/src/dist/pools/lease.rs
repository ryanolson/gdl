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
//! pool ([`Leases::room`]): each lease that ends, other than one whose
//! offer the pool declined, wakes the request whose wait began first. A
//! request waits anew each round, at the back. Only a pool policy's
//! re-queue waits, so a lease's end reads one more word, and touches the
//! waiters only when there are some.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// The requests that wait for room in a follow's pool: woken one at a
/// time, the longest waiting first, as the follow's leases end. The count
/// of waiters spares a lease's end the waiters' lock while none waits.
/// Each lease's end leaves one wake in `wakes`, which one waiter takes:
/// `Notify` keeps at most one wake for waiters that have not registered,
/// so two leases that ended before two waiters registered would wake one.
#[derive(Debug, Default)]
struct Room {
    waiting: AtomicUsize,
    /// The wakes that ended leases left and no waiter took yet: never more
    /// than the waiters a lease's end saw.
    wakes: AtomicUsize,
    woken: Notify,
}

impl Room {
    /// Take one wake that a lease's end left, if one is left.
    fn take(&self) -> bool {
        self.wakes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |wakes| {
                wakes.checked_sub(1)
            })
            .is_ok()
    }
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

    /// Wait for room in the follow's pool: until this wait takes a wake
    /// that a lease's end left, or for `backoff`, or until `deadline`,
    /// whichever comes first. Each lease that ends once the wait has
    /// counted itself, at its first poll, leaves a wake for one waiter, so
    /// no lease's end is lost, however many end at once; the registered
    /// waiter whose wait began first is woken first. The backoff sees room
    /// that another chooser frees, and a lease that ended before the wait
    /// began. Allocates nothing: the waiter is held in this future, and
    /// the timer is the one sleep.
    pub(crate) async fn room(&self, backoff: Duration, deadline: Option<Instant>) {
        let room = &self.2;
        // Counted before the waiter registers: a lease that ends after the
        // count reads it and leaves a wake, which the wait takes below,
        // once it is registered, or is woken to take.
        room.waiting.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting(&room.waiting);
        #[cfg(test)]
        tests::ENABLING.with_borrow_mut(|hook| {
            if let Some(hook) = hook.take() {
                hook();
            }
        });
        let after = Instant::now().checked_add(backoff);
        let until = match (after, deadline) {
            (Some(after), Some(deadline)) => Some(after.min(deadline)),
            (after, deadline) => after.or(deadline),
        };
        loop {
            let woken = room.woken.notified();
            let mut woken = std::pin::pin!(woken);
            // Registered before the wakes are read, so that a wake left
            // after the read also notifies this waiter.
            woken.as_mut().enable();
            if room.take() {
                return;
            }
            match until {
                Some(until) => {
                    tokio::select! {
                        () = woken => {}
                        () = tokio::time::sleep_until(until) => return,
                    }
                }
                None => woken.await,
            }
            // Woken: take the wake, unless another waiter took it first,
            // and then wait again.
        }
    }

    /// One lease ended: leave a wake for a waiter, if one waits, and wake
    /// the registered waiter whose wait began first. Wakes are counted up
    /// to the waiters the end sees, so that wakes that no waiter took,
    /// as when a waiter's wait ended on its backoff first, cannot pile up:
    /// a later wait takes at most as many as there were waiters.
    fn end(&self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
        let room = &self.2;
        let waiting = room.waiting.load(Ordering::SeqCst);
        if waiting > 0
            && room
                .wakes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |wakes| {
                    (wakes < waiting).then_some(wakes + 1)
                })
                .is_ok()
        {
            room.woken.notify_one();
        }
    }
}

/// One request's place among a follow's waiters, given up when its wait
/// ends however it ends. A notification that reaches it after the wait
/// ended passes to the next waiter, as tokio's `Notified` does when
/// dropped, and a wake it did not take stays for the next waiter.
struct Waiting<'a>(&'a AtomicUsize);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
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

    /// The counts of the offers to the pool, if the chooser records them.
    #[inline]
    pub(crate) fn figures(&self) -> Option<&'static PoolFigures> {
        self.follow.as_ref().and_then(|follow| follow.1)
    }

    /// End the lease of an offer the pool did not take, and wake no
    /// waiter: a declined offer frees no room, and waking a waiter for it
    /// would have the waiters offer a full pool in turn, each decline
    /// waking the next. Gives back the pool policy's lease on the choice.
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
    use std::sync::atomic::{AtomicBool, Ordering};
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
    }

    /// A lease that ends while a wait registers wakes that wait: the wait
    /// counts itself before it registers its waiter, so the lease's end
    /// sees the count and leaves a permit that the registration takes. The
    /// control: a lease that ends after the registration wakes it too.
    #[tokio::test]
    async fn a_lease_ending_during_wait_registration_is_not_lost() {
        let leases = Arc::new(Leases::default());
        let hour = Duration::from_secs(3600);
        let mut cx = Context::from_waker(std::task::Waker::noop());

        // A release after registration completes wakes the same wait.
        let held = PoolLease::take(&leases).expect("offered");
        let mut control = Box::pin(leases.room(hour, None));
        assert!(control.as_mut().poll(&mut cx).is_pending());
        drop(held);
        assert!(control.as_mut().poll(&mut cx).is_ready());
        drop(control);
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);

        let held = PoolLease::take(&leases).expect("offered");
        ENABLING.with_borrow_mut(|hook| *hook = Some(Box::new(move || drop(held))));
        let mut waiting = Box::pin(leases.room(hour, None));
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
            Box::pin(leases.room(Duration::from_secs(3600), None)),
            Box::pin(leases.room(Duration::from_secs(3600), None)),
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
    /// registered wake both waits: each end leaves a wake that one wait
    /// takes, where `Notify` alone keeps one permit for both.
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
                    let mut waiting =
                        Box::pin(leases.room(Duration::from_secs(3600), Some(deadline)));
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
        assert_eq!(leases.2.wakes.load(Ordering::SeqCst), 0, "a wake was left");
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
        let mut first = Box::pin(leases.room(hour, None));
        let mut second = Box::pin(leases.room(hour, None));
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
        leases.room(hour, Some(deadline)).await;
        assert_eq!(leases.2.waiting.load(Ordering::SeqCst), 0);
        let mut cancelled = Box::pin(leases.room(hour, None));
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

    /// Requests that wait for room in a follow's pool are woken by the
    /// leases that end, one per lease, the longest waiting first; a lease
    /// whose offer was declined wakes none. A wait allocates nothing. Each
    /// wait's backoff is an hour, so only a lease's end can wake it, and
    /// the waits are polled by hand, so that nothing here depends on time.
    #[tokio::test]
    async fn a_lease_that_ends_wakes_the_longest_waiting_request() {
        let leases = Arc::new(Leases::default());
        let held: Vec<PoolLease> = (0..3)
            .map(|_| PoolLease::take(&leases).expect("offered"))
            .collect();
        let hour = Duration::from_secs(3600);
        let (first, second) = (Arc::new(Noted::default()), Arc::new(Noted::default()));
        let mut waits = [
            Box::pin(leases.room(hour, None)),
            Box::pin(leases.room(hour, None)),
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
        assert_eq!(leases.held(), 2);

        drop(held.next());
        assert!(first.woken() && !second.woken(), "not the longest waiting");
        assert_eq!(waits[0].as_mut().poll(&mut contexts[0]), Poll::Ready(()));
        drop(held.next());
        assert!(second.woken());
        assert_eq!(waits[1].as_mut().poll(&mut contexts[1]), Poll::Ready(()));
        assert_eq!(leases.held(), 0);
        assert_eq!(
            leases.2.waiting.load(Ordering::SeqCst),
            0,
            "a wait that ended is still counted"
        );
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
        leases.room(Duration::from_millis(50), None).await;
        assert_eq!(start.elapsed(), Duration::from_millis(50));
        let start = tokio::time::Instant::now();
        let deadline = start + Duration::from_millis(20);
        leases.room(Duration::from_secs(60), Some(deadline)).await;
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
