//! Admission: how much work one segment instance accepts at once.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use gdl::{Error, ErrorKind, Result};

/// Admission for one segment instance.
///
/// The limit counts root requests, not entries. A request may enter one
/// instance several times (a provisioned backward run and a forward stage of
/// the same hop, or two hops of a nested graph), and each entry after the
/// first rides on the permit the first took. Without that, one request can
/// block itself: with a limit of one, the run it provisioned would hold the
/// only permit its own forward stage then waits for.
///
/// Waiting for a permit is bounded, so admission can reject but never
/// deadlock.
pub(crate) struct Admission {
    permits: Arc<Semaphore>,
    limit: usize,
    wait: Duration,
    /// Woken whenever a root is newly admitted, so an entry that waits for a
    /// permit while another entry of its root takes one joins that entry
    /// instead of waiting for a second permit the root does not need.
    admitted: Notify,
    ledger: Arc<Ledger>,
    attempts: Arc<DashMap<u128, usize>>,
    attempt_limit: usize,
}

/// The roots admitted, each with its permit, and their count, kept apart
/// so that a load report reads it without a lock a request takes. Both
/// change together, when a root is first admitted and when its last share
/// drops, and only then.
struct Ledger {
    held: DashMap<u128, Held>,
    roots: Roots,
}

struct Held {
    shares: usize,
    _permit: OwnedSemaphorePermit,
}

impl Admission {
    pub(crate) fn new(limit: usize, wait: Duration, attempt_limit: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit)),
            limit,
            wait,
            admitted: Notify::new(),
            ledger: Arc::new(Ledger {
                held: DashMap::new(),
                roots: Roots::new(),
            }),
            attempts: Arc::new(DashMap::new()),
            attempt_limit,
        }
    }

    /// Admit one entry of root request `root`.
    pub(crate) async fn admit(&self, root: u128) -> Result<Share> {
        if let Some(share) = self.join(root) {
            return Ok(share);
        }
        let expiry = tokio::time::sleep(self.wait);
        let acquire = Arc::clone(&self.permits).acquire_owned();
        tokio::pin!(expiry, acquire);
        let permit = loop {
            // Registered before the check, so an admission between the check
            // and the wait still wakes this entry.
            let admitted = self.admitted.notified();
            tokio::pin!(admitted);
            admitted.as_mut().enable();
            if let Some(share) = self.join(root) {
                return Ok(share);
            }
            // Biased so a free permit is taken before the timer is ever
            // polled: an uncontended admission registers no timer.
            tokio::select! {
                biased;
                permit = &mut acquire => {
                    break permit.map_err(|_| overloaded("admission is closed"))?;
                }
                () = admitted => {}
                () = &mut expiry => {
                    return Err(overloaded("no admission permit became free in time"));
                }
            }
        };
        match self.ledger.held.entry(root) {
            // Another entry of the same request was admitted between the
            // last check and the permit; join it and give the permit back.
            Entry::Occupied(mut held) => held.get_mut().shares += 1,
            Entry::Vacant(slot) => {
                slot.insert(Held {
                    shares: 1,
                    _permit: permit,
                });
                self.ledger.roots.add(root);
                self.admitted.notify_waiters();
            }
        }
        Ok(Share {
            root,
            ledger: Arc::clone(&self.ledger),
        })
    }

    fn join(&self, root: u128) -> Option<Share> {
        let mut held = self.ledger.held.get_mut(&root)?;
        held.shares += 1;
        Some(Share {
            root,
            ledger: Arc::clone(&self.ledger),
        })
    }

    /// Count one outstanding remote attempt of `root`, or refuse it.
    pub(crate) fn attempt(&self, root: u128) -> Result<AttemptSlot> {
        let mut count = self.attempts.entry(root).or_insert(0);
        if *count >= self.attempt_limit {
            return Err(overloaded("the request has too many outstanding attempts"));
        }
        *count += 1;
        Ok(AttemptSlot {
            root,
            attempts: Arc::clone(&self.attempts),
        })
    }

    /// Roots currently admitted. For tests: must return to zero.
    pub(crate) fn admitted(&self) -> usize {
        self.ledger.held.len()
    }

    /// The roots admitted now and the limit, as a load report reads them:
    /// the ledger's count, read from relaxed counters without a lock a
    /// request takes. Not the permits handed out: one released while
    /// another root waits passes to the waiter before the waiter enters
    /// the ledger, or gives the permit back when it joins its root.
    pub(crate) fn load(&self) -> (usize, usize) {
        (self.ledger.roots.count(), self.limit)
    }

    /// Roots with outstanding attempts. For tests: must return to zero.
    pub(crate) fn attempting(&self) -> usize {
        self.attempts.len()
    }
}

/// The egress quota of one instance: how many streams it may be producing
/// at once. Each producing stream holds a permit from before it attaches
/// until velo has handed its output on, so a full instance refuses new work
/// instead of buffering for it.
///
/// A finished stream's unsent output (up to its slot byte budget plus its
/// credit window) stays in velo until the consumer reads it or leaves; the
/// permit is held until velo says so (`StreamSender::retired`), and no
/// longer than the request's deadline, which every gdl consumer holds. A
/// grace period after the end was tried before velo had that signal, and
/// removed: it capped an instance at `limit / grace` requests per second
/// (measured: 1,024 permits and 5 s refused traffic within seconds at
/// benchmark rates) and still bounded nothing for a consumer that stalls
/// longer than the grace. See docs/src/development/runtime-design.md.
pub(crate) struct Egress {
    permits: Arc<Semaphore>,
    limit: usize,
    wait: Duration,
}

impl Egress {
    pub(crate) fn new(limit: usize, wait: Duration) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit)),
            limit,
            wait,
        }
    }

    /// One producing stream's permit, or a refusal after a bounded wait.
    pub(crate) async fn acquire(&self) -> Result<EgressPermit> {
        let permit = tokio::time::timeout(self.wait, Arc::clone(&self.permits).acquire_owned())
            .await
            .map_err(|_| overloaded("the instance's egress quota is in use"))?
            .map_err(|_| overloaded("egress is closed"))?;
        Ok(EgressPermit { _permit: permit })
    }

    /// Permits currently held.
    pub(crate) fn in_use(&self) -> usize {
        self.limit - self.permits.available_permits()
    }
}

/// One producing stream's share of the egress quota, released when the
/// stream ends.
pub(crate) struct EgressPermit {
    _permit: OwnedSemaphorePermit,
}

fn overloaded(why: &str) -> Error {
    Error::new(ErrorKind::Overloaded, why)
}

/// One entry's share of its root request's admission. The permit is released
/// when the last share of the root drops.
pub(crate) struct Share {
    root: u128,
    ledger: Arc<Ledger>,
}

impl Drop for Share {
    fn drop(&mut self) {
        // `remove_if` holds the shard lock across the check, so a concurrent
        // join cannot slip in between the decrement and the removal.
        if let Some(mut held) = self.ledger.held.get_mut(&self.root) {
            held.shares -= 1;
        }
        let removed = self
            .ledger
            .held
            .remove_if(&self.root, |_, held| held.shares == 0);
        if removed.is_some() {
            self.ledger.roots.remove(self.root);
        }
    }
}

/// The count of admitted roots, sharded by root as the ledger is, so that
/// two roots admitted at once rarely touch one cache line. A root is
/// added and removed on its own shard, after each other, so no shard's
/// count is ever below zero, and the sum of relaxed reads is a count the
/// ledger held while they were made, give or take the roots that changed
/// meanwhile.
struct Roots {
    shards: Box<[Shard]>,
}

/// One shard, alone on its cache line.
#[repr(align(128))]
#[derive(Default)]
struct Shard(AtomicUsize);

impl Roots {
    fn new() -> Self {
        let threads = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let shards = (threads * 4).next_power_of_two();
        Self {
            shards: (0..shards).map(|_| Shard::default()).collect(),
        }
    }

    fn shard(&self, root: u128) -> &AtomicUsize {
        // The length is a power of two; a root's bits are well mixed.
        let folded = (root as u64) ^ ((root >> 64) as u64);
        let at = usize::try_from(folded).unwrap_or(usize::MAX) & (self.shards.len() - 1);
        &self.shards[at].0
    }

    fn add(&self, root: u128) {
        self.shard(root).fetch_add(1, Ordering::Relaxed);
    }

    fn remove(&self, root: u128) {
        self.shard(root).fetch_sub(1, Ordering::Relaxed);
    }

    fn count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.0.load(Ordering::Relaxed))
            .sum()
    }
}

/// One outstanding remote attempt.
pub(crate) struct AttemptSlot {
    root: u128,
    attempts: Arc<DashMap<u128, usize>>,
}

impl Drop for AttemptSlot {
    fn drop(&mut self) {
        if let Some(mut count) = self.attempts.get_mut(&self.root) {
            *count -= 1;
        }
        self.attempts.remove_if(&self.root, |_, count| *count == 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn entries_of_one_root_share_a_permit() {
        let admission = Admission::new(1, Duration::from_millis(20), 8);
        let first = admission.admit(1).await.expect("first");
        let second = admission.admit(1).await.expect("same root rides along");
        let other = admission.admit(2).await;
        assert_eq!(other.err().map(|e| e.kind()), Some(ErrorKind::Overloaded));
        drop(first);
        assert_eq!(
            admission.admitted(),
            1,
            "the root is still held by its second share"
        );
        drop(second);
        assert_eq!(admission.admitted(), 0);
        let _other = admission.admit(2).await.expect("the permit was released");
    }

    #[tokio::test]
    async fn a_waiting_entry_is_admitted_when_a_permit_frees() {
        let admission = Arc::new(Admission::new(1, Duration::from_secs(5), 8));
        let first = admission.admit(1).await.expect("first");
        let waiter = {
            let admission = Arc::clone(&admission);
            tokio::spawn(async move { admission.admit(2).await.map(|_| ()) })
        };
        tokio::task::yield_now().await;
        drop(first);
        waiter
            .await
            .expect("join")
            .expect("admitted after the release");
        assert_eq!(admission.admitted(), 0);
    }

    #[tokio::test]
    async fn concurrent_first_entries_of_one_root_share_the_permit_that_frees() {
        let admission = Arc::new(Admission::new(1, Duration::from_secs(5), 8));
        let other = admission
            .admit(1)
            .await
            .expect("another root holds the only permit");
        let entry = || {
            let admission = Arc::clone(&admission);
            tokio::spawn(async move { admission.admit(2).await })
        };
        let (first, second) = (entry(), entry());
        // Both entries are waiting for the permit before it frees.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        drop(other);
        let first = first.await.expect("join").expect("first entry admitted");
        let second = second
            .await
            .expect("join")
            .expect("second entry joins the first");
        assert_eq!(admission.admitted(), 1);
        drop((first, second));
        assert_eq!(admission.admitted(), 0);
    }

    /// The load counts each admitted root once, however many entries
    /// share it, and nothing once its last share is gone.
    #[tokio::test]
    async fn the_load_counts_admitted_roots_against_the_limit() {
        let admission = Admission::new(4, Duration::from_millis(20), 8);
        assert_eq!(admission.load(), (0, 4));
        let first = admission.admit(1).await.expect("first");
        let second = admission.admit(1).await.expect("same root rides along");
        let other = admission.admit(2).await.expect("another root");
        assert_eq!(admission.load(), (2, 4));
        drop((first, other));
        assert_eq!(
            admission.load(),
            (1, 4),
            "the root is held by its second share"
        );
        drop(second);
        assert_eq!(admission.load(), (0, 4));
    }

    /// A permit released while another root waits passes to the waiter at
    /// once, before the waiter is polled and enters the ledger: the load
    /// counts the roots the ledger holds, not the permits handed out.
    #[tokio::test]
    async fn the_load_counts_the_ledger_not_a_permit_handed_to_a_waiter() {
        let admission = Admission::new(1, Duration::from_secs(5), 8);
        let first = admission.admit(1).await.expect("first");
        let waiting = admission.admit(2);
        tokio::pin!(waiting);
        assert!(
            futures::poll!(waiting.as_mut()).is_pending(),
            "no permit yet"
        );
        drop(first);
        assert_eq!(admission.admitted(), 0, "the ledger holds no root");
        assert_eq!(admission.load(), (0, 1));
        let second = waiting.await.expect("admitted once polled");
        assert_eq!((admission.admitted(), admission.load()), (1, (1, 1)));
        drop(second);
        assert_eq!(admission.load(), (0, 1));
    }

    #[test]
    fn attempts_per_root_are_limited_and_released() {
        let admission = Admission::new(4, Duration::from_millis(1), 2);
        let a = admission.attempt(7).expect("one");
        let b = admission.attempt(7).expect("two");
        assert_eq!(
            admission.attempt(7).err().map(|e| e.kind()),
            Some(ErrorKind::Overloaded)
        );
        let _other_root = admission.attempt(8).expect("another root is separate");
        drop(a);
        let _c = admission.attempt(7).expect("a slot freed");
        drop(b);
        drop(_c);
        drop(_other_root);
        assert_eq!(admission.attempting(), 0);
    }
}
