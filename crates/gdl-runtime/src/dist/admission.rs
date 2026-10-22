//! Admission: how much work one segment instance accepts at once.

use std::sync::Arc;
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
    wait: Duration,
    /// Woken whenever a root is newly admitted, so an entry that waits for a
    /// permit while another entry of its root takes one joins that entry
    /// instead of waiting for a second permit the root does not need.
    admitted: Notify,
    ledger: Arc<DashMap<u128, Held>>,
    attempts: Arc<DashMap<u128, usize>>,
    attempt_limit: usize,
}

struct Held {
    shares: usize,
    _permit: OwnedSemaphorePermit,
}

impl Admission {
    pub(crate) fn new(limit: usize, wait: Duration, attempt_limit: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit)),
            wait,
            admitted: Notify::new(),
            ledger: Arc::new(DashMap::new()),
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
        match self.ledger.entry(root) {
            // Another entry of the same request was admitted between the
            // last check and the permit; join it and give the permit back.
            Entry::Occupied(mut held) => held.get_mut().shares += 1,
            Entry::Vacant(slot) => {
                slot.insert(Held {
                    shares: 1,
                    _permit: permit,
                });
                self.admitted.notify_waiters();
            }
        }
        Ok(Share {
            root,
            ledger: Arc::clone(&self.ledger),
        })
    }

    fn join(&self, root: u128) -> Option<Share> {
        let mut held = self.ledger.get_mut(&root)?;
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
        self.ledger.len()
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
/// longer than the grace. See `agent-docs/plan.md`, rulings 15 and 145.
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
    ledger: Arc<DashMap<u128, Held>>,
}

impl Drop for Share {
    fn drop(&mut self) {
        // `remove_if` holds the shard lock across the check, so a concurrent
        // join cannot slip in between the decrement and the removal.
        if let Some(mut held) = self.ledger.get_mut(&self.root) {
            held.shares -= 1;
        }
        self.ledger
            .remove_if(&self.root, |_, held| held.shares == 0);
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
