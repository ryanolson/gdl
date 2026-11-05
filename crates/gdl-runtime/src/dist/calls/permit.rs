//! A call's permit: how many calls from other segments one instance serves
//! at once, and how long each holds its share (docs/src/concepts/unary.md).
//!
//! A call takes its permit before its service runs and keeps it in its
//! reply: velo sends a reply after the handler returns, and its queue of
//! replies to a peer has no bound, so a permit released at the return would
//! let replies pile up behind a peer that does not read. The reply's bytes
//! own the permit, and it is released when velo lets the bytes go, or once
//! the call's bound plus `call_reply_grace` passes, whichever is first, so
//! that a peer that stops reading cannot hold the limit for ever.
//!
//! One task per instance releases the permits whose time has passed, from
//! a queue ordered by deadline, as `fabric/renewal.rs` renews every held
//! reply from one task.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// The calls one instance serves at once, and their count. Not a tokio
/// semaphore: drain reads the count after it closes the instance, and an
/// accepted call reads the close after it counted itself, both `SeqCst`,
/// so a call is either counted or refused. A semaphore's ordering promises
/// no such thing.
pub(crate) struct CallLimit {
    held: AtomicUsize,
    limit: usize,
    /// The replies that hold a permit, by id. Whoever removes an id, the
    /// reply's drop or the release at its deadline, releases its permit,
    /// so it is released once. Sharded, as the admission ledger is.
    retained: DashMap<u64, ()>,
    next: AtomicU64,
    /// Each retained reply's deadline and id, to the releasing task.
    due: mpsc::UnboundedSender<(Instant, u64)>,
    /// The entries the releasing task holds, as of its last pass.
    #[cfg(test)]
    queue: AtomicUsize,
}

impl CallLimit {
    /// A limit of `limit` calls, whose replies hold their permits for at
    /// most their deadline, which is at least about `grace` after the reply
    /// is written. Starts the instance's releasing task, which ends with the
    /// limit.
    pub(crate) fn new(limit: usize, grace: Duration) -> Arc<Self> {
        let (due, queued) = mpsc::unbounded_channel();
        let calls = Arc::new(Self {
            held: AtomicUsize::new(0),
            limit,
            retained: DashMap::new(),
            next: AtomicU64::new(0),
            due,
            #[cfg(test)]
            queue: AtomicUsize::new(0),
        });
        tokio::spawn(release(Arc::downgrade(&calls), queued, grace));
        calls
    }

    /// A permit, or none when `limit` calls hold one. One compare and swap
    /// while there is room: no lock, no allocation, no wait.
    pub(crate) fn take(self: &Arc<Self>) -> Option<CallPermit> {
        let mut held = self.held.load(Ordering::SeqCst);
        loop {
            if held >= self.limit {
                return None;
            }
            match self.held.compare_exchange_weak(
                held,
                held + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(CallPermit(Some(Arc::clone(self)))),
                Err(now) => held = now,
            }
        }
    }

    /// The permits held now.
    pub(crate) fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }

    /// Release the permit of retained reply `id`, unless it was already.
    fn release(&self, id: u64) {
        if self.retained.remove(&id).is_some() {
            self.held.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// One accepted call's share of its instance's [`CallLimit`], released
/// when it drops, or moved into its reply ([`CallPermit::retain`]), which
/// takes the limit's handle out, so that no hold on the limit is left.
pub(crate) struct CallPermit(Option<Arc<CallLimit>>);

impl Drop for CallPermit {
    fn drop(&mut self) {
        if let Some(calls) = &self.0 {
            calls.held.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl CallPermit {
    /// `reply`'s bytes, which hold this permit until velo lets them go, or
    /// until `until`, whichever is first. No allocation of its own beyond
    /// the bytes' owner, which a reply's bytes take anyway: an insert into
    /// the sharded set and a send to the instance's releasing task, which
    /// wakes it only when it is idle.
    pub(crate) fn retain(mut self, reply: Vec<u8>, until: Instant) -> Bytes {
        // The permit is the retained reply's now, released by its id; the
        // emptied permit releases nothing when it drops.
        let Some(calls) = self.0.take() else {
            return Bytes::from(reply);
        };
        let id = calls.next.fetch_add(1, Ordering::Relaxed);
        calls.retained.insert(id, ());
        // The task ends only with the limit, which the reply holds.
        let _ = calls.due.send((until, id));
        Bytes::from_owner(Retained { reply, calls, id })
    }
}

/// A reply's bytes, with the permit its call holds until they drop.
struct Retained {
    reply: Vec<u8>,
    calls: Arc<CallLimit>,
    id: u64,
}

impl AsRef<[u8]> for Retained {
    fn as_ref(&self) -> &[u8] {
        &self.reply
    }
}

impl Drop for Retained {
    fn drop(&mut self) {
        self.calls.release(self.id);
    }
}

/// The instance's releasing task: it releases each retained reply's permit
/// at its deadline. Idle, it waits for the next reply. Otherwise it sleeps
/// until the earliest deadline it holds, and takes what was queued at
/// least every half `grace`: a reply's deadline is its call's bound plus
/// `grace`, and a reply is written by its bound, so none is released late
/// for want of being read. Entries whose reply already dropped are pruned
/// once they outnumber the held ones, so the queue stays as large as what
/// is held.
async fn release(
    calls: Weak<CallLimit>,
    mut queued: mpsc::UnboundedReceiver<(Instant, u64)>,
    grace: Duration,
) {
    let tick = (grace / 2).max(Duration::from_millis(1));
    let mut due: BinaryHeap<Reverse<(Instant, u64)>> = BinaryHeap::new();
    loop {
        if due.is_empty() {
            match queued.recv().await {
                Some(next) => due.push(Reverse(next)),
                None => return,
            }
        }
        while let Ok(next) = queued.try_recv() {
            due.push(Reverse(next));
        }
        let Some(&Reverse((earliest, _))) = due.peek() else {
            continue;
        };
        tokio::time::sleep_until(earliest.min(Instant::now() + tick)).await;
        let Some(calls) = calls.upgrade() else {
            return;
        };
        while let Ok(next) = queued.try_recv() {
            due.push(Reverse(next));
        }
        let now = Instant::now();
        while let Some(&Reverse((at, id))) = due.peek() {
            if at > now {
                break;
            }
            due.pop();
            calls.release(id);
        }
        let held = calls.retained.len();
        if due.len() > 2 * held + 1024 {
            due.retain(|Reverse((_, id))| calls.retained.contains_key(id));
        }
        #[cfg(test)]
        calls.queue.store(due.len(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The limit is exact: `limit` permits, then none until one drops.
    #[tokio::test]
    async fn a_full_limit_gives_no_permit_until_one_drops() {
        let limit = CallLimit::new(2, Duration::from_secs(1));
        let (a, b) = (limit.take().expect("one"), limit.take().expect("two"));
        assert!(limit.take().is_none(), "a third past the limit");
        assert_eq!(limit.held(), 2);
        drop(a);
        let c = limit.take().expect("the dropped one's room");
        drop((b, c));
        assert_eq!(limit.held(), 0);
    }

    /// A reply's bytes hold their permit until they drop, whether or not
    /// their deadline has passed: velo let them go. The release is the
    /// drop's own, at once.
    #[tokio::test(start_paused = true)]
    async fn the_permit_returns_when_the_replys_bytes_drop() {
        let limit = CallLimit::new(1, Duration::from_secs(1));
        let bytes = limit
            .take()
            .expect("a permit")
            .retain(vec![1, 2, 3], Instant::now() + Duration::from_secs(60));
        assert_eq!(&bytes[..], [1, 2, 3]);
        assert!(limit.take().is_none(), "the reply holds the permit");
        let clone = bytes.clone();
        drop(bytes);
        assert_eq!(limit.held(), 1, "a clone still holds it");
        drop(clone);
        assert_eq!(limit.held(), 0, "released with the bytes");
        // The deadline passing later releases nothing more.
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert_eq!(limit.held(), 0);
        let again = limit.take().expect("a permit");
        assert_eq!(limit.held(), 1);
        drop(again);
    }

    /// Bytes velo never lets go of, as behind a peer that does not read,
    /// hold their permit only until their deadline, each its own, in
    /// whatever order they were written.
    #[tokio::test(start_paused = true)]
    async fn bytes_never_let_go_release_their_permits_at_their_deadlines() {
        let grace = Duration::from_secs(1);
        let limit = CallLimit::new(2, grace);
        let start = Instant::now();
        let (late, early) = (start + Duration::from_millis(2_500), start + grace);
        let a = limit.take().expect("a permit").retain(vec![7], late);
        let b = limit.take().expect("a permit").retain(vec![8], early);
        tokio::time::sleep_until(early - Duration::from_millis(1)).await;
        assert_eq!(limit.held(), 2, "released before its time");
        tokio::time::sleep_until(early + Duration::from_millis(1)).await;
        assert_eq!(limit.held(), 1, "the earlier deadline held past its time");
        tokio::time::sleep_until(late - Duration::from_millis(1)).await;
        assert_eq!(limit.held(), 1, "the later released before its time");
        tokio::time::sleep_until(late + Duration::from_millis(1)).await;
        assert_eq!(limit.held(), 0, "the later held past its time");
        assert_eq!(
            (&a[..], &b[..]),
            (&[7][..], &[8][..]),
            "the bytes stay whole"
        );
        drop((a, b));
        assert_eq!(limit.held(), 0, "a drop after the deadline released again");
    }

    /// Entries whose reply already dropped do not pile up in the queue:
    /// it is pruned to what is held.
    #[tokio::test(start_paused = true)]
    async fn the_queue_keeps_no_more_than_what_is_held() {
        let limit = CallLimit::new(10_000, Duration::from_secs(1));
        let far = Instant::now() + Duration::from_secs(3_600);
        for _ in 0..5_000 {
            drop(limit.take().expect("a permit").retain(vec![0], far));
        }
        let kept = limit.take().expect("a permit").retain(vec![1], far);
        // A tick: the task takes what was queued, then prunes.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            limit.queue.load(Ordering::Relaxed),
            1,
            "dropped replies kept"
        );
        assert_eq!(limit.retained.len(), 1);
        assert_eq!(limit.held(), 1);
        drop(kept);
        assert_eq!(limit.held(), 0);
    }
}
