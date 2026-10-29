//! How a pool's hub follows the pools of a class its class sends to: it
//! finds them in the pool directory, follows each over its pool watch,
//! and relays what it follows to its own instances.
//!
//! One task per class watches the directory and decides, record by
//! record, which pools to follow; each follow is a task of its own that
//! reads one pool's watch into the [`Relay`]. The directory finds pools
//! and the link carries them: a record that expires or is withdrawn never
//! ends a follow, or changes whether it is offered, and a link that ends
//! makes its follow not offered at once, whatever the directory says. A
//! newer epoch in the directory ends a follow too, since the hub it was
//! following is gone. The follow set is the follower's children, the pools
//! whose record names its pool as their parent (`home`), so a pool leaves
//! it only so: a pool that stops taking work stays followed, not offered,
//! until the relay closes its follow. Region plays no part: a parent
//! follows its children wherever they run, and no other pool.
//!
//! A follow the relay closed, once every chooser acknowledged that it is
//! not offered, is followed again only once the directory shows a later
//! write of the pool's record that makes it a child that takes work: until
//! then the
//! directory may still show the pool taking work when its link said it
//! does not, and following it again would only close again.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use gdl_pools::{PoolDirectory, PoolEvent, PoolRecord, PoolState, PoolView, Presence};
use tokio::task::{AbortHandle, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use gdl_watch::{Backoff, PoolWatcher, WatchEnd};

use crate::PoolTiming;

mod avoid;
mod link;
mod offer;
mod relay;
mod release;

pub(crate) use offer::Choosers;
pub use relay::FollowsWatch;
pub(crate) use relay::{Relay, Retirement, expire};
pub(crate) use release::{Release, run as release};

/// The first wait before the directory is watched again after its watch
/// ended or failed: as short as it always was, so that a directory that
/// drops a watch once is watched again as quickly.
const REWATCH: Duration = Duration::from_millis(100);

/// The longest wait before the directory is watched again, however often
/// its watches failed. Longer than an instance's cap on its own hub's
/// watches: every hub of the fleet watches the one directory, so a
/// directory that is down is spared their retries, and the pools already
/// followed go on over their own links meanwhile, so only a pool new to
/// the directory waits for it.
const REWATCH_CAP: Duration = Duration::from_secs(10);

/// The follower of one class: which of its pools this hub follows.
pub(crate) struct Follower<D> {
    pub(crate) directory: Arc<D>,
    pub(crate) relay: Arc<Relay>,
    pub(crate) watcher: PoolWatcher,
    /// The first wait after a follow's link ended before the pool may be
    /// followed again; later ones back off to
    /// [`PoolTiming::REFOLLOW_CAP`].
    pub(crate) refollow: Duration,
    /// The follower's own pool, which its watches name, so that a
    /// followed pool knows who follows it, and which its children name as
    /// their parent.
    pub(crate) me: gdl_watch::Follower,
    /// The class followed.
    pub(crate) class: String,
    /// The follower's contract and placement hashes: a pool with others
    /// serves another deployment, and is ignored.
    pub(crate) contract: u64,
    pub(crate) placement: u64,
}

impl<D: PoolDirectory> Follower<D> {
    /// Follow until `cancel`. Every follow's task ends with it.
    pub(crate) async fn run(self, cancel: CancellationToken) {
        tokio::select! {
            () = self.follow() => {}
            () = cancel.cancelled() => {}
        }
    }

    async fn follow(self) {
        let mut follows = Follows {
            view: PoolView::new(),
            current: HashMap::new(),
            closed: HashMap::new(),
            links: JoinSet::new(),
            lapses: BTreeSet::new(),
            leases: HashMap::new(),
            follower: &self,
        };
        let mut backoff = Backoff::new(REWATCH, REWATCH_CAP);
        loop {
            let mut watch = std::pin::pin!(self.directory.watch(&self.class, None));
            let mut synced = None;
            let end = loop {
                let lapse = follows.next_lapse();
                tokio::select! {
                    event = watch.next() => match event {
                        Some(Ok(event)) => {
                            let snapshot = matches!(event, PoolEvent::Synced { .. });
                            if snapshot {
                                synced.get_or_insert_with(tokio::time::Instant::now);
                            }
                            follows.directory(event);
                            if snapshot {
                                self.relay.synced(&self.class);
                            }
                        }
                        Some(Err(error)) => {
                            tracing::warn!(class = self.class, %error, "the pool directory's watch failed");
                            break WatchEnd::Failed;
                        }
                        None => break WatchEnd::Closed,
                    },
                    Some(ended) = follows.links.join_next() => follows.ended(ended),
                    () = until(lapse) => follows.lapse(Instant::now()),
                }
            };
            // A watch that failed or ended may have been part way through
            // its snapshot: the pools it named so far must not count as
            // held when the next one's snapshot completes, or a pool
            // withdrawn meanwhile would be kept.
            follows.view.apply(PoolEvent::Resnapshot, Instant::now());
            let pause = tokio::time::sleep(backoff.wait(synced, end, tokio::time::Instant::now()));
            tokio::pin!(pause);
            loop {
                let lapse = follows.next_lapse();
                tokio::select! {
                    () = &mut pause => break,
                    Some(ended) = follows.links.join_next() => follows.ended(ended),
                    () = until(lapse) => follows.lapse(Instant::now()),
                }
            }
        }
    }

    /// Whether `record` is a child that takes work: it names this pool as
    /// its parent, is of this deployment, is `ready` or `degraded`, and is
    /// serving. A pool scaled to 0 takes no work, and its record never
    /// lapses though its hub may be gone, so following it would dial that
    /// hub without end.
    fn child(&self, record: &PoolRecord) -> bool {
        record.home.as_deref() == Some(self.me.pool.as_str())
            && record.contract == self.contract
            && record.placement == self.placement
            && matches!(record.state, PoolState::Ready | PoolState::Degraded)
            && record.presence == Presence::Serving
    }
}

/// Wait until `at`; for ever without one.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at.into()).await,
        None => std::future::pending().await,
    }
}

/// The follower's state, owned by its task.
struct Follows<'a, D> {
    /// The class's records, as the directory shows them.
    view: PoolView,
    /// Each pool followed: its link under way, or waiting to follow again.
    current: HashMap<String, Following>,
    /// The pools whose follow the relay closed, each with the epoch and
    /// `seq` of its record when it closed: followed again only from a
    /// later write.
    closed: HashMap<String, (u64, u64)>,
    /// The links' tasks, each returning its pool, its generation, whether
    /// the relay closed it, and the backoff of the pool's links, once it
    /// has ended and waited to follow again. Dropping the set ends them.
    links: JoinSet<(String, u64, bool, Backoff)>,
    /// When each listed `serving` record's lease passes, in order, and
    /// by pool: the relay lists the pool missing from then until a newer
    /// write. A lease passes with no event, so the follower keeps the one
    /// timer, for the earliest. By pool, also when the follower saw the
    /// record's last write, which the pool has been silent since.
    lapses: BTreeSet<(Instant, String)>,
    leases: HashMap<String, (Instant, Instant)>,
    follower: &'a Follower<D>,
}

/// One pool followed.
struct Following {
    generation: u64,
    /// The epoch of the record the follow began from.
    epoch: u64,
    /// The `seq` of the last record of the pool the follower decided on
    /// while it followed the pool: what a write must pass to follow the
    /// pool again once the relay has closed the follow.
    seq: u64,
    /// Cancelled once the relay closes the follow.
    closed: CancellationToken,
    link: AbortHandle,
}

impl<D: PoolDirectory> Follows<'_, D> {
    /// Apply one event of the directory's watch, and decide on the pool it
    /// names, if any: a record is considered when it arrives, never on a
    /// pass over the others.
    ///
    /// A withdrawal the view takes is relayed: the pool drained on
    /// purpose. A pool a completed snapshot no longer holds is not, since
    /// the directory may have lost its record for any reason, and silence
    /// is never a decommission.
    fn directory(&mut self, event: PoolEvent) {
        let (pool, withdrawal) = match &event {
            PoolEvent::Current(record) | PoolEvent::Put { record, .. } => {
                (Some(record.name.clone()), None)
            }
            PoolEvent::Withdrawn { withdrawal, .. } => (None, Some(withdrawal.clone())),
            _ => (None, None),
        };
        let now = Instant::now();
        self.view.apply(event, now);
        if let Some(pool) = pool {
            self.list(&pool, now);
            self.decide(&pool, now, None);
        }
        if let Some(withdrawal) = withdrawal
            && self.view.withdrawn(&withdrawal.pool) == Some(withdrawal.version())
        {
            self.unlease(&withdrawal.pool);
            self.follower
                .relay
                .withdrawn(&self.follower.class, &withdrawal.pool);
        }
    }

    /// List `pool` from the record the view holds of it, if any, whatever
    /// its parent, region, or state: what its choosers' property index
    /// knows. A record of another deployment is not listed. One the view
    /// no longer holds, after a snapshot that dropped it, stays listed as
    /// it was: silence never removes a pool, and only a withdrawal does.
    fn list(&mut self, pool: &str, now: Instant) {
        let Some((record, changed)) = self.view.seen(pool) else {
            return;
        };
        let relay = &self.follower.relay;
        if record.contract != self.follower.contract || record.placement != self.follower.placement
        {
            self.unlease(pool);
            relay.unlist(&self.follower.class, pool);
            return;
        }
        let lapses = record
            .leased()
            .then(|| changed + record.lease)
            .filter(|&at| at >= now);
        relay.record(
            record,
            (record.leased() && lapses.is_none()).then_some(changed),
        );
        self.unlease(pool);
        if let Some(at) = lapses {
            self.lapses.insert((at, pool.to_string()));
            self.leases.insert(pool.to_string(), (at, changed));
        }
    }

    fn unlease(&mut self, pool: &str) {
        if let Some((at, _)) = self.leases.remove(pool) {
            self.lapses.remove(&(at, pool.to_string()));
        }
    }

    /// When the next listed lease passes, if one is held.
    fn next_lapse(&self) -> Option<Instant> {
        self.lapses.first().map(|(at, _)| *at)
    }

    /// The leases that passed by `now`: their pools are listed missing.
    fn lapse(&mut self, now: Instant) {
        while let Some((at, _)) = self.lapses.first()
            && *at <= now
        {
            let (_, pool) = self.lapses.pop_first().expect("one was just found");
            let (_, written) = self.leases.remove(&pool).expect("each lapse has its lease");
            self.follower
                .relay
                .lapsed(&self.follower.class, &pool, written);
        }
    }

    /// Follow `pool` if its record makes it a child that takes work
    /// and it is not followed yet, unless the relay closed its last follow
    /// and the record is no later than it was then; end its follow if the
    /// record has a newer epoch than the one it began from, or says the
    /// pool is scaled to 0, and follow it anew if it can. A follow begun after a link that ended keeps that
    /// link's `backoff`, so that a pool whose links keep failing is
    /// followed again less and less often; any other begins one afresh.
    fn decide(&mut self, pool: &str, now: Instant, backoff: Option<Backoff>) {
        let record = self.view.get(pool, now).cloned();
        if let Some(following) = self.current.get_mut(pool) {
            let newer = record
                .as_ref()
                .is_some_and(|record| record.epoch > following.epoch);
            // A pool scaled to 0 takes no work, so its follow ends as for a
            // newer epoch, kept until its choosers let go of it.
            let scaled = record
                .as_ref()
                .is_some_and(|record| record.presence != Presence::Serving);
            let ends = newer || scaled;
            if !ends && !following.closed.is_cancelled() {
                if let Some(record) = record.as_ref().filter(|r| r.epoch == following.epoch) {
                    following.seq = following.seq.max(record.seq);
                }
                return;
            }
            let following = self
                .current
                .remove(pool)
                .expect("the follow was just found");
            following.link.abort();
            if ends {
                self.follower
                    .relay
                    .end(&self.follower.class, pool, following.generation);
            } else {
                // The relay closed the follow before this write came: the
                // write is news of the pool after the close.
                self.closed
                    .insert(pool.to_string(), (following.epoch, following.seq));
            }
        }
        if let (Some(record), Some(closed)) = (&record, self.closed.get(pool))
            && (record.epoch, record.seq) > *closed
        {
            self.closed.remove(pool);
        }
        match record.filter(|record| self.follower.child(record) && !self.closed.contains_key(pool))
        {
            Some(record) => self.start(record, backoff),
            None => self.follower.relay.forget(&self.follower.class, pool),
        }
    }

    /// A link's task ended: if it is the pool's current follow, follow the
    /// pool again if its record still makes it a child that takes work.
    fn ended(&mut self, ended: Result<(String, u64, bool, Backoff), JoinError>) {
        // A task aborted for a newer epoch was replaced already.
        let Ok((pool, generation, closed, backoff)) = ended else {
            return;
        };
        if self
            .current
            .get(&pool)
            .is_none_or(|following| following.generation != generation)
        {
            return;
        }
        let following = self
            .current
            .remove(&pool)
            .expect("the follow was just found");
        if closed {
            self.closed
                .insert(pool.clone(), (following.epoch, following.seq));
        }
        self.decide(&pool, Instant::now(), Some(backoff));
    }

    fn start(&mut self, record: PoolRecord, backoff: Option<Backoff>) {
        let backoff = backoff
            .unwrap_or_else(|| Backoff::new(self.follower.refollow, PoolTiming::REFOLLOW_CAP));
        let relay = &self.follower.relay;
        let (generation, close) = relay.begin(&record.class, &record.name);
        let name = record.name.clone();
        let (epoch, seq) = (record.epoch, record.seq);
        let closed = close.clone();
        let link = self.links.spawn(link::run(
            Arc::clone(relay),
            self.follower.watcher.clone(),
            self.follower.me.clone(),
            record,
            generation,
            close,
            backoff,
        ));
        self.current.insert(
            name,
            Following {
                generation,
                epoch,
                seq,
                closed,
                link,
            },
        );
    }
}
