//! The pools whose hubs follow this pool, as their pool watches name them:
//! a draining pool's entry instances stop taking work from other pools
//! early once no pool follows it any more, since a follower lets go of a
//! pool only once none of its choosers holds an offer to it.
//!
//! A follower is counted per watch, keyed by the follow it belongs to
//! ([`gdl_watch::Follower`]: the follower's run and the follow's
//! generation), its pool's name only a label: a hub that restarted, and
//! follows the pool again while its earlier watch is still half open, is
//! not ended by that watch's late close. Only a release says that a
//! follower holds no offer to the pool in a follow, which the follower
//! sends once none of its choosers does. To a reader, a watch is in one of
//! these states:
//!
//! - **Open**: it counts as a follower.
//! - **Closed without a release**: it still counts as a follower, since
//!   its follower may still hold offers to the pool that the link's loss
//!   did nothing to end.
//! - **Lost**: closed without a release for the pool's lease. Its record
//!   goes, and it no longer shows as a follower, but it is never taken as
//!   a release: while any watch is lost, the pool never says it is
//!   unfollowed, and whatever waits for the followers waits out its own
//!   bound instead. A release of it, arriving late, still settles it.
//! - **Released**: gone.
//!
//! So a draining pool says it is unfollowed only once every watch that
//! counted has been released, or none ever did. A watch that names no
//! follower, a pool's own instance's or a test's, counts for nothing. Each
//! change is made under the feed's lock, so that the signal that no pool
//! follows the draining pool is sent in order with the rest of the watch.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gdl_pools::PoolState;
use tokio::sync::watch;

use super::feed::Feed;

/// A pool that follows this one, by its class and its name within the
/// class: a label of its watches.
pub(super) type Key = (String, String);

/// One watch of a follower: its label, and the run and generation of the
/// follow it belongs to.
pub(super) type WatchKey = (Key, u64, u64);

/// The watches of the pools that follow this one, and whether the pool has
/// been told that none does while it drains.
pub(super) struct Followers {
    watches: HashMap<WatchKey, Watched>,
    /// The lost watches: their release is not known, and only it settles
    /// them.
    lost: HashSet<WatchKey>,
    /// How long a watch closed without a release still counts.
    lease: Duration,
    /// Whether the pool drains and no watch has counted since: once so, it
    /// stays so.
    unfollowed: bool,
    /// The followers whose watches count, as they change, for whoever
    /// needs to see them.
    shown: watch::Sender<BTreeSet<Key>>,
}

/// One follower's watch, while it counts.
struct Watched {
    /// How many connections hold it open: one, but for a follower that
    /// opened it again under the same follow.
    open: usize,
    /// When its last connection closed, without a release.
    closed: Option<Instant>,
}

impl Followers {
    /// No follower, whose watches closed without a release count for
    /// `lease` after they close.
    pub(super) fn new(lease: Duration) -> Self {
        Self {
            watches: HashMap::new(),
            lost: HashSet::new(),
            lease,
            unfollowed: false,
            shown: watch::Sender::new(BTreeSet::new()),
        }
    }

    /// The watch `key` opened.
    pub(super) fn followed(&mut self, key: &WatchKey) {
        self.lost.remove(key);
        let watched = self.watches.entry(key.clone()).or_insert(Watched {
            open: 0,
            closed: None,
        });
        watched.open += 1;
        watched.closed = None;
        self.show();
    }

    /// A connection of the watch `key` closed, at `now`: if it was the
    /// last, the watch still counts, until it is released, and is lost
    /// once its lease passes. A watch already released counts for nothing
    /// either way.
    pub(super) fn left(&mut self, key: &WatchKey, now: Instant) {
        if let Some(watched) = self.watches.get_mut(key) {
            watched.open -= 1;
            if watched.open == 0 {
                watched.closed = Some(now);
            }
        }
    }

    /// The watch `key`'s follower let go of the pool in it, while the pool
    /// is in `state`: whether, from now on, no pool follows the draining
    /// pool. A release of a watch that is not counted, nor lost, changes
    /// nothing.
    pub(super) fn release(&mut self, key: &WatchKey, state: PoolState) -> bool {
        if self.watches.remove(key).is_some() {
            self.show();
        }
        self.lost.remove(key);
        self.settle(state)
    }

    /// At `now`, mark lost each watch whose lease has passed since it
    /// closed without a release: it no longer shows as a follower, and is
    /// no release either.
    pub(super) fn expire(&mut self, now: Instant) {
        let lease = self.lease;
        let lost: Vec<WatchKey> = self
            .watches
            .iter()
            .filter(|(_, watched)| {
                watched
                    .closed
                    .is_some_and(|closed| now.saturating_duration_since(closed) >= lease)
            })
            .map(|(key, _)| key.clone())
            .collect();
        if lost.is_empty() {
            return;
        }
        for key in lost {
            self.watches.remove(&key);
            self.lost.insert(key);
        }
        self.show();
    }

    /// The pool is in `state`: whether, from now on, it drains and every
    /// watch that counted has been released, which is so only once.
    pub(super) fn settle(&mut self, state: PoolState) -> bool {
        if self.unfollowed
            || state != PoolState::Draining
            || !self.watches.is_empty()
            || !self.lost.is_empty()
        {
            return false;
        }
        self.unfollowed = true;
        true
    }

    /// Every watch that counts, or is lost: what a retirement announced
    /// now waits for ([`super::retiring`]).
    pub(super) fn counted(&self) -> impl Iterator<Item = &WatchKey> {
        self.watches.keys().chain(&self.lost)
    }

    /// Whether the draining pool has no follower any more.
    pub(super) fn unfollowed(&self) -> bool {
        self.unfollowed
    }

    /// The followers whose watches count, as they change.
    pub(super) fn subscribe(&self) -> watch::Receiver<BTreeSet<Key>> {
        self.shown.subscribe()
    }

    fn show(&self) {
        let shown: BTreeSet<Key> = self.watches.keys().map(|(key, ..)| key.clone()).collect();
        self.shown.send_if_modified(|followers| {
            let changed = *followers != shown;
            *followers = shown;
            changed
        });
    }
}

/// One connection of a follower's watch, until it is dropped with the
/// watch.
pub(super) struct Following {
    pub(super) feed: Arc<Feed>,
    pub(super) key: WatchKey,
}

impl Drop for Following {
    fn drop(&mut self) {
        self.feed.unfollow(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::Followers;
    use gdl_pools::PoolState;

    fn watch(run: u64) -> super::WatchKey {
        (("global".to_string(), "g1".to_string()), run, 1)
    }

    /// A draining pool is unfollowed once the last watch that counts is
    /// released, or at once if none counts; once, whatever comes after. A
    /// watch closed without a release still counts.
    #[test]
    fn a_draining_pool_is_unfollowed_once_its_last_watch_is_released() {
        let now = Instant::now();
        let mut followers = Followers::new(Duration::from_secs(30));
        followers.followed(&watch(1));
        assert!(!followers.settle(PoolState::Draining), "followed");
        followers.left(&watch(1), now);
        assert!(
            !followers.settle(PoolState::Draining),
            "closed, not released"
        );
        assert_eq!(followers.subscribe().borrow().len(), 1);
        assert!(followers.release(&watch(1), PoolState::Draining));
        assert!(followers.unfollowed());
        assert!(followers.subscribe().borrow().is_empty());
        followers.followed(&watch(2));
        assert!(
            !followers.release(&watch(2), PoolState::Draining),
            "told twice"
        );

        let mut alone = Followers::new(Duration::from_secs(30));
        assert!(!alone.settle(PoolState::Ready), "not draining");
        assert!(alone.settle(PoolState::Draining), "no follower at all");
    }

    /// Two watches under one follower's name count apart: with one
    /// released, the other still counts. One closed without a release
    /// shows as a follower until its lease has passed since it closed, and
    /// after that is still no release: only its own release settles it.
    #[test]
    fn a_lost_watch_is_never_a_release() {
        let now = Instant::now();
        let lease = Duration::from_secs(30);
        let mut followers = Followers::new(lease);
        followers.followed(&watch(1));
        followers.followed(&watch(2));
        followers.left(&watch(1), now);
        assert!(
            !followers.release(&watch(2), PoolState::Draining),
            "the earlier one counts"
        );
        followers.expire(now + lease / 2);
        assert_eq!(followers.subscribe().borrow().len(), 1);
        followers.expire(now + lease);
        assert!(followers.subscribe().borrow().is_empty());
        assert!(!followers.settle(PoolState::Draining), "lost, not released");
        assert!(followers.release(&watch(1), PoolState::Draining));
    }
}
