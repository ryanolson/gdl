//! The cells whose hubs follow this cell, as their cell watches name them:
//! a draining cell's entry instances stop taking work from other cells
//! early once no cell follows it any more, since a follower lets go of a
//! cell only once none of its choosers holds an offer to it.
//!
//! A follower is counted per watch, keyed by the follow it belongs to
//! ([`gdl_watch::Follower`]: the follower's run and the follow's
//! generation), its cell's name only a label: a hub that restarted, and
//! follows the cell again while its earlier watch is still half open, is
//! not ended by that watch's late close. A watch counts from when it opens
//! until its follower releases it, which the follower does once none of
//! its choosers holds an offer to the cell in that follow. A watch that
//! closes without a release still counts, since its follower may still
//! hold offers to the cell that the link's loss did nothing to end, until
//! the cell's lease has passed since it closed: a follower that never comes
//! back stops counting then, and the entry instances' own bound ends their
//! intake whatever this counts. A watch that names no follower, a cell's
//! own instance's or a test's, counts for nothing. Each change is made
//! under the feed's lock, so that the signal that no cell follows the
//! draining cell is sent in order with the rest of the watch.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gdl_cells::CellState;
use tokio::sync::watch;

use super::feed::Feed;

/// A cell that follows this one, by its class and its name within the
/// class: a label of its watches.
pub(super) type Key = (String, String);

/// One watch of a follower: its label, and the run and generation of the
/// follow it belongs to.
pub(super) type WatchKey = (Key, u64, u64);

/// The watches of the cells that follow this one, and whether the cell has
/// been told that none does while it drains.
pub(super) struct Followers {
    watches: HashMap<WatchKey, Watched>,
    /// How long a watch closed without a release still counts.
    lease: Duration,
    /// Whether the cell drains and no watch has counted since: once so, it
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
    lost: Option<Instant>,
}

impl Followers {
    /// No follower, whose watches closed without a release count for
    /// `lease` after they close.
    pub(super) fn new(lease: Duration) -> Self {
        Self {
            watches: HashMap::new(),
            lease,
            unfollowed: false,
            shown: watch::Sender::new(BTreeSet::new()),
        }
    }

    /// The watch `key` opened.
    pub(super) fn followed(&mut self, key: &WatchKey) {
        let watched = self.watches.entry(key.clone()).or_insert(Watched {
            open: 0,
            lost: None,
        });
        watched.open += 1;
        watched.lost = None;
        self.show();
    }

    /// A connection of the watch `key` closed, at `now`: if it was the
    /// last, the watch still counts, until it is released or its lease
    /// passes. A watch already released counts for nothing either way.
    pub(super) fn left(&mut self, key: &WatchKey, now: Instant) {
        if let Some(watched) = self.watches.get_mut(key) {
            watched.open -= 1;
            if watched.open == 0 {
                watched.lost = Some(now);
            }
        }
    }

    /// The watch `key`'s follower let go of the cell in it, while the cell
    /// is in `state`: whether, from now on, no cell follows the draining
    /// cell. A release of a watch that is not counted changes nothing.
    pub(super) fn release(&mut self, key: &WatchKey, state: CellState) -> bool {
        if self.watches.remove(key).is_some() {
            self.show();
        }
        self.settle(state)
    }

    /// At `now`, while the cell is in `state`, stop counting each watch
    /// whose lease has passed since it closed without a release: whether,
    /// from now on, no cell follows the draining cell.
    pub(super) fn expire(&mut self, now: Instant, state: CellState) -> bool {
        let lease = self.lease;
        let before = self.watches.len();
        self.watches.retain(|_, watched| {
            watched
                .lost
                .is_none_or(|lost| now.saturating_duration_since(lost) < lease)
        });
        if self.watches.len() != before {
            self.show();
        }
        self.settle(state)
    }

    /// The cell is in `state`: whether, from now on, it drains and no watch
    /// counts, which is so only once.
    pub(super) fn settle(&mut self, state: CellState) -> bool {
        if self.unfollowed || state != CellState::Draining || !self.watches.is_empty() {
            return false;
        }
        self.unfollowed = true;
        true
    }

    /// Whether the draining cell has no follower any more.
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
    use gdl_cells::CellState;

    fn watch(run: u64) -> super::WatchKey {
        (("global".to_string(), "g1".to_string()), run, 1)
    }

    /// A draining cell is unfollowed once the last watch that counts is
    /// released, or at once if none counts; once, whatever comes after. A
    /// watch closed without a release still counts.
    #[test]
    fn a_draining_cell_is_unfollowed_once_its_last_watch_is_released() {
        let now = Instant::now();
        let mut followers = Followers::new(Duration::from_secs(30));
        followers.followed(&watch(1));
        assert!(!followers.settle(CellState::Draining), "followed");
        followers.left(&watch(1), now);
        assert!(
            !followers.settle(CellState::Draining),
            "closed, not released"
        );
        assert_eq!(followers.subscribe().borrow().len(), 1);
        assert!(followers.release(&watch(1), CellState::Draining));
        assert!(followers.unfollowed());
        assert!(followers.subscribe().borrow().is_empty());
        followers.followed(&watch(2));
        assert!(
            !followers.release(&watch(2), CellState::Draining),
            "told twice"
        );

        let mut alone = Followers::new(Duration::from_secs(30));
        assert!(!alone.settle(CellState::Ready), "not draining");
        assert!(alone.settle(CellState::Draining), "no follower at all");
    }

    /// Two watches under one follower's name count apart: with one
    /// released, the other still counts, and one closed without a release
    /// counts until its lease has passed since it closed.
    #[test]
    fn each_watch_counts_on_its_own_until_released_or_its_lease_passes() {
        let now = Instant::now();
        let lease = Duration::from_secs(30);
        let mut followers = Followers::new(lease);
        followers.followed(&watch(1));
        followers.followed(&watch(2));
        followers.left(&watch(1), now);
        assert!(
            !followers.release(&watch(2), CellState::Draining),
            "the earlier one counts"
        );
        assert!(!followers.expire(now + lease / 2, CellState::Draining));
        assert_eq!(followers.subscribe().borrow().len(), 1);
        assert!(followers.expire(now + lease, CellState::Draining));
        assert!(followers.subscribe().borrow().is_empty());
    }
}
