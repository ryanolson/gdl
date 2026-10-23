//! The cells whose hubs follow this cell, as their cell watches name them:
//! a draining cell's entry instances stop taking work from other cells
//! early once no cell follows it any more, since a follower lets go of a
//! cell only once none of its choosers holds an offer to it.
//!
//! A follower counts while it holds a watch of the cell open, and a watch
//! that names no follower, a cell's own instance's or a test's, counts for
//! nothing. Each change is made under the feed's lock, so that the signal
//! that no cell follows the draining cell is sent in order with the rest
//! of the watch.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use gdl_cells::CellState;
use tokio::sync::watch;

use super::feed::Feed;

/// A cell that follows this one, by its class and its name within the
/// class.
pub(super) type Key = (String, String);

/// The cells that follow this one, and whether the cell has been told that
/// none does while it drains.
pub(super) struct Followers {
    /// How many watches each follower holds open.
    watches: HashMap<Key, usize>,
    /// Whether the cell drains and no cell has followed it since its
    /// followers let go: once so, it stays so.
    unfollowed: bool,
    /// The followers, as they change, for whoever needs to see them.
    shown: watch::Sender<BTreeSet<Key>>,
}

impl Followers {
    pub(super) fn new() -> Self {
        Self {
            watches: HashMap::new(),
            unfollowed: false,
            shown: watch::Sender::new(BTreeSet::new()),
        }
    }

    /// `follower` opened a watch.
    pub(super) fn followed(&mut self, follower: &Key) {
        let watches = self.watches.entry(follower.clone()).or_default();
        *watches += 1;
        if *watches == 1 {
            self.show();
        }
    }

    /// One of `follower`'s watches ended, while the cell is in `state`:
    /// whether no cell follows the draining cell from now on.
    pub(super) fn left(&mut self, follower: &Key, state: CellState) -> bool {
        let Some(watches) = self.watches.get_mut(follower) else {
            return false;
        };
        *watches -= 1;
        if *watches == 0 {
            self.watches.remove(follower);
            self.show();
        }
        self.settle(state)
    }

    /// The cell is in `state`: whether, from now on, it drains and no cell
    /// follows it, which is so only once.
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

    /// The followers, as they change.
    pub(super) fn subscribe(&self) -> watch::Receiver<BTreeSet<Key>> {
        self.shown.subscribe()
    }

    fn show(&self) {
        self.shown
            .send_replace(self.watches.keys().cloned().collect());
    }
}

/// One follower's watch, counted until it is dropped with the watch.
pub(super) struct Following {
    pub(super) feed: Arc<Feed>,
    pub(super) follower: Key,
}

impl Drop for Following {
    fn drop(&mut self) {
        self.feed.unfollow(&self.follower);
    }
}

#[cfg(test)]
mod tests {
    use super::Followers;
    use gdl_cells::CellState;

    /// A draining cell is unfollowed once its last follower's last watch
    /// ends, or at once if it has none; once, whatever comes after.
    #[test]
    fn a_draining_cell_is_unfollowed_once_its_last_follower_leaves() {
        let mut followers = Followers::new();
        let global = ("global".to_string(), "g1".to_string());
        followers.followed(&global);
        followers.followed(&global);
        assert!(!followers.settle(CellState::Draining), "followed");
        assert!(
            !followers.left(&global, CellState::Draining),
            "a watch left"
        );
        assert_eq!(followers.subscribe().borrow().len(), 1);
        assert!(followers.left(&global, CellState::Draining));
        assert!(followers.unfollowed());
        assert!(followers.subscribe().borrow().is_empty());
        followers.followed(&global);
        assert!(!followers.left(&global, CellState::Draining), "told twice");

        let mut alone = Followers::new();
        assert!(!alone.settle(CellState::Ready), "not draining");
        assert!(alone.settle(CellState::Draining), "no follower at all");
    }
}
