//! The retired entry instances whose retirement the pools that follow
//! this one have yet to acknowledge: a retiree's stop waits for its
//! pool's followers to avoid it, since their choosers pick it from tables
//! this hub does not see.
//!
//! A retirement waits for the follower watches counted when it is
//! announced, in the states [`super::followers`] defines: open, closed
//! without a release, or lost. Each is settled only by an acknowledgement
//! of the retirement under its own key, or by its release, since a
//! follower that let go of the pool makes no new offer to it. A watch lost
//! without a release is never taken as either: it holds the retirement
//! until the retiree's own bound, as it holds a drain. A watch that opens
//! after the retirement is not waited for: its snapshot shows the
//! instance retired before any instance it exposes, so its choosers never
//! offer to it. A watch replaced under the same follow keeps its key, and
//! is still waited for under it.
//!
//! Each change is made under the feed's lock, so that the event that says
//! a retirement is acknowledged is sent in order with the rest of the
//! watch.

use std::collections::{HashMap, HashSet};

use velo_hub::velo::InstanceId;

use super::followers::WatchKey;

/// The retirements not yet acknowledged, each with the watches it waits
/// for.
#[derive(Default)]
pub(super) struct Retiring {
    waiting: HashMap<InstanceId, HashSet<WatchKey>>,
}

impl Retiring {
    /// `instance` retired while `counted` were the follower watches the
    /// pool counts: whether it is acknowledged at once, as it is when none
    /// is counted.
    pub(super) fn retire<'a>(
        &mut self,
        instance: InstanceId,
        counted: impl IntoIterator<Item = &'a WatchKey>,
    ) -> bool {
        let waiting: HashSet<WatchKey> = counted.into_iter().cloned().collect();
        if waiting.is_empty() {
            return true;
        }
        self.waiting.insert(instance, waiting);
        false
    }

    /// The follower of the watch `key` avoids `instance`: whether that
    /// acknowledges the retirement, which it does once every watch it
    /// waited for has. A watch it does not wait for changes nothing.
    pub(super) fn avoided(&mut self, key: &WatchKey, instance: InstanceId) -> bool {
        let Some(waiting) = self.waiting.get_mut(&instance) else {
            return false;
        };
        if !waiting.remove(key) || !waiting.is_empty() {
            return false;
        }
        self.waiting.remove(&instance);
        true
    }

    /// The watch `key` was released: the retirements that are acknowledged
    /// now that it no longer holds them. Costs one look per retirement
    /// waiting, which is at most the pool's retired entry instances.
    pub(super) fn released(&mut self, key: &WatchKey) -> Vec<InstanceId> {
        let mut acknowledged = Vec::new();
        self.waiting.retain(|instance, waiting| {
            if waiting.remove(key) && waiting.is_empty() {
                acknowledged.push(*instance);
                return false;
            }
            true
        });
        acknowledged
    }

    /// `instance` left the hub: its retirement waits for nothing more.
    pub(super) fn left(&mut self, instance: InstanceId) {
        self.waiting.remove(&instance);
    }
}

#[cfg(test)]
mod tests {
    use super::Retiring;
    use velo_hub::velo::InstanceId;

    fn watch(run: u64) -> super::WatchKey {
        (("global".to_string(), "g1".to_string()), run, 1)
    }

    /// With no watch counted, a retirement is acknowledged at once; with
    /// two, once both have acknowledged it or been released, and an
    /// acknowledgement under another key, or of another instance, changes
    /// nothing.
    #[test]
    fn a_retirement_waits_for_each_watch_counted_when_it_was_announced() {
        let mut retiring = Retiring::default();
        let (alone, waited, released) = (
            InstanceId::new_v4(),
            InstanceId::new_v4(),
            InstanceId::new_v4(),
        );
        assert!(retiring.retire(alone, []), "no follower");
        assert!(!retiring.avoided(&watch(1), alone), "acknowledged already");

        assert!(!retiring.retire(waited, [&watch(1), &watch(2)]));
        assert!(!retiring.retire(released, [&watch(1)]));
        assert!(!retiring.avoided(&watch(3), waited), "not waited for");
        assert!(!retiring.avoided(&watch(1), InstanceId::new_v4()));
        assert!(!retiring.avoided(&watch(1), waited), "one of two");
        assert!(!retiring.avoided(&watch(1), waited), "twice");
        assert_eq!(retiring.released(&watch(1)), [released]);
        assert!(retiring.released(&watch(1)).is_empty(), "released twice");
        assert!(retiring.avoided(&watch(2), waited));
    }

    /// A retiree that leaves is no longer waited for: a later release
    /// acknowledges nothing for it.
    #[test]
    fn a_retiree_that_left_waits_for_nothing() {
        let mut retiring = Retiring::default();
        let gone = InstanceId::new_v4();
        assert!(!retiring.retire(gone, [&watch(1)]));
        retiring.left(gone);
        assert!(retiring.released(&watch(1)).is_empty());
    }
}
