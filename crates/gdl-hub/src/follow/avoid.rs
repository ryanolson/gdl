//! Which choosers have yet to avoid a followed pool's retired entry
//! instance: what lets the hub tell the pool's hub, once for its whole
//! pool, that none of its choosers picks the instance again, so that the
//! retiree's stop need not wait out its bound.
//!
//! A retirement waits for the choosers that, when the relay applies it,
//! have said they run a follower and route to the instance's segment: the
//! hub's members of the segments whose hops enter that segment from the
//! pool's class. A chooser that says so later counts as having avoided it
//! at once: it says so before it first watches the relay, whose snapshot
//! shows the instance retired before any instance it exposes, so it never
//! picks it. One that leaves, or whose registration is replaced, counts as
//! having avoided it too. A retirement shown in the snapshot a follow is
//! installed from is not waited for here: the pool's hub did not wait for
//! that follow's watch either, since it began after the retirement.

use std::collections::{HashMap, HashSet};

use velo_hub::velo::InstanceId;

/// One follow's retirements that some chooser has yet to avoid, each with
/// those choosers.
#[derive(Debug, Default)]
pub(super) struct Avoiding {
    waiting: HashMap<InstanceId, HashSet<InstanceId>>,
}

impl Avoiding {
    /// `instance` retired while `choosers` route to its segment: whether
    /// every chooser avoids it at once, as when there is none.
    pub(super) fn retire(&mut self, instance: InstanceId, choosers: HashSet<InstanceId>) -> bool {
        if choosers.is_empty() {
            return true;
        }
        self.waiting.insert(instance, choosers);
        false
    }

    /// `chooser` avoids `instance`: whether every chooser now does. One
    /// the retirement does not wait for changes nothing.
    pub(super) fn avoids(&mut self, instance: InstanceId, chooser: InstanceId) -> bool {
        let Some(waiting) = self.waiting.get_mut(&instance) else {
            return false;
        };
        if !waiting.remove(&chooser) || !waiting.is_empty() {
            return false;
        }
        self.waiting.remove(&instance);
        true
    }

    /// `chooser` left, or no longer counts: the retirements every chooser
    /// now avoids.
    pub(super) fn left(&mut self, chooser: InstanceId) -> Vec<InstanceId> {
        let mut avoided = Vec::new();
        self.waiting.retain(|instance, waiting| {
            if waiting.remove(&chooser) && waiting.is_empty() {
                avoided.push(*instance);
                return false;
            }
            true
        });
        avoided
    }

    /// `instance` left its pool's hub: whether its retirement was waited
    /// for, which it is no more. Its pool's hub forgot it as it left, and a
    /// chooser never shown it again could not answer it.
    pub(super) fn gone(&mut self, instance: InstanceId) -> bool {
        self.waiting.remove(&instance).is_some()
    }

    /// The retirements waited for.
    pub(super) fn waiting(&self) -> impl Iterator<Item = InstanceId> + '_ {
        self.waiting.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::Avoiding;
    use velo_hub::velo::InstanceId;

    /// A retirement no chooser routes to is avoided at once; one two do,
    /// once both have avoided it or left, whatever else avoids it.
    #[test]
    fn a_retirement_waits_for_the_choosers_that_route_to_it() {
        let (retired, other) = (InstanceId::new_v4(), InstanceId::new_v4());
        let (first, second, stranger) = (
            InstanceId::new_v4(),
            InstanceId::new_v4(),
            InstanceId::new_v4(),
        );
        let mut avoiding = Avoiding::default();
        assert!(avoiding.retire(other, HashSet::new()), "no chooser");
        assert!(!avoiding.retire(retired, HashSet::from([first, second])));
        assert!(!avoiding.retire(other, HashSet::from([second])));
        assert!(!avoiding.avoids(retired, stranger), "not waited for");
        assert!(!avoiding.avoids(retired, first));
        assert!(!avoiding.avoids(retired, first), "twice");
        let mut avoided = avoiding.left(second);
        avoided.sort_by_key(|id| id.as_u128());
        let mut both = [other, retired];
        both.sort_by_key(|id| id.as_u128());
        assert_eq!(avoided, both, "the one that left avoids both");
        assert!(avoiding.left(second).is_empty(), "left twice");
        assert_eq!(avoiding.waiting().count(), 0);
    }

    /// A retiree that leaves is waited for no more, and an answer for it
    /// after that changes nothing.
    #[test]
    fn a_retiree_that_leaves_is_forgotten() {
        let (retired, chooser) = (InstanceId::new_v4(), InstanceId::new_v4());
        let mut avoiding = Avoiding::default();
        assert!(!avoiding.retire(retired, HashSet::from([chooser])));
        assert!(avoiding.gone(retired));
        assert!(!avoiding.gone(retired), "gone twice");
        assert!(!avoiding.avoids(retired, chooser));
        assert_eq!(avoiding.waiting().count(), 0);
    }
}
