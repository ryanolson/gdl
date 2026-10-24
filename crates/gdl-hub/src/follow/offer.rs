//! Whether a follow is offered to the hub's choosers, and who has
//! acknowledged that it is not: what lets the hub close a follow without a
//! timer. gdl bounds no request's lifetime, so no wait of fixed length
//! would know that nothing offered to the cell is still running; only the
//! choosers that hold the offers do.
//!
//! An acknowledgement answers the revision it names and no other: every
//! change between offered and not offered is a new revision, and forgets
//! the acknowledgements of the one before. A chooser that acknowledged a
//! revision, and has not yet applied the offer after it when a later
//! revision makes the follow not offered again, has acknowledged nothing
//! that counts for the later one.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use gdl_watch::FollowEvent;
use velo_hub::protocol::RegistrationEpoch;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::InstanceId;

/// One follow's offered state: its revision, whether it is offered, and,
/// while it is not, the choosers that acknowledged the revision.
#[derive(Debug)]
pub(super) struct Offer {
    revision: u64,
    offered: bool,
    acknowledged: HashSet<InstanceId>,
}

impl Offer {
    /// A follow as it is installed, at revision 1.
    pub(super) fn new(offered: bool) -> Self {
        Self {
            revision: 1,
            offered,
            acknowledged: HashSet::new(),
        }
    }

    pub(super) fn offered(&self) -> bool {
        self.offered
    }

    /// Make the follow offered or not: whether that is a change, which is
    /// the next revision and forgets every acknowledgement.
    pub(super) fn set(&mut self, offered: bool) -> bool {
        if self.offered == offered {
            return false;
        }
        self.offered = offered;
        self.revision += 1;
        self.acknowledged.clear();
        true
    }

    /// `chooser` acknowledged `revision`: whether it counts, which it does
    /// only for the current revision while the follow is not offered.
    pub(super) fn acknowledge(&mut self, chooser: InstanceId, revision: u64) -> bool {
        !self.offered && revision == self.revision && self.acknowledged.insert(chooser)
    }

    /// `chooser` left the hub: it no longer needs to acknowledge, and an
    /// acknowledgement it made no longer counts toward `closes`' number.
    pub(super) fn left(&mut self, chooser: InstanceId) {
        self.acknowledged.remove(&chooser);
    }

    /// Whether the follow may close: it is not offered, and each of the
    /// `choosers` of its class acknowledged its revision. Every chooser
    /// that acknowledged is one of them, since one that leaves is taken
    /// out, so counting is enough.
    pub(super) fn closes(&self, choosers: usize) -> bool {
        !self.offered && self.acknowledged.len() >= choosers
    }

    /// Whether a snapshot shows the offered state: only while the follow
    /// is not offered. An earlier reader, which does not know the kind,
    /// fails closed for the cell until a snapshot of it shows none, so a
    /// follow offered again must show none; a reader of this build takes a
    /// follow shown without one as offered, and applies every later
    /// revision, which is greater than the one it then holds.
    pub(super) fn shown(&self) -> bool {
        !self.offered
    }

    /// The relay's event of the offered state of the follow `generation`
    /// of the cell `cell` of `class`.
    pub(super) fn event(&self, class: &str, cell: &str, generation: u64) -> FollowEvent {
        FollowEvent::Offering {
            class: class.to_string(),
            cell: cell.to_string(),
            generation,
            revision: self.revision,
            offered: self.offered,
        }
    }
}

/// The hub's members that choose cells of the classes its cell sends to,
/// and must acknowledge a follow of a class before it closes: those of the
/// segments holding the outer end of a hop that crosses into the class,
/// once they have said that they run a follower of the hub's relay. The
/// other members never offer anything to another class, and neither does
/// one of those segments that runs no follower, as an instance without
/// `[cell]` does: it never acknowledges, and must not hold a follow open.
#[derive(Debug, Default)]
pub(crate) struct Choosers {
    /// For each segment that chooses, the classes it chooses cells of.
    segments: HashMap<String, Arc<[String]>>,
    members: HashMap<InstanceId, Chooser>,
    /// How many members choose cells of each class.
    counts: HashMap<String, usize>,
}

#[derive(Debug)]
struct Chooser {
    incarnation: Incarnation,
    epoch: RegistrationEpoch,
    classes: Arc<[String]>,
    /// Whether it said that it runs a follower, in this registration.
    declared: bool,
}

impl Choosers {
    /// The choosers of the segments `segments` names, each with the
    /// classes it chooses cells of.
    pub(crate) fn new(segments: impl IntoIterator<Item = (String, Vec<String>)>) -> Self {
        Self {
            segments: segments
                .into_iter()
                .map(|(segment, classes)| (segment, classes.into()))
                .collect(),
            ..Self::default()
        }
    }

    /// The hub committed a registration of `id` in `segment`: the
    /// classes it no longer counts for, if it had declared in a
    /// registration this one replaces.
    pub(super) fn joined(
        &mut self,
        segment: &str,
        id: InstanceId,
        incarnation: Incarnation,
        epoch: RegistrationEpoch,
    ) -> Option<Arc<[String]>> {
        let classes = self.segments.get(segment)?;
        let renewed = self.members.insert(
            id,
            Chooser {
                incarnation,
                epoch,
                classes: Arc::clone(classes),
                declared: false,
            },
        );
        // A registration that renews the instance's keeps its declaration;
        // a new one is not counted until it declares again.
        if let Some(renewed) = renewed
            && renewed.declared
        {
            if renewed.epoch == epoch {
                self.members.get_mut(&id).expect("just inserted").declared = true;
            } else {
                self.uncount(&renewed.classes);
                return Some(renewed.classes);
            }
        }
        None
    }

    /// `id`, in its registration of `epoch`, says that it runs a follower
    /// of the hub's relay: from now it counts as a chooser of its classes.
    /// Refused for an instance that is not a member of a chooser's segment
    /// in that registration.
    pub(super) fn declare(&mut self, id: InstanceId, epoch: RegistrationEpoch) -> bool {
        let Some(chooser) = self
            .members
            .get_mut(&id)
            .filter(|chooser| chooser.epoch == epoch)
        else {
            return false;
        };
        if !std::mem::replace(&mut chooser.declared, true) {
            for class in chooser.classes.iter() {
                *self.counts.entry(class.clone()).or_default() += 1;
            }
        }
        true
    }

    fn uncount(&mut self, classes: &[String]) {
        for class in classes {
            if let Some(count) = self.counts.get_mut(class) {
                *count -= 1;
            }
        }
    }

    /// The hub removed an incarnation of an instance: the classes it
    /// chose, if it was a chooser and no later registration of it has
    /// been committed.
    pub(super) fn removed(&mut self, removal: &Removal) -> Option<Arc<[String]>> {
        if self
            .members
            .get(&removal.id)
            .is_none_or(|chooser| chooser.incarnation > removal.incarnation)
        {
            return None;
        }
        let chooser = self.members.remove(&removal.id)?;
        if !chooser.declared {
            return None;
        }
        self.uncount(&chooser.classes);
        Some(chooser.classes)
    }

    /// Whether `id`, in its registration of `epoch`, chooses cells of
    /// `class`.
    pub(super) fn chooses(&self, id: InstanceId, epoch: RegistrationEpoch, class: &str) -> bool {
        self.members.get(&id).is_some_and(|chooser| {
            chooser.declared
                && chooser.epoch == epoch
                && chooser.classes.iter().any(|of| of == class)
        })
    }

    /// How many members choose cells of `class`.
    pub(super) fn count(&self, class: &str) -> usize {
        self.counts.get(class).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::{Choosers, Offer};
    use velo_hub::protocol::RegistrationEpoch;
    use velo_hub::store::{Incarnation, Removal};
    use velo_hub::velo::InstanceId;

    /// Not offered at `r`, offered at `r + 1`, not offered at `r + 2`: an
    /// acknowledgement of `r` that arrives after `r + 2` counts for
    /// nothing, and one of `r + 2` counts.
    #[test]
    fn an_acknowledgement_counts_only_for_its_own_revision() {
        let chooser = InstanceId::new_v4();
        let mut offer = Offer::new(true);
        assert!(!offer.acknowledge(chooser, 1), "offered: nothing to answer");
        assert!(offer.set(false));
        assert!(!offer.set(false), "no change, no revision");
        assert!(offer.set(true));
        assert!(offer.set(false));
        assert!(!offer.acknowledge(chooser, 2), "an earlier revision");
        assert!(!offer.closes(1));
        assert!(offer.acknowledge(chooser, 4));
        assert!(offer.closes(1));
        assert!(offer.set(true));
        assert!(!offer.closes(0), "an offered follow never closes");
    }

    /// A chooser that leaves is no longer waited for, and its
    /// acknowledgement no longer counts for another's.
    #[test]
    fn a_chooser_that_leaves_is_not_waited_for() {
        let (gone, staying) = (InstanceId::new_v4(), InstanceId::new_v4());
        let mut offer = Offer::new(false);
        assert!(offer.closes(0), "no chooser: closed as it is marked");
        assert!(offer.acknowledge(gone, 1));
        offer.left(gone);
        assert!(!offer.closes(1), "the leaver's acknowledgement counted");
        assert!(offer.acknowledge(staying, 1));
        assert!(offer.closes(1));
    }

    /// A member of a segment that chooses counts for each class it
    /// chooses once it declares that it runs a follower, once however often
    /// it declares or renews its registration, and until its last
    /// incarnation is removed; a member of another segment never counts.
    #[test]
    fn the_choosers_of_a_class_are_the_members_of_its_segments() {
        let mut choosers = Choosers::new([("a".to_string(), vec!["model_x".to_string()])]);
        let (id, other) = (InstanceId::new_v4(), InstanceId::new_v4());
        let epoch = RegistrationEpoch::new();
        let (first, second) = (Incarnation::from_u64(1), Incarnation::from_u64(2));
        choosers.joined("a", id, first, epoch);
        choosers.joined("b", other, first, epoch);
        assert_eq!(choosers.count("model_x"), 0, "counted before it declared");
        assert!(!choosers.chooses(id, epoch, "model_x"));
        assert!(
            !choosers.declare(other, epoch),
            "not of a chooser's segment"
        );
        assert!(
            !choosers.declare(id, RegistrationEpoch::new()),
            "another registration"
        );
        assert!(choosers.declare(id, epoch));
        assert!(choosers.declare(id, epoch), "twice");
        choosers.joined("a", id, second, epoch);
        assert_eq!(choosers.count("model_x"), 1);
        assert!(choosers.chooses(id, epoch, "model_x"));
        assert!(!choosers.chooses(id, RegistrationEpoch::new(), "model_x"));
        assert!(!choosers.chooses(other, epoch, "model_x"));
        let earlier = Removal {
            id,
            incarnation: first,
        };
        assert!(choosers.removed(&earlier).is_none(), "a later one holds it");
        let last = Removal {
            id,
            incarnation: second,
        };
        assert!(choosers.removed(&last).is_some());
        assert_eq!(choosers.count("model_x"), 0);
    }
}
