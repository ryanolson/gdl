//! Whether a follow is offered to the hub's choosers, and who has
//! acknowledged that it is not: what lets the hub close a follow without a
//! timer on its offers. gdl bounds no request's lifetime, so no wait of
//! fixed length would know that nothing offered to the cell is still
//! running; only the choosers that hold the offers do.
//!
//! An acknowledgement answers the revision it names and no other: every
//! change between offered and not offered is a new revision, and forgets
//! the acknowledgements of the one before. A chooser that acknowledged a
//! revision, and has not yet applied the offer after it when a later
//! revision makes the follow not offered again, has acknowledged nothing
//! that counts for the later one.
//!
//! The one timer is for a chooser that cannot answer: one that has had no
//! watch of the relay open for the hub's bound (`[cell] lost_chooser_ms`)
//! cannot learn that a follow is not offered, so it is not waited for
//! until it watches again. One that watches is waited for however long
//! its offers last. Closing the follow cuts nothing either way: the
//! cell's entry instances finish what they hold before they leave.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

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

    /// `chooser` left the hub: what it acknowledged is forgotten.
    pub(super) fn left(&mut self, chooser: InstanceId) {
        self.acknowledged.remove(&chooser);
    }

    /// Whether the follow may close: it is not offered, and each chooser
    /// of its class the hub still waits for (`waited`) acknowledged its
    /// revision. What a chooser no longer waited for acknowledged counts
    /// for nothing, so a connected chooser that holds an offer keeps the
    /// follow open whoever else has gone.
    pub(super) fn closes(&self, mut waited: impl Iterator<Item = InstanceId>) -> bool {
        !self.offered && waited.all(|chooser| self.acknowledged.contains(&chooser))
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
    /// For each segment that chooses, the entry segments its hops enter.
    entries: HashMap<String, Arc<[String]>>,
    members: HashMap<InstanceId, Chooser>,
    /// The watches of the relay each instance holds open, by the id the
    /// watch names, and since when it has had none: kept apart from the
    /// members, since a watch may open before its registration is
    /// committed and close after it is removed.
    watching: HashMap<InstanceId, Watching>,
}

/// One instance's watches of the relay.
#[derive(Debug, Clone, Copy)]
struct Watching {
    open: usize,
    /// When its last watch closed, or when it declared with none open.
    since: Instant,
}

#[derive(Debug)]
struct Chooser {
    incarnation: Incarnation,
    epoch: RegistrationEpoch,
    classes: Arc<[String]>,
    entries: Arc<[String]>,
    /// Whether it said that it runs a follower, in this registration.
    declared: bool,
}

impl Choosers {
    /// The choosers of the segments `segments` names, each with the
    /// classes it chooses cells of and the entry segments of those classes
    /// its hops enter.
    pub(crate) fn new(
        segments: impl IntoIterator<Item = (String, Vec<String>, Vec<String>)>,
    ) -> Self {
        let mut choosers = Self::default();
        for (segment, classes, entries) in segments {
            choosers.segments.insert(segment.clone(), classes.into());
            choosers.entries.insert(segment, entries.into());
        }
        choosers
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
        let entries = self.entries.get(segment).cloned().unwrap_or_default();
        let renewed = self.members.insert(
            id,
            Chooser {
                incarnation,
                epoch,
                classes: Arc::clone(classes),
                entries,
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
                return Some(renewed.classes);
            }
        }
        None
    }

    /// `id`, in its registration of `epoch`, says at `now` that it runs a
    /// follower of the hub's relay: from now it counts as a chooser of its
    /// classes. With no watch of the relay open, its bound runs from now,
    /// since it watches only after it declares. Refused for an instance
    /// that is not a member of a chooser's segment in that registration.
    pub(super) fn declare(
        &mut self,
        id: InstanceId,
        epoch: RegistrationEpoch,
        now: Instant,
    ) -> bool {
        let Some(chooser) = self
            .members
            .get_mut(&id)
            .filter(|chooser| chooser.epoch == epoch)
        else {
            return false;
        };
        if !std::mem::replace(&mut chooser.declared, true) {
            let watching = self.watching.entry(id).or_insert(Watching {
                open: 0,
                since: now,
            });
            if watching.open == 0 {
                watching.since = now;
            }
        }
        true
    }

    /// `id` opened a watch of the relay: it is waited for again, if it
    /// was not.
    pub(super) fn opened(&mut self, id: InstanceId, now: Instant) {
        self.watching
            .entry(id)
            .or_insert(Watching {
                open: 0,
                since: now,
            })
            .open += 1;
    }

    /// A watch of the relay that `id` opened closed at `now`: with none
    /// left open, its bound runs from now.
    pub(super) fn closed(&mut self, id: InstanceId, now: Instant) {
        let Some(watching) = self.watching.get_mut(&id) else {
            return;
        };
        watching.open -= 1;
        if watching.open == 0 {
            watching.since = now;
            if !self.members.contains_key(&id) {
                self.watching.remove(&id);
            }
        }
    }

    /// Whether the declared chooser `id` is waited for at `now`: it has a
    /// watch of the relay open, or had one less than `bound` ago, or
    /// declared less than `bound` ago.
    fn watches(&self, id: InstanceId, now: Instant, bound: Duration) -> bool {
        self.watching
            .get(&id)
            .is_some_and(|watching| watching.open > 0 || now < watching.since + bound)
    }

    /// The choosers of `class` the hub waits for at `now`: the members
    /// that said they run a follower and that watch the relay, or stopped
    /// less than `bound` ago. Costs one look per chooser of the hub.
    pub(super) fn waited<'a>(
        &'a self,
        class: &'a str,
        now: Instant,
        bound: Duration,
    ) -> impl Iterator<Item = InstanceId> + 'a {
        self.members
            .iter()
            .filter(move |(id, chooser)| {
                chooser.declared
                    && chooser.classes.iter().any(|of| of == class)
                    && self.watches(**id, now, bound)
            })
            .map(|(id, _)| *id)
    }

    /// When each declared chooser with no watch of the relay open stops
    /// being waited for.
    fn bounds(&self, bound: Duration) -> impl Iterator<Item = Instant> + '_ {
        self.members
            .iter()
            .filter(|(_, chooser)| chooser.declared)
            .filter_map(|(id, _)| self.watching.get(id))
            .filter(|watching| watching.open == 0)
            .map(move |watching| watching.since + bound)
    }

    /// When the next chooser stops being waited for, after `now`, if one
    /// will.
    pub(super) fn next_lost(&self, now: Instant, bound: Duration) -> Option<Instant> {
        self.bounds(bound).filter(|at| *at > now).min()
    }

    /// Whether a chooser stopped being waited for after `since` and by
    /// `now`.
    pub(super) fn lost_since(&self, since: Instant, now: Instant, bound: Duration) -> bool {
        self.bounds(bound).any(|at| at > since && at <= now)
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
        if self
            .watching
            .get(&removal.id)
            .is_some_and(|watching| watching.open == 0)
        {
            self.watching.remove(&removal.id);
        }
        chooser.declared.then_some(chooser.classes)
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

    /// The members, among those that said they run a follower, that choose
    /// cells of `class` and route to their entry segment `segment`: what a
    /// retirement of an instance of the segment waits for. Costs one look
    /// per chooser of the hub.
    pub(super) fn routing_to(&self, class: &str, segment: &str) -> HashSet<InstanceId> {
        self.members
            .iter()
            .filter(|(_, chooser)| {
                chooser.declared
                    && chooser.classes.iter().any(|of| of == class)
                    && chooser.entries.iter().any(|entry| entry == segment)
            })
            .map(|(id, _)| *id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Choosers, Offer};
    use tokio::time::Instant;
    use velo_hub::protocol::RegistrationEpoch;
    use velo_hub::store::{Incarnation, Removal};
    use velo_hub::velo::InstanceId;

    /// The hub's bound on a chooser with no watch of the relay, in these
    /// tests.
    const BOUND: Duration = Duration::from_secs(30);

    fn none() -> std::iter::Empty<InstanceId> {
        std::iter::empty()
    }

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
        assert!(!offer.closes([chooser].into_iter()));
        assert!(offer.acknowledge(chooser, 4));
        assert!(offer.closes([chooser].into_iter()));
        assert!(offer.set(true));
        assert!(!offer.closes(none()), "an offered follow never closes");
    }

    /// A chooser that leaves is no longer waited for, and its
    /// acknowledgement no longer counts for another's.
    #[test]
    fn a_chooser_that_leaves_is_not_waited_for() {
        let (gone, staying) = (InstanceId::new_v4(), InstanceId::new_v4());
        let mut offer = Offer::new(false);
        assert!(offer.closes(none()), "no chooser: closed as it is marked");
        assert!(offer.acknowledge(gone, 1));
        offer.left(gone);
        assert!(
            !offer.closes([staying].into_iter()),
            "the leaver's acknowledgement counted"
        );
        assert!(offer.acknowledge(staying, 1));
        assert!(offer.closes([staying].into_iter()));
    }

    /// A chooser that acknowledged and is then no longer waited for, its
    /// watch lost, does not stand in for a connected chooser that holds an
    /// offer: the follow closes only once that one acknowledges too.
    #[test]
    fn a_lost_choosers_acknowledgement_closes_nothing_for_a_connected_one() {
        let (lost, holding) = (InstanceId::new_v4(), InstanceId::new_v4());
        let mut offer = Offer::new(false);
        assert!(offer.acknowledge(lost, 1));
        assert!(
            !offer.closes([holding].into_iter()),
            "closed on the connected chooser that holds an offer"
        );
        assert!(offer.acknowledge(holding, 1));
        assert!(offer.closes([holding].into_iter()));
    }

    /// A member of a segment that chooses counts for each class it
    /// chooses once it declares that it runs a follower, once however often
    /// it declares or renews its registration, and until its last
    /// incarnation is removed; a member of another segment never counts.
    #[test]
    fn the_choosers_of_a_class_are_the_members_of_its_segments() {
        let mut choosers = Choosers::new([(
            "a".to_string(),
            vec!["model_x".to_string()],
            vec!["f".to_string()],
        )]);
        let now = Instant::now();
        let waited = |choosers: &Choosers| -> Vec<InstanceId> {
            choosers.waited("model_x", now, BOUND).collect()
        };
        let (id, other) = (InstanceId::new_v4(), InstanceId::new_v4());
        let epoch = RegistrationEpoch::new();
        let (first, second) = (Incarnation::from_u64(1), Incarnation::from_u64(2));
        choosers.joined("a", id, first, epoch);
        choosers.joined("b", other, first, epoch);
        assert!(waited(&choosers).is_empty(), "counted before it declared");
        assert!(!choosers.chooses(id, epoch, "model_x"));
        assert!(
            !choosers.declare(other, epoch, now),
            "not of a chooser's segment"
        );
        assert!(
            !choosers.declare(id, RegistrationEpoch::new(), now),
            "another registration"
        );
        assert!(choosers.declare(id, epoch, now));
        assert!(choosers.declare(id, epoch, now), "twice");
        assert_eq!(choosers.routing_to("model_x", "f"), [id].into());
        assert!(
            choosers.routing_to("model_x", "d").is_empty(),
            "no hop enters d"
        );
        choosers.joined("a", id, second, epoch);
        assert_eq!(waited(&choosers), [id]);
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
        assert!(waited(&choosers).is_empty());
    }

    /// One declared chooser of `model_x`.
    fn one() -> (Choosers, InstanceId, Instant) {
        let mut choosers = Choosers::new([(
            "a".to_string(),
            vec!["model_x".to_string()],
            vec!["f".to_string()],
        )]);
        let (id, epoch, at) = (
            InstanceId::new_v4(),
            RegistrationEpoch::new(),
            Instant::now(),
        );
        choosers.joined("a", id, Incarnation::from_u64(1), epoch);
        assert!(choosers.declare(id, epoch, at));
        (choosers, id, at)
    }

    fn is_waited(choosers: &Choosers, now: Instant) -> bool {
        choosers.waited("model_x", now, BOUND).next().is_some()
    }

    /// A chooser that declared and never got a watch of the relay open, as
    /// one refused at the relay's cap, is waited for until the bound has
    /// passed since it declared, and no longer.
    #[test]
    fn a_chooser_that_never_watches_is_waited_for_until_the_bound() {
        let (choosers, _, declared) = one();
        assert!(is_waited(&choosers, declared + BOUND / 2));
        assert_eq!(choosers.next_lost(declared, BOUND), Some(declared + BOUND));
        assert!(!is_waited(&choosers, declared + BOUND));
        assert!(choosers.lost_since(declared, declared + BOUND, BOUND));
        assert!(
            !choosers.lost_since(declared + BOUND, declared + BOUND * 2, BOUND),
            "lost once, not again"
        );
        assert_eq!(choosers.next_lost(declared + BOUND, BOUND), None);
    }

    /// A chooser that watches is waited for however long it watches; once
    /// its last watch closes, for the bound from then; one that watches
    /// again within the bound, or after it, is waited for again.
    #[test]
    fn a_chooser_is_waited_for_while_it_watches_and_for_the_bound_after() {
        let (mut choosers, id, at) = one();
        choosers.opened(id, at);
        choosers.opened(id, at);
        let later = at + BOUND * 10;
        assert!(is_waited(&choosers, later), "a watching chooser was lost");
        assert_eq!(choosers.next_lost(later, BOUND), None);
        choosers.closed(id, later);
        assert!(is_waited(&choosers, later + BOUND * 2), "one watch is open");
        let closed = later + BOUND * 2;
        choosers.closed(id, closed);
        assert_eq!(choosers.next_lost(closed, BOUND), Some(closed + BOUND));
        let back = closed + BOUND / 2;
        choosers.opened(id, back);
        assert!(
            is_waited(&choosers, closed + BOUND * 2),
            "came back in time"
        );
        assert!(!choosers.lost_since(closed, closed + BOUND * 2, BOUND));
        let gone = closed + BOUND * 3;
        choosers.closed(id, gone);
        assert!(!is_waited(&choosers, gone + BOUND));
        choosers.opened(id, gone + BOUND * 2);
        assert!(
            is_waited(&choosers, gone + BOUND * 2),
            "not waited for once back"
        );
    }

    /// A watch that names an instance that is no member is forgotten once
    /// it closes, and so is the record of a member's watches once it is
    /// removed with none open.
    #[test]
    fn watches_are_forgotten_with_their_instance() {
        let (mut choosers, id, at) = one();
        let stranger = InstanceId::new_v4();
        choosers.opened(stranger, at);
        choosers.closed(stranger, at);
        assert!(!choosers.watching.contains_key(&stranger));
        choosers.removed(&Removal {
            id,
            incarnation: Incarnation::from_u64(1),
        });
        assert!(choosers.watching.is_empty());
    }
}
