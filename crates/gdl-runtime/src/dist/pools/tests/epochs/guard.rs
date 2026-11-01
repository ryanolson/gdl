//! The epoch guard: seeded, random interleavings of what a parent hub's
//! relay sends a chooser about one pool, through the follower as it runs,
//! checked after every step. A pool's hubs come and go (a record of a
//! later epoch, a follow that never installs, a lost link, a lapsed
//! record, a scale to 0, a withdrawal and a relist), the relay restarts
//! and numbers its follows from 1 again, the relay's watch and a pool's
//! own watch deliver in any order relative to each other, each watch is
//! opened again with a snapshot, whole or not, and offers are taken and
//! accepted, as `still_eligible` would, from the row that shows the chosen
//! instance when the lease is taken. A seed of every fourth run is a relay
//! of an earlier build, which sends no epoch.
//!
//! After every step it checks two invariants:
//!
//! - (a) the evidence a policy reads of the pool pairs each state with
//!   the epoch of a hub that said it: a condition, a drain, or a
//!   withdrawal is never shown with the epoch of a hub that did not;
//! - (b) an acceptance's epoch is never older than the epoch of the row
//!   whose follow took the offer;
//! - (c) the evidence is never of an earlier hub than the row beside it,
//!   and never unknown beside it, as a policy reads both from one table,
//!   with one exception: after an unlisting that found evidence of another
//!   hub than the follow shown, or none, the pool may read unknown beside
//!   that row until a later word;
//! - (d) evidence of a hub that the follower has heard drain, and not
//!   withdraw, never reads not draining: a drain lasts for its hub's life.
//!   Unknown is allowed. What the follower has heard is kept from the walk,
//!   as each watch delivers it. Not for a relay of an earlier build, which
//!   dates every hub 0.
//!
//! On a failure it prints the seed and the steps. `GDL_EPOCH_GUARD_SEEDS`
//! sets how many seeds run (a soak), and `GDL_EPOCH_GUARD_SEED` runs one.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gdl::{PoolCondition, PoolRequest};
use gdl_pools::PoolState;
use gdl_watch::{FollowEvent, Listing, UnknownFollowEvent, WatchEnd, Watched};
use tokio::time::Instant;
use velo::PeerInfo;

use super::super::super::Pools;
use super::super::super::follow::{Asked, Follows};
use super::super::super::lease::PoolLease;
use super::super::{M1, Recorded, follower, peer};

/// The seeds the suite runs, and the steps of each.
const SEEDS: u64 = 1024;
const STEPS: usize = 400;

/// splitmix64: a generator a seed reproduces on every host.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// What the hub has said of one of the pool's hubs, by the epoch on the
/// wire.
#[derive(Default)]
struct Said {
    conditions: Vec<PoolCondition>,
    drained: bool,
    withdrawn: bool,
}

/// The parent hub's follow of the pool.
#[derive(Clone, Copy)]
struct Following {
    generation: u64,
    epoch: u64,
    installed: bool,
    state: PoolState,
}

/// The parent hub, as a correct relay behaves: the truth the follower is
/// checked against.
struct Hub {
    /// A relay of an earlier build: no epoch on the wire.
    earlier: bool,
    /// The epoch of the pool's last write.
    epoch: u64,
    /// The relay's numbering of its follows, from 1 after each restart.
    generation: u64,
    follow: Option<Following>,
    /// What the relay lists: epoch on the wire, condition, drain.
    listing: Option<(u64, PoolCondition, bool)>,
    withdrawn: bool,
    said: HashMap<u64, Said>,
}

/// The pool's one entry instance, the same across its hubs, so that a
/// later hub's follow can show the instance an offer chose.
fn entry() -> &'static PeerInfo {
    static ENTRY: std::sync::OnceLock<PeerInfo> = std::sync::OnceLock::new();
    ENTRY.get_or_init(peer)
}

impl Hub {
    fn wire(&self, epoch: u64) -> u64 {
        if self.earlier { 0 } else { epoch }
    }

    fn listing(&self) -> Option<Listing> {
        self.listing.map(|(epoch, condition, draining)| Listing {
            region: "r".into(),
            condition,
            properties: gdl_watch::PropertyList::default(),
            missing_age_ms: None,
            draining,
            epoch,
        })
    }

    /// Note what `event` says of the hub it is about.
    fn note(&mut self, event: &FollowEvent) {
        let mut say = |epoch: u64, condition: Option<PoolCondition>, drained: bool, gone: bool| {
            let said = self.said.entry(epoch).or_default();
            if let Some(condition) = condition
                && !said.conditions.contains(&condition)
            {
                said.conditions.push(condition);
            }
            said.drained |= drained;
            said.withdrawn |= gone;
        };
        match event {
            FollowEvent::Listed {
                listing: Some(listing),
                ..
            } => say(
                listing.epoch,
                Some(listing.condition),
                listing.draining,
                false,
            ),
            FollowEvent::Follow {
                epoch,
                state,
                listing,
                ..
            } => {
                if let Some(listing) = listing {
                    say(
                        listing.epoch,
                        Some(listing.condition),
                        listing.draining,
                        false,
                    );
                }
                say(
                    *epoch,
                    Some(PoolCondition::Live),
                    *state == PoolState::Draining,
                    false,
                );
            }
            FollowEvent::Withdrawn { epoch, .. } => say(*epoch, None, false, true),
            _ => {}
        }
    }

    /// The follow event of the current follow, with `listing`.
    fn follow_event(&self, follow: &Following, listing: Option<Listing>) -> FollowEvent {
        FollowEvent::Follow {
            class: "model_x".into(),
            pool: M1.into(),
            generation: follow.generation,
            epoch: self.wire(follow.epoch),
            region: "r".into(),
            state: follow.state,
            load: None,
            load_age_ms: None,
            listing,
        }
    }

    fn exposed(&self, generation: u64) -> FollowEvent {
        FollowEvent::Exposed {
            class: "model_x".into(),
            pool: M1.into(),
            generation,
            segment: "f".into(),
            peer: entry().clone(),
            joined: 1,
        }
    }

    fn listed_event(&self) -> FollowEvent {
        FollowEvent::Listed {
            class: "model_x".into(),
            pool: M1.into(),
            generation: self.generation,
            listing: self.listing(),
        }
    }

    /// A whole watch's snapshot, as the relay takes it now: listings only
    /// when `whole`, which then says so.
    fn snapshot(&mut self, whole: bool) -> Vec<FollowEvent> {
        let mut events = Vec::new();
        match self.follow.filter(|follow| follow.installed) {
            Some(follow) => {
                let listing = whole.then(|| self.listing()).flatten();
                events.push(self.follow_event(&follow, listing));
                if matches!(follow.state, PoolState::Ready) {
                    events.push(self.exposed(follow.generation));
                }
            }
            None if whole && self.listing.is_some() => events.push(self.listed_event()),
            None => {}
        }
        events.push(FollowEvent::Synced { listed: whole });
        for event in &events {
            self.note(event);
        }
        events
    }

    /// A watch of the pool's own follow's snapshot, as the relay takes it
    /// now.
    fn own_snapshot(&mut self) -> Vec<FollowEvent> {
        let mut events = Vec::new();
        if let Some(follow) = self.follow.filter(|follow| follow.installed) {
            events.push(self.follow_event(&follow, self.listing()));
            if matches!(follow.state, PoolState::Ready) {
                events.push(self.exposed(follow.generation));
            }
        }
        events.push(FollowEvent::Synced { listed: true });
        for event in &events {
            self.note(event);
        }
        events
    }
}

/// The relay's watch and the pool's own, each delivering in its order.
struct Wire {
    relay: VecDeque<FollowEvent>,
    /// The pool's own watch, if the follower asked for one: its watch,
    /// and what it has yet to deliver.
    own: Option<(Asked, VecDeque<FollowEvent>)>,
}

/// What the follower has heard of the pool's hubs, by epoch, from what
/// each watch delivered and the follower takes: of a pool read from its
/// own watch, the relay's watch says only its listings and withdrawals,
/// and a watch's snapshot is heard once its marker is.
#[derive(Default)]
struct Heard {
    /// Each generation's hub, from the follows delivered.
    generations: HashMap<u64, u64>,
    drained: HashSet<u64>,
    withdrawn: HashSet<u64>,
    /// The pool's own watch's snapshot, until its marker.
    filling: Option<Vec<FollowEvent>>,
}

impl Heard {
    fn hear(&mut self, event: &FollowEvent) {
        match event {
            FollowEvent::Follow {
                generation,
                epoch,
                state,
                listing,
                ..
            } => {
                self.generations.insert(*generation, *epoch);
                if *state == PoolState::Draining {
                    self.drained.insert(*epoch);
                }
                if let Some(listing) = listing.as_ref().filter(|listing| listing.draining) {
                    self.drained.insert(listing.epoch);
                }
            }
            FollowEvent::State {
                generation,
                state: PoolState::Draining,
                ..
            } => {
                if let Some(epoch) = self.generations.get(generation) {
                    self.drained.insert(*epoch);
                }
            }
            FollowEvent::Listed {
                listing: Some(listing),
                ..
            } if listing.draining => {
                self.drained.insert(listing.epoch);
            }
            FollowEvent::Withdrawn { epoch, .. } => {
                self.withdrawn.insert(*epoch);
            }
            _ => {}
        }
    }

    /// The pool's own watch delivered `event`.
    fn hear_own(&mut self, event: &FollowEvent) {
        match &mut self.filling {
            Some(filling) if matches!(event, FollowEvent::Synced { .. }) => {
                for heard in std::mem::take(filling) {
                    self.hear(&heard);
                }
                self.filling = None;
            }
            Some(filling) => filling.push(event.clone()),
            None => self.hear(event),
        }
    }
}

impl Wire {
    fn send(&mut self, hub: &mut Hub, event: FollowEvent) {
        hub.note(&event);
        if let Some((_, own)) = &mut self.own
            && !matches!(event, FollowEvent::Listed { .. })
        {
            own.push_back(event.clone());
        }
        self.relay.push_back(event);
    }
}

/// An offer taken under a follow, with the epoch of the row it was taken
/// from, and where its lease is told the epoch it was accepted at.
struct Taken {
    lease: PoolLease,
    row: u64,
    told: Arc<Mutex<Option<u64>>>,
}

struct Told(Arc<Mutex<Option<u64>>>);

impl gdl::ChoiceLease for Told {
    fn accepted(&mut self) {
        panic!("the runtime tells the epoch");
    }

    fn accepted_at(&mut self, epoch: u64) {
        *self.0.lock().expect("unpoisoned") = Some(epoch);
    }
}

/// What a policy reads of the pool, and the row's epoch and generation.
fn read(pools: &Pools) -> (Option<gdl::PoolEvidence>, Option<(u64, u64)>) {
    let table = pools.table("f").expect("a table");
    let cx = gdl::Cx::new();
    let request = table.fill(PoolRequest::new(&cx, &(), "r", Instant::now()));
    let row = table
        .pools
        .iter()
        .find(|row| row.pool == M1)
        .map(|row| (row.generation, row.epoch));
    (request.evidence(M1), row)
}

/// Run `seed` for `steps`: the steps taken, or the steps up to the first
/// broken invariant, with what broke.
fn run(seed: u64, steps: usize) -> Result<(), (Vec<String>, String)> {
    let mut rng = Rng(seed);
    let (mut follows, pools, _, _) = follower();
    let start = Instant::now();
    let mut hub = Hub {
        earlier: seed % 4 == 3,
        epoch: 10,
        generation: 0,
        follow: None,
        listing: None,
        withdrawn: false,
        said: HashMap::new(),
    };
    let mut wire = Wire {
        relay: VecDeque::new(),
        own: None,
    };
    let mut heard = Heard::default();
    // Whether an unlisting found evidence of another hub than the follow
    // shown, which (c) then allows to read unknown until a later word.
    let mut unknown = false;
    // Steps for which the pool's own watch, a slow socket, delivers
    // nothing.
    let mut slow = 0_u32;
    let mut chosen: Option<velo::InstanceId> = None;
    let mut taken: Vec<Taken> = Vec::new();
    let mut trace = Vec::new();
    // The pool's first hub: listed, followed, installed.
    let first = hub.wire(hub.epoch);
    hub.listing = Some((first, PoolCondition::Live, false));
    let listed = hub.listed_event();
    wire.send(&mut hub, listed);
    for step in 0..steps {
        let now = start + Duration::from_millis(step as u64);
        // Steps 20 and up weigh the walk toward what loses a drain: a
        // withdrawal, which unlists a pool a lagging own watch still shows;
        // a same-epoch follow of a new generation; and a pool read from
        // its own watch; and a later hub's whole life, drain and withdrawal,
        // told on the relay's watch while the pool's own watch lags; and a
        // later hub's record read before the follower ends the earlier
        // hub's follow, which may drain after, so that a snapshot shows the
        // two together and only the follow says the drain.
        let what = match rng.below(38) {
            // The pool's next hub writes its record: listed at once, and
            // its follow begun, which may never install.
            0 => {
                hub.epoch += 1 + rng.below(3);
                hub.withdrawn = false;
                end_follow(&mut hub, &mut wire);
                hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                let listed = hub.listed_event();
                wire.send(&mut hub, listed);
                begin(&mut hub);
                format!("hub of epoch {} writes its record", hub.epoch)
            }
            1 | 2 => match hub.follow {
                Some(mut follow) if !follow.installed => {
                    follow.installed = true;
                    hub.follow = Some(follow);
                    let event = hub.follow_event(&follow, None);
                    wire.send(&mut hub, event);
                    let exposed = hub.exposed(follow.generation);
                    wire.send(&mut hub, exposed);
                    format!(
                        "follow {} of epoch {} installs",
                        follow.generation, follow.epoch
                    )
                }
                _ => "nothing to install".into(),
            },
            3 => match hub.follow {
                Some(mut follow) if follow.installed && follow.state != PoolState::Draining => {
                    follow.state = PoolState::Draining;
                    hub.follow = Some(follow);
                    wire.send(
                        &mut hub,
                        FollowEvent::State {
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation: follow.generation,
                            state: PoolState::Draining,
                        },
                    );
                    let of = hub.wire(follow.epoch);
                    // A state names a generation, not an epoch: what the
                    // hub of the follow said is noted here.
                    hub.said.entry(of).or_default().drained = true;
                    if let Some(listing) = &mut hub.listing
                        && listing.0 == of
                    {
                        listing.2 = true;
                    }
                    let listed = hub.listed_event();
                    wire.send(&mut hub, listed);
                    format!("follow {} drains", follow.generation)
                }
                // A draining follow the relay withholds, as when its pool
                // is unread: unavailable, while its hub still drains.
                Some(mut follow) if follow.installed => {
                    follow.state = PoolState::Unavailable;
                    hub.follow = Some(follow);
                    wire.send(
                        &mut hub,
                        FollowEvent::State {
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation: follow.generation,
                            state: PoolState::Unavailable,
                        },
                    );
                    format!("follow {} is withheld, unavailable", follow.generation)
                }
                _ => "nothing to drain".into(),
            },
            // The link to the current hub is lost, installed or not: the
            // hub is missing at its own epoch.
            4 => match hub.follow {
                Some(follow) => {
                    end_follow(&mut hub, &mut wire);
                    let of = hub.wire(follow.epoch);
                    if let Some(listing) = &mut hub.listing
                        && listing.0 == of
                        && listing.1 != PoolCondition::ScaledToZero
                    {
                        listing.1 = PoolCondition::Missing;
                    }
                    let listed = hub.listed_event();
                    wire.send(&mut hub, listed);
                    format!("the link of follow {} is lost", follow.generation)
                }
                None => "no link to lose".into(),
            },
            5 => match hub.listing {
                Some((epoch, PoolCondition::Live, draining)) if hub.follow.is_none() => {
                    hub.listing = Some((epoch, PoolCondition::Missing, draining));
                    let listed = hub.listed_event();
                    wire.send(&mut hub, listed);
                    "the record lapses".into()
                }
                _ => "no record to lapse".into(),
            },
            6 if hub.follow.is_none() && hub.listing.is_some() && !hub.withdrawn => {
                begin(&mut hub);
                "followed again".into()
            }
            // An operator scales the pool to 0, past its last write.
            7 if hub.listing.is_some() && rng.chance(40) => {
                hub.epoch += 1;
                end_follow(&mut hub, &mut wire);
                hub.listing = Some((hub.wire(hub.epoch), PoolCondition::ScaledToZero, false));
                let listed = hub.listed_event();
                wire.send(&mut hub, listed);
                format!("scaled to 0 at {}", hub.epoch)
            }
            // The pool withdraws, by its own hub or, past it, an operator.
            8 | 20 | 21 if hub.listing.is_some() && rng.chance(40) => {
                end_follow(&mut hub, &mut wire);
                hub.listing = None;
                let listed = hub.listed_event();
                wire.send(&mut hub, listed);
                hub.epoch += rng.below(2);
                hub.withdrawn = true;
                let withdrawn = FollowEvent::Withdrawn {
                    class: "model_x".into(),
                    pool: M1.into(),
                    generation: hub.generation,
                    epoch: hub.wire(hub.epoch),
                    properties_version: 0,
                };
                wire.send(&mut hub, withdrawn);
                format!("withdrawn at {}", hub.epoch)
            }
            // The relay restarts and reads the directory again: it numbers
            // its follows from 1, and the follower watches it again.
            9 if rng.chance(30) => {
                hub.generation = 0;
                hub.follow = None;
                wire.relay.clear();
                if let Some((asked, _)) = wire.own.take() {
                    follows.own(asked.key, asked.watch, Err(WatchEnd::Closed), now);
                    follows.asked();
                }
                // The pool's hub may have restarted meanwhile too: a later
                // hub, whose follow the new relay numbers from 1 again.
                let restarted = hub.listing.is_some() && !hub.withdrawn && rng.chance(50);
                if restarted {
                    hub.epoch += 1;
                    hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                }
                if hub
                    .listing
                    .is_some_and(|(_, condition, _)| condition != PoolCondition::ScaledToZero)
                {
                    begin(&mut hub);
                    // Its snapshot may come before the follower watches.
                    if rng.chance(50)
                        && let Some(follow) = &mut hub.follow
                    {
                        follow.installed = true;
                    }
                }
                let whole = rng.chance(70);
                rewatch(&mut follows, &mut hub, &mut wire, &mut heard, whole, now);
                format!(
                    "the relay restarts, the pool's hub too: {restarted}, at {}; rewatched whole: {whole}",
                    hub.epoch
                )
            }
            10 | 11 => {
                let whole = rng.chance(70);
                rewatch(&mut follows, &mut hub, &mut wire, &mut heard, whole, now);
                format!("the relay rewatched, whole: {whole}")
            }
            // An event of a kind the follower does not know: the pool is
            // read from its own watch, whose snapshot the relay takes now.
            12 | 24 | 25 if wire.own.is_none() => match read(&pools).1 {
                Some((generation, _)) => {
                    follows.unknown(UnknownFollowEvent {
                        kind: "paused".into(),
                        class: "model_x".into(),
                        pool: M1.into(),
                        generation,
                    });
                    match follows.asked().pop() {
                        Some(asked) => {
                            let snapshot = hub.own_snapshot().into();
                            wire.own = Some((asked, snapshot));
                            heard.filling = Some(Vec::new());
                            format!("the pool is read from its own watch, generation {generation}")
                        }
                        None => "no own watch asked".into(),
                    }
                }
                None => "no row to read again".into(),
            },
            // The pool's own watch is opened again.
            13 if wire.own.is_some() => {
                let (asked, _) = wire.own.take().expect("an own watch");
                heard.filling = None;
                follows.own(asked.key, asked.watch, Err(WatchEnd::Closed), now);
                match follows.asked().pop() {
                    Some(asked) => {
                        let snapshot = hub.own_snapshot().into();
                        wire.own = Some((asked, snapshot));
                        heard.filling = Some(Vec::new());
                        "the own watch reconnects".into()
                    }
                    None => "the own watch is let go".into(),
                }
            }
            26 | 27 if wire.own.is_some() && hub.listing.is_some() && !hub.withdrawn => {
                hub.epoch += 1;
                end_follow(&mut hub, &mut wire);
                hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                let listed = hub.listed_event();
                wire.send(&mut hub, listed);
                begin(&mut hub);
                let mut follow = hub.follow.expect("just begun");
                follow.installed = true;
                follow.state = PoolState::Draining;
                hub.follow = Some(follow);
                let event = hub.follow_event(&follow, None);
                wire.send(&mut hub, event);
                if let Some(listing) = &mut hub.listing {
                    listing.2 = true;
                }
                let listed = hub.listed_event();
                wire.send(&mut hub, listed);
                end_follow(&mut hub, &mut wire);
                hub.listing = None;
                let listed = hub.listed_event();
                wire.send(&mut hub, listed);
                hub.withdrawn = true;
                let withdrawn = FollowEvent::Withdrawn {
                    class: "model_x".into(),
                    pool: M1.into(),
                    generation: hub.generation,
                    epoch: hub.wire(hub.epoch),
                    properties_version: 0,
                };
                wire.send(&mut hub, withdrawn);
                slow = 12;
                // The relay's watch is quick: it delivers up to the
                // unlisting, and its withdrawal waits for a later step.
                while let Some(event) = wire.relay.pop_front() {
                    let unlisted = matches!(event, FollowEvent::Listed { listing: None, .. });
                    if wire.own.is_none()
                        || matches!(
                            event,
                            FollowEvent::Listed { .. } | FollowEvent::Withdrawn { .. }
                        )
                    {
                        heard.hear(&event);
                    }
                    let unlisting = unlisting_beside(&pools, &event);
                    unknown |= unlisting.is_some_and(|(another, _)| another);
                    follows.take(event, now);
                    if let Err(broke) = kept_at_unlisting(&pools, unlisting) {
                        trace.push("an unlisting is delivered".into());
                        return Err((trace, broke));
                    }
                    if unlisted {
                        break;
                    }
                }
                format!(
                    "hub of epoch {} lists, drains, and withdraws, while the own watch lags; the relay delivers to its unlisting",
                    hub.epoch
                )
            }
            // A later hub's record, then the earlier hub's follow drains,
            // and the pool's own watch is opened again: its snapshot shows
            // the draining follow beside the later listing, and only the
            // follow says the drain.
            30 | 31 => match (hub.follow, read(&pools).1) {
                (Some(mut follow), Some((generation, _))) if follow.installed => {
                    hub.epoch += 1;
                    hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                    let listed = hub.listed_event();
                    wire.send(&mut hub, listed);
                    follow.state = PoolState::Draining;
                    hub.follow = Some(follow);
                    hub.said.entry(hub.wire(follow.epoch)).or_default().drained = true;
                    wire.send(
                        &mut hub,
                        FollowEvent::State {
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation: follow.generation,
                            state: PoolState::Draining,
                        },
                    );
                    match wire.own.take() {
                        Some((asked, _)) => {
                            follows.own(asked.key, asked.watch, Err(WatchEnd::Closed), now);
                        }
                        None => follows.unknown(UnknownFollowEvent {
                            kind: "paused".into(),
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation,
                        }),
                    }
                    heard.filling = None;
                    match follows.asked().pop() {
                        Some(asked) => {
                            let snapshot = hub.own_snapshot().into();
                            wire.own = Some((asked, snapshot));
                            heard.filling = Some(Vec::new());
                            format!(
                                "hub of epoch {} lists; follow {} of epoch {} drains; its own watch opens",
                                hub.epoch, follow.generation, follow.epoch
                            )
                        }
                        None => "no own watch asked".into(),
                    }
                }
                _ => "no follow to drain behind a later hub".into(),
            },
            // A follow drains and is withheld, a later hub's record comes,
            // and the follower watches the relay again, whole: the new
            // snapshot shows the follow unavailable beside the later
            // listing, and no longer says the drain the follower heard.
            // Then the pool is read from its own watch.
            32 | 33 => match hub.follow {
                Some(mut follow) if follow.installed && wire.own.is_none() => {
                    let of = hub.wire(follow.epoch);
                    hub.said.entry(of).or_default().drained = true;
                    for state in [PoolState::Draining, PoolState::Unavailable] {
                        let event = FollowEvent::State {
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation: follow.generation,
                            state,
                        };
                        wire.send(&mut hub, event);
                    }
                    // The relay delivers all it holds, in order, before the
                    // follower watches it again.
                    while let Some(event) = wire.relay.pop_front() {
                        heard.hear(&event);
                        let unlisting = unlisting_beside(&pools, &event);
                        unknown |= unlisting.is_some_and(|(another, _)| another);
                        follows.take(event, now);
                        if let Err(broke) = kept_at_unlisting(&pools, unlisting) {
                            trace.push("an unlisting is delivered".into());
                            return Err((trace, broke));
                        }
                    }
                    follow.state = PoolState::Unavailable;
                    hub.follow = Some(follow);
                    hub.epoch += 1;
                    hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                    let listed = hub.listed_event();
                    wire.send(&mut hub, listed);
                    rewatch(&mut follows, &mut hub, &mut wire, &mut heard, true, now);
                    let generation = read(&pools)
                        .1
                        .map_or(follow.generation, |(generation, _)| generation);
                    follows.unknown(UnknownFollowEvent {
                        kind: "paused".into(),
                        class: "model_x".into(),
                        pool: M1.into(),
                        generation,
                    });
                    if let Some(asked) = follows.asked().pop() {
                        let snapshot = hub.own_snapshot().into();
                        wire.own = Some((asked, snapshot));
                        heard.filling = Some(Vec::new());
                    }
                    format!(
                        "follow {} of epoch {} drains and is withheld; hub of epoch {} lists; rewatched whole; own watch opens",
                        follow.generation, follow.epoch, hub.epoch
                    )
                }
                _ => "no follow to withhold behind a later hub".into(),
            },
            // The relay lags far behind the pool's own watch: the pool
            // withdraws, a later hub is listed and followed, a hub past it
            // is listed, and the followed hub drains; the pool's own watch
            // shows the draining follow beside the latest listing, and only
            // then does the relay deliver, up to its stale listing of the
            // followed hub, made before it drained.
            34 | 35 if wire.own.is_none() && hub.listing.is_some() && !hub.withdrawn => {
                match read(&pools).1 {
                    Some((generation, _)) => {
                        end_follow(&mut hub, &mut wire);
                        hub.listing = None;
                        let unlisted = hub.listed_event();
                        wire.send(&mut hub, unlisted);
                        let withdrawn = FollowEvent::Withdrawn {
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation: hub.generation,
                            epoch: hub.wire(hub.epoch),
                            properties_version: 0,
                        };
                        wire.send(&mut hub, withdrawn);
                        hub.epoch += 1;
                        hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                        let listed = hub.listed_event();
                        wire.send(&mut hub, listed);
                        begin(&mut hub);
                        let mut follow = hub.follow.expect("just begun");
                        follow.installed = true;
                        let event = hub.follow_event(&follow, None);
                        wire.send(&mut hub, event);
                        let exposed = hub.exposed(follow.generation);
                        wire.send(&mut hub, exposed);
                        hub.epoch += 1;
                        hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                        let listed = hub.listed_event();
                        wire.send(&mut hub, listed);
                        follow.state = PoolState::Draining;
                        hub.follow = Some(follow);
                        hub.said.entry(hub.wire(follow.epoch)).or_default().drained = true;
                        wire.send(
                            &mut hub,
                            FollowEvent::State {
                                class: "model_x".into(),
                                pool: M1.into(),
                                generation: follow.generation,
                                state: PoolState::Draining,
                            },
                        );
                        follows.unknown(UnknownFollowEvent {
                            kind: "paused".into(),
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation,
                        });
                        if let Some(asked) = follows.asked().pop() {
                            heard.filling = Some(Vec::new());
                            for event in hub.own_snapshot() {
                                heard.hear_own(&event);
                                follows.own(
                                    asked.key.clone(),
                                    asked.watch,
                                    Ok(Watched::Event(event)),
                                    now,
                                );
                            }
                            wire.own = Some((asked, VecDeque::new()));
                        }
                        let mut unlisted = false;
                        while let Some(event) = wire.relay.pop_front() {
                            let stale = unlisted
                                && matches!(
                                    event,
                                    FollowEvent::Listed {
                                        listing: Some(_),
                                        ..
                                    }
                                );
                            unlisted |= matches!(event, FollowEvent::Listed { listing: None, .. });
                            if wire.own.is_none()
                                || matches!(
                                    event,
                                    FollowEvent::Listed { .. } | FollowEvent::Withdrawn { .. }
                                )
                            {
                                heard.hear(&event);
                            }
                            let unlisting = unlisting_beside(&pools, &event);
                            unknown |= unlisting.is_some_and(|(another, _)| another);
                            follows.take(event, now);
                            if let Err(broke) = kept_at_unlisting(&pools, unlisting) {
                                trace.push("an unlisting is delivered".into());
                                return Err((trace, broke));
                            }
                            if stale {
                                break;
                            }
                        }
                        format!(
                            "the relay lags: withdrawn, hub {} followed and draining behind hub {}, read from its own watch; the relay delivers to its stale listing",
                            follow.epoch, hub.epoch
                        )
                    }
                    None => "no row to read again".into(),
                }
            }
            // The pool's own watch takes its snapshot of a later hub while
            // that hub is ready, and holds back its marker; the hub drains
            // and the pool withdraws; the relay delivers up to its
            // unlisting, which forgets the drain beside the earlier hub's
            // row; then the marker comes, at the fence's hub.
            36 | 37 if wire.own.is_none() && hub.listing.is_some() && !hub.withdrawn => {
                match read(&pools).1 {
                    Some((generation, _)) => {
                        end_follow(&mut hub, &mut wire);
                        hub.epoch += 1;
                        hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                        let listed = hub.listed_event();
                        wire.send(&mut hub, listed);
                        begin(&mut hub);
                        let mut follow = hub.follow.expect("just begun");
                        follow.installed = true;
                        follow.state = PoolState::Ready;
                        hub.follow = Some(follow);
                        let event = hub.follow_event(&follow, None);
                        wire.send(&mut hub, event);
                        let exposed = hub.exposed(follow.generation);
                        wire.send(&mut hub, exposed);
                        follows.unknown(UnknownFollowEvent {
                            kind: "paused".into(),
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation,
                        });
                        let Some(asked) = follows.asked().pop() else {
                            trace.push("no own watch asked".into());
                            continue;
                        };
                        heard.filling = Some(Vec::new());
                        let mut snapshot = hub.own_snapshot();
                        let marker = snapshot.pop().expect("a marker");
                        for event in snapshot {
                            heard.hear_own(&event);
                            follows.own(
                                asked.key.clone(),
                                asked.watch,
                                Ok(Watched::Event(event)),
                                now,
                            );
                        }
                        let (key, watch) = (asked.key.clone(), asked.watch);
                        wire.own = Some((asked, VecDeque::new()));
                        follow.state = PoolState::Draining;
                        hub.follow = Some(follow);
                        hub.said.entry(hub.wire(follow.epoch)).or_default().drained = true;
                        wire.send(
                            &mut hub,
                            FollowEvent::State {
                                class: "model_x".into(),
                                pool: M1.into(),
                                generation: follow.generation,
                                state: PoolState::Draining,
                            },
                        );
                        if let Some(listing) = &mut hub.listing {
                            listing.2 = true;
                        }
                        let listed = hub.listed_event();
                        wire.send(&mut hub, listed);
                        end_follow(&mut hub, &mut wire);
                        hub.listing = None;
                        let unlisted = hub.listed_event();
                        wire.send(&mut hub, unlisted);
                        hub.withdrawn = true;
                        let withdrawn = FollowEvent::Withdrawn {
                            class: "model_x".into(),
                            pool: M1.into(),
                            generation: hub.generation,
                            epoch: hub.wire(hub.epoch),
                            properties_version: 0,
                        };
                        wire.send(&mut hub, withdrawn);
                        while let Some(event) = wire.relay.pop_front() {
                            let unlisting_now =
                                matches!(event, FollowEvent::Listed { listing: None, .. });
                            if matches!(
                                event,
                                FollowEvent::Listed { .. } | FollowEvent::Withdrawn { .. }
                            ) {
                                heard.hear(&event);
                            }
                            let unlisting = unlisting_beside(&pools, &event);
                            unknown |= unlisting.is_some_and(|(another, _)| another);
                            follows.take(event, now);
                            if let Err(broke) = kept_at_unlisting(&pools, unlisting) {
                                trace.push("an unlisting is delivered".into());
                                return Err((trace, broke));
                            }
                            if unlisting_now {
                                break;
                            }
                        }
                        heard.hear_own(&marker);
                        follows.own(key, watch, Ok(Watched::Event(marker)), now);
                        format!(
                            "own snapshot of hub {} taken ready, marker held; the hub drains and withdraws; the relay delivers to its unlisting; then the marker",
                            hub.epoch
                        )
                    }
                    None => "no row to read again".into(),
                }
            }
            28 | 29 => match hub.follow {
                Some(follow) if follow.installed => {
                    hub.epoch += 1;
                    hub.listing = Some((hub.wire(hub.epoch), PoolCondition::Live, false));
                    let listed = hub.listed_event();
                    wire.send(&mut hub, listed);
                    format!(
                        "hub of epoch {} writes its record; follow {} of epoch {} is not yet ended",
                        hub.epoch, follow.generation, follow.epoch
                    )
                }
                _ => "no follow to outlive".into(),
            },
            // The link is replaced: a follow of a new generation of the
            // same hub, installed, as after a link that came back.
            22 | 23 => match hub.follow {
                Some(follow) if follow.installed => {
                    end_follow(&mut hub, &mut wire);
                    begin(&mut hub);
                    let mut next = hub.follow.expect("just begun");
                    next.installed = true;
                    hub.follow = Some(next);
                    let event = hub.follow_event(&next, None);
                    wire.send(&mut hub, event);
                    if matches!(next.state, PoolState::Ready) {
                        let exposed = hub.exposed(next.generation);
                        wire.send(&mut hub, exposed);
                    }
                    format!(
                        "follow {} replaced by follow {} of epoch {}",
                        follow.generation, next.generation, next.epoch
                    )
                }
                _ => "no link to replace".into(),
            },
            14..=16 => match wire.relay.pop_front() {
                Some(event) => {
                    let said = format!("relay delivers {event:?}");
                    if wire.own.is_none()
                        || matches!(
                            event,
                            FollowEvent::Listed { .. } | FollowEvent::Withdrawn { .. }
                        )
                    {
                        heard.hear(&event);
                    }
                    let unlisting = unlisting_beside(&pools, &event);
                    unknown |= unlisting.is_some_and(|(another, _)| another);
                    follows.take(event, now);
                    if let Err(broke) = kept_at_unlisting(&pools, unlisting) {
                        trace.push("an unlisting is delivered".into());
                        return Err((trace, broke));
                    }
                    said
                }
                None => "the relay is quiet".into(),
            },
            17 | 18 if slow > 0 => {
                slow -= 1;
                "the own watch is slow".into()
            }
            17 | 18 => match &mut wire.own {
                Some((asked, events)) => match events.pop_front() {
                    Some(event) => {
                        let said = format!("own watch delivers {event:?}");
                        heard.hear_own(&event);
                        follows.own(
                            asked.key.clone(),
                            asked.watch,
                            Ok(Watched::Event(event)),
                            now,
                        );
                        said
                    }
                    None => "the own watch is quiet".into(),
                },
                None => "no own watch".into(),
            },
            // A choice of the pool's instance, and later its lease, taken
            // from the row that shows the instance then, as the runtime
            // checks a choice the selector returns, and later accepted.
            _ => {
                let table = pools.table("f").expect("a table");
                let row = table
                    .pools
                    .iter()
                    .find(|row| row.pool == M1 && !row.eligible.is_empty());
                match (chosen, row) {
                    (None, Some(row)) => {
                        chosen = Some(row.eligible[0]);
                        format!(
                            "chose the instance of follow {} of epoch {}",
                            row.generation, row.epoch
                        )
                    }
                    (Some(instance), _) => {
                        chosen = None;
                        let still = table
                            .pools
                            .iter()
                            .find(|row| row.pool == M1 && row.eligible.contains(&instance));
                        match still.and_then(|row| Some((row, PoolLease::take(&row.leases)?))) {
                            Some((row, lease)) => {
                                let told = Arc::new(Mutex::new(None));
                                let (_, choice) = gdl::PoolChoice::new(0)
                                    .with_lease(Told(Arc::clone(&told)))
                                    .into_parts();
                                taken.push(Taken {
                                    lease: lease.with_choice(choice),
                                    row: row.epoch,
                                    told,
                                });
                                format!(
                                    "took a lease under follow {} of epoch {}",
                                    row.generation, row.epoch
                                )
                            }
                            None => "the choice is no longer eligible".into(),
                        }
                    }
                    (None, None) => match taken.pop() {
                        Some(mut offer) => {
                            offer.lease.accepted();
                            let told = offer.told.lock().expect("unpoisoned").take();
                            if told.is_none_or(|told| told < offer.row) {
                                trace.push("an offer is accepted".into());
                                return Err((
                                    trace,
                                    format!(
                                        "(b) accepted at {told:?}, under a row of epoch {}",
                                        offer.row
                                    ),
                                ));
                            }
                            format!("accepted at {told:?}")
                        }
                        None => "nothing to offer".into(),
                    },
                }
            }
        };
        trace.push(what);
        let (evidence, row) = read(&pools);
        unknown &= evidence.is_none();
        if let Err(broke) = check(&hub, evidence, row, unknown).and_then(|()| {
            // A relay of an earlier build dates every hub 0: a drain
            // cannot be tied to one hub.
            if hub.earlier {
                Ok(())
            } else {
                lasts(&heard, evidence)
            }
        }) {
            return Err((trace, broke));
        }
    }
    Ok(())
}

/// (a): the evidence's state is what the hub of its epoch said; and (c):
/// it is of the hub of the row beside it or a later one, so that a policy
/// never reads a row with an earlier hub's epoch.
fn check(
    hub: &Hub,
    evidence: Option<gdl::PoolEvidence>,
    row: Option<(u64, u64)>,
    unknown: bool,
) -> Result<(), String> {
    if let Some((generation, of)) = row
        && match evidence {
            Some(evidence) => evidence.epoch() < of,
            None => !unknown,
        }
    {
        return Err(format!(
            "(c) {evidence:?} beside the row of follow {generation} of epoch {of}"
        ));
    }
    let Some(evidence) = evidence else {
        return Ok(());
    };
    let epoch = evidence.epoch();
    let Some(said) = hub.said.get(&epoch) else {
        return Err(format!(
            "(a) {evidence:?}: no hub of epoch {epoch} said anything"
        ));
    };
    // A withdrawn pool has no condition worth reading.
    let condition = evidence.is_withdrawn() || said.conditions.contains(&evidence.condition());
    if !condition
        || (evidence.is_draining() && !said.drained)
        || (evidence.is_withdrawn() && !said.withdrawn)
    {
        return Err(format!(
            "(a) {evidence:?}: the hub of epoch {epoch} said {:?}, drained {}, withdrawn {}",
            said.conditions, said.drained, said.withdrawn
        ));
    }
    Ok(())
}

/// (d): evidence of a hub the follower heard drain, and not withdraw,
/// never reads not draining. Unknown is allowed.
fn lasts(heard: &Heard, evidence: Option<gdl::PoolEvidence>) -> Result<(), String> {
    match evidence {
        Some(evidence)
            if heard.drained.contains(&evidence.epoch())
                && !heard.withdrawn.contains(&evidence.epoch())
                && !evidence.is_draining() =>
        {
            Err(format!(
                "(d) {evidence:?}, of a hub the follower heard drain"
            ))
        }
        _ => Ok(()),
    }
}

/// If delivering `event` unlists the pool while a follow of it is shown:
/// whether the evidence beside it is of another hub, or none, which (c)
/// then allows to read unknown beside its row, and the follow's epoch.
fn unlisting_beside(pools: &Pools, event: &FollowEvent) -> Option<(bool, u64)> {
    if !matches!(event, FollowEvent::Listed { listing: None, .. }) {
        return None;
    }
    let (evidence, row) = read(pools);
    let (_, of) = row?;
    let another =
        evidence.is_none_or(|evidence| !evidence.is_withdrawn() && evidence.epoch() != of);
    Some((another, of))
}

/// (c), at an unlisting beside a follow of epoch `of`: the evidence kept is
/// that follow's hub's, or a withdrawal, or none.
fn kept_at_unlisting(pools: &Pools, unlisting: Option<(bool, u64)>) -> Result<(), String> {
    let Some((_, of)) = unlisting else {
        return Ok(());
    };
    match read(pools).0 {
        Some(evidence) if !evidence.is_withdrawn() && evidence.epoch() != of => Err(format!(
            "(c) {evidence:?} kept by an unlisting beside a follow of epoch {of}"
        )),
        _ => Ok(()),
    }
}

/// The relay begins a follow of the pool's current record.
fn begin(hub: &mut Hub) {
    hub.generation += 1;
    // A hub that drains says so to every follow of it.
    let draining = hub.listing.is_some_and(|(_, _, draining)| draining);
    hub.follow = Some(Following {
        generation: hub.generation,
        epoch: hub.epoch,
        installed: false,
        state: if draining {
            PoolState::Draining
        } else {
            PoolState::Ready
        },
    });
}

/// The relay ends its follow, installed or not: a dropped one is sent.
fn end_follow(hub: &mut Hub, wire: &mut Wire) {
    if let Some(follow) = hub.follow.take()
        && follow.installed
    {
        wire.send(
            hub,
            FollowEvent::Dropped {
                class: "model_x".into(),
                pool: M1.into(),
                generation: follow.generation,
            },
        );
    }
}

/// The follower watches the relay again: what the old watch had yet to
/// deliver is lost, and its snapshot replaces it, as does the pool's own
/// watch, which a rewatch lets go of.
fn rewatch(
    follows: &mut Follows<Recorded>,
    hub: &mut Hub,
    wire: &mut Wire,
    heard: &mut Heard,
    whole: bool,
    now: Instant,
) {
    wire.relay.clear();
    wire.own = None;
    heard.filling = None;
    let mut snapshot = follows.snapshot();
    for event in hub.snapshot(whole) {
        heard.hear(&event);
        snapshot.apply(event, now);
    }
    follows.resnapshot(snapshot, now);
    follows.asked();
}

fn seeds() -> Vec<u64> {
    if let Some(seed) = std::env::var("GDL_EPOCH_GUARD_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
    {
        return vec![seed];
    }
    let count = std::env::var("GDL_EPOCH_GUARD_SEEDS")
        .ok()
        .and_then(|count| count.parse().ok())
        .unwrap_or(SEEDS);
    (0..count).collect()
}

/// The guard, over a fixed set of seeds; see the module's documentation.
#[test]
fn every_interleaving_keeps_each_state_with_its_hubs_epoch() {
    for seed in seeds() {
        if let Err((trace, broke)) = run(seed, STEPS) {
            let steps: Vec<String> = trace
                .iter()
                .enumerate()
                .map(|(step, what)| format!("{step:4}: {what}"))
                .collect();
            panic!(
                "seed {seed}: {broke}\nsteps:\n{}\nrerun with GDL_EPOCH_GUARD_SEED={seed}",
                steps.join("\n")
            );
        }
    }
}
