//! What a chooser's follower does with the relay's events: its state, the
//! tables it publishes from it, and the velo registrations it keeps, with
//! no I/O of its own, so that every rule here is tested without a hub.
//!
//! An event changes one cell's follow, and costs the work of that cell's
//! table: the cell's table is made again from its own instances, and its
//! segment's table is a new list of the cells' tables, one pointer per
//! cell, so no event reads another cell's instances. Only a new snapshot
//! makes every table again.
//!
//! A follow the hub marks not offered keeps every instance it held
//! registered, routable or not, until the hub drops it: what was offered
//! to the cell may still be under way, and velo fails what is in flight to
//! a peer it forgets. Once this instance holds no offer to it, the
//! follower says so to its hub, at the revision of the mark
//! ([`Follows::acknowledgements`]); the hub drops the follow once every
//! chooser has. A lease's end does nothing but its decrement, so the
//! follower looks for the follows whose last lease has ended every
//! [`ACK_POLL`] while it has one to acknowledge.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use gdl_cells::CellState;
use gdl_watch::{FollowEvent, Unoffered};
use tokio::time::Instant;
use velo::{InstanceId, PeerInfo};

use super::lease::Leases;
use super::{CellTable, Cells, EntryTable, Load};
use crate::dist::fabric::Registry;
use crate::dist::roster::Live;

/// A followed cell, by its class and its name within the class: two
/// classes may each have a cell of one name.
type Key = (String, String);

/// How often the follower looks for a follow not offered whose last lease
/// has ended, while it has one to acknowledge.
pub(super) const ACK_POLL: Duration = Duration::from_millis(50);

/// How long after its hub did not take an acknowledgement the follower
/// sends it again.
pub(super) const ACK_RETRY: Duration = Duration::from_millis(500);

/// The follower's state: each followed cell's current follow, the tables
/// made from them, and the instances it registered with velo.
pub(crate) struct Follows<R> {
    cells: Cells,
    registry: R,
    /// This instance's own membership: a peer it routes to is never
    /// unregistered here.
    live: Arc<Live>,
    /// How long after an instance is in no table it stays registered:
    /// what was sent to it may still be under way, and velo fails what is
    /// in flight to a peer it forgets.
    drain: Duration,
    /// For each class sent to, the entry segments kept.
    wanted: Arc<Wanted>,
    followed: Followed,
    /// Each entry segment's cells, as last published.
    tables: BTreeMap<String, BTreeMap<Key, Arc<CellTable>>>,
    /// How many current follows hold each instance, exposed or retired.
    held: HashMap<InstanceId, usize>,
    /// The instances registered with velo here.
    registered: HashSet<InstanceId>,
    /// The unregistrations due, in the order they fall due: every one is
    /// `drain` after it was asked for, so the queue is sorted as it grows.
    forget: VecDeque<(Instant, InstanceId)>,
    /// The latest unregistration due for each instance: an earlier entry
    /// in `forget` for one asked for again is passed over.
    due: HashMap<InstanceId, Instant>,
    /// When the follower looks again for a follow to acknowledge, while it
    /// has one.
    poll: Option<Instant>,
}

impl<R: Registry> Follows<R> {
    pub(crate) fn new(
        cells: Cells,
        registry: R,
        live: Arc<Live>,
        drain: Duration,
        wanted: Wanted,
    ) -> Self {
        Self {
            cells,
            registry,
            live,
            drain,
            wanted: Arc::new(wanted),
            followed: Followed::default(),
            tables: BTreeMap::new(),
            held: HashMap::new(),
            registered: HashSet::new(),
            forget: VecDeque::new(),
            due: HashMap::new(),
            poll: None,
        }
    }

    /// A snapshot of the relay to fill, as a watch begins.
    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            wanted: Arc::clone(&self.wanted),
            followed: Followed::default(),
        }
    }

    /// Apply one event of the relay after its snapshot, at `now`, by
    /// [`Followed::apply`]'s rules: what it changed is then published,
    /// with an exposed instance registered first and every instance no
    /// follow holds any more unregistered after the drain. A follow that
    /// is offered again takes leases again before a table shows it; one
    /// that is not is shown so before it refuses them, so that an offer
    /// refused a lease finds the table that no longer offers it.
    pub(crate) fn apply(&mut self, event: FollowEvent, now: Instant) {
        let (key, change) = match self.followed.apply(&self.wanted, event, now) {
            Some(changed) => changed,
            None => return,
        };
        match change {
            Change::Table => {}
            Change::Ended(follow) => self.release(&follow, now),
            Change::Replaced(earlier) => {
                self.publish(&key);
                earlier.unoffer();
                self.poll = Some(now);
                return;
            }
            Change::Offering {
                leases,
                offered: true,
                current,
            } => {
                leases.offer();
                if !current {
                    return;
                }
            }
            Change::Offering {
                leases,
                offered: false,
                current,
            } => {
                if current {
                    self.publish(&key);
                }
                leases.unoffer();
                self.poll = Some(now);
                return;
            }
            // Registered before it is in a table: a call by instance goes
            // only to a peer velo was told of.
            Change::Exposed(peer) => {
                let id = peer.instance_id();
                if self.register(&peer) {
                    self.hold(id);
                } else if let Some(follow) = self.followed.follows.get_mut(&key) {
                    follow.exposed.remove(&id);
                }
            }
            Change::Held(id) => self.hold(id),
            Change::Released(id) => self.unhold(id, now),
        }
        self.publish(&key);
    }

    /// A watch's snapshot is complete: it replaces everything this
    /// follower held, at `now`. The snapshot's generations are taken as
    /// they are, never fenced by an earlier watch's, since a hub that
    /// restarted numbers its follows again from 1. Its new instances are
    /// registered before any table shows them; an instance that no follow
    /// holds any more is unregistered after the drain. A follow shown
    /// again in its generation keeps what this instance holds of it: its
    /// turns, its leases, its acknowledgement of the same revision, and
    /// the instances its state hides, which the snapshot does not show.
    pub(super) fn resnapshot(&mut self, snapshot: Snapshot, now: Instant) {
        let Snapshot { mut followed, .. } = snapshot;
        for follow in followed
            .follows
            .values_mut()
            .chain(followed.closing.values_mut())
        {
            let mut unreachable = Vec::new();
            for (id, (_, peer)) in &follow.exposed {
                if !self.register(peer) {
                    unreachable.push(*id);
                }
            }
            for id in unreachable {
                follow.exposed.remove(&id);
            }
        }
        for (key, follow) in &mut followed.follows {
            if let Some(held) = self.followed.find(key, follow.generation) {
                follow.carry(held);
            }
        }
        for ((key, generation), follow) in &mut followed.closing {
            if let Some(held) = self.followed.find(key, *generation) {
                follow.carry(held);
            }
        }
        let mut held: HashMap<InstanceId, usize> = HashMap::new();
        for follow in followed.follows.values().chain(followed.closing.values()) {
            for id in follow.held() {
                *held.entry(id).or_default() += 1;
            }
        }
        let released: Vec<InstanceId> = self
            .held
            .keys()
            .filter(|id| !held.contains_key(id))
            .copied()
            .collect();
        self.held = held;
        for id in released {
            self.schedule(id, now);
        }
        self.followed = followed;
        let follows = || {
            self.followed
                .follows
                .values()
                .chain(self.followed.closing.values())
        };
        for follow in follows().filter(|follow| follow.offered) {
            follow.leases.offer();
        }
        let mut tables: BTreeMap<String, BTreeMap<Key, Arc<CellTable>>> = BTreeMap::new();
        for (key, follow) in &self.followed.follows {
            for segment in &self.wanted[&key.0] {
                tables
                    .entry(segment.clone())
                    .or_default()
                    .insert(key.clone(), Arc::new(follow.table(key, segment)));
            }
        }
        self.tables = tables;
        for segment in self.wanted.values().flatten() {
            self.store(segment);
        }
        for follow in follows().filter(|follow| !follow.offered) {
            follow.leases.unoffer();
            self.poll = Some(now);
        }
        self.cells.changed();
    }

    /// When the follower looks again for a follow to acknowledge: `None`
    /// while it has none. It is set where a follow may come to need one,
    /// as a change makes it not offered or a snapshot shows it so, and by
    /// each look that leaves one waiting, so that an event never passes
    /// over the follows to find out.
    pub(super) fn next_poll(&self) -> Option<Instant> {
        self.poll
    }

    /// The acknowledgements due at `now`, each of a follow, current or
    /// earlier, that is not offered, that this instance holds no lease on,
    /// and whose revision it has yet to acknowledge: each is marked as
    /// being sent, until [`Follows::acknowledged`] says whether its hub
    /// took it. The follower looks again [`ACK_POLL`] later while a follow
    /// still waits for one.
    pub(super) fn acknowledgements(&mut self, now: Instant) -> Vec<Unoffered> {
        let due = self.followed.due(now);
        self.poll = self.followed.awaiting().then(|| now + ACK_POLL);
        due
    }

    /// The hub took `unoffered`, or did not, when it is sent again after
    /// [`ACK_RETRY`]. One of a revision the follow has since left behind
    /// changes nothing.
    pub(super) fn acknowledged(&mut self, unoffered: &Unoffered, taken: bool, now: Instant) {
        let key = (unoffered.class.clone(), unoffered.cell.clone());
        let Some(follow) = self.followed.find_mut(&key, unoffered.generation) else {
            return;
        };
        if follow.ack != Ack::Sending(unoffered.revision) {
            return;
        }
        if taken {
            follow.ack = Ack::Sent(unoffered.revision);
            return;
        }
        let again = now + ACK_RETRY;
        follow.ack = Ack::Failed(again);
        self.poll = Some(self.poll.map_or(again, |poll| poll.min(again)));
    }

    /// When the next unregistration falls due, if one does.
    pub(super) fn next_due(&self) -> Option<Instant> {
        self.forget.front().map(|(at, _)| *at)
    }

    /// Unregister what has fallen due by `now`: each instance that no
    /// table holds again since, and that this instance's own membership
    /// does not route to.
    pub(crate) fn forget(&mut self, now: Instant) {
        let mut forgot = false;
        while let Some(&(at, id)) = self.forget.front() {
            if at > now {
                break;
            }
            self.forget.pop_front();
            if self.due.get(&id) != Some(&at) {
                continue;
            }
            self.due.remove(&id);
            if self.held.contains_key(&id) || !self.registered.remove(&id) {
                continue;
            }
            // A peer this instance's own membership routes to, or is still
            // draining, is the membership's to unregister when its drain
            // ends: the follower gives it up.
            let (registry, live, cells) = (&self.registry, &self.live, &self.cells);
            cells.registered(|registered| {
                registered.remove(&id);
                if !live.routes_to(id) && !cells.membership_drains(id) {
                    registry.unregister(id);
                }
            });
            forgot = true;
        }
        if forgot {
            self.cells.changed();
        }
    }

    /// Register `peer` with velo, unless it is already: `false` if velo
    /// refused it, which leaves it out of every table.
    fn register(&mut self, peer: &PeerInfo) -> bool {
        let id = peer.instance_id();
        if self.registered.contains(&id) {
            return true;
        }
        let registry = &self.registry;
        let registered = self.cells.registered(|registered| {
            let done = registry.register(peer.clone());
            if done.is_ok() {
                registered.insert(id);
            }
            done
        });
        match registered {
            Ok(()) => {
                self.registered.insert(id);
                true
            }
            Err(error) => {
                tracing::warn!(%id, %error, "a followed cell's entry instance could not be registered with velo");
                false
            }
        }
    }

    fn hold(&mut self, id: InstanceId) {
        *self.held.entry(id).or_default() += 1;
    }

    /// One follow fewer holds `id`: once none does, it is unregistered
    /// after the drain, unless a follow holds it again by then.
    fn unhold(&mut self, id: InstanceId, now: Instant) {
        let Some(count) = self.held.get_mut(&id) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            self.held.remove(&id);
            self.schedule(id, now);
        }
    }

    /// Every instance `follow`, which ended, held.
    fn release(&mut self, follow: &Follow, now: Instant) {
        for id in follow.held() {
            self.unhold(id, now);
        }
    }

    fn schedule(&mut self, id: InstanceId, now: Instant) {
        if !self.registered.contains(&id) {
            return;
        }
        let at = now + self.drain;
        self.due.insert(id, at);
        self.forget.push_back((at, id));
    }

    /// Publish `key`'s cell, as its follow now is or without it, in the
    /// table of each entry segment of its class.
    fn publish(&mut self, key: &Key) {
        let Some(segments) = self.wanted.get(&key.0) else {
            return;
        };
        let follow = self.followed.follows.get(key);
        for segment in segments {
            let cells = self.tables.entry(segment.clone()).or_default();
            match follow {
                Some(follow) => {
                    cells.insert(key.clone(), Arc::new(follow.table(key, segment)));
                }
                None => {
                    cells.remove(key);
                }
            }
        }
        for segment in segments {
            self.store(segment);
        }
        self.cells.changed();
    }

    /// Swap `segment`'s table for one of its cells as they are now: a new
    /// list of the cells' tables, one pointer per followed cell. The follow
    /// set bounds it (a region's home cells, and at most `max_near` near
    /// ones, `agent-docs/cells.md` section 12.2); no other cell's instances
    /// are read or copied.
    fn store(&self, segment: &str) {
        let cells = self
            .tables
            .get(segment)
            .map(|cells| cells.values().cloned().collect())
            .unwrap_or_default();
        self.cells.store(segment, EntryTable { cells });
    }
}

/// For each class sent to, the entry segments kept.
pub(crate) type Wanted = BTreeMap<String, BTreeSet<String>>;

/// The follows one watch has shown: each cell's current follow, the
/// earlier ones the relay has yet to drop, and the newest generation of
/// each cell, so that an older one is ignored even after its cell's follow
/// was dropped. What the follower holds, and a snapshot being filled, are
/// each one of these, changed by the same rules.
#[derive(Default)]
struct Followed {
    follows: HashMap<Key, Follow>,
    /// Earlier follows of a cell, by generation: not offered, and holding
    /// their instances until the relay drops them.
    closing: HashMap<(Key, u64), Follow>,
    generations: HashMap<Key, u64>,
}

/// What one event changed in a cell's follow, beyond its table.
enum Change {
    /// Only what the table shows.
    Table,
    /// The follow was dropped: it holds nothing from now.
    Ended(Box<Follow>),
    /// A newer generation replaced the follow that counts these leases,
    /// which is kept until it is dropped, and offered no more.
    Replaced(Arc<Leases>),
    /// The follow, current or earlier, is offered now, or not.
    Offering {
        leases: Arc<Leases>,
        offered: bool,
        current: bool,
    },
    /// The follow exposes this instance now.
    Exposed(PeerInfo),
    /// The follow holds this instance now, retired.
    Held(InstanceId),
    /// The follow no longer holds this instance.
    Released(InstanceId),
}

impl Followed {
    /// Apply one event, which arrived at `now`: the cell it changed, and
    /// how, or `None` for an event of a class or segment not sent to, or
    /// of a generation this view does not hold, which changes nothing. Of
    /// an earlier generation that the relay has yet to drop, only whether
    /// it is offered and its drop are applied: its link has ended, and
    /// nothing else of it changes.
    fn apply(
        &mut self,
        wanted: &Wanted,
        event: FollowEvent,
        now: Instant,
    ) -> Option<(Key, Change)> {
        let wants = |class: &str, segment: &str| {
            wanted
                .get(class)
                .is_some_and(|segments| segments.contains(segment))
        };
        match event {
            FollowEvent::Follow {
                class,
                cell,
                generation,
                region,
                state,
                load,
            } => {
                let segments = wanted.get(&class)?;
                let key = (class, cell);
                if self
                    .generations
                    .get(&key)
                    .is_some_and(|newest| *newest >= generation)
                {
                    return None;
                }
                self.generations.insert(key.clone(), generation);
                // A newer generation replaces the older at once: nothing
                // of the older one is carried over, and it holds its
                // instances until the relay drops it.
                let older = self.follows.insert(
                    key.clone(),
                    Follow::new(
                        generation,
                        region,
                        state,
                        load.map(|load| Load::new(load, now)),
                        segments,
                    ),
                );
                let Some(mut older) = older else {
                    return Some((key, Change::Table));
                };
                older.offered = false;
                let leases = Arc::clone(&older.leases);
                self.closing.insert((key.clone(), older.generation), older);
                Some((key, Change::Replaced(leases)))
            }
            FollowEvent::State {
                class,
                cell,
                generation,
                state,
            } => {
                let key = (class, cell);
                self.current(&key, generation)?.set_state(state);
                Some((key, Change::Table))
            }
            FollowEvent::Load {
                class,
                cell,
                generation,
                load,
            } => {
                let key = (class, cell);
                self.current(&key, generation)?.load = load.map(|load| Load::new(load, now));
                Some((key, Change::Table))
            }
            FollowEvent::Offering {
                class,
                cell,
                generation,
                revision,
                offered,
            } => {
                let key = (class, cell);
                let current = self
                    .follows
                    .get(&key)
                    .is_some_and(|follow| follow.generation == generation);
                let follow = self.find_mut(&key, generation)?;
                // A follow begins offered at revision 1, which its hub
                // installs not offered for a cell that takes no work: a
                // revision names one state, so only an older one, or the
                // same one again, is ignored.
                if revision < follow.revision
                    || (revision == follow.revision && offered == follow.offered)
                {
                    return None;
                }
                follow.revision = revision;
                follow.offered = offered;
                follow.ack = Ack::None;
                let leases = Arc::clone(&follow.leases);
                Some((
                    key,
                    Change::Offering {
                        leases,
                        offered,
                        current,
                    },
                ))
            }
            FollowEvent::Exposed {
                class,
                cell,
                generation,
                segment,
                peer,
            } => {
                if !wants(&class, &segment) {
                    return None;
                }
                let key = (class, cell);
                let change = self.current(&key, generation)?.expose(segment, peer)?;
                Some((key, change))
            }
            FollowEvent::Retired {
                class,
                cell,
                generation,
                segment,
                instance,
            } => {
                if !wants(&class, &segment) {
                    return None;
                }
                let key = (class, cell);
                let newly = self.current(&key, generation)?.retire(instance, segment);
                Some((
                    key,
                    if newly {
                        Change::Held(instance)
                    } else {
                        Change::Table
                    },
                ))
            }
            FollowEvent::Left {
                class,
                cell,
                generation,
                instance,
                ..
            } => {
                let key = (class, cell);
                if !self.current(&key, generation)?.leave(instance) {
                    return None;
                }
                Some((key, Change::Released(instance)))
            }
            FollowEvent::Dropped {
                class,
                cell,
                generation,
            } => {
                let key = (class, cell);
                if self.current(&key, generation).is_some() {
                    let follow = self.follows.remove(&key)?;
                    return Some((key, Change::Ended(Box::new(follow))));
                }
                let follow = self.closing.remove(&(key.clone(), generation))?;
                Some((key, Change::Ended(Box::new(follow))))
            }
            // The snapshot's marker, which the task reads.
            FollowEvent::Synced => None,
        }
    }

    /// `key`'s follow, if `generation` is its current one.
    fn current(&mut self, key: &Key, generation: u64) -> Option<&mut Follow> {
        self.follows
            .get_mut(key)
            .filter(|follow| follow.generation == generation)
    }

    /// `key`'s follow of `generation`, current or earlier.
    fn find(&self, key: &Key, generation: u64) -> Option<&Follow> {
        match self.follows.get(key) {
            Some(follow) if follow.generation == generation => Some(follow),
            _ => self.closing.get(&(key.clone(), generation)),
        }
    }

    fn find_mut(&mut self, key: &Key, generation: u64) -> Option<&mut Follow> {
        if self
            .follows
            .get(key)
            .is_some_and(|follow| follow.generation == generation)
        {
            return self.follows.get_mut(key);
        }
        self.closing.get_mut(&(key.clone(), generation))
    }

    /// Whether a follow not offered has yet to be acknowledged, or to be
    /// sent again.
    fn awaiting(&self) -> bool {
        self.follows
            .values()
            .chain(self.closing.values())
            .inspect(|_| scanned())
            .any(|follow| !follow.offered && matches!(follow.ack, Ack::None | Ack::Failed(_)))
    }

    /// The acknowledgements due at `now`, marked as being sent.
    fn due(&mut self, now: Instant) -> Vec<Unoffered> {
        let mut due = Vec::new();
        let follows = self.follows.iter_mut().chain(
            self.closing
                .iter_mut()
                .map(|((key, _), follow)| (key, follow)),
        );
        for (key, follow) in follows {
            scanned();
            let ready = match follow.ack {
                Ack::None => true,
                Ack::Failed(at) => at <= now,
                Ack::Sending(_) | Ack::Sent(_) => false,
            };
            if follow.offered || !ready || !follow.leases.idle() {
                continue;
            }
            follow.ack = Ack::Sending(follow.revision);
            due.push(Unoffered {
                class: key.0.clone(),
                cell: key.1.clone(),
                generation: follow.generation,
                revision: follow.revision,
            });
        }
        due
    }
}

/// Where a follow's acknowledgement of its current revision is, while it
/// is not offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ack {
    /// Not sent: the follow is offered, or a lease on it is held.
    None,
    /// Sent at this revision, and not yet answered.
    Sending(u64),
    /// The hub took it at this revision.
    Sent(u64),
    /// The hub did not take it: sent again from this instant.
    Failed(Instant),
}

/// One cell's follow, current or earlier.
#[derive(Debug)]
struct Follow {
    generation: u64,
    region: String,
    state: CellState,
    /// The cell's load summary, as it arrived.
    load: Option<Load>,
    /// Exposed entry instances that have not retired, with their segments.
    exposed: HashMap<InstanceId, (String, PeerInfo)>,
    /// Entry instances a state that takes no work hid: not routable, and
    /// held until they retire, leave, or are exposed again, or the follow
    /// is dropped, since what was offered to them may still be under way.
    hidden: HashMap<InstanceId, (String, PeerInfo)>,
    /// Retired entry instances, with their segments: never exposed again.
    retired: HashMap<InstanceId, String>,
    /// The built-in routers' turn per entry segment, carried into each
    /// table the follow makes.
    turns: BTreeMap<String, Arc<AtomicUsize>>,
    /// The offers this instance holds to the follow.
    leases: Arc<Leases>,
    /// The revision of the follow's offered state, and the state.
    revision: u64,
    offered: bool,
    ack: Ack,
}

impl Follow {
    fn new(
        generation: u64,
        region: String,
        state: CellState,
        load: Option<Load>,
        segments: &BTreeSet<String>,
    ) -> Self {
        Self {
            generation,
            region,
            state,
            load,
            exposed: HashMap::new(),
            hidden: HashMap::new(),
            retired: HashMap::new(),
            turns: segments
                .iter()
                .map(|segment| (segment.clone(), Arc::default()))
                .collect(),
            leases: Arc::default(),
            revision: 1,
            offered: true,
            ack: Ack::None,
        }
    }

    /// Whether the state the cell last sent lets its instances be routed
    /// to: the relay sends `exposed` only then.
    fn exposes(&self) -> bool {
        matches!(self.state, CellState::Ready | CellState::Degraded)
    }

    /// Change the state; one that exposes nothing hides every exposed
    /// instance, which the follow still holds. The relay sends no `left`
    /// for an instance hidden when it leaves, and an `exposed` for each
    /// ready one when a state exposes them again.
    fn set_state(&mut self, state: CellState) {
        self.state = state;
        if !self.exposes() {
            self.hidden.extend(self.exposed.drain());
        }
    }

    /// `peer` of `segment` is exposed: the change, if any. One the state
    /// hid is held already; one that is exposed already, or retired, or
    /// exposed while the state allows none, changes nothing.
    fn expose(&mut self, segment: String, peer: PeerInfo) -> Option<Change> {
        let id = peer.instance_id();
        if !self.exposes() || self.exposed.contains_key(&id) || self.retired.contains_key(&id) {
            return None;
        }
        if self.hidden.remove(&id).is_some() {
            self.exposed.insert(id, (segment, peer));
            return Some(Change::Table);
        }
        self.exposed.insert(id, (segment, peer.clone()));
        Some(Change::Exposed(peer))
    }

    /// Mark `id` retired: whether the follow holds it only now.
    fn retire(&mut self, id: InstanceId, segment: String) -> bool {
        let was_held = self.exposed.remove(&id).is_some() | self.hidden.remove(&id).is_some();
        let newly = self.retired.insert(id, segment).is_none();
        newly && !was_held
    }

    /// `id` left: whether the follow held it.
    fn leave(&mut self, id: InstanceId) -> bool {
        self.exposed.remove(&id).is_some()
            | self.hidden.remove(&id).is_some()
            | self.retired.remove(&id).is_some()
    }

    /// Every instance the follow holds.
    fn held(&self) -> impl Iterator<Item = InstanceId> + '_ {
        self.exposed
            .keys()
            .chain(self.hidden.keys())
            .chain(self.retired.keys())
            .copied()
    }

    /// Take over what this instance holds of the same follow from `held`,
    /// as an earlier watch showed it: the turns, the leases, an
    /// acknowledgement of the same revision, and, while the state hides
    /// them, the instances it held that this view neither exposes nor
    /// retires.
    fn carry(&mut self, held: &Follow) {
        self.turns.clone_from(&held.turns);
        self.leases = Arc::clone(&held.leases);
        if held.revision == self.revision {
            self.ack = held.ack;
        }
        if !self.exposes() {
            for (id, instance) in held.exposed.iter().chain(&held.hidden) {
                if !self.exposed.contains_key(id) && !self.retired.contains_key(id) {
                    self.hidden.insert(*id, instance.clone());
                }
            }
        }
    }

    /// The follow's table for `segment`, its instances in id order: none
    /// eligible while it is not offered.
    fn table(&self, key: &Key, segment: &str) -> CellTable {
        #[cfg(test)]
        READ.with(|read| read.set(read.get() + self.exposed.len() + self.retired.len()));
        let mut eligible: Vec<InstanceId> = self
            .exposed
            .iter()
            .filter(|(_, (of, _))| self.offered && of == segment)
            .map(|(id, _)| *id)
            .collect();
        eligible.sort_by_key(InstanceId::as_u128);
        let mut retired: Vec<InstanceId> = self
            .retired
            .iter()
            .filter(|(_, of)| *of == segment)
            .map(|(id, _)| *id)
            .collect();
        retired.sort_by_key(InstanceId::as_u128);
        let offered = eligible
            .iter()
            .map(|id| gdl::InstanceId::from_u128(id.as_u128()))
            .collect();
        CellTable {
            class: key.0.clone(),
            cell: key.1.clone(),
            id: super::cell_id(&key.0, &key.1),
            generation: self.generation,
            region: self.region.clone(),
            state: self.state,
            load: self.load,
            eligible: eligible.into_boxed_slice(),
            offered,
            retired: retired.into_boxed_slice(),
            turn: self.turns.get(segment).map(Arc::clone).unwrap_or_default(),
            offering: self.offered,
            revision: self.revision,
            leases: Arc::clone(&self.leases),
        }
    }
}

#[cfg(test)]
thread_local! {
    /// The instances the tables made on this thread have read, so that a
    /// test counts an event's work rather than timing it.
    pub(super) static READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// The follows the acknowledgements' bookkeeping on this thread has
    /// looked at.
    pub(super) static SCANNED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Count one follow looked at for an acknowledgement, in a test.
fn scanned() {
    #[cfg(test)]
    SCANNED.with(|scanned| scanned.set(scanned.get() + 1));
}

/// The relay's snapshot as a watch delivers it, applied apart from what
/// the follower holds, which it replaces whole once it is complete: a
/// watch that ends part way through its snapshot changes nothing.
pub(super) struct Snapshot {
    wanted: Arc<Wanted>,
    followed: Followed,
}

impl Snapshot {
    /// Apply one event of the snapshot, which arrived at `now`, before its
    /// `synced`, by the same rules as after it; nothing is registered until
    /// it is complete.
    pub(super) fn apply(&mut self, event: FollowEvent, now: Instant) {
        let _ = self.followed.apply(&self.wanted, event, now);
    }
}
