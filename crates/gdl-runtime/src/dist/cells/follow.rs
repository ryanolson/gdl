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
//! An event of a kind this build does not know leaves the cell it names
//! unread: its tables offer none of its instances until the follower has
//! read the cell's follow again ([`Follows::reread`]), and what the relay
//! sends of the cell meanwhile is held, then applied over what was read,
//! since the reading and the watch are not ordered with each other.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use gdl_cells::CellState;
use gdl_watch::{FollowEvent, UnknownFollowEvent, Watched};
use tokio::time::Instant;
use velo::{InstanceId, PeerInfo};

use super::{CellTable, Cells, EntryTable, Load};
use crate::dist::fabric::Registry;
use crate::dist::roster::Live;

/// A followed cell, by its class and its name within the class: two
/// classes may each have a cell of one name.
pub(super) type Key = (String, String);

/// An item of the relay held while its cell is read again.
type Held = Watched<FollowEvent, UnknownFollowEvent>;

/// The most items of the relay held for one cell while it is read again:
/// past it, the watch ends, and the follower watches again from a
/// snapshot, as a watcher that falls behind the hub's buffer does.
const HELD: usize = 1024;

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
    /// The cells unread being read again, each with what the relay has
    /// sent of it since its reading began, to apply over what is read.
    reading: HashMap<Key, Vec<Held>>,
    /// The readings asked for that the task has not yet begun.
    asked: Vec<Key>,
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
            reading: HashMap::new(),
            asked: Vec::new(),
        }
    }

    /// A snapshot of the relay to fill, as a watch begins.
    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            wanted: Arc::clone(&self.wanted),
            followed: Followed::default(),
            unread: BTreeSet::new(),
            unknown: 0,
        }
    }

    /// Take one event of the relay after its snapshot, at `now`: held while
    /// its cell is read again, applied otherwise. `false` when a reading
    /// has held as much as it may, and the watch is to end.
    pub(super) fn take(&mut self, event: FollowEvent, now: Instant) -> bool {
        // A key is made only while a cell is being read, which is rare.
        if !self.reading.is_empty()
            && let Some(key) = cell_of(&event)
            && let Some(held) = self.reading.get_mut(&key)
        {
            if held.len() >= HELD {
                return false;
            }
            held.push(Watched::Event(event));
            return true;
        }
        self.apply(event, now);
        true
    }

    /// The relay sent an event of a kind this build does not know after
    /// its snapshot: the cell it names is unread until it is read again.
    pub(super) fn unknown(&mut self, event: UnknownFollowEvent) {
        self.cells.unknown();
        self.unread(event);
    }

    /// Leave `event`'s cell unread, and ask for it to be read again, unless
    /// the event is of a class not sent to, or of an earlier generation
    /// than the cell's: then it changes nothing, as a known event of it
    /// would not. One that comes while the cell is read is held, and the
    /// cell read again after.
    fn unread(&mut self, event: UnknownFollowEvent) {
        let key = (event.class, event.cell);
        if let Some(held) = self.reading.get_mut(&key) {
            held.push(Watched::Unknown(UnknownFollowEvent {
                kind: event.kind,
                class: key.0,
                cell: key.1,
                generation: event.generation,
            }));
            return;
        }
        if !self.followed.unread(&self.wanted, &key, event.generation) {
            return;
        }
        tracing::info!(
            class = key.0,
            cell = key.1,
            kind = event.kind,
            "the relay sent an event of a kind this build does not know; the cell is not chosen until it is read again"
        );
        self.publish(&key);
        self.reading.insert(key.clone(), Vec::new());
        self.asked.push(key);
    }

    /// The cells to read again that the task has not yet begun reading.
    pub(super) fn asked(&mut self) -> Vec<Key> {
        std::mem::take(&mut self.asked)
    }

    /// The cell `key` was read again, at `now`, as the relay's follow of it
    /// alone shows it (`events`, without the marker): it replaces the
    /// cell's follow, or ends it if the relay has none, and what the relay
    /// sent of the cell since its reading began is applied over it, by the
    /// rules of any event. Its new instances are registered before a table
    /// shows them; one it no longer holds is unregistered after the drain.
    /// `false` when what was held ends the watch. A reading for a cell that
    /// a new snapshot has replaced since is ignored.
    pub(super) fn reread(&mut self, key: Key, events: Vec<FollowEvent>, now: Instant) -> bool {
        let Some(held) = self.reading.remove(&key) else {
            return true;
        };
        let mut read = Followed::default();
        for event in events {
            let _ = read.apply(&self.wanted, event, now);
        }
        let mut fresh = read.follows.remove(&key);
        if let Some(follow) = &mut fresh {
            let unreachable: Vec<InstanceId> = follow
                .exposed
                .iter()
                .filter(|(_, (_, peer))| !self.register(peer))
                .map(|(id, _)| *id)
                .collect();
            for id in unreachable {
                follow.exposed.remove(&id);
            }
            // Held before the follow it replaces lets go, so that an
            // instance in both is never unregistered.
            for id in follow.held().collect::<Vec<_>>() {
                self.hold(id);
            }
        }
        let replaced = match fresh {
            Some(mut follow) => {
                let generation = follow.generation;
                if let Some(held) = self.followed.follows.get(&key)
                    && held.generation == generation
                {
                    follow.turns.clone_from(&held.turns);
                }
                let newest = self.followed.generations.entry(key.clone()).or_default();
                *newest = (*newest).max(generation);
                self.followed.follows.insert(key.clone(), follow)
            }
            None => self.followed.follows.remove(&key),
        };
        if let Some(replaced) = replaced {
            self.release(&replaced, now);
        }
        self.publish(&key);
        for item in held {
            match item {
                Watched::Event(event) => {
                    if !self.take(event, now) {
                        return false;
                    }
                }
                Watched::Unknown(event) => self.unread(event),
            }
        }
        true
    }

    /// Apply one event of the relay after its snapshot, at `now`, by
    /// [`Followed::apply`]'s rules: what it changed is then published,
    /// with an exposed instance registered first and every instance no
    /// follow holds any more unregistered after the drain.
    pub(crate) fn apply(&mut self, event: FollowEvent, now: Instant) {
        let (key, change) = match self.followed.apply(&self.wanted, event, now) {
            Some(changed) => changed,
            None => return,
        };
        match change {
            Change::Table => {}
            Change::Ended(follow) => self.release(&follow, now),
            Change::Hidden(ids) => {
                for id in ids {
                    self.unhold(id, now);
                }
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
    /// registered before any table shows them; an instance that no table
    /// holds any more is unregistered after the drain.
    pub(super) fn resnapshot(&mut self, snapshot: Snapshot, now: Instant) {
        let Snapshot {
            mut followed,
            unread,
            unknown,
            ..
        } = snapshot;
        // A reading under way is of the watch that ended; the snapshot
        // replaces what it would have read.
        self.reading.clear();
        self.asked.clear();
        for _ in 0..unknown {
            self.cells.unknown();
        }
        for key in unread {
            if let Some(follow) = followed.follows.get_mut(&key) {
                follow.unread = true;
            }
            self.reading.insert(key.clone(), Vec::new());
            self.asked.push(key);
        }
        for follow in followed.follows.values_mut() {
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
        let mut held: HashMap<InstanceId, usize> = HashMap::new();
        for follow in followed.follows.values() {
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
        // A follow shown again in its generation keeps its turns, so that
        // a round robin within the cell goes on across rewatches rather
        // than starting at its first instance each time. A restarted hub
        // numbers its follows from 1 again, so the follow may be a new one
        // under an old number; a turn is only where a round robin starts,
        // so carrying it then costs nothing.
        for (key, follow) in &mut followed.follows {
            if let Some(held) = self.followed.follows.get(key)
                && held.generation == follow.generation
            {
                follow.turns.clone_from(&held.turns);
            }
        }
        self.followed = followed;
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
        self.cells.changed();
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

/// The follows one watch has shown: each cell's current follow, and the
/// newest generation of each cell, so that an older one is ignored even
/// after its cell's follow was dropped. What the follower holds, and a
/// snapshot being filled, are each one of these, changed by the same
/// rules.
#[derive(Default)]
struct Followed {
    follows: HashMap<Key, Follow>,
    generations: HashMap<Key, u64>,
}

/// What one event changed in a cell's follow, beyond its table.
enum Change {
    /// Only what the table shows.
    Table,
    /// The follow ended: dropped, or replaced by a newer generation.
    Ended(Follow),
    /// A state that exposes nothing hid these.
    Hidden(Vec<InstanceId>),
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
    /// of a generation that is not its cell's current one, which changes
    /// nothing.
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
                // of the older one is carried over.
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
                Some((key, older.map_or(Change::Table, Change::Ended)))
            }
            FollowEvent::State {
                class,
                cell,
                generation,
                state,
            } => {
                let key = (class, cell);
                let hidden = self.current(&key, generation)?.set_state(state);
                Some((key, Change::Hidden(hidden)))
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
                let follow = self.current(&key, generation)?;
                let id = peer.instance_id();
                if !follow.can_expose(id) {
                    return None;
                }
                follow.exposed.insert(id, (segment, peer.clone()));
                Some((key, Change::Exposed(peer)))
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
                self.current(&key, generation)?;
                let follow = self.follows.remove(&key)?;
                Some((key, Change::Ended(follow)))
            }
            // The snapshot's marker, which the task reads.
            FollowEvent::Synced => None,
        }
    }

    /// An event of a kind this build does not know names `key`'s follow of
    /// `generation`: whether the cell is to be read again, left unread
    /// meanwhile. Not for a class not sent to, or a generation earlier
    /// than the cell's current or newest one.
    fn unread(&mut self, wanted: &Wanted, key: &Key, generation: u64) -> bool {
        if !wanted.contains_key(&key.0) {
            return false;
        }
        match self.follows.get_mut(key) {
            Some(follow) if follow.generation > generation => false,
            Some(follow) => {
                follow.unread = true;
                true
            }
            None => self
                .generations
                .get(key)
                .is_none_or(|newest| *newest < generation),
        }
    }

    /// `key`'s follow, if `generation` is its current one.
    fn current(&mut self, key: &Key, generation: u64) -> Option<&mut Follow> {
        self.follows
            .get_mut(key)
            .filter(|follow| follow.generation == generation)
    }
}

/// One cell's current follow.
#[derive(Debug)]
struct Follow {
    generation: u64,
    region: String,
    state: CellState,
    /// The cell's load summary, as it arrived.
    load: Option<Load>,
    /// Exposed entry instances that have not retired, with their segments.
    exposed: HashMap<InstanceId, (String, PeerInfo)>,
    /// Retired entry instances, with their segments: never exposed again.
    retired: HashMap<InstanceId, String>,
    /// The built-in routers' turn per entry segment, carried into each
    /// table the follow makes.
    turns: BTreeMap<String, Arc<AtomicUsize>>,
    /// Whether the relay sent an event of a kind this build does not know
    /// for the follow, which the follower has not read again since: its
    /// tables then offer none of its instances, so that it is not chosen.
    /// The one place a cell is kept from choosers for it.
    unread: bool,
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
            retired: HashMap::new(),
            turns: segments
                .iter()
                .map(|segment| (segment.clone(), Arc::default()))
                .collect(),
            unread: false,
        }
    }

    /// Whether the state the cell last sent lets its instances be routed
    /// to: the relay sends `exposed` only then.
    fn exposes(&self) -> bool {
        matches!(self.state, CellState::Ready | CellState::Degraded)
    }

    /// Whether `id` may be exposed: the state allows it, and it is neither
    /// exposed already nor retired.
    fn can_expose(&self, id: InstanceId) -> bool {
        self.exposes() && !self.exposed.contains_key(&id) && !self.retired.contains_key(&id)
    }

    /// Change the state; one that exposes nothing hides every exposed
    /// instance, and returns them. The relay sends no `left` for an
    /// instance hidden when it leaves, and an `exposed` for each when a
    /// state exposes them again, so a hidden instance is dropped.
    fn set_state(&mut self, state: CellState) -> Vec<InstanceId> {
        self.state = state;
        if self.exposes() {
            return Vec::new();
        }
        self.exposed.drain().map(|(id, _)| id).collect()
    }

    /// Mark `id` retired: whether the follow holds it only now.
    fn retire(&mut self, id: InstanceId, segment: String) -> bool {
        let was_exposed = self.exposed.remove(&id).is_some();
        let newly = self.retired.insert(id, segment).is_none();
        newly && !was_exposed
    }

    /// `id` left: whether the follow held it.
    fn leave(&mut self, id: InstanceId) -> bool {
        self.exposed.remove(&id).is_some() | self.retired.remove(&id).is_some()
    }

    /// Every instance the follow holds.
    fn held(&self) -> impl Iterator<Item = InstanceId> + '_ {
        self.exposed.keys().chain(self.retired.keys()).copied()
    }

    /// The follow's table for `segment`, its instances in id order.
    fn table(&self, key: &Key, segment: &str) -> CellTable {
        #[cfg(test)]
        READ.with(|read| read.set(read.get() + self.exposed.len() + self.retired.len()));
        let mut eligible: Vec<InstanceId> = self
            .exposed
            .iter()
            .filter(|(_, (of, _))| !self.unread && of == segment)
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
            unread: self.unread,
            turn: self.turns.get(segment).map(Arc::clone).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
thread_local! {
    /// The instances the tables made on this thread have read, so that a
    /// test counts an event's work rather than timing it.
    pub(super) static READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The relay's snapshot as a watch delivers it, applied apart from what
/// the follower holds, which it replaces whole once it is complete: a
/// watch that ends part way through its snapshot changes nothing.
pub(super) struct Snapshot {
    wanted: Arc<Wanted>,
    followed: Followed,
    /// The cells an event of a kind this build does not know named, to
    /// read again once the snapshot is complete.
    unread: BTreeSet<Key>,
    /// How many such events it had.
    unknown: u64,
}

impl Snapshot {
    /// Apply one event of the snapshot, which arrived at `now`, before its
    /// `synced`, by the same rules as after it; nothing is registered until
    /// it is complete.
    pub(super) fn apply(&mut self, event: FollowEvent, now: Instant) {
        let _ = self.followed.apply(&self.wanted, event, now);
    }

    /// An event of a kind this build does not know, before the snapshot's
    /// `synced`: its cell is read again once the snapshot is complete.
    pub(super) fn unknown(&mut self, event: UnknownFollowEvent) {
        self.unknown += 1;
        let key = (event.class, event.cell);
        if self.followed.unread(&self.wanted, &key, event.generation) {
            self.unread.insert(key);
        }
    }
}

/// The cell `event` names, by class and name; `None` for the marker.
fn cell_of(event: &FollowEvent) -> Option<Key> {
    match event {
        FollowEvent::Follow { class, cell, .. }
        | FollowEvent::State { class, cell, .. }
        | FollowEvent::Load { class, cell, .. }
        | FollowEvent::Exposed { class, cell, .. }
        | FollowEvent::Retired { class, cell, .. }
        | FollowEvent::Left { class, cell, .. }
        | FollowEvent::Dropped { class, cell, .. } => Some((class.clone(), cell.clone())),
        FollowEvent::Synced => None,
    }
}
