//! What a chooser's follower does with the relay's events: its state, the
//! tables it publishes from it, and the velo registrations it keeps, with
//! no I/O of its own, so that every rule here is tested without a hub.
//!
//! An event changes one cell's follow, and costs the work of that cell's
//! table: the cell's table is made again from its own instances, and its
//! segment's table is a new list of the cells' tables, one pointer per
//! cell, so no event reads another cell's instances. Only a new snapshot
//! makes every table again.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use gdl_cells::CellState;
use gdl_watch::FollowEvent;
use tokio::time::Instant;
use velo::{InstanceId, PeerInfo};

use super::{CellTable, Cells, EntryTable};
use crate::dist::fabric::Registry;
use crate::dist::roster::Live;

/// A followed cell, by its class and its name within the class: two
/// classes may each have a cell of one name.
type Key = (String, String);

/// The follower's state: each followed cell's current follow, the tables
/// made from them, and the instances it registered with velo.
pub(super) struct Follows<R> {
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
}

impl<R: Registry> Follows<R> {
    pub(super) fn new(
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
    /// follow holds any more unregistered after the drain.
    pub(super) fn apply(&mut self, event: FollowEvent, now: Instant) {
        let (key, change) = match self.followed.apply(&self.wanted, event) {
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
        let Snapshot { mut followed, .. } = snapshot;
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
    pub(super) fn forget(&mut self, now: Instant) {
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
pub(super) type Wanted = BTreeMap<String, BTreeSet<String>>;

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
    /// Apply one event: the cell it changed, and how, or `None` for an
    /// event of a class or segment not sent to, or of a generation that is
    /// not its cell's current one, which changes nothing.
    fn apply(&mut self, wanted: &Wanted, event: FollowEvent) -> Option<(Key, Change)> {
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
                ..
            } => {
                if !wanted.contains_key(&class) {
                    return None;
                }
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
                let older = self
                    .follows
                    .insert(key.clone(), Follow::new(generation, region, state));
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
    /// Exposed entry instances that have not retired, with their segments.
    exposed: HashMap<InstanceId, (String, PeerInfo)>,
    /// Retired entry instances, with their segments: never exposed again.
    retired: HashMap<InstanceId, String>,
}

impl Follow {
    fn new(generation: u64, region: String, state: CellState) -> Self {
        Self {
            generation,
            region,
            state,
            exposed: HashMap::new(),
            retired: HashMap::new(),
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
            .filter(|(_, (of, _))| of == segment)
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
        CellTable {
            class: key.0.clone(),
            cell: key.1.clone(),
            generation: self.generation,
            region: self.region.clone(),
            state: self.state,
            eligible: eligible.into_boxed_slice(),
            retired: retired.into_boxed_slice(),
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
}

impl Snapshot {
    /// Apply one event of the snapshot, before its `synced`, by the same
    /// rules as after it; nothing is registered until it is complete.
    pub(super) fn apply(&mut self, event: FollowEvent) {
        let _ = self.followed.apply(&self.wanted, event);
    }
}
