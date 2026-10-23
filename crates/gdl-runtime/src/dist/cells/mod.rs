//! The cells a chooser follows: for each entry segment of the hops it
//! sends into another class, and each cell of that class its hub follows,
//! the entry instances it may route to. A chooser is an instance in a cell
//! whose segment holds the outer end of a hop that leaves its class. It
//! reads its own hub's relay (`GET /v1/ext/gdl-cell/follows`) for its life
//! and keeps the tables a request will choose from, one immutable table
//! per entry segment, swapped whole, as the roster's is.

mod follow;
mod task;
#[cfg(test)]
mod tests;

pub(crate) use task::follow;
// The membership's tests drive the follower beside their own.
#[cfg(test)]
pub(crate) use follow::Follows;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
use gdl_cells::CellState;
use gdl_model::Layout;
use tokio::sync::watch;
use velo::InstanceId;

/// The tables of the cells a chooser instance follows, shared by its
/// follower, which writes them, and whoever reads them. Cheap to clone.
///
/// Per entry segment, one table of every followed cell of the segment's
/// class that the chooser's hub relays: the cell's current follow (its
/// generation), its state, its region, the entry instances it exposes,
/// and those that retired, which are never routed to. A table changes
/// only when the relay says so, and is replaced whole.
#[derive(Clone)]
pub struct Cells {
    shared: Arc<Shared>,
}

struct Shared {
    /// Fixed at the launch: one per entry segment the instance sends to.
    segments: BTreeMap<String, ArcSwap<EntryTable>>,
    /// Counts each change of a table, or of what is registered with velo.
    changes: watch::Sender<u64>,
    /// The instances the follower registered with velo, mirrored for
    /// readers; the follower keeps its own copy. Its lock is also the one
    /// lock over the two owners of a peer's registration, this follower
    /// and the instance's membership ([`Cells::exclusive`]).
    registered: watch::Sender<HashSet<InstanceId>>,
    /// The peers the instance's membership saw leave and has not yet
    /// unregistered: until its own drain ends, the follower leaves them
    /// registered. Changed and read only under `registered`'s lock.
    draining: Mutex<HashSet<InstanceId>>,
}

impl Cells {
    /// Empty tables for the entry segments `segments`.
    pub(crate) fn new<'a>(segments: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            shared: Arc::new(Shared {
                segments: segments
                    .into_iter()
                    .map(|segment| (segment.to_string(), ArcSwap::default()))
                    .collect(),
                changes: watch::Sender::new(0),
                registered: watch::Sender::new(HashSet::new()),
                draining: Mutex::default(),
            }),
        }
    }

    /// The entry segments these tables are kept for.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.shared.segments.keys().map(String::as_str)
    }

    /// The cells followed for `segment`, as its table holds them now, by
    /// class and cell; empty for a segment that has no table.
    pub fn followed(&self, segment: &str) -> Vec<FollowedCell> {
        let Some(table) = self.table(segment) else {
            return Vec::new();
        };
        table
            .cells
            .iter()
            .map(|cell| FollowedCell {
                class: cell.class.clone(),
                cell: cell.cell.clone(),
                generation: cell.generation,
                region: cell.region.clone(),
                state: cell.state,
                eligible: cell.eligible.to_vec(),
                retired: cell.retired.to_vec(),
            })
            .collect()
    }

    /// Whether the follower holds `instance` registered with velo: from
    /// before it is first in a table until `drain_ms` after it is in none.
    pub fn is_registered(&self, instance: InstanceId) -> bool {
        self.shared.registered.borrow().contains(&instance)
    }

    /// A counter of the changes to the tables and to what is registered,
    /// for a reader that waits for one.
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.shared.changes.subscribe()
    }

    /// `segment`'s table: one load, no lock, no allocation.
    pub(crate) fn table(&self, segment: &str) -> Option<arc_swap::Guard<Arc<EntryTable>>> {
        self.shared.segments.get(segment).map(ArcSwap::load)
    }

    /// Replace `segment`'s table.
    fn store(&self, segment: &str, table: EntryTable) {
        if let Some(slot) = self.shared.segments.get(segment) {
            slot.store(Arc::new(table));
        }
    }

    /// Run `act` while no registration of the follower can change, with
    /// the set the follower holds: the instance's membership registers a
    /// peer and routes to it, or decides whether to unregister one, inside
    /// it, so that its decision and the follower's never cross. One velo
    /// instance can be both a member of this instance's hub and an entry
    /// of a followed cell, and then the last owner to let go unregisters.
    pub(crate) fn exclusive<T>(&self, act: impl FnOnce(&HashSet<InstanceId>) -> T) -> T {
        act(&self.shared.registered.borrow())
    }

    /// [`Cells::exclusive`] for a chooser's tables, if the instance is a
    /// chooser; for any other instance, `act` with nothing held.
    pub(crate) fn exclusive_of<T>(
        cells: Option<&Cells>,
        act: impl FnOnce(&HashSet<InstanceId>) -> T,
    ) -> T {
        match cells {
            Some(cells) => cells.exclusive(act),
            None => act(&HashSet::new()),
        }
    }

    /// The membership saw `id` leave: `leave` marks it left, and until the
    /// membership's drain of it ends ([`Cells::drained`]) the follower does
    /// not unregister it, whatever its own drain says.
    pub(crate) fn leaving(cells: Option<&Cells>, id: InstanceId, leave: impl FnOnce()) {
        Self::exclusive_of(cells, |_| {
            leave();
            if let Some(cells) = cells {
                cells.draining().insert(id);
            }
        });
    }

    /// The membership's drain of `id` ended: `unregister` runs unless a
    /// table's registration holds it, which the follower then forgets in
    /// its own time.
    pub(crate) fn drained(cells: Option<&Cells>, id: InstanceId, unregister: impl FnOnce()) {
        Self::exclusive_of(cells, |held| {
            if let Some(cells) = cells {
                cells.draining().remove(&id);
            }
            if !held.contains(&id) {
                unregister();
            }
        });
    }

    /// Whether the membership is draining `id`; asked under the lock.
    fn membership_drains(&self, id: InstanceId) -> bool {
        self.draining().contains(&id)
    }

    fn draining(&self) -> std::sync::MutexGuard<'_, HashSet<InstanceId>> {
        self.shared
            .draining
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Change the follower's registrations with `change`, under the lock
    /// [`Cells::exclusive`] takes.
    fn registered<T>(&self, change: impl FnOnce(&mut HashSet<InstanceId>) -> T) -> T {
        let mut result = None;
        self.shared.registered.send_modify(|registered| {
            result = Some(change(registered));
        });
        result.expect("send_modify runs the change")
    }

    /// Tell readers that something changed.
    fn changed(&self) {
        self.shared.changes.send_modify(|changes| *changes += 1);
    }
}

impl std::fmt::Debug for Cells {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cells")
            .field("segments", &self.shared.segments.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// One followed cell of an entry segment, as its table holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowedCell {
    /// The cell's class.
    pub class: String,
    /// The cell, named within its class.
    pub cell: String,
    /// The generation of the hub's follow of the cell that the table is
    /// from: a refollow is a new one, and nothing of an earlier one is kept.
    pub generation: u64,
    /// The region the cell runs in.
    pub region: String,
    /// The cell's state, as its hub last sent it.
    pub state: CellState,
    /// The entry instances of the segment that may be routed to, by id:
    /// none while the state is `draining` or `unavailable`.
    pub eligible: Vec<InstanceId>,
    /// The entry instances of the segment that retired: never routed to.
    pub retired: Vec<InstanceId>,
}

/// One entry segment's table: each followed cell of its class, ordered by
/// class and cell.
#[derive(Debug, Default)]
pub(crate) struct EntryTable {
    pub(crate) cells: Box<[Arc<CellTable>]>,
}

/// One followed cell, for one entry segment, in its current generation.
#[derive(Debug)]
pub(crate) struct CellTable {
    pub(crate) class: String,
    pub(crate) cell: String,
    pub(crate) generation: u64,
    pub(crate) region: String,
    pub(crate) state: CellState,
    pub(crate) eligible: Box<[InstanceId]>,
    pub(crate) retired: Box<[InstanceId]>,
}

/// What `segment` sends into other classes: for each class one of its
/// hops crosses into, the segments holding those hops' inner ends, which
/// are the class's entry segments as this chooser reaches them. Empty for
/// a segment that is not a chooser, and for a placement without classes.
pub(crate) fn entries(layout: &Layout, segment: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut entries: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for boundary in layout.boundaries() {
        let hop = &layout.hops()[boundary.hop];
        let outer = hop.forward_runs.first().map(|run| run.segment.as_str());
        if outer != Some(segment) {
            continue;
        }
        if let Some(inner) = hop.backward_runs.first() {
            entries
                .entry(boundary.into.clone())
                .or_default()
                .insert(inner.segment.clone());
        }
    }
    entries
}
