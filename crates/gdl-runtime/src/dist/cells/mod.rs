//! The cells a chooser follows, and how it routes by them: for each entry
//! segment of the hops it sends into another class, and each cell of that
//! class its hub follows, the entry instances it may route to. A chooser
//! is an instance in a cell whose segment holds the outer end of a hop
//! that leaves its class. It reads its own hub's relay
//! (`GET /v1/ext/gdl-cell/follows`) for its life and keeps the tables a
//! request chooses from, one immutable table per entry segment, swapped
//! whole, as the roster's is. Each hand-off into the other class chooses
//! a cell from its table by the class's cell policy, then an instance of
//! that cell ([`Crossing`]), and offers the request to another cell when
//! that one declines it (`Runtime::fail_over`), damping the cell that
//! declined for the requests after it ([`damping`]). Each offer holds a
//! lease on the cell's follow until its response ends ([`CellLease`]), so
//! that the follower can tell its hub when it no longer holds an offer to
//! a cell the hub stopped offering.

mod crossing;
mod damping;
mod follow;
mod lease;
mod offer;
mod policy;
#[cfg(test)]
pub(crate) mod served;
mod task;
#[cfg(test)]
mod tests;

pub(crate) use crossing::{Crossing, Target, targets};
pub(crate) use lease::CellLease;
pub(crate) use offer::answer;
pub(crate) use task::{REWATCH, REWATCH_CAP, follow, hub_address};
// The membership's tests drive the follower beside their own.
#[cfg(test)]
pub(crate) use follow::Follows;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
pub(crate) use damping::Damping;
use gdl_cells::{CellState, LoadSummary};
use gdl_model::Layout;
use lease::Leases;
use tokio::sync::watch;
use tokio::time::Instant;
use velo::InstanceId;

/// The tables of the cells a chooser instance follows, shared by its
/// follower, which writes them, and whoever reads them. Cheap to clone.
///
/// Per entry segment, one table of every followed cell of the segment's
/// class that the chooser's hub relays: the cell's current follow (its
/// generation), its state, its region, its load summary with the time it
/// arrived, whether it is offered, the entry instances it exposes while it
/// is, and those that retired, which are never routed to. A table changes
/// only when the relay says so, and is replaced whole. The chooser's own
/// region is kept with them, for the cell policy.
#[derive(Clone)]
pub struct Cells {
    shared: Arc<Shared>,
}

struct Shared {
    /// The region of the chooser's own cell.
    region: Arc<str>,
    /// Fixed at the launch: one per entry segment the instance sends to,
    /// each shared with the hand-offs that choose from it.
    segments: BTreeMap<String, Slot>,
    /// For each class sent to, its entry segments' tables by segment, which
    /// a node reads ([`gdl::FollowedCells`]).
    classes: BTreeMap<String, Box<[(String, Slot)]>>,
    /// Counts each change of a table, or of what is registered with velo.
    changes: watch::Sender<u64>,
    /// The cells this chooser damps, shared by every hand-off that
    /// chooses from the tables.
    damping: Arc<Damping>,
    /// The instances the follower registered with velo, mirrored for
    /// readers; the follower keeps its own copy. Its lock is also the one
    /// lock over the two owners of a peer's registration, this follower
    /// and the instance's membership ([`Cells::exclusive`]).
    registered: watch::Sender<HashSet<InstanceId>>,
    /// The peers the instance's membership saw leave and has not yet
    /// unregistered: until its own drain ends, the follower leaves them
    /// registered. Changed and read only under `registered`'s lock.
    draining: Mutex<HashSet<InstanceId>>,
    /// The events of a kind this build does not know the relay has sent.
    unknown: std::sync::atomic::AtomicU64,
}

impl Cells {
    /// Empty tables for `entries`, each an entry segment and its class, of
    /// a chooser in `region` that damps the cells by `damping`.
    pub(crate) fn new<'a>(
        region: &str,
        entries: impl IntoIterator<Item = (&'a str, &'a str)>,
        damping: Damping,
    ) -> Self {
        let mut wanted: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for (class, segment) in entries {
            wanted.entry(class).or_default().insert(segment);
        }
        let segments: BTreeMap<String, Slot> = wanted
            .values()
            .flatten()
            .map(|segment| (segment.to_string(), Arc::default()))
            .collect();
        let classes = wanted
            .iter()
            .map(|(class, entries)| {
                let tables = entries
                    .iter()
                    .map(|entry| (entry.to_string(), Arc::clone(&segments[*entry])))
                    .collect();
                (class.to_string(), tables)
            })
            .collect();
        Self {
            shared: Arc::new(Shared {
                region: Arc::from(region),
                segments,
                classes,
                changes: watch::Sender::new(0),
                damping: Arc::new(damping),
                registered: watch::Sender::new(HashSet::new()),
                draining: Mutex::default(),
                unknown: std::sync::atomic::AtomicU64::new(0),
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
        let now = Instant::now();
        let damped = self.shared.damping.view();
        table
            .cells
            .iter()
            .zip(&table.views)
            .map(|(cell, view)| FollowedCell {
                class: cell.class.clone(),
                cell: cell.cell.clone(),
                generation: cell.generation,
                region: cell.region.clone(),
                state: cell.state,
                load: cell.load.map(|load| *load.summary()),
                load_age: cell
                    .load
                    .map(|load| now.saturating_duration_since(load.arrived())),
                damped: damped
                    .is_damped(cell.id)
                    .then(|| std::time::Duration::from_millis(damped.left(cell.id, now))),
                gone: damped.is_gone(view),
                eligible: cell.eligible.to_vec(),
                retired: cell.retired.to_vec(),
                offered: cell.offering,
                revision: cell.revision,
                leases: cell.leases.held(),
                unread: cell.unread,
            })
            .collect()
    }

    /// How many events of a kind this build does not know the relay has
    /// sent this chooser: each left its cell unread until the chooser read
    /// it again.
    pub fn unknown_events(&self) -> u64 {
        self.shared
            .unknown
            .load(std::sync::atomic::Ordering::Relaxed)
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
        self.shared.segments.get(segment).map(|slot| slot.load())
    }

    /// Where `segment`'s table is kept, for a hand-off that chooses from
    /// it on every request without looking it up.
    pub(crate) fn slot(&self, segment: &str) -> Option<Slot> {
        self.shared.segments.get(segment).map(Arc::clone)
    }

    /// The region of the chooser's own cell.
    pub(crate) fn region(&self) -> Arc<str> {
        Arc::clone(&self.shared.region)
    }

    /// The cells this chooser damps.
    pub(crate) fn damping(&self) -> Arc<Damping> {
        Arc::clone(&self.shared.damping)
    }

    /// Count one event of a kind this build does not know.
    fn unknown(&self) {
        self.shared
            .unknown
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

/// What a node reads of the tables: the table an entry segment's cell
/// policy chooses from, with the class's withdrawn cells, in one load.
impl gdl::__runtime::CellSource for Cells {
    fn read(&self, class: &str, entry: Option<&str>, read: &mut dyn FnMut(gdl::ClassCells<'_>)) {
        let Some(tables) = self.shared.classes.get(class) else {
            return;
        };
        let slot = match (entry, &**tables) {
            (None, [(_, slot)]) => slot,
            (None, _) => return,
            (Some(entry), tables) => match tables.iter().find(|(of, _)| of == entry) {
                Some((_, slot)) => slot,
                None => return,
            },
        };
        let table = slot.load();
        read(gdl::ClassCells::new(&table.views, &table.withdrawn));
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
    /// The cell's load summary, as its hub last sent it: `None` while it
    /// has none. `home_first` chooses by it, and not at all without it;
    /// `round_robin`, the default, does not read it.
    pub load: Option<LoadSummary>,
    /// How long ago, on this instance's clock, the summary reached this
    /// instance's hub: a new watch of the hub's relay is sent each summary
    /// with its age, so a summary the hub has held is as old on every
    /// watch. A cell's hub sends its summary every period, so this is
    /// about the time since the cell's hub last made it. `None` with no
    /// summary.
    pub load_age: Option<std::time::Duration>,
    /// Whether this chooser damps the cell, and how long its window still
    /// lasts: `Some` from a decline of its offer until it accepts one,
    /// and while the window lasts the cell is passed over, unless more than
    /// half of the eligible cells are damped; `None` when it is not damped.
    pub damped: Option<std::time::Duration>,
    /// Whether the cell declined this chooser's offer as gone, as a
    /// draining cell does, in the follow shown here: it is passed over
    /// until the relay shows a new generation, state, or offered state, or
    /// it accepts an offer.
    pub gone: bool,
    /// The entry instances of the segment that may be routed to, by id:
    /// none while the state is `draining` or `unavailable`, or the cell is
    /// unread.
    pub eligible: Vec<InstanceId>,
    /// The entry instances of the segment that retired: never routed to.
    pub retired: Vec<InstanceId>,
    /// Whether the chooser's hub offers the cell to its choosers: its link
    /// lives and its state takes work. A cell that is not offered has no
    /// eligible instance, and is never chosen.
    pub offered: bool,
    /// The revision of the follow's offered state, as the hub numbers its
    /// changes: 1 as the follow is installed, and one more for each change
    /// between offered and not offered.
    pub revision: u64,
    /// How many offers this chooser holds to the follow: requests it sent
    /// to the cell whose response has yet to end.
    pub leases: usize,
    /// Whether the cell is unread: the relay sent an event of a kind this
    /// build does not know for its follow, and the chooser has not read
    /// the follow again since. While it has not, no instance of it is
    /// eligible, so it is not chosen.
    pub unread: bool,
}

/// Where one entry segment's table is kept, swapped whole.
pub(crate) type Slot = Arc<ArcSwap<EntryTable>>;

/// One entry segment's table: each followed cell of its class, ordered by
/// class and cell, and each as a cell policy sees it, index for index;
/// and the cells of the class whose records were withdrawn.
#[derive(Debug, Default)]
pub(crate) struct EntryTable {
    pub(crate) cells: Box<[Arc<CellTable>]>,
    /// `cells` as a cell policy sees them, made with the table so that a
    /// choice borrows them rather than making them per request.
    pub(crate) views: Box<[gdl::Cell]>,
    /// The cells of the class the hub said withdrew their records after
    /// a drain, by name, sorted: kept beside the cells, in the same table,
    /// so that a node reads both from one snapshot. A withdrawn cell may
    /// still be in `cells` until its follow is dropped.
    pub(crate) withdrawn: Box<[Box<str>]>,
}

impl EntryTable {
    /// The table of `cells`, with their views, and no withdrawn cell.
    pub(crate) fn new(cells: Box<[Arc<CellTable>]>) -> Self {
        let views = cells.iter().map(|cell| cell.view()).collect();
        Self {
            cells,
            views,
            withdrawn: Box::default(),
        }
    }

    /// The table with the class's `withdrawn` cells, sorted.
    pub(crate) fn with_withdrawn(mut self, withdrawn: Box<[Box<str>]>) -> Self {
        self.withdrawn = withdrawn;
        self
    }
}

/// One followed cell, for one entry segment, in its current generation.
#[derive(Debug)]
pub(crate) struct CellTable {
    pub(crate) class: String,
    pub(crate) cell: String,
    /// The cell's name as a number ([`cell_id`]), the same in every table
    /// of the cell: what a request that the cell declined excludes.
    pub(crate) id: u64,
    pub(crate) generation: u64,
    pub(crate) region: String,
    pub(crate) state: CellState,
    /// The cell's load summary, as it arrived.
    pub(crate) load: Option<gdl::Load>,
    pub(crate) eligible: Box<[InstanceId]>,
    /// `eligible` as a selector sees it, index for index, made with the
    /// table so that a selection borrows it rather than converting each
    /// id per request.
    pub(crate) offered: Box<[gdl::InstanceId]>,
    pub(crate) retired: Box<[InstanceId]>,
    /// Whether the cell is unread ([`FollowedCell::unread`]): `eligible`
    /// is then empty.
    pub(crate) unread: bool,
    /// The built-in routers' turn within the cell, shared by every table
    /// of the cell's follow, so that a new table keeps the turn.
    pub(crate) turn: Arc<AtomicUsize>,
    /// Whether the follow is offered: `eligible` is empty when it is not.
    pub(crate) offering: bool,
    /// The revision of the follow's offered state.
    pub(crate) revision: u64,
    /// How many times the relay changed the follow's state or offered
    /// state ([`gdl::Damped::is_gone`]).
    pub(crate) changes: u64,
    /// The offers this chooser holds to the follow, shared by every table
    /// of the follow: an offer takes a lease from the table it was chosen
    /// from, and is refused one once the follow is not offered.
    pub(crate) leases: Arc<Leases>,
}

impl CellTable {
    /// The cell as a cell policy sees it.
    fn view(&self) -> gdl::Cell {
        let view = gdl::Cell::new(&*self.class, &*self.cell, &*self.region, self.state)
            .with_entries(self.eligible.len())
            .with_follow(self.generation, self.changes);
        match self.load {
            Some(load) => view.with_load(load),
            None => view,
        }
    }
}

/// The number that names a cell in its tables ([`gdl::Cell::id`]).
pub(crate) use gdl::__runtime::cell_id;

/// Refuse a chooser of `segment` whose placement names, for a class it
/// sends to, a cell policy that `policies` does not register and gdl does
/// not have. Nothing for a segment that sends into no other class.
pub(crate) fn check_policies(
    layout: &Layout,
    segment: &str,
    policies: &gdl::CellPolicies,
) -> gdl::Result<()> {
    // Only whether each name resolves: the thresholds play no part.
    for class in entries(layout, segment).keys() {
        policy::of(layout, class, policies, gdl::HomeFirstAt::DEFAULT)?;
    }
    Ok(())
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
