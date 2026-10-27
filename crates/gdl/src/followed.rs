//! What a node reads of the cells its segment's choosers follow: the same
//! table a cell policy chooses from, the cells the chooser holds gone, and
//! the cells whose records were withdrawn after a drain. A node in a
//! segment that sends into another class (a model gate in front of a set
//! router, say) reads it to tell a class that has no cell taking work
//! (503) from one whose cells were all drained on purpose (404), from the
//! state the chooser routes by.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::policy::{Cell, Damped};

mod index;

pub(crate) use index::NO_MODELS;
pub use index::{ModelIndex, ModelSet};

tokio::task_local! {
    /// The handle of the segment whose nodes are being built.
    static BUILDING: FollowedCells;
}

/// A handle on the cells a segment's choosers follow, for a node in the
/// segment to read. Cheap to clone; every clone reads the same tables.
///
/// A node takes it once, in [`Build::build`](crate::Build::build), with
/// [`FollowedCells::current`], and reads it per request with
/// [`FollowedCells::read`]. The runtime binds it when the segment's
/// chooser launches, after the node is built: until then, and for a
/// segment that sends into no other class or runs without a hub, it is
/// not bound, and a read gives `None`.
#[derive(Clone, Default)]
pub struct FollowedCells {
    source: Arc<OnceLock<Arc<dyn CellSource>>>,
}

impl FollowedCells {
    /// A handle that is not bound: a read gives `None` until the runtime
    /// binds it.
    pub fn new() -> Self {
        Self::default()
    }

    /// The handle of the segment whose nodes the runtime is building:
    /// call it in [`Build::build`](crate::Build::build). Outside a build,
    /// a handle that is never bound.
    pub fn current() -> Self {
        BUILDING.try_with(Self::clone).unwrap_or_default()
    }

    /// A handle bound to fixed tables, for a node's tests: `cells`, by
    /// their class, and the cells named by class and name in `withdrawn`.
    /// A class named by neither is not followed. Each class has one table,
    /// which [`FollowedCells::read_entry`] gives for any entry. No cell is
    /// gone ([`FollowedCells::fixed_with_gone`]).
    pub fn fixed<C, N>(
        cells: impl IntoIterator<Item = Cell>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
    {
        Self::fixed_with_gone(cells, withdrawn, std::iter::empty::<(String, String)>())
    }

    /// [`FollowedCells::fixed`], with the cells named by class and name in
    /// `gone` held gone ([`ClassCells::is_gone`]), as a chooser holds a
    /// cell that declined its offer as gone. A name in `gone` that is not
    /// among `cells` is not gone: a chooser holds only followed cells
    /// gone.
    pub fn fixed_with_gone<C, N, G, M>(
        cells: impl IntoIterator<Item = Cell>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
        gone: impl IntoIterator<Item = (G, M)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
        G: Into<String>,
        M: Into<String>,
    {
        Self::fixed_with_models(
            cells,
            withdrawn,
            gone,
            std::iter::empty::<(String, ModelIndex)>(),
        )
    }

    /// [`FollowedCells::fixed_with_gone`], with each class's model index
    /// ([`ClassCells::models`]) from `models`. A class given no index has
    /// [`ModelIndex::NONE`]: no model, not synced.
    pub fn fixed_with_models<C, N, G, M, K>(
        cells: impl IntoIterator<Item = Cell>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
        gone: impl IntoIterator<Item = (G, M)>,
        models: impl IntoIterator<Item = (K, ModelIndex)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
        G: Into<String>,
        M: Into<String>,
        K: Into<String>,
    {
        let mut classes: BTreeMap<String, (Vec<Cell>, Vec<Box<str>>)> = BTreeMap::new();
        for cell in cells {
            classes
                .entry(cell.class().to_string())
                .or_default()
                .0
                .push(cell);
        }
        for (class, name) in withdrawn {
            let name: String = name.into();
            classes
                .entry(class.into())
                .or_default()
                .1
                .push(name.into_boxed_str());
        }
        let gone: Vec<(String, String)> = gone
            .into_iter()
            .map(|(class, name)| (class.into(), name.into()))
            .collect();
        let mut models: BTreeMap<String, ModelIndex> = models
            .into_iter()
            .map(|(class, index)| (class.into(), index))
            .collect();
        for class in models.keys() {
            classes.entry(class.clone()).or_default();
        }
        let fixed = Fixed(
            classes
                .into_iter()
                .map(|(class, (mut cells, mut withdrawn))| {
                    cells.sort_by(|a, b| a.name().cmp(b.name()));
                    withdrawn.sort();
                    withdrawn.dedup();
                    let damped = cells
                        .iter()
                        .filter(|cell| gone.iter().any(|(c, n)| *c == class && n == cell.name()))
                        .fold(Damped::EMPTY, |damped, cell| damped.with_gone(cell));
                    let index = models.remove(&class).unwrap_or_default();
                    (class, (cells.into(), withdrawn.into(), damped, index))
                })
                .collect(),
        );
        let handle = Self::new();
        handle.bind(Arc::new(fixed));
        handle
    }

    /// Whether the runtime has bound the handle to a chooser's tables.
    pub fn is_bound(&self) -> bool {
        self.source.get().is_some()
    }

    /// Read the cells of `class` as the chooser holds them now, with
    /// `read`: `None` when the handle is not bound, the segment sends into
    /// no class `class`, or it enters `class` at more than one entry
    /// segment, whose tables can differ ([`FollowedCells::read_entry`]).
    /// One load of the table the chooser routes by and one of the cells it
    /// holds gone, with no lock and no allocation: a mark the chooser
    /// writes shows in the next read. `read` runs while both are held, so
    /// keep it short: copy out what outlives it.
    pub fn read<R>(&self, class: &str, read: impl FnOnce(ClassCells<'_>) -> R) -> Option<R> {
        self.load(class, None, read)
    }

    /// [`FollowedCells::read`] of the table of `class` that a hop entering
    /// it at the segment `entry` chooses from. A class's entry segments
    /// list the same cells, but each counts its own entry instances, so
    /// whether a cell [takes work](Cell::takes_work) can differ between
    /// them. `None` also when the segment does not enter `class` at
    /// `entry`.
    pub fn read_entry<R>(
        &self,
        class: &str,
        entry: &str,
        read: impl FnOnce(ClassCells<'_>) -> R,
    ) -> Option<R> {
        self.load(class, Some(entry), read)
    }

    fn load<R>(
        &self,
        class: &str,
        entry: Option<&str>,
        read: impl FnOnce(ClassCells<'_>) -> R,
    ) -> Option<R> {
        let source = self.source.get()?;
        let mut read = Some(read);
        let mut out = None;
        source.read(class, entry, &mut |cells| {
            if let Some(read) = read.take() {
                out = Some(read(cells));
            }
        });
        out
    }

    /// Bind the handle to `source`, unless it is bound already: whether
    /// this call bound it.
    #[doc(hidden)]
    pub fn bind(&self, source: Arc<dyn CellSource>) -> bool {
        self.source.set(source).is_ok()
    }

    /// Run `build` with this handle as the one [`FollowedCells::current`]
    /// gives.
    #[doc(hidden)]
    pub async fn scope<F: Future>(&self, build: F) -> F::Output {
        BUILDING.scope(self.clone(), build).await
    }
}

impl fmt::Debug for FollowedCells {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FollowedCells")
            .field("bound", &self.is_bound())
            .finish()
    }
}

/// Where a [`FollowedCells`] reads from: the runtime's chooser tables, or
/// fixed ones.
#[doc(hidden)]
pub trait CellSource: Send + Sync + 'static {
    /// Call `read` once with the cells of `class` as the entry segment
    /// `entry` holds them, or as its one entry segment holds them when
    /// `entry` is `None`; or not at all when there is no such table.
    fn read(&self, class: &str, entry: Option<&str>, read: &mut dyn FnMut(ClassCells<'_>));
}

/// The cells of one class that a chooser follows, as one moment's table.
#[derive(Debug, Clone, Copy)]
pub struct ClassCells<'a> {
    cells: &'a [Cell],
    withdrawn: &'a [Box<str>],
    damped: &'a Damped,
    models: &'a ModelIndex,
}

/// What a view holds gone when its source says nothing of it.
static NONE_GONE: Damped = Damped::EMPTY;

impl<'a> ClassCells<'a> {
    /// The view of `cells`, sorted by name, and `withdrawn`, sorted, with
    /// no cell gone.
    #[doc(hidden)]
    pub fn new(cells: &'a [Cell], withdrawn: &'a [Box<str>]) -> Self {
        Self {
            cells,
            withdrawn,
            damped: &NONE_GONE,
            models: &NO_MODELS,
        }
    }

    /// The view with the cells `damped` holds gone: the chooser's damping
    /// as its cell policy reads it.
    #[doc(hidden)]
    pub fn with_damped(self, damped: &'a Damped) -> Self {
        Self { damped, ..self }
    }

    /// The view with the class's model index, from the same table as its
    /// cells.
    #[doc(hidden)]
    pub fn with_models(self, models: &'a ModelIndex) -> Self {
        Self { models, ..self }
    }

    /// The class's model index: for each model, every cell of the class
    /// whose record lists it, followed or not, from the same table as
    /// [`ClassCells::cells`]. A model gate reads it to tell a model no cell
    /// lists (404) from one whose cells cannot take work now (503):
    ///
    /// ```
    /// # use gdl::{CellCondition, ClassCells, ModelState};
    /// fn answer(cells: ClassCells<'_>, model: &str) -> u16 {
    ///     let index = cells.models();
    ///     let Some(sets) = index.sets(model, None) else {
    ///         // Not listed: unknown once the directory was read.
    ///         return if index.is_synced() { 404 } else { 503 };
    ///     };
    ///     let routed = sets.iter().any(|set| {
    ///         set.state() == ModelState::Ready
    ///             && set.is_offered()
    ///             && cells.cell(set.cell()).is_some_and(gdl::Cell::takes_work)
    ///     });
    ///     if routed { 200 } else { 503 }
    /// }
    /// # let _ = (answer, CellCondition::Live);
    /// ```
    #[inline]
    pub fn models(&self) -> &'a ModelIndex {
        self.models
    }

    /// The followed cells, by name: what the class's cell policy chooses
    /// from. A cell whose follow ended is not here, whether it was
    /// withdrawn or went silent.
    #[inline]
    pub fn cells(&self) -> &'a [Cell] {
        self.cells
    }

    /// The followed cell named `name`, if it is followed.
    pub fn cell(&self, name: &str) -> Option<&'a Cell> {
        self.cells
            .binary_search_by(|cell| cell.name().cmp(name))
            .ok()
            .map(|index| &self.cells[index])
    }

    /// Whether the cell named `name` withdrew its record after its drain
    /// ended, as the chooser's hub saw it in the cell directory: a cell
    /// drained on purpose. A cell whose lease expired, whose link failed,
    /// or that this chooser never saw is not withdrawn.
    pub fn is_withdrawn(&self, name: &str) -> bool {
        self.withdrawn
            .binary_search_by(|withdrawn| (**withdrawn).cmp(name))
            .is_ok()
    }

    /// The names of the withdrawn cells, sorted.
    pub fn withdrawn(&self) -> impl Iterator<Item = &'a str> + 'a {
        self.withdrawn.iter().map(|name| &**name)
    }

    /// Whether the chooser holds the followed cell named `name` gone: the
    /// cell declined its offer as gone, as a draining cell or a segment
    /// that declines by its own policy does, and the relay has changed
    /// nothing of its follow since, nor has it accepted an offer. The
    /// chooser's cell policies pass such a cell over, whatever its state
    /// and entries say ([`Damped::is_gone`]). A cell that is not followed
    /// is not gone.
    pub fn is_gone(&self, name: &str) -> bool {
        self.cell(name)
            .is_some_and(|cell| self.damped.is_gone(cell))
    }
}

/// Fixed tables: per class, its cells by name, its withdrawn cells, its
/// gone cells, and its model index.
type Tables = BTreeMap<String, (Box<[Cell]>, Box<[Box<str>]>, Damped, ModelIndex)>;

struct Fixed(Tables);

impl CellSource for Fixed {
    fn read(&self, class: &str, _entry: Option<&str>, read: &mut dyn FnMut(ClassCells<'_>)) {
        if let Some((cells, withdrawn, damped, models)) = self.0.get(class) {
            read(
                ClassCells::new(cells, withdrawn)
                    .with_damped(damped)
                    .with_models(models),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CellState;

    /// A handle outside a build is never bound; inside one, it is the
    /// handle the build runs with, so a bind of that one reaches it.
    #[tokio::test]
    async fn current_is_the_building_segments_handle() {
        assert!(!FollowedCells::current().is_bound());
        let segment = FollowedCells::new();
        let taken = segment.scope(async { FollowedCells::current() }).await;
        assert!(!taken.is_bound());
        assert!(segment.bind(Arc::new(Fixed(Tables::new()))));
        assert!(taken.is_bound(), "another handle");
        assert!(!segment.bind(Arc::new(Fixed(Tables::new()))), "bound twice");
    }

    /// Fixed tables read by class: each class's cells by name, and its
    /// withdrawn cells; a class named nowhere is not followed.
    #[test]
    fn fixed_tables_read_by_class() {
        let followed = FollowedCells::fixed(
            [
                Cell::new("model_x", "m2", "r", CellState::Draining),
                Cell::new("model_x", "m1", "r", CellState::Ready).with_entries(1),
                Cell::new("model_y", "y1", "r", CellState::Ready),
            ],
            [("model_x", "m3"), ("model_z", "z1")],
        );
        let x = followed
            .read("model_x", |cells| {
                let names: Vec<&str> = cells.cells().iter().map(Cell::name).collect();
                assert_eq!(names, ["m1", "m2"]);
                assert!(cells.cell("m1").is_some_and(Cell::takes_work));
                assert!(cells.cell("m3").is_none());
                (
                    cells.is_withdrawn("m3"),
                    cells.is_withdrawn("m1"),
                    cells.withdrawn().collect::<Vec<_>>().join(","),
                )
            })
            .expect("model_x");
        assert_eq!(x, (true, false, "m3".to_string()));
        assert_eq!(
            followed.read("model_z", |cells| (
                cells.cells().len(),
                cells.is_withdrawn("z1")
            )),
            Some((0, true))
        );
        assert!(followed.read("model_w", |_| ()).is_none());
        assert_eq!(
            followed.read_entry("model_x", "any", |cells| cells.cells().len()),
            Some(2)
        );
        assert_eq!(
            followed.read("model_x", |cells| cells.is_gone("m1")),
            Some(false),
            "fixed tables hold no cell gone"
        );
    }

    /// Fixed tables give each class the model index they were given, from
    /// the same read as its cells; a class given none reads `NONE`, and a
    /// class given only an index is read as followed with no cell.
    #[test]
    fn fixed_tables_read_each_classs_model_index() {
        use gdl_cells::{CellCondition, ModelState};
        let index = ModelIndex::new(
            true,
            [(
                gdl_model::ModelKey::new("qwen"),
                ModelSet::new("m2", "eu-west", ModelState::Ready, CellCondition::Missing),
            )],
        );
        let followed = FollowedCells::fixed_with_models(
            [
                Cell::new("model_x", "m1", "r", CellState::Ready).with_entries(1),
                Cell::new("model_y", "y1", "r", CellState::Ready),
            ],
            std::iter::empty::<(String, String)>(),
            std::iter::empty::<(String, String)>(),
            [
                ("model_x", index.clone()),
                ("model_z", ModelIndex::new(true, [])),
            ],
        );
        let x = followed
            .read("model_x", |cells| {
                let sets = cells.models().sets("qwen", None).expect("listed");
                (
                    cells.cells().len(),
                    sets[0].cell().to_string(),
                    sets[0].condition(),
                    cells.cell(sets[0].cell()).is_some(),
                )
            })
            .expect("model_x");
        assert_eq!(x, (1, "m2".to_string(), CellCondition::Missing, false));
        assert_eq!(
            followed.read("model_y", |cells| cells.models().is_synced()),
            Some(false),
            "no index given"
        );
        assert_eq!(
            followed.read("model_z", |cells| (
                cells.cells().len(),
                cells.models().is_synced()
            )),
            Some((0, true))
        );
        assert_eq!(
            FollowedCells::fixed(std::iter::empty::<Cell>(), [("model_x", "m3")])
                .read("model_x", |cells| *cells.models() == ModelIndex::NONE),
            Some(true)
        );
    }

    /// Fixed tables hold gone the followed cells named so, as a chooser
    /// does: whatever their state and entries say, and in their class
    /// alone. A gone name that is not followed is not gone.
    #[test]
    fn fixed_tables_hold_the_named_cells_gone() {
        let followed = FollowedCells::fixed_with_gone(
            [
                Cell::new("model_x", "m1", "r", CellState::Ready).with_entries(1),
                Cell::new("model_x", "m2", "r", CellState::Ready).with_entries(1),
                Cell::new("model_y", "m1", "r", CellState::Ready).with_entries(1),
            ],
            [("model_x", "m3")],
            [("model_x", "m1"), ("model_x", "m3")],
        );
        let x = followed
            .read("model_x", |cells| {
                (
                    cells.cell("m1").is_some_and(Cell::takes_work),
                    cells.is_gone("m1"),
                    cells.is_gone("m2"),
                    cells.is_gone("m3"),
                )
            })
            .expect("model_x");
        assert_eq!(x, (true, true, false, false));
        assert_eq!(
            followed.read("model_y", |cells| cells.is_gone("m1")),
            Some(false),
            "another class's cell of the same name"
        );
    }
}
