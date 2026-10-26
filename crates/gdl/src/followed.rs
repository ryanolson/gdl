//! What a node reads of the cells its segment's choosers follow: the same
//! table a cell policy chooses from, and the cells whose records were
//! withdrawn after a drain. A node in a segment that sends into another
//! class (a model gate in front of a set router, say) reads it to tell a
//! class that has no cell taking work (503) from one whose cells were all
//! drained on purpose (404), from the snapshot the chooser routes by.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::policy::Cell;

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
    /// which [`FollowedCells::read_entry`] gives for any entry.
    pub fn fixed<C, N>(
        cells: impl IntoIterator<Item = Cell>,
        withdrawn: impl IntoIterator<Item = (C, N)>,
    ) -> Self
    where
        C: Into<String>,
        N: Into<String>,
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
        let fixed = Fixed(
            classes
                .into_iter()
                .map(|(class, (mut cells, mut withdrawn))| {
                    cells.sort_by(|a, b| a.name().cmp(b.name()));
                    withdrawn.sort();
                    withdrawn.dedup();
                    (class, (cells.into(), withdrawn.into()))
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
    /// One load of the table the chooser routes by, with no lock and no
    /// allocation. `read` runs while the table is held, so keep it short:
    /// copy out what outlives it.
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
}

impl<'a> ClassCells<'a> {
    /// The view of `cells`, sorted by name, and `withdrawn`, sorted.
    #[doc(hidden)]
    pub fn new(cells: &'a [Cell], withdrawn: &'a [Box<str>]) -> Self {
        Self { cells, withdrawn }
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
}

/// Fixed tables: per class, its cells by name and its withdrawn cells.
type Tables = BTreeMap<String, (Box<[Cell]>, Box<[Box<str>]>)>;

struct Fixed(Tables);

impl CellSource for Fixed {
    fn read(&self, class: &str, _entry: Option<&str>, read: &mut dyn FnMut(ClassCells<'_>)) {
        if let Some((cells, withdrawn)) = self.0.get(class) {
            read(ClassCells::new(cells, withdrawn));
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
    }
}
