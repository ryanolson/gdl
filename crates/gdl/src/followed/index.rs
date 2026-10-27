//! The model index of one class's cells: for each model and routing key,
//! every cell of the class whose record lists it, followed or not. A
//! chooser builds it when what it knows of the class's cells changes, off
//! the request path, and a read finds a model by binary search, with no
//! lock and no allocation.

use gdl_cells::{CellCondition, ModelState};
use gdl_model::ModelKey;

/// For each model and routing key, the cells of one class that list it.
/// A chooser's hub learns them from every record of the class in the cell
/// directory, live, missing, or scaled to 0, until the cell withdraws its
/// record, and from the follow link of each cell it follows. A node reads
/// the index with [`ClassCells::models`](crate::ClassCells::models), and a
/// cell policy with [`CellRequest::models`](crate::CellRequest::models),
/// from the same table the cells come from.
///
/// Until the hub's watch of the cell directory has completed its first
/// snapshot, the index is not synced ([`ModelIndex::is_synced`]): a model
/// it does not list may still be listed by a record the hub has not read,
/// so a gate answers 503, not 404.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelIndex {
    synced: bool,
    /// The keys, sorted by model, then routing key, each with the range of
    /// its cells in `sets`.
    keys: Vec<Key>,
    /// Each key's cells, sorted by cell name.
    sets: Vec<ModelSet>,
}

/// [`ModelIndex::NONE`], for a reference that outlives any table.
pub(crate) static NO_MODELS: ModelIndex = ModelIndex::NONE;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Key {
    model: Box<str>,
    routing_key: Option<Box<str>>,
    start: usize,
    end: usize,
}

impl Key {
    fn of(&self) -> (&str, Option<&str>) {
        (&self.model, self.routing_key.as_deref())
    }
}

impl ModelIndex {
    /// The index of a table that has none: no model, not synced.
    pub const NONE: ModelIndex = ModelIndex {
        synced: false,
        keys: Vec::new(),
        sets: Vec::new(),
    };

    /// The index of `sets`, each a cell that lists a model under its key,
    /// `synced` once the directory's first snapshot is read. A cell named
    /// twice under one key is kept once: the first.
    pub fn new(synced: bool, sets: impl IntoIterator<Item = (ModelKey, ModelSet)>) -> Self {
        let mut sets: Vec<(ModelKey, ModelSet)> = sets.into_iter().collect();
        sets.sort_by(|(a, x), (b, y)| a.cmp(b).then_with(|| x.cell.cmp(&y.cell)));
        sets.dedup_by(|(a, x), (b, y)| a == b && x.cell == y.cell);
        let mut keys: Vec<Key> = Vec::new();
        let mut flat = Vec::with_capacity(sets.len());
        for (key, set) in sets {
            match keys.last_mut() {
                Some(last) if last.of() == (key.model.as_str(), key.routing_key.as_deref()) => {
                    last.end += 1;
                }
                _ => keys.push(Key {
                    model: key.model.into_boxed_str(),
                    routing_key: key.routing_key.map(String::into_boxed_str),
                    start: flat.len(),
                    end: flat.len() + 1,
                }),
            }
            flat.push(set);
        }
        Self {
            synced,
            keys,
            sets: flat,
        }
    }

    /// Whether the chooser's hub had read the cell directory's first
    /// snapshot of the class when the index was built. Before then, a
    /// model that no cell lists is not known to be unknown.
    #[inline]
    pub fn is_synced(&self) -> bool {
        self.synced
    }

    /// The cells that list `model` under `routing_key`, by name: `None`
    /// when no cell does. One binary search, no allocation.
    pub fn sets(&self, model: &str, routing_key: Option<&str>) -> Option<&[ModelSet]> {
        let at = self
            .keys
            .binary_search_by(|key| key.of().cmp(&(model, routing_key)))
            .ok()?;
        let key = &self.keys[at];
        Some(&self.sets[key.start..key.end])
    }

    /// Each model and routing key listed, in order, with its cells.
    pub fn models(&self) -> impl Iterator<Item = (&str, Option<&str>, &[ModelSet])> {
        self.keys.iter().map(|key| {
            (
                &*key.model,
                key.routing_key.as_deref(),
                &self.sets[key.start..key.end],
            )
        })
    }

    /// Whether no cell lists a model.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// One cell that lists a model, in a [`ModelIndex`]: the model's state in
/// the cell's list, the cell's condition, and whether the chooser offers
/// the cell work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelSet {
    cell: Box<str>,
    region: Box<str>,
    state: ModelState,
    condition: CellCondition,
    offered: bool,
}

impl ModelSet {
    /// The cell `cell`, in `region`, whose list gives the model `state`,
    /// in `condition`, and not offered.
    pub fn new(
        cell: impl Into<String>,
        region: impl Into<String>,
        state: ModelState,
        condition: CellCondition,
    ) -> Self {
        Self {
            cell: cell.into().into_boxed_str(),
            region: region.into().into_boxed_str(),
            state,
            condition,
            offered: false,
        }
    }

    /// The same, offered or not.
    pub fn with_offered(mut self, offered: bool) -> Self {
        self.offered = offered;
        self
    }

    /// The cell, named within its class.
    #[inline]
    pub fn cell(&self) -> &str {
        &self.cell
    }

    /// The region the cell runs in, from its record.
    #[inline]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// The model's state in the cell's list.
    #[inline]
    pub fn state(&self) -> ModelState {
        self.state
    }

    /// Whether the cell is live, scaled to 0, or missing.
    #[inline]
    pub fn condition(&self) -> CellCondition {
        self.condition
    }

    /// Whether the chooser offers the cell work: its hub follows the cell,
    /// the follow is offered, and the chooser has read it. A cell of
    /// another parent, or in a region this chooser may not route to, is
    /// listed and not offered.
    #[inline]
    pub fn is_offered(&self) -> bool {
        self.offered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(model: &str, routing_key: Option<&str>) -> ModelKey {
        ModelKey {
            model: model.into(),
            routing_key: routing_key.map(Into::into),
        }
    }

    fn set(cell: &str, state: ModelState) -> ModelSet {
        ModelSet::new(cell, "us-east", state, CellCondition::Live)
    }

    /// A model is found by its name and routing key, with its cells by
    /// name; another routing key is another entry, and a model no cell
    /// lists is `None`.
    #[test]
    fn a_model_is_found_by_its_key() {
        let index = ModelIndex::new(
            true,
            [
                (key("qwen", None), set("model-x-02", ModelState::Starting)),
                (key("qwen", None), set("model-x-01", ModelState::Ready)),
                (
                    key("qwen", Some("p1")),
                    set("model-x-03", ModelState::Ready),
                ),
                (key("llama", None), set("model-x-01", ModelState::Drained)),
                (
                    key("qwen", None),
                    set("model-x-01", ModelState::Unavailable),
                ),
            ],
        );
        fn names(sets: &[ModelSet]) -> Vec<&str> {
            sets.iter().map(ModelSet::cell).collect()
        }
        let qwen = index.sets("qwen", None).expect("listed");
        assert_eq!(names(qwen), ["model-x-01", "model-x-02"]);
        assert_eq!(qwen[0].state(), ModelState::Ready, "the first kept");
        assert_eq!(
            names(index.sets("qwen", Some("p1")).expect("listed")),
            ["model-x-03"]
        );
        assert_eq!(index.sets("llama", None).map(<[ModelSet]>::len), Some(1));
        assert!(index.sets("mistral", None).is_none());
        assert!(index.sets("qwen", Some("p2")).is_none());
        assert!(index.is_synced());
        let listed: Vec<(&str, Option<&str>)> =
            index.models().map(|(model, key, _)| (model, key)).collect();
        assert_eq!(
            listed,
            [("llama", None), ("qwen", None), ("qwen", Some("p1"))]
        );
    }

    /// The index of a table that has none lists nothing and is not synced,
    /// so a reader tells it from a synced index that lacks the model.
    #[test]
    fn no_index_is_not_synced() {
        assert!(!ModelIndex::NONE.is_synced());
        assert!(ModelIndex::NONE.is_empty());
        assert!(ModelIndex::NONE.sets("qwen", None).is_none());
        assert_eq!(ModelIndex::default(), ModelIndex::NONE);
        let synced = ModelIndex::new(true, []);
        assert!(synced.is_synced() && synced.sets("qwen", None).is_none());
    }
}
