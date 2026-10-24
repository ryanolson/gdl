use std::collections::{BTreeMap, BTreeSet};

/// The names of the `[routing]` entries and `[cells.<class>]` tables that
/// a placement was given with default values only. A default is not
/// recorded, so that a placement that spells one out is the placement, and
/// has the hash, of one that leaves it unsaid; but the name it was given
/// under is still checked against the graph and the classes, as it would
/// be with any other value, or a default for a node or a class that does
/// not exist would pass unseen.
///
/// Not part of the placement's identity: two placements compare equal
/// whatever their spelled names, and the names are never written.
#[derive(Debug, Clone, Default)]
pub(super) struct Spelled {
    /// Nodes given the default router.
    pub(super) routing: BTreeSet<String>,
    /// Classes given a cell policy of defaults.
    pub(super) cells: BTreeSet<String>,
}

impl PartialEq for Spelled {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Spelled {}

impl Spelled {
    /// The names `written`, the table a placement was read from, gives in
    /// `[routing]` and `[cells]`, less those recorded with a value.
    pub(super) fn read(
        written: &toml::Table,
        routing: &BTreeMap<String, super::RequestRouter>,
        cells: &BTreeMap<String, super::CellPolicy>,
    ) -> Self {
        let keys = |table: &str| -> Vec<String> {
            written
                .get(table)
                .and_then(toml::Value::as_table)
                .map(|table| table.keys().cloned().collect())
                .unwrap_or_default()
        };
        Self {
            routing: keys("routing")
                .into_iter()
                .filter(|node| !routing.contains_key(node))
                .collect(),
            cells: keys("cells")
                .into_iter()
                .filter(|class| !cells.contains_key(class))
                .collect(),
        }
    }
}
