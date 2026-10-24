use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde::de::IgnoredAny;

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
    /// The names `text`, a placement file that has been read, gives in
    /// `[routing]` and `[cells]`, less those recorded with a value.
    pub(super) fn read(
        text: &str,
        routing: &BTreeMap<String, super::RequestRouter>,
        cells: &BTreeMap<String, super::CellPolicy>,
    ) -> Self {
        #[derive(Default, Deserialize)]
        struct Written {
            #[serde(default)]
            routing: BTreeMap<String, IgnoredAny>,
            #[serde(default)]
            cells: BTreeMap<String, IgnoredAny>,
        }
        let written: Written = toml::from_str(text).unwrap_or_default();
        Self {
            routing: written
                .routing
                .into_keys()
                .filter(|node| !routing.contains_key(node))
                .collect(),
            cells: written
                .cells
                .into_keys()
                .filter(|class| !cells.contains_key(class))
                .collect(),
        }
    }
}
