//! One of a spec's layouts as the generator lays it out: its placement,
//! where its tables are in the spec, and where its crates go.

use std::path::PathBuf;

use gdl_model::{Placement, PlacementErrors, Placements, Problem};

use crate::{Error, Spec};

/// One layout of a spec.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Laid<'s> {
    /// The layout's name.
    pub(crate) name: &'s str,
    /// Its placement.
    pub(crate) placement: &'s Placement,
    placements: &'s Placements,
}

impl<'s> Laid<'s> {
    /// The path of table `table` of this layout, as a problem names it:
    /// `segment.front`, or `layout.<name>.segment.front` when the spec has
    /// `[layout]` tables.
    pub(crate) fn table(&self, table: &str) -> String {
        if self.placements.tabled() {
            format!("layout.{}.{table}", self.name)
        } else {
            table.to_string()
        }
    }

    /// `message`, a problem of this layout's that is in none of its
    /// tables, naming the layout when the spec has `[layout]` tables.
    pub(crate) fn named(&self, message: String) -> String {
        if self.placements.tabled() {
            format!("[layout.{}]: {message}", self.name)
        } else {
            message
        }
    }

    /// Whether the spec has other layouts than this one. Its crates are then
    /// in a directory of their own and their packages are named for it, so
    /// that one workspace can hold every layout's crates.
    pub(crate) fn among_several(&self) -> bool {
        self.placements.len() > 1
    }

    /// Where this layout's crates go, from the output directory: the
    /// output itself for a spec of one layout.
    pub(crate) fn dir(&self) -> PathBuf {
        if self.among_several() {
            PathBuf::from(self.name)
        } else {
            PathBuf::new()
        }
    }

    /// The package that segment `segment` of this layout of graph `graph`
    /// generates.
    pub(crate) fn package(&self, graph: &str, segment: &str) -> String {
        if self.among_several() {
            crate::emit::package_name(graph, &format!("{}-{segment}", self.name))
        } else {
            crate::emit::package_name(graph, segment)
        }
    }

    /// `errors`, this layout's placement checked against its graph, as
    /// problems in the layout's tables.
    pub(crate) fn problems(&self, errors: &PlacementErrors) -> Vec<Problem> {
        self.placements.problems(self.name, errors)
    }
}

impl Spec {
    /// Each of the spec's layouts, in name order.
    pub(crate) fn layouts(&self) -> impl Iterator<Item = Laid<'_>> {
        self.placements.iter().map(|(name, placement)| Laid {
            name,
            placement,
            placements: &self.placements,
        })
    }

    /// The spec's one layout, for `what`, which takes one: refused, naming
    /// the layouts, when the spec has several, rather than one chosen.
    pub(crate) fn only_layout(&self, what: &str) -> Result<Laid<'_>, Error> {
        let mut layouts = self.layouts();
        match (layouts.next(), layouts.next()) {
            (Some(only), None) => Ok(only),
            _ => {
                let names: Vec<String> =
                    self.placements.names().map(|n| format!("`{n}`")).collect();
                Err(Error::new(format!(
                    "{what} takes a spec of one layout; this one has layouts {}",
                    names.join(", ")
                )))
            }
        }
    }
}
