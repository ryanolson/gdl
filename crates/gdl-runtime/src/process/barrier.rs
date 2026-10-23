//! Whom an instance waits for at its launch: every segment, or, for an
//! instance in a cell, the segments of its own class alone.

use std::collections::BTreeMap;

use gdl::{Error, ErrorKind, Result};
use gdl_model::Layout;

/// The segments an instance of one segment waits for at its launch, at its
/// rendezvous and at its routing barrier. In a cell the barrier stops at
/// the class boundary: the cells of another class come and go on their
/// own, and are found through the hub's relay, not waited for.
pub(super) struct Barrier<'a> {
    layout: &'a Layout,
    /// The instance's class, in a cell; `None` waits for every segment.
    class: Option<&'a str>,
}

impl<'a> Barrier<'a> {
    /// The barrier of an instance of `segment`, in a cell or not.
    pub(super) fn new(layout: &'a Layout, segment: &str, in_cell: bool) -> Self {
        Self {
            layout,
            class: in_cell.then(|| layout.class_of(segment)).flatten(),
        }
    }

    /// Whether the instance waits for the instances of `other`.
    pub(super) fn waits_for(&self, other: &str) -> bool {
        self.class
            .is_none_or(|class| self.layout.class_of(other) == Some(class))
    }

    /// Refuse a count that `table` gives for a segment the instance does
    /// not wait for, which would otherwise be ignored.
    pub(super) fn check(&self, table: &str, counts: &BTreeMap<String, usize>) -> Result<()> {
        match counts.keys().find(|segment| !self.waits_for(segment)) {
            None => Ok(()),
            Some(other) => Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "{table}: `{other}` is in class `{}`, and an instance in a cell waits only for the segments of its own class",
                    self.layout.class_of(other).unwrap_or_default()
                ),
            )),
        }
    }
}
