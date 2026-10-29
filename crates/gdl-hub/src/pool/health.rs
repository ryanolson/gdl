//! The body of the cell health endpoint.

use std::collections::BTreeMap;

use gdl_cells::CellState;
use serde::{Deserialize, Serialize};

use crate::Quorum;

/// The body of the cell health endpoint: the cell's state, each segment
/// of its class with its usable instances and its quorum, and, for a cell
/// whose class sends to others, each class its settings require cells of
/// (by default, every class it sends to, one cell each) with the count
/// required and the count met, and the classes not met.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellHealth {
    /// The cell's state, from its own quorum: what its record and its
    /// watch carry.
    pub state: CellState,
    /// Each segment of the cell's class, by name.
    pub segments: BTreeMap<String, SegmentHealth>,
    /// Each class the cell's settings require cells of, by name
    /// ([`gdl_model::CellSettings::requirements`]); absent from the body
    /// when there is none, as for a cell of a class that sends to none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requires: BTreeMap<String, RequiredHealth>,
    /// The classes of `requires` with fewer cells met than required, by
    /// name: what the cell lacks to be healthy, besides its own quorum.
    /// Absent from the body when none is. A reader of a build before it
    /// ignores it, and a body without it reads as an empty list, so
    /// [`CellHealth::healthy`] judges by the counts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<String>,
}

impl CellHealth {
    /// The body for `quorum`'s counts in `state`, with none of the
    /// required cells met yet.
    pub(crate) fn of(quorum: &Quorum, state: CellState, requires: &BTreeMap<String, u32>) -> Self {
        // A requirement is at least 1, so each class is missing until met.
        Self {
            state,
            segments: quorum
                .segments()
                .map(|(name, usable, quorum)| (name.to_string(), SegmentHealth { usable, quorum }))
                .collect(),
            requires: requires
                .iter()
                .map(|(class, &required)| (class.clone(), RequiredHealth { required, met: 0 }))
                .collect(),
            missing: requires.keys().cloned().collect(),
        }
    }

    /// `met` ready, followed cells of `class`, if the cell requires cells
    /// of it: whether the body changed. The one place a count met changes,
    /// so that `missing` keeps to the counts.
    pub(crate) fn meet(&mut self, class: &str, met: u32) -> bool {
        let Some(required) = self.requires.get_mut(class) else {
            return false;
        };
        if required.met == met {
            return false;
        }
        required.met = met;
        self.missing = self
            .requires
            .iter()
            .filter(|(_, required)| required.met < required.required)
            .map(|(class, _)| class.clone())
            .collect();
        true
    }

    /// Whether the endpoint answers 200: the cell is `ready`, and each
    /// required class has as many ready, followed cells as it requires.
    pub fn healthy(&self) -> bool {
        // The counts, not `missing`, which a body of an earlier hub lacks.
        self.state == CellState::Ready
            && self
                .requires
                .values()
                .all(|required| required.met >= required.required)
    }
}

/// One class a cell requires cells of, in [`CellHealth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredHealth {
    /// The ready, followed cells of the class the cell needs to be
    /// healthy.
    pub required: u32,
    /// The followed cells of the class whose snapshot is complete and
    /// whose link last said they are `ready`.
    pub met: u32,
}

/// One segment of a cell's class, in [`CellHealth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentHealth {
    /// The instances usable for fresh work.
    pub usable: u32,
    /// The usable instances the cell needs to be ready.
    pub quorum: u32,
}
