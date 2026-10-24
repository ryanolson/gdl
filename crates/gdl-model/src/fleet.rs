use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::cell::{check_cell_name, check_region_name};
use crate::{CellSettings, DirectoryBackend, Layout, Problems, read_toml};

mod errors;

pub use errors::{FleetError, FleetErrors, FleetTable};

/// A deployment's cells: `fleet.toml`. Not part of the placement's
/// identity: cells are runtime topology, like replicas. It says what each
/// class's cells need by default, and which cells there are, with their
/// class, region, home, and quorum overrides; a bundle generator writes
/// each cell's own settings from it ([`Fleet::cell`]).
///
/// ```toml
/// [class.global]
/// ready = { a = 2, b = 2, c = 2 }   # default quorum of a cell of this class, per segment
/// requires = { model_x = 1 }         # the cell's health needs this many ready cells of each class
///
/// [cell.model-x-01-us-east-1]
/// class = "model_x"
/// region = "us-east-1"
/// home = "global-us-east-1"          # the parent cell: required when another class sends to this one
/// ready = { e = 8 }                  # optional: overrides the class's default
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fleet {
    #[serde(rename = "class", default, skip_serializing_if = "BTreeMap::is_empty")]
    classes: BTreeMap<String, ClassDefaults>,
    #[serde(rename = "cell", default, skip_serializing_if = "BTreeMap::is_empty")]
    cells: BTreeMap<String, CellSpec>,
}

/// What every cell of a class needs unless it says otherwise.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassDefaults {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    ready: BTreeMap<String, u32>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    requires: BTreeMap<String, u32>,
}

/// One cell, as the fleet file lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CellSpec {
    class: String,
    region: String,
    /// The parent cell: the cell of a class that sends to this one that
    /// follows this cell and offers it work. Required for a cell of a
    /// class that another class sends to, and refused for one of a class
    /// that nothing sends to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    home: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    ready: BTreeMap<String, u32>,
}

impl CellSpec {
    /// Whether this cell can be `child`'s parent: its class sends to the
    /// child's. Every cell of the fleet deploys one layout, so the classes
    /// alone decide.
    fn sends_to(&self, child: &CellSpec, sends: &Names<'_>) -> bool {
        sends
            .get(self.class.as_str())
            .is_some_and(|into| into.contains(child.class.as_str()))
    }
}

impl Fleet {
    /// Parse a fleet file. An unknown key anywhere is refused; what the
    /// values mean is checked against a layout by [`Fleet::check`].
    pub fn from_toml(text: &str) -> Result<Self, Problems> {
        read_toml(text)
    }

    /// Check the fleet against the layout of the placement it deploys,
    /// reporting every problem. A cell whose class the placement does not
    /// have is reported once: its quorum, and its home, are not judged
    /// against it. Without a layout (the spec did not read), only what
    /// needs none is judged: the cells' names and regions, that each home
    /// is a cell of the fleet, and that no count is 0.
    pub fn check(&self, layout: Option<&Layout>) -> Result<(), FleetErrors> {
        let segments = layout.map(Layout::class_segments).unwrap_or_default();
        let mut sends = Names::new();
        for boundary in layout.map(Layout::boundaries).unwrap_or_default() {
            sends
                .entry(boundary.from.as_str())
                .or_default()
                .insert(boundary.into.as_str());
        }
        let mut errors = Vec::new();
        for (class, defaults) in &self.classes {
            let table = || FleetTable::Class(class.clone());
            // A count of 0 is wrong whatever the classes are.
            if layout.is_none() {
                zero_ready(table, &defaults.ready, &mut errors);
                for (required, _) in defaults.requires.iter().filter(|(_, n)| **n == 0) {
                    errors.push(FleetError::ZeroRequires {
                        class: class.clone(),
                        required: required.clone(),
                    });
                }
                continue;
            }
            let Some(members) = segments.get(class.as_str()) else {
                errors.push(FleetError::UnknownClass {
                    table: table(),
                    class: class.clone(),
                });
                continue;
            };
            check_ready(table, class, members, &defaults.ready, &mut errors);
            let into = sends.get(class.as_str());
            for (required, &count) in &defaults.requires {
                if !into.is_some_and(|into| into.contains(required.as_str())) {
                    errors.push(FleetError::RequiresNotSent {
                        class: class.clone(),
                        required: required.clone(),
                    });
                } else if count == 0 {
                    errors.push(FleetError::ZeroRequires {
                        class: class.clone(),
                        required: required.clone(),
                    });
                }
            }
        }
        for (name, cell) in &self.cells {
            let table = || FleetTable::Cell(name.clone());
            if let Err(reason) = check_cell_name(name) {
                errors.push(FleetError::InvalidCellName {
                    cell: name.clone(),
                    reason,
                });
            }
            if let Err(reason) = check_region_name(&cell.region) {
                errors.push(FleetError::InvalidRegion {
                    cell: name.clone(),
                    reason,
                });
            }
            if layout.is_none() {
                zero_ready(table, &cell.ready, &mut errors);
            } else {
                match segments.get(cell.class.as_str()) {
                    Some(members) => {
                        check_ready(table, &cell.class, members, &cell.ready, &mut errors)
                    }
                    None => errors.push(FleetError::UnknownClass {
                        table: table(),
                        class: cell.class.clone(),
                    }),
                }
            }
            let placed = layout.map(|_| (&segments, &sends));
            errors.extend(self.home_problem(name, cell, placed));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(FleetErrors(errors))
        }
    }

    /// What is wrong with `cell`'s home, if anything. Whether it names a
    /// cell of the fleet needs no layout; whether a cell needs one, and
    /// whether it names a parent, need the classes, and a cell of a class
    /// the placement does not have is reported as that cell's problem.
    fn home_problem(
        &self,
        name: &str,
        cell: &CellSpec,
        placed: Option<(&Names<'_>, &Names<'_>)>,
    ) -> Option<FleetError> {
        if let Some(home) = &cell.home
            && !self.cells.contains_key(home)
        {
            return Some(FleetError::UnknownHome {
                cell: name.into(),
                home: home.clone(),
            });
        }
        let (segments, sends) = placed?;
        if !segments.contains_key(cell.class.as_str()) {
            return None;
        }
        let parents: Vec<String> = sends
            .iter()
            .filter(|(_, into)| into.contains(cell.class.as_str()))
            .map(|(from, _)| from.to_string())
            .collect();
        match &cell.home {
            None if parents.is_empty() => None,
            None => Some(FleetError::MissingHome {
                cell: name.into(),
                class: cell.class.clone(),
                parents,
            }),
            Some(_) if parents.is_empty() => Some(FleetError::HomeOfTopClass {
                cell: name.into(),
                class: cell.class.clone(),
            }),
            Some(home) => {
                let parent = &self.cells[home];
                (segments.contains_key(parent.class.as_str()) && !parent.sends_to(cell, sends))
                    .then(|| FleetError::HomeNotParent {
                        cell: name.into(),
                        home: home.clone(),
                        home_class: parent.class.clone(),
                        class: cell.class.clone(),
                    })
            }
        }
    }

    /// The settings of the cell `name`, with its quorum resolved: for each
    /// segment, the cell's own `ready` if it sets that segment, else its
    /// class's default; a segment with neither is left to
    /// [`CellSettings::quorum`]'s fallback. Its `requires` is its class's. The directory, hub
    /// address, and summary period are not in the fleet file and take their defaults. Meant for a fleet that
    /// [`Fleet::check`] accepted, whose `ready` keys are all segments of
    /// the cell's class.
    pub fn cell(&self, name: &str) -> Option<CellSettings> {
        let cell = self.cells.get(name)?;
        let defaults = self.classes.get(&cell.class);
        let mut ready = defaults
            .map(|class| class.ready.clone())
            .unwrap_or_default();
        ready.extend(cell.ready.iter().map(|(s, n)| (s.clone(), *n)));
        Some(CellSettings {
            name: name.to_string(),
            class: cell.class.clone(),
            region: cell.region.clone(),
            home: cell.home.clone(),
            directory: DirectoryBackend::default(),
            ready,
            requires: defaults
                .map(|class| class.requires.clone())
                .unwrap_or_default(),
            hub: None,
            summary: CellSettings::DEFAULT_SUMMARY,
        })
    }
}

/// Names by a class: its segments, or the classes it sends to.
type Names<'a> = BTreeMap<&'a str, BTreeSet<&'a str>>;

/// Every `ready` count is at least 1: what can be judged of `ready`
/// without the classes.
fn zero_ready(
    table: impl Fn() -> FleetTable,
    ready: &BTreeMap<String, u32>,
    errors: &mut Vec<FleetError>,
) {
    for (segment, _) in ready.iter().filter(|(_, count)| **count == 0) {
        errors.push(FleetError::ZeroReady {
            table: table(),
            segment: segment.clone(),
        });
    }
}

/// Every `ready` key is a segment of `class`, with a count of at least 1.
fn check_ready(
    table: impl Fn() -> FleetTable,
    class: &str,
    members: &BTreeSet<&str>,
    ready: &BTreeMap<String, u32>,
    errors: &mut Vec<FleetError>,
) {
    for (segment, &count) in ready {
        if !members.contains(segment.as_str()) {
            errors.push(FleetError::ReadyNotInClass {
                table: table(),
                segment: segment.clone(),
                class: class.to_string(),
            });
        } else if count == 0 {
            errors.push(FleetError::ZeroReady {
                table: table(),
                segment: segment.clone(),
            });
        }
    }
}
