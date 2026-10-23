use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::cell::{check_cell_name, check_region_name};
use crate::{CellSettings, DirectoryBackend, Error, Layout};

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
/// home = "global-us-east-1"          # optional: a cell of a class that sends to this one
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    home: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    ready: BTreeMap<String, u32>,
}

impl Fleet {
    /// Parse a fleet file. An unknown key anywhere is refused; what the
    /// values mean is checked against a layout by [`Fleet::check`].
    pub fn from_toml(text: &str) -> Result<Self, Error> {
        toml::from_str(text).map_err(|e| Error::new(e.to_string()))
    }

    /// Check the fleet against the layout of the placement it deploys,
    /// reporting every problem. A cell whose class the placement does not
    /// have is reported once: its quorum, and whether its home is of a
    /// parent class, are not judged against it.
    pub fn check(&self, layout: &Layout) -> Result<(), FleetErrors> {
        let segments = layout.class_segments();
        let mut sends: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for boundary in layout.boundaries() {
            sends
                .entry(boundary.from.as_str())
                .or_default()
                .insert(boundary.into.as_str());
        }
        let mut errors = Vec::new();
        for (class, defaults) in &self.classes {
            let table = || FleetTable::Class(class.clone());
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
            let members = segments.get(cell.class.as_str());
            match members {
                Some(members) => check_ready(table, &cell.class, members, &cell.ready, &mut errors),
                None => errors.push(FleetError::UnknownClass {
                    table: table(),
                    class: cell.class.clone(),
                }),
            }
            let Some(home) = &cell.home else {
                continue;
            };
            // Whether the home is a cell of the fleet needs no class; whether
            // it is a parent needs both classes, and an unknown one is
            // reported as its own cell's problem.
            let Some(parent) = self.cells.get(home) else {
                errors.push(FleetError::UnknownHome {
                    cell: name.clone(),
                    home: home.clone(),
                });
                continue;
            };
            let sends_here = sends
                .get(parent.class.as_str())
                .is_some_and(|into| into.contains(cell.class.as_str()));
            if members.is_some() && segments.contains_key(parent.class.as_str()) && !sends_here {
                errors.push(FleetError::HomeNotParent {
                    cell: name.clone(),
                    home: home.clone(),
                    home_class: parent.class.clone(),
                    class: cell.class.clone(),
                });
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(FleetErrors(errors))
        }
    }

    /// The settings of the cell `name`, with its quorum resolved: for each
    /// segment, the cell's own `ready` if it sets that segment, else its
    /// class's default; a segment with neither is left to
    /// [`CellSettings::quorum`]'s fallback. The directory, seeds, and hub
    /// address are not in the fleet file and take their defaults. Meant for a fleet that
    /// [`Fleet::check`] accepted, whose `ready` keys are all segments of
    /// the cell's class.
    pub fn cell(&self, name: &str) -> Option<CellSettings> {
        let cell = self.cells.get(name)?;
        let mut ready = self
            .classes
            .get(&cell.class)
            .map(|class| class.ready.clone())
            .unwrap_or_default();
        ready.extend(cell.ready.iter().map(|(s, n)| (s.clone(), *n)));
        Some(CellSettings {
            name: name.to_string(),
            class: cell.class.clone(),
            region: cell.region.clone(),
            home: cell.home.clone(),
            directory: DirectoryBackend::default(),
            seeds: Vec::new(),
            ready,
            hub: None,
        })
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
