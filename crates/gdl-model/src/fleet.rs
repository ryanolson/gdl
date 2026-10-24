use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::cell::{check_cell_name, check_region_name};
use crate::{CellSettings, DirectoryBackend, Layout, Problems, check_layout_name, read_toml};

mod errors;

pub use errors::{FleetError, FleetErrors, FleetTable};

/// A deployment's cells: `fleet.toml`. Not part of the placement's
/// identity: cells are runtime topology, like replicas. It says what each
/// class's cells need by default, and which cells there are, with their
/// layout, class, region, home, and quorum overrides; a bundle generator writes
/// each cell's own settings from it ([`Fleet::cell`]).
///
/// ```toml
/// [class.global]
/// ready = { a = 2, b = 2, c = 2 }   # default quorum of a cell of this class, per segment
/// requires = { model_x = 1 }         # the cell's health needs this many ready cells of each class
///
/// [cell.model-x-01-us-east-1]
/// layout = "regional"                # the spec's layout it runs: required when the spec has several
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
    /// The layout the cell runs: required when the spec has more than one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    layout: Option<String>,
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
    /// Whether this cell can be `child`'s parent in their layout, whose
    /// classes send as `sends` says: its class sends to the child's. The
    /// layouts themselves are compared first ([`Fleet::home_problem`]):
    /// cells of different layouts never follow or offer to each other.
    fn sends_to(&self, child: &CellSpec, sends: &Names<'_>) -> bool {
        sends
            .get(self.class.as_str())
            .is_some_and(|into| into.contains(child.class.as_str()))
    }
}

/// The classes of one layout, as a fleet is judged against them.
struct Classes<'a> {
    /// The layout's name, when the spec has several: a problem names it.
    named: Option<&'a str>,
    /// Each class's segments.
    segments: Names<'a>,
    /// The classes each class sends to.
    sends: Names<'a>,
}

impl<'a> Classes<'a> {
    fn of(layout: &'a Layout, named: Option<&'a str>) -> Self {
        let mut sends = Names::new();
        for boundary in layout.boundaries() {
            sends
                .entry(boundary.from.as_str())
                .or_default()
                .insert(boundary.into.as_str());
        }
        Self {
            named,
            segments: layout.class_segments(),
            sends,
        }
    }
}

/// Each layout of the spec by its name, with its classes when its
/// placement fits the graph.
type Laid<'a> = BTreeMap<&'a str, Option<Classes<'a>>>;

impl Fleet {
    /// Parse a fleet file. An unknown key anywhere is refused; what the
    /// values mean is checked against the spec's layouts by
    /// [`Fleet::check`].
    pub fn from_toml(text: &str) -> Result<Self, Problems> {
        read_toml(text)
    }

    /// Check the fleet against the layouts of the spec it deploys, each by
    /// its name with its [`Layout`] when its placement fits the graph,
    /// reporting every problem.
    ///
    /// Each cell runs the layout its `layout` names, which must be one of
    /// the spec's; a cell of a spec of one layout may leave it unsaid. Its
    /// class, its quorum, and its home are judged against that layout: a
    /// home is a cell of the same layout, whose class sends to the cell's
    /// there. A class's defaults apply to its cells in every layout with
    /// the class, and are judged against each. A cell whose layout or class
    /// is unknown is reported once: its quorum, and its home, are not
    /// judged against them. What needs a layout's classes is not judged
    /// for a layout given without its [`Layout`], and without any layouts
    /// (the spec did not read) only what needs none is: the cells' names,
    /// regions, and layout names, that each home is a cell of the fleet in
    /// the layout its cell names, and that no count is 0.
    pub fn check(
        &self,
        layouts: Option<&BTreeMap<String, Option<Layout>>>,
    ) -> Result<(), FleetErrors> {
        let several = layouts.is_some_and(|layouts| layouts.len() > 1);
        let laid: Option<Laid<'_>> = layouts.map(|layouts| {
            layouts
                .iter()
                .map(|(name, layout)| {
                    let named = several.then_some(name.as_str());
                    (
                        name.as_str(),
                        layout.as_ref().map(|l| Classes::of(l, named)),
                    )
                })
                .collect()
        });
        let mut errors = Vec::new();
        for (class, defaults) in &self.classes {
            self.check_class(class, defaults, laid.as_ref(), &mut errors);
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
            let runs = match self.layout_of(name, cell, laid.as_ref()) {
                Ok(runs) => runs,
                Err(error) => {
                    errors.push(error);
                    None
                }
            };
            match runs.and_then(|runs| laid.as_ref()?.get(runs)?.as_ref()) {
                None => zero_ready(table, &cell.ready, &mut errors),
                Some(classes) => match classes.segments.get(cell.class.as_str()) {
                    Some(members) => check_ready(
                        table,
                        &cell.class,
                        members,
                        &cell.ready,
                        classes.named,
                        &mut errors,
                    ),
                    None => errors.push(FleetError::UnknownClass {
                        table: table(),
                        class: cell.class.clone(),
                        layout: classes.named.map(str::to_string),
                    }),
                },
            }
            errors.extend(self.home_problem(name, cell, laid.as_ref()));
        }
        // A class's table is judged in each layout with the class, and
        // what is wrong in all of them alike is one problem.
        let mut unique: Vec<FleetError> = Vec::with_capacity(errors.len());
        for error in errors {
            if !unique.contains(&error) {
                unique.push(error);
            }
        }
        if unique.is_empty() {
            Ok(())
        } else {
            Err(FleetErrors(unique))
        }
    }

    /// What is wrong with the defaults of `class`: judged against each
    /// layout with the class, and unknown when no layout has it.
    fn check_class(
        &self,
        class: &str,
        defaults: &ClassDefaults,
        laid: Option<&Laid<'_>>,
        errors: &mut Vec<FleetError>,
    ) {
        let table = || FleetTable::Class(class.to_string());
        let zero = |errors: &mut Vec<FleetError>| {
            zero_ready(table, &defaults.ready, errors);
            for (required, _) in defaults.requires.iter().filter(|(_, n)| **n == 0) {
                errors.push(FleetError::ZeroRequires {
                    class: class.to_string(),
                    required: required.clone(),
                });
            }
        };
        let Some(laid) = laid else {
            // A count of 0 is wrong whatever the classes are.
            return zero(errors);
        };
        let with: Vec<&Classes<'_>> = laid
            .values()
            .flatten()
            .filter(|classes| classes.segments.contains_key(class))
            .collect();
        if with.is_empty() {
            // A layout whose classes are not known may have it.
            if laid.values().any(Option::is_none) {
                return zero(errors);
            }
            errors.push(match laid.len() > 1 {
                true => FleetError::ClassInNoLayout {
                    class: class.to_string(),
                },
                false => FleetError::UnknownClass {
                    table: table(),
                    class: class.to_string(),
                    layout: None,
                },
            });
            return;
        }
        for classes in with {
            let members = &classes.segments[class];
            check_ready(
                table,
                class,
                members,
                &defaults.ready,
                classes.named,
                errors,
            );
            let into = classes.sends.get(class);
            for (required, &count) in &defaults.requires {
                if !into.is_some_and(|into| into.contains(required.as_str())) {
                    errors.push(FleetError::RequiresNotSent {
                        class: class.to_string(),
                        required: required.clone(),
                        layout: classes.named.map(str::to_string),
                    });
                } else if count == 0 {
                    errors.push(FleetError::ZeroRequires {
                        class: class.to_string(),
                        required: required.clone(),
                    });
                }
            }
        }
    }

    /// The layout `cell` runs: the one its `layout` names, or, when it
    /// names none, the spec's only one. `None` when it cannot be told:
    /// without the spec's layouts, a cell that names none runs whichever
    /// the spec has. A name that is not a layout's is the cell's problem.
    fn layout_of<'a>(
        &'a self,
        name: &str,
        cell: &'a CellSpec,
        laid: Option<&Laid<'a>>,
    ) -> Result<Option<&'a str>, FleetError> {
        if let Some(layout) = &cell.layout
            && let Err(reason) = check_layout_name(layout)
        {
            return Err(FleetError::InvalidLayout {
                cell: name.into(),
                reason,
            });
        }
        let Some(laid) = laid else {
            return Ok(cell.layout.as_deref());
        };
        let names = || laid.keys().map(|name| name.to_string()).collect();
        match &cell.layout {
            Some(layout) if laid.contains_key(layout.as_str()) => Ok(Some(layout)),
            Some(layout) => Err(FleetError::UnknownLayout {
                cell: name.into(),
                layout: layout.clone(),
                layouts: names(),
            }),
            None if laid.len() == 1 => Ok(laid.keys().next().copied()),
            None => Err(FleetError::MissingLayout {
                cell: name.into(),
                layouts: names(),
            }),
        }
    }

    /// What is wrong with `cell`'s home, if anything. Whether it names a
    /// cell of the fleet needs no layout, and whether that cell runs the
    /// same layout needs only the layouts' names; whether a cell needs a
    /// home, and whether it names a parent, need the classes, and a cell of
    /// a layout or a class the spec does not have is reported as that
    /// cell's problem.
    fn home_problem(
        &self,
        name: &str,
        cell: &CellSpec,
        laid: Option<&Laid<'_>>,
    ) -> Option<FleetError> {
        if let Some(home) = &cell.home
            && !self.cells.contains_key(home)
        {
            return Some(FleetError::UnknownHome {
                cell: name.into(),
                home: home.clone(),
            });
        }
        let runs = self.layout_of(name, cell, laid).ok().flatten();
        if let Some(home) = &cell.home {
            let parent = &self.cells[home];
            let parent_runs = self.layout_of(home, parent, laid).ok().flatten();
            if let (Some(runs), Some(parent_runs)) = (runs, parent_runs)
                && runs != parent_runs
            {
                return Some(FleetError::HomeInOtherLayout {
                    cell: name.into(),
                    home: home.clone(),
                    home_layout: parent_runs.to_string(),
                    layout: runs.to_string(),
                });
            }
        }
        let Classes {
            segments, sends, ..
        } = laid?.get(runs?)?.as_ref()?;
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
            layout: cell.layout.clone(),
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
/// `layout` names the class's layout, when the spec has several.
fn check_ready(
    table: impl Fn() -> FleetTable,
    class: &str,
    members: &BTreeSet<&str>,
    ready: &BTreeMap<String, u32>,
    layout: Option<&str>,
    errors: &mut Vec<FleetError>,
) {
    for (segment, &count) in ready {
        if !members.contains(segment.as_str()) {
            errors.push(FleetError::ReadyNotInClass {
                table: table(),
                segment: segment.clone(),
                class: class.to_string(),
                layout: layout.map(str::to_string),
            });
        } else if count == 0 {
            errors.push(FleetError::ZeroReady {
                table: table(),
                segment: segment.clone(),
            });
        }
    }
}
