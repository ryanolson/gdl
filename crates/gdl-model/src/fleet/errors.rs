use std::fmt;

use crate::{Error, Problem, Problems};

/// Why a fleet file does not fit the placement it deploys.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FleetError {
    /// A class table, or a cell's `class`, names a class the placement
    /// does not have: the cell's layout's, for a cell of a spec of several
    /// layouts.
    UnknownClass {
        /// The table it is in.
        table: FleetTable,
        /// The unknown name.
        class: String,
        /// The layout without it, when the spec has several.
        layout: Option<String>,
    },
    /// A class table names a class that no layout of the spec has.
    ClassInNoLayout {
        /// The unknown name.
        class: String,
    },
    /// A `ready` key is not a segment of the class.
    ReadyNotInClass {
        /// The table it is in.
        table: FleetTable,
        /// The key.
        segment: String,
        /// The class.
        class: String,
        /// The class's layout, when the spec has several.
        layout: Option<String>,
    },
    /// A `ready` count is 0.
    ZeroReady {
        /// The table it is in.
        table: FleetTable,
        /// The segment.
        segment: String,
    },
    /// A `requires` key is not a class that the class sends to.
    RequiresNotSent {
        /// The class whose table it is in.
        class: String,
        /// The key.
        required: String,
        /// The class's layout, when the spec has several.
        layout: Option<String>,
    },
    /// A `requires` count is 0.
    ZeroRequires {
        /// The class whose table it is in.
        class: String,
        /// The required class.
        required: String,
    },
    /// A cell's name is not a DNS label.
    InvalidCellName {
        /// The cell.
        cell: String,
        /// Why the name is refused.
        reason: String,
    },
    /// A cell's region is not a DNS label.
    InvalidRegion {
        /// The cell.
        cell: String,
        /// Why the region is refused.
        reason: String,
    },
    /// A cell's layout is not a layout's name.
    InvalidLayout {
        /// The cell.
        cell: String,
        /// Why the name is refused.
        reason: String,
    },
    /// A cell names no layout, and the spec has several.
    MissingLayout {
        /// The cell.
        cell: String,
        /// The spec's layouts.
        layouts: Vec<String>,
    },
    /// A cell names a layout the spec does not have.
    UnknownLayout {
        /// The cell.
        cell: String,
        /// The unknown name.
        layout: String,
        /// The spec's layouts.
        layouts: Vec<String>,
    },
    /// A cell's home runs another layout than the cell.
    HomeInOtherLayout {
        /// The cell.
        cell: String,
        /// Its home.
        home: String,
        /// The home's layout.
        home_layout: String,
        /// The cell's layout.
        layout: String,
    },
    /// A cell's home names no cell of the fleet.
    UnknownHome {
        /// The cell.
        cell: String,
        /// Its home.
        home: String,
    },
    /// A cell of a class that another class sends to has no home.
    MissingHome {
        /// The cell.
        cell: String,
        /// Its class.
        class: String,
        /// The classes that send to it.
        parents: Vec<String>,
    },
    /// A cell of a class that nothing sends to has a home.
    HomeOfTopClass {
        /// The cell.
        cell: String,
        /// Its class.
        class: String,
    },
    /// A cell's home is of a class that does not send to the cell's class.
    HomeNotParent {
        /// The cell.
        cell: String,
        /// Its home.
        home: String,
        /// The home's class.
        home_class: String,
        /// The cell's class.
        class: String,
    },
}

/// A table of the fleet file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetTable {
    /// `[class.<name>]`.
    Class(String),
    /// `[cell.<name>]`.
    Cell(String),
}

impl FleetTable {
    /// The table's path, without its brackets.
    fn path(&self) -> String {
        match self {
            Self::Class(name) => format!("class.{name}"),
            Self::Cell(name) => format!("cell.{name}"),
        }
    }
}

impl fmt::Display for FleetTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.path())
    }
}

impl FleetError {
    /// The problem, with the table and the key it is in; its reader adds
    /// the file.
    pub fn problem(&self) -> Problem {
        let class = |name: &str| Some(format!("class.{name}"));
        let cell = |name: &str| Some(format!("cell.{name}"));
        let key = |key: &str| Some(key.to_string());
        let quoted = |names: &[String]| {
            let names: Vec<String> = names.iter().map(|n| format!("`{n}`")).collect();
            names.join(", ")
        };
        let within = |layout: &Option<String>| match layout {
            Some(layout) => format!(" in layout `{layout}`"),
            None => String::new(),
        };
        let (table, key, message) = match self {
            Self::UnknownClass {
                table,
                class,
                layout,
            } => {
                // A class table's own name is the class; a cell names it
                // with its `class` key.
                let key = match table {
                    FleetTable::Class(_) => None,
                    FleetTable::Cell(_) => key("class"),
                };
                let message = match layout {
                    Some(layout) => format!("layout `{layout}` has no class `{class}`"),
                    None => format!("the placement has no class `{class}`"),
                };
                (Some(table.path()), key, message)
            }
            Self::ClassInNoLayout { class: name } => (
                class(name),
                None,
                format!("no layout of the spec has a class `{name}`"),
            ),
            Self::ReadyNotInClass {
                table,
                segment,
                class,
                layout,
            } => (
                Some(table.path()),
                Some(format!("ready.{segment}")),
                format!("not a segment of class `{class}`{}", within(layout)),
            ),
            Self::ZeroReady { table, segment } => (
                Some(table.path()),
                Some(format!("ready.{segment}")),
                "is 0; a quorum is at least 1".into(),
            ),
            Self::RequiresNotSent {
                class: name,
                required,
                layout,
            } => (
                class(name),
                Some(format!("requires.{required}")),
                format!("not a class that `{name}` sends to{}", within(layout)),
            ),
            Self::ZeroRequires {
                class: name,
                required,
            } => (
                class(name),
                Some(format!("requires.{required}")),
                "is 0; a requirement is at least 1".into(),
            ),
            Self::InvalidCellName { cell: name, reason } => (cell(name), None, reason.clone()),
            Self::InvalidRegion { cell: name, reason } => {
                (cell(name), key("region"), reason.clone())
            }
            Self::InvalidLayout { cell: name, reason } => {
                (cell(name), key("layout"), reason.clone())
            }
            Self::MissingLayout {
                cell: name,
                layouts,
            } => (
                cell(name),
                key("layout"),
                format!(
                    "missing; the spec has layouts {}, and a cell names the one it runs",
                    quoted(layouts)
                ),
            ),
            Self::UnknownLayout {
                cell: name,
                layout,
                layouts,
            } => (
                cell(name),
                key("layout"),
                format!(
                    "the spec has no layout `{layout}`; its layouts are {}",
                    quoted(layouts)
                ),
            ),
            Self::HomeInOtherLayout {
                cell: name,
                home,
                home_layout,
                layout,
            } => (
                cell(name),
                key("home"),
                format!(
                    "cell `{home}` runs layout `{home_layout}`, not `{layout}`; a home is a cell of the cell's own layout"
                ),
            ),
            Self::UnknownHome { cell: name, home } => (
                cell(name),
                key("home"),
                format!("the fleet has no cell `{home}`"),
            ),
            Self::MissingHome {
                cell: name,
                class,
                parents,
            } => {
                let parents: Vec<String> = parents.iter().map(|p| format!("`{p}`")).collect();
                (
                    cell(name),
                    key("home"),
                    format!(
                        "missing; a cell of class `{class}` names its parent, a cell of a class that sends to it: {}",
                        parents.join(", ")
                    ),
                )
            }
            Self::HomeOfTopClass { cell: name, class } => (
                cell(name),
                key("home"),
                format!("no class sends to `{class}`, so its cells have no parent"),
            ),
            Self::HomeNotParent {
                cell: name,
                home,
                home_class,
                class,
            } => (
                cell(name),
                key("home"),
                format!(
                    "cell `{home}` is of class `{home_class}`, which does not send to `{class}`; a home is a cell of a class that sends to the cell's"
                ),
            ),
        };
        Problem::at(table, key, message)
    }
}

impl fmt::Display for FleetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.problem().fmt(f)
    }
}

/// Every reason a fleet file was rejected; the check reports all of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetErrors(pub Vec<FleetError>);

impl fmt::Display for FleetErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for FleetErrors {}

impl From<FleetErrors> for Problems {
    fn from(errors: FleetErrors) -> Self {
        Problems(errors.0.iter().map(FleetError::problem).collect())
    }
}

impl From<FleetErrors> for Error {
    fn from(errors: FleetErrors) -> Self {
        Error::new(errors.to_string())
    }
}
