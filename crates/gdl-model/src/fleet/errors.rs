use std::fmt;

use crate::{Error, Problem, Problems};

/// Why a fleet file does not fit the placement it deploys.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FleetError {
    /// A class table, or a cell's `class`, names a class the placement
    /// does not have.
    UnknownClass {
        /// The table it is in.
        table: FleetTable,
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
    /// A cell's home names no cell of the fleet.
    UnknownHome {
        /// The cell.
        cell: String,
        /// Its home.
        home: String,
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
        let (table, key, message) = match self {
            Self::UnknownClass { table, class } => {
                // A class table's own name is the class; a cell names it
                // with its `class` key.
                let key = match table {
                    FleetTable::Class(_) => None,
                    FleetTable::Cell(_) => key("class"),
                };
                (
                    Some(table.path()),
                    key,
                    format!("the placement has no class `{class}`"),
                )
            }
            Self::ReadyNotInClass {
                table,
                segment,
                class,
            } => (
                Some(table.path()),
                Some(format!("ready.{segment}")),
                format!("not a segment of class `{class}`"),
            ),
            Self::ZeroReady { table, segment } => (
                Some(table.path()),
                Some(format!("ready.{segment}")),
                "is 0; a quorum is at least 1".into(),
            ),
            Self::RequiresNotSent {
                class: name,
                required,
            } => (
                class(name),
                Some(format!("requires.{required}")),
                format!("not a class that `{name}` sends to"),
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
            Self::UnknownHome { cell: name, home } => (
                cell(name),
                key("home"),
                format!("the fleet has no cell `{home}`"),
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
