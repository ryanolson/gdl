//! What a child cell's hub reports to its parent cell's hub under the
//! `relay` directory backend, and the parent's answer.

use std::time::Duration;

use gdl_cells::{CellRecord, DirectoryError, Version, Withdrawal};
use serde::{Deserialize, Serialize};

/// The report's route, under the extension's, on the parent hub's public
/// listener: `POST` with a [`Report`], answered `200 OK` with a
/// [`ReportReply`] once the parent's directory holds what it carries, or
/// with a [`ReportRefusal`]: `400 Bad Request` for a record or withdrawal
/// that is malformed, `409 Conflict` for one the parent refuses. The
/// parent takes reports whether or not its own cell is healthy, since its
/// health waits for its children.
///
/// No credential crosses cells: like a registration and velo's traffic,
/// the route trusts the private network. The parent refuses what it can
/// check: a record whose parent is another cell, whose contract or
/// placement hash is not its own, or that is not newer than what it holds.
pub const REPORT_ROUTE: &str = "/report";

/// The route of what a parent cell's hub holds for one child, under the
/// extension's, on its public listener: `GET` answers `200 OK` with the
/// [`gdl_cells::CellRecord`] it holds for the cell `cell` of `class`,
/// expired or not, or `404 Not Found` when it holds none. A child's hub
/// that restarts reads its cell's last record here before its first
/// report, so that its models keep their history. Like the report, it
/// trusts the private network.
pub const HELD_ROUTE: &str = "/report/{class}/{cell}";

/// [`HELD_ROUTE`]'s path for the cell `cell` of `class`.
pub fn held_path(class: &str, cell: &str) -> String {
    format!("/report/{class}/{cell}")
}

/// What a child cell's hub reports to its parent's hub: its record, on
/// each change of state and on its heartbeat, or, once its drain has
/// ended, its withdrawal.
// A report is made once a heartbeat and sent at once: the record is not
// boxed, so that the variant is written and matched as the wire reads.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Report {
    /// The cell's record.
    Record(CellRecord),
    /// The cell's withdrawal of its record.
    Withdrawal(Withdrawal),
}

/// The parent's answer to a report it took: when it expects the next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportReply {
    /// The interval, in milliseconds, at which the parent expects the
    /// child's reports: never longer than the record's lease allows, so
    /// that a child at this cadence is not dropped while it reports.
    pub interval_ms: u64,
}

impl ReportReply {
    /// The interval at which the parent expects the child's reports.
    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }
}

/// Why the parent refused a report, as the body of its refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "refused")]
pub enum ReportRefusal {
    /// The record or withdrawal is malformed (`400 Bad Request`).
    Malformed {
        /// What is wrong with it.
        reason: String,
    },
    /// The parent holds a write of the cell at least as new (`409 Conflict`).
    NotNewer {
        /// The parent's write, as `(epoch, seq)`.
        held: (u64, u64),
        /// The report's, as `(epoch, seq)`.
        offered: (u64, u64),
    },
    /// The parent refuses the report for a rule of its own: the record
    /// names another cell as its parent, or its hashes are not the
    /// parent's (`409 Conflict`).
    Refused {
        /// Which rule, and what the report carried.
        reason: String,
    },
}

impl ReportRefusal {
    /// The refusal of a report whose write the parent's directory refused.
    pub fn of(error: DirectoryError) -> Self {
        match error {
            DirectoryError::Malformed(reason) => Self::Malformed { reason },
            DirectoryError::NotNewer { held, offered, .. } => Self::NotNewer {
                held: (held.epoch, held.seq),
                offered: (offered.epoch, offered.seq),
            },
            DirectoryError::Refused(reason) | DirectoryError::Unavailable(reason) => {
                Self::Refused { reason }
            }
        }
    }

    /// The refusal as the child's directory error, for the cell `cell` of
    /// `class` that the report named.
    pub fn into_error(self, class: &str, cell: &str) -> DirectoryError {
        let version = |(epoch, seq)| Version { epoch, seq };
        match self {
            Self::Malformed { reason } => DirectoryError::Malformed(reason),
            Self::NotNewer { held, offered } => DirectoryError::NotNewer {
                class: class.to_owned(),
                cell: cell.to_owned(),
                held: version(held),
                offered: version(offered),
            },
            Self::Refused { reason } => DirectoryError::Refused(reason),
        }
    }
}

impl Report {
    /// The class and name of the cell the report is of.
    pub fn cell(&self) -> (&str, &str) {
        match self {
            Self::Record(record) => (&record.class, &record.name),
            Self::Withdrawal(withdrawal) => (&withdrawal.class, &withdrawal.cell),
        }
    }
}
