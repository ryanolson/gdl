//! What a child pool's hub reports to its parent pool's hub under the
//! `relay` directory backend, and the parent's answer.

use std::time::Duration;

use gdl_pools::{DirectoryError, PoolRecord, Version, Withdrawal};
use serde::{Deserialize, Serialize};

/// The report's route, under the extension's, on the parent hub's public
/// listener: `POST` with a [`Report`], answered `200 OK` with a
/// [`ReportReply`] once the parent's directory holds what it carries, or
/// with a [`ReportRefusal`]: `400 Bad Request` for a record or withdrawal
/// that is malformed, `409 Conflict` for one the parent refuses. The
/// parent takes reports whether or not its own pool is healthy, since its
/// health waits for its children.
///
/// No credential crosses pools: like a registration and velo's traffic,
/// the route trusts the private network. The parent refuses what it can
/// check: a record whose parent is another pool, whose contract or
/// placement hash is not its own, or that is not newer than what it holds.
pub const REPORT_ROUTE: &str = "/report";

/// The query parameter of a report of a record written after reading the
/// pool's last write ([`gdl_pools::PoolDirectory::publish_after`]): its
/// value is [`after_value`]. The parent writes the record only while that
/// is still the child's last write, or it holds none. A parent of an
/// earlier build ignores it, and writes the record as it writes any.
pub const AFTER: &str = "after";

/// [`AFTER`]'s value for a write made after reading the last write at
/// `last`: `<epoch>.<seq>`, or `none` when none was read.
pub fn after_value(last: Option<Version>) -> String {
    match last {
        Some(Version { epoch, seq }) => format!("{epoch}.{seq}"),
        None => "none".to_owned(),
    }
}

/// The last write an [`AFTER`] value names, as [`after_value`] writes it.
///
/// # Errors
///
/// A value of another shape: the report is malformed.
pub fn parse_after(value: &str) -> Result<Option<Version>, DirectoryError> {
    if value == "none" {
        return Ok(None);
    }
    let parsed = value
        .split_once('.')
        .and_then(|(epoch, seq)| Some((epoch.parse().ok()?, seq.parse().ok()?)));
    match parsed {
        Some((epoch, seq)) => Ok(Some(Version { epoch, seq })),
        None => Err(DirectoryError::Malformed(format!(
            "`{AFTER}={value}` names no write: it is `<epoch>.<seq>` or `none`"
        ))),
    }
}

/// The route of what a parent pool's hub holds for one child, under the
/// extension's, on its public listener: `GET` answers `200 OK` with the
/// [`gdl_pools::PoolRecord`] it holds for the pool `pool` of `class`,
/// expired or not, or `404 Not Found` when it holds none. When it holds the
/// tombstone of the pool's withdrawal, the `404` carries the
/// [`gdl_pools::Withdrawal`] as its body, which a child of an earlier build
/// does not read. A child's hub that restarts reads its pool's last write
/// here before its first report, so that its properties keep their history
/// and it numbers past a withdrawn hub. Like the report, it trusts the
/// private network.
pub const HELD_ROUTE: &str = "/report/{class}/{pool}";

/// [`HELD_ROUTE`]'s path for the pool `pool` of `class`.
pub fn held_path(class: &str, pool: &str) -> String {
    format!("/report/{class}/{pool}")
}

/// What a child pool's hub reports to its parent's hub: its record, on
/// each change of state and on its heartbeat, or, once its drain has
/// ended, its withdrawal.
// A report is made once a heartbeat and sent at once: the record is not
// boxed, so that the variant is written and matched as the wire reads.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Report {
    /// The pool's record.
    Record(PoolRecord),
    /// The pool's withdrawal of its record.
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
    /// The parent holds a write of the pool at least as new (`409 Conflict`).
    NotNewer {
        /// The parent's write, as `(epoch, seq)`.
        held: (u64, u64),
        /// The report's, as `(epoch, seq)`.
        offered: (u64, u64),
    },
    /// The parent refuses the report for a rule of its own: the record
    /// names another pool as its parent, or its hashes are not the
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

    /// The refusal as the child's directory error, for the pool `pool` of
    /// `class` that the report named.
    pub fn into_error(self, class: &str, pool: &str) -> DirectoryError {
        let version = |(epoch, seq)| Version { epoch, seq };
        match self {
            Self::Malformed { reason } => DirectoryError::Malformed(reason),
            Self::NotNewer { held, offered } => DirectoryError::NotNewer {
                class: class.to_owned(),
                pool: pool.to_owned(),
                held: version(held),
                offered: version(offered),
            },
            Self::Refused { reason } => DirectoryError::Refused(reason),
        }
    }
}

impl Report {
    /// The class and name of the pool the report is of.
    pub fn pool(&self) -> (&str, &str) {
        match self {
            Self::Record(record) => (&record.class, &record.name),
            Self::Withdrawal(withdrawal) => (&withdrawal.class, &withdrawal.pool),
        }
    }
}
