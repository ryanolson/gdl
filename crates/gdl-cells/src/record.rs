use std::{fmt, time::Duration};

use serde::{Deserialize, Serialize};

use crate::DirectoryError;

/// One cell's record in the cell directory, published by the cell's hub
/// and read by every hub that may send to the cell.
///
/// Unknown fields are kept out of the type but not refused, so that a hub
/// of an older build still reads the record of a newer one while builds
/// roll cell by cell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellRecord {
    /// The cell's name, a DNS label.
    pub name: String,
    /// The class the cell deploys.
    pub class: String,
    /// The region the cell runs in, a DNS label.
    pub region: String,
    /// The parent cell: the cell of the class that sends to this one whose
    /// hub follows it, and whose choosers alone offer it work. `None` for a
    /// cell of a class that nothing sends to. Optional on the wire, so that
    /// a reader of an earlier build ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home: Option<String>,
    /// The contract hash of the graph the cell serves. A reader follows
    /// only a cell whose hashes equal its own.
    pub contract: u64,
    /// The placement hash of the deployment the cell serves.
    pub placement: u64,
    /// Whether the cell takes new work.
    pub state: CellState,
    /// The writer's incarnation: a hub that restarts takes a new epoch,
    /// which orders after every `seq` of the ones before it.
    pub epoch: u64,
    /// The writer's sequence number within its epoch, raised on every
    /// write: a republish with a higher `seq` is the heartbeat.
    pub seq: u64,
    /// The cell hub's public listener, `host:port`, that a follower
    /// reaches.
    pub hub: String,
    /// How long a reader keeps the record without seeing its `seq`
    /// change. A duration, never an instant: clocks across regions differ,
    /// so each reader measures the age on its own clock.
    #[serde(rename = "lease_ms", with = "millis", default = "default_lease")]
    pub lease: Duration,
    /// The load of the cell's entry segments, when the hub has one: as it
    /// was when the record was written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load: Option<LoadSummary>,
}

impl CellRecord {
    /// The lease of a record that does not name one: three heartbeats of
    /// 10 s.
    pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

    /// The record's place in its writer's order of writes.
    pub fn version(&self) -> Version {
        Version {
            epoch: self.epoch,
            seq: self.seq,
        }
    }

    /// Whether the record can be published: its cell, parent, region, and
    /// class names have their shapes, and its lease is a whole number of
    /// milliseconds, at least one: the unit it is stored in, so that a
    /// stored record expires when the published one would. Every problem is
    /// reported.
    pub fn check(&self) -> Result<(), DirectoryError> {
        let mut problems = names(&self.class, &self.name);
        if let Some(Err(problem)) = self.home.as_deref().map(gdl_model::check_cell_name) {
            problems.push(problem);
        }
        if let Err(problem) = gdl_model::check_region_name(&self.region) {
            problems.push(problem);
        }
        let whole = self.lease.subsec_nanos().is_multiple_of(1_000_000)
            && u64::try_from(self.lease.as_millis()).is_ok();
        if self.lease.is_zero() || !whole {
            problems.push(format!(
                "cell `{}`: the lease must be a whole number of milliseconds, at least 1, not {:?}",
                self.name, self.lease
            ));
        }
        refuse(problems)
    }
}

/// Whether a cell takes new work, as its hub judges from its quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CellState {
    /// Every segment of the class has its quorum of usable instances.
    Ready,
    /// A segment is below its quorum, or every quorum is met but has not
    /// yet held for the readiness hold, and every segment has a usable
    /// instance: chosen only when no ready cell fits.
    Degraded,
    /// A segment has no usable instance: never chosen.
    Unavailable,
    /// The cell is draining: no new work.
    Draining,
}

/// The load of a cell's entry segments, as the cell's hub makes it once a
/// summary period from the instances' reports, and sends it on the cell's
/// watch each period, changed or not.
///
/// `admitted`, `limit`, and `refusals` are sums over the reports the hub
/// counts: the latest report of each usable entry instance, from its
/// current registration, and at most three periods old. `instances`
/// counts every usable entry instance, whether it reported or not, so
/// the sums can cover fewer instances than `instances` says. The hub
/// makes no summary while it counts no report.
///
/// Admission counts roots, not engine work, so a policy built on it is
/// an admission heuristic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LoadSummary {
    /// The roots the instances that reported hold admitted.
    pub admitted: u64,
    /// The admission limit, summed over the same instances.
    pub limit: u64,
    /// The usable entry instances, whether or not their reports are
    /// counted.
    pub instances: u32,
    /// The hand-offs the instances that reported declined, each in the
    /// period before its report.
    pub refusals: u64,
    /// The cell's summary period, in milliseconds: a reader that has had
    /// no summary for a few periods knows its last one is stale, without
    /// knowing the cell's settings.
    pub period_ms: u64,
}

/// A cell's withdrawal of its record: the last write of a cell that
/// drained, ordered with its publishes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Withdrawal {
    /// The class of the cell.
    pub class: String,
    /// The cell whose record is removed.
    pub cell: String,
    /// The writer's epoch, as in [`CellRecord::epoch`].
    pub epoch: u64,
    /// The writer's sequence number, as in [`CellRecord::seq`].
    pub seq: u64,
}

impl Withdrawal {
    /// The withdrawal's place in its writer's order of writes.
    pub fn version(&self) -> Version {
        Version {
            epoch: self.epoch,
            seq: self.seq,
        }
    }

    /// Whether the withdrawal names a cell and class of the right shapes.
    pub fn check(&self) -> Result<(), DirectoryError> {
        refuse(names(&self.class, &self.cell))
    }
}

/// A place in one writer's order of writes. A new epoch orders after every
/// `seq` of an older one; within an epoch, the higher `seq` is newer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version {
    /// The writer's incarnation.
    pub epoch: u64,
    /// The write within the epoch.
    pub seq: u64,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(epoch {}, seq {})", self.epoch, self.seq)
    }
}

fn names(class: &str, cell: &str) -> Vec<String> {
    [
        gdl_model::check_cell_name(cell),
        gdl_model::check_class_name(class),
    ]
    .into_iter()
    .filter_map(Result::err)
    .collect()
}

fn refuse(problems: Vec<String>) -> Result<(), DirectoryError> {
    if problems.is_empty() {
        Ok(())
    } else {
        Err(DirectoryError::Malformed(problems.join("; ")))
    }
}

fn default_lease() -> Duration {
    CellRecord::DEFAULT_LEASE
}

/// A lease as whole milliseconds, the unit later backends store.
mod millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(lease: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(u64::try_from(lease.as_millis()).unwrap_or(u64::MAX))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        u64::deserialize(d).map(Duration::from_millis)
    }
}
