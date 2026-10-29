use std::{fmt, time::Duration};

use serde::{Deserialize, Serialize};

use crate::DirectoryError;

/// One pool's record in the pool directory, published by the pool's hub
/// and read by every hub that may send to the pool.
///
/// Unknown fields are kept out of the type but not refused, so that a hub
/// of an older build still reads the record of a newer one while builds
/// roll pool by pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolRecord {
    /// The pool's name, a DNS label.
    pub name: String,
    /// The class the pool deploys.
    pub class: String,
    /// The region the pool runs in, a DNS label.
    pub region: String,
    /// The parent pool: the pool of the class that sends to this one whose
    /// hub follows it, and whose choosers alone offer it work. `None` for a
    /// pool of a class that nothing sends to. Optional on the wire, so that
    /// a reader of an earlier build ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home: Option<String>,
    /// The contract hash of the graph the pool serves. A reader follows
    /// only a pool whose hashes equal its own.
    pub contract: u64,
    /// The placement hash of the deployment the pool serves.
    pub placement: u64,
    /// Whether the pool takes new work.
    pub state: PoolState,
    /// The writer's incarnation: a hub that restarts takes a new epoch,
    /// which orders after every `seq` of the ones before it.
    pub epoch: u64,
    /// The writer's sequence number within its epoch, raised on every
    /// write: a republish with a higher `seq` is the heartbeat.
    pub seq: u64,
    /// The pool hub's public listener, `host:port`, that a follower
    /// reaches.
    pub hub: String,
    /// How long a reader keeps the record without seeing its `seq`
    /// change. A duration, never an instant: clocks across regions differ,
    /// so each reader measures the age on its own clock.
    #[serde(rename = "lease_ms", with = "millis", default = "default_lease")]
    pub lease: Duration,
    /// The load of the pool's entry segments, when the hub has one: as it
    /// was when the record was written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load: Option<LoadSummary>,
    /// Whether the pool sends a heartbeat, or was scaled to 0 on purpose
    /// and sends none. `serving` when a record does not say.
    #[serde(default)]
    pub presence: Presence,
    /// The models the pool serves, each with its state. An entry stays
    /// until the pool withdraws, unless it is `drained`: at most
    /// [`PoolRecord::MAX_DRAINED`] of those are kept, the oldest dropped
    /// first. Not checked with the record, so that one malformed model
    /// never costs the pool its record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<ModelEntry>,
    /// Raised on every change of `models`, and never lowered, also across
    /// a restart of the pool's hub, which counts from its epoch in the high
    /// bits: a reader that holds this version holds this list.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub models_version: u64,
    /// The models an operator revoked on this pool: none of them is in
    /// `models`, whatever the pool's instances say, until the operator
    /// lifts the revocation. Kept in the record, so that a hub that
    /// restarts keeps them, and counted in `models_version`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revoked: Vec<gdl_model::ModelKey>,
}

impl PoolRecord {
    /// The lease of a record that does not name one: a hub's default
    /// lease, three of its default heartbeats.
    pub const DEFAULT_LEASE: Duration = gdl_model::HubTiming::DEFAULT_LEASE;

    /// The most `drained` model entries a record keeps.
    pub const MAX_DRAINED: usize = 1_000;

    /// The record's place in its writer's order of writes.
    pub fn version(&self) -> Version {
        Version {
            epoch: self.epoch,
            seq: self.seq,
        }
    }

    /// Whether a reader applies the record's lease: only to a pool that
    /// is `serving`, since a pool scaled to 0 sends no heartbeat, and its
    /// record stays until it serves again or withdraws.
    pub fn leased(&self) -> bool {
        self.presence == Presence::Serving
    }

    /// Whether the record can be published: its pool, parent, region, and
    /// class names have their shapes, and its lease is a whole number of
    /// milliseconds, at least one: the unit it is stored in, so that a
    /// stored record expires when the published one would. Every problem is
    /// reported.
    pub fn check(&self) -> Result<(), DirectoryError> {
        let mut problems = names(&self.class, &self.name);
        if let Some(Err(problem)) = self.home.as_deref().map(gdl_model::check_pool_name) {
            problems.push(problem);
        }
        if let Err(problem) = gdl_model::check_region_name(&self.region) {
            problems.push(problem);
        }
        let whole = self.lease.subsec_nanos().is_multiple_of(1_000_000)
            && u64::try_from(self.lease.as_millis()).is_ok();
        if self.lease.is_zero() || !whole {
            problems.push(format!(
                "pool `{}`: the lease must be a whole number of milliseconds, at least 1, not {:?}",
                self.name, self.lease
            ));
        }
        refuse(problems)
    }
}

/// Whether a pool takes new work, as its hub judges from its quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolState {
    /// Every segment of the class has its quorum of usable instances.
    Ready,
    /// A segment is below its quorum, or every quorum is met but has not
    /// yet held for the readiness hold, and every segment has a usable
    /// instance: chosen only when no ready pool fits.
    Degraded,
    /// A segment has no usable instance: never chosen.
    Unavailable,
    /// The pool is draining: no new work.
    Draining,
}

/// Whether a pool is expected to answer: the distinction between a pool
/// scaled to 0 on purpose and one that is expected but silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    /// The pool's hub sends a heartbeat: a reader that sees none for the
    /// lease holds the pool missing.
    #[default]
    Serving,
    /// The pool was scaled to 0 on purpose, and sends no heartbeat.
    ScaledToZero,
}

/// Whether a pool is expected to answer, as a reader judges it from the
/// pool's record and its follow: what a model gate tells a model whose
/// pools are all silent (503) from one decommissioned (404) by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolCondition {
    /// The pool is `serving`, its record is fresh, and its follow, if the
    /// reader's hub follows it, has not been lost.
    Live,
    /// The pool was scaled to 0 on purpose ([`Presence::ScaledToZero`]).
    ScaledToZero,
    /// The pool is `serving`, but its lease has passed without a new
    /// write, or its follow's link was lost and has not been read again:
    /// expected, but silent.
    Missing,
}

/// One model a pool serves, in its record.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelEntry {
    /// The name a client sends.
    pub model: String,
    /// The serving partition, when the pool has more than one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    /// What the pool can do with the model, with its history.
    pub state: ModelState,
    /// The record's `models_version` at which the entry took its state:
    /// it orders the `drained` entries, oldest first.
    pub since: u64,
}

impl ModelEntry {
    /// The entry's key.
    pub fn key(&self) -> gdl_model::ModelKey {
        gdl_model::ModelKey {
            model: self.model.clone(),
            routing_key: self.routing_key.clone(),
        }
    }
}

/// What a pool can do with one model, as its hub computes it from its
/// entry instances: a state keeps what came before it, so that a model
/// that served once never reads as one that never did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelState {
    /// Registered, and never `ready` in this pool.
    Starting,
    /// A usable entry instance can serve it now.
    Ready,
    /// Was `ready`, and no usable entry instance can serve it now. Never
    /// back to `starting`.
    Unavailable,
    /// Being removed on purpose: no new work.
    Draining,
    /// Removed on purpose.
    Drained,
}

/// The load of a pool's entry segments, as the pool's hub makes it once a
/// summary period from the instances' reports, and sends it on the pool's
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
    /// The pool's summary period, in milliseconds: a reader that has had
    /// no summary for a few periods knows its last one is stale, without
    /// knowing the pool's settings.
    pub period_ms: u64,
}

/// A pool's withdrawal of its record: the last write of a pool that
/// drained, ordered with its publishes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Withdrawal {
    /// The class of the pool.
    pub class: String,
    /// The pool whose record is removed.
    pub pool: String,
    /// The writer's epoch, as in [`PoolRecord::epoch`].
    pub epoch: u64,
    /// The writer's sequence number, as in [`PoolRecord::seq`].
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

    /// Whether the withdrawal names a pool and class of the right shapes.
    pub fn check(&self) -> Result<(), DirectoryError> {
        refuse(names(&self.class, &self.pool))
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

fn names(class: &str, pool: &str) -> Vec<String> {
    [
        gdl_model::check_pool_name(pool),
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

fn is_zero(version: &u64) -> bool {
    *version == 0
}

fn default_lease() -> Duration {
    PoolRecord::DEFAULT_LEASE
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
