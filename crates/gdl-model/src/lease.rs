//! Leases kept by a heartbeat: the pool record's in the directory, and an
//! instance's registration with its hub. See docs/src/concepts/hub.md.

use std::time::Duration;

/// Whether `lease` is longer than `heartbeat`, as every lease kept by a
/// heartbeat must be: one no longer lets its readers drop a live holder
/// between two heartbeats. The reason names both values; the caller names
/// the key.
pub fn check_lease(heartbeat: Duration, lease: Duration) -> Result<(), String> {
    if lease <= heartbeat {
        return Err(format!(
            "is {}; it must be longer than the heartbeat, {} ms, or readers let the lease lapse between two heartbeats",
            millis(lease),
            millis(heartbeat)
        ));
    }
    Ok(())
}

/// How long a hub keeps an instance's registration without a heartbeat
/// (`instance_lease_ms`), and how often it drops the registrations whose
/// lease has passed (`prune_ms`). The `gdl-hub` binary reads both from
/// `GDL_HUB__INSTANCE_LEASE_MS` and `GDL_HUB__PRUNE_MS`; a segment reads
/// them in `[hub]`, the same variables, and checks its heartbeat against
/// the lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceLease {
    lease: Duration,
    prune: Duration,
}

impl InstanceLease {
    /// The lease when `instance_lease_ms` is not set: 30 s, six default
    /// instance heartbeats.
    pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

    /// The prune period when `prune_ms` is not set: 10 s.
    pub const DEFAULT_PRUNE: Duration = Duration::from_secs(10);

    /// A lease of `lease` pruned every `prune`. Refused, with the key to
    /// change and why: either one under 1 ms, and a lease not longer than
    /// the prune period, which would drop a registration before a prune
    /// could see it fresh.
    pub fn new(lease: Duration, prune: Duration) -> Result<Self, (&'static str, String)> {
        if lease < Duration::from_millis(1) {
            return Err(("instance_lease_ms", "is 0; it is at least 1".into()));
        }
        if prune < Duration::from_millis(1) {
            return Err(("prune_ms", "is 0; it is at least 1".into()));
        }
        if lease <= prune {
            return Err((
                "instance_lease_ms",
                format!(
                    "is {}; it must be longer than the prune period, {} ms",
                    millis(lease),
                    millis(prune)
                ),
            ));
        }
        Ok(Self { lease, prune })
    }

    /// How long a registration lives without a heartbeat.
    pub fn lease(&self) -> Duration {
        self.lease
    }

    /// How often the hub drops the registrations whose lease has passed.
    pub fn prune(&self) -> Duration {
        self.prune
    }
}

impl Default for InstanceLease {
    fn default() -> Self {
        Self {
            lease: Self::DEFAULT_LEASE,
            prune: Self::DEFAULT_PRUNE,
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
