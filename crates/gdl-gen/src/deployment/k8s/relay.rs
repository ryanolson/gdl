//! What the relay directory asks of a fleet's objects, and only it: a
//! child's hub reports its record to its parent's hub at the parent's
//! Service, so a child must run in its parent's cluster; and a parent's
//! hub keeps its children's records in a directory that outlives the pod,
//! on a claim of its own. Relay is the one directory a fleet's hubs use,
//! so these are the only answers; a directory that every cluster reads
//! would answer all three here, and need no claim.

use crate::deployment::pools::STATE_DIR;
use gdl_model::{Fleet, HUB_PUBLIC_PORT, Parent, PoolSettings, Problem};

/// One pool, as the relay directory deploys it.
pub(super) struct Relay<'p> {
    pool: &'p PoolSettings,
    parent: bool,
}

impl<'p> Relay<'p> {
    /// `pool`, which is a `parent` when its class sends to another.
    pub(super) fn of(pool: &'p PoolSettings, parent: bool) -> Self {
        Self { pool, parent }
    }

    /// Where the pool's hub reports its record: its parent's hub, at the
    /// Service named for the parent, which resolves in its own cluster.
    pub(super) fn parent_hub(&self) -> Option<String> {
        // A fleet refuses a shared pool under `relay`, so only a home has a
        // parent's hub.
        match &self.pool.parent {
            Some(Parent::Pool(home)) => Some(format!("{home}:{HUB_PUBLIC_PORT}")),
            _ => None,
        }
    }

    /// Where a parent's hub keeps its children's records: its claim's
    /// mount.
    pub(super) fn state_dir(&self) -> Option<&'static str> {
        self.parent.then_some(STATE_DIR)
    }
}

/// Each child in `fleet` whose parent runs in another cluster: its hub
/// would report to a Service name that its cluster does not resolve. A
/// pool without a cluster is refused on its own.
pub(super) fn refusals(fleet: &Fleet) -> Vec<Problem> {
    let mut problems = Vec::new();
    for pool in fleet.pools() {
        let Some(Parent::Pool(home)) = &pool.parent else {
            continue;
        };
        let (Some((ours, _)), Some((theirs, _))) =
            (fleet.cluster_of(&pool.name), fleet.cluster_of(home))
        else {
            continue;
        };
        if ours != theirs {
            problems.push(Problem::at(
                Some(format!("pool.{}", pool.name)),
                None,
                format!(
                    "runs in cluster `{ours}`, and its parent `{home}` in cluster `{theirs}`: under the relay directory a child's hub reports to its parent's hub by the parent's Service name, which does not resolve in another cluster; run the child in `{theirs}`"
                ),
            ));
        }
    }
    problems
}
