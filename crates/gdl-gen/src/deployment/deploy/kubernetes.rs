//! The Kubernetes site: a fleet's clusters, reached through `kubectl` and
//! the kube contexts the user already has. See
//! `docs/src/guides/kubernetes.md`.
//!
//! kubectl refuses `--prune` together with server-side apply, so the prune
//! is `deploy`'s own: it deletes the objects labelled with the fleet and
//! the cluster that this deploy did not render, after it drains each pool
//! it removes. A cluster the gdl operator runs holds one `GdlPool` per
//! pool, which this site deletes and the operator stops ([`operated`]).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use gdl_model::Fleet;

use super::Waits;
use super::site::{HubRoutes, Readiness, RenderInput, Running, Scope, Site, Stopping, Which};
use crate::deployment::k8s::{self, label_value};
use crate::{Error, Files};

mod apply;
mod cluster;
mod kube;
mod live;
mod operated;
mod proxy;

use apply::{Applied, allow_restart, apply_fleet};
use cluster::{Clusters, why_not_ready};
use kube::bounded;
pub use kube::{Kube, Kubectl, Reply};
use live::Live;
use operated::Operated;
use proxy::Proxy;

/// The label every object of a fleet carries, with the fleet's name.
const FLEET_LABEL: &str = "gdl.dev/fleet";

/// The label every object of a fleet carries, with the fleet's name for
/// the cluster it is rendered for.
const CLUSTER_LABEL: &str = "gdl.dev/cluster";

/// A fleet's clusters, each a place, reached with one [`Kube`].
pub(super) struct Kubernetes<'k> {
    kube: &'k mut dyn Kube,
    clusters: Clusters,
    waits: Waits,
    /// Whether a change that replaces a pool's hub may be applied where the
    /// gdl operator runs the pools: it sets each `GdlPool`'s
    /// `allowPoolRestart`.
    allow_pool_restart: bool,
    /// What the apply did, which the prune reads.
    applied: Applied,
    /// What [`Site::live`] found in each cluster and is not removed yet.
    found: BTreeMap<String, Found>,
}

/// What [`Site::live`] found in one cluster.
#[derive(Default)]
struct Found {
    /// The objects that no `GdlPool` controls, in the order listed.
    objects: Vec<Live>,
    /// The `GdlPool`s, in the order listed.
    pools: Vec<Operated>,
    /// How many objects were found, by every listing: what `finish` says.
    count: usize,
}

impl<'k> Kubernetes<'k> {
    /// The clusters of `fleet`, reached with `kube`. Refused when the
    /// fleet has no name, which labels its objects.
    pub(super) fn new(
        kube: &'k mut dyn Kube,
        fleet: &Fleet,
        waits: Waits,
        allow_pool_restart: bool,
    ) -> Result<Self, Error> {
        Ok(Self {
            kube,
            clusters: Clusters::of(fleet)?,
            waits,
            allow_pool_restart,
            applied: Applied::default(),
            found: BTreeMap::new(),
        })
    }
}

impl Site for Kubernetes<'_> {
    /// For each cluster: that its context may create Deployments in its
    /// namespace; whether it has the PodMonitor CRD, refusing a fleet that
    /// asks for a PodMonitor where it does not; whether it has the gdl
    /// operator's CRDs, refusing a cluster that asks for the operator
    /// without them; and, where the fleet places a pool with a DGD,
    /// whether it serves dynamo's DGDs.
    fn check(&mut self, fleet: &Fleet) -> Result<(), Error> {
        let kube = &mut *self.kube;
        let budget = self.waits.objects;
        for cluster in &mut self.clusters.0 {
            cluster.check(kube, budget)?;
            cluster.find_monitors(kube, budget)?;
            if fleet.metrics().pod_monitor && !cluster.monitors {
                return Err(Error::new(format!(
                    "cluster `{}`: the fleet asks for a PodMonitor (`[metrics] pod_monitor`), but the cluster has no `{}`: install the Prometheus operator's CRDs there, or leave `pod_monitor` out",
                    cluster.name,
                    kube::PODMONITORS
                )));
            }
            cluster.find_operator(kube, budget)?;
            if cluster.operator && !(cluster.pools && cluster.scalers) {
                return Err(Error::new(format!(
                    "cluster `{}`: the fleet asks the gdl operator to run its pools (`[cluster.{}] operator`), but the cluster lacks `{}` or `{}`: install the operator there (`gdl-operator manifests`), or leave `operator` out",
                    cluster.name,
                    cluster.name,
                    kube::POOLS,
                    kube::SCALERS
                )));
            }
            // Without dynamo's CRD the operator applies nothing of a pool
            // with a DGD, and the pool would stay `Pending` past the health
            // wait.
            let dgds: Vec<&str> = fleet
                .dgds()
                .map(|(pool, _)| pool)
                .filter(|pool| {
                    fleet
                        .cluster_of(pool)
                        .is_some_and(|(name, _)| name == cluster.name)
                })
                .collect();
            if !dgds.is_empty() && !cluster.serves_dgds(kube, budget)? {
                return Err(Error::new(format!(
                    "cluster `{}`: pool `{}` has a DGD, and the cluster does not serve `{}` at `{}`: install dynamo's platform there first (its CRDs and operator), then deploy again",
                    cluster.name,
                    dgds.join("`, `"),
                    kube::DGDS,
                    gdl_k8s::DGD_API_VERSION
                )));
            }
        }
        Ok(())
    }

    /// For each cluster: whether it has the PodMonitor CRD, and the gdl
    /// operator's.
    fn reach(&mut self) -> Result<(), Error> {
        for cluster in &mut self.clusters.0 {
            cluster.find_monitors(self.kube, self.waits.objects)?;
            cluster.find_operator(self.kube, self.waits.objects)?;
        }
        Ok(())
    }

    /// The objects, as [`k8s::k8s_files`] renders them; with
    /// `allowPoolRestart` on each `GdlPool` when the deploy allows it.
    fn render(&self, input: &RenderInput<'_>) -> Result<Files, Error> {
        let mut files = k8s::k8s_files(
            input.spec,
            input.fleet,
            input.dgds,
            input.target,
            None,
            Some(input.digests),
            input.sources,
        )?;
        if self.allow_pool_restart {
            allow_restart(&mut files, &self.clusters)?;
        }
        Ok(files)
    }

    fn apply(
        &mut self,
        fleet: &Fleet,
        files: Files,
        out: Option<&Path>,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        self.applied = apply_fleet(
            self.kube,
            fleet,
            &self.clusters,
            files,
            out,
            &self.waits,
            say,
        )?;
        Ok(())
    }

    /// The fleet's objects that the cluster holds for this cluster, and
    /// its `GdlPool`s: for [`Scope::Prune`], those the apply did not
    /// render, refused when this deploy applied one of them in another
    /// cluster; for [`Scope::Pools`], those of a pool; for [`Scope::Rest`],
    /// every object, and no `GdlPool`.
    fn live(&mut self, place: &str, scope: Scope) -> Result<Running, Error> {
        let cluster = self.clusters.get(place);
        let live = cluster.live(self.kube, self.waits.objects)?;
        if scope == Scope::Rest {
            let found = self.found.entry(place.to_string()).or_default();
            found.count += live.len();
            let driven = live::driven(&live);
            found.objects = live;
            found.pools = Vec::new();
            return Ok(Running {
                notes: Vec::new(),
                driven,
                own: Vec::new(),
            });
        }
        let mut notes: Vec<_> = live::notes(&live).collect();
        let empty = BTreeSet::new();
        let names = self.applied.names.get(place).unwrap_or(&empty);
        let objects: Vec<Live> = match scope {
            Scope::Prune => live
                .into_iter()
                .filter(|object| !names.contains(&(object.kind.clone(), object.name.clone())))
                .collect(),
            _ => live.into_iter().filter(|o| o.pool.is_some()).collect(),
        };
        let pools = operated::pools(self.kube, cluster, self.waits.objects)?;
        notes.extend(pools.iter().map(Operated::note));
        let pools: Vec<Operated> = match scope {
            Scope::Prune => pools
                .into_iter()
                .filter(|pool| !names.contains(&("GdlPool".to_string(), pool.name.clone())))
                .collect(),
            _ => pools,
        };
        let foreign = objects
            .iter()
            .map(|object| {
                (
                    object.kind.as_str(),
                    object.name.as_str(),
                    object.uid.as_str(),
                )
            })
            .chain(
                pools
                    .iter()
                    .map(|pool| ("GdlPool", pool.name.as_str(), pool.uid.as_str())),
            )
            .find(|(_, _, uid)| self.applied.uids.contains(*uid));
        if let Some((kind, name, _)) = foreign {
            return Err(Error::new(format!(
                "cluster `{}`: this deploy applied {kind} `{name}` in another cluster, so it will not prune it: two clusters' contexts reach namespace `{}` of one cluster",
                cluster.name, cluster.namespace
            )));
        }
        let running = Running {
            notes,
            driven: live::driven(&objects),
            own: pools.iter().map(|pool| pool.name.clone()).collect(),
        };
        let found = self.found.entry(place.to_string()).or_default();
        found.count += objects.len() + pools.len();
        found.objects = objects;
        found.pools = pools;
        Ok(running)
    }

    fn hub(&mut self, place: &str) -> Box<dyn HubRoutes + '_> {
        Box::new(Proxy {
            kube: &mut *self.kube,
            at: self.clusters.get(place).at(),
        })
    }

    fn hubless(&mut self, place: &str, pool: &Stopping) -> Result<(), Error> {
        let at = self.clusters.get(place).at();
        proxy::withdrawn_without_hub(self.kube, at, pool, &self.waits)
    }

    /// Delete the objects of `which`, segments' Deployments first and
    /// hubs' next, so that a hub outlives the instances that report to it;
    /// the rest after every hub: a pool's other objects, and the DynamoDB
    /// directory's Local and Job, which the hubs use until they stop.
    fn remove(&mut self, place: &str, which: Which<'_>) -> Result<(), Error> {
        let found = self.found.entry(place.to_string()).or_default();
        let objects: Vec<Live> = match which {
            Which::Rest => std::mem::take(&mut found.objects),
            Which::Pools(pools) => {
                let (gone, kept) =
                    std::mem::take(&mut found.objects)
                        .into_iter()
                        .partition(|object: &Live| {
                            object
                                .pool
                                .as_deref()
                                .is_some_and(|pool| pools.contains(&pool))
                        });
                found.objects = kept;
                gone
            }
        };
        let cluster = self.clusters.get(place);
        let segments: Vec<String> = objects
            .iter()
            .filter(|object| object.is_segment())
            .map(Live::reference)
            .collect();
        let hubs: Vec<String> = objects
            .iter()
            .filter(|object| object.is_hub())
            .map(Live::reference)
            .collect();
        let rest: Vec<String> = objects
            .iter()
            .filter(|object| !object.is_segment() && !object.is_hub())
            .map(Live::reference)
            .collect();
        // Each Deployment's pods may take their grace to stop.
        let grace = objects.iter().filter_map(|o| o.grace).max().unwrap_or(0);
        let seconds = self.waits.objects.as_secs() + grace;
        for batch in [segments, hubs, rest] {
            if batch.is_empty() {
                continue;
            }
            run(
                self.kube,
                cluster.at().delete(&batch, seconds),
                Duration::from_secs(seconds),
                None,
                &format!("deleting from cluster `{}`", cluster.name),
            )?;
        }
        Ok(())
    }

    /// Delete `pools`, `GdlPool`s, with background propagation: each one's
    /// finalizer stops it ([`operated::delete`]).
    fn stop(
        &mut self,
        place: &str,
        pools: &[&str],
        say: &mut dyn FnMut(&str),
    ) -> Result<Vec<String>, Error> {
        let found = self.found.entry(place.to_string()).or_default();
        let batch: Vec<&Operated> = found
            .pools
            .iter()
            .filter(|pool| pools.contains(&pool.name.as_str()))
            .collect();
        let cluster = self.clusters.get(place);
        operated::delete(self.kube, cluster, &batch, &self.waits, say)
    }

    /// Where the operator runs the pool, what its `GdlPool`'s status says
    /// ([`operated::ready`]); elsewhere, whether its hub's health answers
    /// 200, through the API server's service proxy.
    fn ready(&mut self, place: &str, pool: &str, deadline: Instant) -> Result<Readiness, Error> {
        let cluster = self.clusters.get(place);
        if cluster.operator {
            return Ok(match operated::ready(self.kube, cluster, pool, deadline)? {
                Some(why) => Readiness::Waiting(Some(why)),
                None => Readiness::Ready,
            });
        }
        let left = deadline.saturating_duration_since(Instant::now());
        Ok(match self.hub(place).health(pool, left)? {
            true => Readiness::Ready,
            false => Readiness::Waiting(None),
        })
    }

    fn why_not_ready(&mut self, place: &str, pool: &str) -> Result<Vec<String>, Error> {
        let cluster = self.clusters.get(place);
        why_not_ready(self.kube, cluster, pool, self.waits.objects)
    }

    /// For [`Scope::Prune`], say how many objects the prune deleted; for
    /// [`Scope::Rest`], remove the fleet's labels from the namespace, which
    /// stays, with any Secret in it, and say how many objects `down`
    /// deleted.
    fn finish(
        &mut self,
        place: &str,
        scope: Scope,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), Error> {
        let cluster = self.clusters.get(place);
        let count = self.found.get(place).map_or(0, |found| found.count);
        match scope {
            Scope::Prune if count > 0 => say(&format!(
                "prune: deleted {count} object(s) in cluster `{}`",
                cluster.name
            )),
            Scope::Prune | Scope::Pools => {}
            Scope::Rest => {
                run(
                    self.kube,
                    cluster.at().unlabel(&[FLEET_LABEL, CLUSTER_LABEL]),
                    self.waits.objects,
                    None,
                    "removing the fleet's labels from the namespace",
                )?;
                say(&format!(
                    "down: deleted {count} object(s) in cluster `{}`; namespace `{}` stays",
                    cluster.name,
                    cluster.at().namespace
                ));
            }
        }
        Ok(())
    }
}

/// Run `args`, bounded by `budget`, refused with `what` and kubectl's own
/// words unless it succeeds.
fn run(
    kube: &mut dyn Kube,
    args: Vec<String>,
    budget: Duration,
    input: Option<&[u8]>,
    what: &str,
) -> Result<Reply, Error> {
    let args = bounded(args, budget);
    let reply = kube.run(&args, input)?;
    if !reply.success {
        return Err(Error::new(format!(
            "{what}: `kubectl {}` failed: {}",
            args.join(" "),
            reply.stderr.trim()
        )));
    }
    Ok(reply)
}
