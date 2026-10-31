//! `cargo gdl deploy` and `cargo gdl down`: a fleet applied to its
//! clusters, and taken off them, through `kubectl` and the kube contexts
//! the user already has. See `docs/src/guides/kubernetes.md`.
//!
//! `deploy` runs its steps in order and stops at the first failure: check,
//! images (pushed), objects (rendered by the pushed digests), base objects,
//! tables, apply, prune, and the wait for each pool's health. kubectl
//! refuses `--prune` together with server-side apply, so the prune is
//! `deploy`'s own: it deletes the objects labelled with the fleet and the
//! cluster that this deploy did not render, after it drains each pool it
//! removes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use gdl_model::Fleet;
use serde_json::Value;

use super::images::{Digests, Docker, images};
use super::k8s::{self, label_value};
use crate::{Error, Files, Sources, Spec};

mod kube;
mod live;
mod operated;

use kube::{At, CONTROL, GDL, JOBS, MONITORING, PODMONITORS, POOLS, PUBLIC, SCALERS, bounded};
pub use kube::{Kube, Kubectl, Reply};
use live::{Live, Stopping};
use operated::{Operated, Order};

/// The label every object of a fleet carries, with the fleet's name.
const FLEET_LABEL: &str = "gdl.dev/fleet";

/// The label every object of a fleet carries, with the fleet's name for
/// the cluster it is rendered for.
const CLUSTER_LABEL: &str = "gdl.dev/cluster";

/// A pool's hub's routes, as the service proxy reaches them.
const HEALTH: &str = "/v1/ext/gdl-pool/health";
const DRAIN: &str = "/v1/ext/gdl-pool/drain";
const WITHDRAWN: &str = "/v1/ext/gdl-pool/withdrawn";

/// What `deploy` deploys, and where it builds.
#[derive(Debug, Clone)]
pub struct Deploy {
    /// The spec's file.
    pub spec: PathBuf,
    /// The fleet's file.
    pub fleet: PathBuf,
    /// The target directory the images build in.
    pub target: PathBuf,
    /// Where to also write the objects it applies, as `k8s` writes them: a
    /// directory that must not exist.
    pub out: Option<PathBuf>,
    /// How long it waits for each thing it waits on.
    pub waits: Waits,
    /// Where the images take gdl and the crates they build: with
    /// [`Sources::Local`], from the spec's `[gdl] path` and its crates by
    /// path, as they are on this machine.
    pub sources: Sources,
    /// Whether a change that replaces a pool's hub, which drains the pool,
    /// may be applied where the gdl operator runs the pools: it sets each
    /// `GdlPool`'s `allowPoolRestart`. Without it, the operator holds such
    /// a change, and the deploy stops naming the pool and why.
    pub allow_pool_restart: bool,
}

/// How long `deploy` and `down` wait for what they wait on, and how often
/// they look. A wait that runs out fails the command, naming what it
/// waited for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Waits {
    /// Between two reads of something awaited.
    pub interval: Duration,
    /// For every pool's health, after the apply.
    pub health: Duration,
    /// For a pool's drain, beyond the longest stop grace of its pods: the
    /// grace is its drain bound and 30 s.
    pub drain: Duration,
    /// For a namespace to be active, and for each delete.
    pub objects: Duration,
}

impl Default for Waits {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(2),
            health: Duration::from_secs(600),
            drain: Duration::from_secs(60),
            objects: Duration::from_secs(300),
        }
    }
}

/// Deploy the fleet of `how` with `docker` and `kube`, saying each step to
/// `say`. The steps run in order, and the first that fails stops it:
///
/// 1. check the spec and the fleet, as `cargo gdl check --fleet` does, and
///    that each cluster's context may create Deployments in its namespace;
///    ask each cluster whether it has the PodMonitor CRD, and refuse a
///    fleet that asks for a PodMonitor where it does not;
/// 2. build the images and push them ([`images`]);
/// 3. render the objects, each image named by its pushed digest
///    ([`k8s::k8s_files`]); then refuse a pool that the fleet places in
///    another cluster than one that holds any of its Deployments, before
///    anything is applied;
/// 4. apply each cluster's base objects, and wait until its namespace is
///    active. The Namespace is never applied, since an apply would take
///    over the fields its owner set and drop those the render does not
///    list: it is created where none exists, and an existing one only gets
///    the fleet's labels, with `kubectl label`. The DynamoDB directory's cluster goes first,
///    whose base holds the directory's DynamoDB Local, which is waited for
///    until it is ready.
///    Its pod's IP then replaces the placeholder in the other clusters'
///    objects, before their base is applied. The objects, so complete, are
///    written to `out` if given;
/// 5. the tables Job, where the fleet renders one: delete the last run,
///    apply it, and wait until it succeeds; a failure stops the deploy
///    with the Job's log, which names the table;
/// 6. apply each cluster's pools, server-side;
/// 7. prune each cluster: drain each pool that the fleet no longer places
///    there, then delete every object labelled with the fleet and the
///    cluster that this render did not write, and none that it applied in
///    any cluster. A drained pool is deleted once it has withdrawn its
///    record: under `relay` its parent's hub answers 404 for it, and under
///    the DynamoDB directory its own hub says so;
/// 8. wait until each pool's health answers 200, through the API server's
///    service proxy; on a timeout, the error names each pod of an
///    unhealthy pool that is not ready, why it waits, and its events.
pub fn deploy(
    how: &Deploy,
    docker: &mut dyn Docker,
    kube: &mut dyn Kube,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let problems = crate::check_files(&how.spec, Some(&how.fleet), &how.target);
    if !problems.is_empty() {
        return Err(problems.into());
    }
    let fleet = read_fleet(&how.fleet)?;
    let spec = Spec::load(&how.spec).map_err(|e| Error::from(e.in_file(how.spec.display())))?;
    let mut clusters = Clusters::of(&fleet)?;
    for cluster in &mut clusters.0 {
        cluster.check(kube, how.waits.objects)?;
        cluster.find_monitors(kube, how.waits.objects)?;
        if fleet.metrics().pod_monitor && !cluster.monitors {
            return Err(Error::new(format!(
                "cluster `{}`: the fleet asks for a PodMonitor (`[metrics] pod_monitor`), but the cluster has no `{PODMONITORS}`: install the Prometheus operator's CRDs there, or leave `pod_monitor` out",
                cluster.name
            )));
        }
        cluster.find_operator(kube, how.waits.objects)?;
        if cluster.operator && !(cluster.pools && cluster.scalers) {
            return Err(Error::new(format!(
                "cluster `{}`: the fleet asks the gdl operator to run its pools (`[cluster.{}] operator`), but the cluster lacks `{POOLS}` or `{SCALERS}`: install the operator there (`gdl-operator manifests`), or leave `operator` out",
                cluster.name, cluster.name
            )));
        }
    }
    say("check: the spec, the fleet, and each cluster's context");

    let built = images(&spec, &fleet, &how.target, true, how.sources, docker)?;
    say(&format!("images: {} in their registries", built.len()));

    let mut files = k8s::k8s_files(
        &spec,
        &fleet,
        &how.target,
        None,
        Some(&Digests::of(&built)),
        how.sources,
    )?;
    if how.allow_pool_restart {
        allow_restart(&mut files, &clusters)?;
    }
    let rendered = Rendered::of(&files, &clusters)?;
    unmoved(kube, &fleet, &clusters, &how.waits)?;
    // A cluster that changes its kind is refused before anything is
    // applied, in any cluster.
    for cluster in &clusters.0 {
        match cluster.operator {
            true => operated::refuse_opted_in(cluster, &cluster.live(kube, how.waits.objects)?)?,
            false => {
                let pools = documents(&rendered.0[&cluster.name].pools)?;
                operated::refuse_opted_out(kube, cluster, &pools, &how.waits)?;
            }
        }
    }
    // Every object this run applied, in any cluster, by its uid: none is
    // pruned, even where two clusters' contexts reach one namespace.
    let mut applied = BTreeSet::new();

    // The directory's cluster first: the other clusters' hubs name its
    // Local's pod IP, which exists only once the Local runs.
    let home = fleet
        .directory()
        .filter(|directory| directory.local)
        .and_then(|directory| directory.cluster.clone());
    let mut order: Vec<&Cluster> = clusters.0.iter().collect();
    order.sort_by_key(|cluster| Some(&cluster.name) != home.as_ref());
    let mut local = None;
    for cluster in order {
        let objects = &rendered.0[&cluster.name];
        cluster.prepare_namespace(kube, objects, how.waits.objects)?;
        applied.extend(apply(kube, cluster, &objects.base, how.waits.objects)?);
        run(
            kube,
            cluster.at().namespace_active(how.waits.objects.as_secs()),
            how.waits.objects,
            None,
            "waiting for the namespace",
        )?;
        if Some(&cluster.name) == home.as_ref() {
            local = Some(local_ip(kube, cluster, &how.waits)?);
        }
    }
    say("base: each cluster's namespace and service account");
    let mut rendered = rendered;
    if let Some(ip) = &local {
        rendered.fill(ip);
        for text in files.values_mut() {
            *text = text.replace(k8s::PLACEHOLDER, ip);
        }
        say(&format!("base: the DynamoDB Local runs at {ip}"));
    }
    if let Some(out) = &how.out {
        k8s::write_new(out, &files)?;
        say(&format!("k8s: the objects are in {}", out.display()));
    }

    for cluster in &clusters.0 {
        let tables = &rendered.0[&cluster.name].tables;
        if tables.trim().is_empty() {
            continue;
        }
        applied.extend(run_tables(kube, cluster, tables, &how.waits)?);
        say(&format!(
            "tables: the DynamoDB directory's tables are ready, by Job `{}` in cluster `{}`",
            k8s::TABLES,
            cluster.name
        ));
    }

    for cluster in &clusters.0 {
        let pools = &rendered.0[&cluster.name].pools;
        // A cluster the fleet now places no pool in has nothing to apply,
        // and kubectl refuses an apply of nothing; its prune still runs.
        if pools.trim().is_empty() {
            continue;
        }
        applied.extend(apply(kube, cluster, pools, how.waits.objects)?);
    }
    say("apply: each cluster's pools");

    // Every pool this run renders, in any cluster.
    let retained: BTreeSet<String> = fleet.pools().map(|pool| pool.name).collect();
    let mut order = Order::default();
    order.fleet(&fleet);
    let mut plain = Vec::new();
    let mut operated = Vec::new();
    for cluster in &clusters.0 {
        let names = &rendered.0[&cluster.name].names;
        let live = cluster.live(kube, how.waits.objects)?;
        order.live(&live);
        let pruned: Vec<Live> = live
            .into_iter()
            .filter(|object| !names.contains(&(object.kind.clone(), object.name.clone())))
            .collect();
        let pools = operated::pools(kube, cluster, how.waits.objects)?;
        order.operated(&pools);
        let removed: Vec<Operated> = pools
            .into_iter()
            .filter(|pool| !names.contains(&("GdlPool".to_string(), pool.name.clone())))
            .collect();
        let foreign = pruned
            .iter()
            .map(|object| {
                (
                    object.kind.as_str(),
                    object.name.as_str(),
                    object.uid.as_str(),
                )
            })
            .chain(
                removed
                    .iter()
                    .map(|pool| ("GdlPool", pool.name.as_str(), pool.uid.as_str())),
            )
            .find(|(_, _, uid)| applied.contains(*uid));
        if let Some((kind, name, _)) = foreign {
            return Err(Error::new(format!(
                "cluster `{}`: this deploy applied {kind} `{name}` in another cluster, so it will not prune it: two clusters' contexts reach namespace `{}` of one cluster",
                cluster.name, cluster.namespace
            )));
        }
        plain.push((cluster, pruned));
        operated.push((cluster, removed));
    }
    stop_in_order(kube, &order, &plain, &operated, &retained, &how.waits, say)?;
    for ((cluster, pruned), (_, removed)) in plain.iter().zip(&operated) {
        if pruned.len() + removed.len() > 0 {
            say(&format!(
                "prune: deleted {} object(s) in cluster `{}`",
                pruned.len() + removed.len(),
                cluster.name
            ));
        }
    }

    let pools: Vec<(&Cluster, String)> = fleet
        .pools()
        .map(|pool| {
            let (name, _) = fleet
                .cluster_of(&pool.name)
                .expect("k8s_files requires every pool's cluster");
            (clusters.get(name), pool.name)
        })
        .collect();
    healthy(kube, &pools, &how.waits)?;
    say("wait: every pool's health answers 200");
    Ok(())
}

/// Take the fleet in the file `fleet` off its clusters with `kube`, saying
/// each step to `say`: in every cluster, drain each pool that has a parent
/// and delete the pools' objects; then, in every cluster, delete the rest
/// of the objects that carry the fleet's and the cluster's labels, and
/// remove both labels from the namespace, which stays, with any Secret in
/// it. The rest holds the DynamoDB directory's Local and tables Job, which
/// the hubs of every cluster use: so it goes only once no hub runs in any.
pub fn down(
    fleet: &Path,
    waits: &Waits,
    kube: &mut dyn Kube,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let fleet = read_fleet(fleet)?;
    fleet
        .check(None)
        .map_err(|errors| Error::from(gdl_model::Problems::from(errors)))?;
    let mut clusters = Clusters::of(&fleet)?;
    for cluster in &mut clusters.0 {
        cluster.find_monitors(kube, waits.objects)?;
        cluster.find_operator(kube, waits.objects)?;
    }
    // Every pool of the fleet stops, children first across the fleet,
    // whatever kind of cluster each runs in.
    let mut order = Order::default();
    order.fleet(&fleet);
    let mut plain = Vec::new();
    let mut operated = Vec::new();
    let mut deleted = BTreeMap::new();
    for cluster in &clusters.0 {
        let live = cluster.live(kube, waits.objects)?;
        order.live(&live);
        let pools: Vec<Live> = live.into_iter().filter(|o| o.pool.is_some()).collect();
        let ours = operated::pools(kube, cluster, waits.objects)?;
        order.operated(&ours);
        deleted.insert(cluster.name.clone(), pools.len() + ours.len());
        plain.push((cluster, pools));
        operated.push((cluster, ours));
    }
    stop_in_order(
        kube,
        &order,
        &plain,
        &operated,
        &BTreeSet::new(),
        waits,
        say,
    )?;
    for cluster in &clusters.0 {
        let live = cluster.live(kube, waits.objects)?;
        let rest: Vec<&Live> = live.iter().collect();
        stop(kube, cluster, &rest, &BTreeSet::new(), waits, say)?;
        run(
            kube,
            cluster.at().unlabel(&[FLEET_LABEL, CLUSTER_LABEL]),
            waits.objects,
            None,
            "removing the fleet's labels from the namespace",
        )?;
        say(&format!(
            "down: deleted {} object(s) in cluster `{}`; namespace `{}` stays",
            deleted[&cluster.name] + rest.len(),
            cluster.name,
            cluster.at().namespace
        ));
    }
    Ok(())
}

/// Stop the pools of `plain`, each cluster's objects that no `GdlPool`
/// controls, and delete `operated`, each cluster's `GdlPool`s, children
/// first across the fleet, by `order`: every pool at one depth before any
/// at the depth above, whatever kind of cluster each runs in. A plain
/// pool is stopped from here ([`stop`]); a `GdlPool` is deleted, with
/// background propagation, and its finalizer stops it. The objects of
/// `plain` of no pool, or of a pool `retained`, go last, undrained.
fn stop_in_order(
    kube: &mut dyn Kube,
    order: &Order,
    plain: &[(&Cluster, Vec<Live>)],
    operated: &[(&Cluster, Vec<Operated>)],
    retained: &BTreeSet<String>,
    waits: &Waits,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let stopped = |object: &Live| {
        object
            .pool
            .as_ref()
            .filter(|pool| !retained.contains(*pool))
            .map(|pool| order.depth(pool))
    };
    let deepest = plain
        .iter()
        .flat_map(|(_, objects)| objects.iter().filter_map(stopped))
        .chain(
            operated
                .iter()
                .flat_map(|(_, pools)| pools.iter().map(|pool| order.depth(&pool.name))),
        )
        .max();
    if let Some(deepest) = deepest {
        for depth in (0..=deepest).rev() {
            for (cluster, objects) in plain {
                let batch: Vec<&Live> = objects
                    .iter()
                    .filter(|object| stopped(object) == Some(depth))
                    .collect();
                if !batch.is_empty() {
                    stop(kube, cluster, &batch, retained, waits, say)?;
                }
            }
            for (cluster, pools) in operated {
                let batch: Vec<&Operated> = pools
                    .iter()
                    .filter(|pool| order.depth(&pool.name) == depth)
                    .collect();
                operated::delete(kube, cluster, &batch, waits, say)?;
            }
        }
    }
    for (cluster, objects) in plain {
        let rest: Vec<&Live> = objects.iter().filter(|o| stopped(o).is_none()).collect();
        if !rest.is_empty() {
            stop(kube, cluster, &rest, retained, waits, say)?;
        }
    }
    Ok(())
}

/// Set `allowPoolRestart` on each `GdlPool` of `files`, the objects of a
/// render: only a cluster the gdl operator runs has them.
fn allow_restart(files: &mut Files, clusters: &Clusters) -> Result<(), Error> {
    for (path, text) in files.iter_mut() {
        let Some(cluster) = path.iter().next().and_then(|c| c.to_str()) else {
            continue;
        };
        if !clusters.0.iter().any(|c| c.name == cluster && c.operator) {
            continue;
        }
        if path.extension().is_none_or(|e| e != "yaml") {
            continue;
        }
        let mut objects = documents(text)?;
        if !objects.iter().any(|o| o["kind"] == "GdlPool") {
            continue;
        }
        for object in objects.iter_mut().filter(|o| o["kind"] == "GdlPool") {
            object["spec"]["allowPoolRestart"] = true.into();
        }
        *text = k8s::stream(&objects).map_err(super::io)?;
    }
    Ok(())
}

/// The objects of `text`, a YAML stream of JSON documents.
fn documents(text: &str) -> Result<Vec<Value>, Error> {
    text.split("---\n")
        .filter(|document| !document.trim().is_empty())
        .map(|document| {
            serde_json::from_str(document)
                .map_err(|e| Error::new(format!("a rendered object is not JSON: {e}")))
        })
        .collect()
}

fn read_fleet(path: &Path) -> Result<Fleet, Error> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| Error::new(format!("{}: {e}", path.display())))?;
    Fleet::from_toml(&text).map_err(|problems| problems.in_file(path.display()).into())
}

/// Each cluster of a fleet, in name order, and the fleet's label there.
struct Clusters(Vec<Cluster>);

impl Clusters {
    fn of(fleet: &Fleet) -> Result<Self, Error> {
        let fleet_name = fleet.name().ok_or_else(|| {
            Error::new("the fleet has no `[fleet] name`, which labels its objects")
        })?;
        Ok(Self(
            fleet
                .clusters()
                .map(|(cluster, settings)| Cluster {
                    name: cluster.to_string(),
                    context: settings.context.clone(),
                    namespace: settings.namespace.clone(),
                    selector: format!(
                        "{FLEET_LABEL}={},{CLUSTER_LABEL}={}",
                        label_value(fleet_name),
                        label_value(cluster)
                    ),
                    unowned: format!("{FLEET_LABEL}={},!{CLUSTER_LABEL}", label_value(fleet_name)),
                    monitors: false,
                    operator: settings.operator,
                    scalers: false,
                    pools: false,
                })
                .collect(),
        ))
    }

    fn get(&self, name: &str) -> &Cluster {
        self.0
            .iter()
            .find(|cluster| cluster.name == name)
            .expect("a cluster of the fleet")
    }
}

/// One cluster of a fleet.
struct Cluster {
    name: String,
    context: String,
    namespace: String,
    /// The selector of the fleet's objects that were rendered for this
    /// cluster: another cluster's, in a namespace that two contexts reach,
    /// are not this cluster's to prune.
    selector: String,
    /// The selector of the fleet's objects that name no cluster.
    unowned: String,
    /// Whether the cluster has the Prometheus operator's PodMonitor CRD:
    /// only then are the fleet's PodMonitors listed, pruned, and deleted,
    /// whatever the fleet asks now, so that one it no longer asks for goes.
    monitors: bool,
    /// Whether the fleet asks the gdl operator to scale the cluster's
    /// segments (`[cluster.<name>] operator`).
    operator: bool,
    /// Whether the cluster has the gdl operator's `GdlScaler` CRD: only
    /// then are the fleet's scalers listed, pruned, and deleted, so that a
    /// cluster that no longer asks for them loses them.
    scalers: bool,
    /// Whether the cluster has the gdl operator's `GdlPool` CRD: only then
    /// are the fleet's `GdlPool`s listed there.
    pools: bool,
}

impl Cluster {
    fn at(&self) -> At<'_> {
        At {
            context: &self.context,
            namespace: &self.namespace,
        }
    }

    /// Refuse a cluster whose context cannot be reached, or may not create
    /// Deployments in its namespace.
    fn check(&self, kube: &mut dyn Kube, budget: Duration) -> Result<(), Error> {
        let reply = kube.run(&bounded(self.at().can_i(), budget), None)?;
        match reply.stdout.trim() {
            "yes" => Ok(()),
            "no" => Err(Error::new(format!(
                "cluster `{}`: context `{}` may not create Deployments in namespace `{}`",
                self.name, self.context, self.namespace
            ))),
            _ => Err(Error::new(format!(
                "cluster `{}`: context `{}` does not answer: {}",
                self.name,
                self.context,
                reply.stderr.trim()
            ))),
        }
    }

    /// Ask the cluster, once, whether it has the PodMonitor CRD.
    fn find_monitors(&mut self, kube: &mut dyn Kube, budget: Duration) -> Result<(), Error> {
        self.monitors = self.discover(kube, MONITORING, PODMONITORS, budget)?;
        Ok(())
    }

    /// Ask the cluster, once, whether it has the gdl operator's
    /// `GdlScaler` and `GdlPool` CRDs.
    fn find_operator(&mut self, kube: &mut dyn Kube, budget: Duration) -> Result<(), Error> {
        self.scalers = self.discover(kube, GDL, SCALERS, budget)?;
        self.pools = self.discover(kube, GDL, POOLS, budget)?;
        Ok(())
    }

    /// Whether the cluster serves `resource` of API group `group`.
    /// `kubectl api-resources` discovers every API group before it filters
    /// by one, and exits 1 when another group's discovery fails, as a
    /// stale metrics server's does, after printing what it found: what it
    /// printed answers unless the failed groups include `group`. When they
    /// do, nothing it printed answers, a resource listed from another of
    /// the group's versions included: the run stops.
    fn discover(
        &self,
        kube: &mut dyn Kube,
        group: &str,
        resource: &str,
        budget: Duration,
    ) -> Result<bool, Error> {
        let args = bounded(self.at().resources(group), budget);
        let reply = kube.run(&args, None)?;
        let found = reply.stdout.lines().any(|line| line.trim() == resource);
        let others_only = reply
            .stderr
            .contains("unable to retrieve the complete list of server APIs")
            && !reply.stderr.contains(group);
        if !reply.success && !others_only {
            return Err(Error::new(format!(
                "cluster `{}`: asking whether it has `{resource}`: `kubectl {}` failed: {}",
                self.name,
                args.join(" "),
                reply.stderr.trim()
            )));
        }
        Ok(found)
    }

    /// Create the cluster's namespace where none exists, or add the
    /// fleet's labels to the one that does. The Namespace is never
    /// applied: a server-side apply of a namespace that its owner made
    /// with a client-side apply takes over the fields that apply set, and
    /// drops those the render does not list. A create fails on a
    /// namespace that another made after the question, which is then
    /// labelled as one that existed. Any other failure stops the deploy.
    fn prepare_namespace(
        &self,
        kube: &mut dyn Kube,
        objects: &Objects,
        budget: Duration,
    ) -> Result<(), Error> {
        if !self.namespace_exists(kube, budget)? {
            let args = bounded(self.at().create(), budget);
            let reply = kube.run(&args, Some(objects.namespace.as_bytes()))?;
            if reply.success {
                return Ok(());
            }
            if !reply.stderr.contains("(AlreadyExists)") {
                return Err(Error::new(format!(
                    "cluster `{}`: creating the namespace: `kubectl {}` failed: {}",
                    self.name,
                    args.join(" "),
                    reply.stderr.trim()
                )));
            }
        }
        run(
            kube,
            self.at().label(&objects.labels),
            budget,
            None,
            "adding the fleet's labels to the namespace",
        )?;
        Ok(())
    }

    /// Whether the cluster's namespace exists. A question that fails
    /// stops the deploy.
    fn namespace_exists(&self, kube: &mut dyn Kube, budget: Duration) -> Result<bool, Error> {
        let reply = run(
            kube,
            self.at().namespace(),
            budget,
            None,
            &format!(
                "cluster `{}`: asking whether the namespace exists",
                self.name
            ),
        )?;
        Ok(!reply.stdout.trim().is_empty())
    }

    /// The fleet's objects in the cluster's namespace that were rendered
    /// for this cluster. Refused when the namespace holds an object of the
    /// fleet that names no cluster, as a render before `gdl.dev/cluster`
    /// wrote them: it may be another cluster's, where two contexts reach
    /// one namespace, so it is neither this cluster's to delete nor one to
    /// leave running unseen.
    fn live(&self, kube: &mut dyn Kube, budget: Duration) -> Result<Vec<Live>, Error> {
        let reply = run(
            kube,
            self.at().list(&self.unowned, self.monitors, self.scalers),
            budget,
            None,
            "listing the fleet's objects that name no cluster",
        )?;
        let unowned = Live::list(&reply.stdout)
            .map_err(|e| Error::new(format!("cluster `{}`: {e}", self.name)))?;
        if !unowned.is_empty() {
            let objects: Vec<String> = unowned.iter().map(Live::reference).collect();
            return Err(Error::new(format!(
                "cluster `{}`: namespace `{}` holds objects of the fleet that carry no `{CLUSTER_LABEL}` label, so it is not known which cluster's they are: {}. Label the ones that are this cluster's with `kubectl --context {} --namespace {} label {} {CLUSTER_LABEL}={}`, or delete them, then run again",
                self.name,
                self.namespace,
                objects.join(", "),
                self.context,
                self.namespace,
                objects.join(" "),
                label_value(&self.name)
            )));
        }
        let reply = run(
            kube,
            self.at().list(&self.selector, self.monitors, self.scalers),
            budget,
            None,
            "listing the fleet's objects",
        )?;
        // What the gdl operator made is its own: only it changes or
        // deletes it.
        let live = Live::list(&reply.stdout)
            .map_err(|e| Error::new(format!("cluster `{}`: {e}", self.name)))?;
        Ok(live.into_iter().filter(|object| !object.operated).collect())
    }
}

/// The objects of one render, by cluster: the Namespace, the rest of the
/// base, and the pools', each as one YAML stream, and the kind and name of
/// each namespaced object.
struct Rendered(BTreeMap<String, Objects>);

#[derive(Default)]
struct Objects {
    /// The Namespace, created only where none exists.
    namespace: String,
    /// The Namespace's labels, which an existing namespace gets instead.
    labels: Vec<(String, String)>,
    base: String,
    /// The tables Job, in the DynamoDB directory's cluster alone.
    tables: String,
    pools: String,
    names: BTreeSet<(String, String)>,
}

impl Rendered {
    fn of(files: &Files, clusters: &Clusters) -> Result<Self, Error> {
        let mut rendered: BTreeMap<String, Objects> = clusters
            .0
            .iter()
            .map(|cluster| (cluster.name.clone(), Objects::default()))
            .collect();
        for (path, text) in files {
            let Some(cluster) = path.iter().next().and_then(|c| c.to_str()) else {
                continue;
            };
            let Some(objects) = rendered.get_mut(cluster) else {
                continue;
            };
            if path.extension().is_none_or(|e| e != "yaml") {
                continue;
            }
            for document in text.split("---\n").filter(|doc| !doc.trim().is_empty()) {
                let object: Value = serde_json::from_str(document)
                    .map_err(|e| Error::new(format!("{}: {e}", path.display())))?;
                let kind = object["kind"].as_str().unwrap_or_default();
                let stream = if kind == "Namespace" {
                    objects.labels = object["metadata"]["labels"]
                        .as_object()
                        .into_iter()
                        .flatten()
                        .map(|(key, value)| {
                            (key.clone(), value.as_str().unwrap_or_default().to_string())
                        })
                        .collect();
                    &mut objects.namespace
                } else if path.ends_with("base.yaml") {
                    &mut objects.base
                } else if path.ends_with("tables.yaml") {
                    &mut objects.tables
                } else {
                    &mut objects.pools
                };
                stream.push_str("---\n");
                stream.push_str(document);
                if kind != "Namespace" {
                    let name = object["metadata"]["name"].as_str().unwrap_or_default();
                    objects.names.insert((kind.to_string(), name.to_string()));
                }
            }
        }
        Ok(Self(rendered))
    }

    /// Write `ip`, the DynamoDB Local's pod IP, where the render left its
    /// placeholder: in the other clusters' hubs and segments. It is in
    /// their pod templates, so a new IP replaces their pods.
    fn fill(&mut self, ip: &str) {
        for objects in self.0.values_mut() {
            objects.pools = objects.pools.replace(k8s::PLACEHOLDER, ip);
        }
    }
}

/// Wait until the DynamoDB Local of `cluster` is ready, within the objects
/// bound: its pod's IP.
fn local_ip(kube: &mut dyn Kube, cluster: &Cluster, waits: &Waits) -> Result<String, Error> {
    let selector = format!("{},gdl.dev/directory={}", cluster.selector, k8s::LOCAL);
    let deadline = Instant::now() + waits.objects;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let reply = run(
            kube,
            cluster.at().pods(&selector),
            left.max(Duration::from_secs(1)),
            None,
            "listing the DynamoDB Local's pods",
        )?;
        let pods: Value = serde_json::from_str(&reply.stdout)
            .map_err(|e| Error::new(format!("`kubectl get pods` printed no JSON list: {e}")))?;
        let ready = pods["items"]
            .as_array()
            .into_iter()
            .flatten()
            .find_map(|pod| {
                let ready = pod["status"]["conditions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|c| c["type"] == "Ready" && c["status"] == "True");
                let gone = !pod["metadata"]["deletionTimestamp"].is_null();
                pod["status"]["podIP"]
                    .as_str()
                    .filter(|ip| ready && !gone && !ip.is_empty())
                    .map(str::to_string)
            });
        if let Some(ip) = ready {
            return Ok(ip);
        }
        if Instant::now() >= deadline {
            return Err(Error::new(format!(
                "cluster `{}`: the DynamoDB Local `{}` was not ready within {} s",
                cluster.name,
                k8s::LOCAL,
                waits.objects.as_secs()
            )));
        }
        std::thread::sleep(waits.interval);
    }
}

/// Run the tables Job `tables` in `cluster`: delete the last run, since a
/// Job's pod template cannot change and a Job that once succeeded would
/// not make the tables of a new Local, apply it, and wait until it
/// succeeds, within the objects bound. The uids applied. A Job that fails
/// is refused with its pod's log, which names the table.
fn run_tables(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    tables: &str,
    waits: &Waits,
) -> Result<Vec<String>, Error> {
    let at = cluster.at();
    let job = k8s::TABLES;
    let seconds = waits.objects.as_secs();
    run(
        kube,
        at.delete(&[format!("{JOBS}/{job}")], seconds),
        waits.objects,
        None,
        &format!("deleting the last tables Job in cluster `{}`", cluster.name),
    )?;
    let applied = apply(kube, cluster, tables, waits.objects)?;
    let deadline = Instant::now() + waits.objects;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let reply = run(
            kube,
            at.job(job),
            left.max(Duration::from_secs(1)),
            None,
            "reading the tables Job",
        )?;
        let object: Value = serde_json::from_str(&reply.stdout)
            .map_err(|e| Error::new(format!("`kubectl get job` printed no JSON: {e}")))?;
        let count = |key: &str| object["status"][key].as_u64().unwrap_or(0);
        if count("succeeded") > 0 {
            return Ok(applied);
        }
        if count("failed") > 0 {
            let log = kube.run(&bounded(at.job_log(job), waits.objects), None)?;
            let log = if log.success { log.stdout } else { log.stderr };
            return Err(Error::new(format!(
                "cluster `{}`: the tables Job `{job}` failed, so no pool is applied: {}",
                cluster.name,
                log.trim()
            )));
        }
        if Instant::now() >= deadline {
            return Err(Error::new(format!(
                "cluster `{}`: the tables Job `{job}` did not complete within {seconds} s",
                cluster.name
            )));
        }
        std::thread::sleep(waits.interval);
    }
}

/// Refuse a pool that `fleet` places in another cluster than one that
/// holds any of its Deployments. A pool keeps one name and one record
/// wherever it runs: a drain of its old copy would withdraw the record its
/// new copy serves, and a delete of the old copy undrained would cut its
/// requests. A segment whose hub's Deployment is gone still serves, and
/// its parent still holds the pool's record, so any Deployment counts.
/// Read before anything is applied, so a refused move changes nothing.
fn unmoved(
    kube: &mut dyn Kube,
    fleet: &Fleet,
    clusters: &Clusters,
    waits: &Waits,
) -> Result<(), Error> {
    let mut moved = Vec::new();
    for cluster in &clusters.0 {
        // Each pool with a Deployment or a GdlPool here, and whether its
        // hub's Deployment, or its GdlPool, is one.
        let mut live: BTreeMap<String, bool> = BTreeMap::new();
        for deployment in cluster.live(kube, waits.objects)? {
            if deployment.kind != "Deployment" {
                continue;
            }
            if let Some(pool) = &deployment.pool {
                *live.entry(pool.clone()).or_default() |= deployment.is_hub();
            }
        }
        for pool in operated::pools(kube, cluster, waits.objects)? {
            live.insert(pool.name, true);
        }
        for (pool, hub) in live {
            if let Some((placed, _)) = fleet.cluster_of(&pool)
                && placed != cluster.name
            {
                let runs = if hub {
                    "its hub runs"
                } else {
                    "its segments run"
                };
                moved.push(format!(
                    "pool `{pool}` is placed in cluster `{placed}`, but {runs} in cluster `{}`: a pool keeps one name and one record, so it cannot move. To move it, give the pool a new name in cluster `{placed}` and deploy, then remove `{pool}` from the fleet and deploy again, which drains it",
                    cluster.name
                ));
            }
        }
    }
    if moved.is_empty() {
        return Ok(());
    }
    Err(Error::all(moved))
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

/// Apply `objects`, a YAML stream, to `cluster`, server-side: the uid of
/// each object applied.
fn apply(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    objects: &str,
    budget: Duration,
) -> Result<Vec<String>, Error> {
    let what = format!("applying to cluster `{}`", cluster.name);
    let input = Some(objects.as_bytes());
    let reply = run(kube, cluster.at().apply(), budget, input, &what)?;
    let applied = Live::list(&reply.stdout).map_err(|e| Error::new(format!("{what}: {e}")))?;
    Ok(applied.into_iter().map(|object| object.uid).collect())
}

/// Stop `objects` of `cluster`: drain each pool among them that has a
/// parent and is not `retained`, wait until its parent's hub no longer
/// holds its record, and then delete them, segments first and hubs next,
/// so that a hub outlives the instances that report to it.
fn stop(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    objects: &[&Live],
    retained: &BTreeSet<String>,
    waits: &Waits,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let at = cluster.at();
    // A pool that this run still renders keeps serving: only objects it no
    // longer has are deleted, such as a renamed segment's, and it is
    // neither drained nor asked about at its parent.
    let (draining, hubless): (Vec<Stopping>, Vec<Stopping>) = Stopping::of(objects)
        .into_iter()
        .filter(|pool| !retained.contains(&pool.pool))
        .partition(|pool| pool.hub);
    // Checked before any drain starts, so that a pool that cannot be shown
    // to have withdrawn stops the step before anything changes.
    for pool in &hubless {
        withdrawn_without_hub(kube, at, pool, waits)?;
    }
    for pool in &draining {
        run(
            kube,
            at.post(&pool.pool, CONTROL, DRAIN),
            waits.objects,
            Some(b""),
            &format!("draining pool `{}`", pool.pool),
        )?;
        say(&format!("drain: pool `{}` is draining", pool.pool));
    }
    for pool in &draining {
        // Under the DynamoDB directory no parent's hub holds the pool's
        // record, so only the pool's own hub can say it is withdrawn.
        match pool.dynamodb {
            true => withdrawn_by_its_hub(kube, at, pool, waits)?,
            false => withdrawn(kube, at, pool, waits)?,
        }
        say(&format!("drain: pool `{}` has withdrawn", pool.pool));
    }
    let segments: Vec<String> = objects
        .iter()
        .filter(|object| object.is_segment())
        .map(|object| object.reference())
        .collect();
    let hubs: Vec<String> = objects
        .iter()
        .filter(|object| object.is_hub())
        .map(|object| object.reference())
        .collect();
    // The rest after every hub: a pool's other objects, and the DynamoDB
    // directory's Local and Job, which the hubs use until they stop.
    let rest: Vec<String> = objects
        .iter()
        .filter(|object| !object.is_segment() && !object.is_hub())
        .map(|object| object.reference())
        .collect();
    // Each Deployment's pods may take their grace to stop.
    let grace = objects.iter().filter_map(|o| o.grace).max().unwrap_or(0);
    let seconds = waits.objects.as_secs() + grace;
    for batch in [segments, hubs, rest] {
        if batch.is_empty() {
            continue;
        }
        run(
            kube,
            at.delete(&batch, seconds),
            Duration::from_secs(seconds),
            None,
            &format!("deleting from cluster `{}`", cluster.name),
        )?;
    }
    Ok(())
}

/// Wait until the hub of `pool`'s parent no longer holds its record: the
/// pool's drain has ended, and its hub has withdrawn it. Only the parent's
/// hub's own 404 proves it. A parent holds a pool that stopped without a
/// drain, as missing, until an operator withdraws it, so a parent that
/// cannot be reached proves nothing: the wait stops at once when its
/// Service does not exist, and at its bound when it does not answer,
/// naming the pool either way. The relay directory is the one that serves
/// this record: another directory needs another proof.
/// The parent of `pool`, a relay pool, whose hub holds its record: a pool
/// with `parents` alone is refused, since under the relay only one
/// parent's hub holds a pool's record.
fn relay_home(pool: &Stopping) -> Result<&str, Error> {
    pool.home.as_deref().ok_or_else(|| {
        Error::new(format!(
            "pool `{}` names no `home`, and under the relay directory only its one parent's hub can show that it withdrew, so it is not deleted",
            pool.pool
        ))
    })
}

/// Wait until the hub of `pool`, a pool under the DynamoDB directory,
/// says through its own Service that its drained record is withdrawn
/// (`GET /v1/ext/gdl-pool/withdrawn`): the directory has stored the
/// withdrawal. Its health says so too, but answers 503 for a drained pool,
/// and the service proxy hides the body of a 503. The wait is bounded as
/// the relay's, and names the pool when it runs out.
fn withdrawn_by_its_hub(
    kube: &mut dyn Kube,
    at: At<'_>,
    pool: &Stopping,
    waits: &Waits,
) -> Result<(), Error> {
    let args = at.get(&pool.pool, PUBLIC, WITHDRAWN);
    let bound = Duration::from_secs(pool.grace) + waits.drain;
    let deadline = Instant::now() + bound;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let reply = kube.run(&bounded(args.clone(), left), None)?;
        let said = reply.success
            && serde_json::from_str::<Value>(&reply.stdout)
                .is_ok_and(|body| body["withdrawn"] == true);
        if said {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let last = if reply.success {
                format!("its hub still says {}", reply.stdout.trim())
            } else {
                format!("its hub does not answer: {}", reply.stderr.trim())
            };
            return Err(Error::new(format!(
                "pool `{}` did not say within {} s that its record is withdrawn from the DynamoDB directory, so it is not deleted: {last}",
                pool.pool,
                bound.as_secs()
            )));
        }
        std::thread::sleep(waits.interval);
    }
}

fn withdrawn(kube: &mut dyn Kube, at: At<'_>, pool: &Stopping, waits: &Waits) -> Result<(), Error> {
    let home = relay_home(pool)?;
    let route = format!("/v1/ext/gdl-pool/report/{}/{}", pool.class, pool.pool);
    let args = at.get(home, PUBLIC, &route);
    let bound = Duration::from_secs(pool.grace) + waits.drain;
    let deadline = Instant::now() + bound;
    let unproven = |why: &str| {
        Error::new(format!(
            "pool `{}` is not known to have withdrawn from its parent `{home}`, so it is not deleted: {why}",
            pool.pool
        ))
    };
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let reply = kube.run(&bounded(args.clone(), left), None)?;
        if reply.proxied_not_found() {
            return Ok(());
        }
        if reply.no_service(home) {
            return Err(unproven(&format!(
                "the parent's Service does not exist: {}",
                reply.stderr.trim()
            )));
        }
        if Instant::now() >= deadline {
            let last = if reply.success {
                "its hub still holds the record".to_string()
            } else {
                format!("its hub does not answer: {}", reply.stderr.trim())
            };
            return Err(unproven(&format!("after {} s, {last}", bound.as_secs())));
        }
        std::thread::sleep(waits.interval);
    }
}

/// Refuse to delete `pool`, whose hub's Deployment is gone, unless its
/// parent's hub answers 404 for its record: with no hub, nothing can drain
/// it, and a record its parent still holds is offered nothing, but waits
/// for an operator to withdraw it.
fn withdrawn_without_hub(
    kube: &mut dyn Kube,
    at: At<'_>,
    pool: &Stopping,
    waits: &Waits,
) -> Result<(), Error> {
    // Under the DynamoDB directory no parent's hub holds the record, so
    // nothing here can show that the pool withdrew.
    if pool.dynamodb {
        return Err(Error::new(format!(
            "pool `{}` is not known to have withdrawn from the DynamoDB directory, so nothing is deleted: its hub's Deployment is gone, so it can neither drain nor say that its record is withdrawn, and no parent's hub holds its record. Withdraw it on a parent's hub (`POST /v1/ext/gdl-pool/children/{}/{}/withdraw` on the control port), which answers 204 once the directory has stored it; then delete its objects (`kubectl --context {} --namespace {} delete deployments,services,configmaps -l gdl.dev/pool={}`) and deploy again",
            pool.pool, pool.class, pool.pool, at.context, at.namespace, pool.pool
        )));
    }
    let home = relay_home(pool)?;
    let route = format!("/v1/ext/gdl-pool/report/{}/{}", pool.class, pool.pool);
    let reply = kube.run(&bounded(at.get(home, PUBLIC, &route), waits.objects), None)?;
    if reply.proxied_not_found() {
        return Ok(());
    }
    let why = if reply.success {
        let withdraw = format!(
            "/api/v1/namespaces/{}/services/http:{}:{CONTROL}/proxy/v1/ext/gdl-pool/children/{}/{}/withdraw",
            at.namespace, home, pool.class, pool.pool
        );
        format!(
            "its hub's Deployment is gone, so it cannot drain, and its parent still holds its record. Withdraw it on the parent's hub (`POST /v1/ext/gdl-pool/children/{}/{}/withdraw` on the control port), for example with `kubectl --context {} create --raw {withdraw} -f /dev/null`, then deploy again",
            pool.class, pool.pool, at.context
        )
    } else {
        format!(
            "its hub's Deployment is gone, and its parent's hub does not answer: {}",
            reply.stderr.trim()
        )
    };
    Err(Error::new(format!(
        "pool `{}` is not known to have withdrawn from its parent `{home}`, so nothing is deleted: {why}",
        pool.pool
    )))
}

/// Wait until the health of each of `pools`, by its cluster, answers 200.
fn healthy(kube: &mut dyn Kube, pools: &[(&Cluster, String)], waits: &Waits) -> Result<(), Error> {
    let deadline = Instant::now() + waits.health;
    let mut waiting: Vec<&(&Cluster, String)> = pools.iter().collect();
    // Why each pool the operator runs is not ready yet, as its status said.
    let mut reasons: BTreeMap<String, String> = BTreeMap::new();
    loop {
        let mut still = Vec::new();
        for entry in waiting {
            let (cluster, pool) = entry;
            if cluster.operator {
                // The operator reads the hub's health in the cluster, and
                // says it in the pool's status.
                if let Some(why) = operated::ready(kube, cluster, pool, deadline)? {
                    reasons.insert(pool.clone(), why);
                    still.push(entry);
                }
                continue;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            let args = bounded(cluster.at().get(pool, PUBLIC, HEALTH), left);
            if !kube.run(&args, None)?.success {
                still.push(entry);
            }
        }
        waiting = still;
        if waiting.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let mut message = format!(
                "the health of {} pool(s) did not answer 200 within {} s:",
                waiting.len(),
                waits.health.as_secs()
            );
            for (cluster, pool) in waiting {
                message.push_str(&format!("\n- pool `{pool}` in cluster `{}`", cluster.name));
                if let Some(why) = reasons.get(pool) {
                    message.push_str(&format!(": {why}"));
                }
                for line in why_not_ready(kube, cluster, pool, waits.objects)? {
                    message.push_str(&format!("\n  {line}"));
                }
            }
            return Err(Error::new(message));
        }
        std::thread::sleep(waits.interval);
    }
}

/// The pods of `pool` that are not ready, each with why its containers
/// wait and its latest events: these name a missing Secret or image, and
/// no Secret is read.
fn why_not_ready(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    pool: &str,
    budget: Duration,
) -> Result<Vec<String>, Error> {
    /// The events shown for each pod: the latest.
    const EVENTS: usize = 5;
    let selector = format!("{},gdl.dev/pool={}", cluster.selector, label_value(pool));
    let at = cluster.at();
    let reply = run(
        kube,
        at.pods(&selector),
        budget,
        None,
        "listing the pool's pods",
    )?;
    let pods: Value = serde_json::from_str(&reply.stdout)
        .map_err(|e| Error::new(format!("`kubectl get pods` printed no JSON list: {e}")))?;
    let mut lines = Vec::new();
    for pod in pods["items"].as_array().into_iter().flatten() {
        let ready = pod["status"]["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|c| c["type"] == "Ready" && c["status"] == "True");
        if ready {
            continue;
        }
        let name = pod["metadata"]["name"].as_str().unwrap_or_default();
        let mut line = format!(
            "pod `{name}`: {}",
            pod["status"]["phase"].as_str().unwrap_or("Unknown")
        );
        for status in pod["status"]["containerStatuses"]
            .as_array()
            .into_iter()
            .flatten()
        {
            for (state, what) in [("waiting", "waiting"), ("terminated", "terminated")] {
                let found = &status["state"][state];
                if let Some(reason) = found["reason"].as_str() {
                    line.push_str(&format!(", {what}: {reason}"));
                    if let Some(message) = found["message"].as_str() {
                        line.push_str(&format!(" ({message})"));
                    }
                }
            }
        }
        lines.push(line);
        let reply = run(
            kube,
            at.events(name),
            budget,
            None,
            "listing the pod's events",
        )?;
        let events: Value = serde_json::from_str(&reply.stdout)
            .map_err(|e| Error::new(format!("`kubectl get events` printed no JSON list: {e}")))?;
        let events = events["items"].as_array().cloned().unwrap_or_default();
        for event in events.iter().rev().take(EVENTS).rev() {
            lines.push(format!(
                "  {} {}: {}",
                event["type"].as_str().unwrap_or_default(),
                event["reason"].as_str().unwrap_or_default(),
                event["message"].as_str().unwrap_or_default().trim()
            ));
        }
    }
    Ok(lines)
}
