//! The apply of a render to a fleet's clusters: the refusals read before
//! anything changes, each cluster's namespace and base, the DynamoDB
//! directory's Local and tables Job, and the pools.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use gdl_model::Fleet;
use serde_json::Value;

use super::cluster::{Cluster, Clusters};
use super::kube::{JOBS, Kube, bounded};
use super::live::Live;
use super::operated;
use super::{Waits, k8s, run};
use crate::{Error, Files};

/// Apply `files`, the render, to `clusters`, and write them to `out` when
/// given, once complete: steps 3 to 6 of [`super::super::deploy`].
pub(super) fn apply_fleet(
    kube: &mut dyn Kube,
    fleet: &Fleet,
    clusters: &Clusters,
    mut files: Files,
    out: Option<&Path>,
    waits: &Waits,
    say: &mut dyn FnMut(&str),
) -> Result<Applied, Error> {
    let rendered = Rendered::of(&files, clusters)?;
    unmoved(kube, fleet, clusters, waits)?;
    // A cluster that changes its kind is refused before anything is
    // applied, in any cluster.
    for cluster in &clusters.0 {
        match cluster.operator {
            true => operated::refuse_opted_in(cluster, &cluster.live(kube, waits.objects)?)?,
            false => {
                let pools = documents(&rendered.0[&cluster.name].pools)?;
                operated::refuse_opted_out(kube, cluster, &pools, waits)?;
            }
        }
    }
    // Every object this run applied, in any cluster, by its uid: none is
    // pruned, even where two clusters' contexts reach one namespace.
    let mut applied = BTreeSet::new();

    // The directory's cluster first: the other clusters' hubs name the pod
    // IP of its store, a DynamoDB Local or etcd, which exists only once the
    // store runs.
    let store = k8s::Store::of(fleet);
    let home = store.as_ref().map(|store| &store.cluster);
    let mut order: Vec<&Cluster> = clusters.0.iter().collect();
    order.sort_by_key(|cluster| Some(&cluster.name) != home);
    let mut local = None;
    for cluster in order {
        let objects = &rendered.0[&cluster.name];
        cluster.prepare_namespace(kube, objects, waits.objects)?;
        applied.extend(apply(kube, cluster, &objects.base, waits.objects)?);
        run(
            kube,
            cluster.at().namespace_active(waits.objects.as_secs()),
            waits.objects,
            None,
            "waiting for the namespace",
        )?;
        if let Some(store) = store.as_ref().filter(|s| s.cluster == cluster.name) {
            local = Some((store, local_ip(kube, cluster, store, waits)?));
        }
    }
    say("base: each cluster's namespace and service account");
    let mut rendered = rendered;
    if let Some((store, ip)) = &local {
        rendered.fill(store.placeholder, ip);
        for text in files.values_mut() {
            *text = text.replace(store.placeholder, ip);
        }
        say(&format!("base: the {} runs at {ip}", store.what));
    }
    if let Some(out) = out {
        k8s::write_new(out, &files)?;
        say(&format!("k8s: the objects are in {}", out.display()));
    }

    for cluster in &clusters.0 {
        let tables = &rendered.0[&cluster.name].tables;
        if tables.trim().is_empty() {
            continue;
        }
        applied.extend(run_tables(kube, cluster, tables, waits)?);
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
        applied.extend(apply(kube, cluster, pools, waits.objects)?);
    }
    say("apply: each cluster's pools");
    let names = rendered
        .0
        .into_iter()
        .map(|(cluster, objects)| (cluster, objects.names))
        .collect();
    Ok(Applied {
        uids: applied,
        names,
    })
}

/// What an apply did.
#[derive(Debug, Default)]
pub(super) struct Applied {
    /// Every object applied, in any cluster, by its uid: none is pruned,
    /// even where two clusters' contexts reach one namespace.
    pub(super) uids: BTreeSet<String>,
    /// The kind and name of each namespaced object rendered, by cluster.
    pub(super) names: BTreeMap<String, BTreeSet<(String, String)>>,
}

/// Set `allowPoolRestart` on each `GdlPool` of `files`, the objects of a
/// render: only a cluster the gdl operator runs has them.
pub(super) fn allow_restart(files: &mut Files, clusters: &Clusters) -> Result<(), Error> {
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
        *text = k8s::stream(&objects).map_err(crate::deployment::io)?;
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

/// The objects of one render, by cluster: the Namespace, the rest of the
/// base, and the pools', each as one YAML stream, and the kind and name of
/// each namespaced object.
pub(super) struct Rendered(BTreeMap<String, Objects>);

#[derive(Default)]
pub(super) struct Objects {
    /// The Namespace, created only where none exists.
    pub(super) namespace: String,
    /// The Namespace's labels, which an existing namespace gets instead.
    pub(super) labels: Vec<(String, String)>,
    pub(super) base: String,
    /// The tables Job, in the DynamoDB directory's cluster alone.
    pub(super) tables: String,
    pub(super) pools: String,
    pub(super) names: BTreeSet<(String, String)>,
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

    /// Write `ip`, the pod IP of the directory's store, where the render
    /// left its `placeholder`: in the other clusters' hubs and segments. It
    /// is in their pod templates, so a new IP replaces their pods.
    fn fill(&mut self, placeholder: &str, ip: &str) {
        for objects in self.0.values_mut() {
            objects.pools = objects.pools.replace(placeholder, ip);
        }
    }
}

/// Wait until `store`, which `cluster` runs, is ready, within the objects
/// bound: its pod's IP.
fn local_ip(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    store: &k8s::Store,
    waits: &Waits,
) -> Result<String, Error> {
    let selector = format!("{},gdl.dev/directory={}", cluster.selector, store.name);
    let deadline = Instant::now() + waits.objects;
    // An apply that changed the store's pod leaves the old pod ready, at
    // its old IP, until the controller replaces it: wait for the rollout
    // first, so that the IP read is the new pod's. A read of the
    // Deployment, which needs only `get`, where `kubectl rollout status`
    // would need `watch` too.
    let deployment = format!("deployments.apps/{}", store.name);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let reply = run(
            kube,
            cluster.at().object(&deployment),
            left.max(Duration::from_secs(1)),
            None,
            &format!("reading the {}'s Deployment", store.what),
        )?;
        let read: Option<Value> = serde_json::from_str(&reply.stdout).ok();
        if read.as_ref().is_some_and(rolled_out) {
            break;
        }
        if Instant::now() >= deadline {
            return Err(Error::new(format!(
                "cluster `{}`: the {} `{}` did not finish its rollout within {} s",
                cluster.name,
                store.what,
                store.name,
                waits.objects.as_secs()
            )));
        }
        std::thread::sleep(waits.interval);
    }
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let reply = run(
            kube,
            cluster.at().pods(&selector),
            left.max(Duration::from_secs(1)),
            None,
            &format!("listing the {}'s pods", store.what),
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
                "cluster `{}`: the {} `{}` was not ready within {} s",
                cluster.name,
                store.what,
                store.name,
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

/// Whether `deployment`, a Deployment as the API server holds it, has
/// finished its rollout: its controller has seen its latest change, and
/// every one of its pods is of that change, with none of an earlier one
/// left. Kubernetes leaves a count of 0 out of the status.
fn rolled_out(deployment: &Value) -> bool {
    let wanted = deployment["spec"]["replicas"].as_u64().unwrap_or(1);
    let status = &deployment["status"];
    let count = |key: &str| status[key].as_u64().unwrap_or(0);
    deployment["metadata"]["generation"]
        .as_u64()
        .is_some_and(|generation| status["observedGeneration"].as_u64() >= Some(generation))
        && count("updatedReplicas") == wanted
        && count("replicas") == wanted
}
