//! A cluster the gdl operator runs (`[cluster.<name>] operator`): `deploy`
//! applies one `GdlPool` per pool there, and the operator makes and keeps
//! the pool's objects. `deploy` and `down` never list, change or delete
//! those objects. They delete a `GdlPool` alone, with background
//! propagation, so that its finalizer stops the pool in order, and wait
//! until it is gone. A pool is ready once its status says the operator has
//! acted on its generation and its hub is healthy.
//!
//! Pools stop children first, across the whole fleet, whatever kind of
//! cluster each runs in ([`Order`]): a child's drain needs its parent's
//! hub.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use serde_json::Value;

use super::kube::{POOLS, bounded};
use super::live::Live;
use super::{Cluster, Kube, Waits, run};
use crate::Error;

/// The manager that owns a segment Deployment's `replicas` under the
/// operator.
const SCALER_MANAGER: &str = "gdl-scaler";

/// The margin a pool's stop is given past its drain, its segments' stop
/// and its hub's: the time to see each step through.
const STOP_MARGIN: Duration = Duration::from_secs(60);

/// A `GdlPool`, as `deploy` and `down` read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Operated {
    pub(super) name: String,
    pub(super) uid: String,
    pub(super) class: String,
    /// Its `spec.parent`.
    pub(super) parent: Option<Parentage>,
    /// Its `metadata.generation`.
    generation: i64,
    /// Its whole status.
    status: Value,
    /// The longest stop grace of its segments, in seconds.
    grace: u64,
}

/// A pool's parent: one pool, or every pool of a class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Parentage {
    Home(String),
    Parents(String),
}

impl Operated {
    /// The `GdlPool`s of a `kubectl get --output json` list, or one.
    pub(super) fn list(text: &str) -> Result<Vec<Self>, Error> {
        let list: Value = serde_json::from_str(text)
            .map_err(|e| Error::new(format!("`kubectl` printed no JSON: {e}")))?;
        let items = match list.get("items") {
            Some(items) => items.as_array().cloned().unwrap_or_default(),
            None => vec![list],
        };
        items.iter().map(Self::of).collect()
    }

    fn of(object: &Value) -> Result<Self, Error> {
        let text = |value: &Value| value.as_str().map(str::to_string);
        let name = text(&object["metadata"]["name"])
            .ok_or_else(|| Error::new("`kubectl` printed a GdlPool with no name"))?;
        let spec = &object["spec"];
        let parent = match (
            text(&spec["parent"]["home"]),
            text(&spec["parent"]["parents"]),
        ) {
            (Some(home), _) => Some(Parentage::Home(home)),
            (None, Some(class)) => Some(Parentage::Parents(class)),
            (None, None) => None,
        };
        let grace = spec["segments"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|segment| segment["settings"].as_str())
            .filter_map(|settings| gdl_k8s::Settings::read(settings).ok())
            .map(|settings| settings.grace())
            .max()
            .unwrap_or(0);
        Ok(Self {
            uid: text(&object["metadata"]["uid"]).unwrap_or_default(),
            class: text(&spec["class"]).unwrap_or_default(),
            parent,
            generation: object["metadata"]["generation"].as_i64().unwrap_or(0),
            status: object["status"].clone(),
            grace,
            name,
        })
    }

    /// How long its stop may take: its drain, bounded by its segments'
    /// longest grace and 60 s, then its segments' stop, then its hub's, and
    /// a margin.
    fn stop_bound(&self) -> Duration {
        let grace = Duration::from_secs(self.grace);
        grace + Duration::from_secs(60) + grace + Duration::from_secs(30) + STOP_MARGIN
    }

    /// Condition `kind` of its status, when it is on and speaks of its
    /// current generation: its message.
    fn on(&self, kind: &str) -> Option<String> {
        self.status["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|c| c["type"] == kind && c["status"] == "True")
            .filter(|c| c["observedGeneration"].as_i64() == Some(self.generation))
            .map(|c| c["message"].as_str().unwrap_or_default().to_string())
    }

    /// What its status says of its stop, for a message.
    fn stopping(&self) -> String {
        if self.status.is_null() {
            return "the operator has not acted on it: is it running in the namespace?".into();
        }
        let step = self.status["stop"]["step"].as_str().unwrap_or("none");
        let mut said = format!("its stop is at step `{step}`");
        for condition in self.status["conditions"].as_array().into_iter().flatten() {
            let kind = condition["type"].as_str().unwrap_or_default();
            if ["Withdrawn", "HubGone", "ForegroundDelete"].contains(&kind) {
                said.push_str(&format!(
                    "; {kind}: {}",
                    condition["message"].as_str().unwrap_or_default()
                ));
            }
        }
        said
    }
}

/// The `GdlPool`s of `cluster` that carry its fleet's and its labels: none
/// where the cluster has no `GdlPool` CRD, which is then not asked. Where
/// it has the kind, a list the deployer may not make is refused, naming
/// the right: it would hide pools the operator runs.
pub(super) fn pools(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    budget: Duration,
) -> Result<Vec<Operated>, Error> {
    if !cluster.pools {
        return Ok(Vec::new());
    }
    let args = bounded(cluster.at().pools(&cluster.selector), budget);
    let reply = kube.run(&args, None)?;
    // A list the deployer may not make proves nothing: the operator may
    // still run pools there, which a plain deploy would not stop in order.
    if reply.stderr.contains("(Forbidden)") {
        return Err(Error::new(format!(
            "cluster `{}` has the gdl operator's GdlPool kind, and context `{}` may not list `gdlpools.gdl.dev` in namespace `{}`, so whether the operator runs pools of the fleet there is not known: grant the deployer `list` on `gdlpools.gdl.dev`, or remove the gdl CRDs from a cluster the operator does not run",
            cluster.name, cluster.context, cluster.namespace
        )));
    }
    if !reply.success {
        return Err(Error::new(format!(
            "listing the fleet's GdlPools in cluster `{}`: `kubectl {}` failed: {}",
            cluster.name,
            args.join(" "),
            reply.stderr.trim()
        )));
    }
    Operated::list(&reply.stdout)
        .map_err(|e| Error::new(format!("cluster `{}`: {e}", cluster.name)))
}

/// Delete `pools` of `cluster`, each with background propagation and only
/// the object of the uid listed, and wait until each is gone: its
/// finalizer has stopped it. A pool made again under its name since the
/// list, another object, is left in place, and the delete fails naming it
/// and its uid once the others are gone, so that no pool above it stops. A wait that runs out
/// names the step of each pool's stop and why it waits.
pub(super) fn delete(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    pools: &[&Operated],
    waits: &Waits,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    if pools.is_empty() {
        return Ok(());
    }
    let mut deleted = Vec::new();
    // A pool made again since the list: its level is not done, and a
    // parent may still serve it.
    let mut replaced = Vec::new();
    for pool in pools {
        let args = bounded(cluster.at().delete_pool(&pool.name), waits.objects);
        let body = serde_json::json!({
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "propagationPolicy": "Background",
            "preconditions": {"uid": pool.uid},
        });
        let reply = kube.run(&args, Some(body.to_string().as_bytes()))?;
        if reply.success || reply.stderr.contains("(NotFound)") {
            deleted.push(*pool);
        } else if let Some(uid) = recorded_uid(&reply.stderr) {
            replaced.push((pool.name.as_str(), uid.to_string()));
        } else {
            return Err(Error::new(format!(
                "cluster `{}`: deleting GdlPool `{}`: `kubectl {}` failed: {}",
                cluster.name,
                pool.name,
                args.join(" "),
                reply.stderr.trim()
            )));
        }
    }
    let bound = deleted
        .iter()
        .map(|pool| pool.stop_bound())
        .max()
        .unwrap_or_default()
        + waits.objects;
    let deadline = Instant::now() + bound;
    let mut waiting = deleted;
    loop {
        let mut still = Vec::new();
        for pool in waiting {
            let left = deadline.saturating_duration_since(Instant::now());
            let reply = run(
                kube,
                cluster.at().object(&format!("{POOLS}/{}", pool.name)),
                left.max(Duration::from_secs(1)),
                None,
                &format!(
                    "reading GdlPool `{}` in cluster `{}`",
                    pool.name, cluster.name
                ),
            )?;
            let read = match reply.stdout.trim().is_empty() {
                true => Vec::new(),
                false => Operated::list(&reply.stdout)?,
            };
            // The listed one still stops; another of its name, made after
            // its delete, is a replacement, as a refused delete shows one;
            // none: it is gone.
            match read.first() {
                Some(read) if read.uid == pool.uid => still.push(pool),
                Some(read) => replaced.push((pool.name.as_str(), read.uid.clone())),
                None => {}
            }
        }
        waiting = still;
        if waiting.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            let mut message = format!(
                "cluster `{}`: GdlPool(s) not gone within {} s:",
                cluster.name,
                bound.as_secs()
            );
            for pool in self::pools(kube, cluster, waits.objects)? {
                if waiting.iter().any(|w| w.uid == pool.uid) {
                    message.push_str(&format!("\n- pool `{}`: {}", pool.name, pool.stopping()));
                }
            }
            return Err(Error::new(message));
        }
        std::thread::sleep(waits.interval);
    }
    let names: Vec<&str> = pools
        .iter()
        .map(|pool| pool.name.as_str())
        .filter(|name| !replaced.iter().any(|(other, _)| other == name))
        .collect();
    if !names.is_empty() {
        say(&format!(
            "stop: pool(s) {} in cluster `{}` stopped and deleted",
            names.join(", "),
            cluster.name
        ));
    }
    if !replaced.is_empty() {
        return Err(Error::all(replaced.into_iter().map(|(name, uid)| {
            let listed = pools
                .iter()
                .find(|pool| pool.name == name)
                .map_or("", |pool| pool.uid.as_str());
            format!(
                "pool `{name}` in cluster `{}` was made again since it was listed (uid {uid}, where {listed} was listed): it is left in place, and no pool above it is stopped. Find who made it, then run again",
                cluster.name
            )
        })));
    }
    Ok(())
}

/// The uid the API server says the object it holds has, when it refused a
/// delete whose precondition named another: `the UID in record (<uid>)`.
fn recorded_uid(stderr: &str) -> Option<&str> {
    let (_, rest) = stderr.split_once("does not match the UID in record (")?;
    rest.split_once(')').map(|(uid, _)| uid)
}

/// Wait until `pool` of `cluster` is ready: the operator has applied its
/// generation (`Applied`, of that generation), and its hub says it is
/// healthy. A generation the operator
/// holds or refuses stops the wait at once, with its reason.
pub(super) fn ready(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    pool: &str,
    deadline: Instant,
) -> Result<Option<String>, Error> {
    let left = deadline.saturating_duration_since(Instant::now());
    let reply = run(
        kube,
        cluster.at().object(&format!("{POOLS}/{pool}")),
        left.max(Duration::from_secs(1)),
        None,
        &format!("reading GdlPool `{pool}` in cluster `{}`", cluster.name),
    )?;
    // `--ignore-not-found` prints nothing for a pool that is gone.
    if reply.stdout.trim().is_empty() {
        return Ok(Some("its GdlPool does not exist".into()));
    }
    let read = Operated::list(&reply.stdout)?;
    let Some(read) = read.first() else {
        return Ok(Some("its GdlPool does not exist".into()));
    };
    for (kind, remedy) in [
        (
            "PoolRestartPending",
            "deploy again with `--allow-pool-restart` to restart it",
        ),
        (
            "NotOwned",
            "take the fleet down in that cluster, then deploy again",
        ),
    ] {
        if let Some(message) = read.on(kind) {
            return Err(Error::new(format!(
                "pool `{pool}` in cluster `{}`: {message}; {remedy}",
                cluster.name
            )));
        }
    }
    let acted = read.status["observedGeneration"].as_i64() == Some(read.generation);
    if !acted {
        return Ok(Some(read.stopping_or_pending()));
    }
    // The operator stores the generation it acts on before it applies it:
    // only `Applied` for this generation says the apply has landed.
    if read.on("Applied").is_none() {
        return Ok(Some(format!(
            "the operator has not applied its generation {} yet",
            read.generation
        )));
    }
    match read.status["phase"].as_str() {
        Some("Ready") => Ok(None),
        phase => Ok(Some(format!(
            "its phase is {}{}",
            phase.unwrap_or("unknown"),
            read.healthy()
        ))),
    }
}

impl Operated {
    fn stopping_or_pending(&self) -> String {
        if self.status.is_null() {
            return "the operator has not acted on it: is it running in the namespace?".into();
        }
        format!(
            "the operator has not acted on its generation {} yet",
            self.generation
        )
    }

    fn healthy(&self) -> String {
        self.status["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|c| c["type"] == "Healthy")
            .and_then(|c| c["message"].as_str())
            .filter(|m| !m.is_empty())
            .map_or_else(String::new, |m| format!(": {m}"))
    }
}

/// Refuse `cluster`, which does not ask for the operator, when the operator
/// still runs pools of the fleet there, or still owns the `replicas` of a
/// segment Deployment that `rendered` holds: `gdl`'s apply of the count
/// would conflict, and its prune would not stop those pools in order. Read
/// before anything is applied, in any cluster.
pub(super) fn refuse_opted_out(
    kube: &mut dyn Kube,
    cluster: &Cluster,
    rendered: &[Value],
    waits: &Waits,
) -> Result<(), Error> {
    let mut held = Vec::new();
    for pool in pools(kube, cluster, waits.objects)? {
        held.push(format!("GdlPool `{}`", pool.name));
    }
    for object in rendered {
        if object["kind"] != "Deployment" || object["spec"].get("replicas").is_none() {
            continue;
        }
        if object["metadata"]["labels"]["gdl.dev/segment"] == "hub" {
            continue;
        }
        let name = object["metadata"]["name"].as_str().unwrap_or_default();
        let reference = format!("deployments.apps/{name}");
        let reply = run(
            kube,
            cluster.at().object(&reference),
            waits.objects,
            None,
            &format!("reading {reference} in cluster `{}`", cluster.name),
        )?;
        if reply.stdout.trim().is_empty() {
            continue;
        }
        let live: Value = serde_json::from_str(&reply.stdout)
            .map_err(|e| Error::new(format!("`kubectl get {reference}` printed no JSON: {e}")))?;
        let owners = live["metadata"]["managedFields"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|entry| entry["fieldsV1"]["f:spec"].get("f:replicas").is_some())
            .filter_map(|entry| entry["manager"].as_str());
        if owners.into_iter().any(|owner| owner == SCALER_MANAGER) {
            held.push(format!("the `replicas` of Deployment `{name}`"));
        }
    }
    if held.is_empty() {
        return Ok(());
    }
    Err(Error::new(format!(
        "cluster `{}` does not set `operator = true`, but the gdl operator still holds {} there: a deploy without the operator would conflict with it, and would not stop its pools in order. Keep `[cluster.{}] operator = true`; or take the fleet down there with `operator = true` (`cargo gdl down`), then deploy again",
        cluster.name,
        held.join(", "),
        cluster.name
    )))
}

/// Refuse `cluster`, which asks for the operator, when it holds objects of
/// the fleet that no `GdlPool` controls, as an earlier deploy without the
/// operator leaves: the operator changes no object it did not make, so
/// each pool there would wait, refused. Read before anything is applied.
pub(super) fn refuse_opted_in(cluster: &Cluster, live: &[Live]) -> Result<(), Error> {
    let plain: Vec<String> = live
        .iter()
        .filter(|object| object.pool.is_some() && !object.operated)
        .map(Live::reference)
        .collect();
    if plain.is_empty() {
        return Ok(());
    }
    Err(Error::new(format!(
        "cluster `{}` sets `operator = true`, and holds objects of the fleet's pools that the gdl operator did not make: {}. The operator changes no object it did not make. Take the fleet down there without `operator` (`cargo gdl down`), then deploy again",
        cluster.name,
        plain.join(", ")
    )))
}

/// The fleet's pools, each with its parents, from the fleet file and from
/// what every cluster holds: the order in which pools stop, children
/// first.
#[derive(Debug, Default)]
pub(super) struct Order {
    /// Each pool's class.
    class: BTreeMap<String, String>,
    /// Each pool's parent.
    parent: BTreeMap<String, Parentage>,
}

impl Order {
    /// Note `pool` of `class`, whose parent is `parent`.
    pub(super) fn add(&mut self, pool: &str, class: &str, parent: Option<Parentage>) {
        self.class.insert(pool.to_string(), class.to_string());
        if let Some(parent) = parent {
            self.parent.entry(pool.to_string()).or_insert(parent);
        }
    }

    /// Note the pools of `fleet`.
    pub(super) fn fleet(&mut self, fleet: &gdl_model::Fleet) {
        for pool in fleet.pools() {
            let parent = pool.parent.as_ref().map(|parent| match parent {
                gdl_model::Parent::Pool(home) => Parentage::Home(home.clone()),
                gdl_model::Parent::Class(class) => Parentage::Parents(class.clone()),
            });
            self.add(&pool.name, &pool.class, parent);
        }
    }

    /// Note the pools of `live`, a plain cluster's objects.
    pub(super) fn live(&mut self, live: &[Live]) {
        for object in live.iter().filter(|o| o.kind == "Deployment") {
            let (Some(pool), Some(class)) = (&object.pool, &object.class) else {
                continue;
            };
            let parent = match (&object.home, &object.parents) {
                (Some(home), _) => Some(Parentage::Home(home.clone())),
                (None, Some(class)) => Some(Parentage::Parents(class.clone())),
                (None, None) => None,
            };
            self.add(pool, class, parent);
        }
    }

    /// Note `pools`, an operated cluster's.
    pub(super) fn operated(&mut self, pools: &[Operated]) {
        for pool in pools {
            self.add(&pool.name, &pool.class, pool.parent.clone());
        }
    }

    /// How deep `pool` is: 0 for a pool with no parent, else one more than
    /// its deepest parent. A pool not known is a top pool.
    pub(super) fn depth(&self, pool: &str) -> usize {
        self.depth_of(pool, &mut BTreeSet::new())
    }

    fn depth_of(&self, pool: &str, seen: &mut BTreeSet<String>) -> usize {
        // A cycle, which a checked fleet never has, ends at the pool seen.
        if !seen.insert(pool.to_string()) {
            return 0;
        }
        let parents: Vec<&String> = match self.parent.get(pool) {
            None => Vec::new(),
            Some(Parentage::Home(home)) => vec![home],
            Some(Parentage::Parents(class)) => self
                .class
                .iter()
                .filter(|(_, c)| *c == class)
                .map(|(name, _)| name)
                .collect(),
        };
        let deepest = parents
            .into_iter()
            .map(|parent| self.depth_of(parent, &mut seen.clone()))
            .max();
        deepest.map_or(0, |depth| depth + 1)
    }
}
