//! The pool controller against a fake API and a fake hub. The fake plays
//! the API server's fences (a write to an object of another uid, or to a
//! pool or an object at another resourceVersion, is refused as stale),
//! the scalers and the ReplicaSets (a suspended scaler's pods, and a
//! Deployment's at 0, exit when a test says so), and the hub (it says its
//! record is withdrawn once drained, when a test lets it). It logs every
//! write in order, a status write with the step and the uids it names.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::Value;

use super::api::{Kind, Live, Pools};
use super::hub::{Drain, Health, Hubs};
use super::reconcile::{Context, reconcile};
use crate::cluster::{PodView, Refused};
use crate::pool::{FINALIZER, GdlPool, GdlPoolSpec, GdlPoolStatus, StopStep};

#[path = "tests/stop.rs"]
mod stop;

#[path = "tests/apply.rs"]
mod apply;

#[path = "tests/recheck.rs"]
mod recheck;

#[path = "tests/alerts.rs"]
mod alerts;

const UID: &str = "pool-uid";

#[derive(Debug, Clone)]
struct Obj {
    live: Live,
    labelled: bool,
}

#[derive(Debug, Default)]
struct State {
    pool: Option<GdlPool>,
    version: u64,
    objects: BTreeMap<(Kind, String), Obj>,
    /// Running pods, by segment: `hub` for the hub's.
    pods: BTreeMap<String, u32>,
    log: Vec<String>,
    events: Vec<String>,
    next: u32,
    /// The status write after which every write fails, as if the
    /// operator's process ended there.
    crash_after: Option<usize>,
    status_writes: usize,
    crashed: bool,
    // The hub.
    drained: bool,
    /// Whether the hub says its record is withdrawn once drained.
    withdraws: bool,
    unreachable: bool,
    refuses_drain: Option<String>,
    hub_presence: gdl_k8s::Presence,
}

#[derive(Debug, Default)]
struct Fake {
    state: Mutex<State>,
    monitors: bool,
}

impl Fake {
    /// A fake that holds `pool`.
    fn with(pool: GdlPool) -> Self {
        let fake = Self::default();
        fake.state().pool = Some(pool);
        fake.state().version = 1;
        fake.state().withdraws = true;
        fake
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("state")
    }

    /// The pool as the API server holds it now.
    fn pool(&self) -> Option<GdlPool> {
        let state = self.state();
        state.pool.clone().map(|mut pool| {
            pool.metadata.resource_version = Some(state.version.to_string());
            pool
        })
    }

    fn status(&self) -> GdlPoolStatus {
        self.pool().and_then(|p| p.status).unwrap_or_default()
    }

    /// Reconcile the pool as it is now, at `now_ms`.
    async fn reconcile(&self, now_ms: i64) -> Result<Duration, String> {
        let Some(pool) = self.pool() else {
            return Ok(Duration::from_secs(60));
        };
        let cx = Context {
            pools: self,
            hubs: self,
            namespace: "ns",
        };
        reconcile(&cx, &pool, now_ms).await
    }

    /// Delete the pool, as `kubectl delete` with background propagation
    /// does: it stays while it has a finalizer.
    fn delete(&self, foreground: bool) {
        let mut state = self.state();
        let pool = state.pool.as_mut().expect("a pool");
        pool.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                k8s_openapi::jiff::Timestamp::UNIX_EPOCH,
            ));
        if foreground {
            pool.metadata
                .finalizers
                .get_or_insert_default()
                .push("foregroundDeletion".into());
        }
        state.version += 1;
    }

    /// The pods of each segment's Deployment, up to its count, as the
    /// ReplicaSets run them; a suspended scaler's Deployment, and one at
    /// 0, have none once their pods exit.
    fn settle(&self) {
        let mut state = self.state();
        let mut running = BTreeMap::new();
        for ((kind, name), object) in &state.objects {
            if *kind != Kind::Deployment {
                continue;
            }
            let segment = name.strip_prefix("p-").unwrap_or("hub").to_string();
            let suspended = state
                .objects
                .get(&(Kind::GdlScaler, name.clone()))
                .is_some_and(|s| s.live.suspended);
            let count = if suspended {
                0
            } else {
                u32::try_from(object.live.replicas.unwrap_or(1)).unwrap_or(0)
            };
            running.insert(segment, count);
        }
        state.pods = running;
    }

    fn log(&self) -> Vec<String> {
        self.state().log.clone()
    }

    fn uid(&self, kind: Kind, name: &str) -> String {
        self.state().objects[&(kind, name.to_string())]
            .live
            .uid
            .clone()
    }

    /// Make the object again under the same name: a new uid.
    fn recreate(&self, kind: Kind, name: &str) -> String {
        let mut state = self.state();
        state.next += 1;
        let uid = format!("uid-{}", state.next);
        let object = state
            .objects
            .get_mut(&(kind, name.to_string()))
            .expect("an object");
        object.live.uid = uid.clone();
        object.live.suspended = false;
        uid
    }
}

impl State {
    fn check(&self) -> Result<(), Refused> {
        if self.crashed {
            return Err(Refused::Failed("the operator ended".into()));
        }
        Ok(())
    }
}

/// The step and the uids a status names, for the log.
fn described(status: &GdlPoolStatus) -> String {
    match &status.stop {
        None => "none".into(),
        Some(stop) => {
            let uids: Vec<&str> = stop.objects.iter().map(|o| o.uid.as_str()).collect();
            format!("{:?} [{}]", stop.step, uids.join(" "))
        }
    }
}

impl Pools for Fake {
    async fn status(
        &self,
        _: &str,
        status: &GdlPoolStatus,
        version: &str,
    ) -> Result<String, Refused> {
        let mut state = self.state();
        state.check()?;
        if version != state.version.to_string() {
            return Err(Refused::Stale("the pool changed".into()));
        }
        state.version += 1;
        state.pool.as_mut().expect("a pool").status = Some(status.clone());
        let entry = format!("status {}", described(status));
        state.log.push(entry);
        state.status_writes += 1;
        if state.crash_after == Some(state.status_writes) {
            state.crashed = true;
        }
        Ok(state.version.to_string())
    }

    async fn finalizers(
        &self,
        _: &str,
        finalizers: &[String],
        version: &str,
    ) -> Result<String, Refused> {
        let mut state = self.state();
        state.check()?;
        if version != state.version.to_string() {
            return Err(Refused::Stale("the pool changed".into()));
        }
        state.version += 1;
        let deleting = state
            .pool
            .as_ref()
            .is_some_and(|p| p.metadata.deletion_timestamp.is_some());
        if deleting && state.pods.values().any(|n| *n > 0) {
            state
                .log
                .push("VIOLATION: the finalizer went while a pod runs".into());
        }
        state.pool.as_mut().expect("a pool").metadata.finalizers = Some(finalizers.to_vec());
        state
            .log
            .push(format!("finalizers {}", finalizers.join(" ")));
        Ok(state.version.to_string())
    }

    async fn event(&self, _: &GdlPool, _: bool, reason: &str, _: &str) {
        self.state().events.push(reason.to_string());
    }

    async fn labelled(&self, _: &str) -> Result<Vec<Live>, String> {
        Ok(self
            .state()
            .objects
            .values()
            .filter(|o| o.labelled)
            .map(|o| o.live.clone())
            .collect())
    }

    async fn get(&self, kind: Kind, name: &str) -> Result<Option<Live>, String> {
        Ok(self
            .state()
            .objects
            .get(&(kind, name.to_string()))
            .map(|o| o.live.clone()))
    }

    async fn apply(&self, object: &Value, live: Option<&Live>) -> Result<Live, Refused> {
        let uid = live.map(|live| live.uid.as_str());
        let mut state = self.state();
        state.check()?;
        let kind = Kind::of(object["kind"].as_str().unwrap_or_default()).expect("a kind");
        let name = object["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let key = (kind, name.clone());
        if let Some(uid) = uid
            && state.objects.get(&key).is_none_or(|o| o.live.uid != uid)
        {
            return Err(Refused::Stale(format!(
                "{} `{name}` was made again",
                kind.name()
            )));
        }
        state.next += 1;
        let fresh = format!("uid-{}", state.next);
        let version = state.next.to_string();
        let mut parsed = Live::of(kind, object).unwrap_or_else(|| {
            let mut with = object.clone();
            with["metadata"]["uid"] = fresh.clone().into();
            Live::of(kind, &with).expect("a view")
        });
        parsed.version = version;
        let entry = match state.objects.get(&key) {
            Some(old) => {
                parsed.uid = old.live.uid.clone();
                parsed.replica_owners = old.live.replica_owners.clone();
                parsed.suspended = old.live.suspended;
                parsed.replicas = parsed.replicas.or(old.live.replicas);
                format!("apply {}/{name}", kind.name())
            }
            None => {
                parsed.uid = fresh;
                parsed.replicas = parsed.replicas.or(Some(1));
                format!("create {}/{name}", kind.name())
            }
        };
        state.log.push(entry);
        state.objects.insert(
            key,
            Obj {
                live: parsed.clone(),
                labelled: true,
            },
        );
        Ok(parsed)
    }

    async fn write(
        &self,
        kind: Kind,
        name: &str,
        manager: &str,
        spec: Value,
        uid: &str,
        version: Option<&str>,
        _force: bool,
    ) -> Result<(), Refused> {
        let mut state = self.state();
        state.check()?;
        state.next += 1;
        let next = state.next.to_string();
        let key = (kind, name.to_string());
        let fenced = state.objects.get(&key).is_some_and(|object| {
            object.live.uid == uid && version.is_none_or(|v| v == object.live.version)
        });
        if !fenced {
            return Err(Refused::Stale(format!("{} `{name}` changed", kind.name())));
        }
        let segments_run = state
            .pods
            .iter()
            .any(|(segment, n)| segment != "hub" && *n > 0);
        if kind == Kind::Deployment && name == "p" && spec["replicas"] == 0 && segments_run {
            state
                .log
                .push("VIOLATION: the hub stopped while a segment pod runs".into());
        }
        let object = state.objects.get_mut(&key).expect("fenced above");
        if let Some(replicas) = spec["replicas"].as_i64() {
            object.live.replicas = Some(replicas);
            object.live.replica_owners.push(manager.to_string());
        }
        if kind == Kind::GdlScaler && spec.get("replicas").is_none() {
            object.live.suspended = !spec["suspend"].is_null();
        }
        object.live.version = next;
        state.log.push(format!(
            "write {}/{name} {uid} {spec} as {manager}",
            kind.name()
        ));
        Ok(())
    }

    async fn delete(&self, kind: Kind, name: &str, uid: &str) -> Result<(), Refused> {
        let mut state = self.state();
        state.check()?;
        let key = (kind, name.to_string());
        if state.objects.get(&key).is_some_and(|o| o.live.uid != uid) {
            return Err(Refused::Stale(format!(
                "{} `{name}` was made again",
                kind.name()
            )));
        }
        state.objects.remove(&key);
        state
            .log
            .push(format!("delete {}/{name} {uid}", kind.name()));
        Ok(())
    }

    async fn pods(&self, deployment: &Live) -> Result<Vec<PodView>, String> {
        let state = self.state();
        // A Deployment's pods are its segment's, while the Deployment is
        // the one of its name: the hub's for `p`.
        let segment = deployment.name.strip_prefix("p-").unwrap_or("hub");
        let current = state
            .objects
            .get(&(Kind::Deployment, deployment.name.clone()))
            .is_some_and(|held| held.live.uid == deployment.uid);
        let wanted = |s: &str| current && s == segment;
        let mut pods = Vec::new();
        for (segment, count) in state.pods.iter().filter(|(s, _)| wanted(s)) {
            for n in 0..*count {
                pods.push(PodView {
                    name: format!("{segment}-{n}"),
                    uid: format!("{segment}-{n}"),
                    resource_version: "1".into(),
                    ip: None,
                    created_ms: 0,
                    ready: true,
                    terminating: false,
                    routable: None,
                    system_port: None,
                });
            }
        }
        Ok(pods)
    }

    fn monitors(&self) -> bool {
        self.monitors
    }
}

impl Hubs for Fake {
    async fn health(&self, _: &str) -> Result<Health, String> {
        let state = self.state();
        if state.unreachable {
            return Err("the hub does not answer".into());
        }
        Ok(Health {
            healthy: !state.drained,
            state: if state.drained { "draining" } else { "ready" }.into(),
            segments: BTreeMap::new(),
            withdrawn: state.drained && state.withdraws,
            presence: Some(
                match state.hub_presence {
                    gdl_k8s::Presence::Serving => "serving",
                    gdl_k8s::Presence::ScaledToZero => "scaled_to_zero",
                }
                .into(),
            ),
        })
    }

    async fn withdrawn(&self, _: &str) -> Result<bool, String> {
        let state = self.state();
        Ok(state.drained && state.withdraws)
    }

    async fn drain(&self, _: &str) -> Result<Drain, String> {
        let mut state = self.state();
        if let Some(why) = state.refuses_drain.clone() {
            return Ok(Drain::Refused(why));
        }
        state.drained = true;
        state.log.push("drain".into());
        Ok(Drain::Draining)
    }

    async fn presence(&self, _: &str, presence: gdl_k8s::Presence) -> Result<(), String> {
        let mut state = self.state();
        state.hub_presence = presence;
        state.log.push(format!("presence {presence:?}"));
        Ok(())
    }
}

/// The spec of pool `p`, as [`pool`] gives it without a parent.
pub(super) fn spec() -> gdl_k8s::Pool {
    pool(false).spec.pool
}

/// Pool `p`, the render test's two segments, with a parent when `parent`.
fn pool(parent: bool) -> GdlPool {
    let segment = |name: &str, ports: Vec<gdl_k8s::Port>| gdl_k8s::Segment {
        name: name.into(),
        image: format!("registry.example/{name}:tag"),
        settings: "[system]\nport = 9090\n[runtime]\ndrain_ms = 10000\n".into(),
        quorum: 2,
        ports,
        env: Vec::new(),
        secrets: Vec::new(),
        resources: None,
    };
    let spec = gdl_k8s::Pool {
        fleet: "api".into(),
        cluster: "use1".into(),
        class: "model".into(),
        region: "us-east-1".into(),
        deployment: "{\"graph\": 1}".into(),
        hub: gdl_k8s::Hub {
            image: "registry.example/hub:tag".into(),
            env: Vec::new(),
            resources: None,
            state_dir: None,
        },
        segments: vec![
            segment("engine", Vec::new()),
            segment(
                "front",
                vec![gdl_k8s::Port {
                    port: 8080,
                    protocol: gdl_k8s::Protocol::Tcp,
                }],
            ),
        ],
        service_account_name: "api".into(),
        node_selector: BTreeMap::new(),
        tolerations: Vec::new(),
        parent: parent.then(|| gdl_k8s::Parent::Home("global".into())),
        dynamodb: None,
        pod_monitor: false,
        presence: gdl_k8s::Presence::Serving,
        allow_pool_restart: false,
    };
    let mut pool = GdlPool::new("p", GdlPoolSpec { pool: spec });
    pool.metadata.namespace = Some("ns".into());
    pool.metadata.uid = Some(UID.into());
    pool.metadata.generation = Some(1);
    pool
}

/// A fake that holds pool `p`, made and running: its objects applied and
/// its pods up.
async fn running(parent: bool) -> Fake {
    let fake = Fake::with(pool(parent));
    fake.reconcile(0).await.expect("the pool is made");
    fake.settle();
    let mut state = fake.state();
    state.log.clear();
    // Crashes count the stop's status writes alone.
    state.status_writes = 0;
    drop(state);
    fake
}

/// The condition `kind` of the pool's status, as `(status, reason)`.
fn condition(fake: &Fake, kind: &str) -> Option<(String, String)> {
    fake.status()
        .conditions
        .iter()
        .find(|c| c.type_ == kind)
        .map(|c| (c.status.clone(), c.reason.clone()))
}

/// Whether the pool's finalizer is gone.
fn released(fake: &Fake) -> bool {
    fake.pool()
        .and_then(|p| p.metadata.finalizers)
        .is_none_or(|f| !f.iter().any(|f| f == FINALIZER))
}

/// The step the pool's stop stores.
fn step(fake: &Fake) -> Option<StopStep> {
    fake.status().stop.map(|s| s.step)
}
