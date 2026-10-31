//! The scaler against a fake cluster that plays a Deployment's
//! ReplicaSet: it deletes the pod of the lowest deletion cost, or, when a
//! test says so, another one, and makes pods up to the count.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use super::*;
use crate::cluster::{Fence, Refused, Target};
use crate::{GdlScalerSpec, Suspend, TargetRef};

const SERVICE: &str = "global-front";

#[path = "tests/review.rs"]
mod review;

#[path = "tests/two.rs"]
mod two;

#[derive(Debug, Clone)]
struct FakePod {
    view: PodView,
    cost: Option<i32>,
    routable: bool,
    load: u64,
}

#[derive(Debug, Default)]
struct State {
    replicas: i32,
    rolling: bool,
    pods: Vec<FakePod>,
    /// The pods the Service's EndpointSlices list: the controller drops a
    /// pod that is not routable only when the test says it has caught up.
    listed: BTreeSet<String>,
    status: Option<GdlScalerStatus>,
    /// The scaler's resourceVersion, moved by each status write.
    scaler_version: u64,
    /// The Deployment's uid and resourceVersion, moved by each write.
    deployment: String,
    version: u64,
    /// Every write to a pod or the target, in order.
    writes: Vec<String>,
    /// Every write, status writes included, in order.
    events: Vec<String>,
    next: u32,
}

#[derive(Debug, Default)]
struct Fake {
    state: Mutex<State>,
    /// The hub's instance list; `None` when the hub does not answer.
    instances: Mutex<Option<Instances>>,
    /// Pods whose `/metrics` does not answer, by IP.
    silent: Mutex<BTreeSet<String>>,
    /// Held by each read of the pods, so that two reconciles both read
    /// before either writes.
    barrier: Option<tokio::sync::Barrier>,
}

impl Fake {
    /// A target of `loads.len()` ready pods, the n-th created at n s and
    /// holding `loads[n]` admitted requests.
    fn with(loads: &[u64]) -> Self {
        let fake = Self::default();
        {
            let mut state = fake.state.lock().expect("state");
            state.replicas = i32::try_from(loads.len()).expect("a count");
            for load in loads {
                let pod = state.make();
                state.pods.push(FakePod { load: *load, ..pod });
            }
            state.listed = state.pods.iter().map(|p| p.view.uid.clone()).collect();
            state.deployment = "deployment-a".into();
        }
        fake
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("state")
    }

    /// The ReplicaSet's step: delete pods down to the count in
    /// Kubernetes' order, a pod that is not ready first, then the lowest
    /// cost, then the newest (`ActivePodsWithRanks`), or the oldest when
    /// `misrank`; and make pods up to it.
    fn settle(&self, misrank: bool) {
        let mut state = self.state();
        loop {
            let live: Vec<usize> = (0..state.pods.len())
                .filter(|&i| !state.pods[i].view.terminating)
                .collect();
            let replicas = usize::try_from(state.replicas).expect("a count");
            if live.len() > replicas {
                let pick = if misrank {
                    *live.first().expect("a pod")
                } else {
                    *live
                        .iter()
                        .min_by_key(|&&i| {
                            let pod = &state.pods[i];
                            (pod.view.ready, pod.cost.unwrap_or(0), -pod.view.created_ms)
                        })
                        .expect("a pod")
                };
                state.pods[pick].view.terminating = true;
                let name = state.pods[pick].view.name.clone();
                state.writes.push(format!("replicaset deletes {name}"));
            } else if live.len() < replicas {
                let pod = state.make();
                state.listed.insert(pod.view.uid.clone());
                state.pods.push(pod);
            } else {
                break;
            }
        }
    }

    /// Every terminating pod exits.
    fn exit(&self) {
        let mut state = self.state();
        let gone: Vec<String> = state
            .pods
            .iter()
            .filter(|p| p.view.terminating)
            .map(|p| p.view.uid.clone())
            .collect();
        state.pods.retain(|p| !p.view.terminating);
        for uid in gone {
            state.listed.remove(&uid);
        }
    }

    /// The EndpointSlice controller catches up: pods that are not
    /// routable leave the endpoints.
    fn unlist(&self) {
        let mut state = self.state();
        let unroutable: Vec<String> = state
            .pods
            .iter()
            .filter(|p| !p.routable)
            .map(|p| p.view.uid.clone())
            .collect();
        for uid in unroutable {
            state.listed.remove(&uid);
        }
    }

    fn pod(&self, name: &str) -> FakePod {
        self.state()
            .pods
            .iter()
            .find(|p| p.view.name == name)
            .cloned()
            .expect("a pod")
    }

    fn take_writes(&self) -> Vec<String> {
        std::mem::take(&mut self.state().writes)
    }
}

impl State {
    fn make(&mut self) -> FakePod {
        self.next += 1;
        let n = self.next;
        FakePod {
            view: PodView {
                name: format!("pod-{n}"),
                uid: format!("uid-{n}"),
                resource_version: "1".into(),
                ip: Some(format!("10.0.0.{n}")),
                created_ms: i64::from(n) * 1_000,
                ready: true,
                terminating: false,
                routable: Some(true),
                system_port: Some(9090),
            },
            cost: None,
            routable: true,
            load: 0,
        }
    }
}

impl Fake {
    /// Apply `change` to `pod` if it is still at `version`, logging
    /// `write`: its version after.
    fn write_pod(
        &self,
        pod: &PodView,
        version: &str,
        write: String,
        change: impl FnOnce(&mut FakePod),
    ) -> Result<String, Refused> {
        let mut state = self.state();
        let fake = state
            .pods
            .iter_mut()
            .find(|p| p.view.uid == pod.uid)
            .ok_or(Refused::Stale("no pod".into()))?;
        if fake.view.resource_version != version {
            return Err(Refused::Stale(format!("{} changed", pod.name)));
        }
        let before = (fake.routable, fake.cost);
        change(fake);
        let changed = (fake.routable, fake.cost) != before;
        let next = fake.view.resource_version.parse::<u64>().unwrap_or(0) + u64::from(changed);
        fake.view.resource_version = next.to_string();
        state.writes.push(write.clone());
        state.events.push(write);
        Ok(next.to_string())
    }
}

impl Cluster for Fake {
    async fn target(&self, name: &str) -> Result<Option<Target>, String> {
        assert_eq!(name, "global-front");
        let state = self.state();
        Ok(Some(Target {
            fence: Fence {
                uid: state.deployment.clone(),
                resource_version: state.version.to_string(),
            },
            replicas: state.replicas,
            selector: "gdl.dev/pool=global,gdl.dev/segment=front".into(),
            rolling: state.rolling,
        }))
    }

    async fn pods(&self, _: &str) -> Result<Vec<PodView>, String> {
        let pods = self
            .state()
            .pods
            .iter()
            .map(|p| PodView {
                routable: Some(p.routable),
                ..p.view.clone()
            })
            .collect();
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        Ok(pods)
    }

    async fn endpoints(&self, service: &str) -> Result<BTreeSet<String>, String> {
        assert_eq!(service, SERVICE);
        Ok(self.state().listed.clone())
    }

    async fn scale(&self, _: &str, replicas: i32, fence: &Fence) -> Result<String, Refused> {
        let mut state = self.state();
        if fence.uid != state.deployment || fence.resource_version != state.version.to_string() {
            return Err(Refused::Stale("the Deployment changed".into()));
        }
        state.replicas = replicas;
        state.version += 1;
        state.writes.push(format!("replicas {replicas}"));
        state.events.push(format!("replicas {replicas}"));
        Ok(state.version.to_string())
    }

    async fn routable(
        &self,
        pod: &PodView,
        version: &str,
        routable: bool,
    ) -> Result<String, Refused> {
        self.write_pod(
            pod,
            version,
            format!("{} routable {routable}", pod.name),
            |fake| {
                fake.routable = routable;
            },
        )
    }

    async fn cost(
        &self,
        pod: &PodView,
        version: &str,
        cost: Option<i32>,
    ) -> Result<String, Refused> {
        self.write_pod(
            pod,
            version,
            format!("{} cost {cost:?}", pod.name),
            |fake| {
                fake.cost = cost;
            },
        )
    }

    async fn status(
        &self,
        _: &str,
        status: &GdlScalerStatus,
        version: Option<&str>,
    ) -> Result<Option<String>, Refused> {
        let mut state = self.state();
        if version.is_some_and(|v| v != state.scaler_version.to_string()) {
            return Err(Refused::Stale("the scaler changed".into()));
        }
        // A write that changes nothing leaves the version as it was, as
        // the API server does.
        if state.status.as_ref() != Some(status) {
            state.scaler_version += 1;
        }
        state.status = Some(status.clone());
        let victims: Vec<String> = status
            .retiring
            .iter()
            .map(|v| format!("{:?} {}", v.phase, v.uid).to_lowercase())
            .collect();
        state
            .events
            .push(format!("status [{}]", victims.join(", ")));
        Ok(Some(state.scaler_version.to_string()))
    }
}

impl Probe for Fake {
    async fn instances(&self, _: &str) -> Result<Instances, String> {
        self.instances
            .lock()
            .expect("instances")
            .clone()
            .ok_or_else(|| "the hub does not answer".to_string())
    }

    async fn metrics(&self, ip: &str, port: u16) -> Result<String, String> {
        assert_eq!(port, 9090);
        if self.silent.lock().expect("silent").contains(ip) {
            return Err("no answer".into());
        }
        let state = self.state();
        let pod = state
            .pods
            .iter()
            .find(|p| p.view.ip.as_deref() == Some(ip))
            .ok_or("no pod")?;
        Ok(format!(
            "dynamo_component_admission_admitted {}\ndynamo_component_egress_in_use 1\n",
            pod.load
        ))
    }
}

fn spec(replicas: Option<i32>) -> GdlScalerSpec {
    GdlScalerSpec {
        pool: "global".into(),
        segment: "front".into(),
        target_ref: TargetRef {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            name: "global-front".into(),
        },
        replicas,
        min_replicas: None,
        suspend: None,
        max_concurrent_drains: None,
        victim: None,
        retire: None,
        drain_bound: None,
        routing_grace: None,
        service: None,
        metrics_prefix: None,
    }
}

/// One reconcile of a scaler with `spec` and the status `fake` last
/// stored, at `now_ms`.
async fn once(fake: &Fake, spec: &GdlScalerSpec, now_ms: i64) -> Duration {
    let mut scaler = GdlScaler::new("global-front", spec.clone());
    scaler.metadata.generation = Some(1);
    scaler.status = fake.state().status.clone();
    scaler.metadata.resource_version = Some(fake.state().scaler_version.to_string());
    let victims = Victims::default();
    let cx = Context {
        cluster: fake,
        probe: fake,
        victims: &victims,
    };
    reconcile(&cx, &scaler, now_ms).await.expect("reconciles")
}

fn status(fake: &Fake) -> GdlScalerStatus {
    fake.state().status.clone().expect("a status")
}

fn condition(fake: &Fake, kind: &str) -> (String, String) {
    let status = status(fake);
    let condition = status
        .conditions
        .iter()
        .find(|c| c.type_ == kind)
        .expect("the condition");
    (condition.status.clone(), condition.reason.clone())
}

#[tokio::test]
async fn a_scale_down_removes_the_pod_it_chose() {
    let fake = Fake::with(&[5, 1, 3]);
    let spec = spec(Some(2));
    assert_eq!(once(&fake, &spec, 10_000).await, BUSY);
    assert_eq!(
        fake.take_writes(),
        ["pod-2 cost Some(-2147483648)", "replicas 2"],
        "the cost is set before the count drops"
    );
    let victim = &status(&fake).retiring[0];
    assert_eq!(
        (
            victim.pod.as_str(),
            victim.phase,
            victim.admitted,
            victim.egress
        ),
        ("pod-2", RetiringPhase::Removing, 1, 1)
    );
    fake.settle(false);
    assert!(fake.pod("pod-2").view.terminating);
    once(&fake, &spec, 11_000).await;
    assert_eq!(
        status(&fake).retiring.len(),
        1,
        "kept until the pod is gone"
    );
    fake.exit();
    assert_eq!(once(&fake, &spec, 14_000).await, IDLE);
    let status = status(&fake);
    assert!(status.retiring.is_empty());
    let last = status.last.expect("the removal ended");
    assert_eq!(last.chosen, "uid-2");
    assert_eq!(last.deleted, "uid-2");
    assert_eq!(last.outcome, Outcome::Removed);
    assert_eq!(last.duration_ms, 4_000);
    assert_eq!((status.replicas, status.ready_replicas), (2, 2));
    assert_eq!(
        condition(&fake, "Scaling"),
        ("False".into(), "Steady".into())
    );
    assert!(
        fake.take_writes()
            .iter()
            .all(|w| w.starts_with("replicaset"))
    );
}

#[tokio::test]
async fn a_tie_goes_to_the_newest_pod() {
    let fake = Fake::with(&[2, 2, 2]);
    once(&fake, &spec(Some(2)), 0).await;
    assert_eq!(status(&fake).retiring[0].pod, "pod-3");
}

#[tokio::test]
async fn the_hubs_report_stands_in_for_metrics_that_do_not_answer() {
    let fake = Fake::with(&[0, 0, 0]);
    fake.silent.lock().expect("silent").extend([
        "10.0.0.1".to_string(),
        "10.0.0.2".to_string(),
        "10.0.0.3".to_string(),
    ]);
    *fake.instances.lock().expect("instances") = Some(
        serde_json::from_value(serde_json::json!({"instances": [
            {"segment": "front", "addresses": ["10.0.0.1:7000"], "phase": "ready", "load": {"admitted": 0, "limit": 4}},
            {"segment": "front", "addresses": ["10.0.0.2:7000"], "phase": "ready", "load": {"admitted": 3, "limit": 4}},
        ]}))
        .expect("a list"),
    );
    once(&fake, &spec(Some(2)), 0).await;
    let victim = &status(&fake).retiring[0];
    assert_eq!(victim.pod, "pod-1", "an unknown load ranks last");
    assert_eq!(victim.instance.as_deref(), Some("10.0.0.1:7000"));
}

#[tokio::test]
async fn a_scale_down_waits_for_a_rollout_a_terminating_pod_and_a_pod_not_ready() {
    let fake = Fake::with(&[0, 0, 0]);
    let spec = spec(Some(2));
    fake.state().rolling = true;
    assert_eq!(once(&fake, &spec, 0).await, BLOCKED);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "RolloutInProgress".into())
    );
    fake.state().rolling = false;
    // A pod terminating that is no victim: the excess counts the pods that
    // stay, so a fourth is needed for there to be one to remove.
    {
        let mut state = fake.state();
        state.replicas = 4;
        let pod = state.make();
        state.pods.push(pod);
        state.pods[0].view.terminating = true;
    }
    once(&fake, &spec, 0).await;
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "PodTerminating".into())
    );
    fake.state().replicas = 3;
    fake.exit();
    fake.settle(false);
    fake.state().pods[0].view.ready = false;
    once(&fake, &spec, 0).await;
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "PodNotReady".into())
    );
    assert!(
        fake.take_writes()
            .iter()
            .all(|w| w.starts_with("replicaset"))
    );
    assert!(status(&fake).retiring.is_empty());
    fake.state().pods[0].view.ready = true;
    once(&fake, &spec, 0).await;
    assert_eq!(
        condition(&fake, "Blocked"),
        ("False".into(), "NotBlocked".into())
    );
    assert_eq!(status(&fake).retiring.len(), 1);
}

#[tokio::test]
async fn it_never_scales_below_the_minimum() {
    let fake = Fake::with(&[0, 0, 0]);
    let mut spec = spec(Some(1));
    spec.min_replicas = Some(2);
    once(&fake, &spec, 0).await;
    assert_eq!(
        fake.take_writes(),
        ["pod-3 cost Some(-2147483648)", "replicas 2"]
    );
    fake.settle(false);
    fake.exit();
    fake.take_writes();
    once(&fake, &spec, 1_000).await;
    assert!(fake.take_writes().is_empty(), "2 is the minimum");
}

#[tokio::test]
async fn drains_are_paced() {
    let fake = Fake::with(&[0, 0, 0, 0]);
    let spec = spec(Some(2));
    once(&fake, &spec, 0).await;
    assert_eq!(fake.state().replicas, 3, "one victim at a time");
    fake.settle(false);
    once(&fake, &spec, 1_000).await;
    assert_eq!(
        fake.state().replicas,
        3,
        "the next waits for the first to go"
    );
    fake.exit();
    once(&fake, &spec, 2_000).await;
    assert_eq!(fake.state().replicas, 2);
    let mut spec = spec.clone();
    spec.replicas = Some(0);
    spec.min_replicas = Some(1);
    spec.max_concurrent_drains = Some(2);
    fake.settle(false);
    fake.exit();
    fake.state().replicas = 4;
    fake.settle(false);
    once(&fake, &spec, 3_000).await;
    assert_eq!(fake.state().replicas, 2, "two at once");
}

#[tokio::test]
async fn a_wrong_pick_gives_the_victim_back() {
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    let fake = Fake::with(&[9, 0, 9]);
    once(&fake, &spec, 0).await;
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    once(&fake, &spec, 2_000).await;
    assert_eq!(status(&fake).retiring[0].phase, RetiringPhase::Removing);
    fake.take_writes();
    // The ReplicaSet deletes another pod, as it would for a pod that
    // failed at that moment.
    fake.settle(true);
    assert_eq!(fake.take_writes(), ["replicaset deletes pod-1"]);
    once(&fake, &spec, 3_000).await;
    assert_eq!(
        fake.take_writes(),
        ["pod-2 routable true", "pod-2 cost None"],
        "the victim never retired: it serves again"
    );
    let status = status(&fake);
    assert!(status.retiring.is_empty());
    let last = status.last.expect("ended");
    assert_eq!(last.outcome, Outcome::KubernetesChoseAnother);
    assert_eq!(
        (last.chosen.as_str(), last.deleted.as_str()),
        ("uid-2", "uid-1")
    );
}

#[tokio::test]
async fn a_frontend_leaves_its_service_before_its_pod_goes() {
    let fake = Fake::with(&[3, 0, 3]);
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("10s".into());
    once(&fake, &spec, 0).await;
    assert_eq!(fake.take_writes(), ["pod-2 routable false"]);
    assert_eq!(status(&fake).retiring[0].phase, RetiringPhase::Unrouting);
    // Still listed: nothing more until the EndpointSlice drops it.
    once(&fake, &spec, 30_000).await;
    assert!(fake.take_writes().is_empty());
    assert!(status(&fake).retiring[0].unrouted.is_none());
    fake.unlist();
    once(&fake, &spec, 31_000).await;
    assert!(fake.take_writes().is_empty());
    assert!(status(&fake).retiring[0].unrouted.is_some());
    once(&fake, &spec, 40_999).await;
    assert!(
        fake.take_writes().is_empty(),
        "the routing grace has not passed"
    );
    once(&fake, &spec, 41_000).await;
    assert_eq!(
        fake.take_writes(),
        ["pod-2 cost Some(-2147483648)", "replicas 2"]
    );
    fake.settle(false);
    assert!(fake.pod("pod-2").view.terminating);
    fake.exit();
    once(&fake, &spec, 45_000).await;
    let last = status(&fake).last.expect("ended");
    assert_eq!(
        (last.outcome, last.chosen.as_str()),
        (Outcome::Removed, "uid-2")
    );
}

#[tokio::test]
async fn a_scale_up_takes_a_victim_back_before_it_retires() {
    let fake = Fake::with(&[0, 0, 0]);
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    once(&fake, &spec, 0).await;
    assert_eq!(fake.take_writes(), ["pod-3 routable false"]);
    spec.replicas = Some(3);
    once(&fake, &spec, 1_000).await;
    assert_eq!(fake.take_writes(), ["pod-3 routable true"]);
    assert!(status(&fake).retiring.is_empty());
    assert_eq!(fake.state().replicas, 3);
    spec.replicas = Some(4);
    once(&fake, &spec, 2_000).await;
    assert_eq!(fake.take_writes(), ["replicas 4"]);
}

#[tokio::test]
async fn a_removal_past_its_bound_overran() {
    let fake = Fake::with(&[0, 0]);
    let mut spec = spec(Some(1));
    spec.drain_bound = Some("20s".into());
    once(&fake, &spec, 0).await;
    fake.settle(false);
    fake.exit();
    once(&fake, &spec, 30_000).await;
    assert_eq!(status(&fake).last.expect("ended").outcome, Outcome::Removed);
    let fake = Fake::with(&[0, 0]);
    once(&fake, &spec, 0).await;
    fake.settle(false);
    fake.exit();
    once(&fake, &spec, 30_001).await;
    assert_eq!(status(&fake).last.expect("ended").outcome, Outcome::Overran);
}

#[tokio::test]
async fn without_replicas_it_writes_nothing_and_suspended_it_holds_zero() {
    let fake = Fake::with(&[0, 0]);
    let mut spec = spec(None);
    assert_eq!(once(&fake, &spec, 0).await, IDLE);
    assert!(fake.take_writes().is_empty());
    assert_eq!(
        condition(&fake, "Scaling"),
        ("False".into(), "Waiting".into())
    );
    assert_eq!(
        status(&fake).selector,
        "gdl.dev/pool=global,gdl.dev/segment=front"
    );
    spec.replicas = Some(2);
    spec.suspend = Some(Suspend::Zero);
    once(&fake, &spec, 0).await;
    assert_eq!(fake.take_writes(), ["replicas 0"]);
    assert_eq!(
        condition(&fake, "Suspended"),
        ("True".into(), "SuspendedAtZero".into())
    );
}

#[tokio::test]
async fn an_unknown_policy_blocks() {
    let fake = Fake::with(&[0, 0]);
    let mut spec = spec(Some(1));
    spec.victim = Some("byFingerprint".into());
    assert_eq!(once(&fake, &spec, 0).await, BLOCKED);
    assert_eq!(
        condition(&fake, "Blocked"),
        ("True".into(), "UnknownVictimPolicy".into())
    );
    assert!(fake.take_writes().is_empty());
}

#[tokio::test]
async fn an_unchanged_status_is_not_written_again() {
    let fake = Fake::with(&[0, 0]);
    let spec = spec(Some(2));
    once(&fake, &spec, 0).await;
    let first = status(&fake);
    fake.state().status = Some(first.clone());
    once(&fake, &spec, 5_000).await;
    assert_eq!(status(&fake), first, "the transition times stay");
    let counts: BTreeMap<_, _> = first
        .conditions
        .iter()
        .map(|c| (c.type_.clone(), c.status.clone()))
        .collect();
    assert_eq!(counts["Scaling"], "False");
}

/// A frontend's victim whose scale-down was written but whose status was
/// not (a status write that failed) is found terminating while its status
/// still says `unrouting`: the scaler takes it as being deleted, and does
/// not lower the count again.
#[tokio::test]
async fn a_victim_found_terminating_is_not_scaled_down_twice() {
    let fake = Fake::with(&[3, 0, 3]);
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    once(&fake, &spec, 0).await;
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    let unrouted = status(&fake);
    assert_eq!(unrouted.retiring[0].phase, RetiringPhase::Unrouting);
    once(&fake, &spec, 2_000).await;
    assert_eq!(fake.state().replicas, 2);
    // The status write of that reconcile is lost.
    fake.state().status = Some(unrouted);
    fake.settle(false);
    fake.take_writes();
    once(&fake, &spec, 3_000).await;
    assert_eq!(fake.state().replicas, 2, "{:?}", fake.take_writes());
    assert_eq!(status(&fake).retiring[0].phase, RetiringPhase::Removing);
    fake.exit();
    once(&fake, &spec, 4_000).await;
    let last = status(&fake).last.expect("ended");
    assert_eq!(
        (last.outcome, last.chosen.as_str()),
        (Outcome::Removed, "uid-2")
    );
}

/// A removal ends when the victim's pod is gone and its hub no longer
/// lists its instance: an instance that crashed stays listed until its
/// lease lapses. A hub that still lists it a minute after its pod went
/// ends the wait, and the reason says so.
#[tokio::test]
async fn a_removal_ends_once_the_hub_no_longer_lists_the_instance() {
    let listing = |addresses: &[&str]| -> Instances {
        serde_json::from_value(serde_json::json!({"instances": addresses
            .iter()
            .map(|a| serde_json::json!({"segment": "front", "addresses": [a], "phase": "ready"}))
            .collect::<Vec<_>>()}))
        .expect("a list")
    };
    let all = ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000"];
    for (still, at, ended) in [
        (false, 5_000, true),
        (true, 5_000, false),
        (true, 66_000, true),
    ] {
        let fake = Fake::with(&[3, 0, 3]);
        *fake.instances.lock().expect("instances") = Some(listing(&all));
        let spec = spec(Some(2));
        once(&fake, &spec, 0).await;
        assert_eq!(
            status(&fake).retiring[0].instance.as_deref(),
            Some("10.0.0.2:7000")
        );
        fake.settle(false);
        fake.exit();
        let left: Vec<&str> = all
            .iter()
            .copied()
            .filter(|a| still || *a != "10.0.0.2:7000")
            .collect();
        *fake.instances.lock().expect("instances") = Some(listing(&left));
        once(&fake, &spec, 4_000).await;
        if still {
            assert_eq!(
                status(&fake).retiring.len(),
                1,
                "kept while the hub lists it"
            );
            assert!(status(&fake).retiring[0].gone.is_some());
            once(&fake, &spec, at).await;
        }
        let status = status(&fake);
        assert_eq!(status.retiring.is_empty(), ended, "{still} {at}");
        if ended {
            let last = status.last.expect("ended");
            assert_eq!(last.chosen, "uid-2");
            assert_eq!(
                last.reason.contains("still listed"),
                still,
                "{}",
                last.reason
            );
        }
    }
}

fn events(fake: &Fake) -> Vec<String> {
    std::mem::take(&mut fake.state().events)
}

/// Each step is stored before it is taken: the victim before its label,
/// and its commit before its cost and the count.
#[tokio::test]
async fn each_step_is_stored_before_it_is_taken() {
    let fake = Fake::with(&[9, 0, 9]);
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    once(&fake, &spec, 0).await;
    // The reconcile ends with a status write of its counts and conditions.
    assert_eq!(
        events(&fake)[..2],
        ["status [unrouting uid-2]", "pod-2 routable false"]
    );
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    events(&fake);
    once(&fake, &spec, 2_000).await;
    assert_eq!(
        events(&fake),
        [
            "status [committing uid-2]",
            "pod-2 cost Some(-2147483648)",
            "replicas 2",
            "status [removing uid-2]"
        ]
    );
    // Without a Service, the commit is stored before anything is written.
    let fake = Fake::with(&[9, 0, 9]);
    once(&fake, &spec_without_service(), 0).await;
    assert_eq!(
        events(&fake),
        [
            "status [committing uid-2]",
            "pod-2 cost Some(-2147483648)",
            "replicas 2",
            "status [removing uid-2]"
        ]
    );
}

fn spec_without_service() -> GdlScalerSpec {
    spec(Some(2))
}

/// A pod that goes unready after the commit is the one the ReplicaSet
/// deletes, ranking readiness before cost. The ReplicaSet then has
/// nothing left to delete, so the victim, which never retired, gets its
/// cost and its Service back.
#[tokio::test]
async fn a_victim_passed_over_for_a_pod_gone_unready_serves_again() {
    let fake = Fake::with(&[9, 0, 9]);
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    once(&fake, &spec, 0).await;
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    once(&fake, &spec, 2_000).await;
    assert_eq!(status(&fake).retiring[0].phase, RetiringPhase::Removing);
    fake.state().pods[0].view.ready = false;
    fake.settle(false);
    assert!(fake.pod("pod-1").view.terminating);
    assert!(!fake.pod("pod-2").view.terminating);
    fake.take_writes();
    once(&fake, &spec, 3_000).await;
    assert_eq!(
        fake.take_writes(),
        ["pod-2 routable true", "pod-2 cost None"]
    );
    let last = status(&fake).last.expect("ended");
    assert_eq!(last.outcome, Outcome::KubernetesChoseAnother);
    assert_eq!(
        (last.chosen.as_str(), last.deleted.as_str()),
        ("uid-2", "uid-1")
    );
}

/// The control: the count lowered, the victim alive, and one more pod than
/// the count. The ReplicaSet has not acted yet, and the victim stays out
/// of its Service.
#[tokio::test]
async fn a_committed_victim_stays_out_while_the_replicaset_lags() {
    let fake = Fake::with(&[9, 0, 9]);
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    spec.routing_grace = Some("0s".into());
    once(&fake, &spec, 0).await;
    fake.unlist();
    once(&fake, &spec, 1_000).await;
    once(&fake, &spec, 2_000).await;
    fake.take_writes();
    for at in [3_000, 4_000, 5_000] {
        once(&fake, &spec, at).await;
    }
    assert!(fake.take_writes().is_empty());
    assert!(!fake.pod("pod-2").routable);
    assert_eq!(status(&fake).retiring[0].phase, RetiringPhase::Removing);
}

/// A Deployment made again under the same name: the victim chosen from
/// the old one is abandoned, and its pod gets back its Service and cost.
#[tokio::test]
async fn a_victim_of_a_deployment_made_again_is_abandoned() {
    let fake = Fake::with(&[9, 0, 9]);
    let mut spec = spec(Some(2));
    spec.service = Some(SERVICE.into());
    once(&fake, &spec, 0).await;
    assert_eq!(status(&fake).retiring[0].deployment, "deployment-a");
    {
        let mut state = fake.state();
        state.deployment = "deployment-b".into();
        state.pods[1].cost = Some(VICTIM_COST);
    }
    fake.take_writes();
    spec.replicas = Some(3);
    once(&fake, &spec, 1_000).await;
    assert_eq!(
        fake.take_writes(),
        ["pod-2 routable true", "pod-2 cost None"]
    );
    let status = status(&fake);
    assert!(status.retiring.is_empty());
    assert_eq!(status.last.expect("ended").outcome, Outcome::Abandoned);
}
