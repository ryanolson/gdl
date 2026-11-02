//! One reconcile of a `GdlScaler`. The scaler keeps the target at
//! `max(spec.replicas, minReplicas)`. On a scale-down it chooses each
//! victim by its policy, takes it out of its entry Service and waits until
//! it has left the endpoints and the routing grace has passed, then sets
//! its deletion cost to the lowest and writes the target's `replicas` one
//! lower, so that the ReplicaSet deletes that pod. In `retire: signal`
//! mode the instance retires and drains on its SIGTERM. In `retire:
//! request` mode the scaler first asks the instance's hub to retire it,
//! and waits until it has drained and its leave hooks have reported, or
//! until `drainBound`, before it writes the cost and the count; the
//! SIGTERM then ends the instance's hold ([`request`]).
//!
//! Write ahead, never infer. Before each step that changes a pod or the
//! target, the scaler stores in its status the victim, its pod's uid, the
//! Deployment's uid, the target count, and the phase that names the step;
//! each step can be taken again; and a reconcile that finds a step half
//! done finishes or undoes it from what the status says. Every write
//! carries the read it was decided on: the status the scaler's
//! resourceVersion, the target its uid and resourceVersion, a pod its uid
//! and resourceVersion. Each reconcile stores its status, conditionally,
//! before its first pod write, so one whose read of the scaler predates
//! another's stored step writes no pod. A write refused as stale ends the
//! reconcile, and the next one reads again and decides again. That keeps
//! a stale decision from overwriting a newer one in most cases, but not
//! every case of two operators at once, which only a forced deletion of
//! the operator's pod makes possible (`docs/src/guides/operator.md`).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;

use crate::cluster::{Cluster, Fence, PodView, Refused};
use crate::probe::{Held, Instances, Probe};
use crate::victim::{Candidate, Victims};
use crate::{
    GdlScaler, GdlScalerSpec, GdlScalerStatus, Last, Outcome, Retire, Retiring, RetiringPhase,
};

mod request;

/// The deletion cost of a victim: the lowest an int32 annotation holds.
pub const VICTIM_COST: i32 = i32::MIN;

/// How long past its drain bound a victim's pod may still exit and count
/// as removed in time: the runtime's exit passes the bound by up to about
/// 8.5 s (`docs/src/concepts/lifecycle.md`, "The bound").
pub const EXIT_MARGIN: Duration = Duration::from_secs(10);

/// How long after a victim's pod is gone its hub may still list its
/// instance before the removal ends anyway: a crashed instance's lease
/// lapses in about 30 s.
pub const LEAVE_BOUND: Duration = Duration::from_secs(60);

/// When to look again while a victim is being removed.
const BUSY: Duration = Duration::from_secs(1);

/// When to look again while a scale-down is blocked.
const BLOCKED: Duration = Duration::from_secs(5);

/// When to look again with nothing in progress: a change to the scaler or
/// its target wakes it sooner.
const IDLE: Duration = Duration::from_secs(60);

/// When to look again after a write was refused as stale: soon, with a
/// fresh read.
const STALE: Duration = Duration::from_millis(250);

/// What a reconcile reads and writes through.
pub struct Context<'a, C, P> {
    /// The Kubernetes API.
    pub cluster: &'a C,
    /// The pools' hubs and pods.
    pub probe: &'a P,
    /// The registered victim policies.
    pub victims: &'a Victims,
    /// The time now, in milliseconds since the Unix epoch, for a moment
    /// the reconcile records after it has waited on a read or a write.
    pub clock: &'a (dyn Fn() -> i64 + Sync),
}

/// Reconcile `scaler` at `now_ms`, milliseconds since the Unix epoch:
/// when to look again. An error is a read or a write that failed; the
/// status says why a scale-down waits.
pub async fn reconcile<C: Cluster, P: Probe>(
    cx: &Context<'_, C, P>,
    scaler: &GdlScaler,
    now_ms: i64,
) -> Result<Duration, String> {
    let name = scaler
        .metadata
        .name
        .clone()
        .ok_or("a GdlScaler without a name")?;
    let old = scaler.status.clone().unwrap_or_default();
    let mut run = Run {
        cx,
        scaler,
        spec: &scaler.spec,
        request: scaler.spec.retire == Some(Retire::Request),
        name,
        now_ms,
        status: GdlScalerStatus {
            observed_generation: scaler.metadata.generation,
            ..old.clone()
        },
        conditions: old.conditions.clone(),
        generation: scaler.metadata.generation,
        version: scaler.metadata.resource_version.clone(),
        written: old.clone(),
        restored: BTreeSet::new(),
        confirmed: false,
        versions: BTreeMap::new(),
    };
    let requeue = match run.step(old.retiring.clone()).await {
        Ok(requeue) => requeue,
        Err(Stop::Blocked(reason, message)) => {
            run.condition("Blocked", true, reason, &message);
            BLOCKED
        }
        Err(Stop::Stale(why)) => {
            tracing::debug!(scaler = %run.name, %why, "a write was stale; reading again");
            return Ok(STALE);
        }
        Err(Stop::Failed(error)) => return Err(error),
    };
    match run.write(false).await {
        Ok(()) => Ok(requeue),
        Err(Stop::Stale(why)) => {
            tracing::debug!(scaler = %run.name, %why, "the status was stale; reading again");
            Ok(STALE)
        }
        Err(Stop::Failed(error)) => Err(error),
        Err(Stop::Blocked(..)) => unreachable!("a status write does not block"),
    }
}

/// Why a reconcile stopped early.
enum Stop {
    /// It cannot act until something changes: the reason and the message
    /// of the `Blocked` condition.
    Blocked(&'static str, String),
    /// A write was refused: what it was decided on changed.
    Stale(String),
    /// A read or a write failed.
    Failed(String),
}

impl From<String> for Stop {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

impl From<Refused> for Stop {
    fn from(refused: Refused) -> Self {
        match refused {
            Refused::Stale(why) => Self::Stale(why),
            Refused::Failed(error) => Self::Failed(error),
        }
    }
}

struct Run<'a, 'c, C, P> {
    cx: &'a Context<'c, C, P>,
    scaler: &'a GdlScaler,
    spec: &'a GdlScalerSpec,
    /// Whether it retires its victims through their hub
    /// (`retire: request`).
    request: bool,
    name: String,
    now_ms: i64,
    status: GdlScalerStatus,
    conditions: Vec<Condition>,
    generation: Option<i64>,
    /// The scaler's resourceVersion: each status write is conditional on
    /// it, and moves it.
    version: Option<String>,
    /// The status as last stored.
    written: GdlScalerStatus,
    /// The pods this reconcile put back in their Service, by uid.
    restored: BTreeSet<String>,
    /// Whether a status write in this reconcile has landed: its read of
    /// the scaler was current then. Every pod write needs one before it.
    confirmed: bool,
    /// Each pod's resourceVersion after this reconcile wrote it, by uid.
    versions: BTreeMap<String, String>,
}

impl<C: Cluster, P: Probe> Run<'_, '_, C, P> {
    async fn step(&mut self, retiring: Vec<Retiring>) -> Result<Duration, Stop> {
        let spec = self.spec;
        let target_ref = &spec.target_ref;
        if target_ref.api_version != "apps/v1" || target_ref.kind != "Deployment" {
            return Err(Stop::Blocked(
                "UnsupportedTarget",
                format!(
                    "the target is a {} of {}; the scaler scales an apps/v1 Deployment",
                    target_ref.kind, target_ref.api_version
                ),
            ));
        }
        let grace = spec
            .routing_grace()
            .map_err(|e| Stop::Blocked("BadDuration", format!("routingGrace: {e}")))?;
        let bound = spec
            .drain_bound()
            .map_err(|e| Stop::Blocked("BadDuration", format!("drainBound: {e}")))?;
        let unbounded = || {
            Stop::Blocked(
                "DrainBoundRequired",
                "`retire: request` waits for a victim at most `drainBound`, which is not set"
                    .to_string(),
            )
        };
        if self.request && bound.is_none() {
            return Err(unbounded());
        }
        let target = self
            .cx
            .cluster
            .target(&target_ref.name)
            .await?
            .ok_or_else(|| {
                Stop::Blocked(
                    "TargetMissing",
                    format!("Deployment `{}` does not exist", target_ref.name),
                )
            })?;
        self.status.selector = target.selector.clone();
        let pods = self.cx.cluster.pods(&target.selector).await?;
        let mut fence = target.fence.clone();
        let mut replicas = target.replicas;

        // A victim of a Deployment since made again under the same name is
        // abandoned: what the scaler changed on its pod is put back.
        // What is put back is stored first, as every step is. One asked to
        // retire cannot serve again: its pod is deleted instead, whatever
        // count the earlier Deployment had.
        let mut undo = Vec::new();
        let mut current = Vec::new();
        for mut victim in retiring {
            if victim.deployment == fence.uid {
                current.push(victim);
                continue;
            }
            if victim.asked.is_some() {
                if victim.phase != RetiringPhase::Deleting {
                    let reason = "its Deployment was made again under the same name; it had retired, so its pod is deleted".to_string();
                    let chosen = victim.uid.clone();
                    self.finish(victim.clone(), Outcome::Abandoned, chosen, reason);
                }
                victim.phase = RetiringPhase::Deleting;
                victim.deployment = fence.uid.clone();
                current.push(victim);
                continue;
            }
            undo.push(victim.uid.clone());
            let reason = "its Deployment was made again under the same name".to_string();
            self.finish(victim, Outcome::Abandoned, String::new(), reason);
        }
        let mut delete = Vec::new();
        let mut kept = self
            .follow(current, &pods, replicas, bound, &mut undo, &mut delete)
            .await?;
        if !undo.is_empty() || !delete.is_empty() {
            self.ahead(&kept).await?;
            for uid in undo {
                if let Some(pod) = alive(&pods, &uid) {
                    self.put_back(pod, true).await?;
                }
            }
            for uid in delete {
                if let Some(pod) = alive(&pods, &uid) {
                    self.delete(pod).await?;
                }
            }
        }
        // A victim asked to retire is waited for at most the bound, also
        // once the spec has left request mode; one whose pod has gone
        // ended above, and one whose container restarted goes on its
        // SIGTERM at once, so neither needs one.
        for victim in kept.iter_mut().filter(|v| uncommitted(v)) {
            if let Some(pod) = alive(&pods, &victim.uid) {
                self.restarted(victim, pod).await;
            }
        }
        let held = |v: &Retiring| v.asked.is_some() && uncommitted(v) && !v.restarted;
        if bound.is_none() && kept.iter().any(held) {
            self.status.retiring = kept;
            return Err(unbounded());
        }
        self.sweep(&pods, &kept).await?;

        let suspended = spec.suspend.is_some();
        self.condition(
            "Suspended",
            suspended,
            if suspended {
                "SuspendedAtZero"
            } else {
                "NotSuspended"
            },
            if suspended {
                "the target is held at 0"
            } else {
                ""
            },
        );
        let Some(asked) = spec.replicas.filter(|_| !suspended) else {
            if suspended {
                // A victim that may have retired is left: the count of 0
                // deletes its pod with the rest.
                while let Some(at) = kept.iter().position(cancellable) {
                    self.cancel(&mut kept, at, &pods).await?;
                }
                if replicas != 0 {
                    self.cx.cluster.scale(&target_ref.name, 0, &fence).await?;
                }
            }
            self.count(&pods, kept);
            self.condition("Blocked", false, "NotBlocked", "");
            let waiting = if suspended {
                "the target is held at 0"
            } else {
                "spec.replicas is not set"
            };
            self.condition("Scaling", false, "Waiting", waiting);
            return Ok(if suspended && !pods.is_empty() {
                BUSY
            } else {
                IDLE
            });
        };
        let desired = asked.max(spec.min_replicas());

        // A scale-up first takes back the victims not yet committed, the
        // latest chosen first. One that has not retired is given back; one
        // that may have, in request mode, cannot serve again, so its pod is
        // deleted for the ReplicaSet to replace. It writes the count up
        // only once every committed victim's pod is being deleted: a count
        // raised before that would leave the ReplicaSet nothing to delete,
        // and the victim out of its Service for good.
        let pending = |kept: &[Retiring]| {
            i32::try_from(kept.iter().filter(|v| uncommitted(v)).count()).unwrap_or(i32::MAX)
        };
        while desired > replicas - pending(&kept) {
            let Some(at) = kept
                .iter()
                .enumerate()
                .filter(|(_, v)| uncommitted(v))
                .max_by(|(_, a), (_, b)| a.since.cmp(&b.since))
                .map(|(at, _)| at)
            else {
                break;
            };
            if cancellable(&kept[at]) {
                self.cancel(&mut kept, at, &pods).await?;
            } else {
                self.replace(&mut kept, at, &pods).await?;
            }
        }
        let mut waiting = false;
        if desired > replicas {
            if kept.iter().any(|v| alive(&pods, &v.uid).is_some()) {
                waiting = true;
            } else {
                fence.resource_version = self
                    .cx
                    .cluster
                    .scale(&target_ref.name, desired, &fence)
                    .await?;
                replicas = desired;
            }
        }

        // The one victim not yet committed goes on: out of its Service, the
        // routing grace, then its commit; in request mode, its retire and
        // its drain between the two. A commit stored but not written is
        // written again.
        let mut held = None;
        if let Some(at) = kept.iter().position(uncommitted) {
            let pod = alive(&pods, &kept[at].uid)
                .expect("follow keeps a victim not yet committed only while its pod is alive");
            let mut commit = kept[at].phase == RetiringPhase::Committing;
            if kept[at].phase == RetiringPhase::Unrouting {
                if pod.routable != Some(false) {
                    self.ahead(&kept).await?;
                    self.label(pod, false).await?;
                }
                let service = spec.service.as_deref().unwrap_or_default();
                let listed = self.cx.cluster.endpoints(service).await?;
                if !listed.contains(&pod.uid) {
                    match kept[at].unrouted.as_deref().and_then(millis) {
                        None => kept[at].unrouted = Some(rfc3339(self.now_ms)),
                        Some(unrouted) => commit = self.now_ms - unrouted >= millis_of(grace),
                    }
                }
            }
            let bound = bound.unwrap_or_default();
            if self.restarted(&mut kept[at], pod).await {
                commit = true;
            } else if commit && self.request && kept[at].phase == RetiringPhase::Unrouting {
                commit = false;
                held = self.ask(&mut kept, at, pod, bound).await?;
            } else if kept[at].phase == RetiringPhase::Retiring {
                held = self.ask(&mut kept, at, pod, bound).await?;
            } else if kept[at].phase == RetiringPhase::Draining {
                commit = self.hold(&mut kept[at], pod, bound).await;
            }
            if commit {
                replicas = self
                    .commit(&mut kept, at, pod, replicas, &mut fence)
                    .await?;
            }
        }

        // New victims, while none is uncommitted: the live pods that are
        // not leaving, against the desired count.
        let mut blocked = None;
        if !waiting && !kept.iter().any(uncommitted) {
            let leaving = |pod: &PodView| kept.iter().any(|v| v.uid == pod.uid);
            let staying: Vec<&PodView> = pods
                .iter()
                .filter(|p| !p.terminating && !leaving(p))
                .collect();
            let excess =
                usize::try_from(i32::try_from(staying.len()).unwrap_or(i32::MAX) - desired)
                    .unwrap_or(0);
            // In request mode a victim drains before its commit, and still
            // holds work after it until its pod is gone: one at a time.
            let drains = match self.request {
                true => 1,
                false => usize::try_from(spec.max_concurrent_drains()).unwrap_or(1),
            };
            let room = drains.saturating_sub(kept.len());
            if excess == 0 || room == 0 {
                // Nothing to remove, or the drains in progress pace it.
            } else if target.rolling {
                blocked = Some((
                    "RolloutInProgress",
                    "a rollout of the target is in progress".to_string(),
                ));
            } else if let Some(pod) = pods.iter().find(|p| p.terminating && !leaving(p)) {
                blocked = Some((
                    "PodTerminating",
                    format!("pod `{}` is terminating", pod.name),
                ));
            } else if let Some(pod) = staying.iter().find(|p| !p.ready) {
                blocked = Some(("PodNotReady", format!("pod `{}` is not ready", pod.name)));
            } else {
                // A victim leaves its Service before it commits, and only
                // one is ever uncommitted: one at a time with a Service.
                // In request mode a victim drains before it commits, so
                // one drains at a time.
                let count = match spec.service {
                    Some(_) => 1,
                    None if self.request => 1,
                    None => room.min(excess),
                };
                blocked = self
                    .choose(&mut kept, &staying, count, replicas, &mut fence)
                    .await?;
            }
        }
        blocked = blocked.or(held);

        let busy = !kept.is_empty();
        self.count(&pods, kept);
        match &blocked {
            Some((reason, message)) => self.condition("Blocked", true, reason, message),
            None => self.condition("Blocked", false, "NotBlocked", ""),
        }
        let live = self.status.replicas;
        let scaling = busy || live != desired || waiting;
        let message = match waiting {
            true => format!("{live} of {desired}: waiting for a removed pod to be deleted"),
            false => format!("{live} of {desired}"),
        };
        self.condition(
            "Scaling",
            scaling,
            if !scaling {
                "Steady"
            } else if live > desired {
                "ScalingDown"
            } else {
                "ScalingUp"
            },
            &message,
        );
        Ok(if blocked.is_some() {
            BLOCKED
        } else if scaling {
            BUSY
        } else {
            IDLE
        })
    }

    /// Choose up to `count` victims among `staying`, with the target at
    /// `replicas`: each is stored before anything is done to it, then
    /// taken out of its Service, or, without one, committed at once, or
    /// in request mode asked to retire. Why none could be chosen, when
    /// that is so.
    async fn choose(
        &mut self,
        kept: &mut Vec<Retiring>,
        staying: &[&PodView],
        count: usize,
        mut replicas: i32,
        fence: &mut Fence,
    ) -> Result<Option<(&'static str, String)>, Stop> {
        let spec = self.spec;
        let held = self.held(staying).await;
        let instances = self.cx.probe.instances(&spec.pool).await.ok();
        // In request mode without a Service, a victim is asked to retire
        // at once, so only a pod whose instance its hub lists can be one.
        let asked_at_once = self.request && spec.service.is_none();
        if asked_at_once && instances.is_none() {
            return Ok(Some((
                "HubUnavailable",
                format!(
                    "the hub of pool `{}` does not list its instances",
                    spec.pool
                ),
            )));
        }
        let registration = |pod: &PodView| {
            instance_of(instances.as_ref(), pod).and_then(crate::probe::Instance::registration)
        };
        let ranked: Vec<Candidate> = staying
            .iter()
            .filter(|pod| !asked_at_once || registration(pod).is_some())
            .map(|pod| Candidate {
                pod: pod.name.clone(),
                uid: pod.uid.clone(),
                created_ms: pod.created_ms,
                load: crate::probe::load_of(
                    held.get(&pod.uid),
                    instance_of(instances.as_ref(), pod),
                ),
                committed: crate::probe::committed_of(instance_of(instances.as_ref(), pod)),
            })
            .collect();
        if ranked.is_empty() {
            return Ok(Some((
                "InstanceNotListed",
                format!(
                    "the hub of pool `{}` lists no instance of a pod that can go",
                    spec.pool
                ),
            )));
        }
        let Some(chosen) = self.cx.victims.choose(spec.victim(), &ranked, count) else {
            return Err(Stop::Blocked(
                "UnknownVictimPolicy",
                format!("no victim policy is registered as `{}`", spec.victim()),
            ));
        };
        for candidate in chosen {
            let pod = staying
                .iter()
                .find(|p| p.uid == candidate.uid)
                .expect("a candidate is a pod");
            // The last ready pod of a segment never leaves its Service.
            let others = staying
                .iter()
                .filter(|p| p.uid != pod.uid && p.ready && !kept.iter().any(|v| v.uid == p.uid))
                .count();
            if others == 0 {
                break;
            }
            let held = held.get(&pod.uid).copied().unwrap_or_default();
            let (instance_id, incarnation) = match registration(pod) {
                Some((id, incarnation)) if asked_at_once => (Some(id), Some(incarnation)),
                _ => (None, None),
            };
            kept.push(Retiring {
                pod: pod.name.clone(),
                uid: pod.uid.clone(),
                deployment: fence.uid.clone(),
                target: replicas - 1,
                instance: instance_of(instances.as_ref(), pod)
                    .and_then(|i| i.addresses.first())
                    .map(ToString::to_string),
                since: rfc3339(self.now_ms),
                phase: match spec.service {
                    Some(_) => RetiringPhase::Unrouting,
                    None if asked_at_once => RetiringPhase::Retiring,
                    None => RetiringPhase::Committing,
                },
                unrouted: None,
                scaled: None,
                gone: None,
                admitted: held.admitted,
                egress: held.egress,
                anchors: held.anchors,
                asked: asked_at_once.then(|| rfc3339(self.now_ms)),
                instance_id,
                incarnation,
                retired: None,
                handoff: None,
                restarts: asked_at_once.then_some(pod.restarts),
                containers: match asked_at_once {
                    true => pod.containers.clone(),
                    false => Vec::new(),
                },
                restarted: false,
            });
            tracing::info!(
                pod = %pod.name,
                load = ?candidate.load,
                committed = ?candidate.committed,
                policy = spec.victim(),
                "chose a victim"
            );
            self.ahead(kept).await?;
            let at = kept.len() - 1;
            match kept[at].phase {
                RetiringPhase::Unrouting => self.label(pod, false).await?,
                RetiringPhase::Retiring => {
                    let bound = self.spec.drain_bound().ok().flatten().unwrap_or_default();
                    return self.ask(kept, at, pod, bound).await;
                }
                _ => replicas = self.commit(kept, at, pod, replicas, fence).await?,
            }
        }
        Ok(None)
    }

    /// Commit the removal of `kept[at]`, whose pod is `pod`, with the
    /// target at `replicas`: store `committing` at one below it, then set
    /// the pod's cost to the lowest, then write the count. The target's
    /// count after.
    async fn commit(
        &mut self,
        kept: &mut [Retiring],
        at: usize,
        pod: &PodView,
        replicas: i32,
        fence: &mut Fence,
    ) -> Result<i32, Stop> {
        let target = replicas - 1;
        kept[at].phase = RetiringPhase::Committing;
        kept[at].target = target;
        self.ahead(kept).await?;
        self.price(pod, Some(VICTIM_COST)).await?;
        fence.resource_version = self
            .cx
            .cluster
            .scale(&self.spec.target_ref.name, target, fence)
            .await?;
        kept[at].phase = RetiringPhase::Removing;
        kept[at].scaled = Some(rfc3339(self.now_ms));
        Ok(target)
    }

    /// Take back `kept[at]`, a victim not yet committed: it never retired.
    async fn cancel(
        &mut self,
        kept: &mut Vec<Retiring>,
        at: usize,
        pods: &[PodView],
    ) -> Result<(), Stop> {
        let victim = kept.remove(at);
        let uid = victim.uid.clone();
        let cost = victim.phase == RetiringPhase::Committing;
        let reason = "the scale-down was undone before it was committed".to_string();
        self.finish(victim, Outcome::Cancelled, String::new(), reason);
        self.ahead(kept).await?;
        if let Some(pod) = alive(pods, &uid) {
            self.put_back(pod, cost).await?;
        }
        Ok(())
    }

    /// Carry each victim of `retiring` on, by its pod among `pods`, with
    /// the target at `replicas`, from what its status says and what the
    /// target shows: a count at or below its target means its commit was
    /// written. A victim whose pod is gone ends in `status.last`: removed
    /// if committed, lost if not. A committed victim stays out of its
    /// Service until its pod is gone, except one the ReplicaSet passed
    /// over: its pod is alive, the count is at or below its target, and no
    /// more pods live than the count, so the ReplicaSet has nothing left to
    /// delete. In signal mode it never retired, and its uid goes in `undo`,
    /// to get its cost and its label back. In request mode it retired, and
    /// cannot serve again: it is kept as `deleting`, and its uid goes in
    /// `delete`, as does that of each victim being deleted whose pod is
    /// still alive.
    async fn follow(
        &mut self,
        retiring: Vec<Retiring>,
        pods: &[PodView],
        replicas: i32,
        bound: Option<Duration>,
        undo: &mut Vec<String>,
        delete: &mut Vec<String>,
    ) -> Result<Vec<Retiring>, Stop> {
        let mut kept = Vec::new();
        let mut listed: Option<Option<Instances>> = None;
        for mut victim in retiring {
            // How its removal ended was recorded when it was decided; it
            // is kept only until its pod is gone.
            if victim.phase == RetiringPhase::Deleting {
                match pods.iter().find(|p| p.uid == victim.uid) {
                    None => continue,
                    Some(pod) if !pod.terminating => delete.push(pod.uid.clone()),
                    Some(_) => {}
                }
                kept.push(victim);
                continue;
            }
            let committed = victim.phase == RetiringPhase::Removing || replicas <= victim.target;
            let Some(pod) = pods.iter().find(|p| p.uid == victim.uid) else {
                if !committed {
                    let reason = "its pod went before its removal was committed".to_string();
                    self.finish(victim, Outcome::Lost, String::new(), reason);
                    continue;
                }
                // The removal ends with the pod's exit and the instance's
                // leaving its hub. A hub that does not answer cannot say.
                let mut reason = String::new();
                if let Some(address) = victim.instance.clone() {
                    if listed.is_none() {
                        listed = Some(self.cx.probe.instances(&self.spec.pool).await.ok());
                    }
                    let still = listed
                        .as_ref()
                        .and_then(Option::as_ref)
                        .is_some_and(|list| {
                            list.instances
                                .iter()
                                .any(|i| i.addresses.iter().any(|a| a.to_string() == address))
                        });
                    if still {
                        let since = victim
                            .gone
                            .as_deref()
                            .and_then(millis)
                            .unwrap_or(self.now_ms);
                        if self.now_ms - since < millis_of(LEAVE_BOUND) {
                            victim.gone = Some(rfc3339(since));
                            kept.push(victim);
                            continue;
                        }
                        reason = format!(
                            "; its hub still listed it {} s after its pod was gone",
                            LEAVE_BOUND.as_secs()
                        );
                    }
                }
                let late = match (bound, victim.scaled.as_deref().and_then(millis)) {
                    (Some(bound), Some(scaled)) => {
                        self.now_ms - scaled > millis_of(bound + EXIT_MARGIN)
                    }
                    _ => false,
                };
                let outcome = if late {
                    Outcome::Overran
                } else {
                    Outcome::Removed
                };
                let reason = format!(
                    "chosen by `{}` with {} admitted requests{reason}",
                    self.spec.victim(),
                    victim.admitted
                );
                let deleted = victim.uid.clone();
                self.finish(victim, outcome, deleted, reason);
                continue;
            };
            if pod.terminating || committed {
                victim.phase = RetiringPhase::Removing;
                victim.scaled.get_or_insert_with(|| rfc3339(self.now_ms));
            }
            if pod.terminating {
                if let Some(held) = self.held(&[pod]).await.remove(&pod.uid) {
                    victim.admitted = held.admitted;
                    victim.egress = held.egress;
                    victim.anchors = held.anchors;
                }
                kept.push(victim);
                continue;
            }
            let live = pods.iter().filter(|p| !p.terminating).count();
            let passed_over = committed && i32::try_from(live).unwrap_or(i32::MAX) <= replicas;
            if !passed_over {
                kept.push(victim);
                continue;
            }
            let other = pods.iter().find(|p| {
                p.terminating && p.uid != victim.uid && !kept.iter().any(|v| v.uid == p.uid)
            });
            let (deleted, mut reason) = match other {
                Some(other) => (
                    other.uid.clone(),
                    format!("kubernetes chose pod `{}`", other.name),
                ),
                None => (
                    String::new(),
                    "another pod went, so the ReplicaSet deleted none".to_string(),
                ),
            };
            if victim.asked.is_none() {
                undo.push(pod.uid.clone());
                self.finish(victim, Outcome::KubernetesChoseAnother, deleted, reason);
                continue;
            }
            reason.push_str("; the victim had retired, so the scaler deletes its pod");
            self.finish(
                victim.clone(),
                Outcome::KubernetesChoseAnother,
                deleted,
                reason,
            );
            victim.phase = RetiringPhase::Deleting;
            delete.push(pod.uid.clone());
            kept.push(victim);
        }
        Ok(kept)
    }

    /// Give back their Service to the live pods that are out of it and
    /// that no victim names: a victim is stored before its label changes,
    /// so such a pod is one whose removal was undone or lost.
    async fn sweep(&mut self, pods: &[PodView], kept: &[Retiring]) -> Result<(), Stop> {
        for pod in pods {
            if !pod.terminating
                && pod.routable == Some(false)
                && !kept.iter().any(|v| v.uid == pod.uid)
                && !self.restored.contains(&pod.uid)
            {
                self.ahead(kept).await?;
                self.label(pod, true).await?;
            }
        }
        Ok(())
    }

    /// Put `pod`, which never retired, back in its entry Service, and
    /// clear its cost when `cost`. The status that says so is stored.
    async fn put_back(&mut self, pod: &PodView, cost: bool) -> Result<(), Stop> {
        if self.spec.service.is_some() || pod.routable == Some(false) {
            self.label(pod, true).await?;
            self.restored.insert(pod.uid.clone());
        }
        if cost {
            self.price(pod, None).await?;
        }
        Ok(())
    }

    /// Set `pod`'s routable label, at the version this reconcile last saw.
    async fn label(&mut self, pod: &PodView, routable: bool) -> Result<(), Stop> {
        let version = self.version_of(pod);
        let written = self.cx.cluster.routable(pod, &version, routable).await?;
        self.versions.insert(pod.uid.clone(), written);
        Ok(())
    }

    /// Set `pod`'s deletion cost, at the version this reconcile last saw.
    async fn price(&mut self, pod: &PodView, cost: Option<i32>) -> Result<(), Stop> {
        let version = self.version_of(pod);
        let written = self.cx.cluster.cost(pod, &version, cost).await?;
        self.versions.insert(pod.uid.clone(), written);
        Ok(())
    }

    /// `pod`'s resourceVersion: as this reconcile wrote it, or as read. A
    /// pod write needs a status write of this reconcile before it, which
    /// shows that the read it was decided on was current: another operator
    /// stores each step before it writes a pod, so one whose step came
    /// after this reconcile's read moved the scaler's version.
    fn version_of(&self, pod: &PodView) -> String {
        debug_assert!(self.confirmed, "a pod write needs a status write before it");
        self.versions
            .get(&pod.uid)
            .cloned()
            .unwrap_or_else(|| pod.resource_version.clone())
    }

    /// Store the status with `kept` as its victims, conditional on the
    /// scaler's resourceVersion, before the step that follows it: written
    /// if it changed, and in any case once in each reconcile.
    async fn ahead(&mut self, kept: &[Retiring]) -> Result<(), Stop> {
        self.status.retiring = kept.to_vec();
        self.write(!self.confirmed).await
    }

    /// Store the status if it changed since it was last stored, or when
    /// `always`, conditional on the scaler's resourceVersion. A write that
    /// changes nothing still fails on a stale version.
    async fn write(&mut self, always: bool) -> Result<(), Stop> {
        self.status.conditions = self.conditions.clone();
        if self.status == self.written && !always {
            return Ok(());
        }
        let version = self
            .cx
            .cluster
            .status(&self.name, &self.status, self.version.as_deref())
            .await?;
        // An answer without a version leaves the next write conditional on
        // the last one.
        if version.is_some() {
            self.version = version;
        }
        self.written = self.status.clone();
        self.confirmed = true;
        Ok(())
    }

    fn finish(&mut self, victim: Retiring, outcome: Outcome, deleted: String, mut reason: String) {
        if victim.restarted {
            reason.push_str("; restartedDuringHold: its container restarted during the hold, so it went on its SIGTERM, as in signal mode");
        }
        let since = millis(&victim.since).unwrap_or(self.now_ms);
        tracing::info!(pod = %victim.pod, ?outcome, %reason, handoff = ?victim.handoff, "a removal ended");
        self.status.last = Some(Last {
            pod: victim.pod,
            chosen: victim.uid,
            deleted,
            reason,
            duration_ms: u64::try_from(self.now_ms - since).unwrap_or(0),
            outcome,
            handoff: victim.handoff,
        });
    }

    /// What each of `pods` holds, by its uid, of those whose `/metrics`
    /// answered.
    async fn held(&self, pods: &[&PodView]) -> BTreeMap<String, Held> {
        let reads = pods.iter().map(|pod| async move {
            let (ip, port) = (pod.ip.as_deref()?, pod.system_port?);
            let text = self.cx.probe.metrics(ip, port).await.ok()?;
            Some((pod.uid.clone(), crate::probe::held(&text, self.spec)))
        });
        futures::future::join_all(reads)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Fill the status's counts from `pods`, and its victims.
    fn count(&mut self, pods: &[PodView], retiring: Vec<Retiring>) {
        let live = pods.iter().filter(|p| !p.terminating);
        self.status.replicas = i32::try_from(live.clone().count()).unwrap_or(i32::MAX);
        self.status.ready_replicas = i32::try_from(live.filter(|p| p.ready).count()).unwrap_or(0);
        self.status.retiring = retiring;
    }

    /// Set condition `kind`, keeping its transition time while its status
    /// stays.
    fn condition(&mut self, kind: &str, on: bool, reason: &str, message: &str) {
        let status = if on { "True" } else { "False" };
        let since = self
            .conditions
            .iter()
            .find(|c| c.type_ == kind && c.status == status)
            .map(|c| c.last_transition_time.clone());
        let condition = Condition {
            type_: kind.to_string(),
            status: status.to_string(),
            reason: reason.to_string(),
            message: message.to_string(),
            observed_generation: self.generation,
            last_transition_time: since.unwrap_or_else(|| Time(timestamp(self.now_ms))),
        };
        match self.conditions.iter_mut().find(|c| c.type_ == kind) {
            Some(slot) => *slot = condition,
            None => self.conditions.push(condition),
        }
    }
}

/// Whether `victim` is not yet committed: the target's count may still be
/// above its target.
fn uncommitted(victim: &Retiring) -> bool {
    !matches!(
        victim.phase,
        RetiringPhase::Removing | RetiringPhase::Deleting
    )
}

/// Whether `victim`'s removal can be undone: it is not committed, and it
/// has not retired. One whose retire step was stored may have.
fn cancellable(victim: &Retiring) -> bool {
    uncommitted(victim) && victim.asked.is_none()
}

/// The pod of `uid` among `pods`, while it is alive and not being deleted.
fn alive<'p>(pods: &'p [PodView], uid: &str) -> Option<&'p PodView> {
    pods.iter().find(|p| p.uid == uid && !p.terminating)
}

/// The instance `instances` lists at `pod`'s IP.
fn instance_of<'i>(
    instances: Option<&'i Instances>,
    pod: &PodView,
) -> Option<&'i crate::probe::Instance> {
    instances?.at(pod.ip.as_deref()?)
}

fn timestamp(ms: i64) -> Timestamp {
    Timestamp::from_millisecond(ms).unwrap_or(Timestamp::UNIX_EPOCH)
}

/// `ms` as RFC 3339.
fn rfc3339(ms: i64) -> String {
    timestamp(ms).to_string()
}

/// RFC 3339 `text` in milliseconds since the Unix epoch.
fn millis(text: &str) -> Option<i64> {
    text.parse::<Timestamp>().ok().map(|t| t.as_millisecond())
}

fn millis_of(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;
