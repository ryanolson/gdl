//! One reconcile of a `GdlScaler`, in `retire: signal` mode. The scaler
//! keeps the target at `max(spec.replicas, minReplicas)`. On a scale-down
//! it chooses each victim by its policy, takes it out of its entry
//! Service and waits until it has left the endpoints and the routing
//! grace has passed, then sets its deletion cost to the lowest and writes
//! the target's `replicas` one lower, so that the ReplicaSet deletes that
//! pod. The instance retires and drains on its SIGTERM.
//!
//! Write ahead, never infer. Before each step that changes a pod or the
//! target, the scaler stores in its status the victim, its pod's uid, the
//! Deployment's uid, the target count, and the phase that names the step;
//! each step can be taken again; and a reconcile that finds a step half
//! done finishes or undoes it from what the status says. Every write
//! carries the read it was decided on: the status the scaler's
//! resourceVersion, the target its uid and resourceVersion, a pod its uid.
//! A write refused as stale ends the reconcile, and the next one reads
//! again and decides again. See `docs/src/guides/operator.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;

use crate::cluster::{Cluster, Fence, PodView, Refused};
use crate::probe::{Held, Instances, Probe};
use crate::victim::{Candidate, Victims};
use crate::{GdlScaler, GdlScalerSpec, GdlScalerStatus, Last, Outcome, Retiring, RetiringPhase};

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
        spec: &scaler.spec,
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
    match run.write().await {
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
    spec: &'a GdlScalerSpec,
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
        let mut current = Vec::new();
        for victim in retiring {
            if victim.deployment == fence.uid {
                current.push(victim);
                continue;
            }
            if let Some(pod) = alive(&pods, &victim.uid) {
                self.put_back(pod, true).await?;
            }
            let reason = "its Deployment was made again under the same name".to_string();
            self.finish(victim, Outcome::Abandoned, String::new(), reason);
        }
        let mut kept = self.follow(current, &pods, replicas, bound).await?;
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
                while let Some(at) = kept.iter().position(uncommitted) {
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
        // latest chosen first: none has retired. It writes the count up
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
            self.cancel(&mut kept, at, &pods).await?;
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
        // routing grace, then its commit. A commit stored but not written
        // is written again.
        if let Some(at) = kept.iter().position(uncommitted) {
            let pod = alive(&pods, &kept[at].uid)
                .expect("follow keeps a victim not yet committed only while its pod is alive");
            let mut commit = kept[at].phase == RetiringPhase::Committing;
            if kept[at].phase == RetiringPhase::Unrouting {
                if pod.routable != Some(false) {
                    self.cx.cluster.routable(pod, false).await?;
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
            let room = usize::try_from(spec.max_concurrent_drains())
                .unwrap_or(1)
                .saturating_sub(kept.len());
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
                let count = match spec.service {
                    Some(_) => 1,
                    None => room.min(excess),
                };
                self.choose(&mut kept, &staying, count, replicas, &mut fence)
                    .await?;
            }
        }

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
    /// taken out of its Service, or, without one, committed at once.
    async fn choose(
        &mut self,
        kept: &mut Vec<Retiring>,
        staying: &[&PodView],
        count: usize,
        mut replicas: i32,
        fence: &mut Fence,
    ) -> Result<(), Stop> {
        let spec = self.spec;
        let held = self.held(staying).await;
        let instances = self.cx.probe.instances(&spec.pool).await.ok();
        let ranked: Vec<Candidate> = staying
            .iter()
            .map(|pod| Candidate {
                pod: pod.name.clone(),
                uid: pod.uid.clone(),
                created_ms: pod.created_ms,
                load: crate::probe::load_of(
                    held.get(&pod.uid),
                    instance_of(instances.as_ref(), pod),
                ),
            })
            .collect();
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
                    None => RetiringPhase::Committing,
                },
                unrouted: None,
                scaled: None,
                gone: None,
                admitted: held.admitted,
                egress: held.egress,
                anchors: held.anchors,
            });
            tracing::info!(
                pod = %pod.name,
                load = ?candidate.load,
                policy = spec.victim(),
                "chose a victim"
            );
            self.persist(kept).await?;
            let at = kept.len() - 1;
            match kept[at].phase {
                RetiringPhase::Unrouting => self.cx.cluster.routable(pod, false).await?,
                _ => replicas = self.commit(kept, at, pod, replicas, fence).await?,
            }
        }
        Ok(())
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
        if kept[at].phase != RetiringPhase::Committing || kept[at].target != target {
            kept[at].phase = RetiringPhase::Committing;
            kept[at].target = target;
            self.persist(kept).await?;
        }
        self.cx.cluster.cost(pod, Some(VICTIM_COST)).await?;
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
        if let Some(pod) = alive(pods, &victim.uid) {
            let cost = victim.phase == RetiringPhase::Committing;
            self.put_back(pod, cost).await?;
        }
        let reason = "the scale-down was undone before it was committed".to_string();
        self.finish(victim, Outcome::Cancelled, String::new(), reason);
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
    /// delete. In signal mode it never retired, and it gets its cost and
    /// its label back.
    async fn follow(
        &mut self,
        retiring: Vec<Retiring>,
        pods: &[PodView],
        replicas: i32,
        bound: Option<Duration>,
    ) -> Result<Vec<Retiring>, Stop> {
        let mut kept = Vec::new();
        let mut listed: Option<Option<Instances>> = None;
        for mut victim in retiring {
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
            self.put_back(pod, true).await?;
            let other = pods.iter().find(|p| {
                p.terminating && p.uid != victim.uid && !kept.iter().any(|v| v.uid == p.uid)
            });
            let (deleted, reason) = match other {
                Some(other) => (
                    other.uid.clone(),
                    format!("kubernetes chose pod `{}`", other.name),
                ),
                None => (
                    String::new(),
                    "another pod went, so the ReplicaSet deleted none".to_string(),
                ),
            };
            self.finish(victim, Outcome::KubernetesChoseAnother, deleted, reason);
        }
        Ok(kept)
    }

    /// Give back their Service to the live pods that are out of it and
    /// that no victim names: a victim is stored before its label changes,
    /// so such a pod is one whose removal was undone or lost.
    async fn sweep(&self, pods: &[PodView], kept: &[Retiring]) -> Result<(), Stop> {
        for pod in pods {
            if !pod.terminating
                && pod.routable == Some(false)
                && !kept.iter().any(|v| v.uid == pod.uid)
                && !self.restored.contains(&pod.uid)
            {
                self.cx.cluster.routable(pod, true).await?;
            }
        }
        Ok(())
    }

    /// Put `pod`, which never retired, back in its entry Service, and
    /// clear its cost when `cost`.
    async fn put_back(&mut self, pod: &PodView, cost: bool) -> Result<(), Stop> {
        if self.spec.service.is_some() || pod.routable == Some(false) {
            self.cx.cluster.routable(pod, true).await?;
            self.restored.insert(pod.uid.clone());
        }
        if cost {
            self.cx.cluster.cost(pod, None).await?;
        }
        Ok(())
    }

    /// Store the status with `kept` as its victims, conditional on the
    /// scaler's resourceVersion: the intent of the step that follows.
    async fn persist(&mut self, kept: &[Retiring]) -> Result<(), Stop> {
        self.status.retiring = kept.to_vec();
        self.write().await
    }

    /// Store the status if it changed since it was last stored.
    async fn write(&mut self) -> Result<(), Stop> {
        self.status.conditions = self.conditions.clone();
        if self.status == self.written {
            return Ok(());
        }
        let version = self
            .cx
            .cluster
            .status(&self.name, &self.status, self.version.as_deref())
            .await?;
        // An answer without a version leaves the next write unconditional.
        self.version = version;
        self.written = self.status.clone();
        Ok(())
    }

    fn finish(&mut self, victim: Retiring, outcome: Outcome, deleted: String, reason: String) {
        let since = millis(&victim.since).unwrap_or(self.now_ms);
        tracing::info!(pod = %victim.pod, ?outcome, %reason, "a removal ended");
        self.status.last = Some(Last {
            pod: victim.pod,
            chosen: victim.uid,
            deleted,
            reason,
            duration_ms: u64::try_from(self.now_ms - since).unwrap_or(0),
            outcome,
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
    victim.phase != RetiringPhase::Removing
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
