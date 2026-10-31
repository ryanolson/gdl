//! One reconcile of a `GdlScaler`, in `retire: signal` mode. The scaler
//! keeps the target at `max(spec.replicas, minReplicas)`. On a scale-down
//! it chooses each victim by its policy, takes it out of its entry
//! Service and waits until it has left the endpoints and the routing
//! grace has passed, then sets its deletion cost to the lowest and writes
//! the target's `replicas` one lower, so that the ReplicaSet deletes that
//! pod. The instance retires and drains on its SIGTERM. Everything the
//! scaler must remember between reconciles is in its status, so an
//! operator that restarts carries on. See `docs/src/guides/operator.md`.

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;

use crate::cluster::{Cluster, PodView};
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
        now_ms,
        status: GdlScalerStatus {
            observed_generation: scaler.metadata.generation,
            ..old.clone()
        },
        conditions: old.conditions.clone(),
        generation: scaler.metadata.generation,
    };
    let requeue = match run.step(old.retiring.clone()).await {
        Ok(requeue) => requeue,
        Err(Stop::Blocked(reason, message)) => {
            run.condition("Blocked", true, reason, &message);
            BLOCKED
        }
        Err(Stop::Failed(error)) => return Err(error),
    };
    run.status.conditions = run.conditions;
    if run.status != old {
        cx.cluster.status(&name, &run.status).await?;
    }
    Ok(requeue)
}

/// Why a reconcile stopped early.
enum Stop {
    /// It cannot act until something changes: the reason and the message
    /// of the `Blocked` condition.
    Blocked(&'static str, String),
    /// A read or a write failed.
    Failed(String),
}

impl From<String> for Stop {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

struct Run<'a, 'c, C, P> {
    cx: &'a Context<'c, C, P>,
    spec: &'a GdlScalerSpec,
    now_ms: i64,
    status: GdlScalerStatus,
    conditions: Vec<Condition>,
    generation: Option<i64>,
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
        let mut replicas = target.replicas;

        let mut kept = self.follow(retiring, &pods, replicas, bound).await?;

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
                for victim in kept.extract_if(.., |v| v.phase == RetiringPhase::Unrouting) {
                    self.restore(&pods, &victim).await?;
                }
                if replicas != 0 {
                    self.cx.cluster.scale(&target_ref.name, 0).await?;
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

        // A scale-up first takes back the victims still being taken out of
        // their Service, the latest chosen first: none has retired.
        let unrouting = |kept: &[Retiring]| {
            let count = kept
                .iter()
                .filter(|v| v.phase == RetiringPhase::Unrouting)
                .count();
            i32::try_from(count).unwrap_or(i32::MAX)
        };
        while desired > replicas - unrouting(&kept) {
            let Some(at) = kept
                .iter()
                .enumerate()
                .filter(|(_, v)| v.phase == RetiringPhase::Unrouting)
                .max_by(|(_, a), (_, b)| a.since.cmp(&b.since))
                .map(|(at, _)| at)
            else {
                break;
            };
            let victim = kept.remove(at);
            self.restore(&pods, &victim).await?;
        }
        if desired > replicas {
            self.cx.cluster.scale(&target_ref.name, desired).await?;
            replicas = desired;
        }

        // Victims out of their Service, past the routing grace, are
        // deleted: the lowest cost, then one replica fewer.
        if kept.iter().any(|v| v.phase == RetiringPhase::Unrouting) {
            let service = spec.service.as_deref().unwrap_or_default();
            let listed = self.cx.cluster.endpoints(service).await?;
            for victim in &mut kept {
                if victim.phase != RetiringPhase::Unrouting || listed.contains(&victim.uid) {
                    continue;
                }
                let Some(unrouted) = victim.unrouted.as_deref().and_then(millis) else {
                    victim.unrouted = Some(rfc3339(self.now_ms));
                    continue;
                };
                if self.now_ms - unrouted < millis_of(grace) {
                    continue;
                }
                let pod = pods
                    .iter()
                    .find(|p| p.uid == victim.uid)
                    .expect("follow keeps only victims whose pod exists");
                replicas -= 1;
                self.delete(pod, replicas).await?;
                victim.phase = RetiringPhase::Deleting;
                victim.scaled = Some(rfc3339(self.now_ms));
            }
        }

        // New victims.
        let effective = replicas - unrouting(&kept);
        let mut blocked = None;
        if desired < effective {
            let room = usize::try_from(spec.max_concurrent_drains())
                .unwrap_or(1)
                .saturating_sub(kept.len());
            let ours = |pod: &PodView| kept.iter().any(|v| v.uid == pod.uid);
            if room == 0 {
                // The drains in progress pace the scale-down.
            } else if target.rolling {
                blocked = Some((
                    "RolloutInProgress",
                    "a rollout of the target is in progress".to_string(),
                ));
            } else if let Some(pod) = pods.iter().find(|p| p.terminating && !ours(p)) {
                blocked = Some((
                    "PodTerminating",
                    format!("pod `{}` is terminating", pod.name),
                ));
            } else if let Some(pod) = pods.iter().find(|p| !p.terminating && !ours(p) && !p.ready) {
                blocked = Some(("PodNotReady", format!("pod `{}` is not ready", pod.name)));
            } else {
                let count = room.min(usize::try_from(effective - desired).unwrap_or(0));
                let candidates: Vec<&PodView> = pods.iter().filter(|p| !ours(p)).collect();
                let held = self.held(&candidates).await;
                let instances = self.cx.probe.instances(&spec.pool).await.ok();
                let ranked: Vec<Candidate> = candidates
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
                    let pod = candidates
                        .iter()
                        .find(|p| p.uid == candidate.uid)
                        .expect("a candidate is a pod");
                    let held = held.get(&pod.uid).copied().unwrap_or_default();
                    let mut victim = Retiring {
                        pod: pod.name.clone(),
                        uid: pod.uid.clone(),
                        instance: instance_of(instances.as_ref(), pod)
                            .and_then(|i| i.addresses.first())
                            .map(ToString::to_string),
                        since: rfc3339(self.now_ms),
                        phase: RetiringPhase::Unrouting,
                        unrouted: None,
                        scaled: None,
                        gone: None,
                        admitted: held.admitted,
                        egress: held.egress,
                        anchors: held.anchors,
                    };
                    if spec.service.is_some() {
                        self.cx.cluster.routable(pod, false).await?;
                    } else {
                        replicas -= 1;
                        self.delete(pod, replicas).await?;
                        victim.phase = RetiringPhase::Deleting;
                        victim.scaled = Some(rfc3339(self.now_ms));
                    }
                    tracing::info!(
                        pod = %pod.name,
                        load = ?candidate.load,
                        policy = spec.victim(),
                        "chose a victim"
                    );
                    kept.push(victim);
                }
            }
        }

        let busy = !kept.is_empty();
        self.count(&pods, kept);
        match &blocked {
            Some((reason, message)) => self.condition("Blocked", true, reason, message),
            None => self.condition("Blocked", false, "NotBlocked", ""),
        }
        let live = self.status.replicas;
        let scaling = busy || live != desired;
        let message = format!("{live} of {desired}");
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

    /// Carry each victim of `retiring` on, by its pod among `pods`, with
    /// the target at `replicas`: the victims still being removed. A victim
    /// whose pod is gone ends in `status.last`. One whose pod the
    /// ReplicaSet passed over, since another pod went or the scale-down
    /// was undone, gets its cost and its label back: in signal mode it
    /// never retired.
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
            let Some(pod) = pods.iter().find(|p| p.uid == victim.uid) else {
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
            if pod.terminating {
                // Being deleted, whatever the status says: a status read
                // before the write that scaled it down still says
                // `unrouting`, and a victim taken for one would be scaled
                // down again, or taken back.
                if victim.phase == RetiringPhase::Unrouting {
                    victim.phase = RetiringPhase::Deleting;
                    victim.scaled.get_or_insert_with(|| rfc3339(self.now_ms));
                }
                if let Some(held) = self.held(&[pod]).await.remove(&pod.uid) {
                    victim.admitted = held.admitted;
                    victim.egress = held.egress;
                    victim.anchors = held.anchors;
                }
                kept.push(victim);
                continue;
            }
            let live = pods.iter().filter(|p| !p.terminating).count();
            let passed_over = victim.phase == RetiringPhase::Deleting
                && i32::try_from(live).unwrap_or(i32::MAX) <= replicas;
            if !passed_over {
                kept.push(victim);
                continue;
            }
            self.restore(pods, &victim).await?;
            self.cx.cluster.cost(pod, None).await?;
            match pods.iter().find(|p| {
                p.terminating && p.uid != victim.uid && !kept.iter().any(|v| v.uid == p.uid)
            }) {
                Some(other) => {
                    let reason = format!("kubernetes chose pod `{}`", other.name);
                    let deleted = other.uid.clone();
                    self.finish(victim, Outcome::KubernetesChoseAnother, deleted, reason);
                }
                None => {
                    let reason = "the scale-down was undone before the pod went".to_string();
                    self.finish(victim, Outcome::Cancelled, String::new(), reason);
                }
            }
        }
        Ok(kept)
    }

    /// Delete `pod` through its ReplicaSet: the lowest cost, then the
    /// target at `replicas`.
    async fn delete(&self, pod: &PodView, replicas: i32) -> Result<(), Stop> {
        self.cx.cluster.cost(pod, Some(VICTIM_COST)).await?;
        self.cx
            .cluster
            .scale(&self.spec.target_ref.name, replicas)
            .await?;
        Ok(())
    }

    /// Put `victim`, which never retired, back in its entry Service.
    async fn restore(&self, pods: &[PodView], victim: &Retiring) -> Result<(), Stop> {
        if self.spec.service.is_none() {
            return Ok(());
        }
        if let Some(pod) = pods.iter().find(|p| p.uid == victim.uid) {
            self.cx.cluster.routable(pod, true).await?;
        }
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
