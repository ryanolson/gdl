//! The steps of `retire: request`, between a victim's choice (and its
//! routing grace) and its commit: ask its hub to retire its instance, then
//! hold the commit until the hub lists the instance `drained` and its
//! pod's `/metrics` shows every leave hook's outcome, or until
//! `drainBound` has passed since the hub took the request.
//!
//! The retire request is a write like a pod's: the step that names it is
//! stored first, with the instance's id and the incarnation of its
//! registration, and the request goes out only after a status write of the
//! same reconcile has landed. From that store on the victim may have
//! retired, so its scale-down is never undone: a scale-up, or a
//! ReplicaSet that deletes another pod, has the scaler delete the
//! victim's pod itself, for the ReplicaSet to replace.

use std::time::Duration;

use super::{Run, Stop, instance_of, millis, millis_of, rfc3339};
use crate::cluster::{Cluster, PodView};
use crate::probe::{Asked, Probe};
use crate::{Handoff, Outcome, Retiring, RetiringPhase};

/// The reason of the `Blocked` condition, and of its one event, when the
/// hub has not taken a retire request `drainBound` after it was stored.
pub(super) const RETIRE_REFUSED: &str = "RetireRefused";

/// The reason of the event, and the word in `status.last`'s reason, when
/// a victim's container restarted during its hold.
pub(super) const RESTARTED_DURING_HOLD: &str = "RestartedDuringHold";

impl<C: Cluster, P: Probe> Run<'_, '_, C, P> {
    /// Ask the hub of `kept[at]`'s pool, whose pod is `pod`, to retire its
    /// instance, waiting for it to take the request at most `bound`. A victim leaving its Service first reads its instance's
    /// registration, and stores its retire step; one whose registration
    /// changed since is stored again before it is asked. Once the hub
    /// takes the request, the victim is `draining`. Why it waits, when it
    /// is blocked.
    pub(super) async fn ask(
        &mut self,
        kept: &mut [Retiring],
        at: usize,
        pod: &PodView,
        bound: Duration,
    ) -> Result<Option<(&'static str, String)>, Stop> {
        let pool = self.spec.pool.clone();
        let listed = self.cx.probe.instances(&pool).await;
        let instance = listed
            .as_ref()
            .ok()
            .and_then(|list| instance_of(Some(list), pod));
        let registration = instance.and_then(crate::probe::Instance::registration);
        if kept[at].phase == RetiringPhase::Unrouting {
            // Not yet asked: it can still be given back.
            let Some((id, incarnation)) = registration.clone() else {
                let why = match &listed {
                    Err(error) => ("HubUnavailable", error.clone()),
                    Ok(_) => (
                        "InstanceNotListed",
                        format!("the hub of pool `{pool}` does not list pod `{}`", pod.name),
                    ),
                };
                return Ok(Some(why));
            };
            let victim = &mut kept[at];
            victim.phase = RetiringPhase::Retiring;
            victim.instance_id = Some(id);
            victim.incarnation = Some(incarnation);
            victim.asked = Some(rfc3339(self.now_ms));
            victim.restarts = Some(pod.restarts);
            self.ahead(kept).await?;
        } else if let Some((id, incarnation)) = registration.clone()
            && (Some(&id), Some(incarnation))
                != (kept[at].instance_id.as_ref(), kept[at].incarnation)
        {
            // Another registration is another instance: it is recorded
            // before it is asked, in the next reconcile.
            kept[at].instance_id = Some(id);
            kept[at].incarnation = Some(incarnation);
            self.ahead(kept).await?;
            return Ok(None);
        }
        // The request is a write: a status write of this reconcile has
        // shown its read of the scaler current.
        self.ahead(kept).await?;
        let victim = &kept[at];
        let (Some(id), Some(incarnation)) = (victim.instance_id.clone(), victim.incarnation) else {
            return Ok(Some((
                "InstanceNotListed",
                format!("victim `{}` has no instance to retire", victim.pod),
            )));
        };
        let held = instance.is_some_and(|i| i.retire_requested)
            && registration == Some((id.clone(), incarnation));
        let answer = match held {
            true => Ok(Asked::Accepted),
            false => self.cx.probe.retire(&pool, &id, incarnation).await,
        };
        let refused = match answer {
            Ok(Asked::Accepted) => {
                tracing::info!(pod = %victim.pod, instance = %id, incarnation, "its hub took the retire request");
                // The bound counts from the hub's acceptance, not from the
                // start of the reconcile, whose reads and writes came first.
                let accepted = (self.cx.clock)();
                let victim = &mut kept[at];
                victim.phase = RetiringPhase::Draining;
                victim.retired = Some(rfc3339(accepted));
                return Ok(None);
            }
            Ok(Asked::Unknown) => format!("the hub of pool `{pool}` has no member {id}"),
            Ok(Asked::Refused(why)) => {
                format!("the hub of pool `{pool}` refused to retire {id} ({incarnation}): {why}")
            }
            Err(error) => error,
        };
        let asked = kept[at]
            .asked
            .as_deref()
            .and_then(millis)
            .unwrap_or(self.now_ms);
        if self.now_ms - asked < millis_of(bound) {
            tracing::debug!(pod = %kept[at].pod, %refused, "the retire request waits");
            return Ok(None);
        }
        // Fail closed, and say so once: no pod is deleted and the count
        // stays, until the hub takes the request or a scale-up replaces
        // the victim.
        let message = format!(
            "victim `{}` was not retired within drainBound: {refused}",
            kept[at].pod
        );
        let said = self
            .conditions
            .iter()
            .any(|c| c.type_ == "Blocked" && c.status == "True" && c.reason == RETIRE_REFUSED);
        if !said {
            self.cx
                .cluster
                .warn(self.scaler, RETIRE_REFUSED, &message)
                .await;
        }
        Ok(Some((RETIRE_REFUSED, message)))
    }

    /// Whether `victim`, whose hub took its retire request, whose pod is
    /// `pod`, is done: its hub lists the registration it asked, by its id
    /// and incarnation, `drained`, or no longer lists it, and its pod's
    /// `/metrics` shows no leave hook still pending; or `bound` has passed
    /// since the hub took the request. What its `/metrics` says is kept in
    /// `victim`. A registration that left while the pod did not restart
    /// has drained: an instance leaves only after its drain. One whose pod
    /// restarted never reaches here ([`Run::restarted`]).
    pub(super) async fn hold(
        &mut self,
        victim: &mut Retiring,
        pod: &PodView,
        bound: Duration,
    ) -> bool {
        let asked = victim.instance_id.clone().zip(victim.incarnation);
        let drained = match (self.cx.probe.instances(&self.spec.pool).await, asked) {
            (Ok(list), Some((id, incarnation))) => list
                .by_registration(&id, incarnation)
                .is_none_or(|instance| instance.phase == "drained"),
            _ => false,
        };
        let read = match (pod.ip.as_deref(), pod.system_port) {
            (Some(ip), Some(port)) => self.cx.probe.metrics(ip, port).await.ok(),
            _ => None,
        };
        let leave = read
            .as_deref()
            .map(|text| crate::probe::leave(text, self.spec));
        if let Some(text) = read.as_deref() {
            let held = crate::probe::held(text, self.spec);
            victim.admitted = held.admitted;
            victim.egress = held.egress;
            victim.anchors = held.anchors;
        }
        let reported = leave.is_some_and(|leave| leave.pending == 0);
        if let Some(worst) = leave.and_then(|leave| leave.worst) {
            victim.handoff = Some(worst);
        }
        if drained && reported {
            return true;
        }
        let retired = victim
            .retired
            .as_deref()
            .and_then(millis)
            .unwrap_or(self.now_ms);
        if self.now_ms - retired < millis_of(bound) {
            return false;
        }
        if !reported {
            victim.handoff = Some(Handoff::Unreported);
        }
        tracing::info!(
            pod = %victim.pod,
            drained,
            reported,
            "the victim's drain bound passed; its removal goes on"
        );
        true
    }

    /// Whether `victim`, asked to retire, whose pod is `pod`, has a
    /// container that restarted since: a new process, which was never
    /// asked. A restart ends the hold, and the victim goes on its SIGTERM,
    /// as in signal mode, which retires and drains the new process; it
    /// loses only the hold. The first time, its hooks' outcome is
    /// `unreported`, and an event says so.
    pub(super) async fn restarted(&mut self, victim: &mut Retiring, pod: &PodView) -> bool {
        if victim.asked.is_none() || victim.restarts.is_none_or(|at| at == pod.restarts) {
            return false;
        }
        if !victim.restarted {
            victim.restarted = true;
            victim.handoff = Some(Handoff::Unreported);
            let note = format!(
                "victim `{}` restarted during its hold; it goes on its SIGTERM, as in signal mode",
                victim.pod
            );
            tracing::info!(pod = %victim.pod, "{note}");
            self.cx
                .cluster
                .warn(self.scaler, RESTARTED_DURING_HOLD, &note)
                .await;
        }
        true
    }

    /// A scale-up undid the scale-down of `kept[at]`, which may have
    /// retired, so it cannot serve again: its pod is deleted, for the
    /// ReplicaSet to replace. Its removal ends `replaced`; it is kept as
    /// `deleting` until its pod is gone.
    pub(super) async fn replace(
        &mut self,
        kept: &mut [Retiring],
        at: usize,
        pods: &[PodView],
    ) -> Result<(), Stop> {
        let reason = "a scale-up undid the scale-down after the victim retired".to_string();
        self.finish(
            kept[at].clone(),
            Outcome::Replaced,
            kept[at].uid.clone(),
            reason,
        );
        kept[at].phase = RetiringPhase::Deleting;
        self.ahead(kept).await?;
        if let Some(pod) = super::alive(pods, &kept[at].uid) {
            self.delete(pod).await?;
        }
        Ok(())
    }

    /// Delete `pod`, at the version this reconcile last saw.
    pub(super) async fn delete(&mut self, pod: &PodView) -> Result<(), Stop> {
        let version = self.version_of(pod);
        self.cx.cluster.delete(pod, &version).await?;
        tracing::info!(pod = %pod.name, "deleted a retired victim's pod");
        Ok(())
    }
}
