//! The `GdlScaler` custom resource: one per segment of a pool, in the
//! pool's namespace. Its `/scale` subresource is what an HPA, or a person,
//! writes; the operator is then the one writer of its target's `replicas`.
//! See `docs/src/guides/operator.md`.

use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The API group of gdl's custom resources.
pub const GROUP: &str = "gdl.dev";

/// Their version.
pub const VERSION: &str = "v1alpha1";

/// The label every pod of a segment with an entry Service carries, `true`
/// while it may take new connections. Entry Services select on it, so a
/// pod the operator sets to `false` leaves their endpoints. It is not in
/// the Deployment's selector, so the ReplicaSet still owns the pod.
pub const ROUTABLE: &str = "gdl.dev/routable";

/// The annotation by which a ReplicaSet orders the pods it removes: the
/// lowest cost goes first.
pub const DELETION_COST: &str = "controller.kubernetes.io/pod-deletion-cost";

/// The field manager that owns a target's `replicas` from its birth, under
/// which the operator writes the field by forced apply.
pub const SCALER_MANAGER: &str = "gdl-scaler";

/// What a `GdlScaler` asks for: how many instances of one segment of one
/// pool run, and how one is removed.
#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "gdl.dev",
    version = "v1alpha1",
    kind = "GdlScaler",
    namespaced,
    status = "GdlScalerStatus",
    shortname = "gscaler",
    scale = r#"{"specReplicasPath": ".spec.replicas", "statusReplicasPath": ".status.replicas", "labelSelectorPath": ".status.selector"}"#,
    printcolumn = r#"{"name": "Desired", "type": "integer", "jsonPath": ".spec.replicas"}"#,
    printcolumn = r#"{"name": "Current", "type": "integer", "jsonPath": ".status.replicas"}"#,
    printcolumn = r#"{"name": "Ready", "type": "integer", "jsonPath": ".status.readyReplicas"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GdlScalerSpec {
    /// The pool whose segment it scales; its hub's Service has the pool's
    /// name.
    pub pool: String,
    /// The segment.
    pub segment: String,
    /// The object whose `replicas` it writes.
    pub target_ref: TargetRef,
    /// How many instances to run. An HPA writes it through `/scale`. Until
    /// it is set, the scaler does nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub replicas: Option<i32>,
    /// The fewest it scales to: the segment's quorum by default, never
    /// below 1, since the last instance of a segment is not avoided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub min_replicas: Option<i32>,
    /// `zero` holds the target at 0, whatever `replicas` and
    /// `minReplicas` say. Only a pool's own controller writes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspend: Option<Suspend>,
    /// How many instances drain at once: 1 by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_concurrent_drains: Option<i32>,
    /// The registered victim policy that chooses the instance to remove:
    /// `fewestAdmitted` by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub victim: Option<String>,
    /// How the victim is retired: `signal`, by the SIGTERM of its pod's
    /// delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retire: Option<Retire>,
    /// How long a victim's drain may take, as `<n>ms`, `<n>s`, `<n>m` or
    /// `<n>h`: the segment's `drain_ms`. Kubernetes enforces it in signal
    /// mode, at the pod's grace; the scaler reports a drain that ran past
    /// it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(pattern = r"^[0-9]+(ms|s|m|h)$"))]
    pub drain_bound: Option<String>,
    /// How long after a victim leaves its entry Service's endpoints the
    /// scaler waits before it deletes the pod, for kube-proxy and load
    /// balancers to stop sending to it: `10s` by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(pattern = r"^[0-9]+(ms|s|m|h)$"))]
    pub routing_grace: Option<String>,
    /// The segment's entry Service, when it has one: a victim leaves its
    /// endpoints before it drains.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// The prefix of the segment's metric names, its `[system]
    /// metrics_prefix`: `dynamo_component` by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_prefix: Option<String>,
}

/// The object a `GdlScaler` scales.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TargetRef {
    /// `apps/v1`.
    pub api_version: String,
    /// `Deployment`.
    pub kind: String,
    /// Its name, in the scaler's namespace.
    pub name: String,
}

/// A hold of the target at a count, whatever the scaler's `replicas`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Suspend {
    /// Hold the target at 0.
    Zero,
}

/// How a victim is retired.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Retire {
    /// By the SIGTERM of its pod's delete: the instance retires, drains,
    /// and exits itself, bounded by the pod's grace.
    #[default]
    Signal,
}

/// What the scaler observed and did.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdlScalerStatus {
    /// The generation of the spec it last acted on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The target's current count, which `/scale` reports to an HPA.
    #[serde(default)]
    pub replicas: i32,
    /// The selector of the target's pods, as text, for an HPA.
    #[serde(default)]
    pub selector: String,
    /// The target's pods that are ready.
    #[serde(default)]
    pub ready_replicas: i32,
    /// The victims chosen and not gone yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retiring: Vec<Retiring>,
    /// The last removal that ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<Last>,
    /// `Scaling`, `Blocked`, and `Suspended`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A victim the scaler chose, kept until its pod is gone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Retiring {
    /// Its pod's name.
    pub pod: String,
    /// Its pod's uid, by which the scaler tracks it.
    pub uid: String,
    /// Its instance's velo address, `ip:port`, when the hub listed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// When it was chosen, RFC 3339.
    pub since: String,
    /// Where its removal is.
    pub phase: RetiringPhase,
    /// When it left its entry Service's endpoints, RFC 3339: the routing
    /// grace counts from here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unrouted: Option<String>,
    /// When the scaler scaled the target down for it, RFC 3339: its pod's
    /// delete, and so its drain, starts here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaled: Option<String>,
    /// Its admitted requests, as its `/metrics` last said: when chosen,
    /// then while it drains.
    #[serde(default)]
    pub admitted: u64,
    /// Its egress permits in use, read as `admitted` is.
    #[serde(default)]
    pub egress: u64,
    /// Its open anchors, read as `admitted` is.
    #[serde(default)]
    pub anchors: u64,
}

/// Where a victim's removal is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RetiringPhase {
    /// Taken out of its entry Service: the scaler waits until the
    /// endpoints no longer list it, and then the routing grace.
    Unrouting,
    /// Its cost is set and the target scaled down: the ReplicaSet deletes
    /// it, and it drains on its SIGTERM.
    Deleting,
}

/// How the last removal ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Last {
    /// The victim's pod.
    pub pod: String,
    /// The uid of the pod the scaler chose.
    pub chosen: String,
    /// The uid of the pod that was deleted.
    pub deleted: String,
    /// Why it was chosen, or why another went.
    pub reason: String,
    /// From the choice until the pod was gone.
    pub duration_ms: u64,
    /// How it ended.
    pub outcome: Outcome,
}

/// How a removal ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Outcome {
    /// The chosen pod went within its drain bound.
    Removed,
    /// The chosen pod went, after its drain bound.
    Overran,
    /// The ReplicaSet deleted another pod. In signal mode the victim was
    /// never retired, so nothing was lost.
    KubernetesChoseAnother,
    /// The scale-down was undone before the ReplicaSet deleted a pod.
    Cancelled,
}

impl GdlScalerSpec {
    /// The victim policy's name.
    pub fn victim(&self) -> &str {
        self.victim
            .as_deref()
            .unwrap_or(crate::victim::FEWEST_ADMITTED)
    }

    /// The fewest instances it scales to, at least 1.
    pub fn min_replicas(&self) -> i32 {
        self.min_replicas.unwrap_or(1).max(1)
    }

    /// How many victims drain at once, at least 1.
    pub fn max_concurrent_drains(&self) -> i32 {
        self.max_concurrent_drains.unwrap_or(1).max(1)
    }

    /// The routing grace: 10 s unless set.
    pub fn routing_grace(&self) -> Result<Duration, String> {
        self.routing_grace
            .as_deref()
            .map_or(Ok(Duration::from_secs(10)), parse_duration)
    }

    /// The drain bound, when set.
    pub fn drain_bound(&self) -> Result<Option<Duration>, String> {
        self.drain_bound.as_deref().map(parse_duration).transpose()
    }

    /// The metric names' prefix, with the `_` that joins it, or nothing
    /// for an empty prefix.
    pub fn metric(&self, name: &str) -> String {
        match self.metrics_prefix.as_deref().unwrap_or(DEFAULT_PREFIX) {
            "" => name.to_string(),
            prefix => format!("{prefix}_{name}"),
        }
    }
}

/// A segment's metric prefix when its settings name none.
pub const DEFAULT_PREFIX: &str = "dynamo_component";

/// `text`, a count and one unit of `ms`, `s`, `m` or `h`.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let refuse = || format!("`{text}` is not a duration: write a count and one of ms, s, m, h");
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(refuse)?;
    let (count, unit) = text.split_at(split);
    let count: u64 = count.parse().map_err(|_| refuse())?;
    let millis = match unit {
        "ms" => Some(count),
        "s" => count.checked_mul(1_000),
        "m" => count.checked_mul(60_000),
        "h" => count.checked_mul(3_600_000),
        _ => None,
    }
    .ok_or_else(refuse)?;
    Ok(Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_duration_is_a_count_and_a_unit() {
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("20s"), Ok(Duration::from_secs(20)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3600)));
        for refused in ["", "s", "10", "1.5s", "10 s", "-1s", "1d", "1m30s"] {
            assert!(parse_duration(refused).is_err(), "{refused}");
        }
    }

    #[test]
    fn a_metric_takes_the_prefix() {
        let mut spec: GdlScalerSpec = serde_json::from_value(serde_json::json!({
            "pool": "p", "segment": "s",
            "targetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "p-s"},
        }))
        .expect("a spec");
        assert_eq!(
            spec.metric("admission_admitted"),
            "dynamo_component_admission_admitted"
        );
        spec.metrics_prefix = Some(String::new());
        assert_eq!(spec.metric("admission_admitted"), "admission_admitted");
        spec.metrics_prefix = Some("api".into());
        assert_eq!(spec.metric("requests_total"), "api_requests_total");
        assert_eq!(spec.min_replicas(), 1);
        assert_eq!(spec.max_concurrent_drains(), 1);
        assert_eq!(spec.victim(), "fewestAdmitted");
        assert_eq!(spec.routing_grace(), Ok(Duration::from_secs(10)));
    }
}
