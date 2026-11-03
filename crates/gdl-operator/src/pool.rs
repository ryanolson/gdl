//! The `GdlPool` custom resource: one per pool, in its cluster's
//! namespace. It owns the pool's objects, which the operator renders from
//! its spec with `gdl-k8s`, and its finalizer stops the pool in order
//! before garbage collection deletes them. See `docs/src/guides/operator.md`.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

mod api;
mod apply;
mod hub;
mod reconcile;
mod stop;
#[cfg(test)]
mod tests;

pub(crate) use api::KubePools;
pub(crate) use reconcile::{Context, reconcile};

/// The finalizer by which a pool's stop runs before its objects go.
pub const FINALIZER: &str = "gdl.dev/stop";

/// What a `GdlPool` asks for: one pool of a fleet, every value resolved,
/// as `cargo gdl k8s` renders it.
#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "gdl.dev",
    version = "v1alpha1",
    kind = "GdlPool",
    namespaced,
    status = "GdlPoolStatus",
    shortname = "gpool",
    printcolumn = r#"{"name": "Class", "type": "string", "jsonPath": ".spec.class"}"#,
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
pub struct GdlPoolSpec {
    /// The pool.
    #[serde(flatten)]
    pub pool: gdl_k8s::Pool,
}

/// What the operator observed of a pool and did to it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdlPoolStatus {
    /// The generation of the spec it last acted on: applied, or held.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Where the pool is, from its hub's health and its stop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,
    /// Each segment's usable instances and quorum, as its hub counts them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub segments: BTreeMap<String, SegmentStatus>,
    /// The objects the pool renders that do not exist, each as
    /// `<Kind>/<name>`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<String>,
    /// Whether the pool's hub says its drained record is withdrawn.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub withdrawn: bool,
    /// The presence the pool's hub last said it stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presence: Option<gdl_k8s::Presence>,
    /// The generation held because it would replace the pool's hub: none
    /// of it was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held: Option<i64>,
    /// The pool's stop, once it is deleted: the step it is at, written
    /// before the step is taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop: Option<Stop>,
    /// `Applied`, `Healthy`, `Draining`, `Withdrawn`, `HubGone`,
    /// `PodMonitor`, `Dgd`, `PoolRestartPending`, `ForegroundDelete`, and
    /// `Adopted` when a child is not the pool's to change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Where a pool is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Phase {
    /// Its objects are being made, or its hub has not answered yet.
    Pending,
    /// Its hub says it is healthy.
    Ready,
    /// Its hub says a segment is below its quorum, or a class it requires
    /// is missing, but it serves.
    Degraded,
    /// Its hub says it cannot serve.
    Unavailable,
    /// It is drained before it stops.
    Draining,
    /// Its segments, then its hub, are being stopped.
    Stopping,
    /// Its hub says its record is withdrawn.
    Withdrawn,
}

/// One segment of a pool, as its hub counts it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SegmentStatus {
    /// Its instances that are members and ready.
    pub usable: u32,
    /// How many the pool needs ready.
    pub quorum: u32,
}

/// A pool's stop, as its status stores it before each step: a restart
/// takes the step written, or abandons it when an object it names is no
/// longer the one written, and never guesses from what it sees.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Stop {
    /// The step.
    pub step: StopStep,
    /// When the stop began, RFC 3339: the drain's bound counts from here.
    pub since: String,
    /// When the step was written, RFC 3339.
    pub at: String,
    /// The objects the step acts on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub objects: Vec<Object>,
}

/// A step of a pool's stop, in order. A top pool, which a hub refuses to
/// drain, and a pool whose hub is gone, start at `segments`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum StopStep {
    /// The hub is asked to drain the pool, and its record to be withdrawn.
    Drain,
    /// Each segment's scaler is suspended at 0, until no segment pod runs.
    Segments,
    /// The hub's Deployment is scaled to 0, until its pod is gone.
    Hub,
    /// No pod of the pool runs: the finalizer is removed.
    Release,
}

/// An object a step acts on, by its uid: one made again under the same
/// name is another object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Object {
    /// Its kind: `Deployment` or `GdlScaler`.
    pub kind: String,
    /// Its name.
    pub name: String,
    /// Its uid.
    pub uid: String,
}
