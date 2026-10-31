//! The scaler's view of the Kubernetes API, as a trait: [`KubeCluster`]
//! is the real one, and the tests give a fake that plays a Deployment and
//! its ReplicaSet. Each read returns only what the scaler decides on, so a
//! fake needs no Kubernetes types.

use std::collections::BTreeSet;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{ListParams, Patch, PatchParams};
use kube::{Api, Client};
use serde_json::json;

use crate::{DELETION_COST, GdlScaler, GdlScalerStatus, ROUTABLE, SCALER_MANAGER};

/// A target Deployment, as the scaler reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// Its uid and its resourceVersion when read: every write the scaler
    /// decides on from this read carries them.
    pub fence: Fence,
    /// Its `spec.replicas`, which the scaler owns.
    pub replicas: i32,
    /// Its selector's labels as a label selector, `key=value,...`.
    pub selector: String,
    /// Whether a rollout is in progress: the Deployment controller has
    /// not seen its latest spec, or old pods remain.
    pub rolling: bool,
}

impl Target {
    /// The view of `deployment`. `None` for one whose selector is not
    /// labels alone, which the scaler cannot list pods by as text.
    pub fn of(deployment: &Deployment) -> Option<Self> {
        let spec = deployment.spec.as_ref()?;
        if spec
            .selector
            .match_expressions
            .as_ref()
            .is_some_and(|e| !e.is_empty())
        {
            return None;
        }
        let selector = spec
            .selector
            .match_labels
            .as_ref()?
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",");
        let replicas = spec.replicas.unwrap_or(1);
        let status = deployment.status.clone().unwrap_or_default();
        let generation = deployment.metadata.generation.unwrap_or(0);
        let rolling = status.observed_generation.unwrap_or(0) < generation
            || status.updated_replicas.unwrap_or(0) < replicas
            || status.replicas.unwrap_or(0) > status.updated_replicas.unwrap_or(0);
        Some(Self {
            fence: Fence {
                uid: deployment.metadata.uid.clone()?,
                resource_version: deployment.metadata.resource_version.clone()?,
            },
            replicas,
            selector,
            rolling,
        })
    }
}

/// The read of an object a write was decided on: the API server refuses
/// the write if the object was made again (another uid) or changed since
/// (another resourceVersion).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fence {
    /// The object's uid.
    pub uid: String,
    /// Its resourceVersion.
    pub resource_version: String,
}

/// Why a write did not land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// The view it was decided on is out of date: the object changed (409),
    /// was made again (422 on its uid), or is gone (404). The reconcile
    /// stops, and the next one reads again and decides again.
    Stale(String),
    /// Anything else.
    Failed(String),
}

impl Refused {
    fn of(what: &str, error: kube::Error) -> Self {
        match &error {
            kube::Error::Api(status) if [404, 409, 422].contains(&status.code) => {
                Self::Stale(format!("{what}: {error}"))
            }
            _ => Self::Failed(format!("{what}: {error}")),
        }
    }
}

/// A pod of a target, as the scaler reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodView {
    /// Its name.
    pub name: String,
    /// Its uid.
    pub uid: String,
    /// Its IP, once it has one.
    pub ip: Option<String>,
    /// When it was created, in milliseconds since the Unix epoch.
    pub created_ms: i64,
    /// Whether its phase is `Running` and its `Ready` condition is true.
    pub ready: bool,
    /// Whether it is being deleted.
    pub terminating: bool,
    /// Its [`ROUTABLE`] label: `None` without one.
    pub routable: Option<bool>,
    /// The port of its container port named `system`, where it serves
    /// `/metrics`.
    pub system_port: Option<u16>,
}

impl PodView {
    /// The view of `pod`. `None` for one without a name or a uid.
    pub fn of(pod: &Pod) -> Option<Self> {
        let status = pod.status.clone().unwrap_or_default();
        let ready = status.phase.as_deref() == Some("Running")
            && status
                .conditions
                .iter()
                .flatten()
                .any(|c| c.type_ == "Ready" && c.status == "True");
        let system_port = pod
            .spec
            .iter()
            .flat_map(|spec| &spec.containers)
            .flat_map(|container| container.ports.iter().flatten())
            .find(|port| port.name.as_deref() == Some("system"))
            .and_then(|port| u16::try_from(port.container_port).ok());
        Some(Self {
            name: pod.metadata.name.clone()?,
            uid: pod.metadata.uid.clone()?,
            ip: status.pod_ip.filter(|ip| !ip.is_empty()),
            created_ms: pod
                .metadata
                .creation_timestamp
                .as_ref()
                .map_or(0, |time| time.0.as_millisecond()),
            ready,
            terminating: pod.metadata.deletion_timestamp.is_some(),
            routable: pod
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(ROUTABLE))
                .map(|value| value == "true"),
            system_port,
        })
    }
}

/// What the scaler reads and writes in its namespace.
pub trait Cluster {
    /// The Deployment `name`, or `None` when there is none.
    fn target(&self, name: &str) -> impl Future<Output = Result<Option<Target>, String>> + Send;

    /// The pods `selector` selects.
    fn pods(&self, selector: &str) -> impl Future<Output = Result<Vec<PodView>, String>> + Send;

    /// The uids of the pods that Service `service`'s EndpointSlices list,
    /// in any condition: a pod leaves them only once its labels no longer
    /// match the Service's selector, or it is gone.
    fn endpoints(
        &self,
        service: &str,
    ) -> impl Future<Output = Result<BTreeSet<String>, String>> + Send;

    /// Set the Deployment `name`'s `replicas`, as its one owner, only if
    /// it is still as `fence` read it: its resourceVersion after.
    fn scale(
        &self,
        name: &str,
        replicas: i32,
        fence: &Fence,
    ) -> impl Future<Output = Result<String, Refused>> + Send;

    /// Set `pod`'s [`ROUTABLE`] label, only on the pod of its uid.
    fn routable(
        &self,
        pod: &PodView,
        routable: bool,
    ) -> impl Future<Output = Result<(), Refused>> + Send;

    /// Set `pod`'s deletion cost, or remove it, only on the pod of its uid.
    fn cost(
        &self,
        pod: &PodView,
        cost: Option<i32>,
    ) -> impl Future<Output = Result<(), Refused>> + Send;

    /// Write the status of the scaler `name`, only if it is still at
    /// `version` when one is given: its resourceVersion after, when the
    /// answer has one.
    fn status(
        &self,
        name: &str,
        status: &GdlScalerStatus,
        version: Option<&str>,
    ) -> impl Future<Output = Result<Option<String>, Refused>> + Send;
}

/// [`Cluster`] over the Kubernetes API, in one namespace.
#[derive(Clone)]
pub struct KubeCluster {
    deployments: Api<Deployment>,
    pods: Api<Pod>,
    slices: Api<EndpointSlice>,
    scalers: Api<GdlScaler>,
}

impl KubeCluster {
    /// The cluster `client` reaches, in `namespace`.
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            deployments: Api::namespaced(client.clone(), namespace),
            pods: Api::namespaced(client.clone(), namespace),
            slices: Api::namespaced(client.clone(), namespace),
            scalers: Api::namespaced(client, namespace),
        }
    }

    /// Merge `metadata` into `pod`'s, carrying its uid, so that a pod of
    /// the same name made since is refused rather than changed.
    async fn patch_pod(
        &self,
        pod: &PodView,
        mut metadata: serde_json::Value,
    ) -> Result<(), Refused> {
        metadata["uid"] = pod.uid.clone().into();
        self.pods
            .patch(
                &pod.name,
                &PatchParams::default(),
                &Patch::Merge(json!({"metadata": metadata})),
            )
            .await
            .map(drop)
            .map_err(|e| Refused::of(&format!("pod `{}`", pod.name), e))
    }
}

impl Cluster for KubeCluster {
    async fn target(&self, name: &str) -> Result<Option<Target>, String> {
        let deployment = self
            .deployments
            .get_opt(name)
            .await
            .map_err(|e| format!("Deployment `{name}`: {e}"))?;
        match deployment {
            None => Ok(None),
            Some(deployment) => Target::of(&deployment)
                .map(Some)
                .ok_or_else(|| format!("Deployment `{name}` selects its pods by more than labels")),
        }
    }

    async fn pods(&self, selector: &str) -> Result<Vec<PodView>, String> {
        let pods = self
            .pods
            .list(&ListParams::default().labels(selector))
            .await
            .map_err(|e| format!("pods `{selector}`: {e}"))?;
        Ok(pods.items.iter().filter_map(PodView::of).collect())
    }

    async fn endpoints(&self, service: &str) -> Result<BTreeSet<String>, String> {
        let selector = format!("kubernetes.io/service-name={service}");
        let slices = self
            .slices
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| format!("EndpointSlices of `{service}`: {e}"))?;
        Ok(slices
            .items
            .iter()
            .flat_map(|slice| &slice.endpoints)
            .filter_map(|endpoint| endpoint.target_ref.as_ref()?.uid.clone())
            .collect())
    }

    async fn scale(&self, name: &str, replicas: i32, fence: &Fence) -> Result<String, Refused> {
        // A forced apply of the one field: the scaler is its designated
        // owner, so force is its normal mode and it meets no field
        // conflict, and it changes no other field. Its uid and
        // resourceVersion make it a write of what the decision read: a
        // Deployment made again or changed since refuses it.
        let patch = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": name,
                "uid": fence.uid,
                "resourceVersion": fence.resource_version,
            },
            "spec": {"replicas": replicas},
        });
        let written = self
            .deployments
            .patch(
                name,
                &PatchParams::apply(SCALER_MANAGER).force(),
                &Patch::Apply(patch),
            )
            .await
            .map_err(|e| Refused::of(&format!("Deployment `{name}`"), e))?;
        Ok(written.metadata.resource_version.unwrap_or_default())
    }

    async fn routable(&self, pod: &PodView, routable: bool) -> Result<(), Refused> {
        self.patch_pod(pod, json!({"labels": {ROUTABLE: routable.to_string()}}))
            .await
    }

    async fn cost(&self, pod: &PodView, cost: Option<i32>) -> Result<(), Refused> {
        let value = cost.map(|cost| cost.to_string());
        self.patch_pod(pod, json!({"annotations": {DELETION_COST: value}}))
            .await
    }

    async fn status(
        &self,
        name: &str,
        status: &GdlScalerStatus,
        version: Option<&str>,
    ) -> Result<Option<String>, Refused> {
        // A merge patch replaces a list whole, and removes a field set to
        // null: the encoding leaves out an empty list and an absent
        // field, which the patch would then keep as they were, so each is
        // named.
        let mut value = serde_json::to_value(status).map_err(|e| Refused::Failed(e.to_string()))?;
        if let Some(fields) = value.as_object_mut() {
            for list in ["retiring", "conditions"] {
                fields.entry(list).or_insert_with(|| json!([]));
            }
            for field in ["last", "observedGeneration"] {
                fields.entry(field).or_insert(serde_json::Value::Null);
            }
        }
        // With a resourceVersion, the merge patch is refused if another
        // write came between the read and this one.
        let mut patch = json!({"status": value});
        if let Some(version) = version {
            patch["metadata"] = json!({"resourceVersion": version});
        }
        let written = self
            .scalers
            .patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(|e| Refused::of(&format!("GdlScaler `{name}` status"), e))?;
        Ok(written.metadata.resource_version)
    }
}

#[cfg(test)]
mod tests;
