//! The scaler's view of the Kubernetes API, as a trait: [`KubeCluster`]
//! is the real one, and the tests give a fake that plays a Deployment and
//! its ReplicaSet, or a DGD component's scaling adapter and its
//! Deployment. Each read returns only what the scaler decides on, so a
//! fake needs no Kubernetes types.

use std::collections::BTreeSet;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{
    ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, Preconditions,
};
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Api, Client, Resource};
use serde_json::{Value, json};

use crate::{DELETION_COST, GdlScaler, GdlScalerStatus, ROUTABLE, SCALER_MANAGER, TargetRef};

/// A target, as the scaler reads it: a Deployment, or a DGD component's
/// scaling adapter with the Deployment that runs its pods.
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

pub use gdl_k8s::{ADAPTER_API_VERSION, ADAPTER_KIND};

/// What a scaler can target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetKind {
    /// An `apps/v1` Deployment: the scaler writes its `replicas`.
    Deployment,
    /// A DGD component's scaling adapter: the scaler writes its `/scale`,
    /// and dynamo's operator carries the count to the component's
    /// Deployment.
    Adapter,
}

impl TargetKind {
    /// The kind `target` names, or `None` for one the scaler cannot scale.
    pub fn of(target: &TargetRef) -> Option<Self> {
        match (target.api_version.as_str(), target.kind.as_str()) {
            ("apps/v1", "Deployment") => Some(Self::Deployment),
            (ADAPTER_API_VERSION, ADAPTER_KIND) => Some(Self::Adapter),
            _ => None,
        }
    }

    /// Its name, as an object's `kind`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Deployment => "Deployment",
            Self::Adapter => ADAPTER_KIND,
        }
    }
}

impl Target {
    /// The view of a DGD component through its scaling adapter `adapter`,
    /// as JSON, whose pods `deployment` runs. The count and the fence are
    /// the adapter's: the scaler writes the adapter alone. The pods and the
    /// rollout are the Deployment's. A Deployment whose count is not the
    /// adapter's yet is rolling: dynamo's operator has not carried the
    /// last count to it, so the pods do not show it.
    pub fn of_adapter(adapter: &Value, deployment: &Deployment) -> Result<Self, String> {
        let name = adapter["metadata"]["name"].as_str().unwrap_or_default();
        let replicas = adapter["spec"]["replicas"]
            .as_i64()
            .and_then(|r| i32::try_from(r).ok())
            .ok_or_else(|| format!("{ADAPTER_KIND} `{name}` has no `spec.replicas`"))?;
        let text = |field: &str| {
            adapter["metadata"][field]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("{ADAPTER_KIND} `{name}` has no `{field}`"))
        };
        let pods = Self::of(deployment).ok_or_else(|| {
            format!(
                "the Deployment of {ADAPTER_KIND} `{name}` selects its pods by more than labels"
            )
        })?;
        let caught_up = deployment.spec.as_ref().and_then(|s| s.replicas) == Some(replicas);
        Ok(Self {
            fence: Fence {
                uid: text("uid")?,
                resource_version: text("resourceVersion")?,
            },
            replicas,
            selector: pods.selector,
            rolling: pods.rolling || !caught_up,
        })
    }
}

/// The DGD and the component that scaling adapter `adapter`, as JSON,
/// scales, from its `spec.dgdRef`.
pub fn component_of(adapter: &Value) -> Result<(String, String), String> {
    let name = adapter["metadata"]["name"].as_str().unwrap_or_default();
    let field = |key: &str| {
        adapter["spec"]["dgdRef"][key]
            .as_str()
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("{ADAPTER_KIND} `{name}` has no `spec.dgdRef.{key}`"))
    };
    Ok((field("name")?, field("componentName")?))
}

/// The one Deployment among `found` that runs component `component` of
/// DGD `dgd`: found by the labels dynamo's operator puts on it, not by its
/// name, which is dynamo's convention and no contract. Refused unless
/// there is exactly one.
pub fn one_deployment(
    found: Vec<Deployment>,
    dgd: &str,
    component: &str,
) -> Result<Deployment, String> {
    let count = found.len();
    let mut found = found.into_iter();
    match (found.next(), count) {
        (Some(deployment), 1) => Ok(deployment),
        _ => Err(format!(
            "{count} Deployments carry the labels of component `{component}` of DGD `{dgd}`, and the scaler needs exactly one"
        )),
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
    /// `error`, from a write to `what`.
    pub(crate) fn of(what: &str, error: kube::Error) -> Self {
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
    /// Its resourceVersion when read: a write to it carries this.
    pub resource_version: String,
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
    /// Its containers' restart counts, summed: a restart starts a new
    /// process in the same pod.
    pub restarts: i64,
    /// Its containers' ids, sorted, of those that have one: a container
    /// made again has another, even where its restart count starts over
    /// at 0, as after its node restarts.
    pub containers: Vec<String>,
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
            resource_version: pod.metadata.resource_version.clone().unwrap_or_default(),
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
            restarts: status
                .container_statuses
                .iter()
                .flatten()
                .map(|container| i64::from(container.restart_count))
                .sum(),
            containers: {
                let mut ids: Vec<String> = status
                    .container_statuses
                    .iter()
                    .flatten()
                    .filter_map(|container| container.container_id.clone())
                    .filter(|id| !id.is_empty())
                    .collect();
                ids.sort();
                ids
            },
        })
    }
}

/// What the scaler reads and writes in its namespace.
pub trait Cluster {
    /// The object `target` names, or `None` when there is none.
    fn target(
        &self,
        target: &TargetRef,
    ) -> impl Future<Output = Result<Option<Target>, String>> + Send;

    /// The pods `selector` selects.
    fn pods(&self, selector: &str) -> impl Future<Output = Result<Vec<PodView>, String>> + Send;

    /// The uids of the pods that Service `service`'s EndpointSlices list,
    /// in any condition: a pod leaves them only once its labels no longer
    /// match the Service's selector, or it is gone.
    fn endpoints(
        &self,
        service: &str,
    ) -> impl Future<Output = Result<BTreeSet<String>, String>> + Send;

    /// Set the count of the object `target` names, as its one owner, only
    /// if it is still as `fence` read it: its resourceVersion after.
    fn scale(
        &self,
        target: &TargetRef,
        replicas: i32,
        fence: &Fence,
    ) -> impl Future<Output = Result<String, Refused>> + Send;

    /// Set `pod`'s [`ROUTABLE`] label, only on the pod of its uid and
    /// only if it is still at `version`: its resourceVersion after.
    fn routable(
        &self,
        pod: &PodView,
        version: &str,
        routable: bool,
    ) -> impl Future<Output = Result<String, Refused>> + Send;

    /// Set `pod`'s deletion cost, or remove it, as
    /// [`routable`](Cluster::routable) writes.
    fn cost(
        &self,
        pod: &PodView,
        version: &str,
        cost: Option<i32>,
    ) -> impl Future<Output = Result<String, Refused>> + Send;

    /// Delete `pod`, only the pod of its uid and only if it is still at
    /// `version`. Request mode only: a retired victim the scale-down no
    /// longer removes.
    fn delete(
        &self,
        pod: &PodView,
        version: &str,
    ) -> impl Future<Output = Result<(), Refused>> + Send;

    /// Record a warning event on `scaler`, best effort.
    fn warn(&self, scaler: &GdlScaler, reason: &str, note: &str)
    -> impl Future<Output = ()> + Send;

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

/// The resource of dynamo's DGD scaling adapters.
fn adapter_resource() -> ApiResource {
    ApiResource {
        group: "nvidia.com".into(),
        version: "v1beta1".into(),
        api_version: ADAPTER_API_VERSION.into(),
        kind: ADAPTER_KIND.into(),
        plural: "dynamographdeploymentscalingadapters".into(),
    }
}

/// [`Cluster`] over the Kubernetes API, in one namespace.
#[derive(Clone)]
pub struct KubeCluster {
    deployments: Api<Deployment>,
    adapters: Api<DynamicObject>,
    pods: Api<Pod>,
    slices: Api<EndpointSlice>,
    scalers: Api<GdlScaler>,
    recorder: Recorder,
}

impl KubeCluster {
    /// The cluster `client` reaches, in `namespace`.
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            deployments: Api::namespaced(client.clone(), namespace),
            adapters: Api::namespaced_with(client.clone(), namespace, &adapter_resource()),
            pods: Api::namespaced(client.clone(), namespace),
            slices: Api::namespaced(client.clone(), namespace),
            scalers: Api::namespaced(client.clone(), namespace),
            recorder: Recorder::new(
                client,
                Reporter {
                    controller: crate::NAME.into(),
                    instance: std::env::var("HOSTNAME").ok(),
                },
            ),
        }
    }

    /// Merge `metadata` into `pod`'s, carrying its uid and `version`, so
    /// that a pod of the same name made since, or the pod changed since it
    /// was read, refuses it rather than being changed: another writer's
    /// label or cost is never undone by a stale one.
    async fn patch_pod(
        &self,
        pod: &PodView,
        version: &str,
        mut metadata: serde_json::Value,
    ) -> Result<String, Refused> {
        metadata["uid"] = pod.uid.clone().into();
        metadata["resourceVersion"] = version.into();
        let written = self
            .pods
            .patch(
                &pod.name,
                &PatchParams::default(),
                &Patch::Merge(json!({"metadata": metadata})),
            )
            .await
            .map_err(|e| Refused::of(&format!("pod `{}`", pod.name), e))?;
        Ok(written.metadata.resource_version.unwrap_or_default())
    }
}

impl Cluster for KubeCluster {
    async fn target(&self, target: &TargetRef) -> Result<Option<Target>, String> {
        let name = &target.name;
        match TargetKind::of(target) {
            Some(TargetKind::Deployment) => {
                let deployment = self
                    .deployments
                    .get_opt(name)
                    .await
                    .map_err(|e| format!("Deployment `{name}`: {e}"))?;
                match deployment {
                    None => Ok(None),
                    Some(deployment) => Target::of(&deployment).map(Some).ok_or_else(|| {
                        format!("Deployment `{name}` selects its pods by more than labels")
                    }),
                }
            }
            Some(TargetKind::Adapter) => {
                let Some(adapter) = self
                    .adapters
                    .get_opt(name)
                    .await
                    .map_err(|e| format!("{ADAPTER_KIND} `{name}`: {e}"))?
                else {
                    return Ok(None);
                };
                let adapter = serde_json::to_value(&adapter).map_err(|e| e.to_string())?;
                let (dgd, component) = component_of(&adapter)?;
                let selector = format!(
                    "{}={dgd},{}={component}",
                    gdl_k8s::DGD_LABEL,
                    gdl_k8s::COMPONENT_LABEL
                );
                let found = self
                    .deployments
                    .list(&ListParams::default().labels(&selector))
                    .await
                    .map_err(|e| format!("Deployments `{selector}`: {e}"))?;
                let deployment = one_deployment(found.items, &dgd, &component)?;
                Target::of_adapter(&adapter, &deployment).map(Some)
            }
            None => Err(format!(
                "a {} of {} is not a target",
                target.kind, target.api_version
            )),
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

    async fn scale(
        &self,
        target: &TargetRef,
        replicas: i32,
        fence: &Fence,
    ) -> Result<String, Refused> {
        let name = target.name.as_str();
        if TargetKind::of(target) == Some(TargetKind::Adapter) {
            // A merge patch of the adapter's `/scale`, as an HPA writes it.
            // Its resourceVersion makes it a write of what the decision
            // read: an adapter changed since refuses it (409), and one made
            // again has another resourceVersion too.
            let patch = json!({
                "metadata": {"resourceVersion": fence.resource_version},
                "spec": {"replicas": replicas},
            });
            let written = self
                .adapters
                .patch_scale(name, &PatchParams::default(), &Patch::Merge(patch))
                .await
                .map_err(|e| Refused::of(&format!("{ADAPTER_KIND} `{name}`"), e))?;
            return Ok(written.metadata.resource_version.unwrap_or_default());
        }
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

    async fn routable(
        &self,
        pod: &PodView,
        version: &str,
        routable: bool,
    ) -> Result<String, Refused> {
        let labels = json!({"labels": {ROUTABLE: routable.to_string()}});
        self.patch_pod(pod, version, labels).await
    }

    async fn cost(
        &self,
        pod: &PodView,
        version: &str,
        cost: Option<i32>,
    ) -> Result<String, Refused> {
        let value = cost.map(|cost| cost.to_string());
        let annotations = json!({"annotations": {DELETION_COST: value}});
        self.patch_pod(pod, version, annotations).await
    }

    async fn delete(&self, pod: &PodView, version: &str) -> Result<(), Refused> {
        // Its uid and resourceVersion make it a delete of what the
        // decision read: a pod made again, or changed since, refuses it.
        let params = DeleteParams {
            preconditions: Some(Preconditions {
                uid: Some(pod.uid.clone()),
                resource_version: Some(version.to_string()),
            }),
            ..DeleteParams::default()
        };
        self.pods
            .delete(&pod.name, &params)
            .await
            .map(|_| ())
            .map_err(|e| Refused::of(&format!("pod `{}`", pod.name), e))
    }

    async fn warn(&self, scaler: &GdlScaler, reason: &str, note: &str) {
        let event = Event {
            type_: EventType::Warning,
            reason: reason.into(),
            note: Some(note.into()),
            action: "Reconcile".into(),
            secondary: None,
        };
        if let Err(error) = self.recorder.publish(&event, &scaler.object_ref(&())).await {
            tracing::warn!(%reason, %error, "an event was not recorded");
        }
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
            // A merge patch merges an object key by key: a removal
            // without a handoff would keep the last one's.
            if let Some(last) = fields.get_mut("last").and_then(Value::as_object_mut) {
                last.entry("handoff").or_insert(Value::Null);
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
