//! The pool controller's view of the Kubernetes API, as a trait:
//! [`KubePools`] is the real one, and the tests give a fake. Each read
//! returns only what the controller decides on.

use k8s_openapi::api::core::v1::Pod;
use kube::api::{
    ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, Preconditions,
    PropagationPolicy,
};
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Api, Client, Resource, ResourceExt};
use serde_json::{Value, json};

use crate::cluster::{PodView, Refused};
use crate::{GdlPool, GdlPoolStatus};

/// The field manager of every object the controller renders.
pub const MANAGER: &str = "gdl";

/// The annotation on a hub's Deployment that holds a hash of its pod
/// template as rendered, which the pool-restart guard compares.
pub const TEMPLATE: &str = "gdl.dev/template";

/// A kind of object a pool owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// The pool's settings.
    ConfigMap,
    /// A parent hub's claim.
    PersistentVolumeClaim,
    /// The hub's, and each segment's.
    Deployment,
    /// The hub's, and each entry Service.
    Service,
    /// Each segment's scaler.
    GdlScaler,
    /// The pool's PodMonitor.
    PodMonitor,
}

impl Kind {
    /// Every kind, in the order the controller applies them.
    pub const ALL: [Kind; 6] = [
        Kind::ConfigMap,
        Kind::PersistentVolumeClaim,
        Kind::Deployment,
        Kind::Service,
        Kind::GdlScaler,
        Kind::PodMonitor,
    ];

    /// The kind `kind`, as an object names it.
    pub fn of(kind: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.name() == kind)
    }

    /// Its name, as an object's `kind`.
    pub fn name(self) -> &'static str {
        match self {
            Self::ConfigMap => "ConfigMap",
            Self::PersistentVolumeClaim => "PersistentVolumeClaim",
            Self::Deployment => "Deployment",
            Self::Service => "Service",
            Self::GdlScaler => "GdlScaler",
            Self::PodMonitor => "PodMonitor",
        }
    }

    fn resource(self) -> ApiResource {
        let (group, version, plural) = match self {
            Self::ConfigMap => ("", "v1", "configmaps"),
            Self::PersistentVolumeClaim => ("", "v1", "persistentvolumeclaims"),
            Self::Deployment => ("apps", "v1", "deployments"),
            Self::Service => ("", "v1", "services"),
            Self::GdlScaler => ("gdl.dev", "v1alpha1", "gdlscalers"),
            Self::PodMonitor => ("monitoring.coreos.com", "v1", "podmonitors"),
        };
        let api_version = match group {
            "" => version.to_string(),
            group => format!("{group}/{version}"),
        };
        ApiResource {
            group: group.into(),
            version: version.into(),
            api_version,
            kind: self.name().into(),
            plural: plural.into(),
        }
    }
}

/// An object a pool renders, as the controller reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Live {
    /// Its kind.
    pub kind: Kind,
    /// Its name.
    pub name: String,
    /// Its uid.
    pub uid: String,
    /// Its resourceVersion when read.
    pub version: String,
    /// The uid of its controller, when it has one.
    pub controller: Option<String>,
    /// Its [`TEMPLATE`] annotation.
    pub template: Option<String>,
    /// The field managers that own its `spec.replicas`.
    pub replica_owners: Vec<String>,
    /// For a `GdlScaler`, whether it is suspended.
    pub suspended: bool,
    /// For a Deployment, its `spec.replicas`.
    pub replicas: Option<i64>,
    /// For a Deployment, its pods' image.
    pub image: Option<String>,
    /// For a Deployment, its pods' `gdl.dev/config`: a checksum of what
    /// they read from the pool's ConfigMap.
    pub config: Option<String>,
    /// Whether it is being deleted.
    pub deleting: bool,
}

impl Live {
    /// The view of `object`, of kind `kind`.
    pub fn of(kind: Kind, object: &Value) -> Option<Self> {
        let metadata = &object["metadata"];
        let text = |value: &Value| value.as_str().map(str::to_string);
        let controller = metadata["ownerReferences"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|owner| owner["controller"] == true)
            .and_then(|owner| text(&owner["uid"]));
        let replica_owners = metadata["managedFields"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|entry| entry["fieldsV1"]["f:spec"].get("f:replicas").is_some())
            .filter_map(|entry| text(&entry["manager"]))
            .collect();
        Some(Self {
            kind,
            name: text(&metadata["name"])?,
            uid: text(&metadata["uid"])?,
            version: text(&metadata["resourceVersion"]).unwrap_or_default(),
            controller,
            template: text(&metadata["annotations"][TEMPLATE]),
            replica_owners,
            suspended: !object["spec"]["suspend"].is_null(),
            replicas: object["spec"]["replicas"].as_i64(),
            image: text(&object["spec"]["template"]["spec"]["containers"][0]["image"]),
            config: text(&object["spec"]["template"]["metadata"]["annotations"]["gdl.dev/config"]),
            deleting: !metadata["deletionTimestamp"].is_null(),
        })
    }
}

/// What the pool controller reads and writes in its namespace.
pub trait Pools {
    /// Write the status of pool `name`, only if it is still at `version`:
    /// its resourceVersion after.
    fn status(
        &self,
        name: &str,
        status: &GdlPoolStatus,
        version: &str,
    ) -> impl Future<Output = Result<String, Refused>> + Send;

    /// Set the finalizers of pool `name` to `finalizers`, only if it is
    /// still at `version`: its resourceVersion after.
    fn finalizers(
        &self,
        name: &str,
        finalizers: &[String],
        version: &str,
    ) -> impl Future<Output = Result<String, Refused>> + Send;

    /// Record an event on `pool`. A failure is logged, not returned: an
    /// event only says what the status already holds.
    fn event(
        &self,
        pool: &GdlPool,
        warning: bool,
        reason: &str,
        note: &str,
    ) -> impl Future<Output = ()> + Send;

    /// The objects of every kind that carry pool `pool`'s label, whoever
    /// controls them.
    fn labelled(&self, pool: &str) -> impl Future<Output = Result<Vec<Live>, String>> + Send;

    /// The object `name` of kind `kind`, or `None` when there is none.
    fn get(
        &self,
        kind: Kind,
        name: &str,
    ) -> impl Future<Output = Result<Option<Live>, String>> + Send;

    /// Apply `object` as its one owner, by forced server-side apply under
    /// [`MANAGER`], only to the object of uid `uid` when one is given.
    fn apply(
        &self,
        object: &Value,
        uid: Option<&str>,
    ) -> impl Future<Output = Result<Live, Refused>> + Send;

    /// Apply `spec` to the object `name` of kind `kind` under the field
    /// manager `manager`, only to the object of uid `uid`, and, when
    /// `version` is given, only if it is still at it. Forced when
    /// `force`.
    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        kind: Kind,
        name: &str,
        manager: &str,
        spec: Value,
        uid: &str,
        version: Option<&str>,
        force: bool,
    ) -> impl Future<Output = Result<(), Refused>> + Send;

    /// Delete the object `name` of kind `kind`, only the object of uid
    /// `uid`, with background propagation.
    fn delete(
        &self,
        kind: Kind,
        name: &str,
        uid: &str,
    ) -> impl Future<Output = Result<(), Refused>> + Send;

    /// The pods `selector` selects.
    fn pods(&self, selector: &str) -> impl Future<Output = Result<Vec<PodView>, String>> + Send;

    /// Whether the cluster serves PodMonitors, as asked once when the
    /// operator started.
    fn monitors(&self) -> bool;
}

/// [`Pools`] over the Kubernetes API, in one namespace.
#[derive(Clone)]
pub struct KubePools {
    client: Client,
    namespace: String,
    pools: Api<GdlPool>,
    pods: Api<Pod>,
    recorder: Recorder,
    monitors: bool,
}

impl KubePools {
    /// The pools `client` reaches in `namespace`. `monitors` says whether
    /// the cluster serves PodMonitors.
    pub fn new(client: Client, namespace: &str, monitors: bool) -> Self {
        let reporter = Reporter {
            controller: crate::NAME.into(),
            instance: std::env::var("HOSTNAME").ok(),
        };
        Self {
            pools: Api::namespaced(client.clone(), namespace),
            pods: Api::namespaced(client.clone(), namespace),
            recorder: Recorder::new(client.clone(), reporter),
            namespace: namespace.to_string(),
            monitors,
            client,
        }
    }

    fn api(&self, kind: Kind) -> Api<DynamicObject> {
        Api::namespaced_with(self.client.clone(), &self.namespace, &kind.resource())
    }

    fn kinds(&self) -> impl Iterator<Item = Kind> + '_ {
        Kind::ALL
            .into_iter()
            .filter(|kind| self.monitors || *kind != Kind::PodMonitor)
    }
}

fn live(kind: Kind, object: &DynamicObject) -> Result<Live, String> {
    let value = serde_json::to_value(object).map_err(|e| e.to_string())?;
    Live::of(kind, &value).ok_or_else(|| format!("a {} without a name or a uid", kind.name()))
}

impl Pools for KubePools {
    async fn status(
        &self,
        name: &str,
        status: &GdlPoolStatus,
        version: &str,
    ) -> Result<String, Refused> {
        // A merge patch replaces a list whole, and removes a field set to
        // null: the encoding leaves out an empty list or map and an absent
        // field, which the patch would keep as they were, so each is named.
        let mut value = serde_json::to_value(status).map_err(|e| Refused::Failed(e.to_string()))?;
        if let Some(fields) = value.as_object_mut() {
            for list in ["missing", "conditions"] {
                fields.entry(list).or_insert_with(|| json!([]));
            }
            fields.entry("segments").or_insert_with(|| json!(null));
            for field in [
                "observedGeneration",
                "phase",
                "held",
                "stop",
                "withdrawn",
                "presence",
            ] {
                fields.entry(field).or_insert(Value::Null);
            }
        }
        let patch = json!({"metadata": {"resourceVersion": version}, "status": value});
        let written = self
            .pools
            .patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(|e| Refused::of(&format!("GdlPool `{name}` status"), e))?;
        Ok(written.metadata.resource_version.unwrap_or_default())
    }

    async fn finalizers(
        &self,
        name: &str,
        finalizers: &[String],
        version: &str,
    ) -> Result<String, Refused> {
        let patch = json!({"metadata": {"resourceVersion": version, "finalizers": finalizers}});
        let written = self
            .pools
            .patch(name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(|e| Refused::of(&format!("GdlPool `{name}` finalizers"), e))?;
        Ok(written.metadata.resource_version.unwrap_or_default())
    }

    async fn event(&self, pool: &GdlPool, warning: bool, reason: &str, note: &str) {
        let event = Event {
            type_: if warning {
                EventType::Warning
            } else {
                EventType::Normal
            },
            reason: reason.into(),
            note: Some(note.into()),
            action: "Reconcile".into(),
            secondary: None,
        };
        if let Err(error) = self.recorder.publish(&event, &pool.object_ref(&())).await {
            tracing::warn!(pool = %pool.name_any(), %error, "an event was not recorded");
        }
    }

    async fn labelled(&self, pool: &str) -> Result<Vec<Live>, String> {
        let selector = format!("gdl.dev/pool={}", gdl_k8s::label_value(pool));
        let mut found = Vec::new();
        for kind in self.kinds() {
            let list = self
                .api(kind)
                .list(&ListParams::default().labels(&selector))
                .await
                .map_err(|e| format!("{}s of pool `{pool}`: {e}", kind.name()))?;
            for object in &list.items {
                found.push(live(kind, object)?);
            }
        }
        Ok(found)
    }

    async fn get(&self, kind: Kind, name: &str) -> Result<Option<Live>, String> {
        let object = self
            .api(kind)
            .get_opt(name)
            .await
            .map_err(|e| format!("{} `{name}`: {e}", kind.name()))?;
        object.as_ref().map(|object| live(kind, object)).transpose()
    }

    async fn apply(&self, object: &Value, uid: Option<&str>) -> Result<Live, Refused> {
        let kind = object["kind"]
            .as_str()
            .and_then(Kind::of)
            .ok_or_else(|| Refused::Failed(format!("an object of kind {}", object["kind"])))?;
        let name = object["metadata"]["name"].as_str().unwrap_or_default();
        let mut object = object.clone();
        if let Some(uid) = uid {
            object["metadata"]["uid"] = uid.into();
        }
        let written = self
            .api(kind)
            .patch(
                name,
                &PatchParams::apply(MANAGER).force(),
                &Patch::Apply(object),
            )
            .await
            .map_err(|e| Refused::of(&format!("{} `{name}`", kind.name()), e))?;
        live(kind, &written).map_err(Refused::Failed)
    }

    async fn write(
        &self,
        kind: Kind,
        name: &str,
        manager: &str,
        spec: Value,
        uid: &str,
        version: Option<&str>,
        force: bool,
    ) -> Result<(), Refused> {
        let resource = kind.resource();
        let mut metadata = json!({"name": name, "uid": uid});
        if let Some(version) = version {
            metadata["resourceVersion"] = version.into();
        }
        let patch = json!({
            "apiVersion": resource.api_version,
            "kind": resource.kind,
            "metadata": metadata,
            "spec": spec,
        });
        let mut params = PatchParams::apply(manager);
        params.force = force;
        self.api(kind)
            .patch(name, &params, &Patch::Apply(patch))
            .await
            .map_err(|e| Refused::of(&format!("{} `{name}`", kind.name()), e))?;
        Ok(())
    }

    async fn delete(&self, kind: Kind, name: &str, uid: &str) -> Result<(), Refused> {
        let params = DeleteParams {
            propagation_policy: Some(PropagationPolicy::Background),
            preconditions: Some(Preconditions {
                uid: Some(uid.into()),
                resource_version: None,
            }),
            ..DeleteParams::default()
        };
        match self.api(kind).delete(name, &params).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(status)) if status.code == 404 => Ok(()),
            Err(error) => Err(Refused::of(&format!("{} `{name}`", kind.name()), error)),
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

    fn monitors(&self) -> bool {
        self.monitors
    }
}

#[cfg(test)]
#[path = "api/tests.rs"]
mod tests;

/// The labels by which a pool's pods are listed: pool `pool`'s, of fleet
/// `fleet` in its cluster `cluster`, and, for `hub`, its hub's alone or
/// its segments' alone. The cluster keeps apart another cluster's pool of
/// the same name, where two clusters' contexts reach one namespace.
pub fn pod_selector(fleet: &str, cluster: &str, pool: &str, hub: Option<bool>) -> String {
    let mut selector: Vec<String> = [
        ("gdl.dev/fleet", fleet),
        ("gdl.dev/cluster", cluster),
        ("gdl.dev/pool", pool),
    ]
    .into_iter()
    .map(|(key, value)| format!("{key}={}", gdl_k8s::label_value(value)))
    .collect();
    match hub {
        Some(true) => selector.push("gdl.dev/segment=hub".into()),
        Some(false) => selector.push("gdl.dev/segment!=hub".into()),
        None => {}
    }
    selector.join(",")
}
