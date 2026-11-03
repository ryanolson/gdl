//! The pool controller's view of the Kubernetes API, as a trait:
//! [`KubePools`] is the real one, and the tests give a fake. Each read
//! returns only what the controller decides on.

use std::collections::BTreeSet;

use k8s_openapi::api::apps::v1::ReplicaSet;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{
    ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, PostParams,
    Preconditions, PropagationPolicy,
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
    /// The pool's DynamoGraphDeployment, whose components run some of
    /// its segments.
    DynamoGraphDeployment,
}

impl Kind {
    /// Every kind, in the order the controller applies them.
    pub const ALL: [Kind; 7] = [
        Kind::ConfigMap,
        Kind::PersistentVolumeClaim,
        Kind::Deployment,
        Kind::Service,
        Kind::GdlScaler,
        Kind::PodMonitor,
        Kind::DynamoGraphDeployment,
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
            Self::DynamoGraphDeployment => gdl_k8s::DGD_KIND,
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
            Self::DynamoGraphDeployment => ("nvidia.com", "v1beta1", "dynamographdeployments"),
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
    /// Whether [`MANAGER`] owns fields of it by an update, as a create
    /// leaves them: they are moved to its apply before the next one.
    pub created: bool,
    /// For a Deployment, its selector's labels, `key=value,...`.
    pub selector: Option<String>,
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
        let created = metadata["managedFields"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|entry| entry["manager"] == MANAGER && entry["operation"] == "Update");
        let selector = object["spec"]["selector"]["matchLabels"]
            .as_object()
            .map(|labels| {
                labels
                    .iter()
                    .map(|(key, value)| format!("{key}={}", value.as_str().unwrap_or_default()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .filter(|_| kind == Kind::Deployment);
        Some(Self {
            selector,
            created,
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

    /// Write `object` as its one owner, under [`MANAGER`]. With `live`, the
    /// object as read, by forced server-side apply, only to the object of
    /// its uid. Without, by a create, which an object of the name made
    /// since the read refuses (409): a child is never adopted. Either way
    /// the fields end owned by an apply of [`MANAGER`], so that a later
    /// apply removes a field the render no longer writes.
    fn apply(
        &self,
        object: &Value,
        live: Option<&Live>,
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

    /// The pods of `deployment` that have not ended (`Succeeded` or
    /// `Failed`): those its ReplicaSets control, each ReplicaSet one that
    /// `deployment` controls, by uid.
    fn pods(&self, deployment: &Live) -> impl Future<Output = Result<Vec<PodView>, String>> + Send;

    /// The pods of component `component` of the DGD `dgd` that have not
    /// ended, by the labels dynamo's operator puts on them: dynamo's
    /// controllers own them, not the pool.
    fn component_pods(
        &self,
        dgd: &str,
        component: &str,
    ) -> impl Future<Output = Result<Vec<PodView>, String>> + Send;

    /// Whether the cluster serves PodMonitors, as asked once when the
    /// operator started.
    fn monitors(&self) -> bool;

    /// Whether the cluster serves DynamoGraphDeployments, as asked once
    /// when the operator started.
    fn dgds(&self) -> bool;
}

/// [`Pools`] over the Kubernetes API, in one namespace.
#[derive(Clone)]
pub struct KubePools {
    client: Client,
    namespace: String,
    pools: Api<GdlPool>,
    pods: Api<Pod>,
    sets: Api<ReplicaSet>,
    recorder: Recorder,
    monitors: bool,
    dgds: bool,
}

impl KubePools {
    /// The pools `client` reaches in `namespace`. `monitors` and `dgds`
    /// say whether the cluster serves PodMonitors and
    /// DynamoGraphDeployments.
    pub fn new(client: Client, namespace: &str, monitors: bool, dgds: bool) -> Self {
        let reporter = Reporter {
            controller: crate::NAME.into(),
            instance: std::env::var("HOSTNAME").ok(),
        };
        Self {
            pools: Api::namespaced(client.clone(), namespace),
            pods: Api::namespaced(client.clone(), namespace),
            sets: Api::namespaced(client.clone(), namespace),
            recorder: Recorder::new(client.clone(), reporter),
            namespace: namespace.to_string(),
            monitors,
            dgds,
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
            .filter(|kind| self.dgds || *kind != Kind::DynamoGraphDeployment)
    }
}

fn live(kind: Kind, object: &DynamicObject) -> Result<Live, String> {
    let value = serde_json::to_value(object).map_err(|e| e.to_string())?;
    Live::of(kind, &value).ok_or_else(|| format!("a {} without a name or a uid", kind.name()))
}

impl KubePools {
    /// Move the fields [`MANAGER`] owns of the object `name` of uid `uid`
    /// by an update, as its create left them, to its apply, as `kubectl`
    /// moves a client-side apply's: a JSON patch of `managedFields`, only
    /// at the resourceVersion read. A write between refuses it, and it
    /// reads again, a few times: another controller may write the object
    /// at once, as a Deployment's writes its status.
    async fn own(&self, kind: Kind, name: &str, uid: &str) -> Result<DynamicObject, Refused> {
        let api = self.api(kind);
        let what = |e| Refused::of(&format!("{} `{name}`", kind.name()), e);
        let mut last = Refused::Stale(format!("{} `{name}` kept changing", kind.name()));
        for _ in 0..5 {
            let object = api.get(name).await.map_err(what)?;
            if object.metadata.uid.as_deref() != Some(uid) {
                return Err(Refused::Stale(format!(
                    "{} `{name}` was made again",
                    kind.name()
                )));
            }
            let mut fields = object.metadata.managed_fields.clone().unwrap_or_default();
            let mut moved = false;
            for entry in &mut fields {
                if entry.manager.as_deref() == Some(MANAGER)
                    && entry.operation.as_deref() == Some("Update")
                {
                    entry.operation = Some("Apply".into());
                    entry.time = None;
                    moved = true;
                }
            }
            if !moved {
                return Ok(object);
            }
            let version = object.metadata.resource_version.clone().unwrap_or_default();
            let patch: json_patch::Patch = serde_json::from_value(json!([
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "replace", "path": "/metadata/managedFields", "value": fields},
            ]))
            .map_err(|e| Refused::Failed(e.to_string()))?;
            match api
                .patch(name, &PatchParams::default(), &Patch::<()>::Json(patch))
                .await
                .map_err(what)
            {
                Ok(owned) => return Ok(owned),
                Err(Refused::Stale(why)) => last = Refused::Stale(why),
                Err(error) => return Err(error),
            }
        }
        Err(last)
    }
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

    async fn apply(&self, object: &Value, live: Option<&Live>) -> Result<Live, Refused> {
        let kind = object["kind"]
            .as_str()
            .and_then(Kind::of)
            .ok_or_else(|| Refused::Failed(format!("an object of kind {}", object["kind"])))?;
        let name = object["metadata"]["name"].as_str().unwrap_or_default();
        let what = |e| Refused::of(&format!("{} `{name}`", kind.name()), e);
        let Some(live) = live else {
            let made: DynamicObject = serde_json::from_value(object.clone())
                .map_err(|e| Refused::Failed(e.to_string()))?;
            let params = PostParams {
                field_manager: Some(MANAGER.into()),
                ..PostParams::default()
            };
            let made = self.api(kind).create(&params, &made).await.map_err(what)?;
            let uid = made.metadata.uid.clone().unwrap_or_default();
            let owned = self.own(kind, name, &uid).await?;
            return self::live(kind, &owned).map_err(Refused::Failed);
        };
        if live.created {
            self.own(kind, name, &live.uid).await?;
        }
        let mut object = object.clone();
        object["metadata"]["uid"] = live.uid.clone().into();
        let written = self
            .api(kind)
            .patch(
                name,
                &PatchParams::apply(MANAGER).force(),
                &Patch::Apply(object),
            )
            .await
            .map_err(what)?;
        self::live(kind, &written).map_err(Refused::Failed)
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

    async fn pods(&self, deployment: &Live) -> Result<Vec<PodView>, String> {
        let Some(selector) = &deployment.selector else {
            return Ok(Vec::new());
        };
        let listed = ListParams::default().labels(selector);
        let controller = |metadata: &ObjectMeta| {
            metadata
                .owner_references
                .iter()
                .flatten()
                .find(|owner| owner.controller == Some(true))
                .map(|owner| owner.uid.clone())
        };
        let sets: BTreeSet<String> = self
            .sets
            .list(&listed)
            .await
            .map_err(|e| format!("ReplicaSets of `{}`: {e}", deployment.name))?
            .items
            .iter()
            .filter(|set| controller(&set.metadata).as_deref() == Some(deployment.uid.as_str()))
            .filter_map(|set| set.metadata.uid.clone())
            .collect();
        let pods = self
            .pods
            .list(&listed)
            .await
            .map_err(|e| format!("pods of `{}`: {e}", deployment.name))?;
        Ok(pods
            .items
            .iter()
            .filter(|pod| controller(&pod.metadata).is_some_and(|uid| sets.contains(&uid)))
            // A pod that has ended runs nothing, though its object stays;
            // one being deleted runs until its phase says it ended.
            .filter(|pod| {
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
                !matches!(phase, Some("Succeeded" | "Failed"))
            })
            .filter_map(PodView::of)
            .collect())
    }

    async fn component_pods(&self, dgd: &str, component: &str) -> Result<Vec<PodView>, String> {
        let selector = format!(
            "{}={dgd},{}={component}",
            gdl_k8s::DGD_LABEL,
            gdl_k8s::COMPONENT_LABEL
        );
        let pods = self
            .pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| format!("pods of component `{component}` of DGD `{dgd}`: {e}"))?;
        Ok(pods
            .items
            .iter()
            .filter(|pod| {
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
                !matches!(phase, Some("Succeeded" | "Failed"))
            })
            .filter_map(PodView::of)
            .collect())
    }

    fn monitors(&self) -> bool {
        self.monitors
    }

    fn dgds(&self) -> bool {
        self.dgds
    }
}

#[cfg(test)]
#[path = "api/tests.rs"]
mod tests;
