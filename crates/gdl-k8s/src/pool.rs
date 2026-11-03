//! A pool as its objects are rendered from it: every value resolved, so
//! that `cargo gdl k8s` on a laptop and the operator in a cluster render
//! the same objects from it alone. Nothing here is read from a fleet, a
//! spec, or a build. It is a `GdlPool`'s spec, so what only the operator
//! acts on, such as the pool's parent and its presence, is here too.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One pool of a fleet, in its cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    /// The fleet's name.
    pub fleet: String,
    /// The fleet's name for the cluster the pool runs in.
    pub cluster: String,
    /// The pool's class.
    pub class: String,
    /// The pool's region.
    pub region: String,
    /// The pool's layout as its hub admits it: the `DeploymentSpec` as
    /// JSON text, kept verbatim, since its checksum decides when pods are
    /// replaced.
    pub deployment: String,
    /// The pool's hub.
    pub hub: Hub,
    /// Each segment of the pool's class, in name order.
    pub segments: Vec<Segment>,
    /// The ServiceAccount every pod of the pool runs as: the fleet's.
    pub service_account_name: String,
    /// The node labels every pod of the pool must match: the cluster's
    /// platform's OS and architecture, and the cluster's own.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
    /// The taints every pod of the pool tolerates.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,
    /// The pool whose hub holds its record, or the class of its parents:
    /// absent for a top pool, whose hub refuses a drain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<Parent>,
    /// The DynamoDB tables the pool's hub publishes its record to: absent
    /// under the relay directory, where it reports to its parent's hub.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamodb: Option<Dynamodb>,
    /// Whether the pool has a PodMonitor of its pods, for a cluster that
    /// runs the Prometheus operator.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pod_monitor: bool,
    /// Whether the pool serves, or is scaled to 0 and says so in its
    /// record. Absent means `serving`, and `cargo gdl` writes it only
    /// when it is not, so that a redeploy leaves a person's change of it
    /// in place.
    #[serde(default, skip_serializing_if = "Presence::is_serving")]
    pub presence: Presence,
    /// Whether a change that replaces the pool's hub may be applied. A hub
    /// restart drains its pool, so without this the change is held, and
    /// the pool's status says so (`PoolRestartPending`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_pool_restart: bool,
    /// The DynamoGraphDeployment (DGD) that runs the segments placed in
    /// its components, as JSON text, kept verbatim: the pool owns it, and
    /// dynamo's operator runs its pods. Absent for a pool of gdl's
    /// Deployments alone. See `docs/src/guides/operator.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dgd: Option<String>,
}

/// A pool's parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Parent {
    /// The one pool whose hub holds the pool's record.
    Home(String),
    /// The class of the pools it serves, for a shared pool.
    Parents(String),
}

impl Dynamodb {
    /// The tables of `directory`, when it is DynamoDB.
    pub fn of(directory: &gdl_model::DirectoryBackend) -> Option<Self> {
        match directory {
            gdl_model::DirectoryBackend::DynamoDb {
                records_table,
                lists_table,
                aws_region,
                ..
            } => Some(Self {
                records_table: records_table.clone(),
                lists_table: lists_table.clone(),
                aws_region: aws_region.clone(),
            }),
            _ => None,
        }
    }
}

impl From<&gdl_model::Parent> for Parent {
    fn from(parent: &gdl_model::Parent) -> Self {
        match parent {
            gdl_model::Parent::Pool(home) => Self::Home(home.clone()),
            gdl_model::Parent::Class(class) => Self::Parents(class.clone()),
        }
    }
}

/// The DynamoDB tables a pool's hub publishes its record to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Dynamodb {
    /// The table of records.
    pub records_table: String,
    /// The table of lists.
    pub lists_table: String,
    /// The AWS region whose copy of the tables the hub reads and writes.
    pub aws_region: String,
}

/// Whether a pool serves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Presence {
    /// It serves.
    #[default]
    Serving,
    /// Its segments are held at 0, and its record says so.
    ScaledToZero,
}

impl Presence {
    /// Whether it is [`Presence::Serving`].
    pub fn is_serving(&self) -> bool {
        *self == Self::Serving
    }
}

/// A pool's hub.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Hub {
    /// Its image.
    pub image: String,
    /// Its variables, after the pod's IP, which every hub is given.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<Env>,
    /// Its requests and limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// Where it keeps its children's records, on a claim of its own, when
    /// it is a parent under the relay directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_dir: Option<String>,
}

/// A segment of a pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    /// Its name.
    pub name: String,
    /// Its image.
    pub image: String,
    /// Its settings file, TOML, kept verbatim. Its probes, its stop grace,
    /// its drain bound, and its metrics prefix are read from it.
    pub settings: String,
    /// How many instances the pool needs ready.
    pub quorum: u32,
    /// The container ports it publishes, which a Service of its own lists.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<Port>,
    /// Its variables.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<Env>,
    /// The spec's secrets it mounts, each from a Kubernetes Secret.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<Secret>,
    /// Its requests and limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// The component of the pool's DGD that runs it, when dynamo's
    /// operator runs its pods: it then has no Deployment, entry Service
    /// or scaler of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
}

/// A container's variable, as Kubernetes reads it: `$(NAME)` in a value
/// is another variable's, and `$$` is `$`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Env {
    /// Its name.
    pub name: String,
    /// Its value.
    pub value: String,
}

/// A container port that a segment publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Port {
    /// The port.
    #[schemars(range(min = 1))]
    pub port: u16,
    /// Its protocol.
    pub protocol: Protocol,
}

/// The protocol of a published port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum Protocol {
    /// TCP.
    Tcp,
    /// UDP.
    Udp,
    /// SCTP.
    Sctp,
}

impl Protocol {
    /// Its name as Kubernetes spells it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
            Self::Sctp => "SCTP",
        }
    }
}

/// One of the spec's secrets that a segment mounts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Secret {
    /// The spec's name for it: the file it is mounted as, and its key in
    /// the Kubernetes Secret.
    pub name: String,
    /// The Kubernetes Secret that holds it.
    pub holder: String,
}

/// A container's requests and limits, each a quantity by resource name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Resources {
    /// What the scheduler reserves for the container.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<String, String>,
    /// The most the container may use.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,
}

impl From<&gdl_model::Resources> for Resources {
    fn from(resources: &gdl_model::Resources) -> Self {
        let quantities = |q: &gdl_model::Quantities| {
            [("cpu", &q.cpu), ("memory", &q.memory)]
                .into_iter()
                .filter_map(|(name, value)| Some((name.to_string(), value.clone()?)))
                .collect()
        };
        Self {
            requests: quantities(&resources.requests),
            limits: quantities(&resources.limits),
        }
    }
}

/// A taint a pod tolerates, as Kubernetes spells it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    /// The taint's key; absent, with `Exists`, tolerates every taint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// `Equal` or `Exists`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    /// The taint's value, for `Equal`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// The effect it matches; absent matches every effect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
    /// How long a pod stays bound once a `NoExecute` taint is added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

impl From<&gdl_model::Toleration> for Toleration {
    fn from(toleration: &gdl_model::Toleration) -> Self {
        let set = |value: &String| (!value.is_empty()).then(|| value.clone());
        Self {
            key: set(&toleration.key),
            operator: set(&toleration.operator),
            value: set(&toleration.value),
            effect: set(&toleration.effect),
            toleration_seconds: toleration.toleration_seconds,
        }
    }
}
