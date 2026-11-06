use std::collections::BTreeMap;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::placement::{self, ClusterResources, Toleration};
use super::slurm::{SlurmSite, check_site};
use super::written::Written;
use super::{FleetError, FleetTable};
use crate::pool::{check_label, check_region_name};
use crate::problem::{nested, segments};

/// A place that runs pools: `[cluster.<name>]` in the fleet file. Its
/// `kind` says which: `"kubernetes"`, the default, or `"slurm"`. A pool
/// that names it takes its region. Each kind has keys of its own, and a
/// key of the other kind is refused by name.
///
/// Written, a Kubernetes cluster has no `kind`, so that a fleet file from
/// before the tag reads and writes as it did; a Slurm site writes
/// `kind = "slurm"` first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cluster {
    /// A Kubernetes cluster.
    Kubernetes(KubernetesCluster),
    /// A Slurm cluster that runs pools as jobs through srt-slurm.
    Slurm(SlurmSite),
}

impl Cluster {
    /// The `kind` of a Kubernetes cluster, which a table may leave unsaid.
    pub const KUBERNETES: &str = "kubernetes";
    /// The `kind` of a Slurm site.
    pub const SLURM: &str = "slurm";

    /// Its `kind`, as the fleet file names it.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Kubernetes(_) => Self::KUBERNETES,
            Self::Slurm(_) => Self::SLURM,
        }
    }

    /// The region of every pool it runs.
    pub fn region(&self) -> &str {
        match self {
            Self::Kubernetes(cluster) => &cluster.region,
            Self::Slurm(site) => &site.region,
        }
    }

    /// The registry it pulls images from, if it names one.
    pub fn registry(&self) -> Option<&str> {
        match self {
            Self::Kubernetes(cluster) => cluster.registry.as_deref(),
            Self::Slurm(site) => site.registry.as_deref(),
        }
    }

    /// Its nodes' platform, if it names one: `linux/amd64`.
    pub fn platform(&self) -> Option<&str> {
        match self {
            Self::Kubernetes(cluster) => cluster.platform.as_deref(),
            Self::Slurm(site) => site.platform.as_deref(),
        }
    }

    /// The Kubernetes cluster, if it is one.
    pub fn kubernetes(&self) -> Option<&KubernetesCluster> {
        match self {
            Self::Kubernetes(cluster) => Some(cluster),
            Self::Slurm(_) => None,
        }
    }

    /// The Slurm site, if it is one.
    pub fn slurm(&self) -> Option<&SlurmSite> {
        match self {
            Self::Slurm(site) => Some(site),
            Self::Kubernetes(_) => None,
        }
    }
}

impl Serialize for Cluster {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        /// A Slurm site, with its tag first.
        #[derive(Serialize)]
        #[serde(tag = "kind", rename_all = "lowercase")]
        enum Tagged<'a> {
            Slurm(&'a SlurmSite),
        }
        match self {
            Self::Kubernetes(cluster) => cluster.serialize(serializer),
            Self::Slurm(site) => Tagged::Slurm(site).serialize(serializer),
        }
    }
}

// serde's own tagged enums neither default a missing tag nor keep the path
// of an error inside the table, so the table is read whole, as written
// ([`Written`]), its `kind` taken out, and the rest read as that kind, with
// the path inside it carried in the message for the problem's table and
// key ([`nested`]). The parser's line of a problem inside the table is
// lost.
impl<'de> Deserialize<'de> for Cluster {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let kinds = format!("`{}`, the default, or `{}`", Self::KUBERNETES, Self::SLURM);
        let at_kind = |message: &str| D::Error::custom(nested(&["kind".into()], message));
        let entries = match Written::deserialize(deserializer)? {
            Written::Map(entries) => entries,
            other => {
                let message = format!("invalid type: {}, expected a table", shown(&other));
                return Err(D::Error::custom(message));
            }
        };
        let (kind, rest): (Vec<_>, Vec<_>) =
            entries.into_iter().partition(|(key, _)| key == "kind");
        let kind = match kind.as_slice() {
            [] => Self::KUBERNETES.to_string(),
            [(_, Written::Str(kind))] => kind.clone(),
            [(_, other)] => {
                return Err(at_kind(&format!(
                    "is {}, not a string: {kinds}",
                    shown(other)
                )));
            }
            [_, _, ..] => return Err(at_kind("duplicate field `kind`")),
        };
        let rest = Written::Map(rest);
        let read = match kind.as_str() {
            Self::KUBERNETES => serde_path_to_error::deserialize(rest).map(Self::Kubernetes),
            Self::SLURM => serde_path_to_error::deserialize(rest).map(Self::Slurm),
            other => {
                return Err(at_kind(&format!(
                    "`{other}` is not a kind of cluster: {kinds}"
                )));
            }
        };
        read.map_err(|error| {
            let keys = segments(error.path());
            D::Error::custom(nested(&keys, &error.into_inner().to_string()))
        })
    }
}

/// A `kind` that is not a string, as a refusal shows it.
fn shown(value: &Written) -> String {
    match value {
        Written::Unit => "null".into(),
        Written::Bool(value) => value.to_string(),
        Written::I64(value) => value.to_string(),
        Written::U64(value) => value.to_string(),
        Written::F64(value) => value.to_string(),
        Written::Str(value) => format!("{value:?}"),
        Written::Seq(_) => "an array".into(),
        Written::Map(_) => "a table".into(),
    }
}

/// A Kubernetes cluster that runs pools: `[cluster.<name>]` with no `kind`,
/// or `kind = "kubernetes"`. The fleet holds no credential and no secret
/// value: the kube context's credentials stay in the user's kubeconfig,
/// and `secrets` names Kubernetes Secrets that already exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesCluster {
    /// The kube context that reaches the cluster.
    pub context: String,
    /// The namespace the fleet's objects go in: no two clusters of a fleet
    /// share a context and a namespace.
    pub namespace: String,
    /// The region of every pool in the cluster.
    pub region: String,
    /// The registry the cluster pulls images from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry: Option<String>,
    /// The cluster's nodes' platform, as `docker buildx --platform` takes
    /// it: `linux/amd64`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    /// Whether the cluster's pin table runs in a DynamoDB Local of its own.
    #[serde(
        default = "KubernetesCluster::pins_local_default",
        skip_serializing_if = "KubernetesCluster::pins_local_is_default"
    )]
    pub pins_local: bool,
    /// Annotations of the pools' service account, for a cloud role.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub service_account_annotations: BTreeMap<String, String>,
    /// Each secret of the spec, by its name, to the existing Kubernetes
    /// Secret that holds it in this cluster.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, String>,
    /// Environment variables every pool's containers in the cluster get.
    /// Not for secret values: those go in `secrets`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Node labels every pod of the cluster is scheduled by, beside the
    /// operating system and architecture its `platform` gives.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
    /// The taints every pod of the cluster tolerates.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,
    /// Every pod's CPU and memory requests and limits, or none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ClusterResources>,
    /// Whether the gdl operator runs the cluster's pools: each segment gets
    /// a `GdlScaler`, which owns its Deployment's `replicas`, and its pods
    /// leave their entry Service before they go. The cluster must have the
    /// operator's CRD.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub operator: bool,
}

impl KubernetesCluster {
    /// `pins_local` when unsaid: the cluster's pin table is not in a Local
    /// of its own.
    pub const DEFAULT_PINS_LOCAL: bool = false;

    fn pins_local_default() -> bool {
        Self::DEFAULT_PINS_LOCAL
    }

    fn pins_local_is_default(pins_local: &bool) -> bool {
        *pins_local == Self::DEFAULT_PINS_LOCAL
    }
}

/// The `[fleet]` table: what is the fleet's own, not a pool's or a
/// cluster's.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Header {
    /// The fleet's name, which labels the objects it deploys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,
}

impl Header {
    pub(super) fn is_empty(&self) -> bool {
        self.name.is_none()
    }

    pub(super) fn check(&self, errors: &mut Vec<FleetError>) {
        if let Some(name) = &self.name
            && let Err(reason) = check_label("fleet", name)
        {
            errors.push(FleetError::InvalidFleetName { reason });
        }
    }
}

/// What is wrong with the fleet's clusters, each at its own table: their
/// names and regions; a Kubernetes cluster's context, namespace,
/// annotation keys, Secret names, and variables, and two that share a
/// context and a namespace, reported at the later in name order; and what
/// a Slurm site cannot run ([`check_site`]).
pub(super) fn check_clusters(clusters: &BTreeMap<String, Cluster>, errors: &mut Vec<FleetError>) {
    let mut first: BTreeMap<(&str, &str), &str> = BTreeMap::new();
    for (name, cluster) in clusters {
        if let Err(reason) = check_label("cluster", name) {
            errors.push(FleetError::InvalidClusterName {
                cluster: name.clone(),
                reason,
            });
        }
        if let Err(reason) = check_region_name(cluster.region()) {
            errors.push(FleetError::InvalidRegion {
                table: FleetTable::Cluster(name.clone()),
                reason,
            });
        }
        let cluster = match cluster {
            Cluster::Kubernetes(cluster) => cluster,
            Cluster::Slurm(site) => {
                check_site(name, site, errors);
                continue;
            }
        };
        if cluster.context.is_empty() {
            errors.push(FleetError::EmptyContext {
                cluster: name.clone(),
            });
        }
        if !namespace_name(&cluster.namespace) {
            errors.push(FleetError::InvalidNamespace {
                cluster: name.clone(),
                reason: format!(
                    "`{}` is not a Kubernetes namespace: lowercase letters, digits, and `-`, starting and ending with a letter or digit, at most {NAMESPACE_LIMIT} characters",
                    cluster.namespace
                ),
            });
        }
        for (secret, holder) in &cluster.secrets {
            if !subdomain(holder) {
                errors.push(FleetError::InvalidSecretName {
                    cluster: name.clone(),
                    secret: secret.clone(),
                    name: holder.clone(),
                });
            }
        }
        for key in cluster
            .service_account_annotations
            .keys()
            .filter(|key| !annotation_key(key))
        {
            errors.push(FleetError::InvalidAnnotation {
                cluster: name.clone(),
                key: key.clone(),
            });
        }
        placement::check_cluster(
            name,
            &cluster.node_selector,
            &cluster.tolerations,
            cluster.resources.as_ref(),
            errors,
        );
        for variable in cluster.env.keys().filter(|v| !variable_name(v)) {
            errors.push(FleetError::InvalidVariable {
                cluster: name.clone(),
                variable: variable.clone(),
            });
        }
        let place = (cluster.context.as_str(), cluster.namespace.as_str());
        match first.get(&place) {
            Some(earlier) => errors.push(FleetError::SharedNamespace {
                cluster: name.clone(),
                first: earlier.to_string(),
                context: cluster.context.clone(),
                namespace: cluster.namespace.clone(),
            }),
            None => {
                first.insert(place, name);
            }
        }
    }
}

/// The longest name of a Kubernetes namespace: an RFC 1123 label's.
const NAMESPACE_LIMIT: usize = 63;

/// The longest name of a Kubernetes Secret: a DNS subdomain's.
const SUBDOMAIN_LIMIT: usize = 253;

/// Whether `name` can name a Kubernetes namespace: an RFC 1123 label. It
/// may start with a digit, which a pool's or a cluster's name may not.
fn namespace_name(name: &str) -> bool {
    name.len() <= NAMESPACE_LIMIT && rfc_label(name)
}

/// Whether `name` is a DNS subdomain, RFC 1123 labels joined by `.`: the
/// name of a Kubernetes Secret, or an annotation key's prefix.
fn subdomain(name: &str) -> bool {
    name.len() <= SUBDOMAIN_LIMIT && name.split('.').all(rfc_label)
}

/// The longest name of an annotation key, after its prefix.
const ANNOTATION_NAME_LIMIT: usize = 63;

/// Whether `key` is a Kubernetes annotation key: an optional DNS subdomain
/// and `/`, then a name of letters, digits, `-`, `_`, and `.`, starting and
/// ending with a letter or digit.
pub(super) fn annotation_key(key: &str) -> bool {
    let (prefix, name) = match key.split_once('/') {
        Some((prefix, name)) => (Some(prefix), name),
        None => (None, key),
    };
    let alnum = |c: char| c.is_ascii_alphanumeric();
    prefix.is_none_or(subdomain)
        && name.len() <= ANNOTATION_NAME_LIMIT
        && name.starts_with(alnum)
        && name.ends_with(alnum)
        && name
            .chars()
            .all(|c| alnum(c) || matches!(c, '-' | '_' | '.'))
}

/// Whether `label` is lowercase letters, digits, and `-`, starting and
/// ending with a letter or digit: an RFC 1123 label, without its length.
fn rfc_label(label: &str) -> bool {
    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    label.starts_with(alnum)
        && label.ends_with(alnum)
        && label.chars().all(|c| alnum(c) || c == '-')
}

/// Whether `name` can name an environment variable in a container.
fn variable_name(name: &str) -> bool {
    !name.starts_with(|c: char| c.is_ascii_digit())
        && !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}
