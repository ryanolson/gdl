use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{FleetError, FleetTable};
use crate::pool::{check_label, check_region_name};

/// A Kubernetes cluster that runs pools: `[cluster.<name>]` in the fleet
/// file. A pool that names it takes its region. The fleet holds no
/// credential and no secret value: the kube context's credentials stay in
/// the user's kubeconfig, and `secrets` names Kubernetes Secrets that
/// already exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
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
        default = "Cluster::pins_local_default",
        skip_serializing_if = "Cluster::pins_local_is_default"
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
}

impl Cluster {
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
/// names, contexts, regions, namespaces, Secret names, and variables, and two that
/// share a context and a namespace, reported at the later in name order.
pub(super) fn check_clusters(clusters: &BTreeMap<String, Cluster>, errors: &mut Vec<FleetError>) {
    let mut first: BTreeMap<(&str, &str), &str> = BTreeMap::new();
    for (name, cluster) in clusters {
        if let Err(reason) = check_label("cluster", name) {
            errors.push(FleetError::InvalidClusterName {
                cluster: name.clone(),
                reason,
            });
        }
        if cluster.context.is_empty() {
            errors.push(FleetError::EmptyContext {
                cluster: name.clone(),
            });
        }
        if let Err(reason) = check_region_name(&cluster.region) {
            errors.push(FleetError::InvalidRegion {
                table: FleetTable::Cluster(name.clone()),
                reason,
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
            if !secret_name(holder) {
                errors.push(FleetError::InvalidSecretName {
                    cluster: name.clone(),
                    secret: secret.clone(),
                    name: holder.clone(),
                });
            }
        }
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

/// Whether `name` can name a Kubernetes Secret: a DNS subdomain, RFC 1123
/// labels joined by `.`.
fn secret_name(name: &str) -> bool {
    name.len() <= SUBDOMAIN_LIMIT && name.split('.').all(rfc_label)
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
