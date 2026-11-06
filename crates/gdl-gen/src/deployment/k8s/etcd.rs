//! What the etcd directory asks of a fleet's objects. Every hub publishes
//! to the fleet's one etcd, so none reports to a parent's hub or keeps a
//! state claim, and a child may run in another cluster than its parents.
//!
//! One etcd runs in the directory's cluster (`[directory] cluster`), as
//! one DynamoDB Local does: an etcd in each cluster would give each region
//! a directory of its own. Its data is on a claim of its own, so the
//! directory outlives a new pod: a Recreate rollout hands the claim from
//! one pod to the next. Its cluster's hubs reach it at its Service. The
//! other clusters' hubs reach it at its pod's IP, routable across clusters
//! as every pool's data path already requires; the IP exists only once the
//! pod runs, so the render writes a placeholder there, which
//! `cargo gdl deploy` fills in. etcd needs no step before the hubs: each
//! hub writes its record on its first heartbeat.

use gdl_model::{Fleet, FleetBackend, FleetDirectory, KubernetesCluster, Problem, Resources};
use serde_json::{Value, json};

use super::dynamodb::{labels, pod};
use super::objects::resources;
use crate::deployment::etcd::{self, NAME, PORT};

/// The claim that holds etcd's data.
const CLAIM: &str = "etcd-data";

/// The claim's size: the 8 GiB quota, and room for etcd's write-ahead log
/// and snapshots.
const CLAIM_SIZE: &str = "10Gi";

/// The host of etcd's address outside its cluster until `deploy` writes
/// its pod's IP in its place: a name that resolves nowhere, so that a hub
/// given objects that `deploy` did not fill fails at once.
pub(in crate::deployment) const PLACEHOLDER: &str = "ETCD_POD_IP";

/// `fleet`'s directory, when it is etcd.
fn etcd_of(fleet: &Fleet) -> Option<&FleetDirectory> {
    fleet
        .directory()
        .filter(|directory| directory.backend == FleetBackend::Etcd)
}

/// The endpoint of the hubs of cluster `cluster` under `fleet`'s etcd
/// directory: etcd's Service in its own cluster, the placeholder for its
/// pod's IP in any other. None under another directory.
pub(super) fn endpoint(fleet: &Fleet, cluster: &str) -> Option<String> {
    let directory = etcd_of(fleet)?;
    Some(match directory.cluster.as_deref() == Some(cluster) {
        true => address(NAME),
        false => address(PLACEHOLDER),
    })
}

/// etcd's address at `host`.
pub(in crate::deployment) fn address(host: &str) -> String {
    format!("http://{host}:{PORT}")
}

/// Whether `cluster` runs `fleet`'s etcd.
pub(super) fn runs(fleet: &Fleet, cluster: &str) -> bool {
    etcd_of(fleet).is_some_and(|directory| directory.cluster.as_deref() == Some(cluster))
}

/// Each pool of `fleet` that takes the name of etcd's Deployment and
/// Service.
pub(super) fn refusals(fleet: &Fleet) -> Vec<Problem> {
    if etcd_of(fleet).is_none() {
        return Vec::new();
    }
    // A Slurm site makes no such objects, so its pools may take the name.
    fleet
        .pools()
        .filter(|pool| pool.name == NAME)
        .filter(|pool| {
            fleet
                .cluster_of(&pool.name)
                .is_none_or(|(_, cluster)| cluster.slurm().is_none())
        })
        .map(|pool| {
            Problem::at(
                Some(format!("pool.{}", pool.name)),
                None,
                format!(
                    "pool `{}` takes the name of the etcd directory's Deployment and Service, `{NAME}`: rename the pool",
                    pool.name
                ),
            )
        })
        .collect()
}

/// etcd's claim, Deployment, and Service in cluster `name`: one member,
/// with the backend's flags, which gives clients its pod's IP. Its
/// readiness is its `/health`. It asks for `requests`, the cluster's
/// default, when the cluster gives one.
pub(super) fn objects(
    fleet: &str,
    name: &str,
    cluster: &KubernetesCluster,
    requests: Option<&Resources>,
) -> Vec<Value> {
    let labels = labels(fleet, name, NAME);
    let metadata = json!({"name": NAME, "namespace": cluster.namespace, "labels": labels});
    let args = etcd::args(&address("$(POD_IP)"));
    let mut container = json!({
        "name": NAME,
        "image": etcd::IMAGE,
        "command": [etcd::BINARY],
        "args": args,
        // Kubernetes expands `$(POD_IP)` in the arguments.
        "env": [{"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}],
        "ports": [{"name": "client", "containerPort": PORT}],
        "readinessProbe": {
            "httpGet": {"path": "/health", "port": PORT},
            "periodSeconds": 2,
            "timeoutSeconds": 2,
            "failureThreshold": 3,
        },
        "volumeMounts": [{"name": "data", "mountPath": etcd::MOUNT}],
    });
    resources(&mut container, requests);
    let mut spec = pod(fleet, cluster, container, None);
    spec["volumes"] = json!([{"name": "data", "persistentVolumeClaim": {"claimName": CLAIM}}]);
    vec![
        json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": CLAIM, "namespace": cluster.namespace, "labels": labels},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": CLAIM_SIZE}},
            },
        }),
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": metadata,
            "spec": {
                "replicas": 1,
                // One pod at a time holds the claim, and etcd's data.
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": spec,
                },
            },
        }),
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": metadata,
            "spec": {
                "type": "ClusterIP",
                "selector": labels,
                "ports": [{"name": "client", "port": PORT, "targetPort": PORT}],
            },
        }),
    ]
}

/// The README's part on `fleet`'s etcd directory, if it has one.
pub(super) fn readme(fleet: &Fleet) -> String {
    // A fleet of Kubernetes clusters names the one that runs its etcd.
    let Some(home) = etcd_of(fleet).and_then(|directory| directory.cluster.as_deref()) else {
        return String::new();
    };
    format!(
        "\nEvery pool's hub publishes its record to one etcd that cluster `{home}` runs (`base.yaml`: claim `{CLAIM}`, Deployment and Service `{NAME}`), shared by every region. Its hubs reach it at its Service. The other clusters' hubs reach it at its pod's IP, which exists only once the pod runs: their objects name it `{PLACEHOLDER}` (`GDL_POOL__ENDPOINTS=[\"{}\"]`), and `cargo gdl deploy` writes the pod's IP in its place. To apply these objects by hand, replace `{PLACEHOLDER}` with the etcd pod's IP first, and apply them again when a new pod takes another IP. {FLAGS}\n",
        address(PLACEHOLDER)
    )
}

/// What the README says of etcd's flags and its claim.
const FLAGS: &str = "etcd runs with `--auto-compaction-mode=periodic --auto-compaction-retention=1h` and `--quota-backend-bytes=8589934592`: the directory never compacts, so without them etcd would keep each heartbeat until its quota refused every write. Its data is on the claim, from the cluster's default storage class, so a new pod keeps the directory. etcd needs no step before the hubs start: each writes its record on its first heartbeat.";
