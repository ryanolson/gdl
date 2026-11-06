//! What the DynamoDB directory asks of a fleet's objects. Every hub
//! publishes to the fleet's two tables, in its cluster's region, so none
//! reports to a parent's hub or keeps a state claim, and a child may run in
//! another cluster than its parents.
//!
//! With `local = true`, one DynamoDB Local runs in the directory's cluster
//! (`[directory] cluster`), shared by every region as a global table is:
//! a Local in each cluster would give each region a directory of its own.
//! Its cluster's hubs reach it at its Service. The other clusters' hubs
//! reach it at its pod's IP, routable across clusters as every pool's
//! data path already requires; the IP exists only once the pod runs, so
//! the render writes a placeholder there, which `cargo gdl deploy` fills
//! in. The tables Job runs in the directory's cluster alone, from the hub's
//! image (`gdl-hub --tables`), and the hubs start after it succeeds.
//!
//! On AWS the owner makes the global tables, with a replica in each region
//! of the fleet, from `dynamodb/tables.json`: a Job that made an absent
//! table in one region would make a regional table there, and that region
//! a directory of its own.

use gdl_model::{Fleet, FleetDirectory, KubernetesCluster, Problem, Resources, dynamodb_tables};
use serde_json::{Value, json};

use super::objects::{place, resources};
use crate::deployment::pools::DYNAMODB_LOCAL;
use gdl_k8s::label_value;

/// The Local's Deployment and Service, and the tables Job: a pool may not
/// take either name.
pub(in crate::deployment) const LOCAL: &str = "dynamodb-local";
pub(in crate::deployment) const TABLES: &str = "dynamodb-tables";

/// The Local's port.
const PORT: u16 = 8000;

/// The host of the Local's address outside its cluster until `deploy`
/// writes the Local's pod IP in its place: a name that resolves nowhere,
/// so that a hub given objects that `deploy` did not fill fails at once.
pub(in crate::deployment) const PLACEHOLDER: &str = "DYNAMODB_LOCAL_POD_IP";

/// The endpoint of the hubs of cluster `cluster` under `fleet`'s
/// directory: the Local's Service in its own cluster, the placeholder for
/// its pod's IP in any other, and none on AWS.
pub(super) fn endpoint(fleet: &Fleet, cluster: &str) -> Option<String> {
    let directory = fleet.directory().filter(|directory| directory.local)?;
    Some(match directory.cluster.as_deref() == Some(cluster) {
        true => format!("http://{LOCAL}:{PORT}"),
        false => address(PLACEHOLDER),
    })
}

/// The Local's address at `host`.
pub(in crate::deployment) fn address(host: &str) -> String {
    format!("http://{host}:{PORT}")
}

/// Whether `cluster` runs `fleet`'s Local and its tables Job.
pub(super) fn runs_local(fleet: &Fleet, cluster: &str) -> bool {
    fleet
        .directory()
        .is_some_and(|directory| directory.local && directory.cluster.as_deref() == Some(cluster))
}

/// Each pool of `fleet` that takes the name of the directory's objects.
pub(super) fn refusals(fleet: &Fleet) -> Vec<Problem> {
    if fleet.directory().is_none_or(|directory| !directory.local) {
        return Vec::new();
    }
    fleet
        .pools()
        .filter(|pool| [LOCAL, TABLES].contains(&pool.name.as_str()))
        .map(|pool| {
            Problem::at(
                Some(format!("pool.{}", pool.name)),
                None,
                format!(
                    "pool `{}` takes the name of the DynamoDB directory's objects, `{LOCAL}` and `{TABLES}`: rename the pool",
                    pool.name
                ),
            )
        })
        .collect()
}

/// The labels of the directory's object `name` in cluster `cluster`.
fn labels(fleet: &str, cluster: &str, name: &str) -> Value {
    json!({
        "gdl.dev/fleet": label_value(fleet),
        "gdl.dev/cluster": label_value(cluster),
        "gdl.dev/directory": name,
    })
}

/// The pod's placement and account, as a pool's pods have them.
fn pod(fleet: &str, cluster: &KubernetesCluster, container: Value, restart: Option<&str>) -> Value {
    let mut pod = json!({
        "serviceAccountName": fleet,
        "enableServiceLinks": false,
        "containers": [container],
    });
    place(&mut pod, cluster);
    if let Some(restart) = restart {
        pod["restartPolicy"] = restart.into();
    }
    pod
}

/// The Local's Deployment and Service in cluster `name`: one pod, in
/// memory, with `-sharedDb`, so that clients of every region see one
/// database. Its readiness is its port's. It asks for `requests`, the
/// cluster's default, when the cluster gives one.
pub(super) fn local(
    fleet: &str,
    name: &str,
    cluster: &KubernetesCluster,
    requests: Option<&Resources>,
) -> Vec<Value> {
    let labels = labels(fleet, name, LOCAL);
    let metadata = json!({"name": LOCAL, "namespace": cluster.namespace, "labels": labels});
    let mut container = json!({
        "name": LOCAL,
        "image": DYNAMODB_LOCAL,
        "args": ["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-port", PORT.to_string()],
        "ports": [{"name": "dynamodb", "containerPort": PORT}],
        "readinessProbe": {
            "tcpSocket": {"port": PORT},
            "periodSeconds": 2,
            "timeoutSeconds": 2,
            "failureThreshold": 3,
        },
    });
    resources(&mut container, requests);
    vec![
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": metadata,
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": pod(fleet, cluster, container, None),
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
                "ports": [{"name": "dynamodb", "port": PORT, "targetPort": PORT}],
            },
        }),
    ]
}

/// The tables Job of `directory` in cluster `name`, run from `image`, the
/// hub's: it makes the tables in the Local, and checks the keys of those
/// that exist. It runs once: the step tries again itself while the Local
/// does not answer, and a failure names its table in the pod's log. It
/// asks for `requests`, the cluster's default, when the cluster gives one.
pub(super) fn tables_job(
    fleet: &str,
    name: &str,
    cluster: &KubernetesCluster,
    directory: &FleetDirectory,
    image: &str,
    requests: Option<&Resources>,
) -> Value {
    let env = [
        ("GDL_POOL__RECORDS_TABLE", directory.records_table.clone()),
        ("GDL_POOL__LISTS_TABLE", directory.lists_table.clone()),
        ("GDL_POOL__AWS_REGION", cluster.region.clone()),
        ("GDL_POOL__ENDPOINT", format!("http://{LOCAL}:{PORT}")),
    ]
    .map(|(name, value)| {
        json!({"name": name, "value": crate::deployment::variables::string(&value)})
    });
    let mut container = json!({
        "name": "tables",
        "image": image,
        "command": [gdl_k8s::BINARY, "--tables"],
        "env": env,
    });
    resources(&mut container, requests);
    let labels = labels(fleet, name, TABLES);
    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": TABLES, "namespace": cluster.namespace, "labels": labels},
        "spec": {
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": labels},
                "spec": pod(fleet, cluster, container, Some("Never")),
            },
        },
    })
}

/// `dynamodb/tables.json` of `directory` for `fleet` on AWS: the tables,
/// their keys, the lists table's TTL attribute, and every region of the
/// fleet's clusters, each of which needs a replica.
pub(super) fn tables_json(fleet: &Fleet, directory: &FleetDirectory) -> String {
    let regions: std::collections::BTreeSet<&str> = fleet
        .clusters()
        .map(|(_, cluster)| cluster.region())
        .collect();
    let key = |name: &str| json!({"name": name, "type": "S"});
    let (class, pool) = dynamodb_tables::RECORDS_KEYS;
    let (owner, list) = dynamodb_tables::LISTS_KEYS;
    let tables = json!({
        "billing_mode": "PAY_PER_REQUEST",
        "regions": regions,
        "tables": [
            {
                "name": directory.records_table,
                "partition_key": key(class),
                "sort_key": key(pool),
            },
            {
                "name": directory.lists_table,
                "partition_key": key(owner),
                "sort_key": key(list),
                "ttl_attribute": dynamodb_tables::TTL_ATTRIBUTE,
            },
        ],
    });
    let mut text = serde_json::to_string_pretty(&tables).expect("JSON");
    text.push('\n');
    text
}

/// The README's part on `fleet`'s directory, if it has one.
pub(super) fn readme(fleet: &Fleet) -> String {
    let Some(directory) = fleet.directory() else {
        return String::new();
    };
    let tables = format!(
        "`{}` and `{}`",
        directory.records_table, directory.lists_table
    );
    match (directory.local, directory.cluster.as_deref()) {
        (true, Some(home)) => format!(
            "\nEvery pool's hub publishes its record to the DynamoDB tables {tables}, in one DynamoDB Local that cluster `{home}` runs (`base.yaml`: Deployment and Service `{LOCAL}`), shared by every region. Its hubs reach it at its Service. The other clusters' hubs reach it at its pod's IP, which exists only once the pod runs: their objects name it `{PLACEHOLDER}` (`GDL_POOL__ENDPOINT={}`), and `cargo gdl deploy` writes the pod's IP in its place. To apply these objects by hand, replace `{PLACEHOLDER}` with the Local pod's IP first. After the base, and before the pools, apply `{home}/tables.yaml`, the Job `{TABLES}`, and wait until it completes: it makes the tables. The Local keeps them in memory, so a new Local pod starts empty: deploy again.\n",
            address(PLACEHOLDER)
        ),
        _ => format!(
            "\nEvery pool's hub publishes its record to the DynamoDB tables {tables}, in its cluster's region, with the role of its service account. Before the pools start, the owner of the AWS account makes them as global tables, with a replica in every region of the fleet, from `dynamodb/tables.json`: its keys, the lists table's TTL attribute, and the regions. No Job makes them, since one that made an absent table in one region would make a regional table there, and that region would see a directory of its own.\n"
        ),
    }
}
