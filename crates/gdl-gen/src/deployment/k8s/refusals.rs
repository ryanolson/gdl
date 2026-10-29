//! What keeps a fleet off Kubernetes before anything is fetched: what
//! `check` does not judge, because a fleet for Compose needs none of it,
//! and because `check` does not see the spec's secrets.

use gdl_model::{Fleet, Problem};

use super::{objects, relay};
use crate::deployment::Settings;

/// Variables that the render sets in every pod: a cluster's `env` cannot
/// set one, nor any under these prefixes. `GDL_POOL` and `GDL_HUB` alone
/// would set the whole `[pool]` or `[hub]` table. Matched without regard
/// to case, as the settings readers match a key.
const SET: &[&str] = &[objects::POD_IP, "GDL_POOL", "GDL_HUB"];
const SET_PREFIXES: &[&str] = &["GDL_POOL__", "GDL_HUB__"];

/// Variables that hold a static cloud credential, which the objects would
/// write in plain text: a fleet reaches AWS through its service account's
/// role, never a key. Matched without regard to case.
const CREDENTIALS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
];

/// Every problem that keeps `fleet` off Kubernetes with the spec's
/// deployment `settings`, each at its table.
pub(super) fn fleet(fleet: &Fleet, settings: &Settings) -> Vec<Problem> {
    let mut problems = Vec::new();
    if fleet.name().is_none() {
        problems.push(Problem::from(
            "the fleet has no `[fleet] name`: it labels every object the fleet deploys (`gdl.dev/fleet`), by which a deploy finds them again".to_string(),
        ));
    }
    let declared: Vec<&str> = settings.secret_names().collect();
    // A secret is the file of its name in a projected volume, whose paths
    // Kubernetes refuses to start with `..`. The spec already refuses a
    // name with `/`, and `.` and `..` alone.
    for secret in declared.iter().filter(|secret| secret.starts_with("..")) {
        problems.push(Problem::at(
            Some(format!("deployment.secrets.{secret}")),
            None,
            format!(
                "secret `{secret}` starts with `..`, which a Kubernetes projected volume refuses as a file's path: rename it"
            ),
        ));
    }
    for (name, cluster) in fleet.clusters() {
        let table = || Some(format!("cluster.{name}"));
        match &cluster.registry {
            None => problems.push(Problem::at(
                table(),
                None,
                "has no `registry`: the cluster pulls its pools' images from it",
            )),
            Some(registry) => {
                if let Err(message) = objects::check_registry(registry) {
                    problems.push(Problem::at(table(), Some("registry".into()), message));
                }
            }
        }
        match &cluster.platform {
            None => problems.push(Problem::at(
                table(),
                None,
                "has no `platform`: its pools' images are built for it, and its pods run on its nodes",
            )),
            Some(platform) => {
                if let Err(message) = objects::check_platform(platform) {
                    problems.push(Problem::at(table(), Some("platform".into()), message));
                }
            }
        }
        for secret in cluster.secrets.keys() {
            if !declared.contains(&secret.as_str()) {
                problems.push(Problem::at(
                    table(),
                    Some("secrets".into()),
                    format!(
                        "`{secret}` is not a secret of the spec: no `[deployment.secrets.{secret}]` table declares it"
                    ),
                ));
            }
        }
        for variable in cluster.env.keys() {
            let upper = variable.to_ascii_uppercase();
            if CREDENTIALS.contains(&upper.as_str()) {
                problems.push(Problem::at(
                    table(),
                    Some("env".into()),
                    format!(
                        "`{variable}` holds a static credential, which the objects would write in plain text: use the service account's role, or a Secret"
                    ),
                ));
                continue;
            }
            if SET.contains(&upper.as_str()) || SET_PREFIXES.iter().any(|p| upper.starts_with(p)) {
                problems.push(Problem::at(
                    table(),
                    Some("env".into()),
                    format!(
                        "`{variable}` is a variable the render sets in every pod (`{}`, `GDL_POOL`, `GDL_HUB`, `GDL_POOL__*`, `GDL_HUB__*`): set a pool's settings in the fleet's `[pool]` and `[class]` tables",
                        objects::POD_IP
                    ),
                ));
            }
        }
    }
    for pool in fleet.pools() {
        if fleet.cluster_of(&pool.name).is_none() {
            problems.push(Problem::at(
                Some(format!("pool.{}", pool.name)),
                None,
                "has no `cluster`: on Kubernetes every pool runs in one of the fleet's clusters",
            ));
        }
    }
    problems.extend(relay::refusals(fleet));
    problems
}
