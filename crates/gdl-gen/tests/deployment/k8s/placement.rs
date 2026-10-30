//! Where a fleet's pods run, and what they ask for: every Pod template the
//! render writes in a cluster that sets them, the pools' hubs and segments
//! and the DynamoDB directory's Local and tables Job alike, carries the
//! cluster's node selector beside its platform's, its tolerations, and
//! CPU and memory requests and limits. Nothing is left out, since a
//! dedicated node group admits no pod without them.

use super::*;

const PLACED_FLEET: &str = "tests/fixtures/k8s/dynamodb/fleet.toml";

/// Every Pod template in `object`, wherever it holds one.
fn templates<'a>(object: &'a Value, found: &mut Vec<&'a Value>) {
    match object {
        Value::Object(map) => {
            if let Some(spec) = map.get("template").and_then(|t| t.get("spec"))
                && spec.get("containers").is_some()
            {
                found.push(spec);
            }
            for value in map.values() {
                templates(value, found);
            }
        }
        Value::Array(items) => items.iter().for_each(|item| templates(item, found)),
        _ => {}
    }
}

#[test]
fn every_pod_template_carries_its_clusters_placement() {
    let mirror = Mirror::new();
    let text = std::fs::read_to_string(manifest_dir().join(PLACED_FLEET)).expect("the fleet");
    let fleet = fleet(&text);
    let files = render(&mirror, &spec(&mirror), &fleet).expect("objects");
    let mut seen = BTreeMap::new();
    for (path, text) in &files {
        if path.extension().is_none_or(|e| e != "yaml") {
            continue;
        }
        let cluster = path
            .iter()
            .next()
            .expect("a cluster")
            .to_string_lossy()
            .to_string();
        let settings = fleet.cluster(&cluster).expect("the cluster");
        for object in documents(text) {
            let mut found = Vec::new();
            templates(&object, &mut found);
            for pod in found {
                let name = format!("{cluster}/{}", object["metadata"]["name"]);
                let selector = pod["nodeSelector"].as_object().expect("a selector");
                for (key, value) in &settings.node_selector {
                    assert_eq!(selector[key], value.as_str(), "{name}: {key}");
                }
                for key in ["kubernetes.io/os", "kubernetes.io/arch"] {
                    assert!(selector.contains_key(key), "{name}: {key}");
                }
                let tolerations: Vec<gdl_model::Toleration> =
                    serde_json::from_value(rename(&pod["tolerations"])).expect("tolerations");
                assert_eq!(tolerations, settings.tolerations, "{name}");
                for container in pod["containers"].as_array().expect("containers") {
                    for part in ["requests", "limits"] {
                        for kind in ["cpu", "memory"] {
                            assert!(
                                container["resources"][part][kind].is_string(),
                                "{name}: {part}.{kind}"
                            );
                        }
                    }
                }
                *seen
                    .entry(object["kind"].as_str().expect("a kind").to_string())
                    .or_insert(0) += 1;
            }
        }
    }
    // The pools' hubs and segments, and the Local and the tables Job.
    assert_eq!(seen.get("Deployment"), Some(&9), "{seen:?}");
    assert_eq!(seen.get("Job"), Some(&1), "{seen:?}");
}

/// The merge, pinned: the shared pool's engine has its own memory limit
/// over its cluster's default, and an engine in `use1` its cluster's own.
#[test]
fn a_pods_resources_merge_its_pools_over_its_clusters() {
    let mirror = Mirror::new();
    let text = std::fs::read_to_string(manifest_dir().join(PLACED_FLEET)).expect("the fleet");
    let files = render(&mirror, &spec(&mirror), &fleet(&text)).expect("objects");
    let objects = objects(&files);
    let resources = |name: &str| {
        container(&objects[&("Deployment".to_string(), name.to_string())])["resources"].clone()
    };
    assert_eq!(
        resources("model-x-shared-engine"),
        serde_json::json!({
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"cpu": "500m", "memory": "2Gi"},
        })
    );
    assert_eq!(
        resources("model-x-us-engine"),
        serde_json::json!({
            "requests": {"cpu": "250m", "memory": "128Mi"},
            "limits": {"cpu": "1", "memory": "1Gi"},
        })
    );
    assert_eq!(resources("global-us"), resources("dynamodb-local"));
}

/// A toleration as the fleet file spells it: `tolerationSeconds` as
/// `toleration_seconds`.
fn rename(tolerations: &Value) -> Value {
    let text = tolerations
        .to_string()
        .replace("tolerationSeconds", "toleration_seconds");
    serde_json::from_str(&text).expect("JSON")
}
