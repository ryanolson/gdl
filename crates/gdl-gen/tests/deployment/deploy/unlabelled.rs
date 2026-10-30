//! Objects of the fleet that carry no `gdl.dev/cluster`, as a render
//! before that label wrote them, belong to no known cluster: `down` and
//! the prune refuse them by name, and delete nothing.

use super::*;

/// `down` over the pinned pool `global-us`, with or without its cluster
/// label: it never reports success while that pool still runs.
fn down_from_render(cluster_label: bool) {
    let dir = tempfile::Builder::new()
        .prefix("gdl-render-label-")
        .tempdir()
        .expect("a directory");
    let fleet = dir.path().join("fleet.toml");
    std::fs::write(&fleet, fleet_text()).expect("the fleet");
    let log = Log::default();
    let mut kube = Kube::new(&log);
    let rendered = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/k8s/out/use1/pools/global-us.yaml"),
    )
    .expect("the rendered pool");
    for doc in rendered.split("---\n").filter(|doc| !doc.trim().is_empty()) {
        kube.holds("kind-use1", serde_json::from_str(doc).expect("an object"));
    }
    if !cluster_label {
        for object in kube
            .objects
            .get_mut("kind-use1")
            .expect("objects")
            .values_mut()
        {
            object["metadata"]["labels"]
                .as_object_mut()
                .expect("labels")
                .remove("gdl.dev/cluster");
            if let Some(labels) = object["spec"]["template"]["metadata"]["labels"].as_object_mut() {
                labels.remove("gdl.dev/cluster");
            }
        }
    }
    let result = gdl_gen::down(&fleet, &waits(), &mut kube, &mut |_| {});
    let remaining: Vec<_> = kube.objects["kind-use1"].keys().collect();
    assert!(
        result.is_err() || remaining.is_empty(),
        "down reported success but left the rendered fleet running: {remaining:?}"
    );
}

/// With the label, `down` deletes the pool.
#[test]
fn down_finds_a_render_with_the_cluster_label() {
    down_from_render(true);
}

/// Without it, `down` does not report success.
#[test]
fn down_detects_a_render_without_the_cluster_label() {
    down_from_render(false);
}

/// Remove the cluster label from `context`'s Deployment `name`, as a
/// render before that label left it.
fn unlabel(kube: &mut Kube, context: &str, name: &str) {
    kube.objects
        .get_mut(context)
        .and_then(|objects| objects.get_mut(&("Deployment".to_string(), name.to_string())))
        .expect("the object")["metadata"]["labels"]
        .as_object_mut()
        .expect("labels")
        .remove("gdl.dev/cluster");
}

/// `down` names each object with no cluster label and how to settle it,
/// and deletes nothing in that cluster.
#[test]
fn down_names_objects_without_a_cluster_and_deletes_nothing() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-render-label-")
        .tempdir()
        .expect("a directory");
    let fleet = dir.path().join("fleet.toml");
    std::fs::write(&fleet, fleet_text()).expect("the fleet");
    let log = Log::default();
    let mut kube = Kube::new(&log);
    kube.holds(
        "kind-euw1",
        left("Deployment", "global-eu", "global-eu", "hub", None),
    );
    unlabel(&mut kube, "kind-euw1", "global-eu");
    let error = gdl_gen::down(&fleet, &waits(), &mut kube, &mut |_| {})
        .expect_err("an object of no known cluster")
        .to_string();
    for part in [
        "cluster `euw1`: namespace `gdl-api` holds objects of the fleet that carry no `gdl.dev/cluster` label",
        "deployment/global-eu",
        "label deployment/global-eu gdl.dev/cluster=euw1",
    ] {
        assert!(error.contains(part), "{part} in {error}");
    }
    assert!(deletes(&log, "kind-euw1").is_empty());
    assert!(!steps(&log).iter().any(|step| step.ends_with(":unlabel")));
}

/// The prune refuses them too, before it deletes anything there.
#[test]
fn a_prune_refuses_objects_without_a_cluster() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.holds(
        "kind-use1",
        left("Deployment", "model-x-old", "model-x-old", "hub", None),
    );
    kube.holds(
        "kind-use1",
        left("Deployment", "global-old", "global-old", "hub", None),
    );
    unlabel(&mut kube, "kind-use1", "global-old");
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("an object of no known cluster")
        .to_string();
    assert!(error.contains("deployment/global-old"), "{error}");
    assert!(deletes(&log, "kind-use1").is_empty());
}
