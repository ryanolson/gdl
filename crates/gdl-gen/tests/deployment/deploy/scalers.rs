//! Where the gdl operator scales a cluster's segments (`use1` in the
//! fixture fleet), `gdl` never writes `replicas`: the render leaves it
//! out, and a conditional one-field apply under a manager of its own
//! writes it once. A cluster that does not ask for the operator (`euw1`)
//! keeps `replicas` from the fleet file under `gdl`.

use super::*;

const ENGINE: &str = "model-x-us-engine";
const FRONT: &str = "global-us-front";

fn deployed(kube: &mut Kube, log: &Log, mirror: &Mirror, files: &Files) -> Vec<String> {
    let mut docker = Docker {
        log: Rc::clone(log),
        fail: false,
    };
    deploy(mirror, files, &mut docker, kube).expect("deploy")
}

fn held<'k>(kube: &'k Kube, context: &str, kind: &str, name: &str) -> &'k Value {
    &kube.objects[context][&(kind.to_string(), name.to_string())]
}

fn owners(kube: &Kube, context: &str, kind: &str, name: &str) -> Vec<String> {
    kube.owners_of(context, &(kind.to_string(), name.to_string()))
}

/// Each one-field apply: its manager and its stdin.
fn bootstraps(log: &Log) -> Vec<(String, Value)> {
    log.borrow()
        .iter()
        .filter_map(|call| match call {
            Call::Kube { args, input } => args
                .iter()
                .find_map(|a| a.strip_prefix("--field-manager="))
                .filter(|manager| *manager != "gdl")
                .map(|manager| {
                    (
                        manager.to_string(),
                        serde_json::from_str(input).expect("JSON"),
                    )
                }),
            Call::Docker => None,
        })
        .collect()
}

/// A first deploy makes each scaled Deployment and its scaler without
/// `replicas`, then writes each at the quorum under its own manager, with
/// the uid and resourceVersion it read. The other cluster's Deployments
/// carry `replicas` under `gdl`.
#[test]
fn a_new_scaled_segment_is_bootstrapped_at_its_quorum() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut kube = Kube::new(&log);
    deployed(&mut kube, &log, &mirror, &files);
    for (kind, manager) in [("Deployment", "gdl-scaler"), ("GdlScaler", "gdl-bootstrap")] {
        assert_eq!(
            held(&kube, "kind-use1", kind, ENGINE)["spec"]["replicas"],
            2
        );
        assert_eq!(owners(&kube, "kind-use1", kind, ENGINE), [manager]);
        assert_eq!(held(&kube, "kind-use1", kind, FRONT)["spec"]["replicas"], 2);
    }
    let writes = bootstraps(&log);
    assert_eq!(writes.len(), 4, "{writes:?}");
    for (manager, patch) in &writes {
        assert!(["gdl-scaler", "gdl-bootstrap"].contains(&manager.as_str()));
        let metadata = patch["metadata"].as_object().expect("metadata");
        assert!(metadata["uid"].is_string() && metadata["resourceVersion"].is_string());
        assert_eq!(
            patch["spec"]
                .as_object()
                .expect("a spec")
                .keys()
                .collect::<Vec<_>>(),
            ["replicas"]
        );
    }
    assert_eq!(
        held(&kube, "kind-euw1", "Deployment", "model-x-eu-engine")["spec"]["replicas"],
        1
    );
    assert_eq!(
        owners(&kube, "kind-euw1", "Deployment", "model-x-eu-engine"),
        ["gdl"]
    );
    assert!(
        !kube.objects["kind-euw1"]
            .keys()
            .any(|(kind, _)| kind == "GdlScaler")
    );
}

/// Once an HPA or a person has scaled a segment through its scaler, and
/// the scaler has written its Deployment, a deploy writes neither: the
/// scale stands.
#[test]
fn a_redeploy_never_undoes_a_scale() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut kube = Kube::new(&log);
    deployed(&mut kube, &log, &mirror, &files);
    let scaled = |kube: &mut Kube, kind: &str, manager: &str| {
        let object = kube
            .objects
            .get_mut("kind-use1")
            .and_then(|objects| objects.get_mut(&(kind.to_string(), ENGINE.to_string())))
            .expect("held");
        object["spec"]["replicas"] = 5.into();
        kube.owners.insert(
            ("kind-use1".into(), kind.into(), ENGINE.into()),
            [manager.to_string()].into(),
        );
    };
    scaled(&mut kube, "GdlScaler", "kube-controller-manager");
    scaled(&mut kube, "Deployment", "gdl-scaler");
    log.borrow_mut().clear();
    deployed(&mut kube, &log, &mirror, &files);
    assert!(
        bootstraps(&log)
            .iter()
            .all(|(_, patch)| patch["metadata"]["name"] != ENGINE),
        "{:?}",
        bootstraps(&log)
    );
    for kind in ["Deployment", "GdlScaler"] {
        assert_eq!(
            held(&kube, "kind-use1", kind, ENGINE)["spec"]["replicas"],
            5,
            "{kind}"
        );
    }
}

/// A Deployment whose `replicas` `gdl` owns from a deploy before its
/// cluster asked for the operator: the bootstrap shares the field at its
/// live count before the apply that leaves it out, so the apply releases
/// `gdl`'s share without a reset to 1.
#[test]
fn a_cluster_that_opts_in_keeps_its_live_count() {
    let mirror = Mirror::new();
    let before = fleet_text().replace("scaler = true\n", "");
    let files = files(&mirror, &before);
    let log = Log::default();
    let mut kube = Kube::new(&log);
    deployed(&mut kube, &log, &mirror, &files);
    assert_eq!(owners(&kube, "kind-use1", "Deployment", ENGINE), ["gdl"]);
    // Scaled by hand, under gdl's apply, to 3.
    kube.objects
        .get_mut("kind-use1")
        .and_then(|objects| objects.get_mut(&("Deployment".to_string(), ENGINE.to_string())))
        .expect("held")["spec"]["replicas"] = 3.into();
    std::fs::write(&files.fleet, fleet_text()).expect("the fleet asks for the operator");
    log.borrow_mut().clear();
    deployed(&mut kube, &log, &mirror, &files);
    assert_eq!(
        held(&kube, "kind-use1", "Deployment", ENGINE)["spec"]["replicas"],
        3
    );
    assert_eq!(
        owners(&kube, "kind-use1", "Deployment", ENGINE),
        ["gdl-scaler"]
    );
    let order: Vec<String> = steps(&log)
        .into_iter()
        .filter(|step| step.starts_with("kind-use1:"))
        .collect();
    let bootstrap = order.iter().position(|s| s == "kind-use1:bootstrap");
    let pools = order.iter().position(|s| s == "kind-use1:pools");
    assert!(bootstrap < pools, "{order:?}");
    let writes = bootstraps(&log);
    let engine = writes
        .iter()
        .find(|(_, patch)| patch["metadata"]["name"] == ENGINE && patch["kind"] == "Deployment")
        .expect("the engine's Deployment is bootstrapped");
    assert_eq!(
        (engine.0.as_str(), &engine.1["spec"]["replicas"]),
        ("gdl-scaler", &Value::from(3))
    );
}

/// A bootstrap that a scale beats, between its read and its write, is
/// refused by the resourceVersion it carries: the deploy reads again,
/// sees the scale's owner, and writes nothing.
#[test]
fn a_stale_bootstrap_is_refused_and_not_retried_over_a_scale() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut kube = Kube::new(&log);
    kube.raced = 1;
    deployed(&mut kube, &log, &mirror, &files);
    let writes = bootstraps(&log);
    let first = writes[0].1["metadata"]["name"].as_str().expect("a name");
    let kind = writes[0].1["kind"].as_str().expect("a kind");
    assert_eq!(held(&kube, "kind-use1", kind, first)["spec"]["replicas"], 5);
    assert_eq!(owners(&kube, "kind-use1", kind, first), ["kubectl"]);
    assert_eq!(
        writes
            .iter()
            .filter(|(_, patch)| patch["metadata"]["name"] == first && patch["kind"] == kind)
            .count(),
        1,
        "{writes:?}"
    );
}

/// A cluster that asks for the operator but has no `GdlScaler` CRD is
/// refused before anything is built.
#[test]
fn a_cluster_without_the_crd_is_refused() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut kube = Kube::new(&log);
    kube.without_scalers = vec!["kind-use1"];
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let error = deploy(&mirror, &files, &mut docker, &mut kube).expect_err("refused");
    assert!(error.to_string().contains("gdlscalers.gdl.dev"), "{error}");
    assert!(!log.borrow().iter().any(|call| matches!(call, Call::Docker)));
}

/// The pods of the entry Service's segment, each ready, with or without
/// `gdl.dev/routable`.
fn pods(labelled: bool) -> Value {
    let mut labels =
        json!({"gdl.dev/fleet": "api", "gdl.dev/pool": "global-us", "gdl.dev/segment": "front"});
    if labelled {
        labels["gdl.dev/routable"] = "true".into();
    }
    json!({"items": [{
        "metadata": {"name": "global-us-front-1", "labels": labels},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }]})
}

/// The entry Service selects on `gdl.dev/routable` only once every ready
/// pod carries it: the deploy that first labels the pods leaves the live
/// Service's selector as it was, and says so; the next one adds it.
#[test]
fn an_entry_service_selects_on_routable_once_every_pod_has_it() {
    let mirror = Mirror::new();
    let before = fleet_text().replace("scaler = true\n", "");
    let files = files(&mirror, &before);
    let log = Log::default();
    let mut kube = Kube::new(&log);
    deployed(&mut kube, &log, &mirror, &files);
    std::fs::write(&files.fleet, fleet_text()).expect("the fleet asks for the operator");
    kube.pods = pods(false);
    let said = deployed(&mut kube, &log, &mirror, &files);
    let service = held(&kube, "kind-use1", "Service", FRONT);
    assert!(
        service["spec"]["selector"]
            .get("gdl.dev/routable")
            .is_none()
    );
    assert!(said.iter().any(|s| s.contains("deploy again")), "{said:?}");
    let template = &held(&kube, "kind-use1", "Deployment", FRONT)["spec"]["template"];
    assert_eq!(template["metadata"]["labels"]["gdl.dev/routable"], "true");
    kube.pods = pods(true);
    let said = deployed(&mut kube, &log, &mirror, &files);
    let service = held(&kube, "kind-use1", "Service", FRONT);
    assert_eq!(service["spec"]["selector"]["gdl.dev/routable"], "true");
    assert!(!said.iter().any(|s| s.contains("deploy again")), "{said:?}");
    // Once the live Service selects on it, a pod without it changes
    // nothing: it is one the operator took out.
    kube.pods = pods(false);
    deployed(&mut kube, &log, &mirror, &files);
    let service = held(&kube, "kind-use1", "Service", FRONT);
    assert_eq!(service["spec"]["selector"]["gdl.dev/routable"], "true");
}

/// A cluster that no longer asks for the operator, whose segment
/// Deployment's `replicas` `gdl-scaler` still owns: `gdl`'s apply of the
/// count would conflict with it, so the deploy refuses before it applies
/// anything, naming the cluster and the segment.
#[test]
fn a_cluster_that_opts_out_with_scaled_segments_is_refused() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut kube = Kube::new(&log);
    kube.holds(
        "kind-euw1",
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "model-x-eu-engine", "namespace": "gdl-api", "labels": {
                "gdl.dev/fleet": "api", "gdl.dev/pool": "model-x-eu", "gdl.dev/segment": "engine",
                "gdl.dev/class": "model_x",
            }},
            "spec": {"replicas": 4},
        }),
    );
    kube.owners.insert(
        (
            "kind-euw1".into(),
            "Deployment".into(),
            "model-x-eu-engine".into(),
        ),
        ["gdl-scaler".to_string()].into(),
    );
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("refused")
        .to_string();
    for part in [
        "cluster `euw1`",
        "model-x-eu-engine",
        "scaler = true",
        "GdlScaler",
    ] {
        assert!(error.contains(part), "{part} in {error}");
    }
    assert!(
        !steps(&log)
            .iter()
            .any(|s| s.ends_with(":pools") || s.ends_with(":base")),
        "{:?}",
        steps(&log)
    );
}
