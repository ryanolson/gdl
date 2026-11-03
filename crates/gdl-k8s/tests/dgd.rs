//! A pool whose segment runs in a DynamoGraphDeployment (DGD): the pool
//! renders the DGD as one of its objects, and dynamo's operator runs the
//! segment's pods.

use gdl_k8s::{Hub, Pool, Scaling, Segment, children};
use serde_json::{Value, json};

/// A DGD of dynamo's frontend slot, scaled by its scaling adapter, and of
/// three workers.
fn dgd() -> Value {
    json!({
        "apiVersion": "nvidia.com/v1beta1",
        "kind": "DynamoGraphDeployment",
        "metadata": {"name": "p", "labels": {"team": "inference"}},
        "spec": {"components": [
            {
                "name": "Frontend",
                "type": "frontend",
                "replicas": 2,
                "scalingAdapter": {},
                "podTemplate": {"spec": {"containers": [{"name": "main", "image": "registry.example/pool:tag"}]}},
            },
            {"name": "Worker", "type": "decode", "replicas": 3},
        ]},
    })
}

/// Pool `p`: segment `pool` in the DGD's `Frontend`, and segment `front`
/// in a Deployment of its own.
fn pool() -> Pool {
    let segment = |name: &str, component: Option<&str>| Segment {
        name: name.into(),
        image: format!("registry.example/{name}:tag"),
        settings: "[system]\nport = 9090\n".into(),
        quorum: 1,
        ports: Vec::new(),
        env: Vec::new(),
        secrets: Vec::new(),
        resources: None,
        component: component.map(str::to_string),
    };
    Pool {
        fleet: "api".into(),
        cluster: "use1".into(),
        class: "model".into(),
        region: "us-east-1".into(),
        deployment: "{\"graph\": 1}".into(),
        hub: Hub {
            image: "registry.example/hub:tag".into(),
            env: Vec::new(),
            resources: None,
            state_dir: None,
        },
        segments: vec![segment("front", None), segment("pool", Some("Frontend"))],
        service_account_name: "api".into(),
        node_selector: Default::default(),
        tolerations: Vec::new(),
        parent: None,
        dynamodb: None,
        pod_monitor: false,
        presence: Default::default(),
        allow_pool_restart: false,
        dgd: Some(dgd().to_string()),
    }
}

fn find<'a>(objects: &'a [Value], kind: &str, name: &str) -> Option<&'a Value> {
    objects
        .iter()
        .find(|o| o["kind"] == kind && o["metadata"]["name"] == name)
}

fn component<'a>(dgd: &'a Value, name: &str) -> &'a Value {
    dgd["spec"]["components"]
        .as_array()
        .expect("components")
        .iter()
        .find(|c| c["name"] == name)
        .expect("the component")
}

/// The segment in the DGD has no Deployment or entry Service of its
/// own; the hub and the other segment render as in any pool; and the
/// DGD is one of the pool's objects, named after the pool and labelled
/// as its others are.
#[test]
fn a_dgd_segment_runs_in_the_pools_dgd() {
    let objects = children("p", "ns", &pool(), Scaling::Operator).expect("objects");
    for kind in ["Deployment", "Service"] {
        assert!(find(&objects, kind, "p-pool").is_none(), "{kind}");
    }
    assert!(find(&objects, "Deployment", "p").is_some(), "the hub");
    assert!(find(&objects, "Deployment", "p-front").is_some());
    let dgd = find(&objects, "DynamoGraphDeployment", "p").expect("the DGD");
    assert_eq!(dgd["apiVersion"], "nvidia.com/v1beta1");
    assert_eq!(dgd["metadata"]["namespace"], "ns");
    let labels = &dgd["metadata"]["labels"];
    assert_eq!(labels["gdl.dev/pool"], "p");
    assert_eq!(labels["gdl.dev/fleet"], "api");
    assert_eq!(labels["team"], "inference");
    assert!(labels.get("gdl.dev/segment").is_none());
    // The pool's settings ConfigMap still holds the segment's settings,
    // which the DGD's component mounts.
    let config = find(&objects, "ConfigMap", "p").expect("the ConfigMap");
    assert!(config["data"].get("pool.toml").is_some());
}

/// The scaling adapter owns a component's count once it has one, and
/// dynamo's webhook refuses any other writer: the rendered DGD leaves
/// `replicas` out of such a component, and keeps it on the others.
#[test]
fn the_dgd_leaves_an_adapters_count_alone() {
    let objects = children("p", "ns", &pool(), Scaling::Operator).expect("objects");
    let dgd = find(&objects, "DynamoGraphDeployment", "p").expect("the DGD");
    assert!(component(dgd, "Frontend").get("replicas").is_none());
    assert_eq!(component(dgd, "Worker")["replicas"], 3);
}

/// A change to what the pool's ConfigMap holds rolls the pods of each
/// DGD component that runs a segment, as it rolls a segment's
/// Deployment, and never a worker's.
#[test]
fn a_config_change_rolls_the_dgd_segment() {
    let config = |pool: &Pool| {
        let objects = children("p", "ns", pool, Scaling::Operator).expect("objects");
        let dgd = find(&objects, "DynamoGraphDeployment", "p")
            .expect("the DGD")
            .clone();
        let annotation = |name: &str| {
            component(&dgd, name)["podTemplate"]["metadata"]["annotations"]["gdl.dev/config"]
                .clone()
        };
        (annotation("Frontend"), annotation("Worker"))
    };
    let (before, worker) = config(&pool());
    assert!(before.is_string(), "{before}");
    assert!(worker.is_null(), "{worker}");
    let mut changed = pool();
    changed.segments[1].settings = "[system]\nport = 9090\n[runtime]\ndrain_ms = 1000\n".into();
    let (after, worker) = config(&changed);
    assert_ne!(before, after);
    assert!(worker.is_null(), "{worker}");
}

/// A DGD the pool cannot own as written is refused with a reason, never
/// changed to fit: the operator finds its pods by the pool's name, so
/// the DGD must have it, in the pool's namespace.
#[test]
fn a_dgd_that_does_not_fit_the_pool_is_refused() {
    let refused = |pool: Pool, why: &str| {
        let error = children("p", "ns", &pool, Scaling::Operator).expect_err(why);
        assert!(error.contains(why), "{error}");
    };
    let with = |edit: &dyn Fn(&mut Value)| {
        let mut dgd = dgd();
        edit(&mut dgd);
        let mut pool = pool();
        pool.dgd = Some(dgd.to_string());
        pool
    };
    refused(with(&|d| d["metadata"]["name"] = "other".into()), "`other`");
    refused(
        with(&|d| d["metadata"]["namespace"] = "other".into()),
        "`other`",
    );
    refused(
        with(&|d| d["kind"] = "Deployment".into()),
        "DynamoGraphDeployment",
    );
    refused(
        with(&|d| d["apiVersion"] = "nvidia.com/v1alpha1".into()),
        "nvidia.com/v1beta1",
    );
    refused(
        with(&|d| d["spec"]["components"][0]["name"] = "Head".into()),
        "`Frontend`",
    );
    let mut text = pool();
    text.dgd = Some("{".into());
    refused(text, "not JSON");
    let mut none = pool();
    none.dgd = None;
    refused(none, "`Frontend`");
    let mut twice = pool();
    twice.segments[0].component = Some("Frontend".into());
    refused(twice, "`Frontend`");
}

/// A DGD without a name takes the pool's; one with no labels gets the
/// pool's.
#[test]
fn a_dgd_takes_the_pools_name() {
    let mut dgd = dgd();
    dgd["metadata"] = json!({});
    let mut pool = pool();
    pool.dgd = Some(dgd.to_string());
    let objects = children("p", "ns", &pool, Scaling::Operator).expect("objects");
    let dgd = find(&objects, "DynamoGraphDeployment", "p").expect("the DGD");
    assert_eq!(dgd["metadata"]["labels"]["gdl.dev/pool"], "p");
}

/// Dynamo forbids a shared template beside role templates, and each role must roll when its settings change.
#[test]
fn a_role_template_component_keeps_valid_templates_and_rolls_its_pods() {
    let template = json!({"spec": {"containers": [{
        "name": "main",
        "image": "nvcr.io/nvidia/ai-dynamo/vllm-runtime:1.5.0",
    }]}});
    let source = json!({
        "apiVersion": "nvidia.com/v1beta1",
        "kind": "DynamoGraphDeployment",
        "metadata": {"name": "p"},
        "spec": {
            "backendFramework": "vllm",
            "components": [{
                "name": "Frontend",
                "type": "worker",
                "scalingAdapter": {},
                "multinode": {"nodeCount": 2},
                "roles": [
                    {"name": "leader", "podTemplate": template},
                    {"name": "worker", "podTemplate": template},
                ],
            }],
        },
    });
    let mut pool = pool();
    pool.dgd = Some(source.to_string());
    let objects = children("p", "ns", &pool, Scaling::Operator).expect("objects");
    let dgd = find(&objects, "DynamoGraphDeployment", "p").expect("the DGD");
    let rendered = component(dgd, "Frontend");
    assert!(
        rendered.get("podTemplate").is_none(),
        "dynamo refuses podTemplate combined with roles[].podTemplate: {rendered}"
    );
    let checksum = gdl_k8s::checksum(find(&objects, "ConfigMap", "p").expect("the settings"));
    for role in rendered["roles"].as_array().expect("roles") {
        assert_eq!(
            role["podTemplate"]["metadata"]["annotations"]["gdl.dev/config"], checksum,
            "role {} must roll when its settings change",
            role["name"]
        );
    }
}

/// dynamo labels the DGD's pods with its name, the pool's, so a pool
/// whose name is longer than a label's value cannot have a DGD.
#[test]
fn a_pool_name_longer_than_a_label_cannot_have_a_dgd() {
    let name = "p".repeat(64);
    let mut dgd = dgd();
    dgd["metadata"]["name"] = name.clone().into();
    let mut pool = pool();
    pool.dgd = Some(dgd.to_string());
    let error = children(&name, "ns", &pool, Scaling::Fixed).expect_err("refused");
    assert!(error.contains("longer than 63"), "{error}");
}

/// Under the operator, the segment in the DGD has a scaler that writes
/// its component's scaling adapter, named as dynamo names it, and retires
/// each victim through the hub: the segment has no entry Service to take
/// a victim out of. A fixed pool has no scaler.
#[test]
fn a_dgd_segment_is_scaled_through_its_adapter() {
    let objects = children("p", "ns", &pool(), Scaling::Operator).expect("objects");
    let scaler = find(&objects, "GdlScaler", "p-pool").expect("the scaler");
    assert_eq!(
        scaler["spec"]["targetRef"],
        json!({
            "apiVersion": "nvidia.com/v1beta1",
            "kind": "DynamoGraphDeploymentScalingAdapter",
            "name": "p-frontend",
        })
    );
    assert_eq!(scaler["spec"]["retire"], "request");
    assert!(scaler["spec"]["drainBound"].is_string());
    assert!(scaler["spec"].get("service").is_none());
    let front = find(&objects, "GdlScaler", "p-front").expect("the other scaler");
    assert_eq!(front["spec"]["targetRef"]["kind"], "Deployment");

    let fixed = children("p", "ns", &pool(), Scaling::Fixed).expect("objects");
    assert!(fixed.iter().all(|o| o["kind"] != "GdlScaler"));
}

/// The operator scales a DGD segment only through an adapter it can name:
/// a component without `scalingAdapter`, or whose name dynamo would hash
/// into the adapter's, is refused. A fixed pool needs no adapter.
#[test]
fn a_dgd_segment_needs_an_adapter_the_scaler_can_name() {
    let with = |edit: &dyn Fn(&mut Value), segment: &str| {
        let mut dgd = dgd();
        edit(&mut dgd);
        let mut pool = pool();
        pool.dgd = Some(dgd.to_string());
        pool.segments[1].component = Some(segment.into());
        pool
    };
    let unscaled = with(
        &|d| {
            d["spec"]["components"][0]
                .as_object_mut()
                .expect("a component")
                .remove("scalingAdapter");
        },
        "Frontend",
    );
    let error = children("p", "ns", &unscaled, Scaling::Operator).expect_err("refused");
    assert!(error.contains("scalingAdapter"), "{error}");
    children("p", "ns", &unscaled, Scaling::Fixed).expect("a fixed pool needs no adapter");

    let hashed = with(
        &|d| d["spec"]["components"][0]["name"] = "Front_End".into(),
        "Front_End",
    );
    let error = children("p", "ns", &hashed, Scaling::Operator).expect_err("refused");
    assert!(error.contains("p-front_end"), "{error}");
}
