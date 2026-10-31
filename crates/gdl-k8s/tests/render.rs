//! A pool's objects, rendered from the pool alone.

use gdl_k8s::{Env, Hub, Pool, Port, Protocol, Scaling, Segment, children};
use serde_json::Value;

/// A pool of two segments, one with an entry port.
fn pool() -> Pool {
    let segment = |name: &str, ports: Vec<Port>| Segment {
        name: name.into(),
        image: format!("registry.example/{name}:tag"),
        settings: "[system]\nport = 9090\n".into(),
        quorum: 2,
        ports,
        env: vec![Env {
            name: "GDL_HUB__HOST".into(),
            value: "p".into(),
        }],
        secrets: Vec::new(),
        resources: None,
    };
    Pool {
        fleet: "api".into(),
        cluster: "use1".into(),
        class: "global".into(),
        region: "us-east-1".into(),
        deployment: "{\"graph\": 1}".into(),
        hub: Hub {
            image: "registry.example/hub:tag".into(),
            env: Vec::new(),
            resources: None,
            state_dir: None,
        },
        segments: vec![
            segment("engine", Vec::new()),
            segment(
                "front",
                vec![Port {
                    port: 8080,
                    protocol: Protocol::Tcp,
                }],
            ),
        ],
        service_account_name: "api".into(),
        node_selector: Default::default(),
        tolerations: Vec::new(),
        parent: None,
        dynamodb: None,
        pod_monitor: false,
        presence: Default::default(),
        allow_pool_restart: false,
    }
}

/// The pod template of the Deployment `name` among `objects`.
fn template<'a>(objects: &'a [Value], name: &str) -> &'a Value {
    &objects
        .iter()
        .find(|o| o["kind"] == "Deployment" && o["metadata"]["name"] == name)
        .expect("the Deployment")["spec"]["template"]
}

/// A hub restart drains its pool, so the hub's pods are replaced only when
/// what the hub reads changes: the pool's deployment, and not a segment's
/// settings, which replace that segment's pods alone.
#[test]
fn a_segment_settings_change_replaces_its_pods_and_never_the_hub() {
    let before = children("p", "ns", &pool(), Scaling::Operator).expect("objects");
    let mut changed = pool();
    changed.segments[0].settings = "[system]\nport = 9090\n[runtime]\ndrain_ms = 1000\n".into();
    let after = children("p", "ns", &changed, Scaling::Operator).expect("objects");
    assert_eq!(template(&before, "p"), template(&after, "p"));
    assert_ne!(template(&before, "p-engine"), template(&after, "p-engine"));

    let mut moved = pool();
    moved.deployment = "{\"graph\": 2}".into();
    let after = children("p", "ns", &moved, Scaling::Operator).expect("objects");
    assert_ne!(template(&before, "p"), template(&after, "p"));
}

/// Under the operator a segment's Deployment carries no `replicas`, its
/// pods say they take connections, its entry Service selects on that, and
/// it has a `GdlScaler`; fixed, the Deployment runs the quorum.
#[test]
fn the_operator_scales_what_a_fixed_pool_counts() {
    let scaled = children("p", "ns", &pool(), Scaling::Operator).expect("objects");
    let front = scaled
        .iter()
        .find(|o| o["kind"] == "Deployment" && o["metadata"]["name"] == "p-front")
        .expect("front");
    assert!(front["spec"].get("replicas").is_none());
    assert_eq!(
        front["spec"]["template"]["metadata"]["labels"]["gdl.dev/routable"],
        "true"
    );
    let kinds: Vec<&str> = scaled.iter().filter_map(|o| o["kind"].as_str()).collect();
    assert_eq!(
        kinds,
        [
            "ConfigMap",
            "Deployment",
            "Service",
            "Deployment",
            "GdlScaler",
            "Deployment",
            "Service",
            "GdlScaler"
        ]
    );
    let fixed = children("p", "ns", &pool(), Scaling::Fixed).expect("objects");
    let front = fixed
        .iter()
        .find(|o| o["kind"] == "Deployment" && o["metadata"]["name"] == "p-front")
        .expect("front");
    assert_eq!(front["spec"]["replicas"], 2);
    assert!(fixed.iter().all(|o| o["kind"] != "GdlScaler"));
}
