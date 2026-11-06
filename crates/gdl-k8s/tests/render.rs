//! A pool's objects, rendered from the pool alone.

use gdl_k8s::{Env, Hub, Pool, Port, Protocol, Scaling, Segment, children};
use serde_json::{Value, json};

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
        component: None,
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
        dgd: None,
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

/// A pool's PodMonitor is named after it, which the API server keeps its
/// own in a namespace, and selects only its pods.
#[test]
fn a_pool_monitor_selects_the_pools_pods() {
    let monitor = gdl_k8s::pool_monitor("p", "ns", &pool());
    assert_eq!(monitor["metadata"]["name"], "p");
    assert_eq!(
        monitor["spec"]["selector"]["matchLabels"],
        serde_json::json!({"gdl.dev/fleet": "api", "gdl.dev/cluster": "use1", "gdl.dev/pool": "p"})
    );
    let objects = children("p", "ns", &pool(), Scaling::Operator).expect("objects");
    let hub = template(&objects, "p");
    for (key, value) in monitor["spec"]["selector"]["matchLabels"]
        .as_object()
        .expect("labels")
    {
        assert_eq!(&hub["metadata"]["labels"][key], value, "{key}");
    }
}

/// The volume and the mount of the rollover ConfigMap in the pod template
/// `template`, if it has them.
fn rollover(template: &Value) -> (Option<&Value>, Option<&Value>) {
    let volume = template["spec"]["volumes"]
        .as_array()
        .expect("volumes")
        .iter()
        .find(|v| v["name"] == "rollover");
    let mount = template["spec"]["containers"][0]["volumeMounts"]
        .as_array()
        .expect("mounts")
        .iter()
        .find(|m| m["name"] == "rollover");
    (volume, mount)
}

/// Every segment's pods mount the pool's rollover ConfigMap, optional, so
/// that a pod starts without it, as a whole directory, since a `subPath`
/// mount never sees the map change. The hub's pods do not: a change to
/// the hub's template restarts the pool, and the operator holds it
/// without `allowPoolRestart`. The same for `cargo gdl k8s` and the
/// operator.
#[test]
fn every_segment_mounts_the_rollover_map_and_the_hub_does_not() {
    for scaling in [Scaling::Fixed, Scaling::Operator] {
        let objects = children("p", "ns", &pool(), scaling).expect("objects");
        for name in ["p-engine", "p-front"] {
            let (volume, mount) = rollover(template(&objects, name));
            assert_eq!(
                volume,
                Some(
                    &json!({"name": "rollover", "configMap": {"name": "p-rollover", "optional": true}})
                ),
                "{name} {scaling:?}"
            );
            assert_eq!(
                mount,
                Some(
                    &json!({"name": "rollover", "mountPath": "/etc/gdl/rollover", "readOnly": true})
                ),
                "{name} {scaling:?}"
            );
        }
        assert_eq!(
            rollover(template(&objects, "p")),
            (None, None),
            "{scaling:?}"
        );
    }
}

/// The rollover ConfigMap is named from the name the pool's own ConfigMap
/// takes, so the two are named alike whatever that name is.
#[test]
fn the_rollover_map_is_named_from_the_pools_config_map() {
    let name = "a-pool-of-a-long-name";
    let objects = children(name, "ns", &pool(), Scaling::Operator).expect("objects");
    let config = objects
        .iter()
        .find(|o| o["kind"] == "ConfigMap")
        .expect("the ConfigMap");
    let engine = template(&objects, &format!("{name}-engine"));
    let (volume, _) = rollover(engine);
    assert_eq!(
        volume.expect("the volume")["configMap"]["name"],
        format!(
            "{}-rollover",
            config["metadata"]["name"].as_str().expect("a name")
        )
    );
    assert_eq!(gdl_k8s::rollover(name), format!("{name}-rollover"));
}

/// The rollover ConfigMap is outside every checksum: no rendered object
/// is it, so it is never the pool's to write, and a segment's
/// `gdl.dev/config` covers only the pool's own ConfigMap. Its pod
/// template names it and holds nothing of it, so writing it changes no
/// pod template and replaces no pod.
#[test]
fn the_rollover_map_is_outside_every_checksum() {
    for scaling in [Scaling::Fixed, Scaling::Operator] {
        let objects = children("p", "ns", &pool(), scaling).expect("objects");
        assert!(
            objects
                .iter()
                .all(|o| o["metadata"]["name"] != "p-rollover"),
            "{scaling:?}"
        );
        let config = objects
            .iter()
            .find(|o| o["kind"] == "ConfigMap")
            .expect("the ConfigMap");
        let sum = gdl_k8s::checksum(config);
        for name in ["p-engine", "p-front"] {
            assert_eq!(
                template(&objects, name)["metadata"]["annotations"]["gdl.dev/config"],
                sum,
                "{name} {scaling:?}"
            );
        }
    }
}

/// Each segment's container gets `GLIBC_TUNABLES` with malloc's huge
/// pages off when it has none, under either scaling: `cargo gdl k8s` and
/// the operator render pods here. One it has is the user's, and is left
/// byte for byte, a list glibc refuses included. The hub's container does
/// not get it.
#[test]
fn a_segment_container_runs_with_malloc_off_huge_pages() {
    let env = |objects: &[Value], name: &str| -> Value {
        template(objects, name)["spec"]["containers"][0]["env"].clone()
    };
    for (own, expected) in [
        (None, "glibc.malloc.hugetlb=0"),
        (Some("glibc.malloc.arena_max=4"), "glibc.malloc.arena_max=4"),
        (Some("glibc.malloc.hugetlb"), "glibc.malloc.hugetlb"),
        (Some("glibc.malloc.hugetlb=0:"), "glibc.malloc.hugetlb=0:"),
        (Some("glibc.malloc.hugetlb=1"), "glibc.malloc.hugetlb=1"),
        (Some(""), ""),
    ] {
        let mut pool = pool();
        if let Some(own) = own {
            pool.segments[0].env.push(Env {
                name: gdl_model::GLIBC_TUNABLES.into(),
                value: own.into(),
            });
        }
        for scaling in [Scaling::Operator, Scaling::Fixed] {
            let objects = children("p", "ns", &pool, scaling).expect("objects");
            assert_eq!(
                env(&objects, "p-engine"),
                json!([
                    {"name": "GDL_HUB__HOST", "value": "p"},
                    {"name": "GLIBC_TUNABLES", "value": expected},
                ]),
                "{own:?}"
            );
            assert_eq!(
                env(&objects, "p-front"),
                json!([
                    {"name": "GDL_HUB__HOST", "value": "p"},
                    {"name": "GLIBC_TUNABLES", "value": "glibc.malloc.hugetlb=0"},
                ])
            );
            let hub = env(&objects, "p");
            assert!(!hub.to_string().contains("GLIBC_TUNABLES"), "{hub}");
        }
    }
}
