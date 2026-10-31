//! The `GdlPool` that `cargo gdl` writes, read as the operator reads it.

use gdl_operator::GdlPool;

fn pool(presence: gdl_k8s::Presence) -> gdl_k8s::Pool {
    gdl_k8s::Pool {
        fleet: "api".into(),
        cluster: "use1".into(),
        class: "model".into(),
        region: "us-east-1".into(),
        deployment: "{}".into(),
        hub: gdl_k8s::Hub {
            image: "registry.example/hub:tag".into(),
            env: Vec::new(),
            resources: None,
            state_dir: None,
        },
        segments: Vec::new(),
        service_account_name: "api".into(),
        node_selector: Default::default(),
        tolerations: Vec::new(),
        parent: Some(gdl_k8s::Parent::Home("global".into())),
        dynamodb: None,
        pod_monitor: true,
        presence,
        allow_pool_restart: false,
    }
}

/// The object gdl-k8s renders is a `GdlPool`, its spec the pool whole.
/// A pool that serves leaves `presence` out, so a redeploy owns no
/// presence a person set, and the operator reads it as serving.
#[test]
fn a_rendered_pool_reads_as_a_gdl_pool() {
    let serving = pool(gdl_k8s::Presence::Serving);
    let object = gdl_k8s::pool_object("p", "ns", &serving);
    assert!(object["spec"].get("presence").is_none(), "{object}");
    assert!(object["spec"].get("allowPoolRestart").is_none(), "{object}");
    assert_eq!(object["metadata"]["labels"]["gdl.dev/pool"], "p");
    let read: GdlPool = serde_json::from_value(object).expect("a GdlPool");
    assert_eq!(read.spec.pool, serving);
    assert_eq!(read.metadata.namespace.as_deref(), Some("ns"));

    let zero = pool(gdl_k8s::Presence::ScaledToZero);
    let object = gdl_k8s::pool_object("p", "ns", &zero);
    assert_eq!(object["spec"]["presence"], "scaledToZero");
    let read: GdlPool = serde_json::from_value(object).expect("a GdlPool");
    assert_eq!(read.spec.pool, zero);
}
