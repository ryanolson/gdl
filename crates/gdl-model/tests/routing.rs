//! Request router validation and deployment identity.

use gdl_model::{HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo, layout};

fn manifest() -> Manifest {
    Manifest {
        name: "routed".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "D".into(),
                role: Role::Sink,
                binding: Some("d".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("Request", true)],
            backward_types: vec![TypeInfo::new("Reply", true)],
        }],
    }
}
const SPLIT: &str = "[segment.front]\nnodes=['S']\n[segment.worker]\nnodes=['D']\n";

#[test]
fn request_routing_survives_export_and_changes_deployment_identity() {
    let plain = Placement::from_toml(SPLIT).expect("routing fixture");
    let random = Placement::from_toml(&format!("{SPLIT}\n[routing]\nD='random'\n"))
        .expect("routing fixture");
    assert_ne!(plain.hash(), random.hash());
    assert!(
        toml::to_string(&random)
            .expect("routing fixture")
            .contains("random")
    );
    let routed = layout(&manifest(), &random).expect("routing fixture");
    assert_eq!(
        routed.request_router(0, 1),
        gdl_model::RequestRouter::Random
    );
    assert_eq!(
        routed.request_router(0, 0),
        gdl_model::RequestRouter::RoundRobin
    );
}

#[test]
fn routers_must_target_request_boundaries() {
    for target in ["S", "Missing"] {
        let placement = Placement::from_toml(&format!("{SPLIT}\n[routing]\n{target}='random'\n"))
            .expect("routing fixture");
        assert!(
            layout(&manifest(), &placement).is_err(),
            "accepted {target}"
        );
    }
    let local = Placement::from_toml("[segment.all]\nnodes=['S','D']\n[routing]\nD='random'")
        .expect("routing fixture");
    assert!(layout(&manifest(), &local).is_err());
    assert!(Placement::from_toml(&format!("{SPLIT}\n[routing]\nD='custom'\n")).is_err());
}
