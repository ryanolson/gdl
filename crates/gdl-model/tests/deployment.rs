//! Hub registration identity checks.
use gdl_model::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};

#[test]
fn registration_must_match_graph_placement_and_segment() {
    let graph = Manifest {
        name: "test".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "Source".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "Echo".into(),
                role: Role::Sink,
                binding: Some("echo".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "Source".into(),
            inner: "Echo".into(),
            forward: vec![],
            backward: vec![],
            forward_types: vec![TypeInfo::new("Request", true, Default::default())],
            backward_types: vec![TypeInfo::new("Reply", true, Default::default())],
        }],
    };
    let placement = Placement::new().segment("s1", ["Source", "Echo"]);
    let spec = DeploymentSpec::new(graph, placement).expect("valid deployment");
    let registration = spec.registration("s1").expect("segment");
    assert!(spec.validate_registration(&registration).is_ok());
    let mut wrong = registration.clone();
    wrong.contract ^= 1;
    assert!(spec.validate_registration(&wrong).is_err());
    let mut wrong = registration.clone();
    wrong.placement ^= 1;
    assert!(spec.validate_registration(&wrong).is_err());
    let mut wrong = registration;
    wrong.segment = "unknown".into();
    assert!(spec.validate_registration(&wrong).is_err());
}
