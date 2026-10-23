//! Hub registration identity checks, and the names of a segment's hub
//! services.
use gdl_model::{
    DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, SegmentPhase, TypeInfo,
    segment_service,
};

/// Every instance and every hub of a deployment name a segment's services
/// by this rule, so a change to it is a change to the hub protocol: the
/// names are pinned.
#[test]
fn a_segments_hub_services_are_named_by_phase_and_hex() {
    let names: Vec<String> = [
        SegmentPhase::Loaded,
        SegmentPhase::Serving,
        SegmentPhase::Ready,
        SegmentPhase::Drained,
        SegmentPhase::Retired,
    ]
    .into_iter()
    .map(|phase| segment_service("model-e", phase))
    .collect();
    assert_eq!(
        names,
        [
            "gdl-loaded-6d6f64656c2d65",
            "gdl-serving-6d6f64656c2d65",
            "gdl-ready-6d6f64656c2d65",
            "gdl-drained-6d6f64656c2d65",
            "gdl-retired-6d6f64656c2d65",
        ]
    );
}

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
            forward_types: vec![TypeInfo::new("Request", true)],
            backward_types: vec![TypeInfo::new("Reply", true)],
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
