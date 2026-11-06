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

/// A one-segment deployment, `s1`.
fn spec() -> DeploymentSpec {
    let graph = Manifest {
        name: "test".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "Source".into(),
                role: Role::Source,
                binding: None,
                calls: Vec::new(),
                unary: None,
            },
            NodeInfo {
                name: "Echo".into(),
                role: Role::Sink,
                binding: Some("echo".into()),
                calls: Vec::new(),
                unary: None,
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
        properties: Vec::new(),
    };
    let placement = Placement::new().segment("s1", ["Source", "Echo"]);
    DeploymentSpec::new(graph, placement).expect("valid deployment")
}

#[test]
fn registration_must_match_graph_placement_and_segment() {
    let spec = spec();
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

/// A hub of an earlier build refuses a field it does not know, and the
/// runtime reads the hub's `/members` with the same type, so a
/// registration that says nothing of when it leaves is sent as it always
/// was.
#[test]
fn a_registration_without_leave_by_is_sent_byte_for_byte_as_before() {
    let registration = spec().registration("s1").expect("segment");
    assert_eq!(
        serde_json::to_string(&registration).expect("serializes"),
        format!(
            r#"{{"segment":"s1","contract":{},"placement":{}}}"#,
            registration.contract, registration.placement
        )
    );
}

/// When an instance will leave is not part of what it was built for: the
/// hub admits it whatever it says, and still refuses a binary built for
/// another graph or placement.
#[test]
fn a_registration_that_says_when_it_leaves_is_checked_by_its_build_alone() {
    let spec = spec();
    let mut registration = spec.registration("s1").expect("segment");
    registration.leave_by = Some(1_800_000_000_000);
    assert!(spec.validate_registration(&registration).is_ok());
    let sent = serde_json::to_string(&registration).expect("serializes");
    assert!(sent.ends_with(r#","leave_by":1800000000000}"#), "{sent}");
    let read: gdl_model::SegmentRegistration = serde_json::from_str(&sent).expect("reads");
    assert_eq!(read, registration);
    let mut wrong = registration.clone();
    wrong.contract ^= 1;
    assert!(spec.validate_registration(&wrong).is_err());
    let mut wrong = registration;
    wrong.placement ^= 1;
    assert!(spec.validate_registration(&wrong).is_err());
}
