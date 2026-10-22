//! Classes: named sets of segments that deploy together. A class is part of
//! the placement's identity; a placement without classes keeps the identity
//! it had before classes existed (`tests/identity.rs`).

use gdl_model::{
    DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, PlacementErrors, Role, TypeInfo, layout,
};

/// `S -> F -> D`: a source, a forward node, and a sink.
fn manifest() -> Manifest {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    Manifest {
        name: "classed".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("F", Role::Forward),
            node("D", Role::Sink),
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec!["F".into()],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![
                TypeInfo::new("Request", true),
                TypeInfo::new("Tokens", true),
            ],
            backward_types: vec![TypeInfo::new("Reply", true)],
        }],
    }
}

const SEGMENTS: &str =
    "[segment.a]\nnodes = ['S']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = ['D']\n";

const CLASSES: &str = "[class.global]\nsegments = ['a', 'b']\n[class.model_x]\nsegments = ['d']\n";

fn parsed(classes: &str) -> Placement {
    Placement::from_toml(&format!("{SEGMENTS}{classes}")).expect("a placement")
}

fn refused(placement: &Placement) -> PlacementErrors {
    match layout(&manifest(), placement) {
        Ok(_) => panic!("laid out {placement:?}"),
        Err(errors) => errors,
    }
}

#[test]
fn any_class_table_changes_the_hash() {
    let plain = parsed("");
    let classed = parsed(CLASSES);
    assert_ne!(classed.hash(), plain.hash());
    // One class of every segment is what a placement without classes means,
    // but the class's name is part of the deployment's identity.
    let one = parsed("[class.global]\nsegments = ['a', 'b', 'd']\n");
    assert_ne!(one.hash(), plain.hash());
    assert_ne!(one.hash(), classed.hash());
    let renamed = parsed("[class.everything]\nsegments = ['a', 'b', 'd']\n");
    assert_ne!(renamed.hash(), one.hash());
}

#[test]
fn moving_or_renaming_a_class_changes_the_hash() {
    let classed = parsed(CLASSES);
    let moved =
        parsed("[class.global]\nsegments = ['a']\n[class.model_x]\nsegments = ['b', 'd']\n");
    assert_ne!(moved.hash(), classed.hash());
    let renamed =
        parsed("[class.global]\nsegments = ['a', 'b']\n[class.model_y]\nsegments = ['d']\n");
    assert_ne!(renamed.hash(), classed.hash());
}

#[test]
fn the_order_of_segments_and_of_class_tables_does_not_change_the_hash() {
    let classed = parsed(CLASSES);
    let reordered =
        parsed("[class.model_x]\nsegments = ['d']\n[class.global]\nsegments = ['b', 'a']\n");
    assert_eq!(reordered.hash(), classed.hash());
    assert_eq!(reordered, classed.clone().class("global", ["b", "a"]));
}

/// The hash of a classed placement, pinned, so that an encoding change is
/// a decision rather than an accident.
#[test]
fn a_classed_placement_hash_is_pinned() {
    assert_eq!(parsed(CLASSES).hash(), 0xf7ad_bcf6_e339_18fd);
}

/// A class lists segments as a segment lists nodes, so an encoding that
/// wrote a class as it writes a segment would make a class `global` of
/// segments `A` and `B` hash as a segment `global` of nodes `A` and `B`.
#[test]
fn a_class_does_not_hash_as_a_segment() {
    let segment = Placement::from_toml(
        "[segment.A]\nnodes = ['x']\n[segment.B]\nnodes = ['y']\n[segment.global]\nnodes = ['A', 'B']\n",
    )
    .expect("a placement");
    let class = Placement::from_toml(
        "[segment.A]\nnodes = ['x']\n[segment.B]\nnodes = ['y']\n[class.global]\nsegments = ['A', 'B']\n",
    )
    .expect("a placement");
    assert_ne!(segment.hash(), class.hash());
    // Nor as a request router: `A = "random"` is a node and a policy, as
    // a one-segment class is a name and a list.
    let routed = Placement::from_toml("[segment.A]\nnodes = ['x']\n[routing]\nx = 'random'\n")
        .expect("a placement");
    let class = Placement::from_toml("[segment.A]\nnodes = ['x']\n[class.x]\nsegments = ['A']\n")
        .expect("a placement");
    assert_ne!(routed.hash(), class.hash());
}

#[test]
fn classes_survive_toml_and_the_hubs_json() {
    let classed = parsed(CLASSES);
    let text = toml::to_string(&classed).expect("toml");
    assert!(text.contains("[class.global]"), "{text}");
    let back = Placement::from_toml(&text).expect("reads what it wrote");
    assert_eq!(back, classed, "{text}");
    assert_eq!(back.hash(), classed.hash());

    let spec = DeploymentSpec::new(manifest(), classed.clone()).expect("a deployment");
    let json = serde_json::to_string(&spec).expect("json");
    let back: DeploymentSpec = serde_json::from_str(&json).expect("reads what it wrote");
    assert_eq!(back.placement, classed, "{json}");
    assert_eq!(back.placement.hash(), classed.hash());
}

#[test]
fn a_valid_class_table_is_laid_out_with_each_segments_class() {
    let classed = layout(&manifest(), &parsed(CLASSES)).expect("a layout");
    assert_eq!(classed.class_of("a"), Some("global"));
    assert_eq!(classed.class_of("b"), Some("global"));
    assert_eq!(classed.class_of("d"), Some("model_x"));
    assert_eq!(classed.class_of("zz"), None);
    let plain = layout(&manifest(), &parsed("")).expect("a layout");
    assert_eq!(plain.class_of("a"), None);

    let placement = parsed(CLASSES);
    let classes: Vec<(&str, Vec<&str>)> = placement
        .classes()
        .map(|(name, segments)| (name, segments.iter().map(String::as_str).collect()))
        .collect();
    assert_eq!(
        classes,
        [("global", vec!["a", "b"]), ("model_x", vec!["d"])]
    );
    let built = Placement::new()
        .segment("a", ["S"])
        .segment("b", ["F"])
        .segment("d", ["D"])
        .class("model_x", ["d"])
        .class("global", ["a", "b"]);
    assert_eq!(built, parsed(CLASSES));
}

/// Each refusal, as `layout` reports it, from a parsed placement and from
/// the same placement built in code.
#[test]
fn a_bad_class_table_is_refused_by_layout() {
    let built = || {
        Placement::new()
            .segment("a", ["S"])
            .segment("b", ["F"])
            .segment("d", ["D"])
    };
    let cases: [(&str, Placement, &str); 5] = [
        (
            "[class.global]\nsegments = ['a', 'b', 'zz']\n[class.model_x]\nsegments = ['d']\n",
            built()
                .class("global", ["a", "b", "zz"])
                .class("model_x", ["d"]),
            "class `global` lists unknown segment `zz`",
        ),
        (
            "[class.global]\nsegments = ['a', 'b', 'd']\n[class.model_x]\nsegments = []\n",
            built()
                .class("global", ["a", "b", "d"])
                .class("model_x", Vec::<String>::new()),
            "class `model_x` has no segments",
        ),
        (
            "[class.global]\nsegments = ['a', 'b', 'a']\n[class.model_x]\nsegments = ['d']\n",
            built()
                .class("global", ["a", "b", "a"])
                .class("model_x", ["d"]),
            "class `global` lists segment `a` twice",
        ),
        (
            "[class.global]\nsegments = ['a', 'b']\n",
            built().class("global", ["a", "b"]),
            "segment `d` is in no class",
        ),
        (
            "[class.global]\nsegments = ['a', 'b']\n[class.model_x]\nsegments = ['b', 'd']\n",
            built()
                .class("global", ["a", "b"])
                .class("model_x", ["b", "d"]),
            "segment `b` is in several classes: [\"global\", \"model_x\"]",
        ),
    ];
    for (classes, built, expected) in cases {
        let from_text = parsed(classes);
        assert_eq!(from_text, built, "{classes}");
        assert_eq!(refused(&from_text).to_string(), expected, "{classes}");
        assert_eq!(refused(&built).to_string(), expected, "{classes}");
    }
}

/// A class name is a TOML key and a value the environment carries, as a
/// selector's name is, so it has the same shape; `round_robin` and
/// `random` are policies' names, not classes'.
#[test]
fn a_class_name_has_a_selector_names_shape() {
    for bad in [
        "",
        "Global",
        "9x",
        "model-x",
        "model__x",
        "model_",
        &"x".repeat(65),
    ] {
        let text = format!("{SEGMENTS}[class.\"{bad}\"]\nsegments = ['a', 'b', 'd']\n");
        let Err(error) = Placement::from_toml(&text) else {
            panic!("accepted `{bad}`");
        };
        assert!(error.to_string().contains("class"), "{error}");
        let built = Placement::new()
            .segment("a", ["S"])
            .segment("b", ["F"])
            .segment("d", ["D"])
            .class(bad, ["a", "b", "d"]);
        let errors = refused(&built).to_string();
        assert!(
            errors.starts_with(&format!("class `{bad}`: class name `{bad}` must be")),
            "{errors}"
        );
    }
    for good in [
        "global",
        "model_x",
        "x9",
        "round_robin",
        "random",
        &"x".repeat(64),
    ] {
        let text = format!("{SEGMENTS}[class.\"{good}\"]\nsegments = ['a', 'b', 'd']\n");
        let placement = Placement::from_toml(&text).expect("a usable name");
        layout(&manifest(), &placement).expect("a layout");
    }
}

#[test]
fn a_class_table_has_only_its_segments() {
    for bad in [
        "[class.global]\nsegments = ['a', 'b', 'd']\npolicy = 'home_first'\n",
        "[class.global]\nsegments = 3\n",
        "[class.global]\nnodes = ['S']\n",
    ] {
        assert!(
            Placement::from_toml(&format!("{SEGMENTS}{bad}")).is_err(),
            "accepted {bad}"
        );
    }
}

/// Class problems are reported with the placement's other problems.
#[test]
fn every_problem_is_reported_together() {
    let placement = Placement::from_toml(
        "[segment.a]\nnodes = ['S', 'Nope']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = ['D']\n[segment.e]\nnodes = []\n\
         [class.global]\nsegments = ['a', 'zz']\n[class.model_x]\nsegments = ['d', 'd']\n[class.empty]\nsegments = []\n",
    )
    .expect("a placement");
    let errors = refused(&placement).to_string();
    for expected in [
        "segment `a` lists unknown node `Nope`",
        "segment `e` has no nodes",
        "class `global` lists unknown segment `zz`",
        "class `model_x` lists segment `d` twice",
        "class `empty` has no segments",
        "segment `b` is in no class",
        "segment `e` is in no class",
    ] {
        assert!(errors.contains(expected), "{expected} in: {errors}");
    }
}
