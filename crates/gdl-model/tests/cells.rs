//! A placement's cell policies: `[cells.<class>]`, how a chooser picks a
//! cell of the class it sends to. A default is not recorded, so a placement
//! that spells one out is the placement that leaves it unsaid. Each case is
//! checked as parsed and as built in code.

use gdl_model::{
    CellPolicy, DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, PlacementErrors,
    RequestRouter, Role, TypeInfo, layout,
};

/// `S => F { D }`: F's own hop crosses from `global` into `model_x` at its
/// inner end, so `model_x` is the one class a boundary enters.
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
            node("F", Role::Operator),
            node("D", Role::Sink),
        ],
        hops: vec![
            HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "F".into(),
                backward: vec![],
                forward_types: vec![TypeInfo::new("Request", true, Default::default())],
                backward_types: vec![TypeInfo::new("Reply", true, Default::default())],
            },
            HopInfo {
                outer: "F".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![TypeInfo::new("Tokens", true, Default::default())],
                backward_types: vec![TypeInfo::new("Reply", true, Default::default())],
            },
        ],
    }
}

const SEGMENTS: &str =
    "[segment.a]\nnodes = ['S']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = ['D']\n";

const CLASSES: &str = "[class.global]\nsegments = ['a', 'b']\n[class.model_x]\nsegments = ['d']\n";

/// The hash of `CLASSES` at the commit before cell policies existed.
const CLASSED_HASH: u64 = 0xf7ad_bcf6_e339_18fd;

fn parsed(extra: &str) -> Placement {
    Placement::from_toml(&format!("{SEGMENTS}{CLASSES}{extra}")).expect("a placement")
}

fn segments() -> Placement {
    Placement::new()
        .segment("a", ["S"])
        .segment("b", ["F"])
        .segment("d", ["D"])
}

fn built() -> Placement {
    segments()
        .class("global", ["a", "b"])
        .class("model_x", ["d"])
}

fn policy(policy: &str, spill_rtt_ms: u64, max_near: u32) -> CellPolicy {
    CellPolicy {
        policy: policy.into(),
        spill_rtt_ms,
        max_near,
    }
}

fn refusals(placement: &Placement) -> Vec<String> {
    match layout(&manifest(), placement) {
        Ok(_) => panic!("laid out {placement:?}"),
        Err(PlacementErrors(errors)) => errors.iter().map(ToString::to_string).collect(),
    }
}

#[test]
fn a_classed_placement_without_cells_keeps_its_hash() {
    assert_eq!(parsed("").hash(), CLASSED_HASH);
    assert_eq!(built().hash(), CLASSED_HASH);
}

/// Spelled-out defaults, and a table holding only defaults, are no table:
/// the same placement, hash, and text, whatever class the table names.
/// Its name is still checked: see
/// `a_spelled_default_for_a_missing_name_is_refused`.
#[test]
fn defaults_are_not_recorded() {
    let plain = parsed("");
    let text = toml::to_string(&plain).expect("toml");
    for extra in [
        "[cells.model_x]\npolicy = 'home_first'\n",
        "[cells.model_x]\nspill_rtt_ms = 80\n",
        "[cells.model_x]\nmax_near = 16\n",
        "[cells.model_x]\npolicy = 'home_first'\nspill_rtt_ms = 80\nmax_near = 16\n",
        "[cells.model_x]\n",
        "[cells.global]\nmax_near = 16\n",
        "[cells.zz]\npolicy = 'home_first'\n",
    ] {
        let spelled = parsed(extra);
        assert_eq!(spelled, plain, "{extra}");
        assert_eq!(spelled.hash(), CLASSED_HASH, "{extra}");
        assert_eq!(toml::to_string(&spelled).expect("toml"), text, "{extra}");
        // A class that no boundary enters, or that the placement lacks, is
        // still refused by name (below).
        if extra.starts_with("[cells.model_x]") {
            layout(&manifest(), &spelled).expect("a layout");
        }
    }
    for class in ["model_x", "global", "zz"] {
        let spelled = built().cell_policy(class, CellPolicy::default());
        assert_eq!(spelled, plain, "{class}");
        assert_eq!(spelled.hash(), CLASSED_HASH, "{class}");
    }
    // Setting a default removes what was set before, as `route` does.
    let reset = built()
        .cell_policy("model_x", policy("nearest", 40, 2))
        .cell_policy("model_x", CellPolicy::default());
    assert_eq!(reset, plain);
}

/// A table that keeps one value records only that value.
#[test]
fn a_default_beside_a_value_is_dropped() {
    let spelled = parsed("[cells.model_x]\npolicy = 'home_first'\nmax_near = 4\n");
    let terse = parsed("[cells.model_x]\nmax_near = 4\n");
    assert_eq!(spelled, terse);
    assert_eq!(spelled.hash(), terse.hash());
    assert_eq!(
        spelled,
        built().cell_policy("model_x", policy("home_first", 80, 4))
    );
    let text = toml::to_string(&spelled).expect("toml");
    assert!(text.ends_with("[cells.model_x]\nmax_near = 4\n"), "{text}");
}

#[test]
fn each_value_other_than_its_default_changes_the_hash() {
    let cases = [
        (
            "[cells.model_x]\npolicy = 'nearest'\n",
            policy("nearest", 80, 16),
        ),
        (
            "[cells.model_x]\nspill_rtt_ms = 40\n",
            policy("home_first", 40, 16),
        ),
        (
            "[cells.model_x]\nmax_near = 0\n",
            policy("home_first", 80, 0),
        ),
        (
            "[cells.model_x]\npolicy = 'nearest'\nspill_rtt_ms = 0\nmax_near = 1\n",
            policy("nearest", 0, 1),
        ),
        // The class the table names is part of the hash too.
        (
            "[cells.global]\nmax_near = 0\n",
            CellPolicy {
                max_near: 0,
                ..CellPolicy::default()
            },
        ),
    ];
    let mut hashes = vec![CLASSED_HASH];
    for (extra, value) in cases {
        let class = if extra.contains("global") {
            "global"
        } else {
            "model_x"
        };
        let from_text = parsed(extra);
        assert_eq!(from_text, built().cell_policy(class, value), "{extra}");
        let hash = from_text.hash();
        assert!(!hashes.contains(&hash), "{extra} hashes as another case");
        hashes.push(hash);
    }
}

/// The hash of one cell policy, pinned, so that an encoding change is a
/// decision rather than an accident.
#[test]
fn a_cell_policy_hash_is_pinned() {
    assert_eq!(
        parsed("[cells.model_x]\nmax_near = 4\n").hash(),
        CELL_POLICY_HASH
    );
}

const CELL_POLICY_HASH: u64 = 0xbae5_043d_824b_fb00;

/// The cells block starts where a class's name would, and a placement with
/// no classes still hashes its cells, so neither reads as the other.
#[test]
fn a_cell_policy_does_not_hash_as_a_class_or_a_router() {
    let cells =
        Placement::from_toml(&format!("{SEGMENTS}[cells.x]\npolicy = 'd'\n")).expect("a placement");
    let class = Placement::from_toml(&format!("{SEGMENTS}[class.x]\nsegments = ['d']\n"))
        .expect("a placement");
    let routed = Placement::from_toml(&format!("{SEGMENTS}[routing]\nx = {{ selector = 'd' }}\n"))
        .expect("a placement");
    let plain = Placement::from_toml(SEGMENTS).expect("a placement");
    let hashes = [cells.hash(), class.hash(), routed.hash(), plain.hash()];
    for (i, a) in hashes.iter().enumerate() {
        for b in &hashes[i + 1..] {
            assert_ne!(a, b, "{hashes:x?}");
        }
    }
}

#[test]
fn cell_policies_survive_toml_and_the_hubs_json() {
    let placement = parsed("[cells.model_x]\npolicy = 'nearest'\nspill_rtt_ms = 40\n");
    let text = toml::to_string(&placement).expect("toml");
    assert!(text.contains("[cells.model_x]"), "{text}");
    let back = Placement::from_toml(&text).expect("reads what it wrote");
    assert_eq!(back, placement, "{text}");
    assert_eq!(back.hash(), placement.hash());

    let spec = DeploymentSpec::new(manifest(), placement.clone()).expect("a deployment");
    let json = serde_json::to_string(&spec).expect("json");
    let back: DeploymentSpec = serde_json::from_str(&json).expect("reads what it wrote");
    assert_eq!(back.placement, placement, "{json}");
    assert_eq!(back.placement.hash(), placement.hash());

    // Defaults are dropped whichever way the placement is read.
    let plain = DeploymentSpec::new(manifest(), parsed("")).expect("a deployment");
    let mut json: serde_json::Value = serde_json::to_value(&plain).expect("json");
    json["placement"]["cells"] = serde_json::json!({ "model_x": { "max_near": 16 } });
    let back: DeploymentSpec = serde_json::from_value(json).expect("a deployment");
    assert_eq!(back.placement, parsed(""));
    assert_eq!(back.placement.hash(), CLASSED_HASH);
}

#[test]
fn a_cells_table_has_only_its_three_keys() {
    for bad in [
        "[cells.model_x]\nmax_near = 4\nspill = 3\n",
        // Refused whatever the other values are.
        "[cells.model_x]\npolicy = 'home_first'\nsegments = ['d']\n",
        "[cells.model_x]\nmax_near = -1\n",
        "[cells.model_x]\nspill_rtt_ms = -1\n",
        "[cells.model_x]\nspill_rtt_ms = '80'\n",
        "[cells]\nmodel_x = 'home_first'\n",
    ] {
        assert!(
            Placement::from_toml(&format!("{SEGMENTS}{CLASSES}{bad}")).is_err(),
            "accepted {bad}"
        );
    }
}

/// The effective policy of each class a boundary enters, defaults applied;
/// none for a class no boundary enters.
#[test]
fn a_layout_gives_each_entered_class_its_policy() {
    for placement in [parsed(""), built()] {
        let laid = layout(&manifest(), &placement).expect("a layout");
        assert_eq!(laid.cell_policy("model_x"), Some(&CellPolicy::default()));
        assert_eq!(laid.cell_policy("global"), None);
        assert_eq!(laid.cell_policy("zz"), None);
    }
    assert_eq!(
        CellPolicy::default(),
        policy("home_first", 80, 16),
        "the defaults"
    );
    let overridden = [
        parsed("[cells.model_x]\nmax_near = 0\nspill_rtt_ms = 120\n"),
        built().cell_policy("model_x", policy("home_first", 120, 0)),
    ];
    for placement in overridden {
        let laid = layout(&manifest(), &placement).expect("a layout");
        assert_eq!(
            laid.cell_policy("model_x"),
            Some(&policy("home_first", 120, 0))
        );
    }
    let laid = layout(
        &manifest(),
        &parsed("[cells.model_x]\npolicy = 'nearest'\n"),
    )
    .expect("a layout");
    assert_eq!(
        laid.cell_policy("model_x"),
        Some(&policy("nearest", 80, 16))
    );
    // Without classes, nothing is entered.
    let plain = Placement::from_toml(SEGMENTS).expect("a placement");
    let laid = layout(&manifest(), &plain).expect("a layout");
    assert_eq!(laid.cell_policy("model_x"), None);
}

/// Each refusal, from a parsed placement and the same one built in code.
#[test]
fn a_misplaced_cells_table_is_refused_by_layout() {
    let cases = [
        (
            format!("{SEGMENTS}[cells.model_x]\nmax_near = 4\n"),
            segments().cell_policy("model_x", policy("home_first", 80, 4)),
            "cell policy `[cells.model_x]`: the placement has no classes",
        ),
        (
            format!("{SEGMENTS}{CLASSES}[cells.zz]\nmax_near = 4\n"),
            built().cell_policy("zz", policy("home_first", 80, 4)),
            "cell policy `[cells.zz]`: the placement has no class `zz`",
        ),
        (
            format!("{SEGMENTS}{CLASSES}[cells.global]\nspill_rtt_ms = 10\n"),
            built().cell_policy("global", policy("home_first", 10, 16)),
            "cell policy `[cells.global]`: no class boundary enters class `global`; a cell policy picks a cell of a class that another class sends to",
        ),
    ];
    for (text, built, expected) in cases {
        let from_text = Placement::from_toml(&text).expect("a placement");
        assert_eq!(from_text, built, "{text}");
        assert_eq!(refusals(&from_text), [expected], "{text}");
        assert_eq!(refusals(&built), [expected], "{text}");
    }
}

/// A policy is named like a selector: the same shape, and no list of known
/// names, since a custom policy can be registered later.
#[test]
fn a_policy_name_has_a_selector_names_shape() {
    for bad in [
        "",
        "Home",
        "9x",
        "home-first",
        "home__first",
        "home_",
        &"x".repeat(65),
    ] {
        let text = format!("{SEGMENTS}{CLASSES}[cells.model_x]\npolicy = \"{bad}\"\n");
        let Err(error) = Placement::from_toml(&text) else {
            panic!("accepted `{bad}`");
        };
        assert!(error.to_string().contains("[cells.model_x]"), "{error}");
        let built = built().cell_policy("model_x", policy(bad, 80, 16));
        assert_eq!(
            refusals(&built),
            [format!(
                "cell policy `[cells.model_x]`: policy name `{bad}` must be lowercase letters and digits in words joined by single `_`s, start with a letter, and be at most 64 characters"
            )]
        );
    }
    for good in ["nearest", "round_robin", "random", "x9", &"x".repeat(64)] {
        let text = format!("{SEGMENTS}{CLASSES}[cells.model_x]\npolicy = \"{good}\"\n");
        let placement = Placement::from_toml(&text).expect("a usable name");
        layout(&manifest(), &placement).expect("a layout");
    }
}

/// Every cell policy problem is reported, with the placement's others.
#[test]
fn every_cell_policy_problem_is_reported_together() {
    // `S` is the source: no request boundary enters it.
    let placement = built()
        .route("S", RequestRouter::Random)
        .cell_policy("zz", policy("home_first", 80, 4))
        .cell_policy("global", policy("home_first", 80, 4))
        .cell_policy("model_x", policy("Bad", 80, 16));
    let errors = refusals(&placement);
    for expected in [
        "request router `S` must target a request boundary",
        "cell policy `[cells.zz]`: the placement has no class `zz`",
        "cell policy `[cells.global]`: no class boundary enters class `global`",
        "cell policy `[cells.model_x]`: policy name `Bad` must be",
    ] {
        assert!(
            errors.iter().any(|e| e.starts_with(expected)),
            "{expected} in: {errors:?}"
        );
    }
    assert_eq!(errors.len(), 4, "{errors:?}");

    let plain = segments()
        .route("S", RequestRouter::Random)
        .cell_policy("model_x", policy("home_first", 80, 4));
    assert_eq!(
        refusals(&plain),
        [
            "request router `S` must target a request boundary",
            "cell policy `[cells.model_x]`: the placement has no classes",
        ]
    );
}

/// A node in no segment hides the boundaries that would run through it, so
/// no class is judged not entered until every node is placed and every
/// segment classed: the problem is reported once, as what it is.
#[test]
fn an_unplaced_node_does_not_make_a_class_unentered() {
    let placement = Placement::from_toml(
        "[segment.a]\nnodes = ['S']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = []\n\
         [class.global]\nsegments = ['a', 'b']\n[class.model_x]\nsegments = ['d']\n\
         [cells.model_x]\nmax_near = 4\n",
    )
    .expect("a placement");
    let errors = refusals(&placement);
    assert!(!errors.iter().any(|e| e.contains("[cells.")), "{errors:?}");
    assert!(
        errors.iter().any(|e| e == "node `D` is in no segment"),
        "{errors:?}"
    );
}

/// Only a hop with an unplaced end could enter a class the boundaries do
/// not: `D` unplaced leaves `F`'s hop undecided, but its outer end is in
/// `global`, so wherever `D` goes, nothing enters `global`.
#[test]
fn an_unplaced_node_does_not_hide_a_class_it_cannot_enter() {
    let text = "[segment.a]\nnodes = ['S']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = []\n\
                [class.global]\nsegments = ['a', 'b']\n[class.model_x]\nsegments = ['d']\n\
                [cells.global]\nmax_near = 0\n";
    let built = segments()
        .segment("d", Vec::<String>::new())
        .class("global", ["a", "b"])
        .class("model_x", ["d"])
        .cell_policy("global", policy("home_first", 80, 0));
    let parsed = Placement::from_toml(text).expect("a placement");
    assert_eq!(parsed, built);
    for placement in [parsed, built] {
        let errors = refusals(&placement);
        for expected in [
            "node `D` is in no segment",
            "cell policy `[cells.global]`: no class boundary enters class `global`",
        ] {
            assert!(
                errors.iter().any(|e| e.starts_with(expected)),
                "{expected} in: {errors:?}"
            );
        }
    }
}

/// A hop whose inner end is placed in a class and whose outer end is not
/// could enter that class, so it is not judged unentered.
#[test]
fn an_unplaced_outer_end_leaves_its_inner_class_undecided() {
    let placement = Placement::from_toml(
        "[segment.a]\nnodes = []\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = ['D']\n\
         [class.global]\nsegments = ['a']\n[class.model_x]\nsegments = ['b', 'd']\n\
         [cells.model_x]\nmax_near = 0\n",
    )
    .expect("a placement");
    let errors = refusals(&placement);
    assert!(
        errors.iter().any(|e| e == "node `S` is in no segment"),
        "{errors:?}"
    );
    assert!(!errors.iter().any(|e| e.contains("[cells.")), "{errors:?}");
}

/// Two ends in one segment share whatever class the segment is given, so a
/// hop inside an unclassed segment enters no class.
#[test]
fn an_unclassed_shared_segment_cannot_make_a_class_entered() {
    let text = "[segment.a]\nnodes = ['S']\n[segment.m]\nnodes = ['F', 'D']\n\
                [class.global]\nsegments = ['a']\n\
                [cells.global]\nmax_near = 0\n";
    let built = Placement::new()
        .segment("a", ["S"])
        .segment("m", ["F", "D"])
        .class("global", ["a"])
        .cell_policy("global", policy("home_first", 80, 0));
    let parsed = Placement::from_toml(text).expect("a placement");
    assert_eq!(parsed, built);
    for placement in [parsed, built] {
        assert_eq!(
            refusals(&placement),
            [
                "segment `m` is in no class",
                "cell policy `[cells.global]`: no class boundary enters class `global`; a cell policy picks a cell of a class that another class sends to",
            ]
        );
    }
    // The control: the same segments with `F` and `D` apart, where giving
    // `m` a class could make `D`'s hop enter `global`.
    let apart = Placement::from_toml(
        "[segment.a]\nnodes = ['S']\n[segment.m]\nnodes = ['F']\n[segment.n]\nnodes = ['D']\n\
         [class.global]\nsegments = ['a']\n[cells.global]\nmax_near = 0\n",
    )
    .expect("a placement");
    let errors = refusals(&apart);
    assert!(!errors.iter().any(|e| e.contains("[cells.")), "{errors:?}");
}

/// A table that spells out only defaults is the placement that leaves it
/// unsaid, with its hash, but its name is still checked: a default for a
/// class or a node the placement lacks is refused as any value is.
#[test]
fn a_spelled_default_for_a_missing_name_is_refused() {
    for (extra, expected) in [
        (
            "[cells.zz]\npolicy = 'home_first'\n",
            "cell policy `[cells.zz]`: the placement has no class `zz`",
        ),
        (
            "[cells.global]\nmax_near = 16\n",
            "cell policy `[cells.global]`: no class boundary enters class `global`; a cell policy picks a cell of a class that another class sends to",
        ),
        (
            "[routing]\nGhost = 'round_robin'\n",
            "request router `Ghost` must target a request boundary",
        ),
    ] {
        let spelled = parsed(extra);
        assert_eq!(spelled, built(), "{extra}");
        assert_eq!(spelled.hash(), CLASSED_HASH, "{extra}");
        assert_eq!(refusals(&spelled), [expected], "{extra}");
    }
    // The control: a default spelled for a class a boundary enters.
    let entered = parsed("[cells.model_x]\npolicy = 'home_first'\n");
    layout(&manifest(), &entered).expect("a layout");
}
