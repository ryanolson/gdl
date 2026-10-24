//! A placement's cell policies: `[cells.<class>]`, how a chooser picks a
//! cell of the class it sends to. A default is not recorded, so a placement
//! that spells one out is the placement that leaves it unsaid, and the hash
//! covers each class's effective policy, the default included, so that a
//! change of default changes the hash. Each case is checked as parsed and
//! as built in code.

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

/// The hash of `CLASSES`, whose classes have the default policy.
const CLASSED_HASH: u64 = 0xdcf6_8ca1_8078_d0f8;

/// The hash `CLASSES` had while a class with the default policy hashed as
/// one with none, before a change of default changed the hash.
const UNPOLICIED_HASH: u64 = 0xf7ad_bcf6_e339_18fd;

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

fn policy(policy: &str) -> CellPolicy {
    CellPolicy {
        policy: policy.into(),
    }
}

fn refusals(placement: &Placement) -> Vec<String> {
    match layout(&manifest(), placement) {
        Ok(_) => panic!("laid out {placement:?}"),
        Err(PlacementErrors(errors)) => errors.iter().map(ToString::to_string).collect(),
    }
}

/// The default is `round_robin`, and a classed placement that names no
/// policy hashes as one that names it: the hash covers the effective
/// policy, so it is not the hash a placement had while the default went
/// unhashed, and `home_first`, no longer the default, hashes as itself.
#[test]
fn a_classed_placement_hashes_the_default_policy() {
    assert_eq!(CellPolicy::default(), policy("round_robin"));
    let named = parsed("[cells.model_x]\npolicy = 'round_robin'\n");
    for placement in [parsed(""), built(), named] {
        assert_eq!(placement.hash(), CLASSED_HASH);
    }
    assert_ne!(CLASSED_HASH, UNPOLICIED_HASH, "the default went unhashed");
    let home_first = parsed("[cells.model_x]\npolicy = 'home_first'\n");
    assert_ne!(home_first.hash(), CLASSED_HASH);
    assert_ne!(home_first.hash(), UNPOLICIED_HASH);
    assert_eq!(
        home_first,
        built().cell_policy("model_x", policy("home_first"))
    );
}

/// A placement without classes writes nothing for cell policies, and keeps
/// the hash it had before they existed.
#[test]
fn a_placement_without_classes_keeps_its_hash() {
    let plain = Placement::from_toml(SEGMENTS).expect("a placement");
    assert_eq!(plain.hash(), segments().hash());
    assert_eq!(plain.hash(), PLAIN_HASH);
}

/// The hash of `SEGMENTS` at the commit before cell policies existed.
const PLAIN_HASH: u64 = 0xe12f_e75a_992d_b590;

/// Spelled-out defaults, and a table holding only defaults, are no table:
/// the same placement, hash, and text, whatever class the table names.
#[test]
fn defaults_are_not_recorded() {
    let plain = parsed("");
    let text = toml::to_string(&plain).expect("toml");
    for extra in [
        "[cells.model_x]\npolicy = 'round_robin'\n",
        "[cells.model_x]\n",
        "[cells.global]\npolicy = 'round_robin'\n",
        "[cells.zz]\npolicy = 'round_robin'\n",
    ] {
        let spelled = parsed(extra);
        assert_eq!(spelled, plain, "{extra}");
        assert_eq!(spelled.hash(), CLASSED_HASH, "{extra}");
        assert_eq!(toml::to_string(&spelled).expect("toml"), text, "{extra}");
        layout(&manifest(), &spelled).expect("a layout");
    }
    for class in ["model_x", "global", "zz"] {
        let spelled = built().cell_policy(class, CellPolicy::default());
        assert_eq!(spelled, plain, "{class}");
        assert_eq!(spelled.hash(), CLASSED_HASH, "{class}");
    }
    // Setting a default removes what was set before, as `route` does.
    let reset = built()
        .cell_policy("model_x", policy("nearest"))
        .cell_policy("model_x", CellPolicy::default());
    assert_eq!(reset, plain);
}

#[test]
fn each_value_other_than_its_default_changes_the_hash() {
    let cases = [
        ("[cells.model_x]\npolicy = 'nearest'\n", policy("nearest")),
        (
            "[cells.model_x]\npolicy = 'home_first'\n",
            policy("home_first"),
        ),
        // The class the table names is part of the hash too.
        (
            "[cells.global]\npolicy = 'home_first'\n",
            policy("home_first"),
        ),
    ];
    let mut hashes = vec![CLASSED_HASH, UNPOLICIED_HASH];
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
        parsed("[cells.model_x]\npolicy = 'home_first'\n").hash(),
        CELL_POLICY_HASH
    );
}

const CELL_POLICY_HASH: u64 = 0xb43b_f3b0_4ee5_02e6;

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
    let placement = parsed("[cells.model_x]\npolicy = 'nearest'\n");
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
    json["placement"]["cells"] = serde_json::json!({ "model_x": { "policy": "round_robin" } });
    let back: DeploymentSpec = serde_json::from_value(json).expect("a deployment");
    assert_eq!(back.placement, parsed(""));
    assert_eq!(back.placement.hash(), CLASSED_HASH);
}

/// `policy` is a table's one key. `spill_rtt_ms` and `max_near`, which
/// chose near cells, are refused as unknown, from TOML and from the hub's
/// JSON, naming the key.
#[test]
fn a_cells_table_has_only_its_policy_key() {
    for (bad, key) in [
        ("[cells.model_x]\nspill_rtt_ms = 80\n", "spill_rtt_ms"),
        ("[cells.model_x]\nmax_near = 16\n", "max_near"),
        // Refused whatever the other values are.
        (
            "[cells.model_x]\npolicy = 'home_first'\nmax_near = 0\n",
            "max_near",
        ),
        (
            "[cells.model_x]\npolicy = 'home_first'\nsegments = ['d']\n",
            "segments",
        ),
        ("[cells.model_x]\nspill = 3\n", "spill"),
    ] {
        let Err(error) = Placement::from_toml(&format!("{SEGMENTS}{CLASSES}{bad}")) else {
            panic!("accepted {bad}");
        };
        assert!(
            error.to_string().contains(&format!("`{key}`")),
            "{bad}: {error}"
        );
    }
    for bad in [
        "[cells.model_x]\npolicy = 3\n",
        "[cells]\nmodel_x = 'home_first'\n",
    ] {
        assert!(
            Placement::from_toml(&format!("{SEGMENTS}{CLASSES}{bad}")).is_err(),
            "accepted {bad}"
        );
    }
    let plain = DeploymentSpec::new(manifest(), parsed("")).expect("a deployment");
    for key in ["spill_rtt_ms", "max_near"] {
        let mut json: serde_json::Value = serde_json::to_value(&plain).expect("json");
        json["placement"]["cells"] = serde_json::json!({ "model_x": { key: 16 } });
        let Err(error) = serde_json::from_value::<DeploymentSpec>(json) else {
            panic!("accepted `{key}` from JSON");
        };
        assert!(error.to_string().contains(&format!("`{key}`")), "{error}");
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
    assert_eq!(CellPolicy::default(), policy("round_robin"), "the default");
    let overridden = [
        parsed("[cells.model_x]\npolicy = 'home_first'\n"),
        built().cell_policy("model_x", policy("home_first")),
    ];
    for placement in overridden {
        let laid = layout(&manifest(), &placement).expect("a layout");
        assert_eq!(laid.cell_policy("model_x"), Some(&policy("home_first")));
    }
    let laid = layout(
        &manifest(),
        &parsed("[cells.model_x]\npolicy = 'nearest'\n"),
    )
    .expect("a layout");
    assert_eq!(laid.cell_policy("model_x"), Some(&policy("nearest")));
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
            format!("{SEGMENTS}[cells.model_x]\npolicy = 'home_first'\n"),
            segments().cell_policy("model_x", policy("home_first")),
            "cell policy `[cells.model_x]`: the placement has no classes",
        ),
        (
            format!("{SEGMENTS}{CLASSES}[cells.zz]\npolicy = 'home_first'\n"),
            built().cell_policy("zz", policy("home_first")),
            "cell policy `[cells.zz]`: the placement has no class `zz`",
        ),
        (
            format!("{SEGMENTS}{CLASSES}[cells.global]\npolicy = 'home_first'\n"),
            built().cell_policy("global", policy("home_first")),
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
        let built = built().cell_policy("model_x", policy(bad));
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
        .cell_policy("zz", policy("home_first"))
        .cell_policy("global", policy("home_first"))
        .cell_policy("model_x", policy("Bad"));
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
        .cell_policy("model_x", policy("home_first"));
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
         [cells.model_x]\npolicy = 'home_first'\n",
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
                [cells.global]\npolicy = 'home_first'\n";
    let built = segments()
        .segment("d", Vec::<String>::new())
        .class("global", ["a", "b"])
        .class("model_x", ["d"])
        .cell_policy("global", policy("home_first"));
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
         [cells.model_x]\npolicy = 'home_first'\n",
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
                [cells.global]\npolicy = 'home_first'\n";
    let built = Placement::new()
        .segment("a", ["S"])
        .segment("m", ["F", "D"])
        .class("global", ["a"])
        .cell_policy("global", policy("home_first"));
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
         [class.global]\nsegments = ['a']\n[cells.global]\npolicy = 'home_first'\n",
    )
    .expect("a placement");
    let errors = refusals(&apart);
    assert!(!errors.iter().any(|e| e.contains("[cells.")), "{errors:?}");
}
