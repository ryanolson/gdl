//! The fleet file: a deployment's classes' default quorums and
//! requirements, and its pools. It is checked against a layout, and gives
//! each pool's own settings with its quorum resolved.

use std::collections::BTreeMap;

use gdl_model::{
    DirectoryBackend, Fleet, HopInfo, Layout, Manifest, NodeInfo, Placement, Placements,
    PoolSettings, Role, TypeInfo, layout,
};

/// The graph of a model served in pools (docs/src/concepts/pools.md),
/// `Http => Pre { Route { Model { Engine => Detok } } }`.
fn manifest() -> Manifest {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let hop = |outer: &str, inner: &str, backward: &[&str]| HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: backward.iter().map(|&n| n.into()).collect(),
        forward_types: vec![TypeInfo::new("T", true, Default::default())],
        backward_types: (0..=backward.len())
            .map(|_| TypeInfo::new("T", true, Default::default()))
            .collect(),
    };
    Manifest {
        name: "llm".into(),
        schema: 1,
        nodes: vec![
            node("Http", Role::Source),
            node("Pre", Role::Operator),
            node("Route", Role::Operator),
            node("Model", Role::Operator),
            node("Engine", Role::Sink),
            node("Detok", Role::Backward),
        ],
        hops: vec![
            hop("Http", "Pre", &[]),
            hop("Pre", "Route", &[]),
            hop("Route", "Model", &[]),
            hop("Model", "Engine", &["Detok"]),
        ],
    }
}

/// The placement of section 3.2, with its segment names lowercased.
const PLACEMENT: &str = r#"
[segment.a]
nodes = ["Http"]
[segment.b]
nodes = ["Pre"]
[segment.c]
nodes = ["Route"]
[segment.d]
nodes = ["Model"]
[segment.e]
nodes = ["Engine"]
[segment.f]
nodes = ["Detok"]

[class.global]
segments = ["a", "b", "c"]
[class.model_x]
segments = ["d", "e", "f"]

[pools.model_x]
policy = "home_first"

[routing]
Engine = { selector = "least_loaded" }
"#;

/// The fleet of section 3.3, with its segment names lowercased.
const FLEET: &str = r#"
[class.global]
ready = { a = 2, b = 2, c = 2 }
requires = { model_x = 1 }

[class.model_x]
ready = { d = 1, e = 4, f = 2 }

[pool.global-us-east-1]
class = "global"
region = "us-east-1"

[pool.global-us-west-2]
class = "global"
region = "us-west-2"

[pool.global-eu-west-1]
class = "global"
region = "eu-west-1"

[pool.model-x-01-us-east-1]
class = "model_x"
region = "us-east-1"
home = "global-us-east-1"

[pool.model-x-02-us-east-1]
class = "model_x"
region = "us-east-1"
home = "global-us-east-1"
ready = { e = 8 }

[pool.model-x-01-us-west-2]
class = "model_x"
region = "us-west-2"
home = "global-us-west-2"

[pool.model-x-01-eu-west-1]
class = "model_x"
region = "eu-west-1"
home = "global-eu-west-1"
"#;

/// A spec's layouts, as a fleet is checked against them: each layout by
/// its name, with its placement checked against the graph.
type Layouts = BTreeMap<String, Option<Layout>>;

fn placed(text: &str) -> Layout {
    let placement = Placement::from_toml(text).expect("a placement");
    layout(&manifest(), &placement).expect("a layout")
}

/// The one layout of a spec without `[layout]` tables.
fn laid() -> Layouts {
    BTreeMap::from([(Placements::DEFAULT.to_string(), Some(placed(PLACEMENT)))])
}

fn fleet(text: &str) -> Fleet {
    Fleet::from_toml(text).expect("a fleet")
}

/// Each problem the check reports, in its order.
fn problems(text: &str) -> Vec<String> {
    match fleet(text).check(Some(&laid())) {
        Ok(()) => panic!("accepted:\n{text}"),
        Err(errors) => errors.0.iter().map(ToString::to_string).collect(),
    }
}

#[test]
fn the_fleet_of_the_design_checks_clean() {
    fleet(FLEET).check(Some(&laid())).expect("a sound fleet");
}

#[test]
fn a_fleet_survives_toml() {
    let fleet = fleet(FLEET);
    let text = toml::to_string(&fleet).expect("toml");
    assert_eq!(
        Fleet::from_toml(&text).expect("reads what it wrote"),
        fleet,
        "{text}"
    );
    assert_eq!(
        Fleet::from_toml("").expect("an empty fleet"),
        Fleet::default()
    );
}

#[test]
fn a_pools_settings_have_its_quorum_resolved() {
    let fleet = fleet(FLEET);
    let larger = fleet.pool("model-x-02-us-east-1").expect("a pool");
    assert_eq!(
        larger,
        PoolSettings {
            properties: Vec::new(),
            name: "model-x-02-us-east-1".into(),
            layout: None,
            class: "model_x".into(),
            region: "us-east-1".into(),
            home: Some("global-us-east-1".into()),
            directory: DirectoryBackend::DEFAULT,
            // The pool's own `e`; the class's `d` and `f`.
            ready: BTreeMap::from([("d".into(), 1), ("e".into(), 8), ("f".into(), 2)]),
            requires: BTreeMap::new(),
            hub: None,
            summary: PoolSettings::DEFAULT_SUMMARY,
            timing: Default::default(),
        }
    );
    assert!(larger.requires.is_empty(), "model_x requires nothing");
    let global = fleet.pool("global-eu-west-1").expect("a pool");
    assert_eq!(global.home, None);
    assert_eq!(
        global.requires,
        BTreeMap::from([("model_x".into(), 1)]),
        "the class's requirements"
    );
    assert_eq!(global.quorum("a"), 2);
    assert!(fleet.pool("model-x-09-us-east-1").is_none());

    // A segment with neither an override nor a class default is left to
    // the runtime's fallback.
    let sparse = Fleet::from_toml(
        "[class.global]\nready = { a = 3 }\n\
         [pool.g]\nclass = 'global'\nregion = 'r'\nready = { b = 5 }\n\
         [pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n",
    )
    .expect("a fleet");
    sparse.check(Some(&laid())).expect("a sound fleet");
    let g = sparse.pool("g").expect("a pool");
    assert_eq!(g.ready, BTreeMap::from([("a".into(), 3), ("b".into(), 5)]));
    assert_eq!((g.quorum("a"), g.quorum("b"), g.quorum("c")), (3, 5, 1));
    let m = sparse.pool("m").expect("a pool");
    assert!(m.ready.is_empty());
    assert_eq!(m.quorum("e"), 1);
}

/// A class need not be listed, and a class may have no pools.
#[test]
fn classes_and_pools_are_optional() {
    for text in [
        "",
        "[class.model_x]\nready = { e = 4 }\n",
        "[pool.g]\nclass = 'global'\nregion = 'us-east-1'\n\
         [pool.m]\nclass = 'model_x'\nregion = 'us-east-1'\nhome = 'g'\n",
        "[class.global]\n",
    ] {
        fleet(text).check(Some(&laid())).expect("a sound fleet");
    }
}

/// A home in another region is allowed: it is a preference of the policy.
#[test]
fn a_home_may_be_in_another_region() {
    fleet(
        "[pool.g]\nclass = 'global'\nregion = 'us-east-1'\n\
         [pool.m]\nclass = 'model_x'\nregion = 'eu-west-1'\nhome = 'g'\n",
    )
    .check(Some(&laid()))
    .expect("a sound fleet");
}

/// A global pool `g`, with its child: a parent pool has a child of each
/// class its class sends to.
const G: &str = "[pool.g]\nclass = 'global'\nregion = 'r'\n\
                 [pool.g-child]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n";

/// A child of the global pool `h`.
const H_CHILD: &str = "[pool.h-child]\nclass = 'model_x'\nregion = 'r'\nhome = 'h'\n";

#[test]
fn each_refusal_names_its_table_and_key() {
    let cases = [
        (
            "[class.zz]\nready = { a = 1 }\n".to_string(),
            "[class.zz]: the placement has no class `zz`",
        ),
        (
            "[class.global]\nready = { d = 1 }\n".into(),
            "[class.global] ready.d: not a segment of class `global`",
        ),
        (
            "[class.global]\nready = { a = 0 }\n".into(),
            "[class.global] ready.a: is 0; a quorum is at least 1",
        ),
        (
            "[class.global]\nrequires = { global = 1 }\n".into(),
            "[class.global] requires.global: not a class that `global` sends to",
        ),
        (
            "[class.model_x]\nrequires = { global = 1 }\n".into(),
            "[class.model_x] requires.global: not a class that `model_x` sends to",
        ),
        (
            "[class.global]\nrequires = { model_x = 0 }\n".into(),
            "[class.global] requires.model_x: is 0; a requirement is at least 1",
        ),
        (
            format!(
                "{G}[pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\nready = {{ a = 1 }}\n"
            ),
            "[pool.m] ready.a: not a segment of class `model_x`",
        ),
        (
            format!(
                "{G}[pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\nready = {{ e = 0 }}\n"
            ),
            "[pool.m] ready.e: is 0; a quorum is at least 1",
        ),
        (
            "[pool.m]\nclass = 'zz'\nregion = 'r'\n".into(),
            "[pool.m] class: the placement has no class `zz`",
        ),
        (
            "[pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n".into(),
            "[pool.m] home: the fleet has no pool `g`",
        ),
        (
            format!(
                "{G}[pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'n'\n[pool.n]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n"
            ),
            "[pool.m] home: pool `n` is of class `model_x`, which does not send to `model_x`; a home is a pool of a class that sends to the pool's",
        ),
        (
            format!("{G}{H_CHILD}[pool.h]\nclass = 'global'\nregion = 'r'\nhome = 'g'\n"),
            "[pool.h] home: no class sends to `global`, so its pools have no parent",
        ),
    ];
    for (text, expected) in cases {
        assert_eq!(problems(&text), [expected], "{text}");
    }
}

/// A pool whose class is unknown is reported once: its quorum and home are
/// not judged against a class it does not have.
#[test]
fn an_unknown_class_hides_nothing_else_and_repeats_nothing() {
    assert_eq!(
        problems(
            "[pool.m]\nclass = 'zz'\nregion = 'r'\nready = { q = 0 }\nhome = 'g'\n\
             [pool.g]\nclass = 'global'\nregion = 'r'\n\
             [pool.n]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n"
        ),
        ["[pool.m] class: the placement has no class `zz`"]
    );
    // Nor is a home whose own class is unknown judged as a parent.
    assert_eq!(
        problems(
            "[pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n\
             [pool.g]\nclass = 'zz'\nregion = 'r'\n"
        ),
        ["[pool.g] class: the placement has no class `zz`"]
    );
}

#[test]
fn every_problem_is_reported_together() {
    let errors = problems(
        "[class.zz]\n\
         [class.global]\nready = { d = 1 }\nrequires = { model_x = 0 }\n\
         [pool.Global]\nclass = 'global'\nregion = 'US'\n\
         [pool.n]\nclass = 'model_x'\nregion = 'r'\nhome = 'Global'\n\
         [pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'nope'\nready = { e = 0 }\n",
    );
    for expected in [
        "[class.zz]: the placement has no class `zz`",
        "[class.global] ready.d: not a segment of class `global`",
        "[class.global] requires.model_x: is 0",
        "[pool.Global]: pool name `Global` must be",
        "[pool.Global] region: region name `US` must be",
        "[pool.m] home: the fleet has no pool `nope`",
        "[pool.m] ready.e: is 0",
    ] {
        assert!(
            errors.iter().any(|e| e.starts_with(expected)),
            "{expected} in: {errors:?}"
        );
    }
    assert_eq!(errors.len(), 7, "{errors:?}");
}

#[test]
fn unknown_keys_are_refused_anywhere() {
    for bad in [
        "[region.us-east-1]\n",
        "[class.global]\nquorum = { a = 1 }\n",
        "[pool.g]\nclass = 'global'\nregion = 'r'\nzone = 'a'\n",
        "[pool.g]\nclass = 'global'\n",
        "[pool.g]\nregion = 'r'\n",
        "[class.global]\nready = { a = -1 }\n",
        "[class.global]\nready = { a = 'two' }\n",
    ] {
        assert!(Fleet::from_toml(bad).is_err(), "accepted:\n{bad}");
    }
}

/// A pool's name becomes a host name, a URL path segment, part of a hub
/// service name, and part of a certificate identity; a region's is used
/// beside it. Both are DNS labels.
#[test]
fn pool_and_region_names_are_dns_labels() {
    let longest = format!("a{}", "-0".repeat(31));
    assert_eq!(longest.len(), 63);
    for good in [
        "global-us-east-1",
        "model-x-01-us-east-1",
        "us-east-1",
        "a",
        "a1",
        longest.as_str(),
    ] {
        let text = format!(
            "[pool.{good}]\nclass = 'global'\nregion = '{good}'\n\
             [pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = '{good}'\n"
        );
        fleet(&text).check(Some(&laid())).expect("a usable name");
    }
    let too_long = format!("{longest}0");
    for bad in [
        too_long.as_str(),
        "1-us-east",
        "-us-east-1",
        "us-east-1-",
        "Us-east-1",
        "us_east_1",
        "us.east",
        "",
    ] {
        let text = format!(
            "[pool.\"{bad}\"]\nclass = 'global'\nregion = '{bad}'\n\
             [pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = \"{bad}\"\n"
        );
        let rule = "must be lowercase letters, digits, and `-`, start with a letter, end with a letter or digit, and be at most 63 characters";
        assert_eq!(
            problems(&text),
            [
                format!("[pool.{bad}]: pool name `{bad}` {rule}"),
                format!("[pool.{bad}] region: region name `{bad}` {rule}"),
            ],
            "{bad}"
        );
    }
}

/// Whether a home names a pool of the fleet does not depend on the pool's
/// class, so an unknown class does not hide a missing home.
#[test]
fn an_unknown_class_does_not_hide_a_missing_home() {
    assert_eq!(
        problems("[pool.m]\nclass = 'absent'\nregion = 'r'\nhome = 'missing'\n"),
        [
            "[pool.m] class: the placement has no class `absent`",
            "[pool.m] home: the fleet has no pool `missing`",
        ]
    );
    // The control: with a known class, the missing home alone.
    assert_eq!(
        problems("[pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'missing'\n"),
        ["[pool.m] home: the fleet has no pool `missing`"]
    );
}

/// A pool's `home` is its parent: a pool of a class that sends to its
/// own. A pool of a class that another sends to has one; a pool of a
/// class that nothing sends to has none.
#[test]
fn home_is_the_pools_parent() {
    let cases = [
        (
            format!("{G}[pool.m]\nclass = 'model_x'\nregion = 'r'\n"),
            "[pool.m] home: missing; a pool of class `model_x` names its parent, a pool of a class that sends to it: `global`",
        ),
        (
            format!("{G}[pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'nope'\n"),
            "[pool.m] home: the fleet has no pool `nope`",
        ),
        (
            format!(
                "{G}[pool.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'n'\n\
                 [pool.n]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n"
            ),
            "[pool.m] home: pool `n` is of class `model_x`, which does not send to `model_x`; a home is a pool of a class that sends to the pool's",
        ),
        (
            format!("{G}{H_CHILD}[pool.h]\nclass = 'global'\nregion = 'r'\nhome = 'g'\n"),
            "[pool.h] home: no class sends to `global`, so its pools have no parent",
        ),
    ];
    for (text, expected) in cases {
        assert_eq!(problems(&text), [expected], "{text}");
    }
}

/// Without a layout, as when the spec does not read, the fleet is still
/// checked for what needs none: its names, its regions, that each home is
/// one of its pools, and that no count is 0. Which classes and segments
/// there are waits for a layout.
#[test]
fn without_a_layout_only_what_needs_none_is_judged() {
    let text = "[class.zz]\nready = { q = 0 }\n\
                [pool.Bad]\nclass = 'zz'\nregion = 'US'\nhome = 'nope'\nready = { q = 0 }\n\
                [pool.m]\nclass = 'model_x'\nregion = 'r'\n";
    let Err(errors) = fleet(text).check(None) else {
        panic!("accepted:\n{text}");
    };
    let errors: Vec<String> = errors.0.iter().map(ToString::to_string).collect();
    assert_eq!(errors.len(), 5, "{errors:?}");
    for expected in [
        "[class.zz] ready.q: is 0; a quorum is at least 1",
        "[pool.Bad] ready.q: is 0; a quorum is at least 1",
        "[pool.Bad]: pool name `Bad` must be",
        "[pool.Bad] region: region name `US` must be",
        "[pool.Bad] home: the fleet has no pool `nope`",
    ] {
        assert!(
            errors.iter().any(|e| e.starts_with(expected)),
            "{expected} in: {errors:?}"
        );
    }
    fleet(FLEET).check(None).expect("the design's fleet");
}

/// The graph of [`PLACEMENT`] in two layouts: `split`, the design's, and
/// `joint`, whose model pools are of class `model_y`.
fn two_layouts() -> Layouts {
    let joint = PLACEMENT
        .replace("model_x", "model_y")
        .replace("[routing]\nEngine = { selector = \"least_loaded\" }\n", "");
    BTreeMap::from([
        ("joint".to_string(), Some(placed(&joint))),
        ("split".to_string(), Some(placed(PLACEMENT))),
    ])
}

/// A fleet of both layouts: a global pool and a model pool of each.
const TWO_LAYOUTS: &str = r#"
[class.global]
ready = { a = 2 }

[class.model_x]
ready = { e = 4 }

[pool.global-split]
layout = "split"
class = "global"
region = "us-east-1"

[pool.model-x-split]
layout = "split"
class = "model_x"
region = "us-east-1"
home = "global-split"

[pool.global-joint]
layout = "joint"
class = "global"
region = "eu-west-1"

[pool.model-y-joint]
layout = "joint"
class = "model_y"
region = "eu-west-1"
home = "global-joint"
"#;

fn problems_of(text: &str, layouts: &Layouts) -> Vec<String> {
    match fleet(text).check(Some(layouts)) {
        Ok(()) => panic!("accepted:\n{text}"),
        Err(errors) => errors.0.iter().map(ToString::to_string).collect(),
    }
}

/// Each pool runs one layout, and is judged against it: its class, its
/// quorum, and its home are its layout's.
#[test]
fn a_fleet_of_two_layouts_checks_clean() {
    fleet(TWO_LAYOUTS)
        .check(Some(&two_layouts()))
        .expect("a sound fleet");
    // A pool's settings name its layout.
    let pool = fleet(TWO_LAYOUTS).pool("model-y-joint").expect("a pool");
    assert_eq!(pool.layout.as_deref(), Some("joint"));
    assert_eq!(pool.class, "model_y");
}

#[test]
fn a_pool_of_a_spec_of_several_layouts_names_a_layout_of_the_spec() {
    let pool = |extra: &str| {
        format!(
            "{TWO_LAYOUTS}[pool.model-x-02]\nclass = 'model_x'\nregion = 'r'\nhome = 'global-split'\n{extra}"
        )
    };
    for (text, expected) in [
        (
            pool(""),
            "[pool.model-x-02] layout: missing; the spec has layouts `joint`, `split`, and a pool names the one it runs",
        ),
        (
            pool("layout = 'nope'\n"),
            "[pool.model-x-02] layout: the spec has no layout `nope`; its layouts are `joint`, `split`",
        ),
        (
            pool("layout = 'joint'\n").replace(
                "home = 'global-split'\nlayout",
                "home = 'global-joint'\nlayout",
            ),
            "[pool.model-x-02] class: layout `joint` has no class `model_x`",
        ),
    ] {
        assert_eq!(problems_of(&text, &two_layouts()), [expected], "{text}");
    }
}

/// A pool's home is a pool of its own layout: pools of different layouts
/// never follow or offer to each other.
#[test]
fn a_home_is_a_pool_of_the_same_layout() {
    let text = format!(
        "{TWO_LAYOUTS}[pool.model-y-02]\nlayout = 'joint'\nclass = 'model_y'\nregion = 'r'\nhome = 'global-split'\n"
    );
    assert_eq!(
        problems_of(&text, &two_layouts()),
        [
            "[pool.model-y-02] home: pool `global-split` runs layout `split`, not `joint`; a home is a pool of the pool's own layout"
        ]
    );
    // The control: the same pool at home in its own layout.
    let text = text.replace("home = 'global-split'", "home = 'global-joint'");
    fleet(&text).check(Some(&two_layouts())).expect("at home");
    // Without the spec, the layouts the pools declare are compared.
    let text = text.replace("home = 'global-joint'", "home = 'global-split'");
    let Err(errors) = fleet(&text).check(None) else {
        panic!("accepted:\n{text}");
    };
    assert_eq!(
        errors.to_string(),
        "[pool.model-y-02] home: pool `global-split` runs layout `split`, not `joint`; a home is a pool of the pool's own layout"
    );
}

/// With one layout, a pool need not name it, and may: as `default` for a
/// spec without `[layout]` tables.
#[test]
fn a_pool_of_a_spec_of_one_layout_may_name_it() {
    fleet(FLEET).check(Some(&laid())).expect("no layout keys");
    let named = FLEET.replace(
        "[pool.global-us-east-1]\n",
        "[pool.global-us-east-1]\nlayout = \"default\"\n",
    );
    assert_ne!(named, FLEET);
    fleet(&named)
        .check(Some(&laid()))
        .expect("the one layout, named");
    let wrong = named.replace("layout = \"default\"", "layout = \"split\"");
    assert_eq!(
        problems_of(&wrong, &laid()),
        [
            "[pool.global-us-east-1] layout: the spec has no layout `split`; its layouts are `default`"
        ]
    );
}

/// A class's defaults apply to its pools in every layout that has the
/// class, so they are judged against each.
#[test]
fn a_class_table_is_judged_in_every_layout_with_the_class() {
    let text = TWO_LAYOUTS.replace("ready = { a = 2 }", "ready = { a = 2, d = 1 }")
        + "[class.zz]\nready = { a = 1 }\n";
    let found = problems_of(&text, &two_layouts());
    assert_eq!(
        found,
        [
            "[class.global] ready.d: not a segment of class `global` in layout `joint`",
            "[class.global] ready.d: not a segment of class `global` in layout `split`",
            "[class.zz]: no layout of the spec has a class `zz`",
        ],
        "{found:?}"
    );
}

/// A layout whose placement does not fit the graph is still one of the
/// spec's: its pools are not refused for naming it, and what needs its
/// classes waits for it.
#[test]
fn a_layout_that_does_not_validate_is_still_named() {
    let mut layouts = two_layouts();
    layouts.insert("joint".into(), None);
    fleet(TWO_LAYOUTS)
        .check(Some(&layouts))
        .expect("judged where it can be");
    // The pools of the other layout are judged in full.
    let text = TWO_LAYOUTS.replace(
        "[pool.global-split]\nlayout = \"split\"\nclass = \"global\"",
        "[pool.global-split]\nlayout = \"split\"\nclass = \"model_y\"",
    );
    assert_ne!(text, TWO_LAYOUTS);
    let found = problems_of(&text, &layouts);
    assert!(
        found.contains(
            &"[pool.global-split] class: layout `split` has no class `model_y`".to_string()
        ),
        "{found:?}"
    );
}

#[test]
fn a_pools_layout_has_a_layout_names_shape() {
    let text = FLEET.replace(
        "[pool.global-us-east-1]\n",
        "[pool.global-us-east-1]\nlayout = \"US-East\"\n",
    );
    let Err(errors) = fleet(&text).check(None) else {
        panic!("accepted:\n{text}");
    };
    assert!(
        errors
            .to_string()
            .starts_with("[pool.global-us-east-1] layout: layout name `US-East` must be"),
        "{errors}"
    );
}

/// A generator lists the fleet's pools, each with its settings as
/// `Fleet::pool` gives them, in name order.
#[test]
fn a_fleet_lists_its_pools_in_name_order() {
    let fleet = fleet(FLEET);
    let pools: Vec<PoolSettings> = fleet.pools().collect();
    let names: Vec<&str> = pools.iter().map(|pool| pool.name.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted);
    assert_eq!(names.len(), 7, "{names:?}");
    for pool in &pools {
        assert_eq!(Some(pool), fleet.pool(&pool.name).as_ref());
    }
    assert_eq!(Fleet::default().pools().count(), 0);
}
