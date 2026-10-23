//! The fleet file: a deployment's classes' default quorums and
//! requirements, and its cells. It is checked against a layout, and gives
//! each cell's own settings with its quorum resolved.

use std::collections::BTreeMap;

use gdl_model::{
    CellSettings, DirectoryBackend, Fleet, HopInfo, Layout, Manifest, NodeInfo, Placement, Role,
    TypeInfo, layout,
};

/// The graph of `agent-docs/cells.md` section 3.1,
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
        forward_types: vec![TypeInfo::new("T", true)],
        backward_types: (0..=backward.len())
            .map(|_| TypeInfo::new("T", true))
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

[cells.model_x]
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

[cell.global-us-east-1]
class = "global"
region = "us-east-1"

[cell.global-us-west-2]
class = "global"
region = "us-west-2"

[cell.global-eu-west-1]
class = "global"
region = "eu-west-1"

[cell.model-x-01-us-east-1]
class = "model_x"
region = "us-east-1"
home = "global-us-east-1"

[cell.model-x-02-us-east-1]
class = "model_x"
region = "us-east-1"
home = "global-us-east-1"
ready = { e = 8 }

[cell.model-x-01-us-west-2]
class = "model_x"
region = "us-west-2"
home = "global-us-west-2"

[cell.model-x-01-eu-west-1]
class = "model_x"
region = "eu-west-1"
home = "global-eu-west-1"
"#;

fn laid() -> Layout {
    let placement = Placement::from_toml(PLACEMENT).expect("a placement");
    layout(&manifest(), &placement).expect("a layout")
}

fn fleet(text: &str) -> Fleet {
    Fleet::from_toml(text).expect("a fleet")
}

/// Each problem the check reports, in its order.
fn problems(text: &str) -> Vec<String> {
    match fleet(text).check(&laid()) {
        Ok(()) => panic!("accepted:\n{text}"),
        Err(errors) => errors.0.iter().map(ToString::to_string).collect(),
    }
}

#[test]
fn the_fleet_of_the_design_checks_clean() {
    fleet(FLEET).check(&laid()).expect("a sound fleet");
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
fn a_cells_settings_have_its_quorum_resolved() {
    let fleet = fleet(FLEET);
    let larger = fleet.cell("model-x-02-us-east-1").expect("a cell");
    assert_eq!(
        larger,
        CellSettings {
            name: "model-x-02-us-east-1".into(),
            class: "model_x".into(),
            region: "us-east-1".into(),
            home: Some("global-us-east-1".into()),
            directory: DirectoryBackend::Relay,
            seeds: vec![],
            // The cell's own `e`; the class's `d` and `f`.
            ready: BTreeMap::from([("d".into(), 1), ("e".into(), 8), ("f".into(), 2)]),
            hub: None,
        }
    );
    let global = fleet.cell("global-eu-west-1").expect("a cell");
    assert_eq!(global.home, None);
    assert_eq!(global.quorum("a"), 2);
    assert!(fleet.cell("model-x-09-us-east-1").is_none());

    // A segment with neither an override nor a class default is left to
    // the runtime's fallback.
    let sparse = Fleet::from_toml(
        "[class.global]\nready = { a = 3 }\n\
         [cell.g]\nclass = 'global'\nregion = 'r'\nready = { b = 5 }\n\
         [cell.m]\nclass = 'model_x'\nregion = 'r'\n",
    )
    .expect("a fleet");
    sparse.check(&laid()).expect("a sound fleet");
    let g = sparse.cell("g").expect("a cell");
    assert_eq!(g.ready, BTreeMap::from([("a".into(), 3), ("b".into(), 5)]));
    assert_eq!((g.quorum("a"), g.quorum("b"), g.quorum("c")), (3, 5, 1));
    let m = sparse.cell("m").expect("a cell");
    assert!(m.ready.is_empty());
    assert_eq!(m.quorum("e"), 1);
}

/// A class need not be listed, and a class may have no cells.
#[test]
fn classes_and_cells_are_optional() {
    for text in [
        "",
        "[class.model_x]\nready = { e = 4 }\n",
        "[cell.g]\nclass = 'global'\nregion = 'us-east-1'\n",
        "[class.global]\n",
    ] {
        fleet(text).check(&laid()).expect("a sound fleet");
    }
}

/// A home in another region is allowed: it is a preference of the policy.
#[test]
fn a_home_may_be_in_another_region() {
    fleet(
        "[cell.g]\nclass = 'global'\nregion = 'us-east-1'\n\
         [cell.m]\nclass = 'model_x'\nregion = 'eu-west-1'\nhome = 'g'\n",
    )
    .check(&laid())
    .expect("a sound fleet");
}

const G: &str = "[cell.g]\nclass = 'global'\nregion = 'r'\n";

#[test]
fn each_refusal_names_its_table_and_key() {
    let cases = [
        (
            "[class.zz]\nready = { a = 1 }\n".to_string(),
            "[class.zz]: the placement has no class `zz`",
        ),
        (
            "[class.global]\nready = { d = 1 }\n".into(),
            "[class.global] ready: `d` is not a segment of class `global`",
        ),
        (
            "[class.global]\nready = { a = 0 }\n".into(),
            "[class.global] ready: `a` is 0; a quorum is at least 1",
        ),
        (
            "[class.global]\nrequires = { global = 1 }\n".into(),
            "[class.global] requires: `global` is not a class that `global` sends to",
        ),
        (
            "[class.model_x]\nrequires = { global = 1 }\n".into(),
            "[class.model_x] requires: `global` is not a class that `model_x` sends to",
        ),
        (
            "[class.global]\nrequires = { model_x = 0 }\n".into(),
            "[class.global] requires: `model_x` is 0; a requirement is at least 1",
        ),
        (
            format!("{G}[cell.m]\nclass = 'model_x'\nregion = 'r'\nready = {{ a = 1 }}\n"),
            "[cell.m] ready: `a` is not a segment of class `model_x`",
        ),
        (
            format!("{G}[cell.m]\nclass = 'model_x'\nregion = 'r'\nready = {{ e = 0 }}\n"),
            "[cell.m] ready: `e` is 0; a quorum is at least 1",
        ),
        (
            "[cell.m]\nclass = 'zz'\nregion = 'r'\n".into(),
            "[cell.m] class: the placement has no class `zz`",
        ),
        (
            "[cell.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n".into(),
            "[cell.m] home: the fleet has no cell `g`",
        ),
        (
            format!(
                "{G}[cell.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'n'\n[cell.n]\nclass = 'model_x'\nregion = 'r'\n"
            ),
            "[cell.m] home: cell `n` is of class `model_x`, which does not send to `model_x`; a home is a cell of a class that sends to the cell's",
        ),
        (
            format!("{G}[cell.h]\nclass = 'global'\nregion = 'r'\nhome = 'g'\n"),
            "[cell.h] home: cell `g` is of class `global`, which does not send to `global`; a home is a cell of a class that sends to the cell's",
        ),
    ];
    for (text, expected) in cases {
        assert_eq!(problems(&text), [expected], "{text}");
    }
}

/// A cell whose class is unknown is reported once: its quorum and home are
/// not judged against a class it does not have.
#[test]
fn an_unknown_class_hides_nothing_else_and_repeats_nothing() {
    assert_eq!(
        problems(
            "[cell.m]\nclass = 'zz'\nregion = 'r'\nready = { q = 0 }\nhome = 'g'\n\
             [cell.g]\nclass = 'global'\nregion = 'r'\n"
        ),
        ["[cell.m] class: the placement has no class `zz`"]
    );
    // Nor is a home whose own class is unknown judged as a parent.
    assert_eq!(
        problems(
            "[cell.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n\
             [cell.g]\nclass = 'zz'\nregion = 'r'\n"
        ),
        ["[cell.g] class: the placement has no class `zz`"]
    );
}

#[test]
fn every_problem_is_reported_together() {
    let errors = problems(
        "[class.zz]\n\
         [class.global]\nready = { d = 1 }\nrequires = { model_x = 0 }\n\
         [cell.Global]\nclass = 'global'\nregion = 'US'\n\
         [cell.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'nope'\nready = { e = 0 }\n",
    );
    for expected in [
        "[class.zz]: the placement has no class `zz`",
        "[class.global] ready: `d` is not a segment of class `global`",
        "[class.global] requires: `model_x` is 0",
        "[cell.Global]: cell name `Global` must be",
        "[cell.Global] region: region name `US` must be",
        "[cell.m] home: the fleet has no cell `nope`",
        "[cell.m] ready: `e` is 0",
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
        "[cell.g]\nclass = 'global'\nregion = 'r'\nzone = 'a'\n",
        "[cell.g]\nclass = 'global'\n",
        "[cell.g]\nregion = 'r'\n",
        "[class.global]\nready = { a = -1 }\n",
        "[class.global]\nready = { a = 'two' }\n",
    ] {
        assert!(Fleet::from_toml(bad).is_err(), "accepted:\n{bad}");
    }
}

/// A cell's name becomes a host name, a URL path segment, part of a hub
/// service name, and part of a certificate identity; a region's is used
/// beside it. Both are DNS labels.
#[test]
fn cell_and_region_names_are_dns_labels() {
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
        let text = format!("[cell.{good}]\nclass = 'global'\nregion = '{good}'\n");
        fleet(&text).check(&laid()).expect("a usable name");
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
        let text = format!("[cell.\"{bad}\"]\nclass = 'global'\nregion = '{bad}'\n");
        let rule = "must be lowercase letters, digits, and `-`, start with a letter, end with a letter or digit, and be at most 63 characters";
        assert_eq!(
            problems(&text),
            [
                format!("[cell.{bad}]: cell name `{bad}` {rule}"),
                format!("[cell.{bad}] region: region name `{bad}` {rule}"),
            ],
            "{bad}"
        );
    }
}

/// Whether a home names a cell of the fleet does not depend on the cell's
/// class, so an unknown class does not hide a missing home.
#[test]
fn an_unknown_class_does_not_hide_a_missing_home() {
    assert_eq!(
        problems("[cell.m]\nclass = 'absent'\nregion = 'r'\nhome = 'missing'\n"),
        [
            "[cell.m] class: the placement has no class `absent`",
            "[cell.m] home: the fleet has no cell `missing`",
        ]
    );
    // The control: with a known class, the missing home alone.
    assert_eq!(
        problems("[cell.m]\nclass = 'model_x'\nregion = 'r'\nhome = 'missing'\n"),
        ["[cell.m] home: the fleet has no cell `missing`"]
    );
}
