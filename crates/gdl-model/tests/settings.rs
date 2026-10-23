//! A cell's own settings: which cell a process belongs to, how it finds
//! the other cells, and its quorum, checked against the deployment it
//! serves.

use std::collections::BTreeMap;

use gdl_model::{
    CellSettings, DirectoryBackend, HopInfo, Layout, Manifest, NodeInfo, Placement, Role, TypeInfo,
    layout,
};

/// `S => F { D }`: F's own hop crosses from `global` into `model_x`.
fn manifest() -> Manifest {
    let node = |name: &str, role| NodeInfo {
        name: name.into(),
        role,
        binding: (role != Role::Source).then(|| name.to_lowercase()),
    };
    let hop = |outer: &str, inner: &str| HopInfo {
        outer: outer.into(),
        forward: vec![],
        inner: inner.into(),
        backward: vec![],
        forward_types: vec![TypeInfo::new("T", true)],
        backward_types: vec![TypeInfo::new("T", true)],
    };
    Manifest {
        name: "classed".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("F", Role::Operator),
            node("D", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("F", "D")],
    }
}

const SEGMENTS: &str =
    "[segment.a]\nnodes = ['S']\n[segment.b]\nnodes = ['F']\n[segment.d]\nnodes = ['D']\n";

const CLASSES: &str = "[class.global]\nsegments = ['a', 'b']\n[class.model_x]\nsegments = ['d']\n";

fn laid(text: &str) -> Layout {
    layout(
        &manifest(),
        &Placement::from_toml(text).expect("a placement"),
    )
    .expect("a layout")
}

fn classed() -> Layout {
    laid(&format!("{SEGMENTS}{CLASSES}"))
}

fn settings(text: &str) -> CellSettings {
    toml::from_str(text).expect("a cell's settings")
}

const MODEL: &str = r#"
name = "model-x-01-us-east-1"
class = "model_x"
region = "us-east-1"
home = "global-us-east-1"
directory = "memory"
seeds = ["hub-a:1337", "hub-b:1337"]
hub = "model-x-01-hub:1337"
[ready]
d = 4
"#;

#[test]
fn a_cells_settings_are_read_whole() {
    assert_eq!(
        settings(MODEL),
        CellSettings {
            name: "model-x-01-us-east-1".into(),
            class: "model_x".into(),
            region: "us-east-1".into(),
            home: Some("global-us-east-1".into()),
            directory: DirectoryBackend::Memory,
            seeds: vec!["hub-a:1337".into(), "hub-b:1337".into()],
            ready: BTreeMap::from([("d".into(), 4)]),
            hub: Some("model-x-01-hub:1337".into()),
        }
    );
    let text = toml::to_string(&settings(MODEL)).expect("toml");
    assert_eq!(settings(&text), settings(MODEL), "{text}");
}

#[test]
fn only_name_class_and_region_are_required() {
    let least = settings("name = 'g'\nclass = 'global'\nregion = 'r'\n");
    assert_eq!(least.home, None);
    assert_eq!(least.directory, DirectoryBackend::Relay);
    assert!(least.seeds.is_empty());
    assert!(least.ready.is_empty());
    assert_eq!(least.hub, None);
    for missing in [
        "class = 'global'\nregion = 'r'\n",
        "name = 'g'\nregion = 'r'\n",
        "name = 'g'\nclass = 'global'\n",
    ] {
        assert!(
            toml::from_str::<CellSettings>(missing).is_err(),
            "{missing}"
        );
    }
}

#[test]
fn a_misspelled_key_or_an_unknown_backend_is_refused() {
    for bad in [
        "name = 'g'\nclass = 'global'\nregion = 'r'\nhone = 'h'\n",
        "name = 'g'\nclass = 'global'\nregion = 'r'\ndirectory = 'store'\n",
        // A fleet file has no live state, so it is not a backend.
        "name = 'g'\nclass = 'global'\nregion = 'r'\ndirectory = 'static'\n",
        "name = 'g'\nclass = 'global'\nregion = 'r'\nseeds = 'hub-a:1337'\n",
        "name = 'g'\nclass = 'global'\nregion = 'r'\nready = { a = -1 }\n",
    ] {
        assert!(toml::from_str::<CellSettings>(bad).is_err(), "{bad}");
    }
}

/// The `dir` backend's directory is the `dir` key: required with it, and
/// refused with any other backend, so that a path never sits unread.
#[test]
fn the_dir_backend_takes_its_path_and_no_other_backend_does() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let cell = settings(&format!("{LEAST}directory = 'dir'\ndir = '/srv/cells'\n"));
    assert_eq!(
        cell.directory,
        DirectoryBackend::Dir {
            path: "/srv/cells".into()
        }
    );
    let text = toml::to_string(&cell).expect("toml");
    assert_eq!(settings(&text), cell, "{text}");
    for (bad, why) in [
        ("directory = 'dir'\n", "needs `dir`"),
        ("directory = 'dir'\ndir = ''\n", "needs `dir`"),
        ("dir = '/srv/cells'\n", "\"relay\""),
        ("directory = 'relay'\ndir = '/srv/cells'\n", "\"relay\""),
        ("directory = 'memory'\ndir = '/srv/cells'\n", "\"memory\""),
    ] {
        let Err(error) = toml::from_str::<CellSettings>(&format!("{LEAST}{bad}")) else {
            panic!("{bad}: accepted");
        };
        assert!(error.to_string().contains(why), "{bad}: {error}");
    }
}

/// The address other cells' hubs reach this cell's hub at is `host:port`,
/// with a host name, an IPv4 address, or a bracketed IPv6 address.
#[test]
fn a_hub_address_is_host_and_port() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    for good in [
        "hub:1337",
        "hub.cells.internal:1",
        "10.0.0.7:65535",
        "[::1]:1337",
    ] {
        let cell = settings(&format!("{LEAST}hub = '{good}'\n"));
        assert_eq!(cell.hub.as_deref(), Some(good));
    }
    for bad in [
        "hub",
        "hub:",
        ":1337",
        "hub:0",
        "hub:65536",
        "hub:port",
        "hub a:1337",
        "http://hub:1337",
        "[zz]:1337",
        "::1:1337",
    ] {
        let Err(error) = toml::from_str::<CellSettings>(&format!("{LEAST}hub = '{bad}'\n")) else {
            panic!("{bad}: accepted");
        };
        assert!(
            error.to_string().contains(&format!("[cell] hub = {bad:?}")),
            "{bad}: {error}"
        );
    }
}

/// A cell's hub has no segment of its own: its settings fit when the
/// placement has the cell's class and every `ready` key is a segment of
/// it, the instance's rule without the instance.
#[test]
fn a_cells_hub_checks_its_settings_against_the_class() {
    settings(MODEL).check_hub(&classed()).expect("a model cell");
    let cases = [
        (
            laid(SEGMENTS),
            MODEL,
            "[cell] class = \"model_x\", but the placement has no classes",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_y'\nregion = 'r'\n",
            "[cell] class: the placement has no class `model_y`",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_x'\nregion = 'r'\nready = { d = 1, a = 2, b = 1 }\n",
            "[cell] ready: `a` is not a segment of class `model_x`; \
             [cell] ready: `b` is not a segment of class `model_x`",
        ),
    ];
    for (layout, text, expected) in cases {
        let Err(error) = settings(text).check_hub(&layout) else {
            panic!("accepted {text}");
        };
        assert_eq!(error.to_string(), expected, "{text}");
    }
}

/// A segment the settings give no quorum needs one usable instance.
#[test]
fn a_quorum_falls_back_to_one() {
    let cell = settings(MODEL);
    assert_eq!(cell.quorum("d"), 4);
    assert_eq!(cell.quorum("e"), 1);
}

#[test]
fn settings_that_fit_the_deployment_pass() {
    settings(MODEL)
        .check(&classed(), "d")
        .expect("a model cell");
    settings("name = 'g'\nclass = 'global'\nregion = 'r'\nready = { a = 2, b = 1 }\n")
        .check(&classed(), "b")
        .expect("a global cell");
}

#[test]
fn settings_that_do_not_fit_the_deployment_are_refused() {
    let cases = [
        (
            laid(SEGMENTS),
            MODEL,
            "d",
            "[cell] class = \"model_x\", but the placement has no classes",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_y'\nregion = 'r'\n",
            "d",
            "[cell] class: the placement has no class `model_y`",
        ),
        (
            classed(),
            MODEL,
            "a",
            "[cell] class: this instance's segment `a` is in class `global`, not `model_x`",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_x'\nregion = 'r'\nready = { d = 1, a = 2 }\n",
            "d",
            "[cell] ready: `a` is not a segment of class `model_x`",
        ),
    ];
    for (layout, text, segment, expected) in cases {
        let Err(error) = settings(text).check(&layout, segment) else {
            panic!("accepted {text} for `{segment}`");
        };
        assert_eq!(error.to_string(), expected, "{text}");
    }
    // Every problem at once.
    let Err(error) =
        settings("name = 'm'\nclass = 'model_x'\nregion = 'r'\nready = { a = 2, b = 1 }\n")
            .check(&classed(), "a")
    else {
        panic!("accepted");
    };
    assert_eq!(
        error.to_string(),
        "[cell] class: this instance's segment `a` is in class `global`, not `model_x`; \
         [cell] ready: `a` is not a segment of class `model_x`; \
         [cell] ready: `b` is not a segment of class `model_x`"
    );
}

/// A segment the placement does not have is in no class, so not in the
/// cell's.
#[test]
fn a_segment_outside_the_placement_is_not_in_the_cells_class() {
    let Err(error) = settings(MODEL).check(&classed(), "zz") else {
        panic!("accepted a segment the placement does not have");
    };
    assert_eq!(
        error.to_string(),
        "[cell] class: this instance's segment `zz` is not in class `model_x`"
    );
}
