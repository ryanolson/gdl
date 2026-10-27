//! A cell's own settings: which cell a process belongs to, how it finds
//! the other cells, and its quorum, checked against the deployment it
//! serves.

use std::collections::BTreeMap;
use std::time::Duration;

use gdl_model::{
    CellSettings, DirectoryBackend, HopInfo, HubTiming, Layout, Manifest, NodeInfo, Placement,
    Role, TypeInfo, WatchCaps, layout,
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
        forward_types: vec![TypeInfo::new("T", true, Default::default())],
        backward_types: vec![TypeInfo::new("T", true, Default::default())],
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
            layout: None,
            class: "model_x".into(),
            region: "us-east-1".into(),
            home: Some("global-us-east-1".into()),
            directory: DirectoryBackend::Memory,
            ready: BTreeMap::from([("d".into(), 4)]),
            requires: BTreeMap::new(),
            hub: Some("model-x-01-hub:1337".into()),
            summary: Duration::from_secs(10),
            timing: Default::default(),
        }
    );
    let text = toml::to_string(&settings(MODEL)).expect("toml");
    assert_eq!(settings(&text), settings(MODEL), "{text}");
}

#[test]
fn only_name_class_and_region_are_required() {
    let least = settings("name = 'g'\nclass = 'global'\nregion = 'r'\n");
    assert_eq!(least.home, None);
    assert_eq!(least.directory, DirectoryBackend::DEFAULT);
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
        // Nothing reads seeds: the relay mesh among global hubs is not built.
        "name = 'g'\nclass = 'global'\nregion = 'r'\nseeds = ['hub-a:1337']\n",
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
            error.to_string().contains(&format!("[cell] hub: {bad:?}")),
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
            "[cell] class: the placement has no classes, and so no class `model_x`",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_y'\nregion = 'r'\n",
            "[cell] class: the placement has no class `model_y`",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_x'\nregion = 'r'\ndirectory = 'memory'\nready = { d = 1, a = 2, b = 1 }\n",
            "[cell] ready.a: not a segment of class `model_x`\n\
             [cell] ready.b: not a segment of class `model_x`",
        ),
    ];
    for (layout, text, expected) in cases {
        let Err(error) = settings(text).check_hub(&layout) else {
            panic!("accepted {text}");
        };
        assert_eq!(error.to_string(), expected, "{text}");
    }
}

/// The `relay` backend's parent hub is the `parent_hub` key: a hub
/// address, refused with any other backend, so that it never sits
/// unread. A cell's hub needs it when another class sends to its class,
/// since it reports there, and it is refused for a class that nothing
/// sends to, whose cell has no parent.
#[test]
fn the_relay_backend_takes_the_parent_hub_where_a_class_is_sent_to() {
    const CHILD: &str = "name = 'm'\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n";
    let cell = settings(&format!("{CHILD}parent_hub = 'global-hub:1337'\n"));
    assert_eq!(
        cell.directory,
        DirectoryBackend::Relay {
            parent_hub: Some("global-hub:1337".into())
        }
    );
    let text = toml::to_string(&cell).expect("toml");
    assert_eq!(settings(&text), cell, "{text}");
    cell.check_hub(&classed())
        .expect("a child with its parent's hub");
    cell.check(&classed(), "d")
        .expect("an instance of the child");

    for (backend, expected) in [
        ("directory = 'memory'\n", "read only by the `relay` backend"),
        (
            "directory = 'dir'\ndir = '/srv/cells'\n",
            "read only by the `relay` backend",
        ),
        ("", "must be `host:port`"),
    ] {
        let parent = if backend.is_empty() {
            "parent_hub = 'http://global-hub'\n"
        } else {
            "parent_hub = 'global-hub:1337'\n"
        };
        let text = format!("{CHILD}{backend}{parent}");
        let error = toml::from_str::<CellSettings>(&text).expect_err(&text);
        assert!(error.to_string().contains("parent_hub"), "{error}");
        assert!(error.to_string().contains(expected), "{error}");
    }

    // A hub needs it; an instance, which reports nothing, does not.
    let orphan = settings(CHILD);
    orphan
        .check(&classed(), "d")
        .expect("an instance reads no parent hub");
    let Err(error) = orphan.check_hub(&classed()) else {
        panic!("a child's hub without its parent's");
    };
    assert_eq!(
        error.to_string(),
        "[cell] parent_hub: missing: class `global` sends to class `model_x`, so with directory = \"relay\" the cell's hub reports its record to its parent's hub, at this address"
    );

    // A cell of a class that nothing sends to reports nowhere.
    let top = settings("name = 'g'\nclass = 'global'\nregion = 'r'\nparent_hub = 'other:1337'\n");
    for error in [
        top.check_hub(&classed())
            .expect_err("a parent hub for a top cell"),
        top.check(&classed(), "a")
            .expect_err("a parent hub for a top cell"),
    ] {
        assert_eq!(
            error.to_string(),
            "[cell] parent_hub: no class sends to class `global`, so its cell has no parent to report to"
        );
    }
    settings("name = 'g'\nclass = 'global'\nregion = 'r'\n")
        .check_hub(&classed())
        .expect("a top cell reports nowhere");
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
            "[cell] class: the placement has no classes, and so no class `model_x`",
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
            "[cell] ready.a: not a segment of class `model_x`",
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
        "[cell] class: this instance's segment `a` is in class `global`, not `model_x`\n\
         [cell] ready.a: not a segment of class `model_x`\n\
         [cell] ready.b: not a segment of class `model_x`"
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

const GLOBAL: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";

/// A cell's health needs this many ready cells of each class it sends to:
/// `[requires]`, read whole and kept through TOML.
#[test]
fn a_cells_requirements_are_read_whole() {
    let cell = settings(&format!("{GLOBAL}[requires]\nmodel_x = 2\n"));
    assert_eq!(cell.requires, BTreeMap::from([("model_x".into(), 2)]));
    let text = toml::to_string(&cell).expect("toml");
    assert_eq!(settings(&text), cell, "{text}");
    assert!(settings(GLOBAL).requires.is_empty());
    cell.check_hub(&classed()).expect("global sends to model_x");
    cell.check(&classed(), "b")
        .expect("global sends to model_x");
}

/// A requirement names a class the cell's class sends to, with a count of
/// at least 1, for a hub and for an instance alike.
#[test]
fn a_requirement_of_a_class_not_sent_to_or_of_zero_is_refused() {
    let cases = [
        (
            format!("{GLOBAL}[requires]\nglobal = 1\n"),
            "b",
            "[cell] requires.global: not a class that `global` sends to",
        ),
        (
            format!("{GLOBAL}[requires]\nmodel_y = 1\n"),
            "b",
            "[cell] requires.model_y: not a class that `global` sends to",
        ),
        (
            format!("{GLOBAL}[requires]\nmodel_x = 0\n"),
            "b",
            "[cell] requires.model_x: is 0; a requirement is at least 1",
        ),
        (
            "name = 'm'\nclass = 'model_x'\nregion = 'r'\ndirectory = 'memory'\n[requires]\nglobal = 1\n".to_string(),
            "d",
            "[cell] requires.global: not a class that `model_x` sends to",
        ),
    ];
    for (text, segment, expected) in cases {
        let Err(error) = settings(&text).check_hub(&classed()) else {
            panic!("accepted {text}");
        };
        assert_eq!(error.to_string(), expected, "{text}");
        let Err(error) = settings(&text).check(&classed(), segment) else {
            panic!("accepted {text} for `{segment}`");
        };
        assert_eq!(error.to_string(), expected, "{text}");
    }
}

/// The load summary's period is `summary_ms`: 10 s unless set, and at
/// least 1 s, since an instance reports and its hub summarizes once a
/// period.
#[test]
fn the_summary_period_is_read_and_has_a_floor() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    assert_eq!(settings(LEAST).summary, Duration::from_secs(10));
    let cell = settings(&format!("{LEAST}summary_ms = 1000\n"));
    assert_eq!(cell.summary, Duration::from_secs(1));
    let text = toml::to_string(&cell).expect("toml");
    assert_eq!(settings(&text), cell, "{text}");
    for bad in [
        "summary_ms = 999\n",
        "summary_ms = 0\n",
        "summary_ms = -1\n",
    ] {
        let error = toml::from_str::<CellSettings>(&format!("{LEAST}{bad}"))
            .expect_err(bad)
            .to_string();
        assert!(
            bad.contains('-') || error.contains("summary_ms"),
            "{bad}: {error}"
        );
    }
}

/// `layout` names the layout the cell runs, when its spec has several: a
/// layout name, read where it is written, and written back.
#[test]
fn a_cell_names_its_layout() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    assert_eq!(settings(LEAST).layout, None);
    let cell = settings(&format!("{LEAST}layout = 'regional'\n"));
    assert_eq!(cell.layout.as_deref(), Some("regional"));
    let text = toml::to_string(&cell).expect("toml");
    assert!(text.contains("layout = \"regional\""), "{text}");
    assert_eq!(settings(&text), cell, "{text}");
    let value: toml::Value =
        toml::from_str(&format!("{LEAST}layout = 'Regional'\n")).expect("toml");
    let problems = CellSettings::read(value).expect_err("not a layout name");
    assert_eq!(problems.0.len(), 1, "{problems}");
    let problem = &problems.0[0];
    assert_eq!(
        (problem.table.as_deref(), problem.key.as_deref()),
        (Some("cell"), Some("layout")),
        "{problem}"
    );
    assert!(
        problem
            .message
            .starts_with("layout name `Regional` must be"),
        "{problem}"
    );
}

/// The hub's own pace and watch caps are `[cell]` keys that only the
/// cell's hub reads: each one set is read back, and is written back.
#[test]
fn a_cells_hub_timing_is_read_from_its_table() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let cell = settings(&format!(
        "{LEAST}hold_ms = 500\nheartbeat_ms = 2000\nlease_ms = 9000\nwatch_buffer = 64\nlost_chooser_ms = 4000\n[watch_caps]\ncell = 3\nrelay = 4\nfollow = 5\n"
    ));
    assert_eq!(
        cell.timing,
        HubTiming {
            hold: Duration::from_millis(500),
            heartbeat: Duration::from_secs(2),
            lease: Duration::from_secs(9),
            watch_buffer: 64,
            lost_chooser: Duration::from_secs(4),
            watch_caps: WatchCaps {
                cell: 3,
                relay: 4,
                follow: 5,
            },
        }
    );
    let text = toml::to_string(&cell).expect("toml");
    assert_eq!(settings(&text), cell, "{text}");
    // One cap alone leaves the others at their defaults.
    let one = settings(&format!("{LEAST}[watch_caps]\nrelay = 7\n"));
    assert_eq!(
        one.timing.watch_caps,
        WatchCaps {
            cell: 2048,
            relay: 7,
            follow: 16384,
        }
    );
}

/// With none of the hub's keys set, its pace is what it was when the
/// pace was fixed in the binary; and a default is not written out, so a
/// generated `[cell]` table stays as it was.
#[test]
fn with_no_timing_set_a_cells_hub_keeps_its_defaults() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let cell = settings(LEAST);
    assert_eq!(cell.timing.hold, Duration::from_secs(2));
    assert_eq!(cell.timing.heartbeat, Duration::from_secs(10));
    assert_eq!(cell.timing.lease, Duration::from_secs(30));
    assert_eq!(cell.timing.watch_buffer, 1024);
    assert_eq!(cell.timing.lost_chooser, Duration::from_secs(30));
    assert_eq!(
        cell.timing.watch_caps,
        WatchCaps {
            cell: 2048,
            relay: 2048,
            follow: 16384,
        }
    );
    assert_eq!(cell.timing, HubTiming::default());
    let text = toml::to_string(&cell).expect("toml");
    for key in [
        "hold_ms",
        "heartbeat_ms",
        "lease_ms",
        "watch_buffer",
        "lost_chooser_ms",
        "watch_caps",
    ] {
        assert!(!text.contains(key), "{key} in {text}");
    }
}

/// A value the hub cannot run with is refused where it is read, naming
/// its key: a lease no longer than the heartbeat, which readers would
/// let lapse between two publishes, also when only one of the two is
/// set; a heartbeat or a lease of zero; a watch buffer of zero or past
/// its ceiling; a cap of zero, which would refuse every watch; a bound of
/// zero on a lost chooser, which would stop waiting for a chooser the
/// moment its watch of the relay closes, before it can watch again.
#[test]
fn a_hub_timing_the_hub_cannot_run_with_is_refused_by_key() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    for (bad, key, says) in [
        (
            "heartbeat_ms = 5000\nlease_ms = 5000\n",
            "lease_ms",
            "longer than the heartbeat",
        ),
        (
            "heartbeat_ms = 5000\nlease_ms = 4000\n",
            "lease_ms",
            "longer than the heartbeat",
        ),
        // The lease left at its default, 30 s.
        (
            "heartbeat_ms = 30000\n",
            "heartbeat_ms",
            "shorter than the lease",
        ),
        // The heartbeat left at its default, 10 s.
        (
            "lease_ms = 10000\n",
            "lease_ms",
            "longer than the heartbeat",
        ),
        ("heartbeat_ms = 0\n", "heartbeat_ms", "at least 1"),
        ("lease_ms = 0\n", "lease_ms", "at least 1"),
        ("watch_buffer = 0\n", "watch_buffer", "at least 1"),
        ("watch_buffer = 65537\n", "watch_buffer", "at most 65536"),
        ("lost_chooser_ms = 0\n", "lost_chooser_ms", "at least 1"),
        ("[watch_caps]\ncell = 0\n", "watch_caps.cell", "at least 1"),
        (
            "[watch_caps]\nrelay = 0\n",
            "watch_caps.relay",
            "at least 1",
        ),
        (
            "[watch_caps]\nfollow = 0\n",
            "watch_caps.follow",
            "at least 1",
        ),
    ] {
        let value: toml::Value = toml::from_str(&format!("{LEAST}{bad}")).expect("toml");
        let problems = CellSettings::read(value).expect_err(bad);
        assert_eq!(problems.0.len(), 1, "{bad}: {problems}");
        let problem = &problems.0[0];
        assert_eq!(
            (problem.table.as_deref(), problem.key.as_deref()),
            (Some("cell"), Some(key)),
            "{bad}: {problem}"
        );
        assert!(problem.message.contains(says), "{bad}: {problem}");
    }
    // A cap the table does not have is refused, as any other key is.
    let value: toml::Value =
        toml::from_str(&format!("{LEAST}[watch_caps]\nfollows = 1\n")).expect("toml");
    let problems = CellSettings::read(value).expect_err("an unknown cap");
    assert!(problems.to_string().contains("follows"), "{problems}");
    // The bounds themselves are taken.
    let edge = settings(&format!(
        "{LEAST}directory = 'memory'\nheartbeat_ms = 1\nlease_ms = 2\nhold_ms = 0\nwatch_buffer = 65536\n"
    ));
    assert_eq!(edge.timing.watch_buffer, HubTiming::MAX_WATCH_BUFFER);
    assert_eq!(edge.timing.hold, Duration::ZERO);
}

/// Under the `relay` backend, the default, a cell's hub writes its record
/// at its parent's cadence, never less than 100 ms apart, and within a
/// third of the lease: a lease under 300 ms could lapse at the parent
/// between two reports, so it is refused, naming the key. The other
/// backends write at the heartbeat, and take it.
#[test]
fn a_relay_cell_refuses_a_lease_its_reports_cannot_cover() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let value: toml::Value =
        toml::from_str(&format!("{LEAST}heartbeat_ms = 50\nlease_ms = 299\n")).expect("toml");
    let problems = CellSettings::read(value).expect_err("a lease under three reports");
    assert_eq!(problems.0.len(), 1, "{problems}");
    let problem = &problems.0[0];
    assert_eq!(
        (problem.table.as_deref(), problem.key.as_deref()),
        (Some("cell"), Some("lease_ms")),
        "{problem}"
    );
    assert!(problem.message.contains("300"), "{problem}");
    let least = settings(&format!("{LEAST}heartbeat_ms = 50\nlease_ms = 300\n"));
    assert_eq!(least.timing.lease, HubTiming::MIN_RELAY_LEASE);
    for backend in [
        "directory = 'memory'\n",
        "directory = 'dir'\ndir = '/srv/cells'\n",
    ] {
        let cell = settings(&format!(
            "{LEAST}{backend}heartbeat_ms = 50\nlease_ms = 200\n"
        ));
        assert_eq!(
            cell.timing.lease,
            std::time::Duration::from_millis(200),
            "{backend}"
        );
    }
}
