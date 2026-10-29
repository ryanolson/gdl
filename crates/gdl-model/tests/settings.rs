//! A pool's own settings: which pool a process belongs to, how it finds
//! the other pools, and its quorum, checked against the deployment it
//! serves.

use std::collections::BTreeMap;
use std::time::Duration;

use gdl_model::{
    DirectoryBackend, HopInfo, HubTiming, Layout, Manifest, NodeInfo, Placement, PoolSettings,
    PropertyKey, Role, TypeInfo, WatchCaps, layout,
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

fn settings(text: &str) -> PoolSettings {
    toml::from_str(text).expect("a pool's settings")
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
fn a_pools_settings_are_read_whole() {
    assert_eq!(
        settings(MODEL),
        PoolSettings {
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
            properties: Vec::new(),
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
            toml::from_str::<PoolSettings>(missing).is_err(),
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
        assert!(toml::from_str::<PoolSettings>(bad).is_err(), "{bad}");
    }
}

/// The `dir` backend's directory is the `dir` key: required with it, and
/// refused with any other backend, so that a path never sits unread.
#[test]
fn the_dir_backend_takes_its_path_and_no_other_backend_does() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let pool = settings(&format!("{LEAST}directory = 'dir'\ndir = '/srv/pools'\n"));
    assert_eq!(
        pool.directory,
        DirectoryBackend::Dir {
            path: "/srv/pools".into()
        }
    );
    let text = toml::to_string(&pool).expect("toml");
    assert_eq!(settings(&text), pool, "{text}");
    for (bad, why) in [
        ("directory = 'dir'\n", "needs `dir`"),
        ("directory = 'dir'\ndir = ''\n", "needs `dir`"),
        ("dir = '/srv/pools'\n", "\"relay\""),
        ("directory = 'relay'\ndir = '/srv/pools'\n", "\"relay\""),
        ("directory = 'memory'\ndir = '/srv/pools'\n", "\"memory\""),
    ] {
        let Err(error) = toml::from_str::<PoolSettings>(&format!("{LEAST}{bad}")) else {
            panic!("{bad}: accepted");
        };
        assert!(error.to_string().contains(why), "{bad}: {error}");
    }
}

/// The address other pools' hubs reach this pool's hub at is `host:port`,
/// with a host name, an IPv4 address, or a bracketed IPv6 address.
#[test]
fn a_hub_address_is_host_and_port() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    for good in [
        "hub:1337",
        "hub.pools.internal:1",
        "10.0.0.7:65535",
        "[::1]:1337",
    ] {
        let pool = settings(&format!("{LEAST}hub = '{good}'\n"));
        assert_eq!(pool.hub.as_deref(), Some(good));
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
        let Err(error) = toml::from_str::<PoolSettings>(&format!("{LEAST}hub = '{bad}'\n")) else {
            panic!("{bad}: accepted");
        };
        assert!(
            error.to_string().contains(&format!("[pool] hub: {bad:?}")),
            "{bad}: {error}"
        );
    }
}

/// A pool's hub has no segment of its own: its settings fit when the
/// placement has the pool's class and every `ready` key is a segment of
/// it, the instance's rule without the instance.
#[test]
fn a_pools_hub_checks_its_settings_against_the_class() {
    settings(MODEL).check_hub(&classed()).expect("a model pool");
    let cases = [
        (
            laid(SEGMENTS),
            MODEL,
            "[pool] class: the placement has no classes, and so no class `model_x`",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_y'\nregion = 'r'\n",
            "[pool] class: the placement has no class `model_y`",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_x'\nregion = 'r'\ndirectory = 'memory'\nready = { d = 1, a = 2, b = 1 }\n",
            "[pool] ready.a: not a segment of class `model_x`\n\
             [pool] ready.b: not a segment of class `model_x`",
        ),
        (
            classed(),
            "name = 'g'\nclass = 'global'\nregion = 'r'\nstate_dir = '/s'\nproperties = { test = [{ name = 'x' }] }\n",
            "[pool] properties: no class sends to class `global`, so its pool serves no properties to another",
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
/// unread. A pool's hub needs it when another class sends to its class,
/// since it reports there, and it is refused for a class that nothing
/// sends to, whose pool has no parent.
#[test]
fn the_relay_backend_takes_the_parent_hub_where_a_class_is_sent_to() {
    const CHILD: &str = "name = 'm'\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\n";
    let pool = settings(&format!("{CHILD}parent_hub = 'global-hub:1337'\n"));
    assert_eq!(
        pool.directory,
        DirectoryBackend::Relay {
            parent_hub: Some("global-hub:1337".into()),
            state_dir: None,
        }
    );
    let text = toml::to_string(&pool).expect("toml");
    assert_eq!(settings(&text), pool, "{text}");
    pool.check_hub(&classed())
        .expect("a child with its parent's hub");
    pool.check(&classed(), "d")
        .expect("an instance of the child");

    for (backend, expected) in [
        ("directory = 'memory'\n", "read only by the `relay` backend"),
        (
            "directory = 'dir'\ndir = '/srv/pools'\n",
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
        let error = toml::from_str::<PoolSettings>(&text).expect_err(&text);
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
        "[pool] parent_hub: missing: class `global` sends to class `model_x`, so with directory = \"relay\" the pool's hub reports its record to its parent's hub, at this address"
    );
    // A hub whose program gives it its directory reports nowhere either.
    orphan
        .check_hub_given_directory(&classed())
        .expect("a hub with a given directory reads no parent hub");

    // A pool of a class that nothing sends to reports nowhere.
    let top = settings(
        "name = 'g'\nclass = 'global'\nregion = 'r'\nparent_hub = 'other:1337'\nstate_dir = '/s'\n",
    );
    for error in [
        top.check_hub(&classed())
            .expect_err("a parent hub for a top pool"),
        top.check(&classed(), "a")
            .expect_err("a parent hub for a top pool"),
    ] {
        assert_eq!(
            error.to_string(),
            "[pool] parent_hub: no class sends to class `global`, so its pool has no parent to report to"
        );
    }
    settings("name = 'g'\nclass = 'global'\nregion = 'r'\nstate_dir = '/var/lib/gdl-hub'\n")
        .check_hub(&classed())
        .expect("a top pool reports nowhere");
}

/// The `relay` backend's state directory is the `state_dir` key: where a
/// parent pool's hub keeps its children's records across its restart. A
/// pool's hub needs it when its class sends to another class, and it is
/// refused for a class that sends to none, whose pool has no children, and
/// with any other backend, so that it never sits unread.
#[test]
fn the_relay_backend_takes_the_state_dir_where_a_class_sends() {
    const TOP: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let pool = settings(&format!("{TOP}state_dir = '/var/lib/gdl-hub'\n"));
    assert_eq!(
        pool.directory,
        DirectoryBackend::Relay {
            parent_hub: None,
            state_dir: Some("/var/lib/gdl-hub".into()),
        }
    );
    let text = toml::to_string(&pool).expect("toml");
    assert_eq!(settings(&text), pool, "{text}");
    pool.check_hub(&classed()).expect("a parent with its state");
    pool.check(&classed(), "a")
        .expect("an instance of the parent");

    for (bad, why) in [
        ("state_dir = ''\n", "[pool] state_dir: is empty"),
        (
            "directory = 'memory'\nstate_dir = '/s'\n",
            "[pool] state_dir: \"/s\" is read only by the `relay` backend, not by directory = \"memory\"",
        ),
        (
            "directory = 'dir'\ndir = '/srv/pools'\nstate_dir = '/s'\n",
            "[pool] state_dir: \"/s\" is read only by the `relay` backend, not by directory = \"dir\"",
        ),
    ] {
        let Err(error) = toml::from_str::<PoolSettings>(&format!("{TOP}{bad}")) else {
            panic!("{bad}: accepted");
        };
        assert!(error.to_string().contains(why), "{bad}: {error}");
    }

    // A parent's hub needs it; an instance, which holds no records, does
    // not.
    let forgetful = settings(TOP);
    forgetful
        .check(&classed(), "a")
        .expect("an instance reads no state directory");
    let Err(error) = forgetful.check_hub(&classed()) else {
        panic!("a parent's hub without its state");
    };
    assert_eq!(
        error.to_string(),
        "[pool] state_dir: missing: class `global` sends to class `model_x`, so with directory = \"relay\" the pool's hub holds its children's records, and keeps them in this directory across its restart"
    );
    forgetful
        .check_hub_given_directory(&classed())
        .expect("a hub with a given directory keeps no records of its own");

    // A pool of a class that sends to none has no children.
    let leaf = settings(
        "name = 'm'\nclass = 'model_x'\nregion = 'r'\nhome = 'g'\nparent_hub = 'g:1337'\nstate_dir = '/s'\n",
    );
    for error in [
        leaf.check_hub(&classed())
            .expect_err("a state directory for a leaf pool"),
        leaf.check(&classed(), "d")
            .expect_err("a state directory for a leaf pool"),
    ] {
        assert_eq!(
            error.to_string(),
            "[pool] state_dir: class `model_x` sends to no class, so its pool has no children whose records its hub keeps"
        );
    }
}

/// A segment the settings give no quorum needs one usable instance.
#[test]
fn a_quorum_falls_back_to_one() {
    let pool = settings(MODEL);
    assert_eq!(pool.quorum("d"), 4);
    assert_eq!(pool.quorum("e"), 1);
}

#[test]
fn settings_that_fit_the_deployment_pass() {
    settings(MODEL)
        .check(&classed(), "d")
        .expect("a model pool");
    settings("name = 'g'\nclass = 'global'\nregion = 'r'\nready = { a = 2, b = 1 }\n")
        .check(&classed(), "b")
        .expect("a global pool");
}

#[test]
fn settings_that_do_not_fit_the_deployment_are_refused() {
    let cases = [
        (
            laid(SEGMENTS),
            MODEL,
            "d",
            "[pool] class: the placement has no classes, and so no class `model_x`",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_y'\nregion = 'r'\n",
            "d",
            "[pool] class: the placement has no class `model_y`",
        ),
        (
            classed(),
            MODEL,
            "a",
            "[pool] class: this instance's segment `a` is in class `global`, not `model_x`",
        ),
        (
            classed(),
            "name = 'm'\nclass = 'model_x'\nregion = 'r'\nready = { d = 1, a = 2 }\n",
            "d",
            "[pool] ready.a: not a segment of class `model_x`",
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
        "[pool] class: this instance's segment `a` is in class `global`, not `model_x`\n\
         [pool] ready.a: not a segment of class `model_x`\n\
         [pool] ready.b: not a segment of class `model_x`"
    );
}

/// A segment the placement does not have is in no class, so not in the
/// pool's.
#[test]
fn a_segment_outside_the_placement_is_not_in_the_pools_class() {
    let Err(error) = settings(MODEL).check(&classed(), "zz") else {
        panic!("accepted a segment the placement does not have");
    };
    assert_eq!(
        error.to_string(),
        "[pool] class: this instance's segment `zz` is not in class `model_x`"
    );
}

const GLOBAL: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\nstate_dir = '/var/lib/gdl-hub'\n";

/// A pool's health needs this many ready pools of each class it sends to:
/// `[requires]`, read whole and kept through TOML.
#[test]
fn a_pools_requirements_are_read_whole() {
    let pool = settings(&format!("{GLOBAL}[requires]\nmodel_x = 2\n"));
    assert_eq!(pool.requires, BTreeMap::from([("model_x".into(), 2)]));
    let text = toml::to_string(&pool).expect("toml");
    assert_eq!(settings(&text), pool, "{text}");
    assert!(settings(GLOBAL).requires.is_empty());
    pool.check_hub(&classed()).expect("global sends to model_x");
    pool.check(&classed(), "b")
        .expect("global sends to model_x");
}

/// A requirement names a class the pool's class sends to, with a count of
/// at least 1, for a hub and for an instance alike.
#[test]
fn a_requirement_of_a_class_not_sent_to_or_of_zero_is_refused() {
    let cases = [
        (
            format!("{GLOBAL}[requires]\nglobal = 1\n"),
            "b",
            "[pool] requires.global: not a class that `global` sends to",
        ),
        (
            format!("{GLOBAL}[requires]\nmodel_y = 1\n"),
            "b",
            "[pool] requires.model_y: not a class that `global` sends to",
        ),
        (
            format!("{GLOBAL}[requires]\nmodel_x = 0\n"),
            "b",
            "[pool] requires.model_x: is 0; a requirement is at least 1",
        ),
        (
            "name = 'm'\nclass = 'model_x'\nregion = 'r'\ndirectory = 'memory'\n[requires]\nglobal = 1\n".to_string(),
            "d",
            "[pool] requires.global: not a class that `model_x` sends to",
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
    let pool = settings(&format!("{LEAST}summary_ms = 1000\n"));
    assert_eq!(pool.summary, Duration::from_secs(1));
    let text = toml::to_string(&pool).expect("toml");
    assert_eq!(settings(&text), pool, "{text}");
    for bad in [
        "summary_ms = 999\n",
        "summary_ms = 0\n",
        "summary_ms = -1\n",
    ] {
        let error = toml::from_str::<PoolSettings>(&format!("{LEAST}{bad}"))
            .expect_err(bad)
            .to_string();
        assert!(
            bad.contains('-') || error.contains("summary_ms"),
            "{bad}: {error}"
        );
    }
}

/// `layout` names the layout the pool runs, when its spec has several: a
/// layout name, read where it is written, and written back.
#[test]
fn a_pool_names_its_layout() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    assert_eq!(settings(LEAST).layout, None);
    let pool = settings(&format!("{LEAST}layout = 'regional'\n"));
    assert_eq!(pool.layout.as_deref(), Some("regional"));
    let text = toml::to_string(&pool).expect("toml");
    assert!(text.contains("layout = \"regional\""), "{text}");
    assert_eq!(settings(&text), pool, "{text}");
    let value: toml::Value =
        toml::from_str(&format!("{LEAST}layout = 'Regional'\n")).expect("toml");
    let problems = PoolSettings::read(value).expect_err("not a layout name");
    assert_eq!(problems.0.len(), 1, "{problems}");
    let problem = &problems.0[0];
    assert_eq!(
        (problem.table.as_deref(), problem.key.as_deref()),
        (Some("pool"), Some("layout")),
        "{problem}"
    );
    assert!(
        problem
            .message
            .starts_with("layout name `Regional` must be"),
        "{problem}"
    );
}

/// The hub's own pace and watch caps are `[pool]` keys that only the
/// pool's hub reads: each one set is read back, and is written back.
#[test]
fn a_pools_hub_timing_is_read_from_its_table() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let pool = settings(&format!(
        "{LEAST}hold_ms = 500\nheartbeat_ms = 2000\nlease_ms = 9000\nwatch_buffer = 64\nlost_chooser_ms = 4000\nkeyed_capacity = 10\n[watch_caps]\npool = 3\nrelay = 4\nfollow = 5\n"
    ));
    assert_eq!(
        pool.timing,
        HubTiming {
            hold: Duration::from_millis(500),
            heartbeat: Duration::from_secs(2),
            lease: Duration::from_secs(9),
            watch_buffer: 64,
            lost_chooser: Duration::from_secs(4),
            watch_caps: WatchCaps {
                pool: 3,
                relay: 4,
                follow: 5,
            },
            keyed_capacity: 10,
        }
    );
    let text = toml::to_string(&pool).expect("toml");
    assert_eq!(settings(&text), pool, "{text}");
    // One cap alone leaves the others at their defaults.
    let one = settings(&format!("{LEAST}[watch_caps]\nrelay = 7\n"));
    assert_eq!(
        one.timing.watch_caps,
        WatchCaps {
            pool: 2048,
            relay: 7,
            follow: 16384,
        }
    );
}

/// With none of the hub's keys set, its pace is what it was when the
/// pace was fixed in the binary; and a default is not written out, so a
/// generated `[pool]` table stays as it was.
#[test]
fn with_no_timing_set_a_pools_hub_keeps_its_defaults() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let pool = settings(LEAST);
    assert_eq!(pool.timing.hold, Duration::from_secs(2));
    assert_eq!(pool.timing.heartbeat, Duration::from_secs(10));
    assert_eq!(pool.timing.lease, Duration::from_secs(30));
    assert_eq!(pool.timing.watch_buffer, 1024);
    assert_eq!(pool.timing.lost_chooser, Duration::from_secs(30));
    assert_eq!(pool.timing.keyed_capacity, 1_000_000);
    assert_eq!(
        pool.timing.watch_caps,
        WatchCaps {
            pool: 2048,
            relay: 2048,
            follow: 16384,
        }
    );
    assert_eq!(pool.timing, HubTiming::default());
    let text = toml::to_string(&pool).expect("toml");
    for key in [
        "hold_ms",
        "heartbeat_ms",
        "lease_ms",
        "watch_buffer",
        "lost_chooser_ms",
        "watch_caps",
        "keyed_capacity",
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
/// moment its watch of the relay closes, before it can watch again; a
/// keyed record capacity of zero, which could hold no record.
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
        ("[watch_caps]\npool = 0\n", "watch_caps.pool", "at least 1"),
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
        ("keyed_capacity = 0\n", "keyed_capacity", "at least 1"),
    ] {
        let value: toml::Value = toml::from_str(&format!("{LEAST}{bad}")).expect("toml");
        let problems = PoolSettings::read(value).expect_err(bad);
        assert_eq!(problems.0.len(), 1, "{bad}: {problems}");
        let problem = &problems.0[0];
        assert_eq!(
            (problem.table.as_deref(), problem.key.as_deref()),
            (Some("pool"), Some(key)),
            "{bad}: {problem}"
        );
        assert!(problem.message.contains(says), "{bad}: {problem}");
    }
    // A cap the table does not have is refused, as any other key is.
    let value: toml::Value =
        toml::from_str(&format!("{LEAST}[watch_caps]\nfollows = 1\n")).expect("toml");
    let problems = PoolSettings::read(value).expect_err("an unknown cap");
    assert!(problems.to_string().contains("follows"), "{problems}");
    // The bounds themselves are taken.
    let edge = settings(&format!(
        "{LEAST}directory = 'memory'\nheartbeat_ms = 1\nlease_ms = 2\nhold_ms = 0\nwatch_buffer = 65536\n"
    ));
    assert_eq!(edge.timing.watch_buffer, HubTiming::MAX_WATCH_BUFFER);
    assert_eq!(edge.timing.hold, Duration::ZERO);
}

/// Under the `relay` backend, the default, a pool's hub writes its record
/// at its parent's cadence, never less than 100 ms apart, and within a
/// third of the lease: a lease under 300 ms could lapse at the parent
/// between two reports, so it is refused, naming the key. The other
/// backends write at the heartbeat, and take it.
#[test]
fn a_relay_pool_refuses_a_lease_its_reports_cannot_cover() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    let value: toml::Value =
        toml::from_str(&format!("{LEAST}heartbeat_ms = 50\nlease_ms = 299\n")).expect("toml");
    let problems = PoolSettings::read(value).expect_err("a lease under three reports");
    assert_eq!(problems.0.len(), 1, "{problems}");
    let problem = &problems.0[0];
    assert_eq!(
        (problem.table.as_deref(), problem.key.as_deref()),
        (Some("pool"), Some("lease_ms")),
        "{problem}"
    );
    assert!(problem.message.contains("300"), "{problem}");
    let least = settings(&format!("{LEAST}heartbeat_ms = 50\nlease_ms = 300\n"));
    assert_eq!(least.timing.lease, HubTiming::MIN_RELAY_LEASE);
    for backend in [
        "directory = 'memory'\n",
        "directory = 'dir'\ndir = '/srv/pools'\n",
    ] {
        let pool = settings(&format!(
            "{LEAST}{backend}heartbeat_ms = 50\nlease_ms = 200\n"
        ));
        assert_eq!(
            pool.timing.lease,
            std::time::Duration::from_millis(200),
            "{backend}"
        );
    }
}

/// A pool whose instances report no properties of a kind declares the
/// ones of that kind it serves: each a key of the kind's type, compared
/// in its canonical form. A kind that is not a settings key, a key that is not
/// an object or that holds a float, and a key declared twice, also in
/// another encoding, are refused, each naming `properties`.
#[test]
fn a_pool_declares_the_properties_it_serves() {
    let base = "name = 'm'\nclass = 'model_x'\nregion = 'r'\n";
    let declared = settings(&format!(
        "{base}[properties]\ntest = [{{ name = 'b' }}, {{ partition = 'a', name = 'c' }}]\nother = [{{ name = 'a' }}]\n"
    ));
    assert_eq!(
        declared.properties,
        [
            PropertyKey::new("other", serde_json::json!({ "name": "a" })),
            PropertyKey::new("test", serde_json::json!({ "name": "b" })),
            PropertyKey::new("test", serde_json::json!({ "name": "c", "partition": "a" })),
        ]
    );
    let text = toml::to_string(&declared).expect("toml");
    assert_eq!(settings(&text), declared, "{text}");
    assert!(settings(base).properties.is_empty());
    let unsaid = toml::to_string(&settings(base)).expect("toml");
    assert!(!unsaid.contains("properties"), "{unsaid}");

    for (bad, why) in [
        (
            "properties = { Test = [{ name = 'x' }] }",
            "property kind name",
        ),
        ("properties = { test = ['x'] }", "must be an object"),
        (
            "properties = { test = [{ name = 'x', ratio = 0.5 }] }",
            "not an integer",
        ),
        (
            "properties = { test = [{ name = 'x', partition = 'p' }, { partition = 'p', name = 'x' }] }",
            "declared twice",
        ),
        ("models = [{ model = 'x' }]", "unknown field"),
    ] {
        let text = format!("{base}{bad}\n");
        let table: toml::Value = toml::from_str(&text).expect("toml");
        let refused = PoolSettings::read(table).expect_err(&text).to_string();
        let named = if why == "unknown field" {
            "models"
        } else {
            "properties"
        };
        assert!(refused.contains(named), "{text}: {refused}");
        assert!(refused.contains(why), "{text}: {refused}");
    }
}
