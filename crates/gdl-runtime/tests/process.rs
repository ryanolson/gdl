//! A segment process's settings: files, then the `GDL_` environment, read
//! as a generated binary reads them.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use figment::Jail;
use figment::providers::{Format, Toml};
use gdl::ErrorKind;
use gdl_runtime::{CellSettings, DirectoryBackend, Hosted, ProcessConfig};
use gdl_runtime::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use serde::Deserialize;

/// What a segment hosting one node, `Truncate`, declares.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Hosts {
    #[serde(default)]
    node: Nodes,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Nodes {
    #[serde(rename = "Truncate", default)]
    truncate: Option<toml::Table>,
}

impl Hosted for Hosts {
    const KEYS: &'static [&'static str] = &["node"];
    const NAMES: &'static [&'static str] = &["Truncate"];
}

/// A deployment of one segment, `front`, holding a source and a sink.
fn deployment() -> DeploymentSpec {
    let graph = Manifest {
        name: "one".into(),
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
    DeploymentSpec::new(graph, Placement::new().segment("front", ["Source", "Echo"]))
        .expect("valid test fixture")
}

type Config = ProcessConfig<Hosts>;

const BASE: &str = "listen = \"127.0.0.1:0\"\nroster = \"roster\"\n";

/// The settings of `files` merged in order, without the environment, which
/// only `ProcessConfig::load` reads.
fn load(files: &[&str]) -> gdl::Result<Config> {
    let mut figment = figment::Figment::new();
    for file in files {
        figment = figment.merge(Toml::string(file));
    }
    Config::from_figment(figment)
}

#[test]
fn an_unknown_key_is_refused_at_either_level() {
    let Err(error) = load(&[BASE, "listn = \"x\"\n"]) else {
        panic!("a misspelled key")
    };
    assert!(error.to_string().contains("listn"), "{error}");
    let Err(error) = load(&[BASE, "[node.Other]\n"]) else {
        panic!("a node the segment does not host")
    };
    assert!(error.to_string().contains("Other"), "{error}");
}

#[test]
fn a_config_names_one_way_to_find_peers() {
    let Err(error) = load(&["listen = \"127.0.0.1:0\"\n"]) else {
        panic!("neither roster nor hub")
    };
    assert!(
        error.to_string().contains("roster") && error.to_string().contains("hub"),
        "{error}"
    );
    let Err(error) = load(&[BASE, "[hub]\nhost = \"hub\"\n"]) else {
        panic!("both roster and hub")
    };
    assert!(error.to_string().contains("both"), "{error}");
}

#[test]
fn files_merge_in_argument_order() {
    let config = load(&[
        BASE,
        "[node.Truncate]\nmax = 5\n",
        "listen = \"127.0.0.1:1\"\n[node.Truncate]\nmax = 7\n",
    ])
    .expect("two files");
    let truncate = config.hosted.node.truncate.expect("the node's table");
    assert_eq!(truncate["max"].as_integer(), Some(7));
}

/// The build scripts use the same prefix (`GDL_PROFILE`, `GDL_IMAGE_PREFIX`),
/// and a variable meant for them must not stop a segment from starting.
#[test]
fn a_build_variable_is_not_a_setting() {
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", BASE)?;
        jail.set_env("GDL_PROFILE", "release");
        jail.set_env("GDL_IMAGE_PREFIX", "registry.example/");
        Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        Ok(())
    });
}

/// Figment lowercases environment keys, but a node's table is named as the
/// graph spells it: the variable must still reach the table.
#[test]
fn an_environment_value_overrides_a_node_table_by_its_name() {
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}[node.Truncate]\nmax = 5\n"))?;
        jail.set_env("GDL_NODE__TRUNCATE__MAX", "3");
        jail.set_env("GDL_RUNTIME__ADMISSION_LIMIT", "64");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        let truncate = config.hosted.node.truncate.expect("the node's table");
        assert_eq!(truncate["max"].as_integer(), Some(3));
        assert_eq!(config.runtime.admission_limit, 64);
        Ok(())
    });
}

#[test]
fn a_misspelled_environment_setting_is_refused() {
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", BASE)?;
        jail.set_env("GDL_NODE__TRUNCATE__MAX", "3");
        jail.set_env("GDL_NODE__OTHER__MAX", "3");
        let Err(error) = Config::load(["config.toml"]) else {
            panic!("a node the segment does not host")
        };
        assert!(error.to_string().contains("other"), "{error}");
        Ok(())
    });
}

#[test]
fn a_missing_file_is_an_error() {
    let Err(error) = Config::load(["/nonexistent/gdl-config.toml"]) else {
        panic!("no such file")
    };
    assert!(error.to_string().contains("gdl-config.toml"), "{error}");
}

/// A hub's membership is whatever registers; an exact count would be a
/// promise this process cannot keep. A roster directory, where the count
/// is what makes every instance's roster the same, takes no minimum.
#[tokio::test]
async fn instances_apply_to_a_roster_directory_only() {
    let config = load(&[BASE, "[min_replicas]\nfront = 2\n"]).expect("parses");
    let Err(error) = config
        .launch(
            &gdl_runtime::Process::start(&config).await.expect("process"),
            "front",
            &["front"],
            &deployment(),
            async { Ok(()) },
        )
        .await
    else {
        panic!("min_replicas under a roster directory")
    };
    assert!(error.to_string().contains("[min_replicas]"), "{error}");
    let config = load(&[
        "listen = \"127.0.0.1:0\"\n[hub]\nhost = \"hub\"\n",
        "[instances]\nfront = 2\n",
    ])
    .expect("parses");
    let Err(error) = config
        .launch(
            &gdl_runtime::Process::start(&config).await.expect("process"),
            "front",
            &["front"],
            &deployment(),
            async { Ok(()) },
        )
        .await
    else {
        panic!("instances under a hub")
    };
    assert!(error.to_string().contains("[instances]"), "{error}");
}

#[tokio::test]
async fn instances_name_only_known_segments() {
    let config = load(&[BASE, "[instances]\nnope = 2\n"]).expect("parses");
    let Err(error) = config
        .launch(
            &gdl_runtime::Process::start(&config).await.expect("process"),
            "front",
            &["front"],
            &deployment(),
            async { Ok(()) },
        )
        .await
    else {
        panic!("an unknown segment")
    };
    assert!(error.to_string().contains("`nope`"), "{error}");
}

/// A count other than the default reaches the rendezvous: one instance of
/// a segment expected twice waits, and says so.
#[tokio::test]
async fn a_count_other_than_one_is_applied() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-count-")
        .tempdir()
        .expect("roster directory");
    let config = load(&[&format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 200\n[instances]\nfront = 2\n",
        dir.path().display()
    )])
    .expect("parses");
    let Err(error) = config
        .launch(
            &gdl_runtime::Process::start(&config).await.expect("process"),
            "front",
            &["front"],
            &deployment(),
            async { Ok(()) },
        )
        .await
    else {
        panic!("one instance met a count of two")
    };
    assert!(error.to_string().contains("front (1 of 2)"), "{error}");
}

/// A count set in the environment overrides the file's: both name the
/// segment the same way, because a segment name is lowercase.
#[test]
fn an_environment_count_overrides_the_file() {
    Jail::expect_with(|jail| {
        jail.create_file(
            "config.toml",
            "listen = \"127.0.0.1:0\"\nroster = \"roster\"\nrendezvous_ms = 200\n[instances]\nfront = 1\n",
        )?;
        jail.set_env("GDL_INSTANCES__FRONT", "2");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        let Err(error) = runtime.block_on(async {
            let process = gdl_runtime::Process::start(&config).await.expect("process");
            config
                .launch(&process, "front", &["front"], &deployment(), async {
                    Ok(())
                })
                .await
        }) else {
            return Err("the file's count of one won".into());
        };
        assert!(error.to_string().contains("front (1 of 2)"), "{error}");
        Ok(())
    });
}

/// A value set in the environment joins the hub table rather than
/// replacing it, because the table is the only form.
#[test]
fn an_environment_value_joins_the_hub_table() {
    Jail::expect_with(|jail| {
        jail.create_file(
            "config.toml",
            "listen = \"127.0.0.1:0\"\n[hub]\nhost = \"hub\"\n",
        )?;
        jail.set_env("GDL_HUB__HEARTBEAT_MS", "200");
        Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        Ok(())
    });
}

#[test]
fn a_zero_heartbeat_is_refused() {
    let Err(error) = load(&["listen = \"127.0.0.1:0\"\n[hub]\nhost = \"hub\"\nheartbeat_ms = 0\n"])
    else {
        panic!("a lease without a cadence")
    };
    assert!(error.to_string().contains("heartbeat_ms"), "{error}");
}

/// A deployment of two segments in two classes: `front`, in `global`,
/// sends to `back`, in `model_x`, across the one class boundary.
fn classed() -> DeploymentSpec {
    let mut graph = deployment().graph;
    graph.name = "classed".into();
    DeploymentSpec::new(
        graph,
        Placement::new()
            .segment("front", ["Source"])
            .segment("back", ["Echo"])
            .class("global", ["front"])
            .class("model_x", ["back"]),
    )
    .expect("valid test fixture")
}

const CELL: &str =
    "[cell]\nname = \"global-us-east-1\"\nclass = \"global\"\nregion = \"us-east-1\"\n";

#[test]
fn a_cell_table_is_read_from_a_file() {
    assert!(load(&[BASE]).expect("no cell").cell().is_none());
    let config = load(&[
        BASE,
        CELL,
        "[cell]\nhome = \"g\"\nseeds = [\"hub-a:1337\"]\n[cell.ready]\nfront = 3\n",
    ])
    .expect("a cell");
    assert_eq!(
        config.cell(),
        Some(&CellSettings {
            name: "global-us-east-1".into(),
            class: "global".into(),
            region: "us-east-1".into(),
            home: Some("g".into()),
            directory: DirectoryBackend::Relay,
            seeds: vec!["hub-a:1337".into()],
            ready: BTreeMap::from([("front".into(), 3)]),
            requires: BTreeMap::new(),
            hub: None,
        })
    );
    let cell = config.cell().expect("a cell");
    assert_eq!((cell.quorum("front"), cell.quorum("back")), (3, 1));
}

/// `GDL_CELL__*` reaches `[cell]` as any other setting does, joining the
/// file's table: a quorum by segment, and a list of seeds.
#[test]
fn a_cell_table_is_read_from_the_environment() {
    Jail::expect_with(|jail| {
        jail.create_file(
            "config.toml",
            &format!("{BASE}{CELL}[cell.ready]\nfront = 3\n"),
        )?;
        jail.set_env("GDL_CELL__REGION", "us-west-2");
        jail.set_env("GDL_CELL__DIRECTORY", "memory");
        jail.set_env("GDL_CELL__READY__BACK", "4");
        jail.set_env("GDL_CELL__SEEDS", r#"["hub-a:1337", "hub-b:1337"]"#);
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        assert_eq!(
            config.cell(),
            Some(&CellSettings {
                name: "global-us-east-1".into(),
                class: "global".into(),
                region: "us-west-2".into(),
                home: None,
                directory: DirectoryBackend::Memory,
                seeds: vec!["hub-a:1337".into(), "hub-b:1337".into()],
                ready: BTreeMap::from([("back".into(), 4), ("front".into(), 3)]),
                requires: BTreeMap::new(),
                hub: None,
            })
        );
        Ok(())
    });
    // The `dir` backend and its directory.
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}{CELL}"))?;
        jail.set_env("GDL_CELL__DIRECTORY", "dir");
        jail.set_env("GDL_CELL__DIR", "/srv/cells");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        assert_eq!(
            config.cell().map(|cell| &cell.directory),
            Some(&DirectoryBackend::Dir {
                path: "/srv/cells".into()
            })
        );
        Ok(())
    });
    // The whole table from the environment.
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", BASE)?;
        jail.set_env("GDL_CELL__NAME", "model-x-01-us-east-1");
        jail.set_env("GDL_CELL__CLASS", "model_x");
        jail.set_env("GDL_CELL__REGION", "us-east-1");
        jail.set_env("GDL_CELL__HOME", "global-us-east-1");
        jail.set_env("GDL_CELL__READY__BACK", "2");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        let cell = config.cell().expect("a cell");
        assert_eq!(cell.name, "model-x-01-us-east-1");
        assert_eq!(cell.home.as_deref(), Some("global-us-east-1"));
        assert_eq!(cell.directory, DirectoryBackend::Relay);
        assert_eq!(cell.quorum("back"), 2);
        Ok(())
    });
}

#[test]
fn a_misspelled_cell_key_is_refused() {
    let Err(error) = load(&[BASE, CELL, "[cell]\nhone = \"g\"\n"]) else {
        panic!("a misspelled key under [cell]")
    };
    assert!(error.to_string().contains("hone"), "{error}");
    let Err(error) = load(&[BASE, CELL, "[cell]\ndirectory = \"store\"\n"]) else {
        panic!("an unknown directory backend")
    };
    assert!(error.to_string().contains("store"), "{error}");
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}{CELL}"))?;
        jail.set_env("GDL_CELL__DIRECTORY", "static");
        let Err(error) = Config::load(["config.toml"]) else {
            return Err("a fleet file is not a directory backend".into());
        };
        assert!(error.to_string().contains("static"), "{error}");
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}{CELL}"))?;
        jail.set_env("GDL_CELL__DIRECTORY", "dir");
        let Err(error) = Config::load(["config.toml"]) else {
            return Err("the dir backend without its directory".into());
        };
        assert!(error.to_string().contains("needs `dir`"), "{error}");
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}{CELL}"))?;
        jail.set_env("GDL_CELL__HONE", "g");
        let Err(error) = Config::load(["config.toml"]) else {
            return Err("a misspelled variable under [cell]".into());
        };
        assert!(error.to_string().contains("hone"), "{error}");
        Ok(())
    });
}

/// Launch `config` as an instance of `segment` in `deployment`, recording
/// whether it went on to await its nodes' readiness, which comes after the
/// cell check and before every registration and rendezvous.
async fn launch_cell(
    config: &Config,
    segment: &'static str,
    deployment: &DeploymentSpec,
) -> (gdl::Result<gdl_runtime::Launch>, bool) {
    let segments: &'static [&'static str] = if deployment.placement.classes().next().is_some() {
        &["back", "front"]
    } else {
        &["front"]
    };
    let awaited = AtomicBool::new(false);
    let process = gdl_runtime::Process::start(config).await.expect("process");
    let launched = config
        .launch(&process, segment, segments, deployment, async {
            awaited.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;
    (launched, awaited.load(Ordering::SeqCst))
}

/// Each way a `[cell]` can misfit the deployment ends the launch with a
/// `Placement` error, before the instance waits for its nodes, writes to
/// the roster directory, or reaches for a hub.
#[tokio::test]
async fn a_cell_that_does_not_fit_the_deployment_stops_the_launch() {
    let cases = [
        (
            deployment(),
            "front",
            CELL.to_string(),
            "[cell] class = \"global\", but the placement has no classes",
        ),
        (
            classed(),
            "front",
            CELL.replace("\"global\"", "\"model_y\""),
            "[cell] class: the placement has no class `model_y`",
        ),
        (
            classed(),
            "front",
            CELL.replace("\"global\"", "\"model_x\""),
            "[cell] class: this instance's segment `front` is in class `global`, not `model_x`",
        ),
        (
            classed(),
            "front",
            format!("{CELL}[cell.ready]\nback = 1\n"),
            "[cell] ready: `back` is not a segment of class `global`",
        ),
    ];
    for (deployment, segment, cell, expected) in cases {
        let dir = tempfile::Builder::new()
            .prefix("gdl-cell-")
            .tempdir()
            .expect("roster directory");
        let roster = format!(
            "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 200\n",
            dir.path().display()
        );
        let hub = "listen = \"127.0.0.1:0\"\n[hub]\nhost = \"gdl-no-such-hub.invalid\"\n";
        for discovery in [roster.as_str(), hub] {
            let config = load(&[discovery, &cell]).expect("parses");
            let (launched, awaited) = launch_cell(&config, segment, &deployment).await;
            let Err(error) = launched else {
                panic!("launched with {cell}")
            };
            assert_eq!(error.kind(), ErrorKind::Placement, "{error}");
            assert_eq!(
                error.to_string(),
                format!("placement error: {expected}"),
                "{cell}"
            );
            assert!(!awaited, "awaited the nodes with {cell}");
        }
        let written: Vec<_> = std::fs::read_dir(dir.path())
            .expect("roster directory")
            .collect();
        assert!(written.is_empty(), "{written:?}");
    }
}

/// The control: a `[cell]` that fits passes the check, and the launch goes
/// on to wait for the nodes and meet the roster. In a cell it meets only
/// the segments of its own class, so it launches with no instance of the
/// other class; without the cell it waits for that instance and says so.
#[tokio::test]
async fn a_cell_that_fits_the_deployment_launches() {
    for cell in [true, false] {
        let dir = tempfile::Builder::new()
            .prefix("gdl-cell-")
            .tempdir()
            .expect("roster directory");
        let roster = format!(
            "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 200\n",
            dir.path().display()
        );
        let settings = format!("{CELL}[cell.ready]\nfront = 2\n");
        let layers: Vec<&str> = if cell {
            vec![&roster, &settings]
        } else {
            vec![&roster]
        };
        let config = load(&layers).expect("parses");
        let (launched, awaited) = launch_cell(&config, "front", &classed()).await;
        assert!(awaited, "the check stopped a cell that fits");
        match (cell, launched) {
            (true, Ok(_)) => {}
            (true, Err(error)) => panic!("a cell waited for another class: {error}"),
            (false, Ok(_)) => panic!("met a roster with no `back` instance"),
            (false, Err(error)) => {
                assert!(error.to_string().contains("back (0 of 1)"), "{error}");
            }
        }
    }
}
