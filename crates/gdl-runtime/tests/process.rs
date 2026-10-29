//! A segment process's settings: files, then the `GDL_` environment, read
//! as a generated binary reads them.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use figment::Jail;
use figment::providers::{Format, Toml};
use gdl::ErrorKind;
use gdl_runtime::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use gdl_runtime::{DirectoryBackend, Hosted, PoolSettings, ProcessConfig};
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
            forward_types: vec![TypeInfo::new("Request", true, Default::default())],
            backward_types: vec![TypeInfo::new("Reply", true, Default::default())],
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

const POOL: &str =
    "[pool]\nname = \"global-us-east-1\"\nclass = \"global\"\nregion = \"us-east-1\"\n";

#[test]
fn a_pool_table_is_read_from_a_file() {
    assert!(load(&[BASE]).expect("no pool").pool().is_none());
    let config = load(&[
        BASE,
        POOL,
        "[pool]\nhome = \"g\"\n[pool.ready]\nfront = 3\n",
    ])
    .expect("a pool");
    assert_eq!(
        config.pool(),
        Some(&PoolSettings {
            models: Vec::new(),
            name: "global-us-east-1".into(),
            layout: None,
            class: "global".into(),
            region: "us-east-1".into(),
            home: Some("g".into()),
            directory: DirectoryBackend::DEFAULT,
            ready: BTreeMap::from([("front".into(), 3)]),
            requires: BTreeMap::new(),
            hub: None,
            summary: PoolSettings::DEFAULT_SUMMARY,
            timing: Default::default(),
        })
    );
    let pool = config.pool().expect("a pool");
    assert_eq!((pool.quorum("front"), pool.quorum("back")), (3, 1));
}

/// `GDL_POOL__*` reaches `[pool]` as any other setting does, joining the
/// file's table: a quorum by segment, a backend, and the load
/// summary's period.
#[test]
fn a_pool_table_is_read_from_the_environment() {
    Jail::expect_with(|jail| {
        jail.create_file(
            "config.toml",
            &format!("{BASE}{POOL}[pool.ready]\nfront = 3\n"),
        )?;
        jail.set_env("GDL_POOL__REGION", "us-west-2");
        jail.set_env("GDL_POOL__DIRECTORY", "memory");
        jail.set_env("GDL_POOL__READY__BACK", "4");
        jail.set_env("GDL_POOL__SUMMARY_MS", "1500");
        jail.set_env("GDL_POOL__LAYOUT", "regional");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        assert_eq!(
            config.pool(),
            Some(&PoolSettings {
                models: Vec::new(),
                name: "global-us-east-1".into(),
                layout: Some("regional".into()),
                class: "global".into(),
                region: "us-west-2".into(),
                home: None,
                directory: DirectoryBackend::Memory,
                ready: BTreeMap::from([("back".into(), 4), ("front".into(), 3)]),
                requires: BTreeMap::new(),
                hub: None,
                summary: std::time::Duration::from_millis(1500),
                timing: Default::default(),
            })
        );
        Ok(())
    });
    // The `dir` backend and its directory.
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}{POOL}"))?;
        jail.set_env("GDL_POOL__DIRECTORY", "dir");
        jail.set_env("GDL_POOL__DIR", "/srv/pools");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        assert_eq!(
            config.pool().map(|pool| &pool.directory),
            Some(&DirectoryBackend::Dir {
                path: "/srv/pools".into()
            })
        );
        Ok(())
    });
    // The whole table from the environment.
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", BASE)?;
        jail.set_env("GDL_POOL__NAME", "model-x-01-us-east-1");
        jail.set_env("GDL_POOL__CLASS", "model_x");
        jail.set_env("GDL_POOL__REGION", "us-east-1");
        jail.set_env("GDL_POOL__HOME", "global-us-east-1");
        jail.set_env("GDL_POOL__READY__BACK", "2");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        let pool = config.pool().expect("a pool");
        assert_eq!(pool.name, "model-x-01-us-east-1");
        assert_eq!(pool.home.as_deref(), Some("global-us-east-1"));
        assert_eq!(pool.directory, DirectoryBackend::DEFAULT);
        assert_eq!(pool.quorum("back"), 2);
        Ok(())
    });
}

/// A segment of a fleet's Compose bundle: its image's settings name no
/// hub and no pool, which every pool running the image differs in, and
/// its pool's variables give both. The settings need a hub or a roster,
/// so they load only with `GDL_HUB__HOST`.
#[test]
fn a_fleet_bundles_variables_give_a_segment_its_hub_and_pool() {
    const IMAGE: &str =
        "listen = \"0.0.0.0:9000\"\nrendezvous_ms = 60000\n\n[system]\nport = 9090\n";
    let pool = [
        ("GDL_POOL__CLASS", "model_x"),
        ("GDL_POOL__DIR", "/var/lib/gdl/pools"),
        ("GDL_POOL__DIRECTORY", "dir"),
        ("GDL_POOL__HOME", "global-us-east-1"),
        ("GDL_POOL__HUB", "model-x-02-us-east-1:1337"),
        ("GDL_POOL__NAME", "model-x-02-us-east-1"),
        ("GDL_POOL__READY__ENGINE", "2"),
        ("GDL_POOL__REGION", "us-east-1"),
        ("GDL_POOL__SUMMARY_MS", "10000"),
    ];
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", IMAGE)?;
        for (name, value) in pool {
            jail.set_env(name, value);
        }
        assert!(
            Config::load(["config.toml"]).is_err(),
            "neither a hub nor a roster"
        );
        jail.set_env("GDL_HUB__HOST", "model-x-02-us-east-1");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        assert_eq!(
            config.pool(),
            Some(&PoolSettings {
                models: Vec::new(),
                name: "model-x-02-us-east-1".into(),
                layout: None,
                class: "model_x".into(),
                region: "us-east-1".into(),
                home: Some("global-us-east-1".into()),
                directory: DirectoryBackend::Dir {
                    path: "/var/lib/gdl/pools".into()
                },
                ready: BTreeMap::from([("engine".into(), 2)]),
                requires: BTreeMap::new(),
                hub: Some("model-x-02-us-east-1:1337".into()),
                summary: PoolSettings::DEFAULT_SUMMARY,
                timing: Default::default(),
            })
        );
        Ok(())
    });
    // A name that would read as another value is quoted, and a quorum of
    // a segment whose name holds `__` is one variable holding the table.
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", IMAGE)?;
        jail.set_env("GDL_POOL__NAME", "\"true\"");
        jail.set_env("GDL_POOL__CLASS", "global");
        jail.set_env("GDL_POOL__REGION", "us-east-1");
        jail.set_env("GDL_POOL__READY", "{\"front__work\"=2}");
        jail.set_env("GDL_HUB__HOST", "true");
        assert!(
            Config::load(["config.toml"]).is_err(),
            "a host that reads as a boolean"
        );
        jail.set_env("GDL_HUB__HOST", "\"true\"");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        let pool = config.pool().expect("a pool");
        assert_eq!(pool.name, "true");
        assert_eq!(pool.ready, BTreeMap::from([("front__work".into(), 2)]));
        Ok(())
    });
}

#[test]
fn a_misspelled_pool_key_is_refused() {
    let Err(error) = load(&[BASE, POOL, "[pool]\nhone = \"g\"\n"]) else {
        panic!("a misspelled key under [pool]")
    };
    assert!(error.to_string().contains("hone"), "{error}");
    // Nothing reads seeds: the relay mesh among global hubs is not built.
    let Err(error) = load(&[BASE, POOL, "[pool]\nseeds = [\"hub-a:1337\"]\n"]) else {
        panic!("seeds under [pool]")
    };
    assert!(error.to_string().contains("seeds"), "{error}");
    let Err(error) = load(&[BASE, POOL, "[pool]\ndirectory = \"store\"\n"]) else {
        panic!("an unknown directory backend")
    };
    assert!(error.to_string().contains("store"), "{error}");
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}{POOL}"))?;
        jail.set_env("GDL_POOL__DIRECTORY", "static");
        let Err(error) = Config::load(["config.toml"]) else {
            return Err("a fleet file is not a directory backend".into());
        };
        assert!(error.to_string().contains("static"), "{error}");
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}{POOL}"))?;
        jail.set_env("GDL_POOL__DIRECTORY", "dir");
        let Err(error) = Config::load(["config.toml"]) else {
            return Err("the dir backend without its directory".into());
        };
        assert!(error.to_string().contains("needs `dir`"), "{error}");
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.create_file("config.toml", &format!("{BASE}{POOL}"))?;
        jail.set_env("GDL_POOL__HONE", "g");
        let Err(error) = Config::load(["config.toml"]) else {
            return Err("a misspelled variable under [pool]".into());
        };
        assert!(error.to_string().contains("hone"), "{error}");
        Ok(())
    });
}

/// A `[pool]` problem names where it was read from: the file and its key,
/// or the variable.
#[tokio::test]
async fn a_pool_problem_names_its_file_or_its_variable() {
    for (file, variable, expected) in [
        (
            "hone = \"g\"\n",
            None,
            "config.toml: [pool] hone: unknown field",
        ),
        (
            "",
            Some(("GDL_POOL__HONE", "g")),
            "GDL_POOL__HONE: unknown field",
        ),
        (
            "summary_ms = 500\n",
            None,
            "config.toml: [pool] summary_ms: is 500",
        ),
        (
            "",
            Some(("GDL_POOL__SUMMARY_MS", "500")),
            "GDL_POOL__SUMMARY_MS: is 500",
        ),
        (
            "",
            Some(("GDL_POOL__READY__FRONT", "many")),
            "GDL_POOL__READY__FRONT: invalid type",
        ),
        (
            "layout = \"Regional\"\n",
            None,
            "config.toml: [pool] layout: layout name `Regional` must be",
        ),
        (
            "",
            Some(("GDL_POOL__LAYOUT", "re-gional")),
            "GDL_POOL__LAYOUT: layout name `re-gional` must be",
        ),
    ] {
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", &format!("{BASE}{POOL}{file}"))?;
            if let Some((name, value)) = variable {
                jail.set_env(name, value);
            }
            let Err(error) = Config::load(["config.toml"]) else {
                return Err(format!("accepted {file} {variable:?}").into());
            };
            let error = error.to_string();
            assert!(error.contains(expected), "{expected}\nin: {error}");
            Ok(())
        });
    }
    // A table set whole in one variable is that variable's; a setting
    // missing from a table set by variables is the variable's to set.
    for (file, variables, expected) in [
        (
            POOL,
            &[("GDL_POOL__READY", r#"{back="many"}"#)][..],
            "GDL_POOL__READY: invalid type",
        ),
        (
            "",
            &[
                ("GDL_POOL__CLASS", "global"),
                ("GDL_POOL__REGION", "us-east-1"),
            ][..],
            "GDL_POOL__NAME: missing field `name`",
        ),
        (
            POOL,
            &[
                ("GDL_POOL__READY__BACK", "1"),
                ("GDL_POOL__READY", r#"{back="many"}"#),
            ][..],
            "GDL_POOL__READY: invalid type",
        ),
        (
            POOL,
            &[("GDL_POOL__SUMMARY_MS__EXTRA", "1")][..],
            "GDL_POOL__SUMMARY_MS__EXTRA: invalid type",
        ),
        // Set from a file and a variable: the file holds the table.
        (
            "[pool]\nclass = \"global\"\n",
            &[("GDL_POOL__REGION", "us-east-1")][..],
            "config.toml: [pool] name: missing field `name`",
        ),
    ] {
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", &format!("{BASE}{file}"))?;
            for (name, value) in variables {
                jail.set_env(name, value);
            }
            let Err(error) = Config::load(["config.toml"]) else {
                return Err(format!("accepted {variables:?}").into());
            };
            let error = error.to_string();
            assert!(error.contains(expected), "{expected}\nin: {error}");
            Ok(())
        });
    }
    // Found when the instance launches, against its deployment.
    for (file, variable, expected) in [
        (
            "[pool.ready]\nback = 1\n",
            None,
            "config.toml: [pool] ready.back: not a segment of class `global`",
        ),
        (
            "",
            Some(("GDL_POOL__READY__BACK", "1")),
            "GDL_POOL__READY__BACK: not a segment of class `global`",
        ),
        (
            "",
            Some(("GDL_POOL__CLASS", "model_y")),
            "GDL_POOL__CLASS: the placement has no class `model_y`",
        ),
    ] {
        let mut config = None;
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", &format!("{BASE}{POOL}{file}"))?;
            if let Some((name, value)) = variable {
                jail.set_env(name, value);
            }
            config = Some(Config::load(["config.toml"]).map_err(|e| e.to_string())?);
            Ok(())
        });
        let config = config.expect("parses");
        // The check refuses before the roster directory is written.
        let (launched, _) = launch_pool(&config, "front", &classed()).await;
        let Err(error) = launched else {
            panic!("launched with {file} {variable:?}")
        };
        let error = error.to_string();
        assert!(error.contains(expected), "{expected}\nin: {error}");
    }
}

/// The environment overrides the files as a whole, as one provider would:
/// a variable that a later one supersedes does not erase what a file set
/// beside the later one's key.
#[test]
fn the_environment_overrides_the_files_as_a_whole() {
    Jail::expect_with(|jail| {
        jail.create_file(
            "config.toml",
            &format!("{BASE}{POOL}ready = {{ front = 2, back = 3 }}\n"),
        )?;
        jail.set_env("GDL_POOL__READY", "[]");
        jail.set_env("GDL_POOL__READY__FRONT", "1");
        let config = Config::load(["config.toml"]).map_err(|e| e.to_string())?;
        let pool = config.pool().expect("a pool");
        assert_eq!((pool.quorum("front"), pool.quorum("back")), (1, 3));
        Ok(())
    });
}

/// Launch `config` as an instance of `segment` in `deployment`, recording
/// whether it went on to await its nodes' readiness, which comes after the
/// pool check. With a roster directory it comes before the rendezvous;
/// with a hub, after the registration, whose lease is kept while the nodes
/// load.
async fn launch_pool(
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

/// Each way a `[pool]` can misfit the deployment ends the launch with a
/// `Placement` error, before the instance waits for its nodes, writes to
/// the roster directory, or reaches for a hub.
#[tokio::test]
async fn a_pool_that_does_not_fit_the_deployment_stops_the_launch() {
    let cases = [
        (
            deployment(),
            "front",
            POOL.to_string(),
            "[pool] class: the placement has no classes, and so no class `global`",
        ),
        (
            classed(),
            "front",
            POOL.replace("\"global\"", "\"model_y\""),
            "[pool] class: the placement has no class `model_y`",
        ),
        (
            classed(),
            "front",
            POOL.replace("\"global\"", "\"model_x\""),
            "[pool] class: this instance's segment `front` is in class `global`, not `model_x`",
        ),
        (
            classed(),
            "front",
            format!("{POOL}[pool.ready]\nback = 1\n"),
            "[pool] ready.back: not a segment of class `global`",
        ),
    ];
    for (deployment, segment, pool, expected) in cases {
        let dir = tempfile::Builder::new()
            .prefix("gdl-pool-")
            .tempdir()
            .expect("roster directory");
        let roster = format!(
            "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 200\n",
            dir.path().display()
        );
        let hub = "listen = \"127.0.0.1:0\"\n[hub]\nhost = \"gdl-no-such-hub.invalid\"\n";
        for discovery in [roster.as_str(), hub] {
            let config = load(&[discovery, &pool]).expect("parses");
            let (launched, awaited) = launch_pool(&config, segment, &deployment).await;
            let Err(error) = launched else {
                panic!("launched with {pool}")
            };
            assert_eq!(error.kind(), ErrorKind::Placement, "{error}");
            assert_eq!(
                error.to_string(),
                format!("placement error: {expected}"),
                "{pool}"
            );
            assert!(!awaited, "awaited the nodes with {pool}");
        }
        let written: Vec<_> = std::fs::read_dir(dir.path())
            .expect("roster directory")
            .collect();
        assert!(written.is_empty(), "{written:?}");
    }
}

/// The control: a `[pool]` that fits passes the check, and the launch goes
/// on to wait for the nodes and meet the roster. In a pool it meets only
/// the segments of its own class, so it launches with no instance of the
/// other class; without the pool it waits for that instance and says so.
#[tokio::test]
async fn a_pool_that_fits_the_deployment_launches() {
    for pool in [true, false] {
        let dir = tempfile::Builder::new()
            .prefix("gdl-pool-")
            .tempdir()
            .expect("roster directory");
        let roster = format!(
            "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 200\n",
            dir.path().display()
        );
        let settings = format!("{POOL}[pool.ready]\nfront = 2\n");
        let layers: Vec<&str> = if pool {
            vec![&roster, &settings]
        } else {
            vec![&roster]
        };
        let config = load(&layers).expect("parses");
        let (launched, awaited) = launch_pool(&config, "front", &classed()).await;
        assert!(awaited, "the check stopped a pool that fits");
        match (pool, launched) {
            (true, Ok(_)) => {}
            (true, Err(error)) => panic!("a pool waited for another class: {error}"),
            (false, Ok(_)) => panic!("met a roster with no `back` instance"),
            (false, Err(error)) => {
                assert!(error.to_string().contains("back (0 of 1)"), "{error}");
            }
        }
    }
}
