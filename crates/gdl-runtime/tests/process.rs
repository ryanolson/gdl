//! A segment process's settings: files, then the `GDL_` environment, read
//! as a generated binary reads them.

use figment::Jail;
use figment::providers::{Format, Toml};
use gdl_runtime::{DeploymentSpec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo};
use gdl_runtime::{Hosted, ProcessConfig};
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
