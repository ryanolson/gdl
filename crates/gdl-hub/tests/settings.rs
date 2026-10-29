//! The hub binary's settings: a pool's settings from `GDL_POOL__*`, and
//! the hub they make. Every hub here runs in process on OS-assigned ports.

use std::collections::BTreeMap;
use std::time::Duration;

use figment::Jail;
use futures::StreamExt;
use gdl_hub::{PoolTiming, hub_builder, pool_from_env, pool_from_vars};
use gdl_model::{
    DeploymentSpec, DirectoryBackend, HopInfo, HubTiming, Manifest, NodeInfo, Placement,
    PoolSettings, Role, TypeInfo, WatchCaps,
};
use gdl_pools::{DirDirectory, PoolDirectory, PoolEvent, PoolState};
use velo_hub::{Hub, HubConfig};

/// `S => D` in two classes: `global` runs `a`, and `model_x` runs `d`.
fn spec() -> DeploymentSpec {
    let graph = Manifest {
        name: "pools".into(),
        schema: 1,
        nodes: vec![
            NodeInfo {
                name: "S".into(),
                role: Role::Source,
                binding: None,
            },
            NodeInfo {
                name: "D".into(),
                role: Role::Sink,
                binding: Some("d".into()),
            },
        ],
        hops: vec![HopInfo {
            outer: "S".into(),
            forward: vec![],
            inner: "D".into(),
            backward: vec![],
            forward_types: vec![TypeInfo::new("T", true, Default::default())],
            backward_types: vec![TypeInfo::new("T", true, Default::default())],
        }],
    };
    let placement = Placement::new()
        .segment("a", ["S"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["d"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

fn config() -> HubConfig {
    let mut config = HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    config
}

/// The variables of a model pool's hub, which publishes to `dir`.
fn model_pool(jail: &mut Jail, dir: &str) {
    jail.set_env("GDL_POOL__NAME", "model-x-01-us-east-1");
    jail.set_env("GDL_POOL__CLASS", "model_x");
    jail.set_env("GDL_POOL__REGION", "us-east-1");
    jail.set_env("GDL_POOL__HUB", "model-x-01-hub:1337");
    jail.set_env("GDL_POOL__DIRECTORY", "dir");
    jail.set_env("GDL_POOL__DIR", dir);
}

#[test]
fn a_hub_without_pool_variables_has_no_pool() {
    Jail::expect_with(|jail| {
        // The build scripts' variables are not a pool's.
        jail.set_env("GDL_PROFILE", "release");
        jail.set_env("GDL_RUNTIME__ADMISSION_LIMIT", "4");
        assert_eq!(pool_from_env(&spec()).map_err(|e| e.to_string())?, None);
        Ok(())
    });
}

/// Keys are matched without regard to case, and `__` nests them.
#[test]
fn a_pools_settings_are_read_from_the_environment() {
    Jail::expect_with(|jail| {
        model_pool(jail, "/srv/pools");
        jail.set_env("GDL_POOL__READY__D", "2");
        jail.set_env("GDL_POOL__SUMMARY_MS", "2500");
        assert_eq!(
            pool_from_env(&spec()).map_err(|e| e.to_string())?,
            Some(PoolSettings {
                properties: Vec::new(),
                name: "model-x-01-us-east-1".into(),
                layout: None,
                class: "model_x".into(),
                region: "us-east-1".into(),
                home: None,
                directory: DirectoryBackend::Dir {
                    path: "/srv/pools".into()
                },
                ready: BTreeMap::from([("d".into(), 2)]),
                requires: BTreeMap::new(),
                hub: Some("model-x-01-hub:1337".into()),
                summary: Duration::from_millis(2500),
                timing: Default::default(),
            })
        );
        Ok(())
    });
}

/// The hub's pace and its watch caps are `GDL_POOL__*` variables too, and
/// the timing the hub runs with is theirs, each one left out at its
/// default.
#[test]
fn a_pools_hub_timing_is_read_from_the_environment() {
    Jail::expect_with(|jail| {
        model_pool(jail, "/srv/pools");
        jail.set_env("GDL_POOL__HOLD_MS", "500");
        jail.set_env("GDL_POOL__HEARTBEAT_MS", "2000");
        jail.set_env("GDL_POOL__LEASE_MS", "9000");
        jail.set_env("GDL_POOL__WATCH_BUFFER", "64");
        jail.set_env("GDL_POOL__LOST_CHOOSER_MS", "4000");
        jail.set_env("GDL_POOL__WATCH_CAPS__POOL", "3");
        jail.set_env("GDL_POOL__WATCH_CAPS__RELAY", "4");
        jail.set_env("GDL_POOL__WATCH_CAPS__FOLLOW", "5");
        jail.set_env("GDL_POOL__KEYED_CAPACITY", "10");
        let pool = pool_from_env(&spec())
            .map_err(|e| e.to_string())?
            .ok_or("a pool")?;
        assert_eq!(pool.timing.keyed_capacity, 10);
        let timing = PoolTiming::from_settings(&pool.timing);
        assert_eq!(
            timing,
            PoolTiming {
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
                ..PoolTiming::default()
            }
        );
        Ok(())
    });
}

/// With no timing variable set, the hub runs at the pace it ran at when
/// the pace was fixed in the binary.
#[test]
fn with_no_timing_variable_a_pools_hub_keeps_its_pace() {
    Jail::expect_with(|jail| {
        model_pool(jail, "/srv/pools");
        let pool = pool_from_env(&spec())
            .map_err(|e| e.to_string())?
            .ok_or("a pool")?;
        assert_eq!(pool.timing, HubTiming::default());
        let timing = PoolTiming::from_settings(&pool.timing);
        assert_eq!(timing.hold, Duration::from_secs(2));
        assert_eq!(timing.heartbeat, Duration::from_secs(10));
        assert_eq!(timing.lease, Duration::from_secs(30));
        assert_eq!(timing.watch_buffer, 1024);
        assert_eq!(timing.lost_chooser, Duration::from_secs(30));
        assert_eq!(
            timing.watch_caps,
            WatchCaps {
                pool: 2048,
                relay: 2048,
                follow: 16384,
            }
        );
        assert_eq!(timing.refollow, Duration::from_millis(250));
        assert_eq!(timing, PoolTiming::default());
        Ok(())
    });
}

/// Variables given to the reader are read as the same variables in the
/// environment are, so that a generator can check what it writes for a
/// hub without setting them in its own process.
#[test]
fn given_variables_are_read_as_the_environment_is() {
    let given = [
        ("GDL_POOL__NAME", "model-x-01-us-east-1"),
        ("gdl_pool__class", "model_x"),
        ("GDL_POOL__REGION", "us-east-1"),
        ("GDL_POOL__HUB", "model-x-01-hub:1337"),
        ("GDL_POOL__DIRECTORY", "dir"),
        ("GDL_POOL__DIR", "/srv/pools"),
        ("GDL_POOL__READY__D", "2"),
        ("GDL_PROFILE", "release"),
    ];
    Jail::expect_with(|jail| {
        for (name, value) in given {
            jail.set_env(name, value);
        }
        let read = pool_from_vars(&spec(), given.map(|(n, v)| (n.into(), v.into())))
            .map_err(|e| e.to_string())?;
        assert!(read.is_some());
        assert_eq!(read, pool_from_env(&spec()).map_err(|e| e.to_string())?);
        Ok(())
    });
    let misspelled = [("GDL_POOL__HONE", "x")].map(|(n, v)| (n.into(), v.into()));
    let refused = pool_from_vars(&spec(), misspelled).expect_err("a misspelled key");
    assert!(refused.to_string().contains("GDL_POOL__HONE"), "{refused}");
}

/// An IPv6 hub address starts with `[`, as a list would: it is read as
/// the address it is.
#[test]
fn an_ipv6_hub_address_is_read_as_written() {
    Jail::expect_with(|jail| {
        model_pool(jail, "/srv/pools");
        jail.set_env("GDL_POOL__HUB", "[::1]:1337");
        let pool = pool_from_env(&spec()).map_err(|e| e.to_string())?;
        assert_eq!(
            pool.and_then(|pool| pool.hub).as_deref(),
            Some("[::1]:1337")
        );
        Ok(())
    });
}

/// A requirement is `GDL_POOL__REQUIRES__<CLASS>`, its class matched
/// without regard to case, as a quorum's segment is.
#[test]
fn a_pools_requirements_are_read_from_the_environment() {
    Jail::expect_with(|jail| {
        jail.set_env("GDL_POOL__NAME", "global-us-east-1");
        jail.set_env("GDL_POOL__CLASS", "global");
        jail.set_env("GDL_POOL__REGION", "us-east-1");
        jail.set_env("GDL_POOL__STATE_DIR", "/var/lib/gdl-hub");
        jail.set_env("GDL_POOL__REQUIRES__MODEL_X", "2");
        let pool = pool_from_env(&spec()).map_err(|e| e.to_string())?;
        assert_eq!(
            pool.map(|pool| pool.requires),
            Some(BTreeMap::from([("model_x".into(), 2)]))
        );
        Ok(())
    });
}

/// A variable that names no setting, or whose value does not fit its
/// setting or the deployment, is refused, and the refusal names it.
#[test]
fn a_misspelled_or_malformed_pool_variable_is_refused_by_name() {
    for (variable, value, expected) in [
        (
            "GDL_POOL__HONE",
            "global-us-east-1",
            "GDL_POOL__HONE: unknown field",
        ),
        (
            "GDL_POOL__HUB",
            "http://hub:1337",
            "GDL_POOL__HUB: \"http://hub:1337\" must be `host:port`",
        ),
        (
            "GDL_POOL__DIRECTORY",
            "store",
            "GDL_POOL__DIRECTORY: unknown variant",
        ),
        (
            "GDL_POOL__SUMMARY_MS",
            "999",
            "GDL_POOL__SUMMARY_MS: is 999; the period is at least 1000 ms",
        ),
        (
            "GDL_POOL__SUMMARY_MS",
            "soon",
            "GDL_POOL__SUMMARY_MS: invalid type",
        ),
        // Nothing reads seeds: the relay mesh among global hubs is not built.
        (
            "GDL_POOL__SEEDS",
            r#"["hub-a:1337"]"#,
            "GDL_POOL__SEEDS: unknown field",
        ),
        (
            "GDL_POOL__READY__ZZ",
            "2",
            "GDL_POOL__READY__ZZ: not a segment of class `model_x`",
        ),
        (
            "GDL_POOL__REQUIRES__GLOBAL",
            "1",
            "GDL_POOL__REQUIRES__GLOBAL: not a class that `model_x` sends to",
        ),
        (
            "GDL_POOL__CLASS",
            "model_y",
            "GDL_POOL__CLASS: the placement has no class `model_y`",
        ),
        // Against the lease's default, 30 s.
        (
            "GDL_POOL__HEARTBEAT_MS",
            "30000",
            "GDL_POOL__HEARTBEAT_MS: is 30000; it must be shorter than the lease",
        ),
        (
            "GDL_POOL__LEASE_MS",
            "10000",
            "GDL_POOL__LEASE_MS: is 10000; it must be longer than the heartbeat",
        ),
        (
            "GDL_POOL__KEYED_CAPACITY",
            "0",
            "GDL_POOL__KEYED_CAPACITY: is 0, which holds no record",
        ),
        (
            "GDL_POOL__WATCH_BUFFER",
            "0",
            "GDL_POOL__WATCH_BUFFER: is 0",
        ),
        (
            "GDL_POOL__WATCH_CAPS__RELAY",
            "0",
            "GDL_POOL__WATCH_CAPS__RELAY: is 0",
        ),
        (
            "GDL_POOL__WATCH_CAPS__RELAYS",
            "1",
            "GDL_POOL__WATCH_CAPS__RELAYS: unknown field",
        ),
    ] {
        Jail::expect_with(|jail| {
            model_pool(jail, "/srv/pools");
            jail.set_env(variable, value);
            let Err(error) = pool_from_env(&spec()) else {
                return Err(format!("{variable}={value} was accepted").into());
            };
            let error = error.to_string();
            assert!(error.starts_with(expected), "{expected}\nin: {error}");
            Ok(())
        });
    }
    // The variable that supplied the value is named: the later of two
    // that set one key, and one that set a table under a key.
    for (variables, expected) in [
        (
            &[
                ("GDL_POOL__READY__D", "1"),
                ("GDL_POOL__READY", r#"{d="many"}"#),
            ][..],
            "GDL_POOL__READY: invalid type",
        ),
        (
            &[("GDL_POOL__SUMMARY_MS__EXTRA", "1")][..],
            "GDL_POOL__SUMMARY_MS__EXTRA: invalid type",
        ),
    ] {
        Jail::expect_with(|jail| {
            model_pool(jail, "/srv/pools");
            for (name, value) in variables {
                jail.set_env(name, value);
            }
            let Err(error) = pool_from_env(&spec()) else {
                return Err(format!("accepted {variables:?}").into());
            };
            let error = error.to_string();
            assert!(error.starts_with(expected), "{expected}\nin: {error}");
            Ok(())
        });
    }
    // A table set whole in one variable is that variable's.
    Jail::expect_with(|jail| {
        model_pool(jail, "/srv/pools");
        jail.set_env("GDL_POOL__READY", r#"{d="many"}"#);
        let Err(error) = pool_from_env(&spec()) else {
            return Err("a quorum of many was accepted".into());
        };
        let error = error.to_string();
        assert!(
            error.starts_with("GDL_POOL__READY: invalid type"),
            "{error}"
        );
        Ok(())
    });
    Jail::expect_with(|jail| {
        jail.set_env("GDL_POOL__CLASS", "model_x");
        jail.set_env("GDL_POOL__REGION", "us-east-1");
        let Err(error) = pool_from_env(&spec()) else {
            return Err("a pool without a name was accepted".into());
        };
        let error = error.to_string();
        assert!(
            error.starts_with("GDL_POOL__NAME: missing field `name`"),
            "{error}"
        );
        Ok(())
    });
}

/// `GET` on a hub's public listener: the status.
async fn status(hub: &Hub, path: &str) -> u16 {
    let url = format!("http://127.0.0.1:{}{path}", hub.public_addr().port());
    tokio::time::timeout(Duration::from_secs(5), reqwest::get(&url))
        .await
        .expect("the hub answers in time")
        .expect("the hub answers")
        .status()
        .as_u16()
}

fn settings(directory: DirectoryBackend) -> PoolSettings {
    PoolSettings {
        properties: Vec::new(),
        name: "model-x-01-us-east-1".into(),
        layout: None,
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: None,
        directory,
        ready: BTreeMap::new(),
        requires: BTreeMap::new(),
        hub: Some("model-x-01-hub:1337".into()),
        summary: PoolSettings::DEFAULT_SUMMARY,
        timing: Default::default(),
    }
}

/// A lease that expires between heartbeats must be refused for settings built in Rust too.
#[test]
fn hub_builder_refuses_a_typed_lease_no_longer_than_its_heartbeat() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-timing-")
        .tempdir()
        .expect("a directory");
    let mut pool = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    assert!(hub_builder(config(), spec(), Some(pool.clone())).is_ok());
    pool.timing.lease = pool.timing.heartbeat;
    let result = hub_builder(config(), spec(), Some(pool));
    assert!(
        result.is_err(),
        "hub_builder accepted lease_ms = heartbeat_ms = 10000"
    );
    let error = result.err().expect("the invalid lease is refused");
    assert!(error.to_string().contains("lease_ms"), "{error}");
}

/// A bound of zero on a lost chooser, built in code, is refused as the
/// reader refuses it, naming its key: the hub would stop waiting for a
/// chooser the moment its watch of the relay closed.
#[test]
fn hub_builder_refuses_a_typed_lost_chooser_bound_of_zero() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-timing-")
        .tempdir()
        .expect("a directory");
    let mut pool = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    pool.timing.lost_chooser = Duration::ZERO;
    let error = hub_builder(config(), spec(), Some(pool))
        .err()
        .expect("a bound of zero is refused");
    assert!(error.to_string().contains("lost_chooser_ms"), "{error}");
}

/// A watch buffer past its ceiling, built in code, is refused as the
/// reader refuses it, naming its key, before the hub takes its slots.
#[test]
fn hub_builder_refuses_a_typed_watch_buffer_past_its_ceiling() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-timing-")
        .tempdir()
        .expect("a directory");
    let mut pool = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    pool.timing.watch_buffer = HubTiming::MAX_WATCH_BUFFER + 1;
    let Err(error) = hub_builder(config(), spec(), Some(pool)) else {
        panic!("hub_builder accepted a watch buffer past its ceiling");
    };
    assert!(error.to_string().contains("watch_buffer"), "{error}");
}

/// A keyed record capacity of zero, built in code, is refused as the
/// reader refuses it, naming its key.
#[test]
fn hub_builder_refuses_a_typed_keyed_capacity_of_zero() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-timing-")
        .tempdir()
        .expect("a directory");
    let mut pool = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    pool.timing.keyed_capacity = 0;
    let Err(error) = hub_builder(config(), spec(), Some(pool)) else {
        panic!("hub_builder accepted a keyed capacity of zero");
    };
    assert!(error.to_string().contains("keyed_capacity"), "{error}");
}

/// With `dir`, the hub runs the pool's extension, which publishes the
/// pool's record into the directory's files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_with_the_dir_backend_publishes_into_its_directory() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-pools-")
        .tempdir()
        .expect("a directory");
    let pool = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    let hub = hub_builder(config(), spec(), Some(pool))
        .expect("a dir pool")
        .serve()
        .await
        .expect("the hub serves");
    assert_eq!(status(&hub, "/v1/ext/gdl-pool/health").await, 503);
    // Read as another hub on the host reads it.
    let mut watch = DirDirectory::new(dir.path()).watch("model_x", None);
    let published = async {
        loop {
            match watch.next().await {
                Some(Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. })) => {
                    return record;
                }
                Some(Ok(_)) => {}
                other => panic!("the watch ended: {other:?}"),
            }
        }
    };
    let record = tokio::time::timeout(Duration::from_secs(5), published)
        .await
        .expect("the record is published in time");
    assert_eq!(record.name, "model-x-01-us-east-1");
    assert_eq!(record.state, PoolState::Unavailable);
    hub.shutdown().await.expect("the hub stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hub_without_a_pool_runs_admission_alone() {
    let hub = hub_builder(config(), spec(), None)
        .expect("no pool")
        .serve()
        .await
        .expect("the hub serves");
    assert_eq!(status(&hub, "/v1/ext/gdl-pool/health").await, 404);
    assert_eq!(status(&hub, "/v1/ext/gdl/members").await, 200);
    hub.shutdown().await.expect("the hub stops");
}

/// The hub the binary builds runs at its settings' pace: with a cap of one
/// watch of the pool, a second watch while the first is open is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_hub_binary_serves_the_watch_caps_its_settings_give() {
    let dir = tempfile::Builder::new()
        .prefix("gdl-hub-caps-")
        .tempdir()
        .expect("a directory");
    let mut pool = settings(DirectoryBackend::Dir {
        path: dir.path().to_path_buf(),
    });
    pool.timing.watch_caps.pool = 1;
    let hub = hub_builder(config(), spec(), Some(pool))
        .expect("a dir pool")
        .serve()
        .await
        .expect("the hub serves");
    let url = format!(
        "http://127.0.0.1:{}/v1/ext/gdl-pool/watch",
        hub.public_addr().port()
    );
    let held = tokio::time::timeout(Duration::from_secs(5), reqwest::get(&url))
        .await
        .expect("the hub answers in time")
        .expect("the hub answers");
    assert_eq!(held.status().as_u16(), 200, "the one watch is served");
    assert_eq!(
        status(&hub, "/v1/ext/gdl-pool/watch").await,
        503,
        "a second watch is refused"
    );
    drop(held);
    hub.shutdown().await.expect("the hub stops");
}

/// `memory` is one directory inside one process: a hub binary refuses it
/// before it serves, and says what to set.
#[test]
fn the_memory_backend_is_refused() {
    let Err(error) = hub_builder(config(), spec(), Some(settings(DirectoryBackend::Memory))) else {
        panic!("accepted memory");
    };
    let error = error.to_string();
    assert!(error.contains("\"memory\""), "{error}");
    assert!(error.contains("GDL_POOL__PARENT_HUB"), "{error}");
}

/// With `relay`, the default, a child pool's hub needs its parent's hub
/// to report to, and serves once it has one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_with_the_relay_backend_needs_its_parents_hub() {
    let Err(error) = hub_builder(config(), spec(), Some(settings(DirectoryBackend::DEFAULT)))
    else {
        panic!("a child without its parent's hub");
    };
    assert!(
        error.to_string().contains("[pool] parent_hub: missing"),
        "{error}"
    );

    let parent = hub_builder(config(), spec(), None)
        .expect("no pool")
        .serve()
        .await
        .expect("the hub serves");
    let relay = DirectoryBackend::Relay {
        parent_hub: Some(format!("127.0.0.1:{}", parent.public_addr().port())),
        state_dir: None,
    };
    let hub = hub_builder(config(), spec(), Some(settings(relay)))
        .expect("a relay pool")
        .serve()
        .await
        .expect("the hub serves");
    assert_eq!(status(&hub, "/v1/ext/gdl-pool/health").await, 503);
    hub.shutdown().await.expect("the hub stops");
    parent.shutdown().await.expect("the hub stops");
}

/// A hub reads its pool's declared properties of each kind from one
/// variable, a list of tables, `GDL_POOL__PROPERTIES__<KIND>`; one it
/// cannot read names the variable.
#[test]
fn a_pools_declared_properties_are_read_from_the_environment() {
    Jail::expect_with(|jail| {
        model_pool(jail, "/srv/pools");
        jail.set_env(
            "GDL_POOL__PROPERTIES__TEST",
            r#"[{name = "a"}, {partition = "p", name = "b"}]"#,
        );
        let pool = pool_from_env(&spec()).map_err(|e| e.to_string())?;
        assert_eq!(
            pool.map(|pool| pool.properties),
            Some(vec![
                gdl_model::PropertyKey::new("test", serde_json::json!({ "name": "a" }))
                    .expect("a key"),
                gdl_model::PropertyKey::new(
                    "test",
                    serde_json::json!({ "name": "b", "partition": "p" })
                )
                .expect("a key"),
            ])
        );
        jail.set_env("GDL_POOL__PROPERTIES__TEST", r#"["a"]"#);
        let refused = pool_from_env(&spec()).expect_err("a key that is not an object");
        assert!(
            refused.to_string().contains("GDL_POOL__PROPERTIES"),
            "{refused}"
        );
        Ok(())
    });
}
