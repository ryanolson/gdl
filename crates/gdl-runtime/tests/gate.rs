//! A roster that is held keeps every request this instance would send,
//! until it is released.
mod common;

use common::{
    Events, Plan, collect, expected, ms, prompt, reference_graph, reference_manifest, velo_node,
};
use figment::providers::{Format, Toml};
use gdl::{Cx, ErrorKind};
use gdl_runtime::{
    Deployment, DeploymentSpec, Hosted, Placement, ProcessConfig, Roster, RuntimeConfig,
};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

/// A launched segment holds its roster until every instance it can route
/// to is serving; a request that arrives first waits there, bounded by its
/// deadline, and goes on once the roster is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_roster_keeps_requests_until_it_is_released() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("side", ["C", "D"]);
    let (front_node, side_node) = (velo_node().await, velo_node().await);
    let mut roster = Roster::new();
    roster.add("front", front_node.peer_info());
    roster.add("side", side_node.peer_info());
    roster.hold();
    let events = Events::new();
    let manifest = reference_manifest(|_| true);
    let start = |name: &str, node: &velo::Velo| {
        let deployment = Deployment::new(
            node.clone(),
            &manifest,
            &placement,
            name,
            &roster,
            RuntimeConfig::default(),
        )
        .expect("deployment");
        let root = reference_graph(&deployment, &events).expect("graph");
        deployment.start(root).expect("start")
    };
    let front = start("front", &front_node);
    let _side = start("side", &side_node);
    let plan = Plan {
        tokens: 2,
        ..Plan::default()
    };
    let result = front
        .call(
            Cx::new().with_timeout(ms(300)),
            prompt("held", plan.clone()),
        )
        .await;
    assert_eq!(
        result.err().map(|e| e.kind()),
        Some(ErrorKind::DeadlineExceeded)
    );
    assert!(!events.has("C:held"), "a request left a held roster");
    roster.release();
    let out = front
        .call(Cx::new(), prompt("free", plan))
        .await
        .expect("a released roster routes");
    assert_eq!(collect(out).await, expected(0..2));
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NAMES: &'static [&'static str] = &[];
}

/// A launch holds the roster it hands out, and `serve` releases it once
/// the barrier is passed: a request made between the two waits, one made
/// from the serving body goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_launch_holds_its_roster_until_it_serves() {
    let placement = Placement::new()
        .segment("front", ["A", "B", "F", "E", "G"])
        .segment("side", ["C", "D"]);
    let manifest = reference_manifest(|_| true);
    let spec =
        DeploymentSpec::new(manifest.clone(), placement.clone()).expect("valid test fixture");
    let dir = tempfile::Builder::new()
        .prefix("gdl-gate-")
        .tempdir()
        .expect("roster directory");
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nroster = {:?}\nrendezvous_ms = 5000\n[instances]\nfront = 1\nside = 1\n",
        dir.path().display()
    );
    let config: ProcessConfig<Nothing> =
        ProcessConfig::from_figment(figment::Figment::from(Toml::string(&settings)))
            .expect("settings");
    // A process per instance: each has its own velo node.
    let front_process = gdl_runtime::Process::start(&config).await.expect("process");
    let side_process = gdl_runtime::Process::start(&config).await.expect("process");
    let segments: &[&str] = &["front", "side"];
    let (front, side) = tokio::join!(
        config.launch(&front_process, "front", segments, &spec, async { Ok(()) }),
        config.launch(&side_process, "side", segments, &spec, async { Ok(()) })
    );
    let (front, side) = (front.expect("front meets"), side.expect("side meets"));
    let events = Events::new();
    // A short setup timeout bounds a request that a released roster
    // should let through, so that a missing release fails fast.
    let runtime = RuntimeConfig {
        setup_timeout: Duration::from_secs(1),
        ..RuntimeConfig::default()
    };
    let start = |launch: &gdl_runtime::Launch, name: &str| {
        let deployment = Deployment::new(
            launch.node(),
            &manifest,
            &placement,
            name,
            launch.roster(),
            runtime.clone(),
        )
        .expect("deployment");
        let root = reference_graph(&deployment, &events).expect("graph");
        deployment.start(root).expect("start")
    };
    let front_segment = Arc::new(start(&front, "front"));
    let _side_segment = start(&side, "side");
    let plan = Plan {
        tokens: 2,
        ..Plan::default()
    };
    let result = front_segment
        .call(
            Cx::new().with_timeout(ms(300)),
            prompt("early", plan.clone()),
        )
        .await;
    assert_eq!(
        result.err().map(|e| e.kind()),
        Some(ErrorKind::DeadlineExceeded),
        "a request left a launched roster before the barrier"
    );
    assert!(!events.has("C:early"));
    let caller = Arc::clone(&front_segment);
    let (served_front, served_side) = tokio::join!(
        front.serve(common::Idle, |_, _| async move {
            let out = caller.call(Cx::new(), prompt("served", plan)).await?;
            assert_eq!(collect(out).await, expected(0..2));
            Ok(())
        }),
        side.serve(common::Idle, |_, _| async { Ok(()) })
    );
    served_front.expect("a request from the serving body goes through");
    served_side.expect("the side serves");
}
