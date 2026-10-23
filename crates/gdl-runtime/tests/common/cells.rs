//! A deployment of two classes for the cell tests, and its instances'
//! settings: `global` runs `a` (S, the chooser), and `model_x` runs `f`
//! (F, the cell entry) and `d` (D). The boundary is the inner end of S's
//! hop.

use figment::providers::Format as _;
use gdl_runtime::{
    DeploymentSpec, HopInfo, Hosted, Launch, Manifest, NodeInfo, Placement, Process, ProcessConfig,
    Role, TypeInfo,
};
use serde::Deserialize;

/// Every segment of [`spec`].
pub const SEGMENTS: &[&str] = &["a", "f", "d"];

/// The region every cell of these tests runs in: a global cell follows
/// only the model cells of its own region.
pub const REGION: &str = "us-east-1";

/// The global cell.
pub const GLOBAL: &str = "global-01-us-east-1";

/// `S => F { D }`: `a` in `global`, `f` and `d` in `model_x`.
pub fn spec() -> DeploymentSpec {
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
    let graph = Manifest {
        name: "cells".into(),
        schema: 1,
        nodes: vec![
            node("S", Role::Source),
            node("F", Role::Operator),
            node("D", Role::Sink),
        ],
        hops: vec![hop("S", "F"), hop("F", "D")],
    };
    let placement = Placement::new()
        .segment("a", ["S"])
        .segment("f", ["F"])
        .segment("d", ["D"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"]);
    DeploymentSpec::new(graph, placement).expect("valid test fixture")
}

/// The `[cell]` table of an instance of the global cell.
pub fn global_cell() -> String {
    format!(
        "[cell]\nname = '{GLOBAL}'\nclass = 'global'\nregion = '{REGION}'\ndirectory = 'memory'\n"
    )
}

/// The `[cell]` table of an instance of the model cell `name`.
pub fn model_cell(name: &str) -> String {
    format!(
        "[cell]\nname = '{name}'\nclass = 'model_x'\nregion = '{REGION}'\ndirectory = 'memory'\n"
    )
}

/// The settings of an instance at the hub on these ports, heartbeating
/// every 200 ms, waiting for its peers for `rendezvous_ms`, with `extra`
/// appended (a `[cell]` table, `[min_replicas]`, `[runtime]`).
pub fn settings(
    public: u16,
    control: u16,
    rendezvous_ms: u64,
    extra: &str,
) -> ProcessConfig<Nothing> {
    let settings = format!(
        "listen = \"127.0.0.1:0\"\nhub = {{ host = \"127.0.0.1\", public_port = {public}, control_port = {control}, heartbeat_ms = 200 }}\nrendezvous_ms = {rendezvous_ms}\n{extra}"
    );
    ProcessConfig::from_figment(figment::Figment::from(figment::providers::Toml::string(
        &settings,
    )))
    .expect("settings")
}

/// A hub that admits [`spec`]'s segments, on OS-assigned ports, without a
/// cell's extension.
pub async fn admission_hub() -> velo_hub::Hub {
    velo_hub::Hub::builder(hub_config())
        .extension(gdl_hub::GraphAdmission::new(spec()))
        .serve()
        .await
        .expect("the hub serves")
}

/// A hub's configuration on OS-assigned ports.
pub fn hub_config() -> velo_hub::HubConfig {
    let mut config = velo_hub::HubConfig::default();
    config.server.public_port = 0;
    config.server.control_port = 0;
    config
}

/// Launch an instance of `segment` with `config`, in a process of its own.
pub async fn launch(
    config: &ProcessConfig<Nothing>,
    segment: &'static str,
) -> gdl::Result<(Process, Launch)> {
    let process = Process::start(config).await?;
    let launch = config
        .launch(&process, segment, SEGMENTS, &spec(), async { Ok(()) })
        .await?;
    Ok((process, launch))
}

/// Hosts nothing.
#[derive(Deserialize)]
pub struct Nothing {}

impl Hosted for Nothing {
    const KEYS: &'static [&'static str] = &[];
    const NAMES: &'static [&'static str] = &[];
}
