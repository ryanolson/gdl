
//! Segment `all` of graph `film`, hosting `Http`, `Engine`, `UsageTap`.
//!
//! [`start`] deploys it inside a program; the `film-all` binary deploys it on
//! its own, through [`Binary`].

pub use ::film_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "all";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a cell, only for those of its class.
pub const SEGMENTS: &[&str] = &["all"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.all]
nodes = ["Http", "Engine", "UsageTap"]
"#;

/// The implementations of the nodes this segment hosts.
pub struct Nodes {
    /// Node `Engine`.
    pub engine: ::gdl_demo_nodes::MockEngine,
    /// Node `UsageTap`.
    pub usage_tap: ::acme_gdl_nodes::UsageTap,
}

/// Deploy this segment on `node`. Every segment's instances must be in
/// `roster`.
pub fn start(
    node: gdl_runtime::velo::Velo,
    roster: &gdl_runtime::Roster,
    config: gdl_runtime::RuntimeConfig,
    nodes: Nodes,
) -> gdl_runtime::gdl::Result<
    gdl_runtime::Segment<
        impl gdl_runtime::gdl::Next<
            <contract::nodes::Http as gdl_runtime::SourceDecl>::Req,
            <contract::nodes::Http as gdl_runtime::SourceDecl>::Item,
        > + gdl_runtime::Register
        + use<>,
    >,
> {
    let placement = gdl_runtime::Placement::from_toml(PLACEMENT)?;
    let manifest = contract::manifest();
    let deployment =
        gdl_runtime::Deployment::new(node, &manifest, &placement, SEGMENT, roster, config)?;
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new()
            .engine(nodes.engine)
            .usage_tap(nodes.usage_tap),
    )?;
    deployment.start(root)
}

/// What this segment's binary hosts, as its settings name it.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default)]
    node: NodeSettings,
    #[serde(default)]
    frontend: Option<toml::Table>,
}

impl gdl_runtime::Hosted for Settings {
    const KEYS: &'static [&'static str] = &["node", "frontend"];
    const NAMES: &'static [&'static str] = &["Engine", "UsageTap"];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {
    #[serde(rename = "Engine", default)]
    engine: Option<toml::Table>,
    #[serde(rename = "UsageTap", default)]
    usage_tap: Option<toml::Table>,
}

/// What this segment's binary builds: its nodes, and what serves the source.
pub struct Hosts {
    nodes: Nodes,
    frontend: ::gdl_demo_nodes::HttpFrontend,
}

/// What the `film-all` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "film-all";
    const SEGMENT: &'static str = SEGMENT;
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "[Engine]\nmodel = \"film-engine\"\nuppercase = true\n\n[Http]\n\n[UsageTap]\nenabled = true\ntenant = \"acme-film\"\n";
    type Settings = Settings;
    type Hosts = Hosts;

    async fn build(
        settings: Settings,
        builder: &gdl_runtime::Builder,
    ) -> Result<Hosts, gdl_runtime::Failure> {
        let Settings { node, frontend } = settings;
        let nodes = Nodes {
            engine: builder
                .build::<::gdl_demo_nodes::MockEngine>("Engine", node.engine)
                .await?,
            usage_tap: builder
                .build::<::acme_gdl_nodes::UsageTap>("UsageTap", node.usage_tap)
                .await?,
        };
        let frontend = builder
            .build::<::gdl_demo_nodes::HttpFrontend>("Http", frontend)
            .await?;
        Ok(Hosts { nodes, frontend })
    }

    async fn ready(hosts: &Hosts) -> gdl_runtime::gdl::Result<()> {
        tokio::try_join!(
            gdl_runtime::gdl::Build::ready(&hosts.nodes.engine),
            gdl_runtime::gdl::Build::ready(&hosts.nodes.usage_tap),
            gdl_runtime::gdl::Build::ready(&hosts.frontend)
        )?;
        Ok(())
    }

    fn deployment() -> gdl_runtime::gdl::Result<gdl_runtime::DeploymentSpec> {
        Ok(gdl_runtime::DeploymentSpec::new(
            contract::manifest(),
            gdl_runtime::Placement::from_toml(PLACEMENT)?,
        )?)
    }

    fn start(
        hosts: Hosts,
        node: gdl_runtime::velo::Velo,
        roster: &gdl_runtime::Roster,
        runtime: gdl_runtime::RuntimeConfig,
    ) -> gdl_runtime::gdl::Result<gdl_runtime::Serving> {
        let Hosts { nodes, frontend } = hosts;
        let segment = crate::start(node, roster, runtime, nodes)?;
        Ok(gdl_runtime::Serving::new(
            segment,
            |segment, shutdown| async move {
                gdl_runtime::gdl::Frontend::serve(
                    frontend,
                    segment.client(|root| root.clone()),
                    async move { shutdown.cancelled().await },
                )
                .await
            },
        ))
    }
}
