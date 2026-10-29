
//! Segment `main` of graph `http`, hosting `Http`, `Engine`.
//!
//! [`start`] deploys it inside a program; the `http-main` binary deploys it on
//! its own, through [`Binary`].

pub use ::http_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "main";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a pool, only for those of its class.
pub const SEGMENTS: &[&str] = &["main"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.main]
nodes = ["Http", "Engine"]
"#;

/// The implementations of the nodes this segment hosts.
pub struct Nodes {
    /// Node `Engine`.
    pub engine: ::http_frontend::Echo,
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
    let root = contract::assemble(&deployment, contract::Bindings::new().engine(nodes.engine))?;
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
    const NAMES: &'static [&'static str] = &["Engine"];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {
    #[serde(rename = "Engine", default)]
    engine: Option<toml::Table>,
}

/// What this segment's binary builds: its nodes, and what serves the source.
pub struct Hosts {
    nodes: Nodes,
    frontend: ::http_frontend::HttpFrontend,
}

/// What the `http-main` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "http-main";
    const SEGMENT: &'static str = SEGMENT;
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "[Engine]\n\n[Http]\n";
    type Settings = Settings;
    type Hosts = Hosts;

    async fn build(
        settings: Settings,
        builder: &gdl_runtime::Builder,
    ) -> Result<Hosts, gdl_runtime::Failure> {
        let Settings { node, frontend } = settings;
        let nodes = Nodes {
            engine: builder
                .build::<::http_frontend::Echo>("Engine", node.engine)
                .await?,
        };
        let frontend = builder
            .build::<::http_frontend::HttpFrontend>("Http", frontend)
            .await?;
        Ok(Hosts { nodes, frontend })
    }

    async fn ready(hosts: &Hosts) -> gdl_runtime::gdl::Result<()> {
        tokio::try_join!(
            gdl_runtime::gdl::Build::ready(&hosts.nodes.engine),
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
