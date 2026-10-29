
//! Segment `second` of graph `api`, hosting `SecondEngine`.
//!
//! [`start`] deploys it inside a program; the `api-second` binary deploys it on
//! its own, through [`Binary`].

pub use ::api_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "second";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a pool, only for those of its class.
pub const SEGMENTS: &[&str] = &["engine", "front", "second"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.engine]
nodes = ["Engine"]

[segment.front]
nodes = ["Http", "SecondPipeline"]

[segment.second]
nodes = ["SecondEngine"]
"#;

// Each message this segment sends or receives across a cut, or that
// returns through an anchor in it, is a wire type: an edge that carries
// one that is not fails to build here, naming the message and the edge.
// Compile time only.
#[allow(non_camel_case_types)]
type SecondPipeline_to_SecondEngine = <contract::nodes::SecondEngine as gdl_runtime::SinkDecl>::Req;
const _: () = gdl_runtime::gdl::__private::crosses::<SecondPipeline_to_SecondEngine>();
#[allow(non_camel_case_types)]
type SecondEngine_to_SecondPipeline =
    <contract::nodes::SecondPipeline as gdl_runtime::SourceDecl>::Item;
const _: () = gdl_runtime::gdl::__private::crosses::<SecondEngine_to_SecondPipeline>();

/// The implementations of the nodes this segment hosts.
pub struct Nodes {
    /// Node `SecondEngine`.
    pub second_engine: ::api_frontend::Echo,
}

/// Deploy this segment on `node`. Every segment's instances must be in
/// `roster`.
pub fn start(
    node: gdl_runtime::velo::Velo,
    roster: &gdl_runtime::Roster,
    config: gdl_runtime::RuntimeConfig,
    nodes: Nodes,
) -> gdl_runtime::gdl::Result<
    gdl_runtime::Segment<(
        impl gdl_runtime::gdl::Next<
            <contract::nodes::Http as gdl_runtime::SourceDecl>::Req,
            <contract::nodes::Http as gdl_runtime::SourceDecl>::Item,
        > + gdl_runtime::Register
        + use<>,
        (
            impl gdl_runtime::gdl::Next<
                <contract::nodes::SecondPipeline as gdl_runtime::SourceDecl>::Req,
                <contract::nodes::SecondPipeline as gdl_runtime::SourceDecl>::Item,
            > + gdl_runtime::Register
            + use<>,
            (),
        ),
    )>,
> {
    let placement = gdl_runtime::Placement::from_toml(PLACEMENT)?;
    let manifest = contract::manifest();
    let deployment =
        gdl_runtime::Deployment::new(node, &manifest, &placement, SEGMENT, roster, config)?;
    let root = contract::assemble(
        &deployment,
        contract::Bindings::new().second_engine(nodes.second_engine),
    )?;
    deployment.start(root)
}

/// What this segment's binary hosts, as its settings name it.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default)]
    node: NodeSettings,
}

impl gdl_runtime::Hosted for Settings {
    const KEYS: &'static [&'static str] = &["node"];
    const NAMES: &'static [&'static str] = &["SecondEngine"];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {
    #[serde(rename = "SecondEngine", default)]
    second_engine: Option<toml::Table>,
}

/// What the `api-second` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "api-second";
    const SEGMENT: &'static str = SEGMENT;
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "[Engine]\n\n[Http]\nenabled_anthropic = true\nenabled_chat = true\nenabled_responses = true\n\n[Http_anthropic_op]\n\n[Http_chat_b0]\n\n[Http_chat_f0]\n\n[Http_responses_op]\n\n[SecondEngine]\n";
    type Settings = Settings;
    type Hosts = Nodes;

    async fn build(
        settings: Settings,
        builder: &gdl_runtime::Builder,
    ) -> Result<Nodes, gdl_runtime::Failure> {
        let Settings { node } = settings;
        let nodes = Nodes {
            second_engine: builder
                .build::<::api_frontend::Echo>("SecondEngine", node.second_engine)
                .await?,
        };
        Ok(nodes)
    }

    async fn ready(hosts: &Nodes) -> gdl_runtime::gdl::Result<()> {
        tokio::try_join!(gdl_runtime::gdl::Build::ready(&hosts.second_engine))?;
        Ok(())
    }

    fn deployment() -> gdl_runtime::gdl::Result<gdl_runtime::DeploymentSpec> {
        Ok(gdl_runtime::DeploymentSpec::new(
            contract::manifest(),
            gdl_runtime::Placement::from_toml(PLACEMENT)?,
        )?)
    }

    fn start(
        hosts: Nodes,
        node: gdl_runtime::velo::Velo,
        roster: &gdl_runtime::Roster,
        runtime: gdl_runtime::RuntimeConfig,
    ) -> gdl_runtime::gdl::Result<gdl_runtime::Serving> {
        let segment = crate::start(node, roster, runtime, hosts)?;
        Ok(gdl_runtime::Serving::new(
            segment,
            |_segment, shutdown| async move {
                shutdown.cancelled().await;
                Ok(())
            },
        ))
    }
}
