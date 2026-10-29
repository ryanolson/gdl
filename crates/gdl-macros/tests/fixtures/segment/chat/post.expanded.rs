
//! Segment `post` of graph `chat`, hosting `Detokenize`, `Format`.
//!
//! [`start`] deploys it inside a program; the `chat-post` binary deploys it on
//! its own, through [`Binary`].

pub use ::chat_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "post";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a cell, only for those of its class.
pub const SEGMENTS: &[&str] = &["engine", "front", "post"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.engine]
nodes = ["Engine"]

[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number"]

[segment.post]
nodes = ["Detokenize", "Format"]
"#;

// Each message this segment sends or receives across a cut, or that
// returns through an anchor in it, is a wire type: an edge that carries
// one that is not fails to build here, naming the message and the edge.
// Compile time only.
#[allow(non_camel_case_types)]
type Engine_to_Detokenize = <contract::nodes::Detokenize as gdl_runtime::BackwardDecl>::In;
const _: () = gdl_runtime::gdl::__private::crosses::<Engine_to_Detokenize>();
#[allow(non_camel_case_types)]
type Format_to_Preprocess = <contract::nodes::Preprocess as gdl_runtime::OperatorDecl>::DownItem;
const _: () = gdl_runtime::gdl::__private::crosses::<Format_to_Preprocess>();

/// The implementations of the nodes this segment hosts.
pub struct Nodes {
    /// Node `Detokenize`.
    pub detokenize: ::chat_post_nodes::Detokenize,
    /// Node `Format`.
    pub format: ::chat_post_nodes::Format,
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
            <contract::nodes::Frontend as gdl_runtime::SourceDecl>::Req,
            <contract::nodes::Frontend as gdl_runtime::SourceDecl>::Item,
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
            .detokenize(nodes.detokenize)
            .format(nodes.format),
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
    const NAMES: &'static [&'static str] = &["Detokenize", "Format"];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {
    #[serde(rename = "Detokenize", default)]
    detokenize: Option<toml::Table>,
    #[serde(rename = "Format", default)]
    format: Option<toml::Table>,
}

/// What the `chat-post` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "chat-post";
    const SEGMENT: &'static str = SEGMENT;
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str =
        "[Detokenize]\n\n[Engine]\n\n[Format]\n\n[Number]\n\n[Preprocess]\n\n[Truncate]\n";
    type Settings = Settings;
    type Hosts = Nodes;

    async fn build(
        settings: Settings,
        builder: &gdl_runtime::Builder,
    ) -> Result<Nodes, gdl_runtime::Failure> {
        let Settings { node } = settings;
        let nodes = Nodes {
            detokenize: builder
                .build::<::chat_post_nodes::Detokenize>("Detokenize", node.detokenize)
                .await?,
            format: builder
                .build::<::chat_post_nodes::Format>("Format", node.format)
                .await?,
        };
        Ok(nodes)
    }

    async fn ready(hosts: &Nodes) -> gdl_runtime::gdl::Result<()> {
        tokio::try_join!(
            gdl_runtime::gdl::Build::ready(&hosts.detokenize),
            gdl_runtime::gdl::Build::ready(&hosts.format)
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
