
//! Segment `trunc` of graph `chat`, hosting `Truncate`.
//!
//! [`start`] deploys it inside a program; the `chat-trunc` binary deploys it on
//! its own, through [`Binary`].

pub use ::chat_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "trunc";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a pool, only for those of its class.
pub const SEGMENTS: &[&str] = &["core", "trunc"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.core]
nodes = ["Frontend", "Preprocess", "Engine", "Detokenize", "Format", "Number"]

[segment.trunc]
nodes = ["Truncate"]
"#;

// Each message this segment sends or receives across a cut, or that
// returns through an anchor in it, is a wire type: an edge that carries
// one that is not fails to build here, naming the message and the edge.
// Compile time only.
#[allow(non_camel_case_types)]
type Preprocess_to_Truncate = <contract::nodes::Truncate as gdl_runtime::ForwardDecl>::In;
const _: () = gdl_runtime::gdl::__private::crosses::<Preprocess_to_Truncate>();

/// The implementations of the nodes this segment hosts.
pub struct Nodes {
    /// Node `Truncate`.
    pub truncate: ::chat_front_nodes::Truncate,
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
        contract::Bindings::new().truncate(nodes.truncate),
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
    const NAMES: &'static [&'static str] = &["Truncate"];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {
    #[serde(rename = "Truncate", default)]
    truncate: Option<toml::Table>,
}

/// What the `chat-trunc` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "chat-trunc";
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
            truncate: builder
                .build::<::chat_front_nodes::Truncate>("Truncate", node.truncate)
                .await?,
        };
        Ok(nodes)
    }

    async fn ready(hosts: &Nodes) -> gdl_runtime::gdl::Result<()> {
        tokio::try_join!(gdl_runtime::gdl::Build::ready(&hosts.truncate))?;
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
