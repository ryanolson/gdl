
//! Segment `dsp` of graph `asr`, hosting `Resample`.
//!
//! [`start`] deploys it inside a program; the `asr-dsp` binary deploys it on
//! its own, through [`Binary`].

pub use ::asr_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "dsp";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a cell, only for those of its class.
pub const SEGMENTS: &[&str] = &["dsp", "engine", "front"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.dsp]
nodes = ["Resample"]

[segment.engine]
nodes = ["Recognizer", "Punctuate"]

[segment.front]
nodes = ["Client", "Gateway"]
"#;

// Each message this segment sends or receives across a cut, or that
// returns through an anchor in it, is a wire type: an edge that carries
// one that is not fails to build here, naming the message and the edge.
// Compile time only.
#[allow(non_camel_case_types)]
type Gateway_to_Resample_head = <<contract::nodes::Resample as gdl_runtime::ForwardDecl>::In as gdl_runtime::gdl::__private::Parts>::Head;
const _: () = gdl_runtime::gdl::__private::crosses::<Gateway_to_Resample_head>();
#[allow(non_camel_case_types)]
type Gateway_to_Resample_item = <<contract::nodes::Resample as gdl_runtime::ForwardDecl>::In as gdl_runtime::gdl::__private::Parts>::Item;
const _: () = gdl_runtime::gdl::__private::crosses::<Gateway_to_Resample_item>();
#[allow(non_camel_case_types)]
type Resample_to_Recognizer_item = <<contract::nodes::Recognizer as gdl_runtime::SinkDecl>::Req as gdl_runtime::gdl::__private::Parts>::Item;
const _: () = gdl_runtime::gdl::__private::crosses::<Resample_to_Recognizer_item>();

/// The implementations of the nodes this segment hosts.
pub struct Nodes {
    /// Node `Resample`.
    pub resample: ::asr_dsp_nodes::Resample,
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
            <contract::nodes::Client as gdl_runtime::SourceDecl>::Req,
            <contract::nodes::Client as gdl_runtime::SourceDecl>::Item,
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
        contract::Bindings::new().resample(nodes.resample),
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
    const NAMES: &'static [&'static str] = &["Resample"];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {
    #[serde(rename = "Resample", default)]
    resample: Option<toml::Table>,
}

/// What the `asr-dsp` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "asr-dsp";
    const SEGMENT: &'static str = SEGMENT;
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "[Gateway]\n\n[Punctuate]\n\n[Recognizer]\n\n[Resample]\n";
    type Settings = Settings;
    type Hosts = Nodes;

    async fn build(
        settings: Settings,
        builder: &gdl_runtime::Builder,
    ) -> Result<Nodes, gdl_runtime::Failure> {
        let Settings { node } = settings;
        let nodes = Nodes {
            resample: builder
                .build::<::asr_dsp_nodes::Resample>("Resample", node.resample)
                .await?,
        };
        Ok(nodes)
    }

    async fn ready(hosts: &Nodes) -> gdl_runtime::gdl::Result<()> {
        tokio::try_join!(gdl_runtime::gdl::Build::ready(&hosts.resample))?;
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
