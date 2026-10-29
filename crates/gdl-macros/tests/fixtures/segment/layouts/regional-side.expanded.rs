
//! Segment `side` of graph `trio`, hosting `Tell`.
//!
//! [`start`] deploys it inside a program; the `trio-regional-side` binary deploys it on
//! its own, through [`Binary`].

pub use ::trio_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "side";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a cell, only for those of its class.
pub const SEGMENTS: &[&str] = &["engine", "front", "side"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.engine]
nodes = ["Engine", "Spare"]

[segment.front]
nodes = ["Ask", "Preprocess", "Truncate", "Detokenize", "Format", "Number"]

[segment.side]
nodes = ["Tell"]
"#;

// Each message this segment sends or receives across a cut, or that
// returns through an anchor in it, is a wire type: an edge that carries
// one that is not fails to build here, naming the message and the edge.
// Compile time only.
#[allow(non_camel_case_types)]
type Tell_to_Spare = <contract::nodes::Spare as gdl_runtime::SinkDecl>::Req;
const _: () = gdl_runtime::gdl::__private::crosses::<Tell_to_Spare>();
#[allow(non_camel_case_types)]
type Spare_to_Tell = <contract::nodes::Tell as gdl_runtime::SourceDecl>::Item;
const _: () = gdl_runtime::gdl::__private::crosses::<Spare_to_Tell>();

/// The implementations of the nodes this segment hosts.
pub struct Nodes {}

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
            <contract::nodes::Ask as gdl_runtime::SourceDecl>::Req,
            <contract::nodes::Ask as gdl_runtime::SourceDecl>::Item,
        > + gdl_runtime::Register
        + use<>,
        (
            impl gdl_runtime::gdl::Next<
                <contract::nodes::Tell as gdl_runtime::SourceDecl>::Req,
                <contract::nodes::Tell as gdl_runtime::SourceDecl>::Item,
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
    let Nodes {} = nodes;
    let root = contract::assemble(&deployment, contract::Bindings::new())?;
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
    const NAMES: &'static [&'static str] = &[];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {}

/// What the `trio-regional-side` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "trio-regional-side";
    const SEGMENT: &'static str = SEGMENT;
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "[Detokenize]\n\n[Engine]\n\n[Format]\n\n[Number]\n\n[Preprocess]\n\n[Spare]\n\n[Truncate]\n";
    type Settings = Settings;
    type Hosts = Nodes;

    async fn build(
        settings: Settings,
        _builder: &gdl_runtime::Builder,
    ) -> Result<Nodes, gdl_runtime::Failure> {
        let Settings { node } = settings;
        let NodeSettings {} = node;
        let nodes = Nodes {};
        Ok(nodes)
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
