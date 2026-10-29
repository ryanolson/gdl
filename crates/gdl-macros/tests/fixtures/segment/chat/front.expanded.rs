
//! Segment `front` of graph `chat`, hosting `Frontend`, `Preprocess`, `Truncate`, `Number`.
//!
//! [`start`] deploys it inside a program; the `chat-front` binary deploys it on
//! its own, through [`Binary`].

pub use ::chat_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "front";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a pool, only for those of its class.
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
type Truncate_to_Engine = <contract::nodes::Engine as gdl_runtime::SinkDecl>::Req;
const _: () = gdl_runtime::gdl::__private::crosses::<Truncate_to_Engine>();
#[allow(non_camel_case_types)]
type Format_to_Preprocess = <contract::nodes::Preprocess as gdl_runtime::OperatorDecl>::DownItem;
const _: () = gdl_runtime::gdl::__private::crosses::<Format_to_Preprocess>();

// The source's types the binary reads or writes as JSON lines: a type
// the binary cannot read or write fails to build here. Compile time only.
const _: () = gdl_runtime::reads_json::<contract::SourceReq>();
const _: () = gdl_runtime::writes_json::<contract::SourceItem>();

/// The implementations of the nodes this segment hosts.
pub struct Nodes {
    /// Node `Preprocess`.
    pub preprocess: ::chat_front_nodes::Preprocess,
    /// Node `Truncate`.
    pub truncate: ::chat_front_nodes::Truncate,
    /// Node `Number`.
    pub number: ::chat_front_nodes::Number,
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
            .preprocess(nodes.preprocess)
            .truncate(nodes.truncate)
            .number(nodes.number),
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
    const NAMES: &'static [&'static str] = &["Preprocess", "Truncate", "Number"];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {
    #[serde(rename = "Preprocess", default)]
    preprocess: Option<toml::Table>,
    #[serde(rename = "Truncate", default)]
    truncate: Option<toml::Table>,
    #[serde(rename = "Number", default)]
    number: Option<toml::Table>,
}

/// What the `chat-front` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "chat-front";
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
            preprocess: builder
                .build::<::chat_front_nodes::Preprocess>("Preprocess", node.preprocess)
                .await?,
            truncate: builder
                .build::<::chat_front_nodes::Truncate>("Truncate", node.truncate)
                .await?,
            number: builder
                .build::<::chat_front_nodes::Number>("Number", node.number)
                .await?,
        };
        Ok(nodes)
    }

    async fn ready(hosts: &Nodes) -> gdl_runtime::gdl::Result<()> {
        tokio::try_join!(
            gdl_runtime::gdl::Build::ready(&hosts.preprocess),
            gdl_runtime::gdl::Build::ready(&hosts.truncate),
            gdl_runtime::gdl::Build::ready(&hosts.number)
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
            |segment, shutdown| async move {
                let input = tokio::io::BufReader::new(tokio::io::stdin());
                let call = |request: contract::SourceReq| {
                    segment.call::<_, contract::SourceItem>(gdl_runtime::gdl::Cx::new(), request)
                };
                gdl_runtime::serve_json_lines(
                    input,
                    tokio::io::stdout(),
                    shutdown.cancelled(),
                    gdl_runtime::json_codec!(contract::SourceReq, contract::SourceItem),
                    call,
                )
                .await
                .map_err(|e| {
                    gdl_runtime::gdl::Error::new(
                        gdl_runtime::gdl::ErrorKind::Transport,
                        e.to_string(),
                    )
                })
            },
        ))
    }
}
