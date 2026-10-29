
//! Segment `front` of graph `policy`, hosting `Front`.
//!
//! [`start`] deploys it inside a program; the `policy-front` binary deploys it on
//! its own, through [`Binary`].

pub use ::policy_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "front";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a pool, only for those of its class.
pub const SEGMENTS: &[&str] = &["front", "model"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.front]
nodes = ["Front"]

[segment.model]
nodes = ["Model"]

[class.global]
segments = ["front"]

[class.model]
segments = ["model"]

[pools.model]
policy = "by_model"
"#;

// Each message this segment sends or receives across a cut, or that
// returns through an anchor in it, is a wire type: an edge that carries
// one that is not fails to build here, naming the message and the edge.
// Compile time only.
#[allow(non_camel_case_types)]
type Front_to_Model = <contract::nodes::Model as gdl_runtime::SinkDecl>::Req;
const _: () = gdl_runtime::gdl::__private::crosses::<Front_to_Model>();
#[allow(non_camel_case_types)]
type Model_to_Front = <contract::nodes::Front as gdl_runtime::SourceDecl>::Item;
const _: () = gdl_runtime::gdl::__private::crosses::<Model_to_Front>();

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
    gdl_runtime::Segment<
        impl gdl_runtime::gdl::Next<
            <contract::nodes::Front as gdl_runtime::SourceDecl>::Req,
            <contract::nodes::Front as gdl_runtime::SourceDecl>::Item,
        > + gdl_runtime::Register
        + use<>,
    >,
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
    #[serde(default)]
    pool_policy: std::collections::BTreeMap<String, toml::Table>,
}

impl gdl_runtime::Hosted for Settings {
    const KEYS: &'static [&'static str] = &["node", "pool_policy"];
    const NAMES: &'static [&'static str] = &[];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {}

/// What the `policy-front` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "policy-front";
    const SEGMENT: &'static str = SEGMENT;
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "[Model]\n";
    type Settings = Settings;
    type Hosts = Nodes;

    async fn build(
        settings: Settings,
        builder: &gdl_runtime::Builder,
    ) -> Result<Nodes, gdl_runtime::Failure> {
        let Settings {
            node,
            mut pool_policy,
        } = settings;
        let NodeSettings {} = node;
        let nodes = Nodes {};
        let policies: &[&str] = &["by_model"];
        if let Some(unknown) = pool_policy
            .keys()
            .find(|key| !policies.contains(&key.as_str()))
        {
            return Err(format!(
                "[pool_policy.{unknown}]: this segment chooses pools by no policy of that name"
            )
            .into());
        }
        builder
            .build_pool_policy::<::policy_router::ByModel>(
                "by_model",
                pool_policy.remove("by_model"),
            )
            .await?;
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
