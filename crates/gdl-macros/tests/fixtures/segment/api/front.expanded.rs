
//! Segment `front` of graph `api`, hosting `Http`.
//!
//! [`start`] deploys it inside a program; the `api-front` binary deploys it on
//! its own, through [`Binary`].

pub use ::api_contract as contract;

/// This segment's name in the placement.
pub const SEGMENT: &str = "front";

/// Every segment of the placement. An instance waits for all of them,
/// or, in a pool, only for those of its class.
pub const SEGMENTS: &[&str] = &["engine", "front"];

/// The placement this crate was generated for.
pub const PLACEMENT: &str = r#"[segment.engine]
nodes = ["Engine"]

[segment.front]
nodes = ["Http"]
"#;

// Each message this segment sends or receives across a cut, or that
// returns through an anchor in it, is a wire type: an edge that carries
// one that is not fails to build here, naming the message and the edge.
// Compile time only.
#[allow(non_camel_case_types)]
type Http_to_Engine = <contract::nodes::Engine as gdl_runtime::SinkDecl>::Req;
const _: () = gdl_runtime::gdl::__private::crosses::<Http_to_Engine>();
#[allow(non_camel_case_types)]
type Engine_to_Http = <contract::nodes::Http as gdl_runtime::SourceDecl>::Item;
const _: () = gdl_runtime::gdl::__private::crosses::<Engine_to_Http>();

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
    frontend: Option<toml::Table>,
    #[serde(default)]
    adapter: std::collections::BTreeMap<String, toml::Table>,
}

impl gdl_runtime::Hosted for Settings {
    const KEYS: &'static [&'static str] = &["node", "frontend", "adapter"];
    const NAMES: &'static [&'static str] = &[
        "Http_chat_f0",
        "Http_chat_b0",
        "Http_responses_op",
        "Http_anthropic_op",
    ];
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSettings {}

/// What this segment's binary builds: its nodes, and what serves the source.
pub struct Hosts {
    nodes: Nodes,
    frontend: ::api_frontend::ApiFrontend,
    entry_487474705f636861745f6630: ::api_frontend::ExtractChat,
    entry_487474705f636861745f6230: ::api_frontend::FormatChat,
    entry_487474705f726573706f6e7365735f6f70: ::api_frontend::ResponsesAdapter,
    entry_487474705f616e7468726f7069635f6f70: ::api_frontend::AnthropicAdapter,
}

/// What the `api-front` binary hosts and serves; its `main` is
/// `gdl_runtime::segment_main::<Binary>()`.
pub struct Binary;

impl gdl_runtime::SegmentBinary for Binary {
    const PACKAGE: &'static str = "api-front";
    const SEGMENT: &'static str = SEGMENT;
    const SEGMENTS: &'static [&'static str] = SEGMENTS;
    const GRAPH_CONFIG: &'static str = "[Engine]\n\n[Http]\nenabled_anthropic = true\nenabled_chat = true\nenabled_responses = true\n\n[Http_anthropic_op]\n\n[Http_chat_b0]\n\n[Http_chat_f0]\n\n[Http_responses_op]\n";
    type Settings = Settings;
    type Hosts = Hosts;

    async fn build(
        settings: Settings,
        builder: &gdl_runtime::Builder,
    ) -> Result<Hosts, gdl_runtime::Failure> {
        let Settings {
            node,
            frontend,
            mut adapter,
        } = settings;
        let NodeSettings {} = node;
        let nodes = Nodes {};
        let frontend = builder
            .build::<::api_frontend::ApiFrontend>("Http", frontend)
            .await?;
        let enabled: &[&str] = &[
            "Http_chat_f0",
            "Http_chat_b0",
            "Http_responses_op",
            "Http_anthropic_op",
        ];
        if let Some(unknown) = adapter.keys().find(|key| !enabled.contains(&key.as_str())) {
            return Err(format!("[adapter.{unknown}]: no enabled entry node of that name").into());
        }
        let entry_487474705f636861745f6630 = builder
            .build::<::api_frontend::ExtractChat>("Http_chat_f0", adapter.remove("Http_chat_f0"))
            .await?;
        let entry_487474705f636861745f6230 = builder
            .build::<::api_frontend::FormatChat>("Http_chat_b0", adapter.remove("Http_chat_b0"))
            .await?;
        let entry_487474705f726573706f6e7365735f6f70 = builder
            .build::<::api_frontend::ResponsesAdapter>(
                "Http_responses_op",
                adapter.remove("Http_responses_op"),
            )
            .await?;
        let entry_487474705f616e7468726f7069635f6f70 = builder
            .build::<::api_frontend::AnthropicAdapter>(
                "Http_anthropic_op",
                adapter.remove("Http_anthropic_op"),
            )
            .await?;
        Ok(Hosts {
            nodes,
            frontend,
            entry_487474705f636861745f6630,
            entry_487474705f636861745f6230,
            entry_487474705f726573706f6e7365735f6f70,
            entry_487474705f616e7468726f7069635f6f70,
        })
    }

    async fn ready(hosts: &Hosts) -> gdl_runtime::gdl::Result<()> {
        tokio::try_join!(
            gdl_runtime::gdl::Build::ready(&hosts.frontend),
            gdl_runtime::gdl::Build::ready(&hosts.entry_487474705f636861745f6630),
            gdl_runtime::gdl::Build::ready(&hosts.entry_487474705f636861745f6230),
            gdl_runtime::gdl::Build::ready(&hosts.entry_487474705f726573706f6e7365735f6f70),
            gdl_runtime::gdl::Build::ready(&hosts.entry_487474705f616e7468726f7069635f6f70)
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
        let Hosts {
            nodes,
            frontend,
            entry_487474705f636861745f6630,
            entry_487474705f636861745f6230,
            entry_487474705f726573706f6e7365735f6f70,
            entry_487474705f616e7468726f7069635f6f70,
        } = hosts;
        let segment = crate::start(node, roster, runtime, nodes)?;
        Ok(gdl_runtime::Serving::new(
            segment,
            |segment, shutdown| async move {
                let pipeline_48747470 = segment.client(|roots| roots.clone());
                let clients = ::api_frontend::ApiClients {
                    chat: Some(gdl_runtime::gdl::Client::new(
                        gdl_runtime::gdl::Source::new(gdl_runtime::gdl::Hop::new(
                            (std::sync::Arc::new(entry_487474705f636861745f6630), ()),
                            gdl_runtime::gdl::Join::new(pipeline_48747470.clone()),
                            (std::sync::Arc::new(entry_487474705f636861745f6230), ()),
                        )),
                    )),
                    responses: Some(gdl_runtime::gdl::Client::new(
                        gdl_runtime::gdl::Source::new(gdl_runtime::gdl::Hop::new(
                            (),
                            gdl_runtime::gdl::OperatorStage::new(
                                std::sync::Arc::new(entry_487474705f726573706f6e7365735f6f70),
                                gdl_runtime::gdl::Hop::new(
                                    (),
                                    gdl_runtime::gdl::Join::new(pipeline_48747470.clone()),
                                    (),
                                ),
                            ),
                            (),
                        )),
                    )),
                    anthropic: Some(gdl_runtime::gdl::Client::new(
                        gdl_runtime::gdl::Source::new(gdl_runtime::gdl::Hop::new(
                            (),
                            gdl_runtime::gdl::OperatorStage::new(
                                std::sync::Arc::new(entry_487474705f616e7468726f7069635f6f70),
                                gdl_runtime::gdl::Hop::new(
                                    (),
                                    gdl_runtime::gdl::Join::new(pipeline_48747470.clone()),
                                    (),
                                ),
                            ),
                            (),
                        )),
                    )),
                };
                gdl_runtime::gdl::Service::serve(frontend, clients, async move {
                    shutdown.cancelled().await
                })
                .await
            },
        ))
    }
}
