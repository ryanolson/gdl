//! A segment crate's binary. Its lifecycle is the runtime's
//! (`gdl::segment_main`); what is emitted is what differs between
//! segments: `main.rs`, which names the segment's `Binary` and documents
//! its settings, and the library's `Binary`, which implements
//! `gdl::SegmentBinary` with the settings tables, how the hosted
//! implementations are built, and what the segment serves. Each kind of
//! segment (one that serves JSON lines, one with a frontend, one with a
//! service of several entries, one that only serves) is one branch in one
//! place.

use std::fmt::Write;

use gdl_model::Role;

use super::{Hosted, MARKER, Unit};
use crate::service::{EntryHop, ServiceBinding};
use crate::spec::Bind;

impl Unit<'_> {
    pub(super) fn main_rs(&self) -> String {
        let hosted = self.hosted();
        let frontend = self.frontend();
        let service = self.service();
        let mut text = String::new();
        let _ = writeln!(text, "// {MARKER}.");
        let _ = writeln!(text, "// Regenerate with: {}", self.regenerate);
        let _ = writeln!(text);
        text.push_str(&self.settings_doc(&hosted, frontend, service, self.serves()));
        let _ = writeln!(text);
        let _ = writeln!(text, "fn main() -> std::process::ExitCode {{");
        let _ = writeln!(text, "    gdl::segment_main::<{}::Binary>()", self.lib());
        let _ = writeln!(text, "}}");
        text
    }

    /// The service whose entries this segment serves: only the segment
    /// hosting the source serves one.
    fn service(&self) -> Option<&ServiceBinding> {
        self.spec.service.as_ref().filter(|_| self.hosts_source())
    }

    /// The module doc: usage, the settings table, and what the binary does
    /// once every segment is ready.
    fn settings_doc(
        &self,
        hosted: &[Hosted<'_>],
        frontend: Option<&Bind>,
        service: Option<&ServiceBinding>,
        serves: bool,
    ) -> String {
        let package = self.package();
        let mut text = String::new();
        let _ = writeln!(text, "//! Runs segment `{}` on its own.", self.segment);
        let _ = writeln!(text, "//!");
        let _ = writeln!(
            text,
            "//! Usage: `{package} <config.toml> [override.toml ...]`. Files merge in argument"
        );
        let _ = writeln!(
            text,
            "//! order, then `GDL_` environment variables, with `__` between nested keys"
        );
        let _ = writeln!(
            text,
            "//! (`GDL_RUNTIME__ADMISSION_LIMIT=64`). `{package} --healthcheck <config.toml> ...`"
        );
        let _ = writeln!(
            text,
            "//! asks a running instance's health probe instead, as a container health check"
        );
        let _ = writeln!(text, "//! does. The settings are:");
        let _ = writeln!(text, "//!");
        let mut rows = vec![
            (
                "listen = \"127.0.0.1:0\"".to_string(),
                "this instance's address; under a hub, an unspecified one advertises what HOSTNAME resolves to"
                    .to_string(),
            ),
            (
                "roster = \"/run/roster\"".to_string(),
                "a directory every instance shares, fresh for each deployment".to_string(),
            ),
            (
                "# [hub]".to_string(),
                "or the discovery hub: exactly one of the two".to_string(),
            ),
            (
                r#"# host = "hub""#.to_string(),
                "its host; public_port, control_port, and heartbeat_ms (5000) are optional"
                    .to_string(),
            ),
            (
                "rendezvous_ms = 30000".to_string(),
                "optional: how long to wait for the other instances".to_string(),
            ),
            (
                "[instances]".to_string(),
                "optional, with a roster: instances per segment, 1 by default".to_string(),
            ),
            (
                "[min_replicas]".to_string(),
                "optional, with a hub: loaded instances per segment to wait for, 1 by default"
                    .to_string(),
            ),
            (
                "[runtime]".to_string(),
                "optional: gdl::RuntimeConfig".to_string(),
            ),
            (
                "[system]".to_string(),
                "optional: the live and health probes, off unless port is set (DYN_SYSTEM_PORT)"
                    .to_string(),
            ),
        ];
        for node in hosted {
            rows.push((
                format!("[node.{}]", node.name),
                format!("optional: {}'s Build::Config", node.name),
            ));
        }
        if frontend.is_some() {
            rows.push((
                "[frontend]".to_string(),
                "the source frontend's Build::Config".to_string(),
            ));
        }
        if let Some(service) = service {
            for node in service.nodes() {
                rows.push((
                    format!("[adapter.{}]", node.name),
                    format!("optional: entry adapter {}'s Build::Config", node.name),
                ));
            }
        }
        let width = rows.iter().map(|(key, _)| key.len()).max().unwrap_or(0);
        let _ = writeln!(text, "//! ```toml");
        for (key, comment) in rows {
            let _ = writeln!(text, "//! {key:<width$}  # {comment}");
        }
        let _ = writeln!(text, "//! ```");
        let _ = writeln!(
            text,
            "//! Graph-affecting values must match the built graph."
        );
        if serves {
            let _ = writeln!(text, "//!");
            let _ = writeln!(
                text,
                "//! This segment hosts the source. Once every segment is ready, it reads"
            );
            let _ = writeln!(
                text,
                "//! one JSON request per line on stdin and answers each with one line per"
            );
            let _ = writeln!(
                text,
                "//! item, `{{\"item\": ...}}`, then `{{\"done\": null}}` or `{{\"error\": \"...\"}}`."
            );
        } else if frontend.is_some() {
            let _ = writeln!(text, "//!");
            let _ = writeln!(
                text,
                "//! The source frontend serves once every segment is ready, and drains on"
            );
            let _ = writeln!(text, "//! SIGINT or SIGTERM.");
        } else if self.hosts_source() && self.streamed_source() {
            let _ = writeln!(text, "//!");
            let _ = writeln!(
                text,
                "//! This segment hosts the source, which takes a streamed request. One JSON"
            );
            let _ = writeln!(
                text,
                "//! line cannot carry it, so the binary only serves; call it through the library."
            );
        } else if self.hosts_source() {
            let _ = writeln!(text, "//!");
            let _ = writeln!(
                text,
                "//! This segment hosts the source, but the source's types are not serde"
            );
            let _ = writeln!(
                text,
                "//! types, so the binary only serves; call it through the library."
            );
        }
        text
    }

    /// The library's part of the binary: the settings types, what the
    /// binary builds when it hosts more than nodes, and `Binary`.
    pub(super) fn binary_rs(&self) -> String {
        let hosted = self.hosted();
        let frontend = self.frontend();
        let service = self.service();
        let mut text = settings_types(&hosted, frontend, service);
        let built = Built::new(&hosted, frontend, service);
        if built.extra() {
            let _ = writeln!(text);
            text.push_str(&hosts_type(frontend, service));
        }
        let _ = writeln!(text);
        let _ = writeln!(
            text,
            "/// What the `{}` binary hosts and serves; its `main` is",
            self.package()
        );
        let _ = writeln!(text, "/// `gdl::segment_main::<Binary>()`.");
        let _ = writeln!(text, "pub struct Binary;");
        let _ = writeln!(text);
        let _ = writeln!(text, "impl gdl::SegmentBinary for Binary {{");
        let _ = writeln!(
            text,
            "    const PACKAGE: &'static str = {:?};",
            self.package()
        );
        let _ = writeln!(text, "    const SEGMENT: &'static str = SEGMENT;");
        let _ = writeln!(
            text,
            "    const SEGMENTS: &'static [&'static str] = SEGMENTS;"
        );
        let graph_configs = toml::to_string(&self.spec.config).expect("graph config tables");
        let _ = writeln!(
            text,
            "    const GRAPH_CONFIG: &'static str = {graph_configs:?};"
        );
        let _ = writeln!(text, "    type Settings = Settings;");
        let _ = writeln!(text, "    type Hosts = {};", built.ty());
        let _ = writeln!(text);
        text.push_str(&self.build_fn(&hosted, frontend, service, &built));
        if !built.ready.is_empty() {
            let _ = writeln!(text);
            let _ = writeln!(
                text,
                "    async fn ready(hosts: &{}) -> gdl::Result<()> {{",
                built.ty()
            );
            let _ = writeln!(
                text,
                "        tokio::try_join!({})?;",
                built.ready.join(", ")
            );
            let _ = writeln!(text, "        Ok(())");
            let _ = writeln!(text, "    }}");
        }
        let _ = writeln!(text);
        let _ = writeln!(
            text,
            "    fn deployment() -> gdl::Result<gdl::DeploymentSpec> {{"
        );
        let _ = writeln!(
            text,
            "        Ok(gdl::DeploymentSpec::new(contract::manifest(), gdl::Placement::from_toml(PLACEMENT)?)?)"
        );
        let _ = writeln!(text, "    }}");
        let _ = writeln!(text);
        text.push_str(&self.start_fn(frontend, service, &built));
        let _ = writeln!(text, "}}");
        text
    }

    /// `build`: each hosted implementation from its settings table,
    /// before the segment meets its peers, so that a bad table fails
    /// before anyone waits for it.
    fn build_fn(
        &self,
        hosted: &[Hosted<'_>],
        frontend: Option<&Bind>,
        service: Option<&ServiceBinding>,
        built: &Built,
    ) -> String {
        let mut text = String::new();
        let takes = !hosted.is_empty() || frontend.is_some() || service.is_some();
        let _ = writeln!(
            text,
            "    async fn build(settings: Settings, {}: &gdl::Builder) -> Result<{}, gdl::Failure> {{",
            if takes { "builder" } else { "_builder" },
            built.ty()
        );
        let mut fields = vec!["node"];
        if frontend.is_some() {
            fields.push("frontend");
        }
        if service.is_some() {
            fields.push("mut adapter");
        }
        let _ = writeln!(
            text,
            "        let Settings {{ {} }} = settings;",
            fields.join(", ")
        );
        if hosted.is_empty() {
            let _ = writeln!(text, "        let NodeSettings {{}} = node;");
            let _ = writeln!(text, "        let nodes = Nodes {{}};");
        } else {
            let _ = writeln!(text, "        let nodes = Nodes {{");
            for node in hosted {
                let _ = writeln!(
                    text,
                    "            {}: builder.build::<::{}>(\"{}\", node.{}).await?,",
                    node.binding, node.ty, node.name, node.binding
                );
            }
            let _ = writeln!(text, "        }};");
        }
        if let Some(frontend) = frontend {
            let _ = writeln!(
                text,
                "        let frontend = builder.build::<::{}>(\"{}\", frontend).await?;",
                rooted(&frontend.ty),
                self.frontend_source().expect("the pinned source").name
            );
        }
        if let Some(service) = service {
            let enabled: Vec<_> = service.nodes().iter().map(|n| n.name.clone()).collect();
            let _ = writeln!(text, "        let enabled: &[&str] = &{enabled:?};");
            let _ = writeln!(
                text,
                "        if let Some(unknown) = adapter.keys().find(|key| !enabled.contains(&key.as_str())) {{ return Err(format!(\"[adapter.{{unknown}}]: no enabled entry node of that name\").into()); }}"
            );
            for node in service.nodes() {
                let _ = writeln!(
                    text,
                    "        let {} = builder.build::<::{}>({:?}, adapter.remove({:?})).await?;",
                    node.variable(),
                    rooted(&node.bind.ty),
                    node.name,
                    node.name
                );
            }
        }
        if built.extra() {
            let _ = writeln!(text, "        Ok(Hosts {{ {} }})", built.fields.join(", "));
        } else {
            let _ = writeln!(text, "        Ok(nodes)");
        }
        let _ = writeln!(text, "    }}");
        text
    }

    /// `start`: the segment's graph, and the body that serves it once
    /// every segment is ready.
    fn start_fn(
        &self,
        frontend: Option<&Bind>,
        service: Option<&ServiceBinding>,
        built: &Built,
    ) -> String {
        let serves = self.serves();
        let mut text = String::new();
        let _ = writeln!(
            text,
            "    fn start(hosts: {}, node: gdl::velo::Velo, roster: &gdl::Roster, runtime: gdl::RuntimeConfig) -> gdl::Result<gdl::Serving> {{",
            built.ty()
        );
        let nodes = if built.extra() {
            let _ = writeln!(
                text,
                "        let Hosts {{ {} }} = hosts;",
                built.fields.join(", ")
            );
            "nodes"
        } else {
            "hosts"
        };
        // The crate's own `start`: an entry node's variable cannot shadow
        // it, since each is prefixed.
        let _ = writeln!(
            text,
            "        let segment = crate::start(node, roster, runtime, {nodes})?;"
        );
        // The runtime keeps the segment serving through the drain; the body
        // shares it.
        let segment = if frontend.is_some() || serves {
            "segment"
        } else {
            "_segment"
        };
        let _ = writeln!(
            text,
            "        Ok(gdl::Serving::new(segment, |{segment}, shutdown| async move {{"
        );
        text.push_str(&self.serve_body(frontend, service, serves));
        let _ = writeln!(text, "        }}))");
        let _ = writeln!(text, "    }}");
        text
    }

    /// What the body does once every segment is ready: serve the frontend
    /// or the service, answer JSON lines, or wait to be stopped.
    fn serve_body(
        &self,
        frontend: Option<&Bind>,
        service: Option<&ServiceBinding>,
        serves: bool,
    ) -> String {
        let mut text = String::new();
        if frontend.is_some() {
            if let Some(service) = service {
                let roots: Vec<_> = self
                    .manifest
                    .hops
                    .iter()
                    .filter(|h| {
                        self.manifest
                            .node(&h.outer)
                            .is_some_and(|n| n.role == Role::Source)
                    })
                    .collect();
                // A root no entry reaches has no client: the variable would
                // be unused, and the crate built with warnings denied would
                // not build.
                let targeted: std::collections::BTreeSet<&str> = service
                    .entries
                    .iter()
                    .filter_map(|e| e.pipeline.as_ref())
                    .flat_map(EntryHop::targets)
                    .collect();
                for (index, root) in roots.iter().enumerate() {
                    if !targeted.contains(root.outer.as_str()) {
                        continue;
                    }
                    let selector = if roots.len() == 1 {
                        "roots".into()
                    } else {
                        format!("roots.{}0", "1.".repeat(index))
                    };
                    let variable = crate::service::pipeline_variable(&root.outer);
                    let _ = writeln!(
                        text,
                        "            let {variable} = segment.client(|roots| {selector}.clone());"
                    );
                }
                let _ = writeln!(
                    text,
                    "            let clients = ::{} {{",
                    rooted(&service.clients)
                );
                for entry in &service.entries {
                    if let Some(pipeline) = &entry.pipeline {
                        let _ = writeln!(
                            text,
                            "                {}: Some(gdl::Client::new(gdl::Source::new({}))),",
                            entry.field,
                            pipeline.expression()
                        );
                    } else {
                        let _ = writeln!(text, "                {}: None,", entry.field);
                    }
                }
                let _ = writeln!(text, "            }};");
                let _ = writeln!(
                    text,
                    "            gdl::Service::serve(frontend, clients, async move {{ shutdown.cancelled().await }}).await"
                );
            } else {
                let _ = writeln!(
                    text,
                    "            gdl::Frontend::serve(frontend, segment, async move {{ shutdown.cancelled().await }}).await"
                );
            }
        } else if serves {
            let _ = writeln!(
                text,
                "            let input = tokio::io::BufReader::new(tokio::io::stdin());"
            );
            let _ = writeln!(
                text,
                "            let call = |request: contract::SourceReq| segment.call::<_, contract::SourceItem>(gdl::Cx::new(), request);"
            );
            // The stop ends reading, not the answer being written; a
            // second signal ends the process.
            let _ = writeln!(
                text,
                "            gdl::serve_json_lines(input, tokio::io::stdout(), shutdown.cancelled(), call)"
            );
            let _ = writeln!(text, "                .await");
            let _ = writeln!(
                text,
                "                .map_err(|e| gdl::Error::new(gdl::ErrorKind::Transport, e.to_string()))"
            );
        } else {
            let _ = writeln!(text, "            shutdown.cancelled().await;");
            let _ = writeln!(text, "            Ok(())");
        }
        text
    }
}

/// A path from the crate root, written without its leading `::`: the
/// library's `contract` alias must not shadow a crate of that name.
fn rooted(path: &str) -> &str {
    path.trim().trim_start_matches("::")
}

/// What `build` returns: the nodes alone, or `Hosts` when the segment also
/// hosts the source's frontend or its entry adapters; and the readiness of
/// each.
struct Built {
    /// `Hosts`' fields, none when `build` returns the nodes alone.
    fields: Vec<String>,
    ready: Vec<String>,
}

impl Built {
    fn new(
        hosted: &[Hosted<'_>],
        frontend: Option<&Bind>,
        service: Option<&ServiceBinding>,
    ) -> Self {
        let mut fields = Vec::new();
        if frontend.is_some() || service.is_some() {
            fields.push("nodes".to_string());
            fields.extend(frontend.map(|_| "frontend".to_string()));
            fields.extend(service.iter().flat_map(|s| s.nodes()).map(|n| n.variable()));
        }
        let nodes = if fields.is_empty() {
            "hosts"
        } else {
            "hosts.nodes"
        };
        let mut ready: Vec<String> = hosted
            .iter()
            .map(|node| format!("gdl::Build::ready(&{nodes}.{})", node.binding))
            .collect();
        if frontend.is_some() {
            ready.push("gdl::Build::ready(&hosts.frontend)".into());
        }
        if let Some(service) = service {
            ready.extend(
                service
                    .nodes()
                    .iter()
                    .map(|node| format!("gdl::Build::ready(&hosts.{})", node.variable())),
            );
        }
        Self { fields, ready }
    }

    fn extra(&self) -> bool {
        !self.fields.is_empty()
    }

    fn ty(&self) -> &'static str {
        if self.extra() { "Hosts" } else { "Nodes" }
    }
}

/// `Settings` and `NodeSettings`: the tables this segment takes, and what
/// the runtime needs to know to address them from the environment.
fn settings_types(
    hosted: &[Hosted<'_>],
    frontend: Option<&Bind>,
    service: Option<&ServiceBinding>,
) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "/// What this segment's binary hosts, as its settings name it."
    );
    let _ = writeln!(text, "#[derive(serde::Deserialize)]");
    let _ = writeln!(text, "#[serde(deny_unknown_fields)]");
    let _ = writeln!(text, "pub struct Settings {{");
    let _ = writeln!(text, "    #[serde(default)]");
    let _ = writeln!(text, "    node: NodeSettings,");
    let mut keys = vec!["node"];
    if frontend.is_some() {
        let _ = writeln!(text, "    #[serde(default)]");
        let _ = writeln!(text, "    frontend: Option<toml::Table>,");
        keys.push("frontend");
    }
    if service.is_some() {
        let _ = writeln!(text, "    #[serde(default)]");
        let _ = writeln!(
            text,
            "    adapter: std::collections::BTreeMap<String, toml::Table>,"
        );
        keys.push("adapter");
    }
    let _ = writeln!(text, "}}");
    let _ = writeln!(text);
    let names: Vec<&str> = hosted
        .iter()
        .map(|node| node.name)
        .chain(
            service
                .iter()
                .flat_map(|s| s.nodes())
                .map(|node| node.name.as_str()),
        )
        .collect();
    let _ = writeln!(text, "impl gdl::Hosted for Settings {{");
    let _ = writeln!(text, "    const KEYS: &'static [&'static str] = &{keys:?};");
    let _ = writeln!(
        text,
        "    const NAMES: &'static [&'static str] = &{names:?};"
    );
    let _ = writeln!(text, "}}");
    let _ = writeln!(text);
    let _ = writeln!(text, "#[derive(Default, serde::Deserialize)]");
    let _ = writeln!(text, "#[serde(deny_unknown_fields)]");
    if hosted.is_empty() {
        let _ = writeln!(text, "struct NodeSettings {{}}");
    } else {
        let _ = writeln!(text, "struct NodeSettings {{");
        for node in hosted {
            let _ = writeln!(text, "    #[serde(rename = \"{}\", default)]", node.name);
            let _ = writeln!(text, "    {}: Option<toml::Table>,", node.binding);
        }
        let _ = writeln!(text, "}}");
    }
    text
}

/// `Hosts`: the nodes, and the source's frontend or entry adapters, which
/// the serving body takes rather than the graph.
fn hosts_type(frontend: Option<&Bind>, service: Option<&ServiceBinding>) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "/// What this segment's binary builds: its nodes, and what serves the source."
    );
    let _ = writeln!(text, "pub struct Hosts {{");
    let _ = writeln!(text, "    nodes: Nodes,");
    if let Some(frontend) = frontend {
        let _ = writeln!(text, "    frontend: ::{},", rooted(&frontend.ty));
    }
    if let Some(service) = service {
        for node in service.nodes() {
            let _ = writeln!(
                text,
                "    {}: ::{},",
                node.variable(),
                rooted(&node.bind.ty)
            );
        }
    }
    let _ = writeln!(text, "}}");
    text
}
