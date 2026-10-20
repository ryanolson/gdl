//! The binary of a segment crate: `main.rs`, which reads the settings,
//! builds what the segment hosts, and hands the rest to the runtime's
//! process module. Written in parts, each a function, so that each kind of
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
        let service = self.spec.service.as_ref().filter(|_| self.hosts_source());
        let serves = self.serves();
        let takes = !hosted.is_empty() || frontend.is_some() || service.is_some();
        let mut text = String::new();
        let _ = writeln!(text, "// {MARKER}.");
        let _ = writeln!(text, "// Regenerate with: {}", self.regenerate);
        let _ = writeln!(text);
        text.push_str(&self.settings_doc(&hosted, frontend, service, serves));
        let _ = writeln!(text);
        let _ = writeln!(
            text,
            "use {}::{{Nodes, SEGMENT, SEGMENTS, start}};",
            self.lib()
        );
        let _ = writeln!(text);
        let _ = writeln!(
            text,
            "type Failure = Box<dyn std::error::Error + Send + Sync>;"
        );
        let _ = writeln!(text);
        text.push_str(&hosted_types(&hosted, frontend, service));
        let _ = writeln!(text);
        let _ = writeln!(text, "fn main() -> std::process::ExitCode {{");
        let _ = writeln!(text, "    gdl::segment_main(SEGMENT, run())");
        let _ = writeln!(text, "}}");
        let _ = writeln!(text);
        text.push_str(&self.run_fn(&hosted, frontend, service, serves, takes));
        if takes {
            let _ = writeln!(text);
            text.push_str(&self.build_fn());
        }
        text
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

    /// `run`: the settings, the hosted implementations, the launch, and the
    /// body served until the process stops.
    fn run_fn(
        &self,
        hosted: &[Hosted<'_>],
        frontend: Option<&Bind>,
        service: Option<&ServiceBinding>,
        serves: bool,
        takes: bool,
    ) -> String {
        let package = self.package();
        let lib = self.lib();
        let mut text = String::new();
        let _ = writeln!(text, "async fn run() -> Result<(), Failure> {{");
        let _ = writeln!(
            text,
            "    let paths: Vec<_> = std::env::args_os().skip(1).collect();"
        );
        let _ = writeln!(
            text,
            "    if paths.is_empty() {{ return Err(\"usage: {package} [--healthcheck] <config.toml> [override.toml ...]\".into()); }}"
        );
        let _ = writeln!(
            text,
            "    if paths[0] == \"--healthcheck\" {{ if paths.len() == 1 {{ return Err(\"usage: {package} --healthcheck <config.toml> [override.toml ...]\".into()); }} gdl::ProcessConfig::<Hosted>::load(&paths[1..])?.healthcheck().await?; return Ok(()); }}"
        );
        let _ = writeln!(
            text,
            "    let {}config = gdl::ProcessConfig::<Hosted>::load(&paths)?;",
            if takes { "mut " } else { "" }
        );
        // First: a stop while the nodes build is taken, and the probes say
        // the process is live while it starts.
        let _ = writeln!(
            text,
            "    let process = gdl::Process::start(&config).await?;"
        );
        text.push_str(&builds(hosted, frontend, service, self.frontend_source()));
        let _ = writeln!(
            text,
            "    let deployment = gdl::DeploymentSpec::new({lib}::contract::manifest(), gdl::Placement::from_toml({lib}::PLACEMENT)?)?;"
        );
        let _ = writeln!(text, "    let launch = config");
        let _ = writeln!(
            text,
            "        .launch(&process, SEGMENT, SEGMENTS, &deployment, {})",
            ready_expression(hosted, frontend, service)
        );
        let _ = writeln!(text, "        .await?;");
        let _ = writeln!(
            text,
            "    let segment = start(launch.node(), launch.roster(), config.runtime, nodes)?;"
        );
        let _ = writeln!(text, "    launch");
        // The runtime keeps the segment serving through the drain; the body
        // shares it.
        let segment = if frontend.is_some() || serves {
            "segment"
        } else {
            "_segment"
        };
        let _ = writeln!(
            text,
            "        .serve(segment, |{segment}, shutdown| async move {{"
        );
        text.push_str(&self.serve_body(frontend, service, serves));
        let _ = writeln!(text, "        }})");
        let _ = writeln!(text, "        .await?;");
        let _ = writeln!(text, "    Ok(())");
        let _ = writeln!(text, "}}");
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
        let lib = self.lib();
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
                // be unused, and the binary built with warnings denied would
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
                let _ = writeln!(text, "            let clients = {} {{", service.clients);
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
                "            let call = |request: {lib}::contract::SourceReq| segment.call::<_, {lib}::contract::SourceItem>(gdl::Cx::new(), request);"
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

    /// `build`: one implementation from its settings table, with the
    /// graph's own values filled in and checked.
    fn build_fn(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "/// Build one implementation from its config table.");
        let _ = writeln!(
            text,
            "async fn build<T: gdl::Build>(node: &str, config: Option<toml::Table>) -> Result<T, Failure> {{"
        );
        let graph_configs = toml::to_string(&self.spec.config).expect("graph config tables");
        let _ = writeln!(
            text,
            "    let contracts: std::collections::BTreeMap<String, gdl::GraphConfig> = toml::from_str({graph_configs:?})?;"
        );
        let _ = writeln!(
            text,
            "    let graph = contracts.get(node).cloned().unwrap_or_default();"
        );
        let _ = writeln!(
            text,
            "    let providers = gdl::figment::Figment::from(gdl::figment::providers::Serialized::defaults(config.unwrap_or_default()));"
        );
        let _ = writeln!(text, "    gdl::build_node::<T>(&providers, &graph)");
        let _ = writeln!(text, "        .await");
        let _ = writeln!(
            text,
            "        .map_err(|e| format!(\"building {{node}}: {{e}}\").into())"
        );
        let _ = writeln!(text, "}}");
        text
    }
}

/// `Hosted` and `NodeConfigs`: the settings tables this segment takes, and
/// what the runtime needs to know to address them from the environment.
fn hosted_types(
    hosted: &[Hosted<'_>],
    frontend: Option<&Bind>,
    service: Option<&ServiceBinding>,
) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "/// What this segment hosts, as its settings name it."
    );
    let _ = writeln!(text, "#[derive(serde::Deserialize)]");
    let _ = writeln!(text, "#[serde(deny_unknown_fields)]");
    let _ = writeln!(text, "struct Hosted {{");
    let _ = writeln!(text, "    #[serde(default)]");
    let _ = writeln!(text, "    node: NodeConfigs,");
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
    let _ = writeln!(text, "impl gdl::Hosted for Hosted {{");
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
        let _ = writeln!(text, "struct NodeConfigs {{}}");
    } else {
        let _ = writeln!(text, "struct NodeConfigs {{");
        for node in hosted {
            let _ = writeln!(text, "    #[serde(rename = \"{}\", default)]", node.name);
            let _ = writeln!(text, "    {}: Option<toml::Table>,", node.binding);
        }
        let _ = writeln!(text, "}}");
    }
    text
}

/// The hosted implementations, built from their tables before the segment
/// meets its peers, so that a bad table fails before anyone waits for it.
fn builds(
    hosted: &[Hosted<'_>],
    frontend: Option<&Bind>,
    service: Option<&ServiceBinding>,
    source: Option<&gdl_model::NodeInfo>,
) -> String {
    let mut text = String::new();
    if hosted.is_empty() {
        let _ = writeln!(text, "    let NodeConfigs {{}} = config.hosted.node;");
        let _ = writeln!(text, "    let nodes = Nodes {{}};");
    } else {
        let _ = writeln!(text, "    let nodes = Nodes {{");
        for node in hosted {
            let _ = writeln!(
                text,
                "        {}: build::<::{}>(\"{}\", config.hosted.node.{}.take()).await?,",
                node.binding, node.ty, node.name, node.binding
            );
        }
        let _ = writeln!(text, "    }};");
    }
    if let Some(frontend) = frontend {
        let _ = writeln!(
            text,
            "    let frontend = build::<{}>(\"{}\", config.hosted.frontend.take()).await?;",
            frontend.ty,
            source.expect("the pinned source").name
        );
    }
    if let Some(service) = service {
        let enabled: Vec<_> = service.nodes().iter().map(|n| n.name.clone()).collect();
        let _ = writeln!(text, "    let enabled: &[&str] = &{enabled:?};");
        let _ = writeln!(
            text,
            "    if let Some(unknown) = config.hosted.adapter.keys().find(|key| !enabled.contains(&key.as_str())) {{ return Err(format!(\"[adapter.{{unknown}}]: no enabled entry node of that name\").into()); }}"
        );
        for node in service.nodes() {
            let _ = writeln!(
                text,
                "    let {} = build::<{}>({:?}, config.hosted.adapter.remove({:?})).await?;",
                node.variable(),
                node.bind.ty,
                node.name,
                node.name
            );
        }
    }
    text
}

/// Every hosted implementation says when it can take work; the segment
/// announces itself as loaded only then.
fn ready_expression(
    hosted: &[Hosted<'_>],
    frontend: Option<&Bind>,
    service: Option<&ServiceBinding>,
) -> String {
    let mut ready: Vec<String> = hosted
        .iter()
        .map(|node| format!("gdl::Build::ready(&nodes.{})", node.binding))
        .collect();
    if frontend.is_some() {
        ready.push("gdl::Build::ready(&frontend)".into());
    }
    if let Some(service) = service {
        ready.extend(
            service
                .nodes()
                .iter()
                .map(|node| format!("gdl::Build::ready(&{})", node.variable())),
        );
    }
    if ready.is_empty() {
        "async { Ok(()) }".to_string()
    } else {
        format!(
            "async {{ tokio::try_join!({})?; Ok(()) }}",
            ready.join(", ")
        )
    }
}
