//! A segment crate's binary. Its lifecycle is the runtime's
//! (`gdl_runtime::segment_main`); what is emitted is what differs between
//! segments: `main.rs`, which names the segment's `Binary` and documents
//! its settings, and the library's `Binary`, which implements
//! `gdl_runtime::SegmentBinary` with the settings tables, how the hosted
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
        // Here and not in the library: a program that links the library
        // and sets its own allocator would fail to link with two.
        if let Some(allocator) = self.spec.allocator.source() {
            let _ = writeln!(
                text,
                "// The global allocator that the spec's `[binary] allocator` chooses."
            );
            let _ = writeln!(text, "#[global_allocator]");
            let _ = writeln!(text, "static GLOBAL: {ty} = {ty};", ty = allocator.ty);
            let _ = writeln!(text);
        }
        let _ = writeln!(text, "fn main() -> std::process::ExitCode {{");
        // The binary, not the runtime, installs the subscriber: a program
        // that deploys the segment through the library keeps its own. On
        // stderr, since a segment that serves JSON lines answers on stdout.
        // Colour off, though another crate may turn on the `ansi` feature:
        // escape codes are noise in a container's log.
        text.push_str(concat!(
            "    let filter = tracing_subscriber::EnvFilter::builder()\n",
            "        .with_default_directive(tracing_subscriber::filter::LevelFilter::WARN.into())\n",
            "        .from_env_lossy();\n",
            "    tracing_subscriber::fmt()\n",
            "        .with_env_filter(filter)\n",
            "        .with_writer(std::io::stderr)\n",
            "        .with_ansi(false)\n",
            "        .init();\n",
        ));
        let _ = writeln!(
            text,
            "    gdl_runtime::segment_main::<{}::Binary>()",
            self.lib()
        );
        let _ = writeln!(text, "}}");
        text
    }

    /// The service whose entries this segment serves: only the segment
    /// hosting the source serves one.
    fn service(&self) -> Option<&ServiceBinding> {
        self.spec.service.as_ref().filter(|_| self.hosts_source())
    }

    /// The module doc: usage, the settings table, and what the binary does
    /// once the segments it waits for are ready.
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
        let _ = writeln!(
            text,
            "//! does. Log events go to stderr, at the level `RUST_LOG` sets (`warn` by default)."
        );
        let _ = writeln!(text, "//! The settings are:");
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
                "optional: gdl_runtime::RuntimeConfig".to_string(),
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
        for (name, _) in &self.policies {
            rows.push((
                format!("[cell_policy.{name}]"),
                format!("optional: cell policy {name}'s Build::Config"),
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
                "//! This segment hosts the source. Once its segments are ready, it reads"
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
                "//! The source frontend serves once its segments are ready, and drains on"
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

    /// The call's `serve`: what the binary serves once the segments it
    /// waits for are ready. The frontend or the service, when the segment
    /// hosts the pinned source; else JSON lines on stdin, when the source
    /// takes them; else nothing.
    pub(super) fn serve(&self) -> String {
        let Some(frontend) = self.frontend() else {
            return match self.serves() {
                true => "    serve: json_lines;\n".to_string(),
                false => "    serve: none;\n".to_string(),
            };
        };
        let source = &self.frontend_source().expect("the pinned source").name;
        let ty = rooted(&frontend.ty);
        let Some(service) = self.service() else {
            return format!("    serve: frontend {source} = ::{ty};\n");
        };
        let mut text = String::new();
        let _ = writeln!(text, "    serve: service {{");
        let _ = writeln!(text, "        frontend: {source} = ::{ty};");
        let _ = writeln!(text, "        clients: ::{};", rooted(&service.clients));
        let adapters: Vec<String> = service
            .nodes()
            .iter()
            .map(|node| {
                format!(
                    "{:?} {} = ::{};",
                    node.name,
                    node.variable(),
                    rooted(&node.bind.ty)
                )
            })
            .collect();
        nested(&mut text, "adapters", &adapters);
        // A root no entry reaches has no client: the variable would be
        // unused, and the crate built with warnings denied would not build.
        let targeted: std::collections::BTreeSet<&str> = service
            .entries
            .iter()
            .filter_map(|e| e.pipeline.as_ref())
            .flat_map(EntryHop::targets)
            .collect();
        let pipelines: Vec<String> = self
            .manifest
            .hops
            .iter()
            .filter(|h| {
                self.manifest
                    .node(&h.outer)
                    .is_some_and(|n| n.role == Role::Source)
            })
            .enumerate()
            .filter(|(_, root)| targeted.contains(root.outer.as_str()))
            .map(|(index, root)| {
                format!(
                    "{} = {index};",
                    crate::service::pipeline_variable(&root.outer)
                )
            })
            .collect();
        nested(&mut text, "pipelines", &pipelines);
        let entries: Vec<String> = service
            .entries
            .iter()
            .map(|entry| match &entry.pipeline {
                Some(pipeline) => format!("{} = {};", entry.field, pipeline.expression()),
                None => format!("{};", entry.field),
            })
            .collect();
        nested(&mut text, "entries", &entries);
        let _ = writeln!(text, "    }};");
        text
    }
}

/// A list inside `serve: service { ... }`, one item to a line.
fn nested(text: &mut String, key: &str, items: &[String]) {
    if items.is_empty() {
        let _ = writeln!(text, "        {key}: [];");
        return;
    }
    let _ = writeln!(text, "        {key}: [");
    for item in items {
        let _ = writeln!(text, "            {item}");
    }
    let _ = writeln!(text, "        ];");
}

/// A path from the crate root, written without its leading `::`: the
/// library's `contract` alias must not shadow a crate of that name.
pub(super) fn rooted(path: &str) -> &str {
    path.trim().trim_start_matches("::")
}
