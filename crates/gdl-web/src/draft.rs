//! A draft: the graph the page builds, as JSON, and the graph file and
//! spec it becomes. The server writes both texts; the page never does, so
//! one writer decides what is generated, and the user sees the text that
//! is exported.

use std::collections::{BTreeMap, BTreeSet};

use gdl_extract::{Catalog, Entry, NodeEntry, Origin, PortEntry};
use gdl_model::Role;
use serde::Deserialize;

/// The graph the page builds.
#[derive(Debug, Clone, Deserialize)]
pub struct Draft {
    /// Container images, published ports and runtime configuration.
    #[serde(default)]
    pub deployment: toml::Table,
    /// Runtime TOML entered per segment in the Deploy panel.
    #[serde(default)]
    pub deployment_config: BTreeMap<String, String>,
    /// Built-in identity nodes, keyed by their graph name.
    #[serde(default)]
    pub identities: BTreeMap<String, Identity>,
    /// Adapter instances for the enabled entries of a hosted service.
    #[serde(default)]
    pub entries: BTreeMap<String, EntryPipeline>,
    /// The graph's name.
    pub name: String,
    /// The graph's entry point, optionally bound through `nodes` to a frontend.
    pub source: Source,
    /// The graph's other nodes, each an instance of a catalogue node.
    pub nodes: Vec<Instance>,
    /// The source's hop.
    pub hop: Hop,
    /// Additional independently wired pipeline roots.
    #[serde(default)]
    pub pipelines: Vec<Pipeline>,
    /// Display names keyed by pipeline root.
    #[serde(default)]
    pub pipeline_names: BTreeMap<String, String>,
    /// Request-boundary target nodes and their replica selection policies.
    #[serde(default)]
    pub routing: BTreeMap<String, gdl_model::RequestRouter>,
    /// Segment name to the nodes it hosts.
    pub segments: BTreeMap<String, Vec<String>>,
}

/// A built-in identity node or operator with explicit port types.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
pub enum Identity {
    Forward { request: Port },
    Backward { response: Origin },
    Operator { request: Port, response: Origin },
}

/// A named pipeline with its own input/output contract and topology.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    /// The pipeline's entry contract and unique root name.
    pub source: Source,
    /// Its implementation.
    pub hop: Hop,
}

/// An entry arm with an explicit pipeline destination or a nested operator arm.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryPipeline {
    /// Request transforms in order.
    #[serde(default)]
    pub forward: Vec<Instance>,
    /// Optional operator before the destination pipeline.
    pub operator: Option<Instance>,
    /// Explicit destination root; present only at a leaf.
    pub target: Option<String>,
    /// The operator's downstream arm.
    pub nested: Option<Box<EntryPipeline>>,
    /// Response transforms in order from downstream to upstream.
    #[serde(default)]
    pub backward: Vec<Instance>,
}

/// The source: its name, the request it takes, and the item its response
/// streams.
#[derive(Debug, Clone, Deserialize)]
pub struct Source {
    /// Its name in the graph.
    pub name: String,
    /// The request; a streamed request has an item.
    pub request: Port,
    /// The response's item.
    pub response: Origin,
}

/// A request position: a message, or a streamed request's head (`None`
/// for `()`) and item.
#[derive(Debug, Clone, Deserialize)]
pub struct Port {
    /// The message, or the head.
    #[serde(default)]
    pub head: Option<Origin>,
    /// A streamed request's item.
    #[serde(default)]
    pub item: Option<Origin>,
}

/// A graph node: its name, and the catalogue node it is.
#[derive(Debug, Clone, Deserialize)]
pub struct Instance {
    /// Overrides for explicitly marked graph configuration fields.
    #[serde(default)]
    pub config: gdl_model::GraphConfig,
    /// Its name in the graph.
    pub name: String,
    /// The catalogue entry, `name@version`.
    pub entry: String,
    /// The node's name in that entry.
    pub node: String,
}

/// A hop: forward nodes, the inner end, and backward nodes in the order
/// the response passes them. An operator at the inner end owns `nested`.
#[derive(Debug, Clone, Deserialize)]
pub struct Hop {
    /// Forward nodes, in request order.
    #[serde(default)]
    pub forward: Vec<String>,
    /// The sink or operator.
    pub inner: String,
    /// The operator's own hop.
    #[serde(default)]
    pub nested: Option<Box<Hop>>,
    /// Backward nodes, in response order.
    #[serde(default)]
    pub backward: Vec<String>,
}

fn entry_pipeline<'a>(
    pipeline: &EntryPipeline,
    prefix: &str,
    entries: &BTreeMap<String, &'a Entry>,
    packages: &mut BTreeMap<String, &'a Entry>,
    configs: &mut toml::Table,
    problems: &mut Vec<String>,
) -> toml::Table {
    let mut resolve = |instance: &Instance, name: String| -> toml::Value {
        let found = entries.get(&instance.entry).and_then(|entry| {
            entry
                .offer
                .nodes
                .iter()
                .find(|n| n.name == instance.node)
                .map(|node| (*entry, node))
        });
        let Some((entry, node)) = found else {
            problems.push(format!("entry node `{name}` is absent from the catalog"));
            return "".into();
        };
        packages.insert(entry.key(), entry);
        match gdl_model::resolve_config(&node.config, &instance.config) {
            Ok(config) => {
                configs.insert(name, toml::Value::try_from(config).expect("graph config"));
            }
            Err(e) => problems.push(format!("entry node `{name}`: {e}")),
        }
        node.ty.clone().into()
    };
    let mut result = toml::Table::new();
    result.insert(
        "forward".into(),
        pipeline
            .forward
            .iter()
            .enumerate()
            .map(|(i, n)| resolve(n, format!("{prefix}_f{i}")))
            .collect::<Vec<_>>()
            .into(),
    );
    result.insert(
        "backward".into(),
        pipeline
            .backward
            .iter()
            .enumerate()
            .map(|(i, n)| resolve(n, format!("{prefix}_b{i}")))
            .collect::<Vec<_>>()
            .into(),
    );
    if let Some(node) = &pipeline.operator {
        result.insert("operator".into(), resolve(node, format!("{prefix}_op")));
    }
    if let Some(target) = &pipeline.target {
        result.insert("target".into(), target.clone().into());
    }
    if let Some(nested) = &pipeline.nested {
        result.insert(
            "nested".into(),
            entry_pipeline(
                nested,
                &format!("{prefix}_next"),
                entries,
                packages,
                configs,
                problems,
            )
            .into(),
        );
    }
    result
}

/// Where generated crates take gdl from: the server's setting.
#[derive(Debug, Clone)]
pub enum Gdl {
    /// A directory.
    Path(std::path::PathBuf),
    /// A git repository at one revision.
    Git {
        /// The repository.
        url: String,
        /// The revision.
        rev: String,
    },
}

/// A draft rendered: the graph file and the spec that names it as
/// `graph.gdl`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    /// The graph file.
    pub graph: String,
    /// The spec.
    pub spec: String,
}

impl Draft {
    /// The graph file and spec of this draft, every message and node taken
    /// from `catalog`. Each problem is reported.
    pub fn render(&self, catalog: &Catalog, gdl: &Gdl) -> Result<Rendered, Vec<String>> {
        let mut problems = Vec::new();
        let entries: BTreeMap<String, &Entry> = catalog.entries().map(|e| (e.key(), e)).collect();

        // Each instance's catalogue node.
        let mut nodes: BTreeMap<&str, (&Entry, &NodeEntry)> = BTreeMap::new();
        for instance in &self.nodes {
            let found = entries.get(&instance.entry).and_then(|entry| {
                let node = entry.offer.nodes.iter().find(|n| n.name == instance.node)?;
                Some((*entry, node))
            });
            match found {
                Some(found) => {
                    nodes.insert(&instance.name, found);
                }
                None => problems.push(format!(
                    "node `{}`: the catalogue has no node `{}` in `{}`",
                    instance.name, instance.node, instance.entry
                )),
            }
        }

        // Every message the graph carries, each named once.
        let mut used: BTreeSet<&Origin> = BTreeSet::new();
        used.extend(self.source.request.head.iter());
        used.extend(self.source.request.item.iter());
        used.insert(&self.source.response);
        for pipeline in &self.pipelines {
            used.extend(pipeline.source.request.head.iter());
            used.extend(pipeline.source.request.item.iter());
            used.insert(&pipeline.source.response);
        }
        for identity in self.identities.values() {
            match identity {
                Identity::Forward { request } | Identity::Operator { request, .. } => {
                    used.extend(request.head.iter());
                    used.extend(request.item.iter());
                }
                _ => {}
            }
            match identity {
                Identity::Backward { response } | Identity::Operator { response, .. } => {
                    used.insert(response);
                }
                _ => {}
            }
        }
        for (_, node) in nodes.values() {
            for port in &node.ports {
                used.extend(port.head.iter().chain(&port.item));
            }
        }
        let names = Names::new(&used, &entries, &mut problems);
        if !problems.is_empty() {
            return Err(problems);
        }

        let graph = self.graph_text(&nodes, &names);
        let spec = self.spec_text(&nodes, &names, &entries, gdl, &mut problems);
        if problems.is_empty() {
            Ok(Rendered { graph, spec })
        } else {
            Err(problems)
        }
    }

    fn graph_text(&self, nodes: &BTreeMap<&str, (&Entry, &NodeEntry)>, names: &Names) -> String {
        let mut text = format!("name = {:?}, schema = 1;\n", self.name);
        let source = &self.source.request;
        text.push_str(&format!(
            "source {}: {} -> stream {};\n",
            self.source.name,
            request(names, source.head.as_ref(), source.item.as_ref()),
            names.name(&self.source.response)
        ));
        for instance in &self.nodes {
            let Some((_, node)) = nodes.get(instance.name.as_str()) else {
                continue;
            };
            let port = |name: &str| node.ports.iter().find(|p| p.port == name);
            let req = |name: &str| {
                port(name).map_or("()".to_string(), |p: &PortEntry| {
                    request(names, p.head.as_ref(), p.item.as_ref())
                })
            };
            let item = |name: &str| {
                port(name)
                    .and_then(|p| p.head.as_ref())
                    .map_or("()".to_string(), |o| names.name(o))
            };
            let signature = match node.role {
                Role::Sink => format!("{} -> stream {}", req("Req"), item("Item")),
                Role::Forward => format!("{} -> {}", req("In"), req("Out")),
                Role::Backward => format!("{} -> {}", item("In"), item("Out")),
                Role::Operator => format!(
                    "({} -> stream {}) over ({} -> stream {})",
                    req("UpReq"),
                    item("UpItem"),
                    req("DownReq"),
                    item("DownItem")
                ),
                // The source signature was emitted above.
                Role::Source => continue,
            };
            text.push_str(&format!("{} {}: {signature};\n", node.role, instance.name));
        }
        for pipeline in &self.pipelines {
            text.push_str(&format!(
                "source {}: {} -> stream {};\n",
                pipeline.source.name,
                request(
                    names,
                    pipeline.source.request.head.as_ref(),
                    pipeline.source.request.item.as_ref()
                ),
                names.name(&pipeline.source.response)
            ));
        }
        for (name, identity) in &self.identities {
            let signature = match identity {
                Identity::Forward { request: port } => format!(
                    "request {}",
                    request(names, port.head.as_ref(), port.item.as_ref())
                ),
                Identity::Backward { response } => format!("response {}", names.name(response)),
                Identity::Operator {
                    request: port,
                    response,
                } => format!(
                    "{} -> stream {}",
                    request(names, port.head.as_ref(), port.item.as_ref()),
                    names.name(response)
                ),
            };
            text.push_str(&format!("identity {name}: {signature};\n"));
        }
        text.push_str(&format!(
            "topology: {} => {};\n",
            self.source.name,
            topology(&self.hop)
        ));
        for pipeline in &self.pipelines {
            text.push_str(&format!(
                "topology: {} => {};\n",
                pipeline.source.name,
                topology(&pipeline.hop)
            ));
        }
        text
    }

    fn spec_text(
        &self,
        nodes: &BTreeMap<&str, (&Entry, &NodeEntry)>,
        names: &Names,
        entries: &BTreeMap<String, &Entry>,
        gdl: &Gdl,
        problems: &mut Vec<String>,
    ) -> String {
        let mut spec = toml::Table::new();
        if !self.pipeline_names.is_empty() {
            spec.insert(
                "pipeline_names".into(),
                toml::Value::try_from(&self.pipeline_names).expect("pipeline names"),
            );
        }
        let mut configs = toml::Table::new();
        for instance in &self.nodes {
            if let Some((_, node)) = nodes.get(instance.name.as_str()) {
                match gdl_model::resolve_config(&node.config, &instance.config) {
                    Ok(config) if !config.is_empty() => {
                        configs.insert(
                            instance.name.clone(),
                            toml::Value::try_from(config).expect("scalar graph values"),
                        );
                    }
                    Ok(_) => {}
                    Err(error) => problems.push(format!("node `{}`: {error}", instance.name)),
                }
            }
        }
        if !configs.is_empty() {
            spec.insert("config".into(), configs.into());
        }
        let mut graph = toml::Table::new();
        graph.insert("path".into(), "graph.gdl".into());
        spec.insert("graph".into(), graph.into());

        let mut messages = toml::Table::new();
        let crates: Vec<&Entry> = names.crates.iter().map(|key| entries[key]).collect();
        messages.insert("crates".into(), crate_list(&crates, problems).into());
        if !names.pins.is_empty() {
            let pins: toml::Table = names
                .pins
                .iter()
                .map(|(n, p)| (n.clone(), toml::Value::from(p.clone())))
                .collect();
            messages.insert("pin".into(), pins.into());
        }
        spec.insert("messages".into(), messages.into());

        let mut node_crates: BTreeMap<String, &Entry> = BTreeMap::new();
        let mut adapters = toml::Table::new();
        for (name, pipeline) in &self.entries {
            let mut configs = toml::Table::new();
            let value = entry_pipeline(
                pipeline,
                &format!("{}_{}", self.source.name, name),
                entries,
                &mut node_crates,
                &mut configs,
                problems,
            );
            adapters.insert(name.clone(), value.into());
            spec.entry("config")
                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                .as_table_mut()
                .expect("config table")
                .extend(configs);
        }
        if !adapters.is_empty() {
            spec.insert("entry".into(), adapters.into());
        }
        for (entry, _) in nodes.values() {
            node_crates.insert(entry.key(), entry);
        }
        let mut pins = toml::Table::new();
        for (name, (_, node)) in nodes {
            // A pin binds a node whose graph name differs from its own, and
            // one that two listed crates both offer.
            let offered = node_crates
                .values()
                .filter(|e| e.offer.nodes.iter().any(|n| n.name == node.name))
                .count();
            if node.role == Role::Source || node.name != *name || offered > 1 {
                pins.insert((*name).to_string(), node.ty.clone().into());
            }
        }
        let node_crates: Vec<&Entry> = node_crates.into_values().collect();
        let mut node_table = toml::Table::new();
        node_table.insert("crates".into(), crate_list(&node_crates, problems).into());
        if !pins.is_empty() {
            node_table.insert("pin".into(), pins.into());
        }
        spec.insert("nodes".into(), node_table.into());

        let mut gdl_table = toml::Table::new();
        match gdl {
            Gdl::Path(path) => {
                gdl_table.insert("path".into(), path.display().to_string().into());
            }
            Gdl::Git { url, rev } => {
                gdl_table.insert("git".into(), url.clone().into());
                gdl_table.insert("rev".into(), rev.clone().into());
            }
        }
        spec.insert("gdl".into(), gdl_table.into());

        let mut segments = toml::Table::new();
        for (segment, members) in &self.segments {
            let mut table = toml::Table::new();
            table.insert(
                "nodes".into(),
                members
                    .iter()
                    .map(|n| toml::Value::from(n.clone()))
                    .collect::<Vec<_>>()
                    .into(),
            );
            segments.insert(segment.clone(), table.into());
        }
        spec.insert("segment".into(), segments.into());
        if !self.routing.is_empty() {
            spec.insert(
                "routing".into(),
                toml::Value::try_from(&self.routing).expect("router policies are TOML strings"),
            );
        }
        let mut deployment = self.deployment.clone();
        for (name, text) in &self.deployment_config {
            if !self.segments.contains_key(name) {
                problems.push(format!("runtime config names unknown segment `{name}`"));
                continue;
            }
            match toml::from_str::<toml::Table>(text) {
                Ok(config) => {
                    let segment = deployment
                        .entry("segment")
                        .or_insert_with(|| toml::Table::new().into());
                    if let Some(segment) = segment.as_table_mut() {
                        let settings = segment
                            .entry(name.clone())
                            .or_insert_with(|| toml::Table::new().into());
                        if let Some(settings) = settings.as_table_mut() {
                            settings.insert("config".into(), config.into());
                        } else {
                            problems.push(format!("deployment segment `{name}` must be a table"));
                        }
                    } else {
                        problems.push("deployment.segment must be a table".into());
                    }
                }
                Err(error) => problems.push(format!("runtime config for `{name}`: {error}")),
            }
        }
        spec.insert("deployment".into(), deployment.into());
        toml::to_string(&spec).expect("a spec is representable in TOML")
    }
}

/// How each carried message is named in the graph file, the crates the
/// spec lists for them, and the pins their names need.
struct Names {
    names: BTreeMap<Origin, String>,
    /// Entry keys of the crates that offer each message at a public path:
    /// its own definition, its own export, or another crate's export.
    crates: BTreeSet<String>,
    /// Graph name to public path, where a name alone would not resolve to
    /// the message.
    pins: BTreeMap<String, String>,
}

impl Names {
    fn new(
        used: &BTreeSet<&Origin>,
        entries: &BTreeMap<String, &Entry>,
        problems: &mut Vec<String>,
    ) -> Self {
        let mut names = Self {
            names: BTreeMap::new(),
            crates: BTreeSet::new(),
            pins: BTreeMap::new(),
        };
        // Each message: its name where it is defined, and the crate that
        // offers it at a public path (its own, or one that exports it, when
        // its own defines it in a private module). That crate is listed.
        let mut defined: Vec<(&Origin, &str)> = Vec::new();
        let mut public: BTreeMap<&Origin, &str> = BTreeMap::new();
        for origin in used {
            let key = format!("{}@{}", origin.package, origin.version);
            let Some(own) = entries
                .get(&key)
                .filter(|e| e.offer.source == origin.source)
            else {
                // The directory it was read from, at another version: the
                // crates read against the old one name a message that went.
                let now = entries.values().find(|e| {
                    e.offer.source == origin.source && origin.source.starts_with("path+")
                });
                problems.push(match now {
                    Some(now) => format!(
                        "message `{}`: it was read from {key}, and the catalogue now holds that directory as {}: add again the crates that use it",
                        origin.path,
                        now.key()
                    ),
                    None => format!(
                        "message `{}`: its crate, {key}, is not in the catalogue: add it first",
                        origin.path
                    ),
                });
                continue;
            };
            let Some(message) = own.offer.messages.iter().find(|m| m.path == origin.path) else {
                problems.push(format!(
                    "message `{}`: {key} does not define it",
                    origin.path
                ));
                continue;
            };
            let offered_at = if message.public {
                Some((key.clone(), message.path.as_str()))
            } else {
                public_export(own, origin)
                    .map(|e| (key.clone(), e.path.as_str()))
                    .or_else(|| {
                        entries.iter().find_map(|(key, entry)| {
                            public_export(entry, origin).map(|e| (key.clone(), e.path.as_str()))
                        })
                    })
            };
            match offered_at {
                Some((listed, path)) => {
                    names.crates.insert(listed);
                    public.insert(origin, path);
                    defined.push((origin, &message.name));
                }
                None => problems.push(format!(
                    "message `{}` has no public path in the catalogue's crates: export it",
                    origin.path
                )),
            }
        }
        // What the listed crates offer by each name: the messages a name
        // could resolve to.
        let mut offered: BTreeMap<&str, BTreeSet<Origin>> = BTreeMap::new();
        for key in &names.crates {
            let offer = &entries[key].offer;
            for m in &offer.messages {
                offered.entry(&m.name).or_default().insert(Origin {
                    package: offer.package.clone(),
                    version: offer.version.clone(),
                    source: offer.source.clone(),
                    path: m.path.clone(),
                });
            }
            for e in &offer.exports {
                offered.entry(&e.name).or_default().insert(e.origin.clone());
            }
        }
        for (origin, name) in defined {
            // A second message of one name takes a name of its own.
            let mut graph_name = name.to_string();
            let mut n = 2;
            while names.names.values().any(|taken| *taken == graph_name) {
                graph_name = format!("{name}{n}");
                n += 1;
            }
            // Pinned where the name alone would not resolve to it.
            let ambiguous = offered.get(graph_name.as_str()).is_none_or(|o| o.len() > 1);
            if graph_name != name || ambiguous {
                names
                    .pins
                    .insert(graph_name.clone(), public[origin].to_string());
            }
            names.names.insert(origin.clone(), graph_name);
        }
        names
    }

    /// The message's name as the graph file spells it: raw where it is a
    /// keyword (`r#match`), which the file reads back as the name.
    fn name(&self, origin: &Origin) -> String {
        gdl_syntax::identifier(&self.names[origin])
    }
}

/// `entry`'s public export of `origin`, if it has one.
fn public_export<'e>(entry: &'e Entry, origin: &Origin) -> Option<&'e gdl_extract::ExportEntry> {
    entry
        .offer
        .exports
        .iter()
        .find(|e| e.origin == *origin && e.public)
}

/// A request position as the graph file writes it.
fn request(names: &Names, head: Option<&Origin>, item: Option<&Origin>) -> String {
    let head = head.map_or("()".to_string(), |o| names.name(o));
    match item {
        None => head,
        Some(item) if head == "()" => format!("stream {}", names.name(item)),
        Some(item) => format!("{head} + stream {}", names.name(item)),
    }
}

/// A hop's topology, after its outer end.
fn topology(hop: &Hop) -> String {
    let mut parts: Vec<String> = hop.forward.clone();
    parts.push(match &hop.nested {
        Some(nested) => format!("{} {{ {} }}", hop.inner, topology(nested)),
        None => hop.inner.clone(),
    });
    parts.extend(hop.backward.iter().cloned());
    parts.join(" => ")
}

/// Entries as a spec lists them: a registry crate by `name@version`, a
/// path crate by its directory, and a Git crate by its URL and commit.
fn crate_list(entries: &[&Entry], problems: &mut Vec<String>) -> Vec<toml::Value> {
    entries
        .iter()
        .filter_map(|entry| {
            let offer = &entry.offer;
            if gdl_gen::CRATES_IO.contains(&offer.source.as_str()) {
                Some(format!("{}@{}", offer.package, offer.version).into())
            } else if let Some(git) = offer.source.strip_prefix("git+") {
                let (repository, commit) = git.rsplit_once('#')?;
                let url = repository.split('?').next()?;
                let mut entry = toml::map::Map::new();
                entry.insert("package".into(), offer.package.clone().into());
                entry.insert("git".into(), url.to_string().into());
                entry.insert("rev".into(), commit.to_string().into());
                let lib = offer.messages.iter().map(|m| m.path.as_str())
                    .chain(offer.nodes.iter().map(|n| n.ty.as_str()))
                    .chain(offer.exports.iter().map(|e| e.path.as_str()))
                    .find_map(|path| path.split("::").next());
                if let Some(lib) = lib { entry.insert("lib".into(), lib.to_string().into()); }
                Some(toml::Value::Table(entry))
            } else if let Some(dir) = offer.source.strip_prefix("path+file://") {
                Some(dir.to_string().into())
            } else {
                problems.push(format!(
                    "{}: a crate from {} cannot be listed yet: only crates.io, Git, and directories",
                    entry.key(),
                    offer.source
                ));
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use gdl_extract::{MessageEntry, Offer};

    use super::*;

    const REGISTRY: &str = "registry+https://github.com/rust-lang/crates.io-index";

    fn origin(path: &str) -> Origin {
        Origin {
            package: "proto".into(),
            version: "0.1.0".into(),
            source: REGISTRY.into(),
            path: path.into(),
        }
    }

    /// A catalogue of one protocols crate, whose messages are `match` (a
    /// keyword, spelled `r#match` in Rust) and `Token`, and one node crate.
    fn catalog(dir: &std::path::Path) -> Catalog {
        let mut catalog = Catalog::load(dir).expect("catalogue");
        let message = |name: &str, path: &str| MessageEntry {
            name: name.into(),
            path: path.into(),
            wire: true,
            public: true,
        };
        for offer in [
            Offer {
                package: "proto".into(),
                version: "0.1.0".into(),
                source: REGISTRY.into(),
                messages: vec![
                    message("Token", "proto::Token"),
                    message("match", "proto::match"),
                ],
                exports: Vec::new(),
                nodes: Vec::new(),
            },
            Offer {
                package: "nodes".into(),
                version: "0.1.0".into(),
                source: REGISTRY.into(),
                messages: Vec::new(),
                exports: Vec::new(),
                nodes: vec![NodeEntry {
                    entries: Vec::new(),
                    clients: None,
                    config: Vec::new(),
                    name: "Engine".into(),
                    role: gdl_model::Role::Sink,
                    ty: "nodes::Engine".into(),
                    ports: vec![
                        PortEntry {
                            port: "Req".into(),
                            head: Some(origin("proto::match")),
                            item: None,
                        },
                        PortEntry {
                            port: "Item".into(),
                            head: Some(origin("proto::Token")),
                            item: None,
                        },
                    ],
                }],
            },
        ] {
            catalog
                .add(Entry {
                    added: "2026-10-19T00:00:00Z".into(),
                    checksum: None,
                    offer,
                })
                .expect("added");
        }
        catalog
    }

    /// A message named like a keyword is written raw in the graph file,
    /// which reads it back by its name.
    #[test]
    fn a_keyword_message_is_written_raw() {
        let dir = std::env::temp_dir().join(format!("gdl-web-keyword-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let catalog = catalog(&dir);
        let draft = Draft {
            deployment: toml::Table::new(),
            deployment_config: BTreeMap::new(),
            identities: BTreeMap::new(),
            pipelines: Vec::new(),
            pipeline_names: BTreeMap::new(),
            entries: BTreeMap::new(),
            routing: BTreeMap::new(),
            name: "keyword".into(),
            source: Source {
                name: "A".into(),
                request: Port {
                    head: Some(origin("proto::match")),
                    item: None,
                },
                response: origin("proto::Token"),
            },
            nodes: vec![Instance {
                config: Default::default(),
                name: "Engine".into(),
                entry: "nodes@0.1.0".into(),
                node: "Engine".into(),
            }],
            hop: Hop {
                forward: Vec::new(),
                inner: "Engine".into(),
                nested: None,
                backward: Vec::new(),
            },
            segments: BTreeMap::from([(
                "all".to_string(),
                vec!["A".to_string(), "Engine".to_string()],
            )]),
        };
        let rendered = draft
            .render(&catalog, &Gdl::Path("/gdl".into()))
            .expect("rendered");
        assert_eq!(
            rendered.graph,
            "name = \"keyword\", schema = 1;\nsource A: r#match -> stream Token;\nsink Engine: r#match -> stream Token;\ntopology: A => Engine;\n"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A spec lists a registry crate by version, which means crates.io: a
    /// crate from another registry cannot be listed.
    #[test]
    fn a_crate_from_another_registry_is_refused() {
        let elsewhere = Entry {
            added: "2026-10-19T00:00:00Z".into(),
            checksum: None,
            offer: Offer {
                package: "proto".into(),
                version: "0.1.0".into(),
                source: "sparse+https://registry.example.com/index/".into(),
                messages: Vec::new(),
                exports: Vec::new(),
                nodes: Vec::new(),
            },
        };
        let mut problems = Vec::new();
        assert!(crate_list(&[&elsewhere], &mut problems).is_empty());
        assert_eq!(
            problems,
            [
                "proto@0.1.0: a crate from sparse+https://registry.example.com/index/ cannot be listed yet: only crates.io, Git, and directories"
            ]
        );
    }

    /// Two messages of one name: the second carried is numbered, and each
    /// name is pinned to its message, since the name alone resolves to two.
    #[test]
    fn two_messages_of_one_name_are_named_apart_and_pinned() {
        let dir = std::env::temp_dir().join(format!("gdl-web-twins-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut catalog = Catalog::load(&dir).expect("catalogue");
        for package in ["other", "proto"] {
            let offer = Offer {
                package: package.into(),
                version: "0.1.0".into(),
                source: REGISTRY.into(),
                messages: vec![MessageEntry {
                    name: "Token".into(),
                    path: format!("{package}::Token"),
                    wire: true,
                    public: true,
                }],
                exports: Vec::new(),
                nodes: Vec::new(),
            };
            catalog
                .add(Entry {
                    added: "2026-10-19T00:00:00Z".into(),
                    checksum: None,
                    offer,
                })
                .expect("added");
        }
        let other = Origin {
            package: "other".into(),
            ..origin("other::Token")
        };
        let draft = Draft {
            deployment: toml::Table::new(),
            deployment_config: BTreeMap::new(),
            identities: BTreeMap::new(),
            pipelines: Vec::new(),
            pipeline_names: BTreeMap::new(),
            entries: BTreeMap::new(),
            routing: BTreeMap::new(),
            name: "twins".into(),
            source: Source {
                name: "A".into(),
                request: Port {
                    head: Some(other),
                    item: None,
                },
                response: origin("proto::Token"),
            },
            nodes: Vec::new(),
            hop: Hop {
                forward: Vec::new(),
                inner: "B".into(),
                nested: None,
                backward: Vec::new(),
            },
            segments: BTreeMap::new(),
        };
        let rendered = draft
            .render(&catalog, &Gdl::Path("/gdl".into()))
            .expect("rendered");
        assert!(
            rendered
                .graph
                .contains("source A: Token -> stream Token2;\n"),
            "{}",
            rendered.graph
        );
        assert!(
            rendered.spec.contains("Token = \"other::Token\""),
            "{}",
            rendered.spec
        );
        assert!(
            rendered.spec.contains("Token2 = \"proto::Token\""),
            "{}",
            rendered.spec
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A node two listed crates both offer is pinned in each instance's
    /// binding: by name alone it would bind to neither.
    #[test]
    fn a_node_two_crates_offer_is_pinned() {
        let dir = std::env::temp_dir().join(format!("gdl-web-two-nodes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut catalog = catalog(&dir);
        let engine = catalog
            .entries()
            .find(|e| e.offer.package == "nodes")
            .expect("nodes")
            .offer
            .nodes[0]
            .clone();
        let cuda = Offer {
            package: "cuda".into(),
            version: "0.1.0".into(),
            source: REGISTRY.into(),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes: vec![NodeEntry {
                entries: Vec::new(),
                clients: None,
                config: Vec::new(),
                ty: "cuda::Engine".into(),
                ..engine
            }],
        };
        catalog
            .add(Entry {
                added: "2026-10-19T00:00:00Z".into(),
                checksum: None,
                offer: cuda,
            })
            .expect("added");
        let draft = Draft {
            deployment: toml::Table::new(),
            deployment_config: BTreeMap::new(),
            identities: BTreeMap::new(),
            pipelines: Vec::new(),
            pipeline_names: BTreeMap::new(),
            entries: BTreeMap::new(),
            routing: BTreeMap::new(),
            name: "twice".into(),
            source: Source {
                name: "A".into(),
                request: Port {
                    head: Some(origin("proto::match")),
                    item: None,
                },
                response: origin("proto::Token"),
            },
            nodes: vec![
                Instance {
                    config: Default::default(),
                    name: "Engine".into(),
                    entry: "nodes@0.1.0".into(),
                    node: "Engine".into(),
                },
                Instance {
                    config: Default::default(),
                    name: "Engine2".into(),
                    entry: "cuda@0.1.0".into(),
                    node: "Engine".into(),
                },
            ],
            hop: Hop {
                forward: Vec::new(),
                inner: "Engine".into(),
                nested: None,
                backward: Vec::new(),
            },
            segments: BTreeMap::new(),
        };
        let rendered = draft
            .render(&catalog, &Gdl::Path("/gdl".into()))
            .expect("rendered");
        assert!(
            rendered.spec.contains("Engine = \"nodes::Engine\""),
            "{}",
            rendered.spec
        );
        assert!(
            rendered.spec.contains("Engine2 = \"cuda::Engine\""),
            "{}",
            rendered.spec
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A message its crate defines in a private module, exported by a
    /// facade crate: the draft lists the facade too, and the spec checks.
    #[test]
    fn a_message_exported_only_by_a_facade_is_composed() {
        use gdl_extract::ExportEntry;
        let dir = std::env::temp_dir().join(format!("gdl-web-facade-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut catalog = Catalog::load(&dir).expect("catalogue");
        let message = |name: &str, path: &str, public: bool| MessageEntry {
            name: name.into(),
            path: path.into(),
            wire: true,
            public,
        };
        let offer = |package: &str, messages, exports, nodes| Offer {
            package: package.into(),
            version: "0.1.0".into(),
            source: REGISTRY.into(),
            messages,
            exports,
            nodes,
        };
        for offer in [
            offer(
                "proto",
                vec![
                    message("Words", "proto::inner::Words", false),
                    message("Token", "proto::Token", true),
                ],
                Vec::new(),
                Vec::new(),
            ),
            offer(
                "facade",
                Vec::new(),
                vec![ExportEntry {
                    name: "Words".into(),
                    path: "facade::Words".into(),
                    origin: origin("proto::inner::Words"),
                    wire: true,
                    public: true,
                }],
                Vec::new(),
            ),
            offer(
                "nodes",
                Vec::new(),
                Vec::new(),
                vec![NodeEntry {
                    entries: Vec::new(),
                    clients: None,
                    config: Vec::new(),
                    name: "Engine".into(),
                    role: Role::Sink,
                    ty: "nodes::Engine".into(),
                    ports: vec![
                        PortEntry {
                            port: "Req".into(),
                            head: Some(origin("proto::inner::Words")),
                            item: None,
                        },
                        PortEntry {
                            port: "Item".into(),
                            head: Some(origin("proto::Token")),
                            item: None,
                        },
                    ],
                }],
            ),
        ] {
            catalog
                .add(Entry {
                    added: "2026-10-19T00:00:00Z".into(),
                    checksum: None,
                    offer,
                })
                .expect("added");
        }
        let draft = Draft {
            deployment: toml::Table::new(),
            deployment_config: BTreeMap::new(),
            identities: BTreeMap::new(),
            pipelines: Vec::new(),
            pipeline_names: BTreeMap::new(),
            entries: BTreeMap::new(),
            routing: BTreeMap::new(),
            name: "facaded".into(),
            source: Source {
                name: "A".into(),
                request: Port {
                    head: Some(origin("proto::inner::Words")),
                    item: None,
                },
                response: origin("proto::Token"),
            },
            nodes: vec![Instance {
                config: Default::default(),
                name: "Engine".into(),
                entry: "nodes@0.1.0".into(),
                node: "Engine".into(),
            }],
            hop: Hop {
                forward: Vec::new(),
                inner: "Engine".into(),
                nested: None,
                backward: Vec::new(),
            },
            segments: BTreeMap::from([(
                "all".to_string(),
                vec!["A".to_string(), "Engine".to_string()],
            )]),
        };
        let rendered = draft
            .render(&catalog, &Gdl::Path("/gdl".into()))
            .expect("rendered");
        assert!(
            rendered
                .spec
                .contains("crates = [\"facade@0.1.0\", \"proto@0.1.0\"]"),
            "{}",
            rendered.spec
        );
        let spec = gdl_gen::Spec::from_text(&rendered.spec, &rendered.graph, &dir).expect("spec");
        let offers: Vec<&gdl_gen::Offer> = catalog.entries().map(|e| &e.offer).collect();
        gdl_gen::check(&spec, &dir.join("target"), gdl_gen::Offers::Given(&offers))
            .expect("checks");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A node read against a directory's crate at one version, after the
    /// directory was added again at another: the message names the version
    /// that went, and says what to add again.
    #[test]
    fn a_node_read_against_an_older_version_says_to_add_it_again() {
        let dir = std::env::temp_dir().join(format!("gdl-web-reread-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut catalog = Catalog::load(&dir).expect("catalogue");
        let path = |package: &str| format!("path+file:///work/{package}");
        let words = Origin {
            package: "proto".into(),
            version: "0.1.0".into(),
            source: path("proto"),
            path: "proto::Words".into(),
        };
        for offer in [
            Offer {
                package: "proto".into(),
                version: "0.2.0".into(),
                source: path("proto"),
                messages: vec![MessageEntry {
                    name: "Words".into(),
                    path: "proto::Words".into(),
                    wire: true,
                    public: true,
                }],
                exports: Vec::new(),
                nodes: Vec::new(),
            },
            Offer {
                package: "nodes".into(),
                version: "0.1.0".into(),
                source: path("nodes"),
                messages: Vec::new(),
                exports: Vec::new(),
                nodes: vec![NodeEntry {
                    entries: Vec::new(),
                    clients: None,
                    config: Vec::new(),
                    name: "Engine".into(),
                    role: Role::Sink,
                    ty: "nodes::Engine".into(),
                    ports: vec![
                        PortEntry {
                            port: "Req".into(),
                            head: Some(words.clone()),
                            item: None,
                        },
                        PortEntry {
                            port: "Item".into(),
                            head: Some(words.clone()),
                            item: None,
                        },
                    ],
                }],
            },
        ] {
            catalog
                .add(Entry {
                    added: "2026-10-19T00:00:00Z".into(),
                    checksum: None,
                    offer,
                })
                .expect("added");
        }
        let draft = Draft {
            deployment: toml::Table::new(),
            deployment_config: BTreeMap::new(),
            identities: BTreeMap::new(),
            pipelines: Vec::new(),
            pipeline_names: BTreeMap::new(),
            entries: BTreeMap::new(),
            routing: BTreeMap::new(),
            name: "reread".into(),
            source: Source {
                name: "A".into(),
                request: Port {
                    head: Some(words.clone()),
                    item: None,
                },
                response: words,
            },
            nodes: vec![Instance {
                config: Default::default(),
                name: "Engine".into(),
                entry: "nodes@0.1.0".into(),
                node: "Engine".into(),
            }],
            hop: Hop {
                forward: Vec::new(),
                inner: "Engine".into(),
                nested: None,
                backward: Vec::new(),
            },
            segments: BTreeMap::new(),
        };
        let problems = draft
            .render(&catalog, &Gdl::Path("/gdl".into()))
            .expect_err("stale");
        assert_eq!(
            problems,
            [
                "message `proto::Words`: it was read from proto@0.1.0, and the catalogue now holds that directory as proto@0.2.0: add again the crates that use it"
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
