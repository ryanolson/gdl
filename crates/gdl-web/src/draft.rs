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
    /// The graph's name.
    pub name: String,
    /// The graph's entry point: it takes no implementation.
    pub source: Source,
    /// The graph's other nodes, each an instance of a catalogue node.
    pub nodes: Vec<Instance>,
    /// The source's hop.
    pub hop: Hop,
    /// Segment name to the nodes it hosts.
    pub segments: BTreeMap<String, Vec<String>>,
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
                // The reader offers no source: it implements no role trait.
                Role::Source => continue,
            };
            text.push_str(&format!("{} {}: {signature};\n", node.role, instance.name));
        }
        text.push_str(&format!(
            "topology: {} => {};\n",
            self.source.name,
            topology(&self.hop)
        ));
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
            if node.name != *name || offered > 1 {
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
        toml::to_string(&spec).expect("a spec is representable in TOML")
    }
}

/// How each carried message is named in the graph file, the crates that
/// define them, and the pins their names need.
struct Names {
    names: BTreeMap<Origin, String>,
    /// Entry keys of the defining crates.
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
        // Each message's name where it is defined, and its crate.
        let mut defined: Vec<(&Origin, &str)> = Vec::new();
        for origin in used {
            let key = format!("{}@{}", origin.package, origin.version);
            let message = entries.get(&key).and_then(|entry| {
                (entry.offer.source == origin.source)
                    .then(|| entry.offer.messages.iter().find(|m| m.path == origin.path))
                    .flatten()
            });
            match message {
                Some(message) => {
                    names.crates.insert(key);
                    defined.push((origin, &message.name));
                }
                None => problems.push(format!(
                    "message `{}`: its crate, {key}, is not in the catalogue: add it first",
                    origin.path
                )),
            }
        }
        // What the listed crates offer by each name: the messages a name
        // could resolve to.
        let mut offered: BTreeMap<&str, BTreeSet<Origin>> = BTreeMap::new();
        let mut public: BTreeMap<Origin, String> = BTreeMap::new();
        for key in &names.crates {
            let offer = &entries[key].offer;
            for m in &offer.messages {
                let origin = Origin {
                    package: offer.package.clone(),
                    version: offer.version.clone(),
                    source: offer.source.clone(),
                    path: m.path.clone(),
                };
                if m.public {
                    public
                        .entry(origin.clone())
                        .or_insert_with(|| m.path.clone());
                }
                offered.entry(&m.name).or_default().insert(origin);
            }
            for e in &offer.exports {
                if e.public {
                    public
                        .entry(e.origin.clone())
                        .or_insert_with(|| e.path.clone());
                }
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
                match public.get(origin) {
                    Some(path) => {
                        names.pins.insert(graph_name.clone(), path.clone());
                    }
                    None => problems.push(format!(
                        "message `{}` has no public path in the catalogue's crates: export it",
                        origin.path
                    )),
                }
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
/// path crate by its directory.
fn crate_list(entries: &[&Entry], problems: &mut Vec<String>) -> Vec<toml::Value> {
    entries
        .iter()
        .filter_map(|entry| {
            let offer = &entry.offer;
            if gdl_gen::CRATES_IO.contains(&offer.source.as_str()) {
                Some(format!("{}@{}", offer.package, offer.version).into())
            } else if let Some(dir) = offer.source.strip_prefix("path+file://") {
                Some(dir.to_string().into())
            } else {
                problems.push(format!(
                    "{}: a crate from {} cannot be listed yet: only crates.io and directories",
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
                "proto@0.1.0: a crate from sparse+https://registry.example.com/index/ cannot be listed yet: only crates.io and directories"
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
                    name: "Engine".into(),
                    entry: "nodes@0.1.0".into(),
                    node: "Engine".into(),
                },
                Instance {
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
}
