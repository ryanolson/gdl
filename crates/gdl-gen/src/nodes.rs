//! Binding a graph file's nodes to the nodes that node crates offer, as
//! the source reader found them.
//!
//! Each graph node other than the source binds to the one offered node of
//! its name, or to the one a pin names. The node must be what the graph
//! says it is: the same role, and at each port the same message, by
//! identity, as the graph's name for that port resolved to.

use std::collections::BTreeMap;

use gdl_extract::{NodeEntry, Offer, Origin};
use gdl_syntax::{Graph, Request, Role, Sig};

use crate::resolve::Resolved;
#[cfg(test)]
use crate::spec::CrateSource;
use crate::spec::{Bind, NodeCrate};

/// The binding of each graph node other than the source that binds, keyed
/// by node name, and every problem with the others: reported with the
/// spec's other problems. `offers` are the node crates' offers, in the
/// order of `crates`; `messages` is the graph's resolution.
pub(crate) fn bind(
    graph: &Graph,
    crates: &[NodeCrate],
    offers: &[Offer],
    messages: &BTreeMap<String, Resolved>,
    pins: &BTreeMap<String, String>,
) -> (BTreeMap<String, Bind>, Vec<String>) {
    // The type a generated segment names, as Rust spells it: a library or
    // a node named like a keyword is written raw.
    let ty = |index: usize, node: &NodeEntry| {
        format!(
            "{}::{}",
            gdl_syntax::identifier(&crates[index].lib),
            gdl_syntax::identifier(&node.name)
        )
    };
    let unraw = |path: &str| path.replace("r#", "");
    let offered: Vec<(usize, &NodeEntry)> = offers
        .iter()
        .enumerate()
        .flat_map(|(i, o)| o.nodes.iter().map(move |n| (i, n)))
        .collect();
    let mut problems = Vec::new();
    for name in pins.keys() {
        let bindable = graph
            .nodes
            .iter()
            .any(|n| n.ident == name && n.role() != Role::Source);
        if !bindable {
            problems.push(format!(
                "[nodes] pin `{name}`: the graph has no node `{name}` that takes an implementation"
            ));
        }
    }

    let mut binds = BTreeMap::new();
    for node in graph.nodes.iter().filter(|n| n.role() != Role::Source) {
        let name = node.ident.to_string();
        let found: Vec<&(usize, &NodeEntry)> = match pins.get(&name) {
            Some(path) => offered
                .iter()
                .filter(|(i, n)| unraw(&ty(*i, n)) == unraw(path))
                .collect(),
            None => offered.iter().filter(|(_, n)| n.name == name).collect(),
        };
        let (index, entry) = match (found.as_slice(), pins.get(&name)) {
            ([one], _) => **one,
            ([], Some(path)) => {
                problems.push(format!(
                    "[nodes] pin `{name}`: no [nodes] crate offers `{path}`"
                ));
                continue;
            }
            ([], None) if crates.is_empty() => {
                problems.push(format!(
                    "node `{name}`: a graph spec binds each node to a node that a [nodes] crate offers, and the spec lists no [nodes] crate"
                ));
                continue;
            }
            ([], None) => {
                let mut names: Vec<String> = offered
                    .iter()
                    .map(|(_, n)| format!("`{}`", n.name))
                    .collect();
                names.sort();
                names.dedup();
                problems.push(format!(
                    "node `{name}`: no [nodes] crate offers it; they offer {}",
                    if names.is_empty() {
                        "no node".to_string()
                    } else {
                        names.join(", ")
                    }
                ));
                continue;
            }
            (several, _) => {
                let paths: Vec<String> = several
                    .iter()
                    .map(|(i, n)| format!("`{}`", ty(*i, n)))
                    .collect();
                problems.push(format!(
                    "node `{name}` is offered by more than one [nodes] crate: {}; choose one under [nodes] pin",
                    paths.join(", ")
                ));
                continue;
            }
        };
        let bound = ty(index, entry);
        let before = problems.len();
        check(&name, &node.sig, entry, &bound, messages, &mut problems);
        if problems.len() > before {
            continue;
        }
        let krate = &crates[index];
        binds.insert(
            name,
            Bind {
                package: krate.package.clone(),
                lib: krate.lib.clone(),
                source: krate.source.clone(),
                ty: bound,
            },
        );
    }
    (binds, problems)
}

/// A message at a port as the graph has it: its name, and the origin the
/// name resolved to, if it did.
type Named = (String, Option<Origin>);

/// A port as the graph has it: the head (`None` for `()`), and the item
/// of a streamed request.
type GraphPort = (Option<Named>, Option<Option<Named>>);

/// Check that `entry` is what the graph's signature `sig` for node `name`
/// says: its role, and at each port the message the graph's name resolved
/// to, by identity.
fn check(
    name: &str,
    sig: &Sig,
    entry: &NodeEntry,
    bound: &str,
    messages: &BTreeMap<String, Resolved>,
    problems: &mut Vec<String>,
) {
    let role = match sig {
        Sig::Source { .. } => Role::Source,
        Sig::Sink { .. } => Role::Sink,
        Sig::Forward { .. } => Role::Forward,
        Sig::Backward { .. } => Role::Backward,
        Sig::Operator { .. } => Role::Operator,
    };
    if entry.role != role {
        problems.push(format!(
            "node `{name}`: the graph declares a {role}, but `{bound}` is a {}",
            entry.role
        ));
        return;
    }
    // What the graph says is at each port, in the role trait's order: the
    // head (`None` for `()`), and the item of a streamed request.
    let message = |ty: &syn::Type| -> Option<Named> {
        crate::names::message(ty).map(|name| {
            let origin = messages.get(&name).map(|r| r.origin.clone());
            (name, origin)
        })
    };
    let request = |req: &Request| (message(&req.head), req.item.as_ref().map(&message));
    let ports: Vec<(&str, GraphPort)> = match sig {
        Sig::Sink { req, item } => vec![("Req", request(req)), ("Item", (message(item), None))],
        Sig::Forward { input, output } => vec![("In", request(input)), ("Out", request(output))],
        Sig::Backward { input, output } => {
            vec![
                ("In", (message(input), None)),
                ("Out", (message(output), None)),
            ]
        }
        Sig::Operator {
            up_req,
            up_item,
            down_req,
            down_item,
        } => vec![
            ("UpReq", request(up_req)),
            ("UpItem", (message(up_item), None)),
            ("DownReq", request(down_req)),
            ("DownItem", (message(down_item), None)),
        ],
        Sig::Source { .. } => unreachable!("the source takes no implementation"),
    };
    let same = |graph: &Option<Named>, offered: &Option<Origin>| match (graph, offered) {
        (None, None) => true,
        (Some((_, Some(a))), Some(b)) => a == b,
        _ => false,
    };
    for (port, (head, item)) in ports {
        let Some(offered) = entry.ports.iter().find(|p| p.port == port) else {
            problems.push(format!("node `{name}`: `{bound}` has no port `{port}`"));
            continue;
        };
        let item_same = match (&item, &offered.item) {
            (Some(graph), offered) => offered.is_some() && same(graph, offered),
            (None, offered) => offered.is_none(),
        };
        if !(same(&head, &offered.head) && item_same) {
            problems.push(format!(
                "node `{name}`: port `{port}`: the graph has {}, but `{bound}` takes {}",
                graph_side(&head, &item),
                crate_side(&offered.head, &offered.item)
            ));
        }
    }
}

/// A message by its path and its package, so that two messages of one
/// path in two packages read apart.
fn shown(origin: &Origin) -> String {
    format!("`{}` ({} {})", origin.path, origin.package, origin.version)
}

fn graph_side(head: &Option<Named>, item: &Option<Option<Named>>) -> String {
    let one = |m: &Option<Named>| match m {
        None => "`()`".to_string(),
        Some((_, Some(origin))) => shown(origin),
        Some((name, None)) => format!("`{name}`"),
    };
    match item {
        None => one(head),
        Some(item) => format!("{} + stream {}", one(head), one(item)),
    }
}

fn crate_side(head: &Option<Origin>, item: &Option<Origin>) -> String {
    let one = |o: &Option<Origin>| o.as_ref().map_or("`()`".to_string(), shown);
    match item {
        None => one(head),
        Some(_) => format!("{} + stream {}", one(head), one(item)),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use gdl_extract::PortEntry;

    use super::*;

    fn graph() -> Graph {
        crate::names::GraphFile::parse(crate::names::tests::CHAT, Path::new("chat.gdl"))
            .expect("graph")
            .graph
    }

    fn origin(path: &str) -> Origin {
        Origin {
            package: "chat-protocols".into(),
            version: "0.1.0".into(),
            source: "path+file:///crates/protocols".into(),
            path: format!("chat_protocols::{path}"),
        }
    }

    /// Every chat message, resolved to `chat_protocols`.
    fn messages() -> BTreeMap<String, Resolved> {
        ["Prompt", "Chunk", "Words", "Text", "Token", "Piece"]
            .iter()
            .map(|n| {
                (
                    (*n).to_string(),
                    Resolved {
                        origin: origin(n),
                        path: format!("chat_protocols::{n}"),
                        wire: true,
                    },
                )
            })
            .collect()
    }

    fn krate(lib: &str) -> NodeCrate {
        NodeCrate {
            package: lib.replace('_', "-"),
            lib: lib.to_string(),
            source: CrateSource::Path(PathBuf::from(format!("/nodes/{lib}"))),
        }
    }

    /// A node entry: role, name, and each port's head and item by message
    /// name (`()` for none).
    fn node(role: Role, name: &str, ports: &[(&str, &str, Option<&str>)]) -> NodeEntry {
        let side = |m: &str| (m != "()").then(|| origin(m));
        NodeEntry {
            name: name.to_string(),
            role,
            ty: format!("x::{name}"),
            ports: ports
                .iter()
                .map(|(port, head, item)| PortEntry {
                    port: (*port).to_string(),
                    head: side(head),
                    item: item.and_then(side),
                })
                .collect(),
        }
    }

    fn offer(nodes: Vec<NodeEntry>) -> Offer {
        Offer {
            package: "x".into(),
            version: "0.1.0".into(),
            source: "path+file:///x".into(),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes,
        }
    }

    fn front() -> Vec<NodeEntry> {
        vec![
            node(
                Role::Operator,
                "Preprocess",
                &[
                    ("UpReq", "Prompt", None),
                    ("UpItem", "Chunk", None),
                    ("DownReq", "Words", None),
                    ("DownItem", "Text", None),
                ],
            ),
            node(
                Role::Forward,
                "Truncate",
                &[("In", "Words", None), ("Out", "Words", None)],
            ),
            node(
                Role::Backward,
                "Number",
                &[("In", "Chunk", None), ("Out", "Chunk", None)],
            ),
        ]
    }

    fn engine() -> Vec<NodeEntry> {
        vec![node(
            Role::Sink,
            "Engine",
            &[("Req", "Words", None), ("Item", "Token", None)],
        )]
    }

    fn post() -> Vec<NodeEntry> {
        vec![
            node(
                Role::Backward,
                "Detokenize",
                &[("In", "Token", None), ("Out", "Piece", None)],
            ),
            node(
                Role::Backward,
                "Format",
                &[("In", "Piece", None), ("Out", "Text", None)],
            ),
        ]
    }

    fn chat() -> (Vec<NodeCrate>, Vec<Offer>) {
        (
            vec![krate("front"), krate("engine"), krate("post")],
            vec![offer(front()), offer(engine()), offer(post())],
        )
    }

    fn bound(
        crates: &[NodeCrate],
        offers: &[Offer],
        pins: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, Bind>, Vec<String>> {
        match bind(&graph(), crates, offers, &messages(), pins) {
            (binds, problems) if problems.is_empty() => Ok(binds),
            (_, problems) => Err(problems),
        }
    }

    #[test]
    fn each_node_binds_to_the_offered_node_of_its_name() {
        let (crates, offers) = chat();
        let binds = bound(&crates, &offers, &BTreeMap::new()).expect("bound");
        let types: Vec<(&str, &str)> = binds
            .iter()
            .map(|(name, b)| (name.as_str(), b.ty.as_str()))
            .collect();
        assert_eq!(
            types,
            [
                ("Detokenize", "post::Detokenize"),
                ("Engine", "engine::Engine"),
                ("Format", "post::Format"),
                ("Number", "front::Number"),
                ("Preprocess", "front::Preprocess"),
                ("Truncate", "front::Truncate"),
            ]
        );
        assert_eq!(
            binds["Engine"].source,
            CrateSource::Path(PathBuf::from("/nodes/engine"))
        );
    }

    /// A node of another role, or with another message at a port, is
    /// refused; messages compare by identity, so another package's message
    /// of the same path is another message.
    #[test]
    fn an_offered_node_must_match_the_graph() {
        let (crates, mut offers) = chat();
        offers[1] = offer(vec![node(
            Role::Sink,
            "Engine",
            &[("Req", "Words", None), ("Item", "Piece", None)],
        )]);
        offers[0].nodes[1] = node(
            Role::Backward,
            "Truncate",
            &[("In", "Words", None), ("Out", "Words", None)],
        );
        let mut other = node(
            Role::Backward,
            "Format",
            &[("In", "Piece", None), ("Out", "Text", None)],
        );
        other.ports[1].head.as_mut().expect("Text").package = "other-protocols".into();
        offers[2].nodes[1] = other;
        let problems = bound(&crates, &offers, &BTreeMap::new()).expect_err("mismatch");
        assert_eq!(
            problems,
            [
                "node `Truncate`: the graph declares a forward, but `front::Truncate` is a backward",
                "node `Engine`: port `Item`: the graph has `chat_protocols::Token` (chat-protocols 0.1.0), but `engine::Engine` takes `chat_protocols::Piece` (chat-protocols 0.1.0)",
                "node `Format`: port `Out`: the graph has `chat_protocols::Text` (chat-protocols 0.1.0), but `post::Format` takes `chat_protocols::Text` (other-protocols 0.1.0)",
            ]
        );
    }

    /// Whether a request is streamed is part of the port.
    #[test]
    fn a_streamed_port_must_match_the_graph() {
        let (crates, mut offers) = chat();
        offers[1] = offer(vec![node(
            Role::Sink,
            "Engine",
            &[("Req", "Words", Some("Token")), ("Item", "Token", None)],
        )]);
        let problems = bound(&crates, &offers, &BTreeMap::new()).expect_err("streamed");
        assert_eq!(
            problems,
            [
                "node `Engine`: port `Req`: the graph has `chat_protocols::Words` (chat-protocols 0.1.0), but `engine::Engine` takes `chat_protocols::Words` (chat-protocols 0.1.0) + stream `chat_protocols::Token` (chat-protocols 0.1.0)"
            ]
        );
    }

    #[test]
    fn a_node_offered_twice_binds_only_when_pinned() {
        let (mut crates, mut offers) = chat();
        crates.push(krate("cuda"));
        offers.push(offer(engine()));
        let problems = bound(&crates, &offers, &BTreeMap::new()).expect_err("twice");
        assert_eq!(
            problems,
            [
                "node `Engine` is offered by more than one [nodes] crate: `engine::Engine`, `cuda::Engine`; choose one under [nodes] pin"
            ]
        );
        let pins = BTreeMap::from([("Engine".to_string(), "cuda::Engine".to_string())]);
        assert_eq!(
            bound(&crates, &offers, &pins).expect("pinned")["Engine"].ty,
            "cuda::Engine"
        );
    }

    /// A pin binds a graph node to an offered node of another name.
    #[test]
    fn a_pin_may_bind_a_node_to_one_of_another_name() {
        let (crates, mut offers) = chat();
        offers[1] = offer(vec![node(
            Role::Sink,
            "Mock",
            &[("Req", "Words", None), ("Item", "Token", None)],
        )]);
        let pins = BTreeMap::from([("Engine".to_string(), "engine::Mock".to_string())]);
        assert_eq!(
            bound(&crates, &offers, &pins).expect("pinned")["Engine"].ty,
            "engine::Mock"
        );
    }

    /// A library named like a keyword is reached as `r#gen`, in the type
    /// the segment names; a pin may spell it either way.
    #[test]
    fn a_keyword_library_binds_by_its_raw_path() {
        let (mut crates, offers) = chat();
        crates[1] = krate("gen");
        let binds = bound(&crates, &offers, &BTreeMap::new()).expect("bound");
        assert_eq!(binds["Engine"].ty, "r#gen::Engine");
        for pin in ["r#gen::Engine", "gen::Engine"] {
            let pins = BTreeMap::from([("Engine".to_string(), pin.to_string())]);
            assert!(bound(&crates, &offers, &pins).is_ok(), "{pin}");
        }
    }

    #[test]
    fn every_unbound_node_and_bad_pin_is_reported() {
        let (crates, mut offers) = chat();
        offers[1].nodes.clear();
        offers[2].nodes.retain(|n| n.name != "Format");
        let pins = BTreeMap::from([
            ("Frontend".to_string(), "front::Frontend".to_string()),
            ("Detokenize".to_string(), "post::Detokeniser".to_string()),
        ]);
        let problems = bound(&crates, &offers, &pins).expect_err("unbound");
        assert_eq!(
            problems,
            [
                "[nodes] pin `Frontend`: the graph has no node `Frontend` that takes an implementation",
                "node `Engine`: no [nodes] crate offers it; they offer `Detokenize`, `Number`, `Preprocess`, `Truncate`",
                "[nodes] pin `Detokenize`: no [nodes] crate offers `post::Detokeniser`",
                "node `Format`: no [nodes] crate offers it; they offer `Detokenize`, `Number`, `Preprocess`, `Truncate`",
            ]
        );
        let problems = bound(&[], &[], &BTreeMap::new()).expect_err("no crates");
        assert_eq!(problems.len(), 6);
        assert!(
            problems[0].contains("the spec lists no [nodes] crate"),
            "{problems:?}"
        );
    }
}
