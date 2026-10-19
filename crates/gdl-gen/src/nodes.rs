//! Binding a graph file's nodes to the nodes that node crates declare.
//!
//! A node crate declares its nodes in a nodes file, which the generator
//! reads without compiling the crate; `#[gdl::node]` keeps the file true
//! when the crate compiles. Each graph node other than the source binds to
//! the one declared node of its name, or to the one a pin names, and the
//! graph's signature for it must be the declaration's.

use std::collections::BTreeMap;

use gdl_syntax::{Graph, Node, Role};

use crate::Error;
use crate::spec::{Bind, NodeCrate};

/// The declared nodes of `crates`, each with the index of its crate.
/// Every nodes file that does not parse is reported, with positions.
pub(crate) fn read(crates: &[NodeCrate]) -> Result<Vec<(usize, Node)>, Error> {
    let mut declared = Vec::new();
    let mut problems = Vec::new();
    for (index, krate) in crates.iter().enumerate() {
        let parsed = std::fs::read_to_string(&krate.file)
            .map_err(|e| Error::new(format!("reading {}: {e}", krate.file.display())))
            .and_then(|text| crate::names::tokens(&text, &krate.file))
            .and_then(|tokens| {
                gdl_syntax::declarations(tokens).map_err(|e| crate::names::located(e, &krate.file))
            });
        match parsed {
            Ok(nodes) => declared.extend(nodes.into_iter().map(|node| (index, node))),
            Err(error) => problems.push(error.to_string()),
        }
    }
    if problems.is_empty() {
        Ok(declared)
    } else {
        Err(Error::new(problems.join("\n")))
    }
}

/// The binding of each graph node other than the source that binds, keyed
/// by node name, and every problem with the others: reported with the
/// spec's other problems.
pub(crate) fn bind(
    graph: &Graph,
    crates: &[NodeCrate],
    declared: &[(usize, Node)],
    pins: &BTreeMap<String, String>,
) -> (BTreeMap<String, Bind>, Vec<String>) {
    // The type a declared node names: `<lib>::<Name>`, by ruling 43.
    let ty = |&(index, ref node): &(usize, Node)| format!("{}::{}", crates[index].lib, node.ident);
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
        let found: Vec<&(usize, Node)> = match pins.get(&name) {
            Some(path) => declared.iter().filter(|d| ty(d) == *path).collect(),
            None => declared.iter().filter(|(_, d)| d.ident == name).collect(),
        };
        let choice = match (found.as_slice(), pins.get(&name)) {
            ([one], _) => *one,
            ([], Some(path)) => {
                problems.push(format!(
                    "[nodes] pin `{name}`: no [nodes] crate declares `{path}`"
                ));
                continue;
            }
            ([], None) if crates.is_empty() => {
                problems.push(format!(
                    "node `{name}`: a graph spec binds each node to a node that a [nodes] crate declares, and the spec lists no [nodes] crate"
                ));
                continue;
            }
            ([], None) => {
                let mut names: Vec<String> = declared
                    .iter()
                    .map(|(_, d)| format!("`{}`", d.ident))
                    .collect();
                names.sort();
                names.dedup();
                problems.push(format!(
                    "node `{name}`: no [nodes] crate declares it; they declare {}",
                    if names.is_empty() {
                        "no node".to_string()
                    } else {
                        names.join(", ")
                    }
                ));
                continue;
            }
            (several, _) => {
                let paths: Vec<String> = several.iter().map(|d| format!("`{}`", ty(d))).collect();
                problems.push(format!(
                    "node `{name}` is declared by more than one [nodes] crate: {}; choose one under [nodes] pin",
                    paths.join(", ")
                ));
                continue;
            }
        };

        // The graph's signature must be the declaration's: compared as the
        // syntax writes both, so the role, which requests are streamed, and
        // every message name must agree.
        let (index, declaration) = choice;
        let names = |n: &str| n.to_string();
        let graph_says = format!(
            "{} {name}: {}",
            crate::contract::role(node.role()),
            crate::contract::signature(&node.sig, &names)
        );
        let crate_says = format!(
            "{} {}: {}",
            crate::contract::role(declaration.role()),
            declaration.ident,
            crate::contract::signature(&declaration.sig, &names)
        );
        let same = node.role() == declaration.role()
            && crate::contract::signature(&node.sig, &names)
                == crate::contract::signature(&declaration.sig, &names);
        if !same {
            problems.push(format!(
                "node `{name}`: the graph declares `{graph_says}`, but `{}` is declared `{crate_says}` in {}",
                ty(choice),
                crates[*index].file.display()
            ));
            continue;
        }
        let krate = &crates[*index];
        binds.insert(
            name,
            Bind {
                package: krate.package.clone(),
                lib: krate.lib.clone(),
                path: krate.path.clone(),
                ty: ty(choice),
            },
        );
    }
    (binds, problems)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    /// `bind`, as a result: the bindings when every node binds.
    fn bound(
        graph: &Graph,
        crates: &[NodeCrate],
        declared: &[(usize, Node)],
        pins: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, Bind>, Vec<String>> {
        match super::bind(graph, crates, declared, pins) {
            (binds, problems) if problems.is_empty() => Ok(binds),
            (_, problems) => Err(problems),
        }
    }

    fn graph() -> Graph {
        crate::names::GraphFile::parse(crate::names::tests::CHAT, Path::new("chat.gdl"))
            .expect("graph")
            .graph
    }

    fn krate(lib: &str) -> NodeCrate {
        NodeCrate {
            package: lib.replace('_', "-"),
            lib: lib.to_string(),
            path: PathBuf::from(format!("/nodes/{lib}")),
            file: PathBuf::from(format!("/nodes/{lib}/gdl/nodes.gdl")),
        }
    }

    fn declare(index: usize, text: &str) -> Vec<(usize, Node)> {
        gdl_syntax::declarations(text.parse().expect("tokens"))
            .expect("declarations")
            .into_iter()
            .map(|node| (index, node))
            .collect()
    }

    const FRONT: &str = "operator Preprocess: (Prompt -> stream Chunk) over (Words -> stream Text);
                         forward  Truncate: Words -> Words;
                         backward Number: Chunk -> Chunk;";
    const ENGINE: &str = "sink Engine: Words -> stream Token;";
    const POST: &str = "backward Detokenize: Token -> Piece;
                        backward Format: Piece -> Text;";

    fn chat() -> (Vec<NodeCrate>, Vec<(usize, Node)>) {
        let crates = vec![krate("front"), krate("engine"), krate("post")];
        let mut declared = declare(0, FRONT);
        declared.extend(declare(1, ENGINE));
        declared.extend(declare(2, POST));
        (crates, declared)
    }

    #[test]
    fn each_node_binds_to_the_declared_node_of_its_name() {
        let (crates, declared) = chat();
        let binds = bound(&graph(), &crates, &declared, &BTreeMap::new()).expect("bound");
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
        assert_eq!(binds["Engine"].package, "engine");
        assert_eq!(binds["Engine"].path, Path::new("/nodes/engine"));
    }

    /// A declaration that disagrees with the graph in its role, in whether
    /// a request is streamed, or in a message name is refused.
    #[test]
    fn a_declaration_must_match_the_graph() {
        let (mut crates, mut declared) = chat();
        crates.push(krate("other"));
        declared.retain(|(_, d)| d.ident != "Engine" && d.ident != "Truncate");
        declared.extend(declare(1, "sink Engine: Words -> stream Piece;"));
        declared.extend(declare(0, "backward Truncate: Words -> Words;"));
        let problems = bound(&graph(), &crates, &declared, &BTreeMap::new()).expect_err("mismatch");
        assert_eq!(
            problems,
            [
                "node `Truncate`: the graph declares `forward Truncate: Words -> Words`, but `front::Truncate` is declared `backward Truncate: Words -> Words` in /nodes/front/gdl/nodes.gdl",
                "node `Engine`: the graph declares `sink Engine: Words -> stream Token`, but `engine::Engine` is declared `sink Engine: Words -> stream Piece` in /nodes/engine/gdl/nodes.gdl",
            ]
        );
    }

    #[test]
    fn a_node_declared_twice_binds_only_when_pinned() {
        let (mut crates, mut declared) = chat();
        crates.push(krate("cuda"));
        declared.extend(declare(3, ENGINE));
        let problems = bound(&graph(), &crates, &declared, &BTreeMap::new()).expect_err("twice");
        assert_eq!(
            problems,
            [
                "node `Engine` is declared by more than one [nodes] crate: `engine::Engine`, `cuda::Engine`; choose one under [nodes] pin"
            ]
        );
        let pins = BTreeMap::from([("Engine".to_string(), "cuda::Engine".to_string())]);
        let binds = bound(&graph(), &crates, &declared, &pins).expect("pinned");
        assert_eq!(binds["Engine"].ty, "cuda::Engine");
    }

    /// A pin binds a graph node to a declared node of another name.
    #[test]
    fn a_pin_may_bind_a_node_to_a_declaration_of_another_name() {
        let (crates, mut declared) = chat();
        declared.retain(|(_, d)| d.ident != "Engine");
        declared.extend(declare(1, "sink Mock: Words -> stream Token;"));
        let pins = BTreeMap::from([("Engine".to_string(), "engine::Mock".to_string())]);
        let binds = bound(&graph(), &crates, &declared, &pins).expect("pinned");
        assert_eq!(binds["Engine"].ty, "engine::Mock");
    }

    #[test]
    fn every_unbound_node_and_bad_pin_is_reported() {
        let (crates, mut declared) = chat();
        declared.retain(|(_, d)| d.ident != "Engine" && d.ident != "Format");
        let pins = BTreeMap::from([
            ("Frontend".to_string(), "front::Frontend".to_string()),
            ("Detokenize".to_string(), "post::Detokeniser".to_string()),
        ]);
        let problems = bound(&graph(), &crates, &declared, &pins).expect_err("unbound");
        assert_eq!(
            problems,
            [
                "[nodes] pin `Frontend`: the graph has no node `Frontend` that takes an implementation",
                "node `Engine`: no [nodes] crate declares it; they declare `Detokenize`, `Number`, `Preprocess`, `Truncate`",
                "[nodes] pin `Detokenize`: no [nodes] crate declares `post::Detokeniser`",
                "node `Format`: no [nodes] crate declares it; they declare `Detokenize`, `Number`, `Preprocess`, `Truncate`",
            ]
        );
        let problems = bound(&graph(), &[], &[], &BTreeMap::new()).expect_err("no crates");
        assert_eq!(problems.len(), 6);
        assert!(
            problems[0].contains("the spec lists no [nodes] crate"),
            "{problems:?}"
        );
    }
}
