//! The contract crate generated from a graph file: the graph's `graph!`
//! invocation, with each message name replaced by the full path of the type
//! it resolved to.
//!
//! The paths are rooted and written in full, not imported: the contract
//! hash covers each type as the invocation writes it, so two deployments
//! whose names resolve to different crates get different hashes.

use std::path::Path;

use gdl_syntax::{Graph, Request, Role, Sig};
use syn::Type;

use crate::Resolution;
use crate::emit::MARKER;
use crate::spec::{GdlDep, MessageCrate};

/// The contract crate's `src/lib.rs`: the graph of `file`, each message at
/// the path it resolved to.
pub(crate) fn lib_rs(graph: &Graph, resolution: &Resolution, file: &Path) -> String {
    let path = |name: &str| {
        let path = &resolution.messages[name].path;
        let segments: Vec<String> = path.split("::").map(gdl_syntax::identifier).collect();
        format!("::{}", segments.join("::"))
    };
    let body = body(graph, &path);
    let indented: String = body.lines().map(|line| format!("    {line}\n")).collect();
    let file = file.file_name().unwrap_or_default().to_string_lossy();
    format!(
        "// {MARKER}.\n\
         // Generated from the graph file `{file}`.\n\
         \n\
         //! The contract of graph `{name}`: its graph, with each message named\n\
         //! by the full path of the type it resolved to.\n\
         \n\
         gdl::graph! {{\n{indented}}}\n",
        name = graph.name.value(),
    )
}

/// The contract crate's `Cargo.toml`: it depends on gdl and on each message
/// crate, at the paths `path` writes.
pub(crate) fn cargo_toml(
    package: &str,
    gdl: &GdlDep,
    crates: &[MessageCrate],
    path: &dyn Fn(&Path) -> String,
) -> String {
    let gdl = format!("{{ {} }}", gdl.source(path));
    let mut text = format!(
        "# {MARKER}.\n\
         \n\
         [package]\n\
         name = \"{package}\"\n\
         version = \"0.0.0\"\n\
         edition = \"2024\"\n\
         publish = false\n\
         \n\
         [dependencies]\n\
         gdl = {gdl}\n"
    );
    for krate in crates {
        text.push_str(&format!(
            "{} = {{ {} }}\n",
            krate.package,
            krate.source.source(path)
        ));
    }
    text
}

/// The body of the `graph!` invocation for `graph`, with each message name
/// written as `path(name)` gives it. Printed from the checked model, never
/// by replacing names in the file's text: a node may share its name with a
/// message.
pub(crate) fn body(graph: &Graph, path: &dyn Fn(&str) -> String) -> String {
    let mut text = format!(
        "name = {}, schema = {};\n",
        lit(&graph.name.value()),
        graph.schema.base10_digits()
    );
    for node in &graph.nodes {
        text.push_str(&format!(
            "{:<8} {}: {};\n",
            role(node.role()),
            node.ident,
            signature(&node.sig, path)
        ));
    }
    text.push_str(&format!(
        "topology: {} => {};\n",
        graph.source().ident,
        hop(graph, 0)
    ));
    text
}

/// A node's role as the syntax writes it.
pub(crate) fn role(role: Role) -> &'static str {
    match role {
        Role::Source => "source",
        Role::Operator => "operator",
        Role::Forward => "forward",
        Role::Sink => "sink",
        Role::Backward => "backward",
    }
}

/// A node's signature as the syntax writes it, after the node's name, with
/// each message name written as `path(name)` gives it.
pub(crate) fn signature(sig: &Sig, path: &dyn Fn(&str) -> String) -> String {
    let ty = |ty: &Type| crate::names::message(ty).map_or_else(|| "()".to_string(), |n| path(&n));
    let request = |req: &Request| match (&req.item, crate::names::message(&req.head)) {
        (None, _) => ty(&req.head),
        (Some(item), None) => format!("stream {}", ty(item)),
        (Some(item), Some(_)) => format!("{} + stream {}", ty(&req.head), ty(item)),
    };
    match sig {
        Sig::Source { req, item } | Sig::Sink { req, item } => {
            format!("{} -> stream {}", request(req), ty(item))
        }
        Sig::Operator {
            up_req,
            up_item,
            down_req,
            down_item,
        } => format!(
            "({} -> stream {}) over ({} -> stream {})",
            request(up_req),
            ty(up_item),
            request(down_req),
            ty(down_item)
        ),
        Sig::Forward { input, output } => format!("{} -> {}", request(input), request(output)),
        Sig::Backward { input, output } => format!("{} -> {}", ty(input), ty(output)),
    }
}

/// Hop `index` as the topology writes it after its outer end: forward
/// nodes, the inner end with its own hop in braces, backward nodes.
fn hop(graph: &Graph, index: usize) -> String {
    let hop = &graph.hops[index];
    let mut parts: Vec<String> = hop.forward.iter().map(ToString::to_string).collect();
    // Hops run outermost first, each owned by the previous one's inner end.
    match graph.hops.get(index + 1) {
        Some(next) if next.outer == hop.inner => {
            parts.push(format!(
                "{} {{ {} }}",
                hop.inner,
                self::hop(graph, index + 1)
            ));
        }
        _ => parts.push(hop.inner.to_string()),
    }
    parts.extend(hop.backward.iter().map(ToString::to_string));
    parts.join(" => ")
}

/// A Rust string literal holding `text`.
fn lit(text: &str) -> String {
    format!("{text:?}")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// A message crate listed by version is a dependency on exactly that
    /// version; one listed by directory, a path.
    #[test]
    fn the_contract_depends_on_each_message_crate_as_listed() {
        use crate::spec::CrateSource;
        let crates = [
            MessageCrate {
                package: "chat-protocols".into(),
                lib: "chat_protocols".into(),
                source: CrateSource::Version("0.3.1".into()),
            },
            MessageCrate {
                package: "local-protocols".into(),
                lib: "local_protocols".into(),
                source: CrateSource::Path("/work/local".into()),
            },
        ];
        let text = cargo_toml(
            "chat-contract",
            &GdlDep::Version("0.1".into()),
            &crates,
            &|p| format!("{:?}", p.display().to_string()),
        );
        assert!(
            text.contains("chat-protocols = { version = \"=0.3.1\" }\n"),
            "{text}"
        );
        assert!(
            text.contains("local-protocols = { path = \"/work/local\" }\n"),
            "{text}"
        );
    }
    use crate::names::GraphFile;

    fn checked(text: &str) -> Graph {
        GraphFile::parse(text, Path::new("g.gdl"))
            .expect("graph")
            .graph
    }

    fn reparse(body: &str) -> Graph {
        Graph::parse(body.parse().expect("tokens")).expect("the printed body is a graph")
    }

    fn shape(graph: &Graph) -> Vec<String> {
        let mut out: Vec<String> = graph
            .nodes
            .iter()
            .map(|n| format!("{} {:?}", n.ident, n.role()))
            .collect();
        for hop in &graph.hops {
            out.push(format!(
                "{} [{}] {} [{}]",
                hop.outer,
                hop.forward
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" "),
                hop.inner,
                hop.backward
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
        }
        out
    }

    fn edges(graph: &Graph) -> Vec<String> {
        let show = |ty: &Type| quote::quote!(#ty).to_string();
        let request = |r: &Request| match &r.item {
            None => show(&r.head),
            Some(item) => format!("{} + stream {}", show(&r.head), show(item)),
        };
        let mut out: Vec<String> = graph
            .forward_edges()
            .iter()
            .map(|e| {
                format!(
                    "{} -> {}: {} / {}",
                    e.from,
                    e.to,
                    request(e.sent),
                    request(e.taken)
                )
            })
            .collect();
        out.extend(graph.backward_edges().iter().map(|e| {
            format!(
                "{} -> {}: {} / {}",
                e.from,
                e.to,
                show(e.sent),
                show(e.taken)
            )
        }));
        out
    }

    /// The printed body is the same graph: the same nodes and hops, and
    /// each message replaced by its path on both ends of every edge.
    #[test]
    fn the_printed_body_is_the_same_graph_with_paths() {
        let graph = checked(crate::names::tests::CHAT);
        let path = |name: &str| format!("::chat_protocols::{name}");
        let printed = reparse(&body(&graph, &path));
        assert_eq!(shape(&printed), shape(&graph));
        let expected: Vec<String> = edges(&graph)
            .iter()
            .map(|e| {
                let (nodes, types) = e.split_once(": ").expect("edge");
                let types = types
                    .split(" / ")
                    .map(|t| format!(":: chat_protocols :: {t}"))
                    .collect::<Vec<_>>()
                    .join(" / ");
                format!("{nodes}: {types}")
            })
            .collect();
        assert_eq!(edges(&printed), expected);
    }

    /// A node that shares its name with a message keeps its name, and
    /// streamed requests keep their heads, `()` included.
    #[test]
    fn only_message_positions_take_paths() {
        let graph = checked(
            r#"name = "s", schema = 2;
source   Chunk: Session + stream Chunk -> stream Chunk;
operator Gate: (Session + stream Chunk -> stream Chunk) over (stream Chunk -> stream Chunk);
sink     Sink: stream Chunk -> stream Chunk;
backward Back: Chunk -> Chunk;
topology: Chunk => Gate { Sink => Back };
"#,
        );
        let printed = body(&graph, &|name| format!("::p::{name}"));
        assert_eq!(
            printed,
            "name = \"s\", schema = 2;\n\
             source   Chunk: ::p::Session + stream ::p::Chunk -> stream ::p::Chunk;\n\
             operator Gate: (::p::Session + stream ::p::Chunk -> stream ::p::Chunk) over (stream ::p::Chunk -> stream ::p::Chunk);\n\
             sink     Sink: stream ::p::Chunk -> stream ::p::Chunk;\n\
             backward Back: ::p::Chunk -> ::p::Chunk;\n\
             topology: Chunk => Gate { Sink => Back };\n"
        );
        assert_eq!(shape(&reparse(&printed)), shape(&graph));
    }
}
