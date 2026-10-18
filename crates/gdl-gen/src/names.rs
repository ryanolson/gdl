//! Reading a graph file: the body of a `graph!` invocation, whose message
//! positions hold message names instead of Rust types.
//!
//! The compiler checks that the types along a `graph!` edge agree. A graph
//! file is checked before any crate is compiled, so the names along each
//! edge are compared here.

use std::collections::BTreeMap;
use std::path::Path;

use gdl_syntax::{Graph, Request, Sig};
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::Type;
use syn::spanned::Spanned as _;

use crate::Error;

/// A graph file whose structure is checked and whose edges agree.
#[derive(Debug)]
pub(crate) struct GraphFile {
    /// Each message name, with the nodes that declare it, in declaration
    /// order.
    pub(crate) uses: BTreeMap<String, Vec<String>>,
}

/// What a message position holds.
#[derive(PartialEq, Eq)]
enum Name {
    /// `()`: the head of a pure stream, or an empty value.
    Unit,
    /// A message name.
    Message(String),
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unit => f.write_str("()"),
            Self::Message(name) => f.write_str(name),
        }
    }
}

impl GraphFile {
    /// Read and check the graph file at `path`.
    pub(crate) fn read(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::new(format!("reading {}: {e}", path.display())))?;
        Self::parse(&text, path)
    }

    /// Check `text`, read from `file`. Every problem is reported at once,
    /// each with its line and column, except a structural one: as in
    /// `graph!`, the first of those stops the check.
    pub(crate) fn parse(text: &str, file: &Path) -> Result<Self, Error> {
        let at = |span: Span, message: &dyn std::fmt::Display| {
            let start = span.start();
            format!(
                "{}:{}:{}: {message}",
                file.display(),
                start.line,
                start.column + 1
            )
        };
        let tokens: TokenStream = text
            .parse()
            .map_err(|e: proc_macro2::LexError| Error::new(at(e.span(), &e)))?;
        let graph = Graph::parse(tokens).map_err(|error| {
            let lines: Vec<String> = error.into_iter().map(|e| at(e.span(), &e)).collect();
            Error::new(lines.join("\n"))
        })?;

        let mut problems = Vec::new();
        let mut uses: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for node in &graph.nodes {
            for ty in port_types(&node.sig) {
                match name(ty) {
                    Ok(Name::Message(name)) => {
                        let users = uses.entry(name).or_default();
                        let user = node.ident.to_string();
                        if !users.contains(&user) {
                            users.push(user);
                        }
                    }
                    Ok(Name::Unit) => {}
                    Err(()) => problems.push(at(
                        ty.span(),
                        &format!(
                            "a graph file names messages, and `{}` is not a message name",
                            ty.to_token_stream()
                        ),
                    )),
                }
            }
        }
        // The `()` that a bare `stream Item` stands for carries the span of
        // its `stream` keyword, so every position has a place in the file.
        let mut differ = |from: &syn::Ident, to: &syn::Ident, sent: &Type, taken_ty: &Type| {
            // A position that is not a name is reported above.
            if let (Ok(sent), Ok(taken)) = (name(sent), name(taken_ty))
                && sent != taken
            {
                problems.push(at(
                    taken_ty.span(),
                    &format!("`{to}` takes `{taken}`, but `{from}` sends `{sent}`"),
                ));
            }
        };
        for edge in graph.forward_edges() {
            differ(edge.from, edge.to, &edge.sent.head, &edge.taken.head);
            // `graph!`'s own check makes both ends streamed or neither.
            if let (Some(sent), Some(taken)) = (&edge.sent.item, &edge.taken.item) {
                differ(edge.from, edge.to, sent, taken);
            }
        }
        for edge in graph.backward_edges() {
            differ(edge.from, edge.to, edge.sent, edge.taken);
        }
        if problems.is_empty() {
            Ok(Self { uses })
        } else {
            Err(Error::new(problems.join("\n")))
        }
    }
}

/// The message name a position holds: a bare identifier, or `()`.
fn name(ty: &Type) -> Result<Name, ()> {
    match ty {
        Type::Tuple(tuple) if tuple.elems.is_empty() => Ok(Name::Unit),
        Type::Path(path) if path.qself.is_none() => match path.path.get_ident() {
            Some(ident) => Ok(Name::Message(ident.to_string())),
            None => Err(()),
        },
        _ => Err(()),
    }
}

/// Every message position of a node, in declaration order.
fn port_types(sig: &Sig) -> Vec<&Type> {
    fn request(req: &Request) -> impl Iterator<Item = &Type> {
        std::iter::once(&req.head).chain(&req.item)
    }
    match sig {
        Sig::Source { req, item } | Sig::Sink { req, item } => {
            request(req).chain(std::iter::once(item)).collect()
        }
        Sig::Operator {
            up_req,
            up_item,
            down_req,
            down_item,
        } => request(up_req)
            .chain(std::iter::once(up_item))
            .chain(request(down_req))
            .chain(std::iter::once(down_item))
            .collect(),
        Sig::Forward { input, output } => request(input).chain(request(output)).collect(),
        Sig::Backward { input, output } => vec![input, output],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<GraphFile, String> {
        GraphFile::parse(text, Path::new("chat.gdl")).map_err(|e| e.to_string())
    }

    const CHAT: &str = r#"name = "chat", schema = 1;
source   Frontend: Prompt -> stream Chunk;
operator Preprocess: (Prompt -> stream Chunk) over (Words -> stream Text);
forward  Truncate: Words -> Words;
sink     Engine: Words -> stream Token;
backward Detokenize: Token -> Piece;
backward Format: Piece -> Text;
backward Number: Chunk -> Chunk;
topology: Frontend => Preprocess { Truncate => Engine => Detokenize => Format } => Number;
"#;

    #[test]
    fn a_graph_file_lists_each_message_with_the_nodes_that_declare_it() {
        let file = parse(CHAT).expect("graph");
        let names: Vec<&str> = file.uses.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            ["Chunk", "Piece", "Prompt", "Text", "Token", "Words"]
        );
        assert_eq!(file.uses["Words"], ["Preprocess", "Truncate", "Engine"]);
    }

    /// Every disagreement is reported, each at the receiving end's type.
    #[test]
    fn every_edge_whose_names_differ_is_reported_with_its_position() {
        let text = CHAT
            .replace("sink     Engine: Words", "sink     Engine: Tokens")
            .replace("backward Format: Piece", "backward Format: Pieces");
        let error = parse(&text).expect_err("the names differ");
        assert_eq!(
            error,
            "chat.gdl:5:18: `Engine` takes `Tokens`, but `Truncate` sends `Words`\n\
             chat.gdl:7:18: `Format` takes `Pieces`, but `Detokenize` sends `Piece`"
        );
    }

    #[test]
    fn a_streamed_edge_compares_its_head_and_its_item() {
        let text = r#"name = "asr", schema = 1;
source   Client: Session + stream Audio -> stream Transcript;
forward  Resample: Session + stream Audio -> Session + stream Pcm;
sink     Recognizer: Session + stream Pcmm -> stream Transcript;
topology: Client => Resample => Recognizer;
"#;
        let error = parse(text).expect_err("the items differ");
        assert_eq!(
            error,
            "chat.gdl:4:39: `Recognizer` takes `Pcmm`, but `Resample` sends `Pcm`"
        );
        let unit = text.replace("Session + stream Pcmm", "stream Pcm");
        let error = parse(&unit).expect_err("the heads differ");
        assert_eq!(
            error,
            "chat.gdl:4:22: `Recognizer` takes `()`, but `Resample` sends `Session`"
        );
    }

    /// Whether an edge is streamed is structure, checked before names, as
    /// in `graph!`; every edge whose ends disagree on it is reported.
    #[test]
    fn every_edge_that_disagrees_on_streaming_is_reported() {
        let text = r#"name = "g", schema = 1;
source   A: Head + stream Item -> stream Reply;
forward  B: Head -> Head + stream Item;
sink     C: Head -> stream Reply;
topology: A => B => C;
"#;
        let error = parse(text).expect_err("not streamed");
        assert_eq!(
            error,
            "chat.gdl:3:13: the request on this edge is streamed: declare it as `Head + stream Item`\n\
             chat.gdl:4:13: the request on this edge is streamed: declare it as `Head + stream Item`"
        );
    }

    #[test]
    fn a_rust_type_is_not_a_message_name() {
        let text = CHAT
            .replace(
                "forward  Truncate: Words -> Words;",
                "forward  Truncate: Words -> chat::Words;",
            )
            .replace(
                "sink     Engine: Words -> stream Token;",
                "sink     Engine: Words -> stream Vec<Token>;",
            );
        let error = parse(&text).expect_err("not names");
        assert_eq!(
            error,
            "chat.gdl:4:29: a graph file names messages, and `chat :: Words` is not a message name\n\
             chat.gdl:5:34: a graph file names messages, and `Vec < Token >` is not a message name"
        );
    }

    #[test]
    fn a_structural_error_points_into_the_file() {
        let text = CHAT.replace(
            "topology: Frontend => Preprocess",
            "topology: Frontend => Truncate => Preprocess",
        );
        let error = parse(&text).expect_err("not a hop");
        assert!(error.starts_with("chat.gdl:9:"), "{error}");
        let error =
            parse("name = \"x\", schema = 1; source A: \"unterminated").expect_err("not tokens");
        assert!(error.starts_with("chat.gdl:1:"), "{error}");
    }
}
