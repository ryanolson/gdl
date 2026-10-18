//! A checked graph: every node declared once, the topology well formed, and
//! every hop's stages and edge types worked out. Type agreement along an
//! edge is left to the reader: in `graph!` the expansion states each edge's
//! type once, so a mismatch is a type error at the node's binding; for a
//! graph file, `gdl-gen` compares the names on each of [`Graph::edges`].

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::{Ident, LitInt, LitStr, Type};

use crate::parse::{Element, GraphInput, Request, Sig};

/// A graph whose structure is checked: the result of [`Graph::parse`].
pub struct Graph {
    /// The graph's name.
    pub name: LitStr,
    /// The schema version.
    pub schema: LitInt,
    /// Every node, in declaration order.
    pub nodes: Vec<Node>,
    /// Outermost first.
    pub hops: Vec<Hop>,
}

/// One declared node.
pub struct Node {
    /// The node's name.
    pub ident: Ident,
    /// Its role and port types.
    pub sig: Sig,
    /// The node's setter on `Bindings` and its field there: the node's name
    /// in snake case, raw when it is a keyword. The source has none.
    pub binding: Option<Ident>,
}

/// One hop: outer end, forward chain, inner end, backward chain.
pub struct Hop {
    /// The source or operator that owns the hop.
    pub outer: Ident,
    /// Forward nodes in request order.
    pub forward: Vec<Ident>,
    /// The sink or operator at the inner end.
    pub inner: Ident,
    /// Backward nodes in response order.
    pub backward: Vec<Ident>,
    /// The request on each forward edge, outer end first:
    /// `forward.len() + 1`.
    pub forward_types: Vec<Request>,
    /// The type on each backward edge, inner end first: `backward.len() + 1`.
    pub backward_types: Vec<Type>,
}

impl Node {
    /// The node's role, from its signature.
    pub fn role(&self) -> Role {
        match self.sig {
            Sig::Source { .. } => Role::Source,
            Sig::Operator { .. } => Role::Operator,
            Sig::Forward { .. } => Role::Forward,
            Sig::Sink { .. } => Role::Sink,
            Sig::Backward { .. } => Role::Backward,
        }
    }
}

/// A node's role.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    /// The graph's entry point.
    Source,
    /// Owns a nested hop and sees both directions.
    Operator,
    /// Transforms the request.
    Forward,
    /// Ends a hop and produces its response.
    Sink,
    /// Transforms the response.
    Backward,
}

impl Graph {
    /// Parse a graph body and check its structure: the macro's input, or a
    /// graph file's text as tokens.
    pub fn parse(tokens: proc_macro2::TokenStream) -> syn::Result<Self> {
        Self::check(syn::parse2::<GraphInput>(tokens)?)
    }

    fn check(input: GraphInput) -> syn::Result<Self> {
        let mut by_name: BTreeMap<String, usize> = BTreeMap::new();
        for (index, node) in input.nodes.iter().enumerate() {
            if by_name.insert(node.ident.to_string(), index).is_some() {
                return Err(syn::Error::new(node.ident.span(), "node declared twice"));
            }
        }
        let mut bindings: BTreeMap<String, Ident> = BTreeMap::new();
        let mut nodes: Vec<Node> = Vec::with_capacity(input.nodes.len());
        for decl in input.nodes {
            // The name goes into the manifest and the generated segment
            // code as written; `r#` would have to be stripped in one and
            // kept in the other.
            if decl.ident.to_string().starts_with("r#") {
                return Err(syn::Error::new(
                    decl.ident.span(),
                    "a node name cannot be a raw identifier",
                ));
            }
            let binding = match decl.sig {
                Sig::Source { .. } => None,
                _ => Some(binding(&decl.ident, &mut bindings)?),
            };
            nodes.push(Node {
                ident: decl.ident,
                sig: decl.sig,
                binding,
            });
        }
        let sources: Vec<&Node> = nodes.iter().filter(|n| n.role() == Role::Source).collect();
        match sources.as_slice() {
            [_] => {}
            [] => {
                return Err(syn::Error::new(
                    input.topology_span,
                    "a graph has exactly one source",
                ));
            }
            [_, second, ..] => {
                return Err(syn::Error::new(
                    second.ident.span(),
                    "a graph has exactly one source",
                ));
            }
        }

        let mut builder = Builder {
            nodes: &nodes,
            by_name: &by_name,
            used: BTreeSet::new(),
            hops: Vec::new(),
        };
        let mut list = input.topology.into_iter();
        let first = list
            .next()
            .ok_or_else(|| syn::Error::new(input.topology_span, "the topology is empty"))?;
        let source = builder.node(&first.ident)?;
        if source.role() != Role::Source || first.nested.is_some() {
            return Err(syn::Error::new(
                first.ident.span(),
                "the topology starts with the source, with no braces",
            ));
        }
        builder.hop(first.ident, list.collect(), input.topology_span)?;

        for node in &nodes {
            if !builder.used.contains(&node.ident.to_string()) {
                return Err(syn::Error::new(
                    node.ident.span(),
                    "node is declared but not in the topology",
                ));
            }
        }
        let hops = builder.hops;
        Ok(Self {
            name: input.name,
            schema: input.schema,
            nodes,
            hops,
        })
    }

    /// The declared node named `ident`, which must be in the graph.
    pub fn node(&self, ident: &Ident) -> &Node {
        self.nodes
            .iter()
            .find(|n| n.ident == *ident)
            .expect("checked: every topology node is declared")
    }

    /// The graph's one source.
    pub fn source(&self) -> &Node {
        self.nodes
            .iter()
            .find(|n| n.role() == Role::Source)
            .expect("checked: one source")
    }
}

/// The binding name of a non-source node, checked against the names the
/// expansion already uses: `Bindings` has a method per binding, so a binding
/// must be a method name, unique among the nodes, and not one of the methods
/// `Bindings` has anyway.
fn binding(ident: &Ident, taken: &mut BTreeMap<String, Ident>) -> syn::Result<Ident> {
    let name = ident.to_string();
    let snake = snake_case(&name);
    if matches!(snake.as_str(), "crate" | "self" | "super") {
        return Err(syn::Error::new(
            ident.span(),
            format!(
                "node `{name}` gets the binding name `{snake}`, which cannot name a method; rename the node"
            ),
        ));
    }
    if matches!(snake.as_str(), "new" | "default") {
        return Err(syn::Error::new(
            ident.span(),
            format!(
                "node `{name}` gets the binding name `{snake}`, which `Bindings` already uses; rename the node"
            ),
        ));
    }
    if let Some(first) = taken.get(&snake) {
        return Err(syn::Error::new(
            ident.span(),
            format!("nodes `{first}` and `{name}` both get the binding name `{snake}`; rename one"),
        ));
    }
    taken.insert(snake.clone(), ident.clone());
    Ok(if is_keyword(&snake) {
        Ident::new_raw(&snake, ident.span())
    } else {
        Ident::new(&snake, ident.span())
    })
}

fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (index, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// Every keyword a raw identifier can spell: the strict and reserved
/// keywords of every edition (`crate`, `self`, `super`, and `Self` cannot be
/// raw, and are rejected above). Escaping a word that is a keyword only in a
/// later edition costs nothing.
fn is_keyword(word: &str) -> bool {
    matches!(
        word,
        "abstract"
            | "as"
            | "async"
            | "await"
            | "become"
            | "box"
            | "break"
            | "const"
            | "continue"
            | "do"
            | "dyn"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "final"
            | "fn"
            | "for"
            | "gen"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "macro"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "override"
            | "priv"
            | "pub"
            | "ref"
            | "return"
            | "static"
            | "struct"
            | "trait"
            | "true"
            | "try"
            | "type"
            | "typeof"
            | "unsafe"
            | "unsized"
            | "use"
            | "virtual"
            | "where"
            | "while"
            | "yield"
    )
}

struct Builder<'a> {
    nodes: &'a [Node],
    by_name: &'a BTreeMap<String, usize>,
    used: BTreeSet<String>,
    hops: Vec<Hop>,
}

impl Builder<'_> {
    fn node(&self, ident: &Ident) -> syn::Result<&Node> {
        self.by_name
            .get(&ident.to_string())
            .map(|&index| &self.nodes[index])
            .ok_or_else(|| syn::Error::new(ident.span(), "unknown node"))
    }

    fn take(&mut self, ident: &Ident) -> syn::Result<()> {
        if !self.used.insert(ident.to_string()) {
            return Err(syn::Error::new(
                ident.span(),
                "node appears twice in the topology",
            ));
        }
        Ok(())
    }

    /// Build the hop owned by `outer` from the elements after it, then the
    /// hops nested inside its inner end.
    fn hop(&mut self, outer: Ident, list: Vec<Element>, span: Span) -> syn::Result<()> {
        self.take(&outer)?;
        let mut forward = Vec::new();
        let mut elements = list.into_iter();
        let inner = loop {
            let element = elements
                .next()
                .ok_or_else(|| syn::Error::new(span, "a hop needs a sink or an operator"))?;
            match self.node(&element.ident)?.role() {
                Role::Forward if element.nested.is_none() => forward.push(element.ident),
                Role::Sink if element.nested.is_some() => {
                    return Err(syn::Error::new(
                        element.ident.span(),
                        "a sink owns no nested hop",
                    ));
                }
                Role::Operator if element.nested.is_none() => {
                    return Err(syn::Error::new(
                        element.ident.span(),
                        "an operator needs its hop in braces: `Op { ... }`",
                    ));
                }
                Role::Sink | Role::Operator => break element,
                Role::Forward => {
                    return Err(syn::Error::new(
                        element.ident.span(),
                        "only an operator owns a nested hop",
                    ));
                }
                Role::Source => {
                    return Err(syn::Error::new(
                        element.ident.span(),
                        "the source can only start the topology",
                    ));
                }
                Role::Backward => {
                    return Err(syn::Error::new(
                        element.ident.span(),
                        "a backward node cannot come before the hop's sink or operator",
                    ));
                }
            }
        };
        let mut backward = Vec::new();
        for element in elements {
            let node = self.node(&element.ident)?;
            if node.role() != Role::Backward || element.nested.is_some() {
                return Err(syn::Error::new(
                    element.ident.span(),
                    "after a hop's sink or operator, only backward nodes follow",
                ));
            }
            backward.push(element.ident);
        }

        let outer_node = self.node(&outer)?;
        let down_req = match &outer_node.sig {
            Sig::Source { req, .. } => req.clone(),
            Sig::Operator { down_req, .. } => down_req.clone(),
            _ => unreachable!("checked: the outer end is a source or an operator"),
        };
        let mut forward_types = vec![down_req];
        for ident in &forward {
            if let Sig::Forward { input, output } = &self.node(ident)?.sig {
                agree(forward_types.last(), input)?;
                forward_types.push(output.clone());
            }
        }
        let inner_node = self.node(&inner.ident)?;
        match &inner_node.sig {
            Sig::Sink { req, .. } | Sig::Operator { up_req: req, .. } => {
                agree(forward_types.last(), req)?;
            }
            _ => unreachable!("checked: the inner end is a sink or an operator"),
        }
        let inner_item = match &inner_node.sig {
            Sig::Sink { item, .. } => item.clone(),
            Sig::Operator { up_item, .. } => up_item.clone(),
            _ => unreachable!("checked: the inner end is a sink or an operator"),
        };
        let mut backward_types = vec![inner_item];
        for ident in &backward {
            if let Sig::Backward { output, .. } = &self.node(ident)?.sig {
                backward_types.push(output.clone());
            }
        }
        let inner_role = inner_node.role();
        for ident in forward.iter().chain(&backward) {
            self.take(ident)?;
        }

        self.hops.push(Hop {
            outer,
            forward,
            inner: inner.ident.clone(),
            backward,
            forward_types,
            backward_types,
        });
        match (inner_role, inner.nested) {
            (Role::Sink, _) => self.take(&inner.ident),
            // The operator is this hop's inner end and the next hop's outer
            // end; `hop` marks it used.
            (Role::Operator, Some(nested)) => {
                self.hop(inner.ident.clone(), nested, inner.ident.span())
            }
            _ => unreachable!("checked: a sink, or an operator with its hop"),
        }
    }
}

/// A forward edge carries a stream at both of its ends or at neither. The
/// types on it are checked by the compiler; this gives the streamed and the
/// plain form of one type a clear error instead of a type mismatch.
fn agree(sent: Option<&Request>, declared: &Request) -> syn::Result<()> {
    let Some(sent) = sent else {
        return Ok(());
    };
    match (&sent.item, &declared.item) {
        (Some(_), None) => Err(syn::Error::new(
            declared.head.span(),
            "the request on this edge is streamed: declare it as `Head + stream Item`",
        )),
        (None, Some(item)) => Err(syn::Error::new(
            item.span(),
            "the request on this edge is not streamed: declare it without `stream`",
        )),
        _ => Ok(()),
    }
}
