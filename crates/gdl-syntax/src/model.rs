//! A checked graph: every node declared once, the topology well formed, and
//! every hop's stages and edge types worked out. Type agreement along an
//! edge is left to the reader: in `graph!` the expansion states each edge's
//! type once, so a mismatch is a type error at the node's binding; for a
//! graph file, `gdl-gen` compares the names on each of [`Graph::edges`].

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::{Ident, LitInt, LitStr, Type};

use gdl_model::Role;

use crate::parse::{Element, GraphInput, PropertyDecl, Request, Sig};

/// A graph whose structure is checked: the result of [`Graph::parse`].
pub struct Graph {
    /// The graph's name.
    pub name: LitStr,
    /// The schema version.
    pub schema: LitInt,
    /// Every node, in declaration order.
    pub nodes: Vec<Node>,
    /// Each source root followed by its nested hops, in topology order.
    pub hops: Vec<Hop>,
    /// Every declared property kind, in declaration order.
    pub properties: Vec<Property>,
}

/// One declared property kind: `property model: Model;`.
pub struct Property {
    /// The kind's name.
    pub kind: Ident,
    /// The message type of its keys.
    pub ty: Type,
}

/// One declared node.
pub struct Node {
    /// The node's name.
    pub ident: Ident,
    /// Its role and port types.
    pub sig: Sig,
    /// The node's setter on `Bindings` and its field there: the node's name
    /// in snake case, raw when it is a keyword. Sources and built-in identities have none.
    pub binding: Option<Ident>,
    /// The property kinds its instance has: `provides model`.
    pub provides: Vec<Ident>,
    /// The property kinds it reads, each with the class whose pools it
    /// reads them from: `needs model from sets`.
    pub needs: Vec<(Ident, Ident)>,
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

/// One edge: the type its sending end declares, and the type its receiving
/// end declares. In a well-typed graph the two are the same type.
pub struct Edge<'a, T> {
    /// The sending node.
    pub from: &'a Ident,
    /// The receiving node.
    pub to: &'a Ident,
    /// What `from` declares it sends.
    pub sent: &'a T,
    /// What `to` declares it takes.
    pub taken: &'a T,
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
            Sig::Around { .. } => Role::Around,
        }
    }
}

impl Graph {
    /// Parse a graph body and check its structure: the macro's input, or a
    /// graph file's text as tokens. Text that nests deeper than
    /// [`MAX_DEPTH`](crate::MAX_DEPTH) is refused before it is parsed, so
    /// no text can overflow the caller's stack.
    pub fn parse(tokens: proc_macro2::TokenStream) -> syn::Result<Self> {
        crate::check_depth(&tokens, crate::MAX_DEPTH, &|| {
            format!(
                "this nests more than {} levels deep, and a graph may not: \
                 each bracket, `<`, prefix operator, and keyword such as `return` is a level",
                crate::MAX_DEPTH
            )
        })?;
        Self::check(syn::parse2::<GraphInput>(tokens)?)
    }

    fn check(input: GraphInput) -> syn::Result<Self> {
        let mut by_name: BTreeMap<String, usize> = BTreeMap::new();
        for (index, node) in input.nodes.iter().enumerate() {
            if by_name.insert(node.ident.to_string(), index).is_some() {
                return Err(syn::Error::new(node.ident.span(), "node declared twice"));
            }
        }
        let properties = properties(input.properties)?;
        let mut bindings: BTreeMap<String, Ident> = BTreeMap::new();
        let mut nodes: Vec<Node> = Vec::with_capacity(input.nodes.len());
        for decl in input.nodes {
            declared_uses(&decl, &properties)?;
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
                _ if decl.builtin => None,
                _ => Some(binding(&decl.ident, &mut bindings)?),
            };
            nodes.push(Node {
                ident: decl.ident,
                sig: decl.sig,
                binding,
                provides: decl.provides,
                needs: decl.needs,
            });
        }
        let sources: Vec<&Node> = nodes.iter().filter(|n| n.role() == Role::Source).collect();
        if sources.is_empty() {
            return Err(syn::Error::new(
                input.topology_span,
                "a graph needs a source",
            ));
        }

        let mut builder = Builder {
            nodes: &nodes,
            by_name: &by_name,
            used: BTreeSet::new(),
            hops: Vec::new(),
            streaming: None,
        };
        for topology in input.topology {
            let mut list = topology.into_iter();
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
        }

        for node in &nodes {
            if !builder.used.contains(&node.ident.to_string()) {
                return Err(syn::Error::new(
                    node.ident.span(),
                    "node is declared but not in the topology",
                ));
            }
        }
        if let Some(error) = builder.streaming {
            return Err(error);
        }
        let hops = builder.hops;
        Ok(Self {
            name: input.name,
            schema: input.schema,
            nodes,
            hops,
            properties,
        })
    }

    /// Every request edge, outermost hop first, each in request order.
    pub fn forward_edges(&self) -> Vec<Edge<'_, Request>> {
        let mut edges = Vec::new();
        for hop in &self.hops {
            let path: Vec<&Ident> = std::iter::once(&hop.outer)
                .chain(&hop.forward)
                .chain(std::iter::once(&hop.inner))
                .collect();
            for pair in path.windows(2) {
                let (from, to) = (pair[0], pair[1]);
                let sent = match &self.node(from).sig {
                    Sig::Source { req, .. } => req,
                    Sig::Operator { down_req, .. } => down_req,
                    Sig::Forward { output, .. } => output,
                    Sig::Around { down_req, .. } => down_req,
                    Sig::Sink { .. } | Sig::Backward { .. } => {
                        unreachable!("checked: a request leaves a source, operator, or forward")
                    }
                };
                let taken = match &self.node(to).sig {
                    Sig::Forward { input, .. } => input,
                    Sig::Sink { req, .. } => req,
                    Sig::Operator { up_req, .. } | Sig::Around { up_req, .. } => up_req,
                    Sig::Source { .. } | Sig::Backward { .. } => {
                        unreachable!("checked: a request reaches a forward, sink, or operator")
                    }
                };
                edges.push(Edge {
                    from,
                    to,
                    sent,
                    taken,
                });
            }
        }
        edges
    }

    /// Every response edge, outermost hop first, each in response order.
    pub fn backward_edges(&self) -> Vec<Edge<'_, Type>> {
        let mut edges = Vec::new();
        for hop in &self.hops {
            let path: Vec<&Ident> = std::iter::once(&hop.inner)
                .chain(&hop.backward)
                .chain(std::iter::once(&hop.outer))
                .collect();
            for pair in path.windows(2) {
                let (from, to) = (pair[0], pair[1]);
                let sent = match &self.node(from).sig {
                    Sig::Sink { item, .. } => item,
                    Sig::Operator { up_item, .. } => up_item,
                    Sig::Backward { output, .. } => output,
                    Sig::Around { up_item, .. } => up_item,
                    Sig::Source { .. } | Sig::Forward { .. } => {
                        unreachable!("checked: a response leaves a sink, operator, or backward")
                    }
                };
                let taken = match &self.node(to).sig {
                    Sig::Backward { input, .. } => input,
                    Sig::Source { item, .. } => item,
                    Sig::Operator { down_item, .. } | Sig::Around { down_item, .. } => down_item,
                    Sig::Sink { .. } | Sig::Forward { .. } => {
                        unreachable!("checked: a response reaches a backward, source, or operator")
                    }
                };
                edges.push(Edge {
                    from,
                    to,
                    sent,
                    taken,
                });
            }
        }
        edges
    }

    /// The declared node named `ident`, which must be in the graph.
    pub fn node(&self, ident: &Ident) -> &Node {
        self.nodes
            .iter()
            .find(|n| n.ident == *ident)
            .expect("checked: every topology node is declared")
    }

    /// The first declared source, which may host the frontend.
    pub fn source(&self) -> &Node {
        self.nodes
            .iter()
            .find(|n| n.role() == Role::Source)
            .expect("checked: at least one source")
    }
}

/// The declared kinds, each a property kind's name, once.
fn properties(decls: Vec<PropertyDecl>) -> syn::Result<Vec<Property>> {
    let mut properties: Vec<Property> = Vec::with_capacity(decls.len());
    for PropertyDecl { kind, ty } in decls {
        let name = kind.to_string();
        gdl_model::check_property_kind(&name).map_err(|why| syn::Error::new(kind.span(), why))?;
        if properties.iter().any(|p| p.kind == kind) {
            return Err(syn::Error::new(
                kind.span(),
                format!("property kind `{name}` is declared twice"),
            ));
        }
        properties.push(Property { kind, ty });
    }
    Ok(properties)
}

/// What a node provides and needs: declared kinds, each once, read from
/// classes of a class name's shape, and none for a built-in identity,
/// which has no implementation to set or read them.
fn declared_uses(decl: &crate::parse::NodeDecl, properties: &[Property]) -> syn::Result<()> {
    let node = &decl.ident;
    if decl.builtin
        && let Some(first) = decl.provides.first().or(decl.needs.first().map(|(k, _)| k))
    {
        return Err(syn::Error::new(
            first.span(),
            "a built-in identity has no implementation, so it provides and needs no property",
        ));
    }
    let declared = |kind: &Ident| -> syn::Result<()> {
        if properties.iter().any(|p| p.kind == *kind) {
            return Ok(());
        }
        Err(syn::Error::new(
            kind.span(),
            format!(
                "property kind `{kind}` is not declared: declare it with `property {kind}: <Type>;`"
            ),
        ))
    };
    for (index, kind) in decl.provides.iter().enumerate() {
        declared(kind)?;
        if decl.provides[..index].contains(kind) {
            return Err(syn::Error::new(
                kind.span(),
                format!("node `{node}` provides property kind `{kind}` twice"),
            ));
        }
    }
    for (index, (kind, class)) in decl.needs.iter().enumerate() {
        declared(kind)?;
        gdl_model::check_class_name(&class.to_string())
            .map_err(|why| syn::Error::new(class.span(), why))?;
        if decl.needs[..index].contains(&(kind.clone(), class.clone())) {
            return Err(syn::Error::new(
                kind.span(),
                format!("node `{node}` needs property kind `{kind}` from class `{class}` twice"),
            ));
        }
    }
    Ok(())
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

/// The message name at a message position of a graph file: `Some` for a bare name, with any `r#` dropped (`r#Prompt` is
/// `Prompt`); `None` for `()`; `Err` for anything else, which is a Rust
/// type, not a message name.
#[allow(clippy::result_unit_err)]
pub fn message_name(ty: &Type) -> Result<Option<String>, ()> {
    use syn::ext::IdentExt as _;
    match ty {
        Type::Tuple(tuple) if tuple.elems.is_empty() => Ok(None),
        Type::Path(path) if path.qself.is_none() => path
            .path
            .get_ident()
            .map(|ident| Some(ident.unraw().to_string()))
            .ok_or(()),
        _ => Err(()),
    }
}

/// `word` as Rust code must spell it: raw (`r#gen`) when it is a keyword of
/// any edition, as is.
pub fn identifier(word: &str) -> String {
    if is_keyword(word) {
        format!("r#{word}")
    } else {
        word.to_string()
    }
}

/// Every keyword a raw identifier can spell: the strict and reserved
/// keywords of every edition (`crate`, `self`, `super`, and `Self` cannot be
/// raw, and are rejected above). Escaping a word that is a keyword only in a
/// later edition costs nothing.
pub(crate) fn is_keyword(word: &str) -> bool {
    KEYWORDS.contains(&word)
}

/// The keywords of every edition, strict and reserved.
pub(crate) const KEYWORDS: [&str; 48] = [
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "do", "dyn",
    "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl", "in", "let",
    "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref", "return",
    "static", "struct", "trait", "true", "try", "type", "typeof", "unsafe", "unsized", "use",
    "virtual", "where", "while", "yield",
];

struct Builder<'a> {
    nodes: &'a [Node],
    by_name: &'a BTreeMap<String, usize>,
    used: BTreeSet<String>,
    hops: Vec<Hop>,
    /// Every edge whose ends disagree on streaming. One such edge leaves
    /// the rest of the graph well formed, so all are collected and
    /// reported together once the topology is checked.
    streaming: Option<syn::Error>,
}

impl<'a> Builder<'a> {
    fn node(&self, ident: &Ident) -> syn::Result<&'a Node> {
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
        let (mut forward, mut backward) = (Vec::new(), Vec::new());
        let inner = self.stages(list, span, &mut forward, &mut backward)?;

        let outer_node = self.node(&outer)?;
        let down_req = match &outer_node.sig {
            Sig::Source { req, .. } => req.clone(),
            Sig::Operator { down_req, .. } => down_req.clone(),
            _ => unreachable!("checked: the outer end is a source or an operator"),
        };
        let mut forward_types = vec![down_req];
        for ident in &forward {
            match &self.node(ident)?.sig {
                Sig::Forward { input, output }
                | Sig::Around {
                    up_req: input,
                    down_req: output,
                    ..
                } => {
                    self.agree(forward_types.last(), input);
                    forward_types.push(output.clone());
                }
                _ => {}
            }
        }
        let inner_node = self.node(&inner.ident)?;
        match &inner_node.sig {
            Sig::Sink { req, .. } | Sig::Operator { up_req: req, .. } => {
                self.agree(forward_types.last(), req);
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
            match &self.node(ident)?.sig {
                Sig::Backward { output, .. }
                | Sig::Around {
                    up_item: output, ..
                } => backward_types.push(output.clone()),
                _ => {}
            }
        }
        let inner_role = inner_node.role();
        // An around node is in both chains, and is used once.
        let only_backward = backward.iter().filter(|b| !forward.contains(b));
        for ident in forward.iter().chain(only_backward) {
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
            (Role::Sink, _) => {
                self.used.insert(inner.ident.to_string());
                Ok(())
            }
            // The operator is this hop's inner end and the next hop's outer
            // end; `hop` marks it used.
            (Role::Operator, Some(nested)) => {
                self.hop(inner.ident.clone(), nested, inner.ident.span())
            }
            _ => unreachable!("checked: a sink, or an operator with its hop"),
        }
    }
}

impl Builder<'_> {
    /// The stages of a hop after its outer end: forward nodes, then its inner
    /// end, then backward nodes, into `forward` and `backward`. An around
    /// node's braces hold the rest of the same hop, so its forward half
    /// comes before what they hold and its backward half after. Returns the
    /// inner end.
    fn stages(
        &mut self,
        list: Vec<Element>,
        span: Span,
        forward: &mut Vec<Ident>,
        backward: &mut Vec<Ident>,
    ) -> syn::Result<Element> {
        let mut elements = list.into_iter();
        let inner = loop {
            let element = elements
                .next()
                .ok_or_else(|| syn::Error::new(span, "a hop needs a sink or an operator"))?;
            match self.node(&element.ident)?.role() {
                Role::Forward if element.nested.is_none() => forward.push(element.ident),
                Role::Around => {
                    let Some(nested) = element.nested else {
                        return Err(syn::Error::new(
                            element.ident.span(),
                            "an around node needs the rest of its hop in braces: `Around { ... }`",
                        ));
                    };
                    forward.push(element.ident.clone());
                    let inner = self.stages(nested, element.ident.span(), forward, backward)?;
                    backward.push(element.ident);
                    break inner;
                }
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
                        "only an operator or an around node takes braces",
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
        Ok(inner)
    }

    /// A forward edge carries a stream at both of its ends or at neither.
    /// The types on it are checked by the compiler; this gives the streamed
    /// and the plain form of one type a clear error instead of a type
    /// mismatch.
    fn agree(&mut self, sent: Option<&Request>, declared: &Request) {
        let Some(sent) = sent else {
            return;
        };
        let error = match (&sent.item, &declared.item) {
            (Some(_), None) => syn::Error::new(
                declared.head.span(),
                "the request on this edge is streamed: declare it as `Head + stream Item`",
            ),
            (None, Some(item)) => syn::Error::new(
                item.span(),
                "the request on this edge is not streamed: declare it without `stream`",
            ),
            _ => return,
        };
        match &mut self.streaming {
            Some(first) => first.combine(error),
            None => self.streaming = Some(error),
        }
    }
}
