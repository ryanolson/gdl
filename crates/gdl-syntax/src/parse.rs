//! The macro's syntax:
//!
//! ```text
//! name = "chat", schema = 1;
//! source   A: Req -> stream Chunk;
//! operator B: (Req -> stream Chunk) over (Pre -> stream Out);
//! forward  C: Pre -> Pre;
//! sink     D: Pre -> stream Raw;
//! backward F: Raw -> Tok;
//! backward E: Tok -> Out;
//! backward G: Chunk -> Chunk;
//! topology: A => B { C => D => F => E } => G;
//! ```
//!
//! Built-in identities use `identity N: request Req;`, `identity N: response Item;`,
//! or `identity N: Req -> stream Item;` for an operator. They take no implementation binding.
//!
//! A request position (a source's, sink's, or operator's request, and both
//! sides of a forward node) also takes a streamed request: `Head + stream
//! Item`, or `stream Item` for a head of `()`.
//!
//! Among the nodes, `property model: Model;` declares a property kind and
//! the message type of its keys. After its signature, a node says which
//! kinds its instance has, `provides model, region`, and which it reads
//! from the pools of a class, `needs model from sets`:
//!
//! ```text
//! property model: Model;
//! sink Engine: Req -> stream Item provides model;
//! ```
//!
//! A unary node takes one request and returns one response, apart from
//! the topology: `unary Tok: Text -> Tokens;`, with `idempotent` after its
//! signature when it is safe to run twice. After its signature, any node
//! names the unary nodes it calls, `calls Tok, Overlap`:
//!
//! ```text
//! unary    Tok: Text -> Tokens idempotent;
//! operator Route: (Req -> stream Chunk) over (Req -> stream Chunk) calls Tok;
//! ```

use syn::ext::IdentExt as _;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitInt, LitStr, Token, Type, braced, parenthesized};

mod kw {
    syn::custom_keyword!(name);
    syn::custom_keyword!(schema);
    syn::custom_keyword!(source);
    syn::custom_keyword!(identity);
    syn::custom_keyword!(request);
    syn::custom_keyword!(response);
    syn::custom_keyword!(operator);
    syn::custom_keyword!(forward);
    syn::custom_keyword!(sink);
    syn::custom_keyword!(backward);
    syn::custom_keyword!(around);
    syn::custom_keyword!(stream);
    syn::custom_keyword!(over);
    syn::custom_keyword!(topology);
    syn::custom_keyword!(property);
    syn::custom_keyword!(provides);
    syn::custom_keyword!(needs);
    syn::custom_keyword!(from);
    syn::custom_keyword!(unary);
    syn::custom_keyword!(calls);
    syn::custom_keyword!(idempotent);
}

pub(crate) struct GraphInput {
    pub(crate) name: LitStr,
    pub(crate) schema: LitInt,
    pub(crate) properties: Vec<PropertyDecl>,
    pub(crate) nodes: Vec<NodeDecl>,
    pub(crate) topology: Vec<Vec<Element>>,
    pub(crate) topology_span: proc_macro2::Span,
}

pub(crate) struct NodeDecl {
    pub(crate) builtin: bool,
    pub(crate) ident: Ident,
    pub(crate) sig: Sig,
    /// The kinds after `provides`.
    pub(crate) provides: Vec<Ident>,
    /// Each kind and class after `needs`: `model from sets`.
    pub(crate) needs: Needs,
    /// The unary nodes after `calls`.
    pub(crate) calls: Vec<Ident>,
    /// Where `idempotent` was said, if it was.
    pub(crate) idempotent: Option<proc_macro2::Span>,
}

/// Each kind a node needs, and the class it reads the kind from.
pub(crate) type Needs = Vec<(Ident, Ident)>;

/// `property model: Model;`
pub(crate) struct PropertyDecl {
    pub(crate) kind: Ident,
    pub(crate) ty: Type,
}

// Built once per declared node while the macro expands; its size costs
// nothing at run time, and boxing the operator variant would only add
// indirection to the expansion code.
/// A node's role and the message types at its ports.
#[allow(clippy::large_enum_variant)]
pub enum Sig {
    /// `source A: Req -> stream Item;`
    Source {
        /// The request the caller sends.
        req: Request,
        /// The item of the response the caller receives.
        item: Type,
    },
    /// `operator B: (UpReq -> stream UpItem) over (DownReq -> stream DownItem);`
    Operator {
        /// The request it takes from its outer hop.
        up_req: Request,
        /// The item of the response it returns to its outer hop.
        up_item: Type,
        /// The request it sends into the hop it owns.
        down_req: Request,
        /// The item of the response it reads from the hop it owns.
        down_item: Type,
    },
    /// `forward C: In -> Out;`
    Forward {
        /// The request it takes.
        input: Request,
        /// The request it passes on.
        output: Request,
    },
    /// `sink D: Req -> stream Item;`
    Sink {
        /// The request it takes.
        req: Request,
        /// The item of the response it returns.
        item: Type,
    },
    /// `backward F: In -> Out;`
    Backward {
        /// The response item it takes.
        input: Type,
        /// The response item it passes on.
        output: Type,
    },
    /// `unary T: Req -> Resp;`
    Unary {
        /// The request a caller sends.
        req: Type,
        /// The response the caller receives.
        resp: Type,
    },
    /// `around P: (UpReq -> stream UpItem) over (DownReq -> stream DownItem);`
    Around {
        /// The request it takes from the outer end's side.
        up_req: Request,
        /// The item it returns toward the outer end.
        up_item: Type,
        /// The request it passes on toward the inner end.
        down_req: Request,
        /// The item it reads from the inner end's side.
        down_item: Type,
    },
}

/// A request type: a value, or a head and a stream of items.
#[derive(Clone)]
pub struct Request {
    /// The value, or the head of a streamed request: `()` for `stream Item`.
    pub head: Type,
    /// The item type of a streamed request.
    pub item: Option<Type>,
}

/// One element of a topology list: a node, and for an operator, the hop it
/// owns in braces. An around node's braces hold the rest of its own hop.
pub(crate) struct Element {
    pub(crate) ident: Ident,
    pub(crate) nested: Option<Vec<Element>>,
}

impl Parse for GraphInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        input.parse::<kw::name>()?;
        input.parse::<Token![=]>()?;
        let name: LitStr = input.parse()?;
        input.parse::<Token![,]>()?;
        input.parse::<kw::schema>()?;
        input.parse::<Token![=]>()?;
        let schema: LitInt = input.parse()?;
        input.parse::<Token![;]>()?;

        let (mut properties, mut nodes) = (Vec::new(), Vec::new());
        while !input.peek(kw::topology) {
            if input.peek(kw::property) {
                properties.push(input.parse()?);
            } else {
                nodes.push(input.parse()?);
            }
        }
        let topology_kw = input.parse::<kw::topology>()?;
        input.parse::<Token![:]>()?;
        let mut topology = vec![parse_list(input)?];
        input.parse::<Token![;]>()?;
        while !input.is_empty() {
            input.parse::<kw::topology>()?;
            input.parse::<Token![:]>()?;
            topology.push(parse_list(input)?);
            input.parse::<Token![;]>()?;
        }
        Ok(Self {
            name,
            schema,
            properties,
            nodes,
            topology,
            topology_span: topology_kw.span,
        })
    }
}

impl Parse for NodeDecl {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        let sig_kind = if lookahead.peek(kw::source) {
            input.parse::<kw::source>()?;
            0
        } else if lookahead.peek(kw::identity) {
            input.parse::<kw::identity>()?;
            5
        } else if lookahead.peek(kw::operator) {
            input.parse::<kw::operator>()?;
            1
        } else if lookahead.peek(kw::forward) {
            input.parse::<kw::forward>()?;
            2
        } else if lookahead.peek(kw::sink) {
            input.parse::<kw::sink>()?;
            3
        } else if lookahead.peek(kw::backward) {
            input.parse::<kw::backward>()?;
            4
        } else if lookahead.peek(kw::around) {
            input.parse::<kw::around>()?;
            6
        } else if lookahead.peek(kw::unary) {
            input.parse::<kw::unary>()?;
            7
        } else {
            return Err(lookahead.error());
        };
        let ident: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let sig = match sig_kind {
            5 => {
                if input.peek(kw::request) {
                    input.parse::<kw::request>()?;
                    let req = request(input)?;
                    Sig::Forward {
                        input: req.clone(),
                        output: req,
                    }
                } else if input.peek(kw::response) {
                    input.parse::<kw::response>()?;
                    let item = item_type(input)?;
                    Sig::Backward {
                        input: item.clone(),
                        output: item,
                    }
                } else {
                    let (req, item) = streaming(input)?;
                    Sig::Operator {
                        up_req: req.clone(),
                        up_item: item.clone(),
                        down_req: req,
                        down_item: item,
                    }
                }
            }
            0 => {
                let (req, item) = streaming(input)?;
                Sig::Source { req, item }
            }
            1 | 6 => {
                let up;
                parenthesized!(up in input);
                let (up_req, up_item) = streaming(&up)?;
                input.parse::<kw::over>()?;
                let down;
                parenthesized!(down in input);
                let (down_req, down_item) = streaming(&down)?;
                if sig_kind == 6 {
                    Sig::Around {
                        up_req,
                        up_item,
                        down_req,
                        down_item,
                    }
                } else {
                    Sig::Operator {
                        up_req,
                        up_item,
                        down_req,
                        down_item,
                    }
                }
            }
            2 => {
                let input_req = request(input)?;
                input.parse::<Token![->]>()?;
                let output = request(input)?;
                Sig::Forward {
                    input: input_req,
                    output,
                }
            }
            7 => {
                let req = unary_type(input)?;
                input.parse::<Token![->]>()?;
                let resp = unary_type(input)?;
                Sig::Unary { req, resp }
            }
            4 => {
                let input_ty = item_type(input)?;
                input.parse::<Token![->]>()?;
                let output = item_type(input)?;
                Sig::Backward {
                    input: input_ty,
                    output,
                }
            }
            _ => {
                let (req, item) = streaming(input)?;
                Sig::Sink { req, item }
            }
        };
        let Clauses {
            provides,
            needs,
            calls,
            idempotent,
        } = clauses(input)?;
        input.parse::<Token![;]>()?;
        Ok(Self {
            ident,
            sig,
            builtin: sig_kind == 5,
            provides,
            needs,
            calls,
            idempotent,
        })
    }
}

impl Parse for PropertyDecl {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        input.parse::<kw::property>()?;
        // Any word, keywords included, so that the kind check, not the
        // parser, says what is wrong with one.
        let kind = Ident::parse_any(input)?;
        input.parse::<Token![:]>()?;
        let ty = Type::without_plus(input)?;
        input.parse::<Token![;]>()?;
        Ok(Self { kind, ty })
    }
}

/// What a node says after its signature.
struct Clauses {
    provides: Vec<Ident>,
    needs: Needs,
    calls: Vec<Ident>,
    idempotent: Option<proc_macro2::Span>,
}

/// `provides a, b`, `needs a from x, b from y`, `calls A, B`, and
/// `idempotent`, each at most once, in any order, after a node's
/// signature.
fn clauses(input: ParseStream<'_>) -> syn::Result<Clauses> {
    let (mut provides, mut needs, mut calls, mut idempotent) = (None, None, None, None);
    loop {
        if input.peek(kw::provides) {
            let said = input.parse::<kw::provides>()?;
            if provides.is_some() {
                return Err(syn::Error::new(
                    said.span,
                    "`provides` is said once: list its kinds after it, `provides a, b`",
                ));
            }
            let mut kinds = vec![Ident::parse_any(input)?];
            while input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
                kinds.push(Ident::parse_any(input)?);
            }
            provides = Some(kinds);
        } else if input.peek(kw::needs) {
            let said = input.parse::<kw::needs>()?;
            if needs.is_some() {
                return Err(syn::Error::new(
                    said.span,
                    "`needs` is said once: list its kinds after it, `needs a from x, b from y`",
                ));
            }
            let mut kinds = Vec::new();
            loop {
                let kind = Ident::parse_any(input)?;
                input.parse::<kw::from>()?;
                kinds.push((kind, Ident::parse_any(input)?));
                if !input.peek(Token![,]) {
                    break;
                }
                input.parse::<Token![,]>()?;
            }
            needs = Some(kinds);
        } else if input.peek(kw::calls) {
            let said = input.parse::<kw::calls>()?;
            if calls.is_some() {
                return Err(syn::Error::new(
                    said.span,
                    "`calls` is said once: list its unary nodes after it, `calls A, B`",
                ));
            }
            let mut services = vec![input.parse::<Ident>()?];
            while input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
                services.push(input.parse::<Ident>()?);
            }
            calls = Some(services);
        } else if input.peek(kw::idempotent) {
            let said = input.parse::<kw::idempotent>()?;
            if idempotent.is_some() {
                return Err(syn::Error::new(said.span, "`idempotent` is said once"));
            }
            idempotent = Some(said.span);
        } else {
            return Ok(Clauses {
                provides: provides.unwrap_or_default(),
                needs: needs.unwrap_or_default(),
                calls: calls.unwrap_or_default(),
                idempotent,
            });
        }
    }
}

/// `Req -> stream Item`
fn streaming(input: ParseStream<'_>) -> syn::Result<(Request, Type)> {
    let req = request(input)?;
    input.parse::<Token![->]>()?;
    input.parse::<kw::stream>()?;
    let item: Type = input.parse()?;
    Ok((req, item))
}

/// A unary node's request or response: one value, never streamed.
fn unary_type(input: ParseStream<'_>) -> syn::Result<Type> {
    const ONE: &str =
        "a unary node takes one request and returns one response; neither is streamed";
    if input.peek(kw::stream) {
        return Err(input.error(ONE));
    }
    let ty = Type::without_plus(input)?;
    if input.peek(Token![+]) {
        return Err(input.error(ONE));
    }
    Ok(ty)
}

/// A backward node's type: an item, which cannot itself be streamed.
fn item_type(input: ParseStream<'_>) -> syn::Result<Type> {
    let ty = Type::without_plus(input)?;
    if input.peek(Token![+]) {
        return Err(input
            .error("only a request can be streamed; a backward node's types are response items"));
    }
    Ok(ty)
}

/// The item type after `stream`.
fn stream_item(input: ParseStream<'_>) -> syn::Result<Type> {
    if input.peek(Token![->]) || input.peek(Token![;]) || input.is_empty() {
        return Err(input.error("`stream` needs the item type after it"));
    }
    Type::without_plus(input)
}

/// `Req`, `Head + stream Item`, or `stream Item`. The head is parsed
/// without `+`, which would otherwise read `Head + stream` as a trait object.
fn request(input: ParseStream<'_>) -> syn::Result<Request> {
    if input.peek(kw::stream) {
        let stream = input.parse::<kw::stream>()?;
        let item = stream_item(input)?;
        let head = syn::parse_quote_spanned!(stream.span => ());
        return Ok(Request {
            head,
            item: Some(item),
        });
    }
    let head = Type::without_plus(input)?;
    if !input.peek(Token![+]) {
        return Ok(Request { head, item: None });
    }
    input.parse::<Token![+]>()?;
    input.parse::<kw::stream>()?;
    let item = stream_item(input)?;
    Ok(Request {
        head,
        item: Some(item),
    })
}

/// `X => Y { ... } => Z`
fn parse_list(input: ParseStream<'_>) -> syn::Result<Vec<Element>> {
    let mut list = vec![parse_element(input)?];
    while input.peek(Token![=>]) {
        input.parse::<Token![=>]>()?;
        list.push(parse_element(input)?);
    }
    Ok(list)
}

fn parse_element(input: ParseStream<'_>) -> syn::Result<Element> {
    let ident: Ident = input.parse()?;
    let nested = if input.peek(syn::token::Brace) {
        let inner;
        braced!(inner in input);
        let list = parse_list(&inner)?;
        if !inner.is_empty() {
            return Err(inner.error("unexpected input in a hop"));
        }
        Some(list)
    } else {
        None
    };
    Ok(Element { ident, nested })
}
