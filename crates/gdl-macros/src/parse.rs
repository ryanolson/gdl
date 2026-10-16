//! The macro's syntax:
//!
//! ```text
//! name = "chat", schema = 1;
//! source   A: Req -> stream Chunk;
//! operator B: (Req -> stream Chunk) over (Pre -> stream Out);
//! forward  C: Pre -> Pre;
//! sink     D: Pre -> stream Raw;
//! backward F: Raw -> Tok;
//! topology: A => B { C => D => F } => G;
//! ```
//!
//! A request position (a source's, sink's, or operator's request, and both
//! sides of a forward node) also takes a streamed request: `Head + stream
//! Item`, or `stream Item` for a head of `()`.

use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitInt, LitStr, Token, Type, braced, parenthesized};

mod kw {
    syn::custom_keyword!(name);
    syn::custom_keyword!(schema);
    syn::custom_keyword!(source);
    syn::custom_keyword!(operator);
    syn::custom_keyword!(forward);
    syn::custom_keyword!(sink);
    syn::custom_keyword!(backward);
    syn::custom_keyword!(stream);
    syn::custom_keyword!(over);
    syn::custom_keyword!(topology);
}

pub(crate) struct GraphInput {
    pub(crate) name: LitStr,
    pub(crate) schema: LitInt,
    pub(crate) nodes: Vec<NodeDecl>,
    pub(crate) topology: Vec<Element>,
    pub(crate) topology_span: proc_macro2::Span,
}

pub(crate) struct NodeDecl {
    pub(crate) ident: Ident,
    pub(crate) sig: Sig,
}

// Built once per declared node while the macro expands; its size costs
// nothing at run time, and boxing the operator variant would only add
// indirection to the expansion code.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Sig {
    Source {
        req: Request,
        item: Type,
    },
    Operator {
        up_req: Request,
        up_item: Type,
        down_req: Request,
        down_item: Type,
    },
    Forward {
        input: Request,
        output: Request,
    },
    Sink {
        req: Request,
        item: Type,
    },
    Backward {
        input: Type,
        output: Type,
    },
}

/// A request type: a value, or a head and a stream of items.
#[derive(Clone)]
pub(crate) struct Request {
    pub(crate) head: Type,
    /// The item type of a streamed request.
    pub(crate) item: Option<Type>,
}

/// One element of a topology list: a node, and for an operator, the hop it
/// owns in braces.
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

        let mut nodes = Vec::new();
        while !input.peek(kw::topology) {
            nodes.push(input.parse()?);
        }
        let topology_kw = input.parse::<kw::topology>()?;
        input.parse::<Token![:]>()?;
        let topology = parse_list(input)?;
        input.parse::<Token![;]>()?;
        if !input.is_empty() {
            return Err(input.error("unexpected input after the topology"));
        }
        Ok(Self {
            name,
            schema,
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
        } else {
            return Err(lookahead.error());
        };
        let ident: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let sig = match sig_kind {
            0 => {
                let (req, item) = streaming(input)?;
                Sig::Source { req, item }
            }
            1 => {
                let up;
                parenthesized!(up in input);
                let (up_req, up_item) = streaming(&up)?;
                input.parse::<kw::over>()?;
                let down;
                parenthesized!(down in input);
                let (down_req, down_item) = streaming(&down)?;
                Sig::Operator {
                    up_req,
                    up_item,
                    down_req,
                    down_item,
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
        input.parse::<Token![;]>()?;
        Ok(Self { ident, sig })
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
