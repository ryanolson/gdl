//! The input of `segment!`, as the generator writes it:
//!
//! ```text
//! contract: ::chat_contract;
//! package: "chat-front";
//! segment: "front";
//! segments: ["engine", "front", "post"];
//! placement: "...";
//! graph_config: "...";
//! roots: [Frontend];
//! crossings: [crosses Truncate_to_Engine = Engine SinkDecl Req; ...];
//! feeds: [reads_json, writes_json];
//! nodes: [Preprocess preprocess = ::chat_front_nodes::Preprocess; ...];
//! policies: ["by_model" = ::policy_router::ByModel; ...];
//! serve: json_lines;
//! ```
//!
//! A crossing is its check (`crosses` or `anchors`), its alias, and its
//! port: the receiving node, the declaration trait, the port, and `head` or
//! `item` for one part of a streamed port. `serve` is `json_lines`, `none`,
//! `frontend Node = path`, or `service { ... }`: the frontend, the clients'
//! type, the entry adapters, the roots the entries reach by index, and each
//! entry's expression, or none.

use syn::parse::{Parse, ParseStream};
use syn::{Expr, Ident, LitInt, LitStr, Path, Token, braced, bracketed};

/// Everything one segment's library is generated from.
pub(crate) struct Segment {
    pub(crate) contract: Path,
    pub(crate) package: LitStr,
    pub(crate) segment: LitStr,
    pub(crate) segments: Vec<LitStr>,
    pub(crate) placement: LitStr,
    pub(crate) graph_config: LitStr,
    /// The graph's source roots, in hop order.
    pub(crate) roots: Vec<Ident>,
    pub(crate) crossings: Vec<Crossing>,
    /// `reads_json` and `writes_json`, as the binary needs them.
    pub(crate) feeds: Vec<Ident>,
    /// The hosted nodes that take an implementation.
    pub(crate) nodes: Vec<Node>,
    /// The pool policies it builds and registers, by name.
    pub(crate) policies: Vec<(LitStr, Path)>,
    pub(crate) serve: Serve,
}

/// A message checked to be a wire type.
pub(crate) struct Crossing {
    /// `crosses` or `anchors`.
    pub(crate) check: Ident,
    pub(crate) alias: Ident,
    pub(crate) node: Ident,
    pub(crate) decl: Ident,
    pub(crate) port: Ident,
    /// `head` or `item`, for one part of a streamed port.
    pub(crate) part: Option<Ident>,
}

/// A hosted node: its name in the graph, its binding, and its type.
pub(crate) struct Node {
    pub(crate) name: Ident,
    pub(crate) binding: Ident,
    pub(crate) ty: Path,
}

/// What the binary serves once its segments are ready.
pub(crate) enum Serve {
    /// One JSON request per line on stdin.
    JsonLines,
    /// Nothing: it waits to be stopped.
    None,
    /// The frontend pinned on the source.
    Frontend {
        name: Ident,
        ty: Path,
    },
    Service(Service),
}

/// A frontend with typed entries.
pub(crate) struct Service {
    pub(crate) frontend: Ident,
    pub(crate) frontend_ty: Path,
    pub(crate) clients: Path,
    pub(crate) adapters: Vec<Adapter>,
    /// Each root an entry reaches: its variable and its index in the roots.
    pub(crate) pipelines: Vec<(Ident, usize)>,
    /// Each entry's field, and its pipeline unless it is off.
    pub(crate) entries: Vec<(Ident, Option<Expr>)>,
}

/// An entry adapter: its name in the settings, its variable, its type.
pub(crate) struct Adapter {
    pub(crate) name: LitStr,
    pub(crate) variable: Ident,
    pub(crate) ty: Path,
}

impl Serve {
    /// The frontend the binary builds, by its source's name and type, and
    /// the service around it.
    pub(crate) fn hosts(&self) -> (Option<(&Ident, &Path)>, Option<&Service>) {
        match self {
            Self::Frontend { name, ty } => (Some((name, ty)), None),
            Self::Service(service) => (
                Some((&service.frontend, &service.frontend_ty)),
                Some(service),
            ),
            Self::JsonLines | Self::None => (None, None),
        }
    }
}

/// `name:`, the key of the next field.
fn key(input: ParseStream<'_>, name: &str) -> syn::Result<()> {
    let found: Ident = input.parse()?;
    if found != name {
        return Err(syn::Error::new(found.span(), format!("expected `{name}`")));
    }
    input.parse::<Token![:]>()?;
    Ok(())
}

/// `[a, b, c]`.
fn list<T: Parse>(input: ParseStream<'_>) -> syn::Result<Vec<T>> {
    let content;
    bracketed!(content in input);
    Ok(content
        .parse_terminated(T::parse, Token![,])?
        .into_iter()
        .collect())
}

/// `[a; b; c;]`, each item parsed by `item`.
fn entries<T>(
    input: ParseStream<'_>,
    item: impl Fn(ParseStream<'_>) -> syn::Result<T>,
) -> syn::Result<Vec<T>> {
    let content;
    bracketed!(content in input);
    let mut items = Vec::new();
    while !content.is_empty() {
        items.push(item(&content)?);
        content.parse::<Token![;]>()?;
    }
    Ok(items)
}

impl Parse for Segment {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let field = |name: &str| key(input, name);
        let end = || input.parse::<Token![;]>().map(drop);
        field("contract")?;
        let contract = input.parse()?;
        end()?;
        field("package")?;
        let package = input.parse()?;
        end()?;
        field("segment")?;
        let segment = input.parse()?;
        end()?;
        field("segments")?;
        let segments = list(input)?;
        end()?;
        field("placement")?;
        let placement = input.parse()?;
        end()?;
        field("graph_config")?;
        let graph_config = input.parse()?;
        end()?;
        field("roots")?;
        let roots = list(input)?;
        end()?;
        field("crossings")?;
        let crossings = entries(input, |input| {
            let check = input.parse()?;
            let alias = input.parse()?;
            input.parse::<Token![=]>()?;
            let node = input.parse()?;
            let decl = input.parse()?;
            let port = input.parse()?;
            let part = if input.peek(Ident) {
                Some(input.parse()?)
            } else {
                None
            };
            Ok(Crossing {
                check,
                alias,
                node,
                decl,
                port,
                part,
            })
        })?;
        end()?;
        field("feeds")?;
        let feeds = list(input)?;
        end()?;
        field("nodes")?;
        let nodes = entries(input, |input| {
            let name = input.parse()?;
            let binding = input.parse()?;
            input.parse::<Token![=]>()?;
            let ty = input.parse()?;
            Ok(Node { name, binding, ty })
        })?;
        end()?;
        field("policies")?;
        let policies = entries(input, |input| {
            let name = input.parse()?;
            input.parse::<Token![=]>()?;
            Ok((name, input.parse()?))
        })?;
        end()?;
        field("serve")?;
        let serve = input.parse()?;
        end()?;
        Ok(Self {
            contract,
            package,
            segment,
            segments,
            placement,
            graph_config,
            roots,
            crossings,
            feeds,
            nodes,
            policies,
            serve,
        })
    }
}

impl Parse for Serve {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let kind: Ident = input.parse()?;
        match kind.to_string().as_str() {
            "json_lines" => Ok(Self::JsonLines),
            "none" => Ok(Self::None),
            "frontend" => {
                let name = input.parse()?;
                input.parse::<Token![=]>()?;
                Ok(Self::Frontend {
                    name,
                    ty: input.parse()?,
                })
            }
            "service" => {
                let content;
                braced!(content in input);
                Ok(Self::Service(content.parse()?))
            }
            _ => Err(syn::Error::new(
                kind.span(),
                "expected `json_lines`, `none`, `frontend`, or `service`",
            )),
        }
    }
}

impl Parse for Service {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let end = || input.parse::<Token![;]>().map(drop);
        key(input, "frontend")?;
        let frontend = input.parse()?;
        input.parse::<Token![=]>()?;
        let frontend_ty = input.parse()?;
        end()?;
        key(input, "clients")?;
        let clients = input.parse()?;
        end()?;
        key(input, "adapters")?;
        let adapters = entries(input, |input| {
            let name = input.parse()?;
            let variable = input.parse()?;
            input.parse::<Token![=]>()?;
            Ok(Adapter {
                name,
                variable,
                ty: input.parse()?,
            })
        })?;
        end()?;
        key(input, "pipelines")?;
        let pipelines = entries(input, |input| {
            let variable = input.parse()?;
            input.parse::<Token![=]>()?;
            let index: LitInt = input.parse()?;
            Ok((variable, index.base10_parse()?))
        })?;
        end()?;
        key(input, "entries")?;
        let entries = entries(input, |input| {
            let field = input.parse()?;
            if input.peek(Token![;]) {
                return Ok((field, None));
            }
            input.parse::<Token![=]>()?;
            Ok((field, Some(input.parse()?)))
        })?;
        end()?;
        Ok(Self {
            frontend,
            frontend_ty,
            clients,
            adapters,
            pipelines,
            entries,
        })
    }
}
