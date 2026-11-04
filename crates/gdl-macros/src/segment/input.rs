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
//! nodes: [unary Tokenize tokenize = ::nodes::Tokenize; Preprocess preprocess = ::chat_front_nodes::Preprocess; ...];
//! policies: ["by_model" = ::policy_router::ByModel; ...];
//! selectors: ["by_length" = ::routers::ByLength; ...];
//! serve: json_lines;
//! ```
//!
//! `selectors` is written only when the segment sends through a selector
//! that the spec binds, so that a spec that binds none keeps its golden
//! crates (`crates/gdl-gen/tests/fixtures/golden`).
//!
//! A crossing is its check (`crosses`, `anchors` or `calls`), its alias, and its
//! port: the receiving node, the declaration trait, the port, and `head` or
//! `item` for one part of a streamed port. `serve` is `json_lines`, `none`,
//! `frontend Node = path`, or `service { ... }`: the frontend, the clients'
//! type, the entry adapters, the roots the entries reach by index, each
//! entry's expression, or none, and what the entries need, if anything.

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
    /// The selectors it builds and registers, by name.
    pub(crate) selectors: Vec<(LitStr, Path)>,
    pub(crate) serve: Serve,
}

/// A message checked to be a wire type.
pub(crate) struct Crossing {
    /// `crosses`, `anchors` or `calls`.
    pub(crate) check: Ident,
    pub(crate) alias: Ident,
    pub(crate) node: Ident,
    pub(crate) decl: Ident,
    pub(crate) port: Ident,
    /// `head` or `item`, for one part of a streamed port.
    pub(crate) part: Option<Ident>,
}

/// A hosted node: its name in the graph, its binding, and its type, and
/// whether it is a unary node, which is written `unary Name binding = path;`.
pub(crate) struct Node {
    pub(crate) name: Ident,
    pub(crate) binding: Ident,
    pub(crate) ty: Path,
    pub(crate) unary: bool,
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
    /// What the entries need, one line for each kind of each entry: left
    /// out when they need none.
    pub(crate) needs: Vec<Need>,
}

/// One kind an entry needs, its class, and the entry's adapters, which
/// read it: `"model" = "sets" ["Http_chat_op"];`.
pub(crate) struct Need {
    pub(crate) kind: LitStr,
    pub(crate) class: LitStr,
    pub(crate) adapters: Vec<LitStr>,
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
            let mut name: Ident = input.parse()?;
            // `unary Name binding`: three names before the `=`.
            let unary = name == "unary" && input.peek(Ident) && input.peek2(Ident);
            if unary {
                name = input.parse()?;
            }
            let binding = input.parse()?;
            input.parse::<Token![=]>()?;
            let ty = input.parse()?;
            Ok(Node {
                name,
                binding,
                ty,
                unary,
            })
        })?;
        end()?;
        let named = |input: ParseStream<'_>| -> syn::Result<(LitStr, Path)> {
            let name = input.parse()?;
            input.parse::<Token![=]>()?;
            Ok((name, input.parse()?))
        };
        field("policies")?;
        let policies = entries(input, named)?;
        end()?;
        let selectors = if input
            .fork()
            .parse::<Ident>()
            .is_ok_and(|key| key == "selectors")
        {
            field("selectors")?;
            let selectors = entries(input, named)?;
            end()?;
            selectors
        } else {
            Vec::new()
        };
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
            selectors,
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
        let wired = entries(input, |input| {
            let field = input.parse()?;
            if input.peek(Token![;]) {
                return Ok((field, None));
            }
            input.parse::<Token![=]>()?;
            Ok((field, Some(input.parse()?)))
        })?;
        end()?;
        let needs = if input.is_empty() {
            Vec::new()
        } else {
            key(input, "needs")?;
            let needs = entries(input, |input| {
                let kind = input.parse()?;
                input.parse::<Token![=]>()?;
                Ok(Need {
                    kind,
                    class: input.parse()?,
                    adapters: list(input)?,
                })
            })?;
            end()?;
            needs
        };
        Ok(Self {
            frontend,
            frontend_ty,
            clients,
            adapters,
            pipelines,
            entries: wired,
            needs,
        })
    }
}
