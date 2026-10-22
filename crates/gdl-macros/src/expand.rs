//! What `graph!` generates:
//!
//! - `nodes`: one marker type per node, implementing the node's declaration
//!   traits, so `gdl::Absent<nodes::X>` has exactly node X's port types;
//! - `Bindings`: a typestate struct with one slot per node that needs an implementation,
//!   `Absent` until bound; each setter states the node's role and types, so
//!   a mismatched implementation fails to compile at the setter named for
//!   the node;
//! - `manifest()`: the graph as data, with each edge's wire capability
//!   decided by the probe at the edge's concrete type;
//! - `assemble()`: the distributed hops for one segment;
//! - `local()`: the whole graph as nested calls in one process;
//! - `SourceReq` and `SourceItem`: the source's request and item types, so
//!   code outside the contract crate (generated segment crates) can name
//!   the graph's entry types without parsing them out of the macro input.
//!
//! The port types are the caller's tokens, so every one of them appears only
//! where the caller's names are in scope: at the invocation's own level, never
//! inside the generated `nodes` module (where `super::` and a node's marker
//! would change what a type names), and never next to a generic parameter
//! or a local the caller could not know about. Names the expansion
//! introduces start with `__gdl` or `__Gdl`, parameters included (a
//! parameter name is a pattern, which a caller's constant of that name would
//! turn into a constant pattern). Each kind of introduced name has its own
//! prefix, so a node's name can never make two of them equal. Every other
//! path the expansion writes is rooted (`::std`, `::core`, or the gdl path it
//! was given).

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{Ident, Type};

use gdl_syntax::{Graph, Hop, Node, Request, Role, Sig};

pub(crate) fn expand(graph: &Graph, krate: &TokenStream) -> TokenStream {
    let nodes = expand_nodes(graph, krate);
    let bindings = expand_bindings(graph, krate);
    let manifest = expand_manifest(graph, krate);
    let assemble = expand_assemble(graph, krate);
    let local = expand_local(graph, krate);
    let source = expand_source_types(graph, krate);
    let edges = expand_edges(graph);
    quote! {
        #source
        #edges
        #nodes
        #bindings
        #manifest
        #assemble
        #local
    }
}

/// `SourceReq` and `SourceItem`, for a graph with one root; with several,
/// no root is "the" source, and the aliases would name whichever was
/// declared first.
fn expand_source_types(graph: &Graph, krate: &TokenStream) -> TokenStream {
    if graph
        .nodes
        .iter()
        .filter(|n| n.role() == Role::Source)
        .count()
        != 1
    {
        return TokenStream::new();
    }
    let (req, item) = source_types(graph, krate);
    quote! {
        /// The request type the graph's source sends.
        pub type SourceReq = #req;
        /// The item type the graph's source receives.
        pub type SourceItem = #item;
    }
}

fn source_types<'g>(graph: &'g Graph, krate: &TokenStream) -> (TokenStream, &'g Type) {
    match &graph.source().sig {
        Sig::Source { req, item } => (request(req, krate), item),
        _ => unreachable!("checked: the source is a source"),
    }
}

/// A request's port type: the value type, or `Streamed<Head, Item>`.
fn request(req: &Request, krate: &TokenStream) -> TokenStream {
    let head = &req.head;
    match &req.item {
        None => quote!(#head),
        Some(item) => quote!(#krate::gdl::Streamed<#head, #item>),
    }
}

/// One assertion per edge that the type a node declares for it is the type
/// the node before it sends, spanned at the declaration. Without them a
/// mismatch surfaces only where the expansion composes the hop, as an error
/// on the whole invocation that names runtime types.
fn expand_edges(graph: &Graph) -> TokenStream {
    let mut checks = Vec::new();
    let check = |checks: &mut Vec<TokenStream>, sent: &Type, declared: &Type| {
        checks.push(quote_spanned! { declared.span() =>
            let _: ::core::marker::PhantomData<#sent> = ::core::marker::PhantomData::<#declared>;
        });
    };
    // The model checked that both ends of an edge agree on whether it is
    // streamed, so a request is checked part by part.
    let check_request = |checks: &mut Vec<TokenStream>, sent: &Request, declared: &Request| {
        check(checks, &sent.head, &declared.head);
        if let (Some(sent), Some(declared)) = (&sent.item, &declared.item) {
            check(checks, sent, declared);
        }
    };
    for hop in &graph.hops {
        let (down_req, down_item) = match &graph.node(&hop.outer).sig {
            Sig::Source { req, item } => (req, item),
            Sig::Operator {
                down_req,
                down_item,
                ..
            } => (down_req, down_item),
            _ => unreachable!("checked: the outer end is a source or an operator"),
        };
        let mut sent = down_req;
        for ident in &hop.forward {
            if let Sig::Forward { input, output } = &graph.node(ident).sig {
                check_request(&mut checks, sent, input);
                sent = output;
            }
        }
        let mut items = match &graph.node(&hop.inner).sig {
            Sig::Sink { req, item } => {
                check_request(&mut checks, sent, req);
                item
            }
            Sig::Operator {
                up_req, up_item, ..
            } => {
                check_request(&mut checks, sent, up_req);
                up_item
            }
            _ => unreachable!("checked: the inner end is a sink or an operator"),
        };
        for ident in &hop.backward {
            if let Sig::Backward { input, output } = &graph.node(ident).sig {
                check(&mut checks, items, input);
                items = output;
            }
        }
        check(&mut checks, items, down_item);
    }
    quote! {
        const _: () = {
            fn __gdl_edges() {
                #(#checks)*
            }
        };
    }
}

/// Nodes that take an implementation binding, in declaration order.
fn bound_nodes(graph: &Graph) -> impl Iterator<Item = &Node> {
    graph.nodes.iter().filter(|n| n.binding.is_some())
}

/// The type parameter for a node's slot.
fn generic(node: &Node) -> Ident {
    format_ident!("__GdlSlot{}", node.ident)
}

fn field(node: &Node) -> &Ident {
    node.binding
        .as_ref()
        .expect("checked: a bound node has a binding")
}

/// The local a node's slot is moved into: prefixed, so it shadows neither a
/// parameter nor another local of the generated function.
fn local_var(node: &Node) -> Ident {
    let name = node
        .binding
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            node.ident
                .to_string()
                .bytes()
                .map(|b| format!("{b:02x}"))
                .collect()
        });
    format_ident!("__gdl_node_{}", name.trim_start_matches("r#"))
}

fn role_bound(node: &Node, krate: &TokenStream) -> TokenStream {
    match &node.sig {
        Sig::Operator {
            up_req,
            up_item,
            down_req,
            down_item,
        } => {
            let (up_req, down_req) = (request(up_req, krate), request(down_req, krate));
            quote! {
                #krate::gdl::Operator<UpReq = #up_req, UpItem = #up_item, DownReq = #down_req, DownItem = #down_item>
            }
        }
        Sig::Forward { input, output } => {
            let (input, output) = (request(input, krate), request(output, krate));
            quote! { #krate::gdl::Forward<In = #input, Out = #output> }
        }
        Sig::Sink { req, item } => {
            let req = request(req, krate);
            quote! { #krate::gdl::Sink<Req = #req, Item = #item> }
        }
        Sig::Backward { input, output } => {
            quote! { #krate::gdl::Backward<In = #input, Out = #output> }
        }
        Sig::Source { .. } => unreachable!("the source takes no binding"),
    }
}

fn expand_nodes(graph: &Graph, krate: &TokenStream) -> TokenStream {
    let markers = graph.nodes.iter().map(|node| {
        let ident = &node.ident;
        let doc = format!("Node `{ident}` of the graph.");
        quote! {
            #[doc = #doc]
            pub struct #ident;
        }
    });
    // Outside the module: the declarations name the caller's types.
    let decls = graph.nodes.iter().map(|node| {
        let ident = &node.ident;
        let name = ident.to_string();
        let (role, decl) = match &node.sig {
            Sig::Source { req, item } => {
                let req = request(req, krate);
                (
                    quote!(Source),
                    quote! { impl #krate::SourceDecl for nodes::#ident { type Req = #req; type Item = #item; } },
                )
            }
            Sig::Operator {
                up_req,
                up_item,
                down_req,
                down_item,
            } => {
                let (up_req, down_req) = (request(up_req, krate), request(down_req, krate));
                (
                quote!(Operator),
                quote! {
                    impl #krate::OperatorDecl for nodes::#ident {
                        type UpReq = #up_req;
                        type UpItem = #up_item;
                        type DownReq = #down_req;
                        type DownItem = #down_item;
                    }
                },
                )
            }
            Sig::Forward { input, output } => {
                let (input, output) = (request(input, krate), request(output, krate));
                (
                    quote!(Forward),
                    quote! { impl #krate::ForwardDecl for nodes::#ident { type In = #input; type Out = #output; } },
                )
            }
            Sig::Sink { req, item } => {
                let req = request(req, krate);
                (
                    quote!(Sink),
                    quote! { impl #krate::SinkDecl for nodes::#ident { type Req = #req; type Item = #item; } },
                )
            }
            Sig::Backward { input, output } => (
                quote!(Backward),
                quote! { impl #krate::BackwardDecl for nodes::#ident { type In = #input; type Out = #output; } },
            ),
        };
        quote! {
            impl #krate::NodeDecl for nodes::#ident {
                const NAME: &'static ::core::primitive::str = #name;
                const ROLE: #krate::Role = #krate::Role::#role;
            }
            #decl
        }
    });
    quote! {
        /// The graph's nodes, as declaration markers.
        pub mod nodes {
            #(#markers)*
        }
        #(#decls)*
    }
}

fn expand_bindings(graph: &Graph, krate: &TokenStream) -> TokenStream {
    let nodes: Vec<&Node> = bound_nodes(graph).collect();
    let generics: Vec<Ident> = nodes.iter().map(|n| generic(n)).collect();
    let fields: Vec<&Ident> = nodes.iter().map(|n| field(n)).collect();
    let idents: Vec<&Ident> = nodes.iter().map(|n| &n.ident).collect();
    let defaults = generics
        .iter()
        .zip(&idents)
        .map(|(g, ident)| quote! { #g = #krate::Absent<nodes::#ident> });
    let setters = nodes.iter().enumerate().map(|(index, node)| {
        let setter = fields[index];
        let bound = role_bound(node, krate);
        let doc = format!("Bind node `{}`.", node.ident);
        let out_generics = generics.iter().enumerate().map(|(i, g)| {
            if i == index {
                quote!(__GdlImpl)
            } else {
                quote!(#g)
            }
        });
        let moves = fields.iter().enumerate().map(|(i, f)| {
            if i == index {
                quote! { #f: ::std::sync::Arc::new(__gdl_implementation) }
            } else {
                quote! { #f: self.#f }
            }
        });
        quote! {
            #[doc = #doc]
            pub fn #setter<__GdlImpl: #bound>(self, __gdl_implementation: __GdlImpl) -> Bindings<#(#out_generics),*> {
                Bindings { #(#moves),* }
            }
        }
    });
    quote! {
        /// One implementation slot per node; a slot is `Absent` until bound.
        pub struct Bindings<#(#defaults),*> {
            #(#fields: ::std::sync::Arc<#generics>,)*
        }

        impl Bindings {
            /// Every slot absent.
            pub fn new() -> Self {
                Bindings { #(#fields: ::std::sync::Arc::new(#krate::Absent::new()),)* }
            }
        }

        impl ::core::default::Default for Bindings {
            fn default() -> Self {
                Self::new()
            }
        }

        impl<#(#generics),*> Bindings<#(#generics),*> {
            #(#setters)*
        }
    }
}

fn string(text: &str) -> TokenStream {
    // Fully qualified: a caller's trait with a `from` function, in scope at
    // the invocation, would make `String::from` ambiguous.
    quote! { <::std::string::String as ::core::convert::From<&::core::primitive::str>>::from(#text) }
}

fn type_info(ty: &Type, krate: &TokenStream) -> TokenStream {
    let name = quote!(#ty).to_string();
    quote! { #krate::TypeInfo::new(#name, #krate::wire_codec!(#ty).is_some()) }
}

fn request_info(req: &Request, krate: &TokenStream) -> TokenStream {
    let Some(item) = &req.item else {
        return type_info(&req.head, krate);
    };
    let head = &req.head;
    let (head_name, item_name) = (quote!(#head).to_string(), quote!(#item).to_string());
    quote! {
        #krate::TypeInfo::streamed(
            #head_name,
            #krate::wire_codec!(#head).is_some(),
            #item_name,
            #krate::wire_codec!(#item).is_some(),
        )
    }
}

/// The codec of a forward edge, chosen by its syntax: a streamed request
/// gets a stream codec, which exists only when both parts are wire types.
fn request_codec(req: &Request, krate: &TokenStream) -> TokenStream {
    let head = &req.head;
    match &req.item {
        None => quote! { #krate::wire_codec!(#head) },
        Some(item) => quote! { #krate::stream_codec!(#head, #item) },
    }
}

fn expand_manifest(graph: &Graph, krate: &TokenStream) -> TokenStream {
    let name = string(&graph.name.value());
    let schema = &graph.schema;
    let nodes = graph.nodes.iter().map(|node| {
        let name = string(&node.ident.to_string());
        let role = match node.role() {
            Role::Source => quote!(Source),
            Role::Operator => quote!(Operator),
            Role::Forward => quote!(Forward),
            Role::Sink => quote!(Sink),
            Role::Backward => quote!(Backward),
        };
        let binding = match &node.binding {
            None => quote!(::core::option::Option::None),
            Some(binding) => {
                let binding = string(&binding.to_string());
                quote!(::core::option::Option::Some(#binding))
            }
        };
        quote! {
            #krate::NodeInfo {
                name: #name,
                role: #krate::Role::#role,
                binding: #binding,
            }
        }
    });
    let hops = graph.hops.iter().map(|hop| {
        let outer = string(&hop.outer.to_string());
        let inner = string(&hop.inner.to_string());
        let forward = hop.forward.iter().map(|i| string(&i.to_string()));
        let backward = hop.backward.iter().map(|i| string(&i.to_string()));
        let forward_types = hop.forward_types.iter().map(|t| request_info(t, krate));
        let backward_types = hop.backward_types.iter().map(|t| type_info(t, krate));
        quote! {
            #krate::HopInfo {
                outer: #outer,
                forward: ::std::vec![#(#forward),*],
                inner: #inner,
                backward: ::std::vec![#(#backward),*],
                forward_types: ::std::vec![#(#forward_types),*],
                backward_types: ::std::vec![#(#backward_types),*],
            }
        }
    });
    quote! {
        /// The graph as data: nodes, hops, and each edge's type and wire
        /// capability.
        pub fn manifest() -> #krate::Manifest {
            #krate::Manifest {
                name: #name,
                schema: #schema,
                nodes: ::std::vec![#(#nodes),*],
                hops: ::std::vec![#(#hops),*],
            }
        }
    }
}

/// Shared parts of `assemble` and `local`: generics, bounds, and moving each
/// slot out of `Bindings` into its local.
struct Signature {
    generics: Vec<Ident>,
    bounds: Vec<TokenStream>,
    unpack: TokenStream,
}

fn signature(graph: &Graph, krate: &TokenStream) -> Signature {
    let nodes: Vec<&Node> = bound_nodes(graph).collect();
    let fields = nodes.iter().map(|n| field(n));
    let locals = nodes.iter().map(|n| local_var(n));
    let builtins = graph
        .nodes
        .iter()
        .filter(|n| n.binding.is_none())
        .filter_map(|node| {
            let local = local_var(node);
            let implementation = match &node.sig {
                Sig::Operator {
                    up_req, up_item, ..
                } => {
                    let req = request(up_req, krate);
                    quote!(#krate::gdl::IdentityOperator::<#req,#up_item>::new())
                }
                Sig::Forward { input, .. } => {
                    let req = request(input, krate);
                    quote!(#krate::gdl::IdentityNode::<#req>::new())
                }
                Sig::Backward { input, .. } => quote!(#krate::gdl::IdentityNode::<#input>::new()),
                _ => return None,
            };
            Some(quote!(let #local=::std::sync::Arc::new(#implementation);))
        });
    Signature {
        generics: nodes.iter().map(|n| generic(n)).collect(),
        bounds: nodes
            .iter()
            .map(|n| {
                let g = generic(n);
                let bound = role_bound(n, krate);
                quote! { #g: #bound }
            })
            .collect(),
        unpack: quote! { let Bindings { #(#fields: #locals),* } = __gdl_bindings; #(#builtins)* },
    }
}

fn root_result(
    graph: &Graph,
    krate: &TokenStream,
    generics: &[Ident],
    distributed: bool,
) -> (TokenStream, TokenStream) {
    let roots: Vec<_> = graph
        .hops
        .iter()
        .enumerate()
        .filter(|(_, h)| graph.node(&h.outer).role() == Role::Source)
        .collect();
    let multiple = roots.len() > 1;
    let mut result_type = quote!(());
    let mut result_value = quote!(());
    for (index, hop) in roots.into_iter().rev() {
        let Sig::Source { req, item } = &graph.node(&hop.outer).sig else {
            unreachable!()
        };
        let req = request(req, krate);
        let (ty, value) = if distributed {
            let name = format_ident!("__gdl_hop_{}", index);
            (
                quote!(impl #krate::gdl::Next<#req, #item> + #krate::Register + use<#(#generics),*>),
                quote!(#name),
            )
        } else {
            let name = format_ident!("__gdl_local_{}", index);
            (
                quote!(#krate::gdl::Source<impl #krate::gdl::Next<#req, #item> + use<#(#generics),*>>),
                quote!(#krate::gdl::Source::new(#name)),
            )
        };
        if !multiple {
            return (ty, value);
        }
        result_type = quote!((#ty, #result_type));
        result_value = quote!((#value, #result_value));
    }
    (result_type, result_value)
}

fn expand_assemble(graph: &Graph, krate: &TokenStream) -> TokenStream {
    let Signature {
        generics,
        bounds,
        unpack,
    } = signature(graph, krate);
    let (result_type, result_value) = root_result(graph, krate, &generics, true);
    // Innermost hop first, so each operator can take the hop below it.
    let hops = graph.hops.iter().enumerate().rev().map(|(index, hop)| {
        let name = format_ident!("__gdl_hop_{}", index);
        let index_lit = index as u16;
        let fwd = distributed_forward(graph, hop, krate);
        let bwd = distributed_backward(graph, hop, krate);
        let inner_node = graph.node(&hop.inner);
        let inner_local = local_var(inner_node);
        let inner = match inner_node.role() {
            Role::Sink => quote! { #krate::DSink::new(::std::sync::Arc::clone(&#inner_local)) },
            _ => {
                let next = format_ident!("__gdl_hop_{}", index + 1);
                quote! { #krate::DOperator::new(#inner_local, #next) }
            }
        };
        quote! {
            let #name = __gdl_deployment.hop(#index_lit, #fwd, #inner, #bwd)?;
        }
    });
    quote! {
        /// The graph as one segment of a deployment sees it. Slots for nodes
        /// this segment does not host may stay `Absent`.
        pub fn assemble<#(#generics),*>(
            __gdl_deployment: &#krate::Deployment,
            __gdl_bindings: Bindings<#(#generics),*>,
        ) -> #krate::gdl::Result<#result_type>
        where
            #(#bounds,)*
        {
            __gdl_deployment.check_contract(manifest().contract_hash())?;
            #unpack
            #(#hops)*
            ::core::result::Result::Ok(#result_value)
        }
    }
}

fn distributed_forward(graph: &Graph, hop: &Hop, krate: &TokenStream) -> TokenStream {
    let last = hop.forward_types.last().expect("one type per forward edge");
    let last = request_codec(last, krate);
    let mut chain = quote! { #krate::FwdEnd::new(#last) };
    for (ident, req) in hop.forward.iter().zip(&hop.forward_types).rev() {
        let f = local_var(graph.node(ident));
        let codec = request_codec(req, krate);
        chain = quote! { #krate::FwdStep::new(#f, #codec, #chain) };
    }
    chain
}

fn distributed_backward(graph: &Graph, hop: &Hop, krate: &TokenStream) -> TokenStream {
    let last = hop
        .backward_types
        .last()
        .expect("one type per backward edge");
    let mut chain = quote! { #krate::BwdEnd::new(#krate::wire_codec!(#last)) };
    for (ident, ty) in hop.backward.iter().zip(&hop.backward_types).rev() {
        let f = local_var(graph.node(ident));
        chain = quote! { #krate::BwdStep::new(#f, #krate::wire_codec!(#ty), #chain) };
    }
    chain
}

fn expand_local(graph: &Graph, krate: &TokenStream) -> TokenStream {
    let Signature {
        generics,
        bounds,
        unpack,
    } = signature(graph, krate);
    let (result_type, result_value) = root_result(graph, krate, &generics, false);
    let hops = graph.hops.iter().enumerate().rev().map(|(index, hop)| {
        let name = format_ident!("__gdl_local_{}", index);
        let mut fwd = quote!(());
        for ident in hop.forward.iter().rev() {
            let f = local_var(graph.node(ident));
            fwd = quote! { (#f, #fwd) };
        }
        let mut bwd = quote!(());
        for ident in hop.backward.iter().rev() {
            let f = local_var(graph.node(ident));
            bwd = quote! { (#f, #bwd) };
        }
        let inner_node = graph.node(&hop.inner);
        let inner_local = local_var(inner_node);
        let inner = match inner_node.role() {
            Role::Sink => {
                quote! { #krate::gdl::SinkStage::new(::std::sync::Arc::clone(&#inner_local)) }
            }
            _ => {
                let next = format_ident!("__gdl_local_{}", index + 1);
                quote! { #krate::gdl::OperatorStage::new(#inner_local, #next) }
            }
        };
        quote! {
            let #name = #krate::gdl::Hop::new(#fwd, #inner, #bwd);
        }
    });
    quote! {
        /// The whole graph in one process, as nested calls.
        pub fn local<#(#generics),*>(
            __gdl_bindings: Bindings<#(#generics),*>,
        ) -> #result_type
        where
            #(#bounds,)*
        {
            #unpack
            #(#hops)*
            #result_value
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every path the expansion writes to the runtime goes through the path
    /// it was given, so a contract crate can depend on the runtime under any
    /// name.
    #[test]
    fn the_expansion_names_the_runtime_only_through_the_given_path() {
        let input: TokenStream = quote! {
            name = "g", schema = 1;
            source   A: u32 -> stream u32;
            operator B: (u32 -> stream u32) over (u32 -> stream u32);
            forward  C: u32 -> u32;
            sink     D: u32 -> stream u32;
            backward E: u32 -> u32;
            topology: A => B { C => D => E };
        };
        let graph = Graph::parse(input).expect("graph");
        let expansion = expand(&graph, &quote!(::pipeline));
        let mut idents = Vec::new();
        collect_idents(expansion, &mut idents);
        assert!(idents.iter().any(|i| i == "pipeline"));
        // The node traits are the definitions crate's, which the runtime
        // re-exports as `gdl`: the expansion reaches them as `pipeline::gdl`.
        let direct = idents
            .iter()
            .enumerate()
            .any(|(at, i)| i == "gdl" && (at == 0 || idents[at - 1] != "pipeline"));
        assert!(!direct, "the expansion names `gdl` directly");
    }

    fn collect_idents(tokens: TokenStream, out: &mut Vec<String>) {
        for token in tokens {
            match token {
                proc_macro2::TokenTree::Ident(ident) => out.push(ident.to_string()),
                proc_macro2::TokenTree::Group(group) => collect_idents(group.stream(), out),
                _ => {}
            }
        }
    }
}
