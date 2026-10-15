//! What `graph!` generates:
//!
//! - `nodes`: one marker type per node, implementing the node's declaration
//!   traits, so `gdl::Absent<nodes::X>` has exactly node X's port types;
//! - `Bindings`: a typestate struct with one slot per non-source node,
//!   `Absent` until bound; each setter states the node's role and types, so
//!   a mismatched implementation fails to compile at the setter named for
//!   the node;
//! - `manifest()`: the graph as data, with each edge's wire capability
//!   decided by the probe at the edge's concrete type;
//! - `assemble()`: the distributed hops for one segment;
//! - `local()`: the whole graph as nested calls in one process.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Ident, Type};

use crate::model::{Graph, Hop, Node, Role};
use crate::parse::Sig;

pub(crate) fn expand(graph: &Graph) -> TokenStream {
    let nodes = expand_nodes(graph);
    let bindings = expand_bindings(graph);
    let manifest = expand_manifest(graph);
    let assemble = expand_assemble(graph);
    let local = expand_local(graph);
    quote! {
        #nodes
        #bindings
        #manifest
        #assemble
        #local
    }
}

/// Non-source nodes, in declaration order: the ones that take a binding.
fn bound_nodes(graph: &Graph) -> impl Iterator<Item = &Node> {
    graph.nodes.iter().filter(|n| n.role() != Role::Source)
}

fn generic(node: &Node) -> Ident {
    format_ident!("{}Impl", node.ident)
}

fn field(node: &Node) -> Ident {
    let snake = snake_case(&node.ident.to_string());
    if is_keyword(&snake) {
        Ident::new_raw(&snake, node.ident.span())
    } else {
        Ident::new(&snake, node.ident.span())
    }
}

fn role_bound(node: &Node) -> TokenStream {
    match &node.sig {
        Sig::Operator {
            up_req,
            up_item,
            down_req,
            down_item,
        } => quote! {
            ::gdl::Operator<UpReq = #up_req, UpItem = #up_item, DownReq = #down_req, DownItem = #down_item>
        },
        Sig::Forward { input, output } => quote! { ::gdl::Forward<In = #input, Out = #output> },
        Sig::Sink { req, item } => quote! { ::gdl::Sink<Req = #req, Item = #item> },
        Sig::Backward { input, output } => quote! { ::gdl::Backward<In = #input, Out = #output> },
        Sig::Source { .. } => unreachable!("the source takes no binding"),
    }
}

fn expand_nodes(graph: &Graph) -> TokenStream {
    let decls = graph.nodes.iter().map(|node| {
        let ident = &node.ident;
        let name = ident.to_string();
        let (role, decl) = match &node.sig {
            Sig::Source { req, item } => (
                quote!(Source),
                quote! { impl ::gdl::SourceDecl for #ident { type Req = #req; type Item = #item; } },
            ),
            Sig::Operator {
                up_req,
                up_item,
                down_req,
                down_item,
            } => (
                quote!(Operator),
                quote! {
                    impl ::gdl::OperatorDecl for #ident {
                        type UpReq = #up_req;
                        type UpItem = #up_item;
                        type DownReq = #down_req;
                        type DownItem = #down_item;
                    }
                },
            ),
            Sig::Forward { input, output } => (
                quote!(Forward),
                quote! { impl ::gdl::ForwardDecl for #ident { type In = #input; type Out = #output; } },
            ),
            Sig::Sink { req, item } => (
                quote!(Sink),
                quote! { impl ::gdl::SinkDecl for #ident { type Req = #req; type Item = #item; } },
            ),
            Sig::Backward { input, output } => (
                quote!(Backward),
                quote! { impl ::gdl::BackwardDecl for #ident { type In = #input; type Out = #output; } },
            ),
        };
        let doc = format!("Node `{name}` of the graph.");
        quote! {
            #[doc = #doc]
            pub struct #ident;
            impl ::gdl::NodeDecl for #ident {
                const NAME: &'static str = #name;
                const ROLE: ::gdl::Role = ::gdl::Role::#role;
            }
            #decl
        }
    });
    quote! {
        /// The graph's nodes, as declaration markers.
        pub mod nodes {
            #[allow(unused_imports)]
            use super::*;
            #(#decls)*
        }
    }
}

fn expand_bindings(graph: &Graph) -> TokenStream {
    let nodes: Vec<&Node> = bound_nodes(graph).collect();
    let generics: Vec<Ident> = nodes.iter().map(|n| generic(n)).collect();
    let fields: Vec<Ident> = nodes.iter().map(|n| field(n)).collect();
    let idents: Vec<&Ident> = nodes.iter().map(|n| &n.ident).collect();
    let defaults = generics
        .iter()
        .zip(&idents)
        .map(|(g, ident)| quote! { #g = ::gdl::Absent<nodes::#ident> });
    let setters = nodes.iter().enumerate().map(|(index, node)| {
        let setter = &fields[index];
        let bound = role_bound(node);
        let doc = format!("Bind node `{}`.", node.ident);
        let out_generics = generics
            .iter()
            .enumerate()
            .map(|(i, g)| if i == index { quote!(X) } else { quote!(#g) });
        let moves = fields.iter().enumerate().map(|(i, f)| {
            if i == index {
                quote! { #f: ::std::sync::Arc::new(implementation) }
            } else {
                quote! { #f: self.#f }
            }
        });
        quote! {
            #[doc = #doc]
            pub fn #setter<X: #bound>(self, implementation: X) -> Bindings<#(#out_generics),*> {
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
                Bindings { #(#fields: ::std::sync::Arc::new(::gdl::Absent::new()),)* }
            }
        }

        impl ::std::default::Default for Bindings {
            fn default() -> Self {
                Self::new()
            }
        }

        impl<#(#generics),*> Bindings<#(#generics),*> {
            #(#setters)*
        }
    }
}

fn type_info(ty: &Type) -> TokenStream {
    let name = quote!(#ty).to_string();
    quote! { ::gdl::TypeInfo::new(#name, ::gdl::wire_codec!(#ty).is_some()) }
}

fn expand_manifest(graph: &Graph) -> TokenStream {
    let name = &graph.name;
    let schema = &graph.schema;
    let nodes = graph.nodes.iter().map(|node| {
        let name = node.ident.to_string();
        let role = match node.role() {
            Role::Source => quote!(Source),
            Role::Operator => quote!(Operator),
            Role::Forward => quote!(Forward),
            Role::Sink => quote!(Sink),
            Role::Backward => quote!(Backward),
        };
        quote! { ::gdl::NodeInfo { name: #name.to_string(), role: ::gdl::Role::#role } }
    });
    let hops = graph.hops.iter().map(|hop| {
        let outer = hop.outer.to_string();
        let inner = hop.inner.to_string();
        let forward = hop.forward.iter().map(|i| i.to_string());
        let backward = hop.backward.iter().map(|i| i.to_string());
        let forward_types = hop.forward_types.iter().map(type_info);
        let backward_types = hop.backward_types.iter().map(type_info);
        quote! {
            ::gdl::HopInfo {
                outer: #outer.to_string(),
                forward: vec![#(#forward.to_string()),*],
                inner: #inner.to_string(),
                backward: vec![#(#backward.to_string()),*],
                forward_types: vec![#(#forward_types),*],
                backward_types: vec![#(#backward_types),*],
            }
        }
    });
    quote! {
        /// The graph as data: nodes, hops, and each edge's type and wire
        /// capability.
        pub fn manifest() -> ::gdl::Manifest {
            ::gdl::Manifest {
                name: #name.to_string(),
                schema: #schema,
                nodes: vec![#(#nodes),*],
                hops: vec![#(#hops),*],
            }
        }
    }
}

/// Shared parts of `assemble` and `local`: generics, bounds, destructuring.
struct Signature {
    generics: Vec<Ident>,
    bounds: Vec<TokenStream>,
    fields: Vec<Ident>,
    req: Type,
    item: Type,
}

fn signature(graph: &Graph) -> Signature {
    let nodes: Vec<&Node> = bound_nodes(graph).collect();
    let (req, item) = match &graph.source().sig {
        Sig::Source { req, item } => (req.clone(), item.clone()),
        _ => unreachable!("checked: the source is a source"),
    };
    Signature {
        generics: nodes.iter().map(|n| generic(n)).collect(),
        bounds: nodes
            .iter()
            .map(|n| {
                let g = generic(n);
                let bound = role_bound(n);
                quote! { #g: #bound }
            })
            .collect(),
        fields: nodes.iter().map(|n| field(n)).collect(),
        req,
        item,
    }
}

fn expand_assemble(graph: &Graph) -> TokenStream {
    let Signature {
        generics,
        bounds,
        fields,
        req,
        item,
    } = signature(graph);
    // Innermost hop first, so each operator can take the hop below it.
    let hops = graph.hops.iter().enumerate().rev().map(|(index, hop)| {
        let name = format_ident!("__gdl_hop_{}", index);
        let index_lit = index as u16;
        let fwd = distributed_forward(graph, hop);
        let bwd = distributed_backward(graph, hop);
        let inner = &hop.inner;
        let inner_field = field(graph.node(inner));
        let inner = match graph.node(inner).role() {
            Role::Sink => quote! { ::gdl::DSink::new(#inner_field) },
            _ => {
                let next = format_ident!("__gdl_hop_{}", index + 1);
                quote! { ::gdl::DOperator::new(#inner_field, #next) }
            }
        };
        quote! {
            let #name = deployment.hop(#index_lit, #fwd, #inner, #bwd)?;
        }
    });
    quote! {
        /// The graph as one segment of a deployment sees it. Slots for nodes
        /// this segment does not host may stay `Absent`.
        pub fn assemble<#(#generics),*>(
            deployment: &::gdl::Deployment,
            bindings: Bindings<#(#generics),*>,
        ) -> ::gdl::Result<impl ::gdl::Next<#req, #item> + ::gdl::Register + use<#(#generics),*>>
        where
            #(#bounds,)*
        {
            let Bindings { #(#fields),* } = bindings;
            #(#hops)*
            Ok(__gdl_hop_0)
        }
    }
}

fn distributed_forward(graph: &Graph, hop: &Hop) -> TokenStream {
    let last = hop.forward_types.last().expect("one type per forward edge");
    let mut chain = quote! { ::gdl::FwdEnd::new(::gdl::wire_codec!(#last)) };
    for (ident, ty) in hop.forward.iter().zip(&hop.forward_types).rev() {
        let f = field(graph.node(ident));
        chain = quote! { ::gdl::FwdStep::new(#f, ::gdl::wire_codec!(#ty), #chain) };
    }
    chain
}

fn distributed_backward(graph: &Graph, hop: &Hop) -> TokenStream {
    let last = hop
        .backward_types
        .last()
        .expect("one type per backward edge");
    let mut chain = quote! { ::gdl::BwdEnd::new(::gdl::wire_codec!(#last)) };
    for (ident, ty) in hop.backward.iter().zip(&hop.backward_types).rev() {
        let f = field(graph.node(ident));
        chain = quote! { ::gdl::BwdStep::new(#f, ::gdl::wire_codec!(#ty), #chain) };
    }
    chain
}

fn expand_local(graph: &Graph) -> TokenStream {
    let Signature {
        generics,
        bounds,
        fields,
        req,
        item,
    } = signature(graph);
    let hops = graph.hops.iter().enumerate().rev().map(|(index, hop)| {
        let name = format_ident!("__gdl_local_{}", index);
        let mut fwd = quote!(());
        for ident in hop.forward.iter().rev() {
            let f = field(graph.node(ident));
            fwd = quote! { (#f, #fwd) };
        }
        let mut bwd = quote!(());
        for ident in hop.backward.iter().rev() {
            let f = field(graph.node(ident));
            bwd = quote! { (#f, #bwd) };
        }
        let inner_field = field(graph.node(&hop.inner));
        let inner = match graph.node(&hop.inner).role() {
            Role::Sink => quote! { ::gdl::SinkStage::new(#inner_field) },
            _ => {
                let next = format_ident!("__gdl_local_{}", index + 1);
                quote! { ::gdl::OperatorStage::new(#inner_field, #next) }
            }
        };
        quote! {
            let #name = ::gdl::Hop::new(#fwd, #inner, #bwd);
        }
    });
    quote! {
        /// The whole graph in one process, as nested calls.
        pub fn local<#(#generics),*>(
            bindings: Bindings<#(#generics),*>,
        ) -> ::gdl::Source<impl ::gdl::Next<#req, #item> + use<#(#generics),*>>
        where
            #(#bounds,)*
        {
            let Bindings { #(#fields),* } = bindings;
            #(#hops)*
            ::gdl::Source::new(__gdl_local_0)
        }
    }
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

fn is_keyword(word: &str) -> bool {
    matches!(
        word,
        "as" | "async"
            | "await"
            | "break"
            | "const"
            | "continue"
            | "crate"
            | "dyn"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "gen"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "try"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "yield"
    )
}
