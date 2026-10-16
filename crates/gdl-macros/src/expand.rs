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
//! introduces start with `__gdl` or `__Gdl`; every other path it writes is
//! rooted (`::std`, `::core`, or the gdl path it was given).

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Ident, Type};

use crate::model::{Graph, Hop, Node, Role};
use crate::parse::Sig;

pub(crate) fn expand(graph: &Graph, krate: &TokenStream) -> TokenStream {
    let nodes = expand_nodes(graph, krate);
    let bindings = expand_bindings(graph, krate);
    let manifest = expand_manifest(graph, krate);
    let assemble = expand_assemble(graph, krate);
    let local = expand_local(graph, krate);
    let source = expand_source_types(graph);
    quote! {
        #source
        #nodes
        #bindings
        #manifest
        #assemble
        #local
    }
}

fn expand_source_types(graph: &Graph) -> TokenStream {
    let (req, item) = source_types(graph);
    quote! {
        /// The request type the graph's source sends.
        pub type SourceReq = #req;
        /// The item type the graph's source receives.
        pub type SourceItem = #item;
    }
}

fn source_types(graph: &Graph) -> (&Type, &Type) {
    match &graph.source().sig {
        Sig::Source { req, item } => (req, item),
        _ => unreachable!("checked: the source is a source"),
    }
}

/// Non-source nodes, in declaration order: the ones that take a binding.
fn bound_nodes(graph: &Graph) -> impl Iterator<Item = &Node> {
    graph.nodes.iter().filter(|n| n.role() != Role::Source)
}

/// The type parameter for a node's slot.
fn generic(node: &Node) -> Ident {
    format_ident!("__Gdl{}", node.ident)
}

fn field(node: &Node) -> &Ident {
    node.binding
        .as_ref()
        .expect("checked: a bound node has a binding")
}

/// The local a node's slot is moved into: prefixed, so it shadows neither a
/// parameter nor another local of the generated function.
fn local_var(node: &Node) -> Ident {
    let name = field(node).to_string();
    format_ident!("__gdl_{}", name.trim_start_matches("r#"))
}

fn role_bound(node: &Node, krate: &TokenStream) -> TokenStream {
    match &node.sig {
        Sig::Operator {
            up_req,
            up_item,
            down_req,
            down_item,
        } => quote! {
            #krate::Operator<UpReq = #up_req, UpItem = #up_item, DownReq = #down_req, DownItem = #down_item>
        },
        Sig::Forward { input, output } => quote! { #krate::Forward<In = #input, Out = #output> },
        Sig::Sink { req, item } => quote! { #krate::Sink<Req = #req, Item = #item> },
        Sig::Backward { input, output } => {
            quote! { #krate::Backward<In = #input, Out = #output> }
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
            Sig::Source { req, item } => (
                quote!(Source),
                quote! { impl #krate::SourceDecl for nodes::#ident { type Req = #req; type Item = #item; } },
            ),
            Sig::Operator {
                up_req,
                up_item,
                down_req,
                down_item,
            } => (
                quote!(Operator),
                quote! {
                    impl #krate::OperatorDecl for nodes::#ident {
                        type UpReq = #up_req;
                        type UpItem = #up_item;
                        type DownReq = #down_req;
                        type DownItem = #down_item;
                    }
                },
            ),
            Sig::Forward { input, output } => (
                quote!(Forward),
                quote! { impl #krate::ForwardDecl for nodes::#ident { type In = #input; type Out = #output; } },
            ),
            Sig::Sink { req, item } => (
                quote!(Sink),
                quote! { impl #krate::SinkDecl for nodes::#ident { type Req = #req; type Item = #item; } },
            ),
            Sig::Backward { input, output } => (
                quote!(Backward),
                quote! { impl #krate::BackwardDecl for nodes::#ident { type In = #input; type Out = #output; } },
            ),
        };
        quote! {
            impl #krate::NodeDecl for nodes::#ident {
                const NAME: &'static str = #name;
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
                quote!(__GdlBound)
            } else {
                quote!(#g)
            }
        });
        let moves = fields.iter().enumerate().map(|(i, f)| {
            if i == index {
                quote! { #f: ::std::sync::Arc::new(implementation) }
            } else {
                quote! { #f: self.#f }
            }
        });
        quote! {
            #[doc = #doc]
            pub fn #setter<__GdlBound: #bound>(self, implementation: __GdlBound) -> Bindings<#(#out_generics),*> {
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
    quote! { ::std::string::String::from(#text) }
}

fn type_info(ty: &Type, krate: &TokenStream) -> TokenStream {
    let name = quote!(#ty).to_string();
    quote! { #krate::TypeInfo::new(#name, #krate::wire_codec!(#ty).is_some()) }
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
        let forward_types = hop.forward_types.iter().map(|t| type_info(t, krate));
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
        unpack: quote! { let Bindings { #(#fields: #locals),* } = bindings; },
    }
}

fn expand_assemble(graph: &Graph, krate: &TokenStream) -> TokenStream {
    let Signature {
        generics,
        bounds,
        unpack,
    } = signature(graph, krate);
    let (req, item) = source_types(graph);
    // Innermost hop first, so each operator can take the hop below it.
    let hops = graph.hops.iter().enumerate().rev().map(|(index, hop)| {
        let name = format_ident!("__gdl_hop_{}", index);
        let index_lit = index as u16;
        let fwd = distributed_forward(graph, hop, krate);
        let bwd = distributed_backward(graph, hop, krate);
        let inner_node = graph.node(&hop.inner);
        let inner_local = local_var(inner_node);
        let inner = match inner_node.role() {
            Role::Sink => quote! { #krate::DSink::new(#inner_local) },
            _ => {
                let next = format_ident!("__gdl_hop_{}", index + 1);
                quote! { #krate::DOperator::new(#inner_local, #next) }
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
            deployment: &#krate::Deployment,
            bindings: Bindings<#(#generics),*>,
        ) -> #krate::Result<impl #krate::Next<#req, #item> + #krate::Register + use<#(#generics),*>>
        where
            #(#bounds,)*
        {
            #unpack
            #(#hops)*
            ::core::result::Result::Ok(__gdl_hop_0)
        }
    }
}

fn distributed_forward(graph: &Graph, hop: &Hop, krate: &TokenStream) -> TokenStream {
    let last = hop.forward_types.last().expect("one type per forward edge");
    let mut chain = quote! { #krate::FwdEnd::new(#krate::wire_codec!(#last)) };
    for (ident, ty) in hop.forward.iter().zip(&hop.forward_types).rev() {
        let f = local_var(graph.node(ident));
        chain = quote! { #krate::FwdStep::new(#f, #krate::wire_codec!(#ty), #chain) };
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
    let (req, item) = source_types(graph);
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
            Role::Sink => quote! { #krate::SinkStage::new(#inner_local) },
            _ => {
                let next = format_ident!("__gdl_local_{}", index + 1);
                quote! { #krate::OperatorStage::new(#inner_local, #next) }
            }
        };
        quote! {
            let #name = #krate::Hop::new(#fwd, #inner, #bwd);
        }
    });
    quote! {
        /// The whole graph in one process, as nested calls.
        pub fn local<#(#generics),*>(
            bindings: Bindings<#(#generics),*>,
        ) -> #krate::Source<impl #krate::Next<#req, #item> + use<#(#generics),*>>
        where
            #(#bounds,)*
        {
            #unpack
            #(#hops)*
            #krate::Source::new(__gdl_local_0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every path the expansion writes to the runtime goes through the path
    /// it was given, so a contract crate can depend on gdl under any name.
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
        let graph = Graph::check(syn::parse2(input).expect("parse")).expect("check");
        let expansion = expand(&graph, &quote!(::pipeline));
        let mut idents = Vec::new();
        collect_idents(expansion, &mut idents);
        assert!(idents.iter().any(|i| i == "pipeline"));
        assert!(
            !idents.iter().any(|i| i == "gdl"),
            "the expansion names `gdl` directly"
        );
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
