//! `#[derive(Node)]`: register a node type, with each role trait it
//! implements and the messages at its ports.
//!
//! A derive sees the type, not its impls, so the role is probed: for each
//! role trait, an autoref probe calls a method that exists only when the
//! type implements the trait with a message at every port. The probe runs
//! where the type is concrete, as the wire probe does. The source reader
//! finds the same impl in the crate's source.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::DeriveInput;
use syn::ext::IdentExt as _;
use syn::spanned::Spanned as _;

/// The expansion. Node crates depend on gdl, so it names the support items
/// through `root`, gdl's `__private` module.
pub(crate) fn expand(input: &DeriveInput, root: &TokenStream) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let name = ident.unraw().to_string();
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "a node type cannot be generic: a graph names one type per node",
        ));
    }
    if !matches!(input.vis, syn::Visibility::Public(_)) {
        return Err(syn::Error::new(
            ident.span(),
            format!(
                "node `{name}`: a node type must be `pub`, for the generated segment crates to name it"
            ),
        ));
    }
    // The generated segments name the type `<crate>::<Name>`: the same type
    // must be there.
    let root_check = quote_spanned! {ident.span()=>
        fn __gdl_node_is_at_the_crate_root(
            node: ::core::marker::PhantomData<#ident>,
        ) -> ::core::marker::PhantomData<crate::#ident> {
            node
        }
    };
    Ok(quote! {
        const _: () = {
            #root_check

            #[#root::linkme::distributed_slice(#root::GDL_NODE_CATALOGUE)]
            #[linkme(crate = #root::linkme)]
            static __GDL_NODE: #root::Node = #root::Node {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                name: #name,
                krate: ::core::env!("CARGO_CRATE_NAME"),
                roles: || {
                    #[allow(unused_imports)]
                    use #root::{
                        BackwardNo as _, BackwardYes as _, ForwardNo as _, ForwardYes as _,
                        OperatorNo as _, OperatorYes as _, SinkNo as _, SinkYes as _,
                    };
                    let probe = #root::RoleProbe::<#ident>::new();
                    let mut roles = ::std::vec::Vec::new();
                    roles.extend((&probe).__gdl_sink());
                    roles.extend((&probe).__gdl_forward());
                    roles.extend((&probe).__gdl_backward());
                    roles.extend((&probe).__gdl_operator());
                    roles
                },
            };
        };
    })
}
