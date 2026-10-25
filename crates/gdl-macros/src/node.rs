//! `#[derive(Node)]`: register a node type, with each role trait it
//! implements and the messages at its ports.
//!
//! A derive sees the type, not its impls, so the role is probed: for each
//! role trait, an autoref probe calls a method that exists only when the
//! type implements the trait with a message at every port. The probe runs
//! where the type is concrete, as the wire probe does. The source reader
//! finds the same impl in the crate's source.
//!
//! A generic type is not concrete where it is derived, so its derive
//! registers nothing: it implements `Generic` for every instantiation, and
//! an exported alias registers the instantiation it names, probing it
//! there.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::DeriveInput;
use syn::ext::IdentExt as _;

/// The expansion. Node crates depend on gdl, so it names the support items
/// through `root`, gdl's `__private` module.
pub(crate) fn expand(input: &DeriveInput, root: &TokenStream) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let name = ident.unraw().to_string();
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
    let types = gdl_syntax::node_types(&input.attrs)?;
    if !input.generics.params.is_empty() {
        return generic(input, &types, root);
    }
    let config = if let Some(path) = &types.config {
        quote! { || {
            fn check<T: ::gdl::Build<Config = #path>>() {}
            check::<#ident>();
            <#path as ::gdl::ConfigSchema>::graph_fields()
        } }
    } else {
        quote!(::std::vec::Vec::new)
    };
    // The type the derive names must be the build's, for the source
    // reader; the definitions registered are the build's, named or not,
    // so `extract --verify` sees a node that leaves them unnamed.
    let named = types.metrics.as_ref().map(|path| {
        quote! {
            fn check<T: ::gdl::Build<Metrics = #path>>() {}
            check::<#ident>();
        }
    });
    let probed = metrics(root, &quote!(#ident));
    let metrics = quote! { || {
        #named
        (#probed)()
    } };
    let roles = roles(root, &quote!(#ident));
    Ok(quote! {
        const _: () = {
            #root_check

            #[#root::linkme::distributed_slice(#root::GDL_NODE_CATALOGUE)]
            #[linkme(crate = #root::linkme)]
            static __GDL_NODE: #root::Node = #root::Node {
                config: #config,
                metrics: #metrics,
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                name: #name,
                krate: ::core::env!("CARGO_CRATE_NAME"),
                roles: #roles,
            };
        };
    })
}

/// The registration's `roles`: each role trait concrete type `ty`
/// implements, probed where it is concrete.
pub(crate) fn roles(root: &TokenStream, ty: &TokenStream) -> TokenStream {
    quote! { || {
        #[allow(unused_imports)]
        use #root::{
            BackwardNo as _, BackwardYes as _, ForwardNo as _, ForwardYes as _,
            OperatorNo as _, OperatorYes as _, SinkNo as _, SinkYes as _,
            FrontendNo as _, FrontendYes as _, ServiceNo as _, ServiceYes as _,
        };
        let probe = #root::RoleProbe::<#ty>::new();
        let mut roles = ::std::vec::Vec::new();
        roles.extend((&probe).__gdl_frontend());
        roles.extend((&probe).__gdl_service());
        roles.extend((&probe).__gdl_sink());
        roles.extend((&probe).__gdl_forward());
        roles.extend((&probe).__gdl_backward());
        roles.extend((&probe).__gdl_operator());
        roles
    } }
}

/// The registration's `metrics`: what concrete type `ty`'s build
/// registers, probed where it is concrete.
pub(crate) fn metrics(root: &TokenStream, ty: &TokenStream) -> TokenStream {
    quote! { || {
        #[allow(unused_imports)]
        use #root::{MetricsNo as _, MetricsYes as _};
        (&#root::RoleProbe::<#ty>::new()).__gdl_metrics()
    } }
}

/// A generic node: `Generic` for every instantiation, which an exported
/// alias registers. The configuration and metrics the derive names must be
/// the build's; that is a bound on the impl, so an alias of an
/// instantiation whose build takes others fails where it names it.
fn generic(
    input: &DeriveInput,
    types: &gdl_syntax::NodeTypes,
    root: &TokenStream,
) -> syn::Result<TokenStream> {
    crate::type_parameters_only(&input.generics, "node")?;
    let ident = &input.ident;
    let (impl_generics, ty_generics, clause) = input.generics.split_for_impl();
    let mut clause = clause.cloned().unwrap_or_else(|| syn::parse_quote!(where));
    let named: Vec<TokenStream> = [("Config", &types.config), ("Metrics", &types.metrics)]
        .into_iter()
        .filter_map(|(name, path)| {
            let name = quote::format_ident!("{name}");
            path.as_ref().map(|path| quote!(#name = #path))
        })
        .collect();
    if !named.is_empty() {
        clause
            .predicates
            .push(syn::parse_quote!(#ident #ty_generics: ::gdl::Build<#(#named),*>));
    }
    // `instantiation` reads the configuration's fields, which a parameter
    // has only when it is bounded so: the type need not say it.
    if let Some(path) = &types.config {
        clause
            .predicates
            .push(syn::parse_quote!(#path: ::gdl::ConfigSchema));
    }
    let config = match &types.config {
        Some(path) => quote!(<#path as ::gdl::ConfigSchema>::graph_fields),
        None => quote!(::std::vec::Vec::new),
    };
    Ok(quote! {
        impl #impl_generics #root::Generic for #ident #ty_generics #clause {
            const KIND: #root::Kind = #root::Kind::Node;
            fn instantiation() -> #root::Instantiation {
                #root::Instantiation::Node { config: #config }
            }
        }
    })
}
