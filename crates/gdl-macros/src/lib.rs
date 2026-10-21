//! The `graph!` macro: one declaration of a graph's nodes, message types,
//! and topology, expanded into everything the runtime and the generator need
//! from it. Reached through `gdl::graph!`; see that macro for the syntax.
//! Also the markers that offer a crate's messages and nodes to graphs:
//! `derive(Message)` and `#[export]`, each reached through `gdl_message` or
//! through `gdl`, and `derive(Node)`, reached through `gdl`.

use proc_macro::TokenStream;
use proc_macro2::{TokenStream as Tokens, TokenTree};

mod config;
mod endpoint;
mod expand;
mod export;
mod message;
mod metrics;
mod node;

/// The expansion behind `gdl::graph!`, which calls it as
/// `graph! { $crate; <graph> }`. The leading path is how the expansion names
/// the runtime crate: a contract crate may depend on gdl under another name,
/// and `$crate` resolves to it whatever the name is.
#[doc(hidden)]
#[proc_macro]
pub fn graph(input: TokenStream) -> TokenStream {
    let Some((krate, body)) = split_crate(input.into()) else {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "expected `<path to gdl>;` before the graph; use `gdl::graph!`",
        )
        .to_compile_error()
        .into();
    };
    match gdl_syntax::Graph::parse(body) {
        Ok(graph) => expand::expand(&graph, &krate).into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// The expansion behind `gdl_message::Message`: the `Message` and `Shape`
/// impls, and one catalogue entry for the type, named through
/// `::gdl_message`.
#[proc_macro_derive(Message)]
pub fn message(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    result(message::expand(&input, &through_message()))
}

/// The same derive, reached as `gdl::Message`: named through `::gdl`, so a
/// crate that depends on gdl alone can use it.
#[proc_macro_derive(GdlMessage)]
pub fn gdl_message(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    result(message::expand(&input, &through_gdl()))
}

/// The expansion behind `gdl_message::export`.
#[proc_macro_attribute]
pub fn export(attr: TokenStream, item: TokenStream) -> TokenStream {
    result(export::expand(attr.into(), item.into(), &through_message()))
}

/// The same attribute, reached as `gdl::export`.
#[proc_macro_attribute]
pub fn gdl_export(attr: TokenStream, item: TokenStream) -> TokenStream {
    result(export::expand(attr.into(), item.into(), &through_gdl()))
}

/// The expansion behind `gdl::Node`.
#[proc_macro_derive(Node, attributes(gdl))]
pub fn node(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    result(node::expand(&input, &through_gdl()))
}

fn through_message() -> Tokens {
    quote::quote!(::gdl_message::__private)
}

fn through_gdl() -> Tokens {
    quote::quote!(::gdl::__private)
}

fn result(expansion: syn::Result<Tokens>) -> TokenStream {
    match expansion {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// The tokens before the first top-level `;`, and the rest.
fn split_crate(input: Tokens) -> Option<(Tokens, Tokens)> {
    let mut tokens = input.into_iter();
    let mut krate = Tokens::new();
    for token in tokens.by_ref() {
        if matches!(&token, TokenTree::Punct(p) if p.as_char() == ';') {
            return (!krate.is_empty()).then(|| (krate, tokens.collect()));
        }
        krate.extend([token]);
    }
    None
}

/// Expose explicitly marked graph configuration fields.
#[proc_macro_derive(Config, attributes(gdl))]
pub fn config(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    result(config::expand(&input))
}

/// A node's metrics, from a struct of prometheus metric fields.
#[proc_macro_derive(Metrics, attributes(gdl))]
pub fn metrics(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    result(metrics::expand(&input))
}

/// Describe the typed entry clients of a hosted service.
#[proc_macro_derive(Endpoints, attributes(gdl))]
pub fn endpoints(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    result(endpoint::expand(&input))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idents(tokens: Tokens, out: &mut Vec<String>) {
        for token in tokens {
            match token {
                TokenTree::Ident(ident) => out.push(ident.to_string()),
                TokenTree::Group(group) => idents(group.stream(), out),
                _ => {}
            }
        }
    }

    /// A crate that depends on gdl alone can use the markers that gdl
    /// re-exports: their expansions name gdl, never `gdl_message`.
    #[test]
    fn markers_through_gdl_name_only_gdl() {
        let input: syn::DeriveInput = syn::parse_quote!(
            pub struct Words;
        );
        let use_item = quote::quote!(
            pub use inner::Words;
        );
        let expansions = [
            message::expand(&input, &through_gdl()).expect("message"),
            export::expand(Tokens::new(), use_item, &through_gdl()).expect("export"),
            node::expand(&input, &through_gdl()).expect("node"),
        ];
        for expansion in expansions {
            let mut names = Vec::new();
            idents(expansion, &mut names);
            assert!(names.iter().any(|n| n == "gdl"), "{names:?}");
            assert!(!names.iter().any(|n| n == "gdl_message"), "{names:?}");
        }
        let mut names = Vec::new();
        idents(
            message::expand(&input, &through_message()).expect("message"),
            &mut names,
        );
        assert!(names.iter().any(|n| n == "gdl_message"), "{names:?}");
    }
}
