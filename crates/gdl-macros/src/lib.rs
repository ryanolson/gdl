//! The `graph!` macro: one declaration of a graph's nodes, message types,
//! and topology, expanded into everything the runtime and the generator need
//! from it. Reached through `gdl::graph!`; see that macro for the syntax.
//! Also `#[derive(Message)]`, reached as `gdl_message::Message`.

use proc_macro::TokenStream;
use proc_macro2::{TokenStream as Tokens, TokenTree};

mod expand;
mod message;

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
/// impls, and one catalogue entry for the type.
#[proc_macro_derive(Message)]
pub fn message(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    match message::expand(&input) {
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
