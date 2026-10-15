//! The `graph!` macro: one declaration of a graph's nodes, message types,
//! and topology, expanded into everything the runtime and the generator need
//! from it. Re-exported as `gdl::graph!`; see that crate for the syntax.

use proc_macro::TokenStream;

mod expand;
mod model;
mod parse;

/// Declare a graph. See `gdl::graph!` for the syntax and what it generates.
#[proc_macro]
pub fn graph(input: TokenStream) -> TokenStream {
    let parsed = syn::parse_macro_input!(input as parse::GraphInput);
    match model::Graph::check(parsed) {
        Ok(graph) => expand::expand(&graph).into(),
        Err(error) => error.to_compile_error().into(),
    }
}
