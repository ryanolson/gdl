//! A proc macro that depends on another `gdl`.

use proc_macro::TokenStream;

/// Expands to nothing.
#[proc_macro]
pub fn nothing(_: TokenStream) -> TokenStream {
    let _ = gdl::GDL_MESSAGE_CATALOGUE.len();
    TokenStream::new()
}
