//! A proc macro that reads a catalogue while two are linked into it.

use proc_macro::TokenStream;

/// Panics in the compiler: two catalogues of one name are linked here.
#[proc_macro]
pub fn boom(_: TokenStream) -> TokenStream {
    let _ = stray::GDL_MESSAGE_CATALOGUE.len();
    let _ = gdl::Catalogue::linked();
    TokenStream::new()
}
