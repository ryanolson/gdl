//! `#[derive(Message)]`: register a type in the message catalogue of every
//! program that links its crate.

use proc_macro2::TokenStream;
use quote::quote;
use syn::DeriveInput;
use syn::spanned::Spanned;

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "a message cannot be generic: a graph names it by its name alone, so it must be one concrete type",
        ));
    }
    let ident = &input.ident;
    let name = ident.to_string();
    // A derive has no `$crate`, so the expansion names gdl as `::gdl`. The
    // `const _` block keeps the static's name out of the caller's module.
    Ok(quote! {
        const _: () = {
            #[::gdl::__private::linkme::distributed_slice(::gdl::__private::MESSAGES)]
            #[linkme(crate = ::gdl::__private::linkme)]
            static __GDL_MESSAGE: ::gdl::__private::MessageEntry = ::gdl::__private::MessageEntry {
                package: ::core::env!("CARGO_PKG_NAME"),
                name: #name,
                module: ::core::module_path!(),
                wire: || ::gdl::wire_codec!(#ident).is_some(),
            };
        };
    })
}
