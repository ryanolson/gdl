//! `#[derive(Message)]`: register a type in the message catalogue of every
//! program that links its crate.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::DeriveInput;
use syn::ext::IdentExt as _;
use syn::spanned::Spanned;

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "a message cannot be generic: a graph names it by its name alone, so it must be one concrete type",
        ));
    }
    let ident = &input.ident;
    // `r#Prompt` is `Prompt`: graphs name the type, not its spelling.
    let name = ident.unraw().to_string();
    // Every port type crosses tasks. Checked here, at the definition, so a
    // protocols crate cannot publish a message that no graph can carry.
    let bounds = quote_spanned! {ident.span()=>
        fn __gdl_message<T: ::core::marker::Send + 'static>() {}
        let _ = __gdl_message::<#ident>;
    };
    // A derive has no `$crate`, so the expansion names gdl as `::gdl`. The
    // `const _` block keeps the static's name out of the caller's module.
    Ok(quote! {
        const _: () = {
            #bounds
            #[::gdl::__private::linkme::distributed_slice(::gdl::__private::GDL_MESSAGE_CATALOGUE)]
            #[linkme(crate = ::gdl::__private::linkme)]
            static __GDL_MESSAGE: ::gdl::__private::MessageEntry = ::gdl::__private::MessageEntry {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                name: #name,
                module: ::core::module_path!(),
                wire: || ::gdl::wire_codec!(#ident).is_some(),
            };
        };
    })
}
