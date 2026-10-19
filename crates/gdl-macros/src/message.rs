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
    // `Message: Send + 'static`: every port type crosses tasks. The impl is
    // spanned at the type, so a type that is not `Send` is reported at its
    // definition, and a protocols crate cannot publish a message that no
    // graph can carry.
    let message = quote_spanned! {ident.span()=>
        impl ::gdl_message::Message for #ident {
            const NAME: &'static str = #name;
            const PACKAGE: &'static str = ::core::env!("CARGO_PKG_NAME");
            const PATH: &'static str = ::core::concat!(::core::module_path!(), "::", #name);
        }
    };
    // A derive has no `$crate`, so the expansion names `::gdl_message`,
    // which a crate deriving `Message` depends on. The `const _` block keeps
    // the static's name out of the caller's module.
    Ok(quote! {
        #message

        impl ::gdl_message::Shape for #ident {
            const HEAD: &'static str = #name;
            const ITEM: ::core::option::Option<&'static str> = ::core::option::Option::None;
        }

        const _: () = {
            #[::gdl_message::__private::linkme::distributed_slice(::gdl_message::__private::GDL_MESSAGE_CATALOGUE)]
            #[linkme(crate = ::gdl_message::__private::linkme)]
            static __GDL_MESSAGE: ::gdl_message::__private::Entry = ::gdl_message::__private::Entry {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                name: #name,
                module: ::core::module_path!(),
                wire: || ::gdl_message::__gdl_wire!(#ident),
            };
        };
    })
}
