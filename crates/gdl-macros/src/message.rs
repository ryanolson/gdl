//! `#[derive(Message)]`: implement `Message` and `Shape`, and register the
//! type in the catalogue of every program that links its crate.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::DeriveInput;
use syn::ext::IdentExt as _;
use syn::spanned::Spanned;

/// The expansion, naming the support items through `root`: the
/// `__private` module of `gdl_message` or of `gdl`, whichever crate the
/// derive was reached through. A derive has no `$crate`.
pub(crate) fn expand(input: &DeriveInput, root: &TokenStream) -> syn::Result<TokenStream> {
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
        impl #root::MessageTrait for #ident {
            const NAME: &'static str = #name;
            const IDENTITY: #root::Identity = #root::Identity {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                path: ::core::concat!(::core::module_path!(), "::", #name),
            };
        }
    };
    // The `const _` block keeps the static's name out of the caller's
    // module.
    Ok(quote! {
        #message

        impl #root::Shape for #ident {
            const STREAMED: bool = false;
            const NESTED: bool = false;
            const HEAD: ::core::option::Option<#root::Identity> =
                ::core::option::Option::Some(<#ident as #root::MessageTrait>::IDENTITY);
            const ITEM: ::core::option::Option<#root::Identity> = ::core::option::Option::None;
        }

        const _: () = {
            #[#root::linkme::distributed_slice(#root::GDL_MESSAGE_CATALOGUE)]
            #[linkme(crate = #root::linkme)]
            static __GDL_MESSAGE: #root::Entry = #root::Entry {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                name: #name,
                module: ::core::module_path!(),
                wire: || {
                    #[allow(unused_imports)]
                    use #root::{WireNo as _, WireYes as _};
                    (&#root::WireProbe::<#ident>::new()).__gdl_wire()
                },
            };
        };
    })
}
