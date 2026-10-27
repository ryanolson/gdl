//! `#[derive(Message)]`: implement `Message` and `Shape`, and register the
//! type in the catalogue of every program that links its crate. On a
//! generic type, implement `Shape` and `Generic` for every instantiation,
//! and register nothing: an exported alias registers each instantiation
//! where it is concrete.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::DeriveInput;
use syn::ext::IdentExt as _;

/// The expansion, naming the support items through `root`: the
/// `__private` module of `gdl_message` or of `gdl`, whichever crate the
/// derive was reached through. A derive has no `$crate`.
pub(crate) fn expand(input: &DeriveInput, root: &TokenStream) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    // `r#Prompt` is `Prompt`: graphs name the type, not its spelling.
    let name = ident.unraw().to_string();
    let attrs = input
        .attrs
        .iter()
        .map(|a| a.meta.clone())
        .collect::<Vec<_>>();
    let encoding = match gdl_syntax::message_encoding(&attrs)? {
        gdl_model::Encoding::Msgpack => quote!(#root::Encoding::Msgpack),
        gdl_model::Encoding::MsgpackArray => {
            positional(input, &attrs, &name)?;
            quote!(#root::Encoding::MsgpackArray)
        }
        gdl_model::Encoding::Json => quote!(#root::Encoding::Json),
    };
    if !input.generics.params.is_empty() {
        return generic(input, root, &encoding);
    }
    // `Message: Send + 'static`: every port type crosses tasks. The impl is
    // spanned at the type, so a type that is not `Send` is reported at its
    // definition, and a protocols crate cannot publish a message that no
    // graph can carry.
    let message = quote_spanned! {ident.span()=>
        impl #root::MessageTrait for #ident {
            const NAME: &'static str = #name;
            const ENCODING: #root::Encoding = #encoding;
            const IDENTITY: #root::Identity = #root::Identity {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                path: ::core::concat!(::core::module_path!(), "::", #name),
            };
        }
    };
    let wire = wire(root, &quote!(#ident));
    // The `const _` block keeps the static's name out of the caller's
    // module.
    Ok(quote! {
        #message

        impl #root::Offered for #ident {
            const ENCODING: #root::Encoding = #encoding;
            fn identity() -> #root::Identity {
                <#ident as #root::MessageTrait>::IDENTITY
            }
        }

        impl #root::Shape for #ident {
            const STREAMED: bool = false;
            const NESTED: bool = false;
            fn head() -> ::core::option::Option<#root::Identity> {
                ::core::option::Option::Some(<#ident as #root::MessageTrait>::IDENTITY)
            }
            fn item() -> ::core::option::Option<#root::Identity> {
                ::core::option::Option::None
            }
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
                encoding: #encoding,
                wire: #wire,
            };
        };
    })
}

/// Refuse a `msgpack_array` message with a field that one serde direction
/// omits: positions would shift, and the value would decode into the
/// wrong fields or not at all. Checked here, once, so that encoding a
/// value costs nothing more. A nested type's fields are not visible here.
fn positional(input: &DeriveInput, container: &[syn::Meta], name: &str) -> syn::Result<()> {
    let metas = |attrs: &[syn::Attribute]| attrs.iter().map(|a| a.meta.clone()).collect::<Vec<_>>();
    let parts: Vec<(Option<&syn::Variant>, &syn::Fields)> = match &input.data {
        syn::Data::Struct(data) => vec![(None, &data.fields)],
        syn::Data::Enum(data) => data.variants.iter().map(|v| (Some(v), &v.fields)).collect(),
        syn::Data::Union(_) => Vec::new(),
    };
    for (variant, fields) in parts {
        let attrs: Vec<Vec<syn::Meta>> = fields.iter().map(|f| metas(&f.attrs)).collect();
        let unnamed = matches!(fields, syn::Fields::Unnamed(_));
        let variant_attrs = variant.map(|v| metas(&v.attrs));
        let Some((index, key)) =
            gdl_syntax::positional_omission(container, &attrs, unnamed, variant_attrs.as_deref())
        else {
            continue;
        };
        let field = fields
            .iter()
            .nth(index)
            .expect("the omission is one of the fields");
        let own = field
            .ident
            .as_ref()
            .map_or_else(|| index.to_string(), |ident| ident.unraw().to_string());
        let label = match variant {
            Some(variant) => format!("{}::{own}", variant.ident.unraw()),
            None => own,
        };
        let at: &dyn quote::ToTokens = match &field.ident {
            Some(ident) => ident,
            None => &field.ty,
        };
        return Err(syn::Error::new_spanned(
            at,
            gdl_syntax::positional_refusal(name, &label, key),
        ));
    }
    Ok(())
}

/// The registration's `wire`: whether concrete type `ty` is a wire type,
/// probed where it is concrete, as `node::roles` probes a node's roles.
pub(crate) fn wire(root: &TokenStream, ty: &TokenStream) -> TokenStream {
    quote! { || {
        #[allow(unused_imports)]
        use #root::{WireNo as _, WireYes as _};
        (&#root::WireProbe::<#ty>::new()).__gdl_wire()
    } }
}

/// A generic message: every instantiation is a port type, with an identity
/// that names its type, and the generic type's encoding. Nothing is
/// registered: the catalogue holds concrete types, which an exported alias
/// names.
fn generic(
    input: &DeriveInput,
    root: &TokenStream,
    encoding: &TokenStream,
) -> syn::Result<TokenStream> {
    crate::type_parameters_only(&input.generics, "message")?;
    let ident = &input.ident;
    let (impl_generics, ty_generics, clause) = input.generics.split_for_impl();
    // Every message crosses tasks: an instantiation that is not `Send` is
    // refused where an alias names it.
    let mut sent = clause.cloned().unwrap_or_else(|| syn::parse_quote!(where));
    sent.predicates
        .push(syn::parse_quote!(#ident #ty_generics: ::core::marker::Send + 'static));
    // `type_name` writes each argument by its definition path, as the
    // source reader writes an instantiation; `extract --verify` compares
    // the two.
    let identity = quote! {
        #root::Identity {
            package: ::core::env!("CARGO_PKG_NAME"),
            version: ::core::env!("CARGO_PKG_VERSION"),
            dir: ::core::env!("CARGO_MANIFEST_DIR"),
            path: ::core::any::type_name::<Self>(),
        }
    };
    Ok(quote! {
        impl #impl_generics #root::Offered for #ident #ty_generics #clause {
            const ENCODING: #root::Encoding = #encoding;
            fn identity() -> #root::Identity {
                #identity
            }
        }

        impl #impl_generics #root::Shape for #ident #ty_generics #clause {
            const STREAMED: bool = false;
            const NESTED: bool = false;
            fn head() -> ::core::option::Option<#root::Identity> {
                ::core::option::Option::Some(<Self as #root::Offered>::identity())
            }
            fn item() -> ::core::option::Option<#root::Identity> {
                ::core::option::Option::None
            }
        }

        impl #impl_generics #root::Generic for #ident #ty_generics #sent {
            const KIND: #root::Kind = #root::Kind::Message;
            fn instantiation() -> #root::Instantiation {
                #root::Instantiation::Message {
                    identity: <Self as #root::Offered>::identity(),
                    encoding: #encoding,
                }
            }
        }
    })
}
