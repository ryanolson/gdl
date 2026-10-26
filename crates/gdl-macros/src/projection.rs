//! `#[derive(Projection)]`: implement `gdl::Projection` for a struct that
//! names its edge type with `#[gdl(of = <type>)]`, and check at compile
//! time that the edge type has each of the struct's fields, under the
//! same name and of the same type. Without the check, a missing field
//! would pass the segment's start and fail every request's read.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned as _;

pub(crate) fn expand(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "a projection is a struct with named fields",
        ));
    };
    let syn::Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &data.fields,
            "a projection reads fields by name, so its fields are named",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "a projection cannot be generic: it names one edge type",
        ));
    }
    let of = of(input)?;
    // serde would accept both, and every read would then fail or read
    // nothing: a projection skips the edge type's other fields, and the
    // edge type writes each field under one name.
    if let Some(attr) = serde_word(&input.attrs, "deny_unknown_fields") {
        return Err(syn::Error::new_spanned(
            attr,
            "a projection skips the fields it does not read, so it cannot deny unknown fields",
        ));
    }
    for field in &fields.named {
        if let Some(attr) = serde_word(&field.attrs, "alias") {
            return Err(syn::Error::new_spanned(
                attr,
                "a projection reads the one name its edge type writes, so its field takes no alias",
            ));
        }
    }
    // Each field is read through the edge type, spanned at the field, so
    // a field the edge type lacks is reported there with its name, and one
    // of another type as a mismatch. `field::<_, T>` takes the field's
    // type by inference, which never coerces: a `Box<T>` field is not a
    // `T` field. The reads are in the impl, where a field's type may name
    // `Self`.
    let checks = fields.named.iter().map(|field| {
        let name = field.ident.as_ref().expect("named fields");
        let ty = &field.ty;
        quote_spanned! {field.span()=>
            ::gdl::__private::field::<_, #ty>(&of.#name);
        }
    });
    let ident = &input.ident;
    Ok(quote! {
        impl ::gdl::__private::ProjectionDerived for #ident {}
        impl ::gdl::Projection for #ident {
            type Of = #of;

            fn __fields(of: &Self::Of) {
                let _ = of;
                #(#checks)*
            }
        }
    })
}

/// The type in `#[gdl(of = <type>)]`.
fn of(input: &syn::DeriveInput) -> syn::Result<syn::Type> {
    let mut of = None;
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("gdl")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("of") {
                of = Some(meta.value()?.parse::<syn::Type>()?);
                Ok(())
            } else {
                Err(meta.error("expected `of = <the edge type>`"))
            }
        })?;
    }
    of.ok_or_else(|| {
        syn::Error::new_spanned(
            &input.ident,
            "a projection names the edge type it reads a part of: `#[gdl(of = <type>)]`",
        )
    })
}

/// The first `#[serde(...)]` attribute that names `word` at its top level,
/// as `deny_unknown_fields` or `alias = "..."` are. A value such as
/// `with = "..."` is a literal, so it never matches.
fn serde_word<'a>(attrs: &'a [syn::Attribute], word: &str) -> Option<&'a syn::Attribute> {
    attrs.iter().find(|attr| {
        attr.path().is_ident("serde")
            && match &attr.meta {
                syn::Meta::List(list) => list.tokens.clone().into_iter().any(
                    |token| matches!(token, proc_macro2::TokenTree::Ident(ident) if ident == word),
                ),
                _ => false,
            }
    })
}
