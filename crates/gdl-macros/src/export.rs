//! `#[export]`: keep a `pub use` of messages, and register each name it
//! binds with the identity of the message it names; or keep a `pub type`
//! that names an instantiation of a generic message or node, and register
//! the instantiation under the alias's name.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::ext::IdentExt as _;
use syn::spanned::Spanned as _;
use syn::{Ident, ItemType, ItemUse, UseTree};

/// What an export may offer: messages alone, through `gdl_message`, which
/// knows no node; or messages and nodes, through gdl.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Offers {
    Messages,
    MessagesAndNodes,
}

/// The expansion, naming the support items through `root`, as for
/// `derive(Message)`.
pub(crate) fn expand(
    attr: TokenStream,
    item: TokenStream,
    root: &TokenStream,
    offers: Offers,
) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new(
            attr.span(),
            "`#[export]` takes no arguments: the `use` or the `type` names what it exports",
        ));
    }
    let item: syn::Item = syn::parse2(item)?;
    let item = match item {
        syn::Item::Use(item) => item,
        syn::Item::Type(item) => return alias(item, root, offers),
        other => {
            return Err(syn::Error::new(
                other.span(),
                "`#[export]` goes on a `pub use` of messages, or on a `pub type` that names an instantiation of a generic message or node",
            ));
        }
    };
    uses(item, root)
}

fn uses(item: ItemUse, root: &TokenStream) -> syn::Result<TokenStream> {
    if !matches!(item.vis, syn::Visibility::Public(_)) {
        return Err(syn::Error::new(
            item.span(),
            "an export offers a public path: write `pub use`",
        ));
    }
    let mut names = Vec::new();
    bindings(&item.tree, None, &mut names)?;
    let statics = names.iter().enumerate().map(|(index, ident)| {
        let name = ident.unraw().to_string();
        let static_ident = format_ident!("__GDL_EXPORT_{index}");
        let wire = crate::message::wire(root, &quote!(#ident));
        // Spanned at the name, so that a message that cannot cross tasks
        // is reported where the export offers it.
        let sent = quote_spanned! {ident.span()=>
            const _: () = #root::sent::<#ident>();
        };
        quote! {
            #sent

            #[#root::linkme::distributed_slice(#root::GDL_EXPORT_CATALOGUE)]
            #[linkme(crate = #root::linkme)]
            static #static_ident: #root::Export = #root::Export {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                name: #name,
                module: ::core::module_path!(),
                origin: <#ident as #root::Offered>::identity,
                encoding: <#ident as #root::Offered>::ENCODING,
                wire: #wire,
            };
        }
    });
    Ok(quote! {
        #item

        const _: () = {
            #(#statics)*
        };
    })
}

/// Each name a use tree binds; `parent` is the segment before `tree`,
/// which `{self}` binds. A glob binds names no one wrote, so it cannot be
/// an export.
fn bindings(tree: &UseTree, parent: Option<&Ident>, out: &mut Vec<Ident>) -> syn::Result<()> {
    match tree {
        UseTree::Path(p) => bindings(&p.tree, Some(&p.ident), out),
        UseTree::Name(n) if n.ident == "self" => match parent {
            Some(parent) => {
                out.push(parent.clone());
                Ok(())
            }
            None => Err(syn::Error::new(n.ident.span(), "`self` names no message")),
        },
        UseTree::Name(n) => {
            out.push(n.ident.clone());
            Ok(())
        }
        UseTree::Rename(r) => {
            if r.rename == "_" {
                return Err(syn::Error::new(
                    r.rename.span(),
                    "an export offers a name: `as _` offers none",
                ));
            }
            out.push(r.rename.clone());
            Ok(())
        }
        UseTree::Glob(g) => Err(syn::Error::new(
            g.star_token.span,
            "an export names each message it offers: a glob names none",
        )),
        UseTree::Group(g) => g.items.iter().try_for_each(|t| bindings(t, parent, out)),
    }
}

/// A `pub type` of an instantiation: kept, and registered under its name
/// with what the generic type's derive states and what is probed here,
/// where the type is concrete.
fn alias(item: ItemType, root: &TokenStream, offers: Offers) -> syn::Result<TokenStream> {
    if !matches!(item.vis, syn::Visibility::Public(_)) {
        return Err(syn::Error::new(
            item.span(),
            "an export offers a public path: write `pub type`",
        ));
    }
    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            item.generics.span(),
            "an alias names one instantiation: it takes no parameters of its own",
        ));
    }
    let ident = &item.ident;
    let name = ident.unraw().to_string();
    let ty = quote!(#ident);
    let wire = crate::message::wire(root, &ty);
    // `gdl_message` cannot probe a node's roles: it does not know them.
    let (kind, roles, metrics) = match offers {
        Offers::Messages => (
            quote! {
                const _: () = ::core::assert!(
                    ::core::matches!(<#ident as #root::Generic>::KIND, #root::Kind::Message),
                    "`gdl_message::export` offers messages: export a node's instantiation with `gdl::export`",
                );
            },
            quote!(::std::vec::Vec::new),
            quote!(::std::vec::Vec::new),
        ),
        Offers::MessagesAndNodes => (
            TokenStream::new(),
            crate::node::roles(root, &ty),
            crate::node::metrics(root, &ty),
        ),
    };
    Ok(quote! {
        #item

        const _: () = {
            #kind

            #[#root::linkme::distributed_slice(#root::GDL_ALIAS_CATALOGUE)]
            #[linkme(crate = #root::linkme)]
            static __GDL_ALIAS: #root::Alias = #root::Alias {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                name: #name,
                module: ::core::module_path!(),
                krate: ::core::env!("CARGO_CRATE_NAME"),
                instantiation: <#ident as #root::Generic>::instantiation,
                wire: #wire,
                metrics: #metrics,
                roles: #roles,
            };
        };
    })
}
