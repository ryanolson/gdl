//! `#[export]`: keep a `pub use` of messages, and register each name it
//! binds with the identity of the message it names.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt as _;
use syn::spanned::Spanned as _;
use syn::{Ident, ItemUse, UseTree};

/// The expansion, naming the support items through `root`, as for
/// `derive(Message)`.
pub(crate) fn expand(
    attr: TokenStream,
    item: TokenStream,
    root: &TokenStream,
) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new(
            attr.span(),
            "`#[export]` takes no arguments: the `use` names what it exports",
        ));
    }
    let item: ItemUse = syn::parse2(item)?;
    if !matches!(item.vis, syn::Visibility::Public(_)) {
        return Err(syn::Error::new(
            item.span(),
            "an export offers a public path: write `pub use`",
        ));
    }
    let mut names = Vec::new();
    bindings(&item.tree, &mut names)?;
    let statics = names.iter().enumerate().map(|(index, ident)| {
        let name = ident.unraw().to_string();
        let static_ident = format_ident!("__GDL_EXPORT_{index}");
        quote! {
            #[#root::linkme::distributed_slice(#root::GDL_EXPORT_CATALOGUE)]
            #[linkme(crate = #root::linkme)]
            static #static_ident: #root::Export = #root::Export {
                package: ::core::env!("CARGO_PKG_NAME"),
                version: ::core::env!("CARGO_PKG_VERSION"),
                dir: ::core::env!("CARGO_MANIFEST_DIR"),
                name: #name,
                module: ::core::module_path!(),
                origin: <#ident as #root::MessageTrait>::IDENTITY,
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

/// Each name a use tree binds. A glob binds names no one wrote, so it
/// cannot be an export.
fn bindings(tree: &UseTree, out: &mut Vec<Ident>) -> syn::Result<()> {
    match tree {
        UseTree::Path(p) => bindings(&p.tree, out),
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
        UseTree::Group(g) => g.items.iter().try_for_each(|t| bindings(t, out)),
    }
}
