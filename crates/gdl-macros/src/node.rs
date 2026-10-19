//! `#[gdl::node]`: check a node type against its crate's nodes file.
//!
//! The generator reads a node crate's nodes file without compiling the
//! crate. This attribute is what keeps the file true: when the crate
//! compiles, the type must be a declared node, reachable at the crate root
//! under the node's name, implementing the declared role with the declared
//! message at each port.

use std::path::{Path, PathBuf};

use gdl_syntax::{Node, Request, Sig};
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::ext::IdentExt as _;
use syn::spanned::Spanned as _;
use syn::{DeriveInput, Ident, LitStr, Type};

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new(
            attr.span(),
            "`#[gdl::node]` takes no arguments: the type's name is the node's",
        ));
    }
    let input: DeriveInput = syn::parse2(item.clone())?;
    let ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "a node type cannot be generic: a nodes file declares one type per node",
        ));
    }
    let name = ident.unraw().to_string();
    if !matches!(input.vis, syn::Visibility::Public(_)) {
        return Err(syn::Error::new(
            ident.span(),
            format!(
                "node `{name}`: a node type must be `pub`, for the generated segment crates to name it"
            ),
        ));
    }
    let (manifest, file, shown) = nodes_file(ident.span())?;
    let text = std::fs::read_to_string(&file)
        .map_err(|e| syn::Error::new(ident.span(), format!("reading {}: {e}", file.display())))?;
    let declared = text
        .parse::<TokenStream>()
        .map_err(|e| syn::Error::new(ident.span(), format!("{shown}: {e}")))
        .and_then(|tokens| {
            gdl_syntax::declarations(tokens)
                .map_err(|e| syn::Error::new(ident.span(), format!("{shown}: {e}")))
        })?;
    // `r#Engine` and `Engine` are one name, on either side.
    let Some(node) = declared.iter().find(|n| n.ident.unraw() == name) else {
        let names: Vec<String> = declared
            .iter()
            .map(|n| format!("`{}`", n.ident.unraw()))
            .collect();
        return Err(syn::Error::new(
            ident.span(),
            format!(
                "`{name}` is not declared in {shown}, which declares {}",
                if names.is_empty() {
                    "no node".to_string()
                } else {
                    names.join(", ")
                }
            ),
        ));
    };

    let mut checks = Vec::new();
    for (role, port, expected) in ports(node) {
        let (head, item) = expected.map_err(|ty| {
            syn::Error::new(
                ident.span(),
                format!("{shown}: node `{name}` names `{ty}`, which is not a message name"),
            )
        })?;
        let role_ident = Ident::new(role, ident.span());
        let port_ident = Ident::new(port, ident.span());
        let declared = match &item {
            None => head.clone(),
            Some(item) if head == "()" => format!("stream {item}"),
            Some(item) => format!("{head} + stream {item}"),
        };
        let message = LitStr::new(
            &format!(
                "node `{name}`: its `{role}::{port}` is not `{declared}`, as {shown} declares"
            ),
            ident.span(),
        );
        let item = match item {
            None => quote!(::core::option::Option::None),
            Some(item) => quote!(::core::option::Option::Some(#item)),
        };
        checks.push(quote_spanned! {ident.span()=>
            const _: () = ::core::assert!(
                ::gdl::__private::shape_is::<<#ident as ::gdl::#role_ident>::#port_ident>(#head, #item),
                #message
            );
        });
    }
    let file = LitStr::new(&file.to_string_lossy(), Span::call_site());
    let manifest = LitStr::new(&manifest.to_string_lossy(), Span::call_site());
    let root = quote_spanned! {ident.span()=>
        // The generator names the type `<crate>::<Node>`: the same type must
        // be there.
        fn __gdl_node_is_at_the_crate_root(
            node: ::core::marker::PhantomData<#ident>,
        ) -> ::core::marker::PhantomData<crate::#ident> {
            node
        }
    };
    Ok(quote! {
        #item

        const _: () = {
            // An edit to the nodes file, or to the manifest that names it,
            // rebuilds the crate.
            const _: &str = ::core::include_str!(#file);
            const _: &str = ::core::include_str!(#manifest);
            #root
            #(#checks)*
        };
    })
}

/// The manifest of the crate being compiled, its nodes file, and the
/// file's path as the manifest writes it.
fn nodes_file(span: Span) -> syn::Result<(PathBuf, PathBuf, String)> {
    let missing = || {
        syn::Error::new(
            span,
            "`#[gdl::node]` needs `[package.metadata.gdl] nodes = \"<file>\"` in Cargo.toml",
        )
    };
    let dir = std::env::var_os("CARGO_MANIFEST_DIR").ok_or_else(missing)?;
    let dir = Path::new(&dir);
    let manifest = dir.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest).map_err(|_| missing())?;
    let table: toml::Table = toml::from_str(&text).map_err(|_| missing())?;
    let file = table
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("gdl"))
        .and_then(|g| g.get("nodes"))
        .and_then(toml::Value::as_str)
        .ok_or_else(missing)?;
    Ok((manifest, dir.join(file), file.to_string()))
}

/// The expected message names, or the position that holds no message name,
/// as written.
type Names = Result<(String, Option<String>), String>;

/// Each port of a declared node: its role trait, the associated type, and
/// the message names the declaration gives it.
fn ports(node: &Node) -> Vec<(&'static str, &'static str, Names)> {
    let request = |req: &Request| -> Names {
        let head = name(&req.head)?;
        match &req.item {
            None => Ok((head, None)),
            Some(item) => Ok((head, Some(name(item)?))),
        }
    };
    let value = |ty: &Type| -> Names { Ok((name(ty)?, None)) };
    match &node.sig {
        Sig::Sink { req, item } => {
            vec![("Sink", "Req", request(req)), ("Sink", "Item", value(item))]
        }
        Sig::Forward { input, output } => vec![
            ("Forward", "In", request(input)),
            ("Forward", "Out", request(output)),
        ],
        Sig::Backward { input, output } => vec![
            ("Backward", "In", value(input)),
            ("Backward", "Out", value(output)),
        ],
        Sig::Operator {
            up_req,
            up_item,
            down_req,
            down_item,
        } => vec![
            ("Operator", "UpReq", request(up_req)),
            ("Operator", "UpItem", value(up_item)),
            ("Operator", "DownReq", request(down_req)),
            ("Operator", "DownItem", value(down_item)),
        ],
        Sig::Source { .. } => unreachable!("checked: a nodes file declares no source"),
    }
}

/// A message position's name, `()` for the unit.
fn name(ty: &Type) -> Result<String, String> {
    match gdl_syntax::message_name(ty) {
        Ok(Some(name)) => Ok(name),
        Ok(None) => Ok("()".to_string()),
        Err(()) => Err(quote!(#ty).to_string()),
    }
}
