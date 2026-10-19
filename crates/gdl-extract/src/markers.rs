//! The three markers, found in a crate's modules, and the entries they make.

use quote::ToTokens as _;
use syn::punctuated::Punctuated;
use syn::{Meta, Token};

use crate::cfg::Settings;
use crate::metadata::Graph;
use crate::resolve::{Def, Resolver, segments};
use crate::tree::{Location, TypeDef};
use crate::{ExportEntry, Extraction, MessageEntry, NodeEntry, Offer, Origin, PortEntry};

/// What package `package` offers.
pub(crate) fn extract(graph: &Graph, settings: &Settings, package: usize) -> Extraction {
    let mut resolver = Resolver::new(graph, settings);
    let info = &graph.packages[package];
    let mut offer = Offer {
        package: info.name.clone(),
        version: info.version.clone(),
        source: info.source.clone(),
        messages: Vec::new(),
        exports: Vec::new(),
        nodes: Vec::new(),
    };
    let mut problems = Vec::new();
    let Some(krate) = resolver.krate(package) else {
        return Extraction {
            offer,
            problems: vec![format!("package `{}` has no library", info.name)],
        };
    };

    for module in 0..resolver.crates[krate].modules.len() {
        // Messages: types that derive `Message`.
        for index in 0..resolver.crates[krate].modules[module].types.len() {
            if let Some(origin) = message(&mut resolver, Def::Type(krate, module, index)) {
                let wire = wire(&mut resolver, krate, module, index);
                offer.messages.push(MessageEntry {
                    name: resolver.crates[krate].modules[module].types[index]
                        .name
                        .clone(),
                    path: origin.path,
                    wire,
                });
            }
        }
        // Exports: `pub use` items marked `#[gdl_message::export]`.
        let uses: Vec<(Option<String>, Vec<String>, bool, Location)> = resolver.crates[krate]
            .modules[module]
            .uses
            .iter()
            .filter(|u| {
                marked(
                    &u.attrs,
                    "export",
                    &["gdl_message", "gdl"],
                    &resolver,
                    krate,
                    module,
                )
            })
            .map(|u| (u.name.clone(), u.path.clone(), u.absolute, u.at.clone()))
            .collect();
        for (name, path, absolute, at) in uses {
            let Some(name) = name else {
                problems.push(format!(
                    "{at}: an export names its messages: a glob exports nothing"
                ));
                continue;
            };
            let found = resolver
                .resolve(krate, module, &path, absolute)
                .and_then(|def| message(&mut resolver, def));
            match found {
                Some(origin) => offer.exports.push(ExportEntry {
                    path: public_path(&resolver, krate, module, &name),
                    name,
                    origin,
                }),
                None => problems.push(format!(
                    "{at}: export `{name}`: `{}` names no type that derives `Message`",
                    path.join("::")
                )),
            }
        }
        // Nodes: role trait impls marked `#[gdl::node]`.
        let impls: Vec<usize> = resolver.crates[krate].modules[module]
            .impls
            .iter()
            .enumerate()
            .filter(|(_, i)| marked(&i.attrs, "node", &["gdl"], &resolver, krate, module))
            .map(|(index, _)| index)
            .collect();
        for index in impls {
            match node(&mut resolver, krate, module, index) {
                Ok(entry) => offer.nodes.push(entry),
                Err(problem) => problems.push(problem),
            }
        }
    }
    offer.messages.sort_by(|a, b| a.name.cmp(&b.name));
    offer.exports.sort_by(|a, b| a.name.cmp(&b.name));
    offer.nodes.sort_by(|a, b| a.name.cmp(&b.name));
    let mut all = resolver.problems;
    all.extend(problems);
    Extraction {
        offer,
        problems: all,
    }
}

/// The origin of the message `def` names, if it is a type that derives
/// `Message`.
fn message(resolver: &mut Resolver<'_>, def: Def) -> Option<Origin> {
    let Def::Type(krate, module, index) = def else {
        return None;
    };
    let ty = &resolver.crates[krate].modules[module].types[index];
    if !derives(ty, "Message") {
        return None;
    }
    let package = &resolver.graph.packages[resolver.crates[krate].package];
    Some(Origin {
        package: package.name.clone(),
        version: package.version.clone(),
        source: package.source.clone(),
        path: public_path(resolver, krate, module, &ty.name),
    })
}

/// `<crate>::<module path>::<name>`.
fn public_path(resolver: &Resolver<'_>, krate: usize, module: usize, name: &str) -> String {
    let krate = &resolver.crates[krate];
    std::iter::once(krate.name.as_str())
        .chain(krate.modules[module].path.iter().map(String::as_str))
        .chain(std::iter::once(name))
        .collect::<Vec<_>>()
        .join("::")
}

/// Whether a type derives a trait of this last name. The derive is matched
/// by name, not resolved: a derive macro's path leads into a proc-macro
/// crate, which the reader does not follow.
fn derives(ty: &TypeDef, name: &str) -> bool {
    ty.attrs.iter().any(|meta| {
        let Meta::List(list) = meta else {
            return false;
        };
        list.path.is_ident("derive")
            && list
                .parse_args_with(Punctuated::<syn::Path, Token![,]>::parse_terminated)
                .is_ok_and(|paths| {
                    paths
                        .iter()
                        .any(|p| p.segments.last().is_some_and(|s| s.ident == name))
                })
    })
}

/// Whether a type is a wire type: it derives `Serialize` and `Deserialize`,
/// or its crate implements them for it.
fn wire(resolver: &mut Resolver<'_>, krate: usize, module: usize, index: usize) -> bool {
    let ty = &resolver.crates[krate].modules[module].types[index];
    let derived = |name| derives(ty, name);
    let (mut serialize, mut deserialize) = (derived("Serialize"), derived("Deserialize"));
    if serialize && deserialize {
        return true;
    }
    let target = Def::Type(krate, module, index);
    for m in 0..resolver.crates[krate].modules.len() {
        let impls: Vec<(String, syn::Type)> = resolver.crates[krate].modules[m]
            .impls
            .iter()
            .filter_map(|i| {
                let last = i.trait_path.as_ref()?.segments.last()?.ident.to_string();
                Some((last, i.self_ty.clone()))
            })
            .collect();
        for (trait_name, self_ty) in impls {
            if trait_name != "Serialize" && trait_name != "Deserialize" {
                continue;
            }
            let syn::Type::Path(path) = &self_ty else {
                continue;
            };
            let (path, absolute) = segments(&path.path);
            if resolver.resolve(krate, m, &path, absolute) == Some(target) {
                serialize |= trait_name == "Serialize";
                deserialize |= trait_name == "Deserialize";
            }
        }
    }
    serialize && deserialize
}

/// Whether `attrs` hold the marker `name`: written `<krate>::<name>` for
/// one of `krates`, or `<name>` where a `use` of the module imports it from
/// one of them.
fn marked(
    attrs: &[Meta],
    name: &str,
    krates: &[&str],
    resolver: &Resolver<'_>,
    krate: usize,
    module: usize,
) -> bool {
    attrs.iter().any(|meta| {
        let (path, _) = segments(meta.path());
        match path.as_slice() {
            [k, n] => n == name && krates.contains(&k.as_str()),
            [n] if n == name => resolver.crates[krate].modules[module].uses.iter().any(|u| {
                u.name.as_deref() == Some(name)
                    && u.path.len() == 2
                    && krates.contains(&u.path[0].as_str())
                    && u.path[1] == name
            }),
            _ => false,
        }
    })
}

/// The node entry of impl `index` of a module.
fn node(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    index: usize,
) -> Result<NodeEntry, String> {
    let item = &resolver.crates[krate].modules[module].impls[index];
    let at = item.at.clone();
    let role_name = item
        .trait_path
        .as_ref()
        .and_then(|p| p.segments.last())
        .map(|s| s.ident.to_string())
        .unwrap_or_default();
    let (role, ports): (&str, &[&str]) = match role_name.as_str() {
        "Sink" => ("sink", &["Req", "Item"]),
        "Forward" => ("forward", &["In", "Out"]),
        "Backward" => ("backward", &["In", "Out"]),
        "Operator" => ("operator", &["UpReq", "UpItem", "DownReq", "DownItem"]),
        _ => {
            return Err(format!(
                "{at}: `#[gdl::node]` goes on an impl of `Sink`, `Forward`, `Backward`, or `Operator`"
            ));
        }
    };
    let syn::Type::Path(self_ty) = &item.self_ty else {
        return Err(format!("{at}: a node's type is a named type"));
    };
    let (self_path, absolute) = segments(&self_ty.path);
    let types = item.types.clone();
    let Some(Def::Type(c, m, i)) = resolver.resolve(krate, module, &self_path, absolute) else {
        return Err(format!(
            "{at}: the node type `{}` is not a struct, enum, or union this crate defines",
            self_path.join("::")
        ));
    };
    if c != krate {
        return Err(format!(
            "{at}: the node type `{}` is defined in another crate",
            self_path.join("::")
        ));
    }
    let name = resolver.crates[c].modules[m].types[i].name.clone();
    let ty = format!("{}::{name}", resolver.crates[krate].name);
    // The generated segments name the type at the crate root.
    if resolver.member(krate, 0, &name, 0) != Some(Def::Type(c, m, i)) {
        return Err(format!(
            "{at}: node `{name}`: the crate root does not reach it as `{ty}`"
        ));
    }
    let mut entries = Vec::with_capacity(ports.len());
    for &port in ports {
        let Some((_, port_ty, port_at)) = types.iter().find(|(n, _, _)| n == port) else {
            return Err(format!(
                "{at}: node `{name}`: the impl has no `type {port}`"
            ));
        };
        let (head, item) = match streamed(port_ty) {
            Some((head, item)) => (head, Some(item)),
            None => (port_ty, None),
        };
        let unresolved = |ty: &syn::Type| {
            format!(
                "{port_at}: node `{name}`: port `{port}`: `{}` names no type that derives `Message`",
                ty.to_token_stream()
            )
        };
        let head = match head {
            syn::Type::Tuple(t) if t.elems.is_empty() => None,
            ty => Some(message_type(resolver, krate, module, ty).ok_or_else(|| unresolved(ty))?),
        };
        let item = match item {
            None => None,
            Some(ty) => {
                Some(message_type(resolver, krate, module, ty).ok_or_else(|| unresolved(ty))?)
            }
        };
        entries.push(PortEntry {
            port: port.to_string(),
            head,
            item,
        });
    }
    Ok(NodeEntry {
        name,
        role: role.to_string(),
        ty,
        ports: entries,
    })
}

/// A streamed request's head and item: `Streamed<H, T>`.
fn streamed(ty: &syn::Type) -> Option<(&syn::Type, &syn::Type)> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let last = path.path.segments.last()?;
    if last.ident != "Streamed" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    let types: Vec<&syn::Type> = args
        .args
        .iter()
        .filter_map(|a| match a {
            syn::GenericArgument::Type(t) => Some(t),
            _ => None,
        })
        .collect();
    match types.as_slice() {
        [head, item] => Some((head, item)),
        _ => None,
    }
}

/// The origin of the message a port type names.
fn message_type(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    ty: &syn::Type,
) -> Option<Origin> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    if path.qself.is_some() {
        return None;
    }
    let (path, absolute) = segments(&path.path);
    let def = resolver.resolve(krate, module, &path, absolute)?;
    message(resolver, def)
}
