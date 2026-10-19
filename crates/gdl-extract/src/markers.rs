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
                .and_then(|def| Some((message(&mut resolver, def)?, def)));
            match found {
                Some((origin, Def::Type(c, m, i))) => offer.exports.push(ExportEntry {
                    path: public_path(&resolver, krate, module, &name),
                    name,
                    origin,
                    wire: wire(&mut resolver, c, m, i),
                }),
                Some(_) => unreachable!("a message is a type"),
                None => problems.push(format!(
                    "{at}: export `{name}`: `{}` names no type that derives `Message`",
                    path.join("::")
                )),
            }
        }
    }
    // Nodes: types that derive `Node`, each with its role impl, wherever
    // in the crate that is.
    for module in 0..resolver.crates[krate].modules.len() {
        for index in 0..resolver.crates[krate].modules[module].types.len() {
            if !derives(&resolver.crates[krate].modules[module].types[index], "Node") {
                continue;
            }
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
/// or its crate implements them for it. A derived `Deserialize` of a type
/// that borrows is not `DeserializeOwned`, so it gives no codec.
fn wire(resolver: &mut Resolver<'_>, krate: usize, module: usize, index: usize) -> bool {
    let ty = &resolver.crates[krate].modules[module].types[index];
    let derived = |name| derives(ty, name);
    let (mut serialize, mut deserialize) =
        (derived("Serialize"), derived("Deserialize") && !ty.borrows);
    if serialize && deserialize {
        return true;
    }
    let target = Def::Type(krate, module, index);
    for m in 0..resolver.crates[krate].modules.len() {
        let impls: Vec<(String, syn::Type)> = resolver.crates[krate].modules[m]
            .impls
            .iter()
            .filter_map(|i| {
                let last = imported_as(resolver, krate, m, i.trait_path.as_ref()?)?;
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
/// one of `krates`, or a single name that a `use` of the module binds to
/// `<krate>::<name>`, under that name or another.
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
            [n] => resolver.crates[krate].modules[module].uses.iter().any(|u| {
                u.name.as_deref() == Some(n.as_str())
                    && u.path.len() == 2
                    && krates.contains(&u.path[0].as_str())
                    && u.path[1] == name
            }),
            _ => false,
        }
    })
}

/// The node entry of type `index` of a module, which derives `Node`.
fn node(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    index: usize,
) -> Result<NodeEntry, String> {
    let def = Def::Type(krate, module, index);
    let ty_def = &resolver.crates[krate].modules[module].types[index];
    let (name, at) = (ty_def.name.clone(), ty_def.at.clone());
    let ty = format!("{}::{name}", resolver.crates[krate].name);
    // The generated segments name the type at the crate root.
    if resolver.member(krate, 0, &name, 0) != Some(def) {
        return Err(format!(
            "{at}: node `{name}`: the crate root does not reach it as `{ty}`"
        ));
    }

    // Its role trait impls: the one that makes it a node.
    let mut found = Vec::new();
    for m in 0..resolver.crates[krate].modules.len() {
        let impls: Vec<RoleImpl> = resolver.crates[krate].modules[m]
            .impls
            .iter()
            .filter_map(|i| {
                let role = imported_as(resolver, krate, m, i.trait_path.as_ref()?)?;
                Some((role, i.self_ty.clone(), i.types.clone()))
            })
            .collect();
        for (role, self_ty, types) in impls {
            let Some(role) = role_of(&role) else {
                continue;
            };
            let syn::Type::Path(path) = &self_ty else {
                continue;
            };
            let (path, absolute) = segments(&path.path);
            if resolver.resolve(krate, m, &path, absolute) == Some(def) {
                found.push((m, role, types));
            }
        }
    }
    let (impl_module, (role, ports), types) = match found.as_slice() {
        [] => {
            return Err(format!(
                "{at}: node `{name}`: the crate implements no role trait for it"
            ));
        }
        [(m, role, types)] => (*m, *role, types.clone()),
        several => {
            let roles: Vec<&str> = several.iter().map(|(_, (role, _), _)| *role).collect();
            return Err(format!(
                "{at}: node `{name}` implements more than one role trait: {}",
                roles.join(", ")
            ));
        }
    };

    let mut entries = Vec::with_capacity(ports.len());
    for &port in ports {
        let Some((_, port_ty, port_at)) = types.iter().find(|(n, _, _)| n == port) else {
            return Err(format!(
                "{at}: node `{name}`: its impl has no `type {port}`"
            ));
        };
        let (head, item) = match streamed(resolver, krate, impl_module, port_ty) {
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
            ty => {
                Some(message_type(resolver, krate, impl_module, ty).ok_or_else(|| unresolved(ty))?)
            }
        };
        let item = match item {
            None => None,
            Some(ty) => {
                Some(message_type(resolver, krate, impl_module, ty).ok_or_else(|| unresolved(ty))?)
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

/// A trait impl's trait name, self type, and associated types.
type RoleImpl = (String, syn::Type, Vec<(String, syn::Type, Location)>);

/// A role trait's role and its ports, by the trait's name.
fn role_of(name: &str) -> Option<(&'static str, &'static [&'static str])> {
    match name {
        "Sink" => Some(("sink", &["Req", "Item"])),
        "Forward" => Some(("forward", &["In", "Out"])),
        "Backward" => Some(("backward", &["In", "Out"])),
        "Operator" => Some(("operator", &["UpReq", "UpItem", "DownReq", "DownItem"])),
        _ => None,
    }
}

/// A streamed request's head and item: `Streamed<H, T>`, in module
/// `module`.
fn streamed<'t>(
    resolver: &Resolver<'_>,
    krate: usize,
    module: usize,
    ty: &'t syn::Type,
) -> Option<(&'t syn::Type, &'t syn::Type)> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    if imported_as(resolver, krate, module, &path.path)? != "Streamed" {
        return None;
    }
    let last = path.path.segments.last()?;
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

/// The name a trait or type path ends in, through a rename: `Stage` after
/// `use gdl::Forward as Stage` is `Forward`. Role traits, `Streamed`, and
/// serde's traits are matched by name, as derives are; this follows one
/// `use` of a single-name path in its module, so a rename does not hide
/// them.
fn imported_as(
    resolver: &Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
) -> Option<String> {
    let (segments, absolute) = segments(path);
    let last = segments.last()?.clone();
    if segments.len() > 1 || absolute {
        return Some(last);
    }
    let renamed = resolver.crates[krate].modules[module]
        .uses
        .iter()
        .find(|u| u.name.as_deref() == Some(last.as_str()))
        .and_then(|u| u.path.last().cloned());
    Some(renamed.unwrap_or(last))
}
