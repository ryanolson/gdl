//! The three markers, found in a crate's modules, and the entries they make.

use std::collections::HashMap;
use std::rc::Rc;

use quote::ToTokens as _;
use syn::punctuated::Punctuated;
use syn::{Meta, Token};

use gdl_model::Role;

use crate::cfg::Settings;
use crate::metadata::Graph;
use crate::resolve::{Def, Resolver, segments};
use crate::tree::{Location, TypeDef, UsePath, Vis};
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
    let mut impls = Impls::default();
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
                let wire = wire(&mut resolver, &mut impls, krate, module, index);
                let ty = &resolver.crates[krate].modules[module].types[index];
                offer.messages.push(MessageEntry {
                    name: ty.name.clone(),
                    path: origin.path,
                    wire,
                    public: ty.vis == Vis::Public && public(&resolver, krate, module),
                });
            }
        }
        // Exports: `pub use` items marked `#[gdl_message::export]`.
        let reachable = public(&resolver, krate, module);
        // One answer per `use` item, whose names share its attributes:
        // deciding per name costs names times attributes.
        let mut decided: HashMap<*const [Meta], bool> = HashMap::new();
        let uses: Vec<ExportUse> = resolver.crates[krate].modules[module]
            .uses
            .iter()
            .filter(|u| {
                *decided.entry(Rc::as_ptr(&u.attrs)).or_insert_with(|| {
                    marked(
                        &u.attrs,
                        "export",
                        &["gdl_message", "gdl"],
                        &resolver,
                        krate,
                        module,
                    )
                })
            })
            .map(|u| {
                let public = u.vis == Vis::Public && reachable;
                (
                    u.name.clone(),
                    u.path.clone(),
                    u.absolute,
                    u.at.clone(),
                    public,
                )
            })
            .collect();
        for (name, path, absolute, at, public) in uses {
            let Some(name) = name else {
                problems.push(format!(
                    "{at}: an export names its messages: a glob exports nothing"
                ));
                continue;
            };
            let found = resolver
                .resolve(krate, module, path.segments(), absolute)
                .and_then(|def| Some((message(&mut resolver, def)?, def)));
            match found {
                Some((origin, Def::Type(c, m, i))) => offer.exports.push(ExportEntry {
                    path: public_path(&resolver, krate, module, &name),
                    name,
                    origin,
                    wire: wire(&mut resolver, &mut impls, c, m, i),
                    public,
                }),
                Some(_) => unreachable!("a message is a type"),
                None => problems.push(format!(
                    "{at}: export `{name}`: `{}` names no type that derives `Message`",
                    path.segments().join("::")
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
            match node(&mut resolver, &mut impls, krate, module, index) {
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

/// A `use` an export marks: the name it binds, its path, whether that is
/// absolute, where it is, and whether the name is public.
type ExportUse = (Option<String>, UsePath, bool, Location, bool);

/// Whether code outside the crate can name module `module`: it and every
/// module above it are `pub`.
fn public(resolver: &Resolver<'_>, krate: usize, module: usize) -> bool {
    let modules = &resolver.crates[krate].modules;
    let mut at = Some(module);
    while let Some(m) = at {
        if m != 0 && modules[m].vis != Vis::Public {
            return false;
        }
        at = modules[m].parent;
    }
    true
}

/// `<crate>::<module path>::<name>`.
fn public_path(resolver: &Resolver<'_>, krate: usize, module: usize, name: &str) -> String {
    let krate = &resolver.crates[krate];
    let mut names = vec![name];
    let mut at = module;
    while let Some(parent) = krate.modules[at].parent {
        names.push(&krate.modules[at].name);
        at = parent;
    }
    names.push(&krate.name);
    names.reverse();
    names.join("::")
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
fn wire(
    resolver: &mut Resolver<'_>,
    impls: &mut Impls,
    krate: usize,
    module: usize,
    index: usize,
) -> bool {
    let ty = &resolver.crates[krate].modules[module].types[index];
    let derived = |name| derives(ty, name);
    let (mut serialize, mut deserialize) =
        (derived("Serialize"), derived("Deserialize") && !ty.borrows);
    if serialize && deserialize {
        return true;
    }
    for found in impls.of(resolver, Traits::Serde, Def::Type(krate, module, index)) {
        serialize |= found.name == TraitName::Serialize;
        deserialize |= found.name == TraitName::Deserialize;
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
            [n] => {
                let m = &resolver.crates[krate].modules[module];
                m.uses_named(n).iter().any(|&i| {
                    matches!(
                        m.uses[i].path.segments().as_slice(),
                        [k, last] if krates.contains(k) && *last == name
                    )
                })
            }
            _ => false,
        }
    })
}

/// The node entry of type `index` of a module, which derives `Node`.
fn node(
    resolver: &mut Resolver<'_>,
    impls: &mut Impls,
    krate: usize,
    module: usize,
    index: usize,
) -> Result<NodeEntry, String> {
    let def = Def::Type(krate, module, index);
    let ty_def = &resolver.crates[krate].modules[module].types[index];
    let (name, at) = (ty_def.name.clone(), ty_def.at.clone());
    let ty = format!("{}::{name}", resolver.crates[krate].name);
    let attrs: Vec<syn::Attribute> = ty_def
        .attrs
        .iter()
        .map(|meta| syn::parse_quote!(#[#meta]))
        .collect();
    let types = gdl_syntax::node_types(&attrs).map_err(|e| format!("{at}: {e}"))?;
    let config = if let Some(path) = &types.config {
        let (path, absolute) = segments(path);
        let Some(Def::Type(c, m, i)) = resolver.resolve(krate, module, &path, absolute) else {
            return Err(format!("{at}: config type cannot be resolved"));
        };
        let definition = &resolver.crates[c].modules[m].types[i];
        if !derives(definition, "Config") {
            return Err(format!("{at}: config type must derive Config"));
        }
        let fields = definition
            .fields
            .as_ref()
            .ok_or_else(|| format!("{at}: config must be a struct"))?;
        gdl_syntax::graph_fields(fields).map_err(|e| format!("{at}: {e}"))?
    } else {
        Vec::new()
    };
    let metrics = if let Some(path) = &types.metrics {
        let (path, absolute) = segments(path);
        let Some(Def::Type(c, m, i)) = resolver.resolve(krate, module, &path, absolute) else {
            return Err(format!("{at}: metrics type cannot be resolved"));
        };
        let definition = &resolver.crates[c].modules[m].types[i];
        if !derives(definition, "Metrics") {
            return Err(format!("{at}: metrics type must derive Metrics"));
        }
        let fields = definition
            .fields
            .as_ref()
            .ok_or_else(|| format!("{at}: metrics must be a struct"))?;
        gdl_syntax::metric_fields(fields).map_err(|e| format!("{at}: {e}"))?
    } else {
        Vec::new()
    };
    // The generated segments name the type at the crate root.
    if resolver.public_member(krate, 0, &name) != Some(def) {
        return Err(format!(
            "{at}: node `{name}`: the crate root does not reach it publicly as `{ty}`"
        ));
    }

    // Its role trait impls: the one that makes it a node.
    let found: Vec<(Role, ImplOf)> = impls
        .of(resolver, Traits::Role, def)
        .iter()
        .filter_map(|found| match found.name {
            TraitName::Role(role) => Some((role, *found)),
            _ => None,
        })
        .collect();
    let (impl_module, role, types) = match found.as_slice() {
        [] => {
            return Err(format!(
                "{at}: node `{name}`: the crate implements no role trait for it"
            ));
        }
        [(role, found)] => (
            found.module,
            *role,
            resolver.crates[krate].modules[found.module].impls[found.index]
                .types
                .clone(),
        ),
        several => {
            let roles: Vec<&str> = several.iter().map(|(role, _)| role.name()).collect();
            return Err(format!(
                "{at}: node `{name}` implements more than one role trait: {}",
                roles.join(", ")
            ));
        }
    };

    let (clients, service_entries) = if let Some((_, ty, _)) =
        types.iter().find(|(n, _, _)| n == "Clients")
    {
        let syn::Type::Path(path) = ty else {
            return Err(format!("{at}: Clients must name a public Endpoints struct"));
        };
        let (path, absolute) = segments(&path.path);
        let Some(Def::Type(c, m, i)) = resolver.resolve(krate, impl_module, &path, absolute) else {
            return Err(format!("{at}: Clients cannot be resolved"));
        };
        let definition = &resolver.crates[c].modules[m].types[i];
        if !derives(definition, "Endpoints") {
            return Err(format!("{at}: Clients must derive Endpoints"));
        }
        let fields = definition
            .fields
            .as_ref()
            .ok_or_else(|| format!("{at}: Clients must be a struct"))?;
        let descriptors = gdl_syntax::endpoints(fields).map_err(|e| format!("{at}: {e}"))?;
        let client_path = public_path(resolver, c, m, &definition.name);
        let mut entries = Vec::new();
        for entry in descriptors {
            if !config.iter().any(|f| {
                f.name == entry.enabled && matches!(f.default, gdl_model::ConfigValue::Bool(_))
            }) {
                return Err(format!(
                    "{at}: endpoint `{}` enabling field `{}` is not a graph boolean",
                    entry.name, entry.enabled
                ));
            }
            let request = message_type(resolver, c, m, &entry.request)
                .ok_or_else(|| format!("{at}: endpoint request must be a Message"))?;
            let response = message_type(resolver, c, m, &entry.response)
                .ok_or_else(|| format!("{at}: endpoint response must be a Message"))?;
            entries.push(crate::EndpointEntry {
                name: entry.name.to_string(),
                path: entry.path,
                enabled: entry.enabled,
                request: PortEntry {
                    port: "Req".into(),
                    head: Some(request),
                    item: None,
                },
                response: PortEntry {
                    port: "Item".into(),
                    head: Some(response),
                    item: None,
                },
            });
        }
        (Some(client_path), entries)
    } else {
        (None, Vec::new())
    };
    let ports = if clients.is_some() {
        &[][..]
    } else {
        role.ports()
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
        clients,
        entries: service_entries,
        config,
        metrics,
        name,
        role,
        ty,
        ports: entries,
    })
}

/// The impls of serde's traits and of the role traits in each crate read,
/// by the type they are for: each crate's impls of one kind are resolved
/// once, since walking them for every type costs types times impls.
#[derive(Default)]
struct Impls(HashMap<(usize, Traits), HashMap<Def, Vec<ImplOf>>>);

impl Impls {
    /// The impls of the traits of `traits` for `def`, in the order of its
    /// crate's modules.
    fn of(&mut self, resolver: &mut Resolver<'_>, traits: Traits, def: Def) -> &[ImplOf] {
        let (Def::Type(krate, ..) | Def::Module(krate, _)) = def;
        let by_type = self
            .0
            .entry((krate, traits))
            .or_insert_with(|| index(resolver, krate, traits));
        by_type.get(&def).map_or(&[], Vec::as_slice)
    }
}

/// Crate `krate`'s impls of the traits of `traits`, by the type they are
/// for. Only those impls are resolved, as before there was an index.
fn index(resolver: &mut Resolver<'_>, krate: usize, traits: Traits) -> HashMap<Def, Vec<ImplOf>> {
    let mut by_type: HashMap<Def, Vec<ImplOf>> = HashMap::new();
    for module in 0..resolver.crates[krate].modules.len() {
        let found: Vec<(usize, TraitName, syn::Type)> = resolver.crates[krate].modules[module]
            .impls
            .iter()
            .enumerate()
            .filter_map(|(index, i)| {
                let name = imported_as(resolver, krate, module, i.trait_path.as_ref()?)?;
                Some((index, traits.name(&name)?, i.self_ty.clone()))
            })
            .collect();
        for (index, name, self_ty) in found {
            let syn::Type::Path(path) = &self_ty else {
                continue;
            };
            let (path, absolute) = segments(&path.path);
            if let Some(target) = resolver.resolve(krate, module, &path, absolute) {
                by_type.entry(target).or_default().push(ImplOf {
                    name,
                    module,
                    index,
                });
            }
        }
    }
    by_type
}

/// The traits an impl index holds: serde's, for the wire check, or the
/// role traits, for a node. Indexed apart, so that each resolves only the
/// impls it did before there was an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Traits {
    Serde,
    Role,
}

impl Traits {
    /// The trait of this kind with this name, if there is one.
    fn name(self, name: &str) -> Option<TraitName> {
        match (self, name) {
            (Self::Serde, "Serialize") => Some(TraitName::Serialize),
            (Self::Serde, "Deserialize") => Some(TraitName::Deserialize),
            (Self::Serde, _) => None,
            (Self::Role, name) => role_of(name).map(TraitName::Role),
        }
    }
}

/// A trait an index holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TraitName {
    Serialize,
    Deserialize,
    Role(Role),
}

/// An impl of a trait for a type: the trait, and the impl's module and its
/// index among the module's impls.
#[derive(Clone, Copy)]
struct ImplOf {
    name: TraitName,
    module: usize,
    index: usize,
}

/// The role whose trait has this name.
fn role_of(name: &str) -> Option<Role> {
    match name {
        "Frontend" | "Service" => Some(Role::Source),
        "Sink" => Some(Role::Sink),
        "Forward" => Some(Role::Forward),
        "Backward" => Some(Role::Backward),
        "Operator" => Some(Role::Operator),
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
    let m = &resolver.crates[krate].modules[module];
    let renamed = m
        .uses_named(&last)
        .first()
        .and_then(|&i| m.uses[i].path.last())
        .map(str::to_string);
    Some(renamed.unwrap_or(last))
}
