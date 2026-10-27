//! The three markers, found in a crate's modules, and the entries they make.
//! An exported `pub type` that names an instantiation of a generic message
//! or node makes an export or a node of the alias's name.

use std::collections::HashMap;
use std::rc::Rc;

use quote::ToTokens as _;
use syn::Meta;
use syn::ext::IdentExt as _;

use gdl_model::Role;

use crate::canon::{Env, Ty, canonical};
use crate::cfg::Settings;
use crate::external::{Item, derives, gdls, marks, named};
use crate::impls::{ImplOf, Impls, Traits, matching};
use crate::metadata::Graph;
use crate::reads::Counted;
use crate::resolve::{Def, Resolver, segments};
use crate::tree::{Location, UsePath, Vis};
use crate::wire::{plain_wire, ty_wire};
use crate::{
    Argument, ExportEntry, Extraction, MessageEntry, NodeEntry, Offer, Origin, PortEntry, Skipped,
};

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
        policies: Vec::new(),
        skipped: Vec::new(),
        skipped_policies: Vec::new(),
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
                let wire = plain_wire(&mut resolver, &mut impls, Def::Type(krate, module, index));
                let ty = &resolver.crates[krate].modules[module].types[index];
                let encoding = match gdl_syntax::message_encoding(&ty.attrs) {
                    Ok(encoding) => encoding,
                    Err(error) => {
                        problems.push(format!("{}: {error}", ty.at));
                        continue;
                    }
                };
                offer.messages.push(MessageEntry {
                    name: ty.name.clone(),
                    path: origin.path,
                    wire,
                    encoding,
                    public: ty.vis == Vis::Public && public(&resolver, krate, module),
                });
            }
        }
        // Exports: `pub use` items marked `#[gdl_message::export]`.
        let reachable = public(&resolver, krate, module);
        // One answer per `use` item, whose names share its attributes:
        // deciding per name costs names times attributes.
        let mut decided: Counted<HashMap<*const [Meta], bool>> = Default::default();
        let uses: Vec<ExportUse> = resolver.crates[krate].modules[module]
            .uses
            .iter()
            .filter(|u| {
                *decided
                    .entry(Rc::as_ptr(&u.attrs))
                    .or_insert_with(|| exported(&u.attrs, &resolver, krate, module))
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
            let found = match resolver.resolve(krate, module, path.segments(), absolute) {
                Some(def) => ty_of(&mut resolver, def).and_then(|ty| match ty {
                    Some(ty) => Ok(offered(&mut resolver, &ty)?.map(|message| (message, ty))),
                    None => Ok(None),
                }),
                None => Ok(None),
            };
            match found {
                Ok(Some((message, ty))) => offer.exports.push(ExportEntry {
                    path: public_path(&resolver, krate, module, &name),
                    name,
                    origin: message.origin,
                    wire: match &ty {
                        Ty::Def(def, args) if args.is_empty() => {
                            plain_wire(&mut resolver, &mut impls, *def)
                        }
                        _ => ty_wire(&mut resolver, &mut impls, &ty),
                    },
                    encoding: message.encoding,
                    public,
                }),
                Ok(None) => {
                    let generic = matches!(
                        resolver.resolve(krate, module, path.segments(), absolute),
                        Some(Def::Type(c, m, i))
                            if !resolver.crates[c].modules[m].types[i].params.is_empty()
                    );
                    let what = if generic {
                        "names a generic type: export an instantiation of it with `#[export] pub type`"
                    } else {
                        "names no type that derives `Message`"
                    };
                    problems.push(format!(
                        "{at}: export `{name}`: `{}` {what}",
                        path.segments().join("::")
                    ));
                }
                Err(problem) => problems.push(format!("{at}: export `{name}`: {problem}")),
            }
        }
        // Aliases marked `#[export]`: an instantiation, under the alias's name.
        for index in 0..resolver.crates[krate].modules[module].types.len() {
            let ty = &resolver.crates[krate].modules[module].types[index];
            if ty.alias.is_none() || !exported(&ty.attrs, &resolver, krate, module) {
                continue;
            }
            match alias(&mut resolver, &mut impls, krate, module, index, reachable) {
                Ok(Aliased::Message(entry)) => offer.exports.push(entry),
                Ok(Aliased::Node(entry)) => offer.nodes.push(entry),
                Ok(Aliased::Skipped(skipped)) => offer.skipped.push(skipped),
                Err(problem) => problems.push(problem),
            }
        }
    }
    // Nodes: types that derive `Node`, each with its role impl, wherever
    // in the crate that is.
    for module in 0..resolver.crates[krate].modules.len() {
        for index in 0..resolver.crates[krate].modules[module].types.len() {
            let ty = &resolver.crates[krate].modules[module].types[index];
            // A generic node is offered by the aliases of its
            // instantiations.
            if !derives(ty, "Node") || !ty.params.is_empty() || ty.other_params {
                continue;
            }
            let (name, at) = (ty.name.clone(), ty.at.clone());
            let def = Def::Type(krate, module, index);
            match node(&mut resolver, &mut impls, def, Vec::new(), (name, at, def)) {
                Ok(entry) => offer.nodes.push(entry),
                Err(problem) => problems.push(problem),
            }
        }
    }
    let (policies, left_out) = crate::policies::policies(&mut resolver, krate);
    offer.policies = policies;
    offer.skipped_policies = left_out;
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
    // A generic message is offered by the aliases of its instantiations.
    if !derives(ty, "Message") || !ty.params.is_empty() || ty.other_params {
        return None;
    }
    let package = &resolver.graph.packages[resolver.crates[krate].package];
    Some(Origin {
        package: package.name.clone(),
        version: package.version.clone(),
        source: package.source.clone(),
        path: public_path(resolver, krate, module, &ty.name),
        arguments: Vec::new(),
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
pub(crate) fn public_path(
    resolver: &Resolver<'_>,
    krate: usize,
    module: usize,
    name: &str,
) -> String {
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

/// Whether `attrs`, written in module `module` of crate `krate`, hold the
/// export marker ([`marks`]).
fn exported(attrs: &[Meta], resolver: &Resolver<'_>, krate: usize, module: usize) -> bool {
    attrs
        .iter()
        .any(|meta| marks(resolver, krate, module, meta.path(), Item::Export))
}

/// A node's name, where its name is written, and the type the crate root
/// must reach publicly under that name: the node's type, or an alias of
/// an instantiation of it.
type Named = (String, Location, Def);

/// The node entry of `def`, a type that derives `Node`, with `args` for its
/// type parameters, named `named`.
fn node(
    resolver: &mut Resolver<'_>,
    impls: &mut Impls,
    def: Def,
    args: Vec<Ty>,
    (name, at, named): Named,
) -> Result<NodeEntry, String> {
    let Def::Type(krate, module, index) = def else {
        unreachable!("a node is a type");
    };
    let ty_def = &resolver.crates[krate].modules[module].types[index];
    let (Def::Type(named_krate, ..) | Def::Module(named_krate, _) | Def::Trait(named_krate, ..)) =
        named
    else {
        unreachable!("a node is named by its type or its alias");
    };
    let ty = format!("{}::{name}", resolver.crates[named_krate].name);
    let attrs: Vec<syn::Attribute> = ty_def
        .attrs
        .iter()
        .map(|meta| syn::parse_quote!(#[#meta]))
        .collect();
    let types = gdl_syntax::node_types(&attrs).map_err(|e| format!("{at}: {e}"))?;
    // The types the derive names, with its parameters standing for this
    // instantiation's arguments: `config = C` names each alias's own.
    let env: Env = ty_def
        .params
        .iter()
        .cloned()
        .zip(args.iter().cloned())
        .collect();
    let path_type = |path: &syn::Path| {
        syn::Type::Path(syn::TypePath {
            qself: None,
            path: path.clone(),
        })
    };
    let config = match &types.config {
        Some(path) => {
            let ty = path_type(path);
            let (_, fields) =
                derived_struct(resolver, krate, module, &ty, &env, Derived::Config, &at)?;
            gdl_syntax::graph_fields(&fields).map_err(|e| format!("{at}: {e}"))?
        }
        None => Vec::new(),
    };
    let metrics = match &types.metrics {
        Some(path) => {
            let ty = path_type(path);
            let (_, fields) =
                derived_struct(resolver, krate, module, &ty, &env, Derived::Metrics, &at)?;
            gdl_syntax::metric_fields(&fields).map_err(|e| format!("{at}: {e}"))?
        }
        None => Vec::new(),
    };
    // The generated segments name the type at the crate root.
    if resolver.public_member(named_krate, 0, &name) != Some(named) {
        return Err(format!(
            "{at}: node `{name}`: the crate root does not reach it publicly as `{ty}`"
        ));
    }

    // Its role trait impls: the one that makes it a node, of those for
    // this instantiation, with the impl's parameters bound by it.
    let candidates: Vec<(Role, ImplOf)> = impls
        .of(resolver, Traits::Role, def)
        .iter()
        .filter_map(|found| match found.item {
            Item::Role(role) => Some((role, *found)),
            _ => None,
        })
        .collect();
    let target = Ty::Def(def, args);
    let mut found: Vec<(Role, ImplOf, Env)> = Vec::new();
    for (role, of) in candidates {
        if let Some(env) = matching(resolver, krate, of, &target) {
            found.push((role, of, env));
        }
    }
    let (impl_module, role, types, env) = match found.as_slice() {
        [] => {
            return Err(format!(
                "{at}: node `{name}`: the crate implements no role trait for it"
            ));
        }
        [(role, found, env)] => (
            found.module,
            *role,
            resolver.crates[krate].modules[found.module].impls[found.index]
                .types
                .clone(),
            env.clone(),
        ),
        several => {
            let roles: Vec<&str> = several.iter().map(|(role, ..)| role.name()).collect();
            return Err(format!(
                "{at}: node `{name}` implements more than one role trait: {}",
                roles.join(", ")
            ));
        }
    };

    let (clients, service_entries) = match types.iter().find(|(n, _, _)| n == "Clients") {
        // With the impl's parameters bound by the instantiation: `type
        // Clients = C` names each alias's own.
        Some((_, ty, _)) => service(resolver, (krate, impl_module, &env), ty, &config, &at)?,
        None => (None, Vec::new()),
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
        let unresolved = |ty: &syn::Type| {
            format!(
                "{port_at}: node `{name}`: port `{port}`: `{}` names no type that derives `Message`",
                ty.to_token_stream()
            )
        };
        // A type the reader cannot read is a problem with the reason
        // `canonical` gives, which names the part it cannot read.
        let unread =
            |problem: String| format!("{port_at}: node `{name}`: port `{port}`: {problem}");
        let read = |resolver: &mut Resolver<'_>, ty: &syn::Type| {
            canonical(resolver, krate, impl_module, ty, &env).map_err(unread)
        };
        // The head and the item, with the impl's parameters bound, each
        // with the syntax a problem names it by. gdl's `Streamed`, written
        // at the port or bound to a parameter it names, is told by its
        // definition, so that a crate's own type of that name is not taken
        // for it; by the name it is imported under only where the reader
        // cannot resolve the port's type, as when it does not read gdl.
        let (head, item) = match (
            canonical(resolver, krate, impl_module, port_ty, &env),
            streamed(resolver, krate, impl_module, port_ty),
        ) {
            (Ok(ty), _) => match streamed_ty(resolver, &ty) {
                Some((head, item)) => ((head, port_ty), Some((item, port_ty))),
                None => ((ty, port_ty), None),
            },
            (Err(_), Some((head, item))) => (
                (read(resolver, head)?, head),
                Some((read(resolver, item)?, item)),
            ),
            (Err(problem), None) => return Err(unread(problem)),
        };
        let message = |resolver: &mut Resolver<'_>, (ty, written): (Ty, &syn::Type)| {
            offered(resolver, &ty)
                .ok()
                .flatten()
                .map(|o| o.origin)
                .ok_or_else(|| unresolved(written))
        };
        // `()` is no message, whether written at the port or bound to the
        // parameter the port names.
        let head = match head {
            (Ty::Unit, _) => None,
            head => Some(message(resolver, head)?),
        };
        let item = match item {
            None => None,
            Some(item) => Some(message(resolver, item)?),
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

/// A service's client bundle, its `type Clients` written in module
/// `module` of crate `krate` with the parameters `env` binds, and its
/// typed entries: each must be enabled by a graph boolean of `config`.
fn service(
    resolver: &mut Resolver<'_>,
    (krate, module, env): (usize, usize, &Env),
    ty: &syn::Type,
    config: &[gdl_model::ConfigField],
    at: &Location,
) -> Result<(Option<String>, Vec<crate::EndpointEntry>), String> {
    let syn::Type::Path(_) = ty else {
        return Err(format!("{at}: Clients must name a public Endpoints struct"));
    };
    let (Def::Type(c, m, i), fields) =
        derived_struct(resolver, krate, module, ty, env, Derived::Clients, at)?
    else {
        unreachable!("a struct is a type");
    };
    let descriptors = gdl_syntax::endpoints(&fields).map_err(|e| format!("{at}: {e}"))?;
    let client_path = public_path(resolver, c, m, &resolver.crates[c].modules[m].types[i].name);
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
        let request = message_type(resolver, c, m, &entry.request, &Env::new())
            .ok_or_else(|| format!("{at}: endpoint request must be a Message"))?;
        let response = message_type(resolver, c, m, &entry.response, &Env::new())
            .ok_or_else(|| format!("{at}: endpoint response must be a Message"))?;
        let port = |port: &str, head: Origin| PortEntry {
            port: port.into(),
            head: Some(head),
            item: None,
        };
        entries.push(crate::EndpointEntry {
            name: entry.name.to_string(),
            path: entry.path,
            enabled: entry.enabled,
            request: port("Req", request),
            response: port("Item", response),
        });
    }
    Ok((Some(client_path), entries))
}

/// A struct a node's derive or impl names, for what its fields declare.
#[derive(Clone, Copy)]
enum Derived {
    /// `#[gdl(config = ..)]`.
    Config,
    /// `#[gdl(metrics = ..)]`.
    Metrics,
    /// A service's `type Clients`.
    Clients,
}

impl Derived {
    /// What a problem calls it, and the derive it must have.
    fn named(self) -> (&'static str, &'static str) {
        match self {
            Self::Config => ("config type", "Config"),
            Self::Metrics => ("metrics type", "Metrics"),
            Self::Clients => ("Clients", "Endpoints"),
        }
    }
}

/// The struct `ty`, written in module `module` with the parameters `env`
/// binds, names, and its fields: it must derive what `kind` needs. A
/// problem, at `at`, otherwise.
///
/// Only the struct's fields are read, so its arguments are not: a
/// parameter left to its default, or an argument the reader cannot name,
/// such as `Arc<u8>`, names the struct all the same. A path is resolved
/// as it is written, and a parameter of a generic node (`config = C`) as
/// the type its alias binds it to.
fn derived_struct(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    ty: &syn::Type,
    env: &Env,
    kind: Derived,
    at: &Location,
) -> Result<(Def, syn::Fields), String> {
    let (what, derive) = kind.named();
    let found = match ty {
        syn::Type::Path(p) if p.qself.is_none() => {
            let parameter = p
                .path
                .get_ident()
                .and_then(|ident| env.get(&ident.unraw().to_string()));
            match parameter {
                Some(Ty::Def(def, _)) => Some(*def),
                Some(_) => None,
                None => {
                    let (path, absolute) = segments(&p.path);
                    resolver.resolve(krate, module, &path, absolute)
                }
            }
        }
        _ => None,
    };
    let Some(def @ Def::Type(c, m, i)) = found else {
        return Err(format!("{at}: {what} cannot be resolved"));
    };
    let definition = &resolver.crates[c].modules[m].types[i];
    // An alias carries no derives, so the derive check below would say
    // that a type which derives the trait does not.
    if definition.alias.is_some() {
        return Err(format!(
            "{at}: {what} `{}` is a type alias, which the reader does not follow: name the type it aliases",
            ty.to_token_stream()
        ));
    }
    if !derives(definition, derive) {
        return Err(format!("{at}: {what} must derive {derive}"));
    }
    let fields = definition
        .fields
        .clone()
        .ok_or_else(|| format!("{at}: {what} must be a struct"))?;
    Ok((def, fields))
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
    if named(resolver, krate, module, &path.path)? != Item::Streamed {
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

/// A streamed request's head and item, when `ty` is gdl's `Streamed`.
fn streamed_ty(resolver: &Resolver<'_>, ty: &Ty) -> Option<(Ty, Ty)> {
    let Ty::Def(def @ Def::Type(..), args) = ty else {
        return None;
    };
    match args.as_slice() {
        [head, item] if gdls(resolver, *def, Item::Streamed) => Some((head.clone(), item.clone())),
        _ => None,
    }
}

/// The origin of the message a port type names, written in a module with
/// the parameters `env` binds: a message, or an instantiation of a generic
/// one, through aliases.
fn message_type(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    ty: &syn::Type,
    env: &Env,
) -> Option<Origin> {
    let ty = canonical(resolver, krate, module, ty, env).ok()?;
    Some(offered(resolver, &ty).ok()??.origin)
}

/// A message a type is. Its wire capability is read where an export
/// offers it, not here: a port, which needs its identity alone, would
/// read the serde attributes of every field of its message.
pub(crate) struct Offered {
    pub(crate) origin: Origin,
    encoding: gdl_model::Encoding,
}

/// The message `ty` is, if it is one: a type that derives `Message`, or an
/// instantiation of a generic one, identified by its generic type's
/// package and by its type.
pub(crate) fn offered(resolver: &mut Resolver<'_>, ty: &Ty) -> Result<Option<Offered>, String> {
    let Ty::Def(def @ Def::Type(c, m, i), args) = ty else {
        return Ok(None);
    };
    let definition = &resolver.crates[*c].modules[*m].types[*i];
    if !derives(definition, "Message") {
        return Ok(None);
    }
    let encoding = gdl_syntax::message_encoding(&definition.attrs)
        .map_err(|error| format!("{}: {error}", definition.at))?;
    let origin = if args.is_empty() {
        let Some(origin) = message(resolver, *def) else {
            return Ok(None);
        };
        origin
    } else {
        let package = &resolver.graph.packages[resolver.crates[*c].package];
        let mut arguments = Vec::new();
        for arg in args {
            defining(resolver, arg, &mut arguments);
        }
        Origin {
            package: package.name.clone(),
            version: package.version.clone(),
            source: package.source.clone(),
            path: ty.render(resolver),
            arguments,
        }
    };
    Ok(Some(Offered { origin, encoding }))
}

/// Push the package that defines `ty`, and each of its arguments, in the
/// order `Ty::render` writes them.
fn defining(resolver: &Resolver<'_>, ty: &Ty, out: &mut Vec<Argument>) {
    let (krate, args) = match ty {
        Ty::Def(Def::Type(krate, ..), args) => (krate, args),
        // The standard library's is no package's, but what it holds is.
        Ty::Std(_, args) => {
            for arg in args {
                defining(resolver, arg, out);
            }
            return;
        }
        _ => return,
    };
    let package = &resolver.graph.packages[resolver.crates[*krate].package];
    out.push(Argument {
        package: package.name.clone(),
        version: package.version.clone(),
        source: package.source.clone(),
    });
    for arg in args {
        defining(resolver, arg, out);
    }
}

/// The type `def` names on its own: a type without parameters, or the
/// type an alias without parameters names; none for a generic type, which
/// names no one type.
fn ty_of(resolver: &mut Resolver<'_>, def: Def) -> Result<Option<Ty>, String> {
    let Def::Type(c, m, i) = def else {
        return Ok(None);
    };
    let definition = &resolver.crates[c].modules[m].types[i];
    if !definition.params.is_empty() || definition.other_params {
        return Ok(None);
    }
    match definition.alias.clone() {
        Some(target) => canonical(resolver, c, m, &target, &Env::new()).map(Some),
        None => Ok(Some(Ty::Def(def, Vec::new()))),
    }
}

/// What an exported alias offers.
enum Aliased {
    Message(ExportEntry),
    Node(NodeEntry),
    /// An instantiation the reader cannot name: left out, with why, and no
    /// problem of the crate's.
    Skipped(Skipped),
}

/// The instantiation that alias `index` of a module names, which is
/// marked `#[export]`, offered under the alias's name. `reachable` is
/// whether code outside the crate can name the module.
fn alias(
    resolver: &mut Resolver<'_>,
    impls: &mut Impls,
    krate: usize,
    module: usize,
    index: usize,
    reachable: bool,
) -> Result<Aliased, String> {
    let alias = &resolver.crates[krate].modules[module].types[index];
    let (name, at, vis) = (alias.name.clone(), alias.at.clone(), alias.vis);
    if !alias.params.is_empty() || alias.other_params {
        return Err(format!(
            "{at}: `{name}`: an alias names one instantiation: it takes no parameters of its own"
        ));
    }
    if vis != Vis::Public {
        return Err(format!(
            "{at}: `{name}`: an export offers a public path: write `pub type`"
        ));
    }
    let target = alias.alias.clone().expect("an alias names a type");
    let ty = match canonical(resolver, krate, module, &target, &Env::new()) {
        Ok(ty) => ty,
        Err(problem) => {
            return Ok(Aliased::Skipped(Skipped {
                name,
                problem: format!("{at}: {problem}"),
            }));
        }
    };
    let none = || {
        format!(
            "{at}: `{name}` names no instantiation of a generic message or node; a message that is not generic is offered under another path with `#[export] pub use`"
        )
    };
    let Ty::Def(def @ Def::Type(c, m, i), args) = &ty else {
        return Err(none());
    };
    if args.is_empty() {
        return Err(none());
    }
    let definition = &resolver.crates[*c].modules[*m].types[*i];
    if derives(definition, "Node") {
        let named = (name, at, Def::Type(krate, module, index));
        return node(resolver, impls, *def, args.clone(), named).map(Aliased::Node);
    }
    let message = offered(resolver, &ty)
        .map_err(|problem| format!("{at}: `{name}`: {problem}"))?
        .ok_or_else(none)?;
    Ok(Aliased::Message(ExportEntry {
        path: public_path(resolver, krate, module, &name),
        name,
        origin: message.origin,
        wire: ty_wire(resolver, impls, &ty),
        encoding: message.encoding,
        public: reachable,
    }))
}
