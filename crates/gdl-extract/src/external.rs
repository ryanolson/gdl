//! Which item of a crate the reader does not read a path names: serde's
//! traits, the standard library's traits and types, and gdl's export
//! marker, role traits, `Streamed`, and `CellPolicy`. One table holds each
//! item with its name, the crates a path to it may start from, and whether
//! the prelude brings it. One resolver reads a path in its module, through
//! the `use` that binds a single name, and compares it with the table.
//!
//! The checks differ in how much of the path they hold to, so each asks
//! the resolver its own question: [`named`], by the name alone, through a
//! rename; [`known_trait`] and [`standard`], by a path from the item's
//! crate that nothing in the module rebinds, or by the prelude's bare name;
//! [`marks`], by `<crate>::<name>` exactly; [`gdls`], by the definition a
//! path resolves to in gdl; [`derives`], by a derive's last name. A name
//! the reader cannot follow is another's,
//! so the wire check refuses it; a name trick it does not chase is capped,
//! since the segment's crossing check catches it.

use std::collections::HashSet;

use syn::Meta;
use syn::punctuated::Punctuated;

use gdl_model::Role;

use crate::resolve::{Def, Resolver, segments};
use crate::tree::TypeDef;

/// An item of a crate the reader does not read, that it tells by its path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Item {
    /// A trait the reader judges a bound on.
    Trait(Known),
    /// A type of the standard library's that the reader names.
    Std(Std),
    /// gdl's `#[export]`.
    Export,
    /// gdl's `Streamed`.
    Streamed,
    /// gdl's `CellPolicy`.
    CellPolicy,
    /// A role trait of gdl's.
    Role(Role),
}

/// A trait the reader judges a bound on, and the derives it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Known {
    /// serde's `Serialize`.
    Serialize,
    /// serde's `Deserialize`.
    Deserialize,
    /// serde's `DeserializeOwned`.
    DeserializeOwned,
    /// The standard library's `Default`.
    Default,
    /// The standard library's `Sized`.
    Sized,
}

/// The types of the standard library's the reader names and judges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Std {
    String,
    Vec,
    VecDeque,
    Option,
    Box,
    HashMap,
    BTreeMap,
}

impl Std {
    /// Its path, as `core::any::type_name` writes it.
    pub(crate) fn path(self) -> &'static str {
        match self {
            Self::String => "alloc::string::String",
            Self::Vec => "alloc::vec::Vec",
            Self::VecDeque => "alloc::collections::vec_deque::VecDeque",
            Self::Option => "core::option::Option",
            Self::Box => "alloc::boxed::Box",
            Self::HashMap => "std::collections::hash::map::HashMap",
            Self::BTreeMap => "alloc::collections::btree::map::BTreeMap",
        }
    }

    /// The arguments it takes, without a default's.
    pub(crate) fn arity(self) -> usize {
        match self {
            Self::String => 0,
            Self::Vec | Self::VecDeque | Self::Option | Self::Box => 1,
            Self::HashMap | Self::BTreeMap => 2,
        }
    }
}

/// An item of the table: its name, the crates a path to it may start
/// from, and whether the prelude brings it by its name.
struct Row {
    item: Item,
    name: &'static str,
    crates: &'static [&'static str],
    prelude: bool,
}

/// The crates whose `Serialize` and `Deserialize` are serde's: serde, and
/// its derive's.
const SERDE: &[&str] = &["serde", "serde_derive"];

/// The crates of the standard library a path to `Default` or `Sized` may
/// begin with.
const STD_TRAITS: &[&str] = &["std", "core"];

/// The crates of the standard library a path to one of its types may
/// begin with.
const STD_TYPES: &[&str] = &["std", "alloc", "core"];

/// The crates the export marker is written from.
const MARKER: &[&str] = &["gdl_message", "gdl"];

const GDL: &[&str] = &["gdl"];

const fn row(item: Item, name: &'static str, crates: &'static [&'static str]) -> Row {
    Row {
        item,
        name,
        crates,
        prelude: false,
    }
}

const fn prelude(item: Item, name: &'static str, crates: &'static [&'static str]) -> Row {
    Row {
        item,
        name,
        crates,
        prelude: true,
    }
}

/// Every item the reader tells by its path. A name is in it once.
const TABLE: &[Row] = &[
    row(Item::Trait(Known::Serialize), "Serialize", SERDE),
    row(Item::Trait(Known::Deserialize), "Deserialize", SERDE),
    row(
        Item::Trait(Known::DeserializeOwned),
        "DeserializeOwned",
        SERDE,
    ),
    prelude(Item::Trait(Known::Default), "Default", STD_TRAITS),
    prelude(Item::Trait(Known::Sized), "Sized", STD_TRAITS),
    prelude(Item::Std(Std::String), "String", STD_TYPES),
    prelude(Item::Std(Std::Vec), "Vec", STD_TYPES),
    row(Item::Std(Std::VecDeque), "VecDeque", STD_TYPES),
    prelude(Item::Std(Std::Option), "Option", STD_TYPES),
    prelude(Item::Std(Std::Box), "Box", STD_TYPES),
    row(Item::Std(Std::HashMap), "HashMap", STD_TYPES),
    row(Item::Std(Std::BTreeMap), "BTreeMap", STD_TYPES),
    row(Item::Export, "export", MARKER),
    row(Item::Streamed, "Streamed", GDL),
    row(Item::CellPolicy, "CellPolicy", GDL),
    row(Item::Role(Role::Source), "Frontend", GDL),
    row(Item::Role(Role::Source), "Service", GDL),
    row(Item::Role(Role::Sink), "Sink", GDL),
    row(Item::Role(Role::Forward), "Forward", GDL),
    row(Item::Role(Role::Backward), "Backward", GDL),
    row(Item::Role(Role::Operator), "Operator", GDL),
];

/// The row of this name.
fn row_named(name: &str) -> Option<&'static Row> {
    TABLE.iter().find(|row| row.name == name)
}

/// The row of `item`, of an item that one name names.
fn row_of(item: Item) -> Option<&'static Row> {
    TABLE.iter().find(|row| row.item == item)
}

/// The row of the name `path` ends in, written in module `module` of crate
/// `krate`, through a rename: `Stage` after `use gdl::Forward as Stage` is
/// `Forward`. It follows the first `use` of a single-name path in its
/// module, so a rename does not hide a name.
fn row_written(
    resolver: &Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
) -> Option<&'static Row> {
    let (segments, absolute) = segments(path);
    let last = segments.last()?;
    if segments.len() > 1 || absolute {
        return row_named(last);
    }
    let m = &resolver.crates[krate].modules[module];
    let renamed = m
        .uses_named(last)
        .first()
        .and_then(|&i| m.uses[i].path.last());
    row_named(renamed.unwrap_or(last))
}

/// The item the name `path` ends in names, written in module `module` of
/// crate `krate`, through a rename, by that name alone. Role traits, a
/// port's `Streamed` whose path cannot be resolved, and the serde traits
/// of a message that is not generic are matched so; serde's traits,
/// `Default`, and `Sized` elsewhere start from it and are then told by
/// their paths ([`known_trait`]).
pub(crate) fn named(
    resolver: &Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
) -> Option<Item> {
    row_written(resolver, krate, module, path).map(|row| row.item)
}

/// Which trait the reader judges `path`, written in module `module` of
/// crate `krate`, names, as a bound, an impl's trait, or a derive: serde's
/// where the path leads to serde, the standard library's where it leads
/// there or is the prelude's name and nothing in scope binds it. None for
/// any other, and for one the reader cannot follow. What impls, bounds,
/// and derives judge all goes through this one test.
pub(crate) fn known_trait(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
) -> Option<Known> {
    told(resolver, krate, module, path, |item| match item {
        Item::Trait(known) => Some(known),
        _ => None,
    })
}

/// The type of the standard library's that `path`, written in module
/// `module` of crate `krate`, names, if the reader knows it: by its name
/// alone where the prelude brings it and nothing in the module binds the
/// name, or by a path to it from `std`, `alloc`, or `core`, or a `use` of
/// one.
pub(crate) fn standard(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
) -> Option<Std> {
    told(resolver, krate, module, path, |item| match item {
        Item::Std(std) => Some(std),
        _ => None,
    })
}

/// What `pick` takes of the item `path` names, where the path leads to it
/// ([`leads_to`]).
fn told<T>(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
    pick: impl FnOnce(Item) -> Option<T>,
) -> Option<T> {
    let row = row_written(resolver, krate, module, path)?;
    let picked = pick(row.item)?;
    leads_to(resolver, krate, module, path, row).then_some(picked)
}

/// Whether `path`, written in module `module` of crate `krate`, leads to
/// the item of `row`: it is the prelude's bare name where the prelude
/// brings the item and nothing in the module binds the name, or it is a
/// path to it from one of its crates ([`rooted`]). A type's bare name may
/// carry its arguments, `Vec<T>`; a trait's is its name alone.
fn leads_to(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
    row: &Row,
) -> bool {
    let bare = path.leading_colon.is_none()
        && path.segments.len() == 1
        && path.segments[0].ident == row.name
        && (matches!(row.item, Item::Std(_)) || path.segments[0].arguments.is_none());
    let prelude = row.prelude && bare && binds(resolver, krate, module, row.name) == Some(false);
    prelude || rooted(resolver, krate, module, path, row)
}

/// Whether `path`, written in module `module` of crate `krate`, is a path
/// to the item of `row` from one of its crates' roots, such as
/// `serde::de::Deserialize`, or a single name that a `use` of the module
/// binds to one, under that name or another. The path's first name is the
/// crate's only where nothing in the module binds it, such as a local
/// `mod serde`, or where it is written `::serde`: a name the reader cannot
/// tell is another's.
fn rooted(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
    row: &Row,
) -> bool {
    let fits = |s: &Spelled<'_>| s.len > 1 && row.crates.contains(&s.first) && s.last == row.name;
    let (written, absolute) = segments(path);
    let [single] = written.as_slice() else {
        return Spelled::of(&written, absolute).is_some_and(|s| {
            fits(&s) && (s.absolute || binds(resolver, krate, module, s.first) == Some(false))
        });
    };
    // One `use` at a time, in order: telling what the module binds may
    // read another crate.
    let uses = resolver.crates[krate].modules[module]
        .uses_named(single)
        .to_vec();
    uses.into_iter().any(|i| {
        let u = &resolver.crates[krate].modules[module].uses[i];
        let segments = u.path.segments();
        let first = match Spelled::of(&segments, u.absolute) {
            Some(s) if fits(&s) => (!s.absolute).then(|| s.first.to_string()),
            _ => return false,
        };
        first.is_none_or(|first| binds(resolver, krate, module, &first) == Some(false))
    })
}

/// Whether `path`, an attribute's path written in module `module` of
/// crate `krate`, is the marker of `item`: `<crate>::<name>` for one of the
/// item's crates, or a single name that a `use` of the module binds to
/// that, under that name or another. A marker is told by its spelling
/// alone, whatever the module binds.
pub(crate) fn marks(
    resolver: &Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
    item: Item,
) -> bool {
    let Some(row) = row_of(item) else {
        return false;
    };
    let fits = |s: Spelled<'_>| s.len == 2 && row.crates.contains(&s.first) && s.last == row.name;
    let (written, absolute) = segments(path);
    let [single] = written.as_slice() else {
        return Spelled::of(&written, absolute).is_some_and(fits);
    };
    let m = &resolver.crates[krate].modules[module];
    m.uses_named(single).iter().any(|&i| {
        let u = &m.uses[i];
        Spelled::of(&u.path.segments(), u.absolute).is_some_and(fits)
    })
}

/// What the table needs of a path: its first and last names, how many
/// names it has, and whether it starts with `::`. A path of two or more
/// names stands for itself; a single name, for each path a `use` of its
/// module binds it to.
struct Spelled<'a> {
    first: &'a str,
    last: &'a str,
    len: usize,
    absolute: bool,
}

impl<'a> Spelled<'a> {
    fn of<S: AsRef<str>>(segments: &'a [S], absolute: bool) -> Option<Self> {
        Some(Self {
            first: segments.first()?.as_ref(),
            last: segments.last()?.as_ref(),
            len: segments.len(),
            absolute,
        })
    }
}

/// Whether `def`, a type or a trait, is gdl's `item`: defined in the
/// package gdl under the item's name.
pub(crate) fn gdls(resolver: &Resolver<'_>, def: Def, item: Item) -> bool {
    let (Def::Type(krate, ..) | Def::Trait(krate, ..)) = def else {
        return false;
    };
    let package = &resolver.graph.packages[resolver.crates[krate].package];
    if package.name != "gdl" {
        return false;
    }
    let name = match def {
        Def::Type(c, m, i) => &resolver.crates[c].modules[m].types[i].name,
        Def::Trait(c, m, i) => &resolver.crates[c].modules[m].traits[i].name,
        Def::Module(..) | Def::Opaque => return false,
    };
    row_of(item).is_some_and(|row| row.name == name)
}

/// Whether anything in module `module` of crate `krate` binds `name`,
/// beside the prelude and the crates it depends on: `Some(false)` when
/// nothing does, so that the name is the prelude's or a crate's; `Some(true)`
/// when a declaration, a `use`, or a glob of a module of the crate binds
/// it; none when a glob of another crate's module might, which the reader
/// does not read to find out.
fn binds(resolver: &mut Resolver<'_>, krate: usize, module: usize, name: &str) -> Option<bool> {
    binds_in(resolver, krate, module, name, &mut HashSet::new())
}

/// [`binds`], with the modules whose globs are already being read: a
/// glob cycle, `use super::*` in a child of a module with `use child::*`,
/// is read once.
fn binds_in(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    name: &str,
    seen: &mut HashSet<usize>,
) -> Option<bool> {
    if !seen.insert(module) {
        return Some(false);
    }
    let m = &resolver.crates[krate].modules[module];
    if m.declares(name) || !m.uses_named(name).is_empty() {
        return Some(true);
    }
    let globs: Vec<(Vec<String>, bool)> = m
        .globs()
        .iter()
        .map(|&i| {
            let u = &m.uses[i];
            let segments = u.path.segments().iter().map(|s| s.to_string()).collect();
            (segments, u.absolute)
        })
        .collect();
    for (segments, absolute) in globs {
        // A glob of this crate's module: `crate::..`, `self::..`,
        // `super::..`, or a child's. Any other is another crate's.
        let local = !absolute
            && segments.first().is_some_and(|first| {
                matches!(first.as_str(), "crate" | "self" | "super")
                    || resolver.crates[krate].modules[module]
                        .children
                        .contains_key(first)
            });
        if !local {
            return None;
        }
        match resolver.resolve(krate, module, &segments, false) {
            Some(Def::Module(c, g)) if c == krate => {
                if binds_in(resolver, krate, g, name, seen)? {
                    return Some(true);
                }
            }
            _ => return None,
        }
    }
    Some(false)
}

/// The path lists of the derives of a type, in order.
fn derive_lists(ty: &TypeDef) -> impl Iterator<Item = Punctuated<syn::Path, syn::Token![,]>> + '_ {
    ty.attrs.iter().filter_map(|meta| match meta {
        Meta::List(list) if list.path.is_ident("derive") => list
            .parse_args_with(Punctuated::<syn::Path, syn::Token![,]>::parse_terminated)
            .ok(),
        _ => None,
    })
}

/// Whether `path` ends in `name`.
fn ends_in(path: &syn::Path, name: &str) -> bool {
    path.segments.last().is_some_and(|s| s.ident == name)
}

/// Whether a type derives a trait of this last name. The derive is matched
/// by name, not resolved: a derive macro's path leads into a proc-macro
/// crate, which the reader does not follow.
pub(crate) fn derives(ty: &TypeDef, name: &str) -> bool {
    derive_lists(ty).any(|paths| paths.iter().any(|p| ends_in(p, name)))
}

/// Whether type `def` derives `known`, serde's `Serialize` or
/// `Deserialize`, or the standard library's `Default`: a derive of that
/// last name ([`derives`]) whose path names the trait ([`known_trait`]).
pub(crate) fn derives_from(resolver: &mut Resolver<'_>, def: Def, known: Known) -> bool {
    let Def::Type(krate, module, index) = def else {
        return false;
    };
    let Some(row) = row_of(Item::Trait(known)) else {
        return false;
    };
    let definition = &resolver.crates[krate].modules[module].types[index];
    // Most types derive no such trait: the paths are kept only for one
    // that does.
    if !derives(definition, row.name) {
        return false;
    }
    let paths: Vec<syn::Path> = derive_lists(definition)
        .flatten()
        .filter(|p| ends_in(p, row.name))
        .collect();
    paths
        .iter()
        .any(|path| known_trait(resolver, krate, module, path) == Some(known))
}
