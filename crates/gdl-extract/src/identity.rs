//! Which trait or type of serde's or the standard library's a path names:
//! by a path from the crate's root, or a single name that a `use`, or the
//! prelude, brings, where nothing in the module binds the name otherwise.
//! A name the reader cannot follow is another's, so the wire check refuses
//! it; a name trick it does not chase is capped, since the segment's crossing check catches it.

use std::collections::HashSet;

use syn::Meta;

use crate::markers::{derives, imported_as};
use crate::resolve::{Def, Resolver, segments};

/// The crates whose `Serialize` and `Deserialize` are serde's: serde, and
/// its derive's.
const SERDE: &[&str] = &["serde", "serde_derive"];

/// Whether `path`, written in module `module` of crate `krate`, names
/// `name` of one of `krates`: a path from such a crate's root that ends in
/// `name`, such as `serde::de::Deserialize`, or a single name that a `use`
/// of the module binds to one, under that name or another. The path's
/// first name is the crate's only where nothing in the module binds it,
/// such as a local `mod serde`, or where it is written `::serde`: a name
/// the reader cannot tell is another's.
pub(crate) fn names_of(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
    name: &str,
    krates: &[&str],
) -> bool {
    let (written, absolute) = segments(path);
    let paths: Vec<(Vec<String>, bool)> = match written.as_slice() {
        [single] => {
            let m = &resolver.crates[krate].modules[module];
            m.uses_named(single)
                .iter()
                .map(|&i| {
                    let u = &m.uses[i];
                    let segments = u.path.segments().iter().map(|s| s.to_string()).collect();
                    (segments, u.absolute)
                })
                .collect()
        }
        _ => vec![(written, absolute)],
    };
    paths.into_iter().any(|(segments, absolute)| {
        segments.len() > 1
            && krates.contains(&segments[0].as_str())
            && segments.last().map(String::as_str) == Some(name)
            && (absolute || binds(resolver, krate, module, &segments[0]) == Some(false))
    })
}

/// A trait the reader judges a bound on, and the derives it reads.
#[derive(Clone, Copy, PartialEq, Eq)]
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
    // A single name through the `use` that renames it: `Ser` after
    // `use serde::Serialize as Ser` is `Serialize`.
    let name = imported_as(resolver, krate, module, path)?;
    let (known, krates): (Known, &[&str]) = match name.as_str() {
        "Serialize" => (Known::Serialize, SERDE),
        "Deserialize" => (Known::Deserialize, SERDE),
        "DeserializeOwned" => (Known::DeserializeOwned, SERDE),
        "Default" => (Known::Default, STD),
        "Sized" => (Known::Sized, STD),
        _ => return None,
    };
    // The prelude's `Default` and `Sized`, by their names alone.
    let prelude = krates == STD
        && path.is_ident(&name)
        && binds(resolver, krate, module, &name) == Some(false);
    (prelude || names_of(resolver, krate, module, path, &name, krates)).then_some(known)
}

/// The crates of the standard library a path to `Default` or `Sized` may
/// begin with.
const STD: &[&str] = &["std", "core"];

/// Whether anything in module `module` of crate `krate` binds `name`,
/// beside the prelude and the crates it depends on: `Some(false)` when
/// nothing does, so that the name is the prelude's or a crate's; `Some(true)`
/// when a declaration, a `use`, or a glob of a module of the crate binds
/// it; none when a glob of another crate's module might, which the reader
/// does not read to find out.
pub(crate) fn binds(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    name: &str,
) -> Option<bool> {
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

/// Whether type `def` derives the trait `name`: serde's `Serialize` or
/// `Deserialize`, or the standard library's `Default`, as the derive's
/// path names it.
pub(crate) fn derives_from(resolver: &mut Resolver<'_>, def: Def, name: &str) -> bool {
    let Def::Type(krate, module, index) = def else {
        return false;
    };
    let definition = &resolver.crates[krate].modules[module].types[index];
    if !derives(definition, name) {
        return false;
    }
    let paths: Vec<syn::Path> = definition
        .attrs
        .iter()
        .filter_map(|meta| match meta {
            Meta::List(list) if list.path.is_ident("derive") => list
                .parse_args_with(
                    syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
                )
                .ok(),
            _ => None,
        })
        .flatten()
        .filter(|p| p.segments.last().is_some_and(|s| s.ident == name))
        .collect();
    let wanted = match name {
        "Serialize" => Known::Serialize,
        "Deserialize" => Known::Deserialize,
        "Default" => Known::Default,
        _ => return false,
    };
    paths
        .iter()
        .any(|path| known_trait(resolver, krate, module, path) == Some(wanted))
}
