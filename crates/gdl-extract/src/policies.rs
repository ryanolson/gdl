//! The cell policies a crate implements: each impl of `CellPolicy` for a
//! type that a generated segment can name, with the head it reads.

use crate::canon::{Env, Ty, canonical};

use crate::markers::{derives, offered};
use crate::metadata::Graph;
use crate::resolve::{Def, Resolver, segments};
use crate::{PolicyEntry, PolicyHead, Skipped};

/// The cell policies of crate `krate`, and those it leaves out with why:
/// the impls whose trait the resolver follows to gdl's `CellPolicy`. Only
/// a package that depends on gdl, itself or through others, can implement
/// it, so a crate that does not has none, and the paths of one that does
/// enter only such packages: a crate holds many impls of other traits, and
/// their crates are not read for them (`tests/cost.rs` guards it).
pub(crate) fn policies(
    resolver: &mut Resolver<'_>,
    krate: usize,
) -> (Vec<PolicyEntry>, Vec<Skipped>) {
    let (mut found, mut skipped) = (Vec::new(), Vec::new());
    let reaches = reaching_gdl(resolver.graph);
    if !reaches[resolver.crates[krate].package] {
        return (found, skipped);
    }
    // Set only while an impl's trait is resolved: the policy's own types
    // and head may be in crates that do not depend on gdl.
    let mut gate = Some(reaches);
    for module in 0..resolver.crates[krate].modules.len() {
        let paths: Vec<(usize, syn::Path)> = resolver.crates[krate].modules[module]
            .impls
            .iter()
            .enumerate()
            .filter_map(|(index, i)| Some((index, i.trait_path.clone()?)))
            .collect();
        for (index, path) in paths {
            resolver.admit = gate.take();
            let gdls = gdls(resolver, krate, module, &path);
            gate = resolver.admit.take();
            if !gdls {
                continue;
            }
            match policy(resolver, krate, module, index) {
                Ok(entry) => found.push(entry),
                Err(left_out) => skipped.push(left_out),
            }
        }
    }
    found.sort_by(|a: &PolicyEntry, b| a.name.cmp(&b.name));
    (found, skipped)
}

/// Which packages of `graph` are gdl or depend on it, by index.
fn reaching_gdl(graph: &Graph) -> Vec<bool> {
    // Normal dependencies make no cycle, so each package is settled once
    // its dependencies are; `None` while it is being settled.
    fn settle(graph: &Graph, package: usize, reach: &mut [Option<bool>]) -> bool {
        if let Some(known) = reach[package] {
            return known;
        }
        reach[package] = Some(false);
        let deps: Vec<usize> = graph.packages[package]
            .deps
            .iter()
            .map(|&(_, p)| p)
            .collect();
        let found = graph.packages[package].name == "gdl"
            || deps.into_iter().any(|dep| settle(graph, dep, reach));
        reach[package] = Some(found);
        found
    }
    let mut reach = vec![None; graph.packages.len()];
    (0..graph.packages.len())
        .map(|package| settle(graph, package, &mut reach))
        .collect()
}

/// Whether `path`, an impl's trait written in module `module` of crate
/// `krate`, is gdl's `CellPolicy`, by where the resolver finds it defined.
/// A trait of that name that the crate declares, or one that a local
/// module named `gdl` holds, is not gdl's, and its impls would not
/// register.
fn gdls(resolver: &mut Resolver<'_>, krate: usize, module: usize, path: &syn::Path) -> bool {
    let (names, absolute) = segments(path);
    let Some(Def::Trait(c, m, i)) = resolver.resolve(krate, module, &names, absolute) else {
        return false;
    };
    let package = &resolver.graph.packages[resolver.crates[c].package];
    package.name == "gdl" && resolver.crates[c].modules[m].traits[i].name == "CellPolicy"
}

/// The policy impl `index` of module `module` is for, or why no segment
/// could name it.
fn policy(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    index: usize,
) -> Result<PolicyEntry, Skipped> {
    let i = &resolver.crates[krate].modules[module].impls[index];
    let (self_ty, params) = (i.self_ty.clone(), i.params());
    let head = i.types.iter().find(|(name, ..)| name == "Head").cloned();
    let written = match &self_ty {
        syn::Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default(),
        _ => String::new(),
    };
    let at = head
        .as_ref()
        .map(|(_, _, at)| format!("{at}: "))
        .unwrap_or_default();
    let skip = |why: String| Skipped {
        name: written.clone(),
        problem: format!("{at}cell policy `{written}` {why}"),
    };
    if !params.is_empty() {
        return Err(skip(
            "is generic: a segment names one type, so implement the policy for one".into(),
        ));
    }
    let Ok(Ty::Def(def @ Def::Type(c, m, t), args)) =
        canonical(resolver, krate, module, &self_ty, &Env::new())
    else {
        return Err(skip("is for a type the reader cannot name".into()));
    };
    let name = resolver.crates[c].modules[m].types[t].name.clone();
    let ty = format!("{}::{name}", resolver.crates[krate].name);
    if c != krate || !args.is_empty() || resolver.public_member(krate, 0, &name) != Some(def) {
        return Err(skip(format!(
            "is not reached from the crate root as `{ty}`"
        )));
    }
    let Some((_, head, _)) = head else {
        return Err(skip("has no `type Head`".into()));
    };
    let head = canonical(resolver, krate, module, &head, &Env::new())
        .ok()
        .and_then(|head| read_head(resolver, &head))
        .ok_or_else(|| {
            skip(format!(
                "reads a `Head` that is not a message, `()`, `gdl::Encoded`, or `gdl::Projected` of a message: `{}`",
                quote::ToTokens::to_token_stream(&head)
            ))
        })?;
    Ok(PolicyEntry { name, ty, head })
}

/// What a policy's `Head`, resolved, reads: `()`, gdl's `Encoded` or
/// `Projected<P>`, told by their definitions, or a message.
fn read_head(resolver: &mut Resolver<'_>, head: &Ty) -> Option<PolicyHead> {
    if *head == Ty::Unit {
        return Some(PolicyHead::Unit);
    }
    if let Ty::Def(Def::Type(c, m, i), args) = head {
        let package = &resolver.graph.packages[resolver.crates[*c].package];
        if package.name == "gdl" {
            match (
                resolver.crates[*c].modules[*m].types[*i].name.as_str(),
                args.as_slice(),
            ) {
                ("Encoded", []) => return Some(PolicyHead::Encoded),
                ("Projected", [projection]) => {
                    let of = projection_of(resolver, projection)?;
                    return Some(PolicyHead::Projected {
                        projection: projection.render(resolver),
                        of,
                    });
                }
                _ => {}
            }
        }
    }
    Some(PolicyHead::Message(offered(resolver, head).ok()??.origin))
}

/// The message a projection is a part of: the type its
/// `derive(Projection)` names in `#[gdl(of = <type>)]`, read where the
/// projection is defined. gdl seals `Projection`, so the derive is the only
/// way a crate implements it.
fn projection_of(resolver: &mut Resolver<'_>, projection: &Ty) -> Option<crate::Origin> {
    let Ty::Def(Def::Type(krate, module, index), _) = projection else {
        return None;
    };
    let definition = &resolver.crates[*krate].modules[*module].types[*index];
    if !derives(definition, "Projection") {
        return None;
    }
    let of = definition.attrs.iter().find_map(|meta| {
        let syn::Meta::List(list) = meta else {
            return None;
        };
        if !list.path.is_ident("gdl") {
            return None;
        }
        let mut of = None;
        list.parse_nested_meta(|meta| {
            if meta.path.is_ident("of") {
                of = Some(meta.value()?.parse::<syn::Type>()?);
            }
            Ok(())
        })
        .ok()?;
        of
    })?;
    let ty = canonical(resolver, *krate, *module, &of, &Env::new()).ok()?;
    Some(offered(resolver, &ty).ok()??.origin)
}
