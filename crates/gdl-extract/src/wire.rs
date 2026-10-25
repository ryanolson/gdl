//! Whether an instantiation is a wire type, `Default`, and sized: what the
//! wire check judges of each direction of serde's, derived or written by
//! hand, and of what each bounds.

use std::collections::HashSet;

use crate::attrs::{
    CONTAINER, Fill, Side, bounded_on, judged, only, remote, serde_bounds, serde_default,
};
use syn::ext::IdentExt as _;

use crate::canon::{DEPTH, Env, Std, Ty, canonical};
use crate::identity::{Known, derives_from, known_trait};
use crate::impls::{ImplOf, Impls, TraitName, Traits, matching};
use crate::markers::derives;
use crate::resolve::{Def, Resolver};

/// Whether `ty` is a wire type: it has serde's traits in both directions,
/// and each type a direction bounds is a wire type, or `Default`, as that
/// direction bounds it. A direction is derived, and bounds what serde's
/// derive infers, or written by hand for `ty`, by an impl whose self type
/// is `ty`, and bounds what that impl bounds: so an impl for one
/// instantiation of a generic type serves that one alone. A derived
/// `Deserialize` of a type that borrows is not `DeserializeOwned`, so it
/// gives no codec.
pub(crate) fn ty_wire(resolver: &mut Resolver<'_>, impls: &mut Impls, ty: &Ty) -> bool {
    wire_at(resolver, impls, ty, 0)
}

/// [`ty_wire`], `depth` checks deep: what each check needs is a part of
/// the type it checks, so the check ends, but a defect that broke that
/// would recurse without end; past `canon::DEPTH` the answer is local.
fn wire_at(resolver: &mut Resolver<'_>, impls: &mut Impls, ty: &Ty, depth: usize) -> bool {
    if depth > DEPTH {
        return false;
    }
    match ty {
        // Each primitive, and `()`, is a wire type.
        Ty::Prim(_) | Ty::Unit => return true,
        Ty::Param(_) | Ty::Def(Def::Module(..), _) => return false,
        Ty::Std(std, args) => {
            return match std {
                Std::String => true,
                // Serde's impls bound only the element.
                Std::Vec | Std::VecDeque | Std::Option | Std::Box => {
                    wire_at(resolver, impls, &args[0], depth + 1)
                }
                // Deserializing one bounds the key by `Eq` and `Hash`, or
                // by `Ord`, which the reader judges only for a key it
                // knows has them: a string, or a primitive other than a
                // float.
                Std::HashMap | Std::BTreeMap => {
                    let key = match &args[0] {
                        Ty::Std(Std::String, _) | Ty::Unit => true,
                        Ty::Prim(name) => !name.starts_with('f'),
                        _ => false,
                    };
                    key && wire_at(resolver, impls, &args[1], depth + 1)
                }
            };
        }
        Ty::Def(Def::Type(..), _) => {}
    }
    // Both directions' needs, merged per type, so that each type is judged
    // once: judged once per direction, each level of a type's own nesting
    // doubled the work while the type grew by one.
    let mut merged: Vec<(Ty, Bounded)> = Vec::new();
    for side in [Side::Serialize, Side::Deserialize] {
        let Some(needs) = needs(resolver, impls, ty, side) else {
            return false;
        };
        for (arg, need) in needs {
            match merged.iter_mut().find(|(seen, _)| *seen == arg) {
                Some((_, all)) => {
                    all.wire |= need.wire;
                    all.default |= need.default;
                    all.sized |= need.sized;
                }
                None => merged.push((arg, need)),
            }
        }
    }
    merged.into_iter().all(|(arg, need)| {
        (!need.wire || wire_at(resolver, impls, &arg, depth + 1))
            && (!need.default || defaults_at(resolver, impls, &arg, depth + 1))
            && (!need.sized || sized(resolver, &arg, depth + 1))
    })
}

/// Whether a message with no type parameters, offered as itself, is a wire
/// type: it derives traits of serde's names, `Deserialize` of a type that
/// borrows aside, or its crate implements traits of those names for it.
/// Names, not identities, and no attribute is read: this is how the reader
/// judged every message before generic ones, and a message that is not
/// generic is judged so still, so that the cuts the generator allows for it
/// do not change. A misread one fails the segment's crossing check at
/// build time. An instantiation's arguments are judged strictly, by
/// [`ty_wire`].
pub(crate) fn plain_wire(resolver: &mut Resolver<'_>, impls: &mut Impls, def: Def) -> bool {
    let Def::Type(krate, module, index) = def else {
        return false;
    };
    let ty = &resolver.crates[krate].modules[module].types[index];
    let (mut serialize, mut deserialize) = (
        derives(ty, "Serialize"),
        derives(ty, "Deserialize") && !ty.borrows,
    );
    if serialize && deserialize {
        return true;
    }
    for found in impls.of(resolver, Traits::SerdeNamed, def) {
        serialize |= found.name == TraitName::Serialize;
        deserialize |= found.name == TraitName::Deserialize;
    }
    serialize && deserialize
}

/// What `ty`'s serde trait for `side` bounds, each type with what it must
/// be; none when `ty` has no such trait, or when the reader cannot name a
/// type its impl bounds.
fn needs(
    resolver: &mut Resolver<'_>,
    impls: &mut Impls,
    ty: &Ty,
    side: Side,
) -> Option<Vec<(Ty, Bounded)>> {
    let Ty::Def(def @ Def::Type(krate, module, index), args) = ty else {
        unreachable!("a type a crate defines");
    };
    let (krate, def) = (*krate, *def);
    let borrows = resolver.crates[krate].modules[*module].types[*index].borrows;
    let derived = match side {
        Side::Serialize => derives_from(resolver, def, "Serialize"),
        Side::Deserialize => derives_from(resolver, def, "Deserialize") && !borrows,
    };
    let definition = &resolver.crates[krate].modules[*module].types[*index];
    if derived {
        // A container attribute the reader does not read may change what
        // the derive makes at all: `remote` makes no trait for the type
        // itself, whether it is generic or not. `from`, `into`, and
        // `try_from` route through another type, which serde bounds by its
        // own trait where the type has no arguments, so that the type's
        // build fails unless it is one; with arguments, what they bound is
        // not read.
        let unread = if args.is_empty() {
            remote(definition.attrs.iter())
        } else {
            !only(definition.attrs.iter(), CONTAINER)
        };
        if unread {
            return None;
        }
        // A container `#[serde(default)]` fills a missing field from the
        // type's own default: serde bounds the type itself by `Default`,
        // unless the container's bound replaces serde's.
        let mut needs = Vec::new();
        if side == Side::Deserialize
            && serde_bounds(definition.attrs.iter(), side).is_none()
            && serde_default(definition.attrs.iter()) == Some(Fill::Default)
        {
            needs.push((
                ty.clone(),
                Bounded {
                    default: true,
                    ..Bounded::default()
                },
            ));
        }
        // A type with no arguments bounds none: its fields need not be read.
        if args.is_empty() {
            return Some(needs);
        }
        // A variant's or a field's attribute the reader does not read may
        // change what serde bounds.
        if !judged(definition) {
            return None;
        }
        // Owned, so that the bounds' traits can be told by the resolver.
        let definition = definition.clone();
        let mut known = |path: &syn::Path| known_trait(resolver, krate, *module, path);
        let (mut wire, mut default) = (HashSet::new(), HashSet::new());
        bounded_on(&definition, side, &mut wire, &mut default, &mut known)?;
        needs.extend(definition.params.iter().zip(args).map(|(param, arg)| {
            let need = Bounded {
                wire: wire.contains(param),
                default: default.contains(param),
                sized: false,
            };
            (arg.clone(), need)
        }));
        return Some(needs);
    }
    let name = match side {
        Side::Serialize => TraitName::Serialize,
        Side::Deserialize => TraitName::Deserialize,
    };
    let found: Vec<ImplOf> = impls.of(resolver, Traits::Serde, def).to_vec();
    for of in found.into_iter().filter(|of| of.name == name) {
        if let Some(env) = matching(resolver, krate, of, ty) {
            let written = &resolver.crates[krate].modules[of.module].impls[of.index];
            if side == Side::Deserialize && !every_lifetime(written) {
                return None;
            }
            return impl_needs(resolver, krate, of, &env);
        }
    }
    None
}

/// Whether `Deserialize` impl `written` holds for every input lifetime, as
/// `DeserializeOwned` needs: the lifetime its trait names is one of its own
/// parameters, which nothing bounds and which bounds nothing, so that it
/// may be any. Not one for `'static` alone, nor one for a `'de` that must
/// outlive another lifetime: the instantiation is then local. A bound of a
/// type parameter by the lifetime, `T: 'de`, needs only that `T` borrows
/// nothing, which an argument the reader names never does.
fn every_lifetime(written: &crate::tree::Impl) -> bool {
    let Some(syn::PathArguments::AngleBracketed(args)) = written
        .trait_path
        .as_ref()
        .and_then(|path| path.segments.last())
        .map(|segment| &segment.arguments)
    else {
        return false;
    };
    let mut args = args.args.iter();
    let (Some(syn::GenericArgument::Lifetime(de)), None) = (args.next(), args.next()) else {
        return false;
    };
    let generics = &written.generics;
    let mut lifetimes = generics.lifetimes();
    if !lifetimes.any(|param| param.lifetime == *de && param.bounds.is_empty()) {
        return false;
    }
    let bounds_it = |param: &syn::LifetimeParam| param.bounds.iter().any(|b| b == de);
    if generics.lifetimes().any(bounds_it) {
        return false;
    }
    generics.where_clause.as_ref().is_none_or(|clause| {
        clause.predicates.iter().all(|predicate| match predicate {
            syn::WherePredicate::Lifetime(p) => {
                p.lifetime != *de && !p.bounds.iter().any(|b| b == de)
            }
            _ => true,
        })
    })
}

/// What impl `of` bounds, each type with the impl's parameters bound by
/// `env`: a bound on serde's traits needs a wire type, one on `Default` a
/// `Default` one, one on `Sized`, explicit or implicit, a sized one, and
/// `?Sized` or a lifetime nothing. None when
/// it bounds by any other trait, which the reader cannot judge, or bounds
/// a type that is not one of its parameters: the instantiation is then
/// local.
fn impl_needs(
    resolver: &mut Resolver<'_>,
    krate: usize,
    of: ImplOf,
    env: &Env,
) -> Option<Vec<(Ty, Bounded)>> {
    let generics = resolver.crates[krate].modules[of.module].impls[of.index]
        .generics
        .clone();
    let mut known = |path: &syn::Path| known_trait(resolver, krate, of.module, path);
    let mut bounds: Vec<(syn::Type, Bounded)> = Vec::new();
    for param in generics.type_params() {
        let ty = syn::Type::Path(syn::TypePath {
            qself: None,
            path: param.ident.clone().into(),
        });
        let mut need = need_of(&param.bounds, &mut known)?;
        // A parameter is `Sized` unless a bound of its own says `?Sized`.
        need.sized |= !relaxed(&generics, &param.ident);
        bounds.push((ty, need));
    }
    if let Some(clause) = &generics.where_clause {
        for predicate in &clause.predicates {
            if let syn::WherePredicate::Type(predicate) = predicate {
                bounds.push((
                    predicate.bounded_ty.clone(),
                    need_of(&predicate.bounds, &mut known)?,
                ));
            }
        }
    }
    let params = generics
        .type_params()
        .map(|p| p.ident.unraw().to_string())
        .collect::<Vec<_>>();
    let mut needs = Vec::new();
    for (ty, need) in bounds {
        if !need.wire && !need.default && !need.sized {
            continue;
        }
        // A parameter alone: a bound on another type, such as the impl's
        // own self type, the reader does not judge.
        match &ty {
            syn::Type::Path(p)
                if p.qself.is_none()
                    && p.path
                        .get_ident()
                        .is_some_and(|i| params.contains(&i.unraw().to_string())) => {}
            _ => return None,
        }
        let ty = canonical(resolver, krate, of.module, &ty, env).ok()?;
        needs.push((ty, need));
    }
    Some(needs)
}

/// What a list of bounds needs of the type it bounds: serde's traits a
/// wire type, `Default` a `Default` one, `Sized` a sized one, and `?Sized`
/// and a lifetime nothing, each as `known` tells the trait a path names.
/// None for any other bound, which the reader cannot judge: `Copy`, or
/// `Sync`, of a wire type does not follow, nor does a trait of serde's name
/// that is not serde's.
pub(crate) fn need_of<'b>(
    bounds: impl IntoIterator<Item = &'b syn::TypeParamBound>,
    known: &mut dyn FnMut(&syn::Path) -> Option<Known>,
) -> Option<Bounded> {
    let mut need = Bounded::default();
    for bound in bounds {
        let t = match bound {
            syn::TypeParamBound::Lifetime(_) => continue,
            syn::TypeParamBound::Trait(t) => t,
            _ => return None,
        };
        match known(&t.path)? {
            // `?Sized` relaxes the parameter's own `Sized`, read apart.
            Known::Sized if matches!(t.modifier, syn::TraitBoundModifier::Maybe(_)) => {}
            Known::Sized => need.sized = true,
            Known::Serialize | Known::Deserialize | Known::DeserializeOwned => need.wire = true,
            Known::Default => need.default = true,
        }
    }
    Some(need)
}

/// Whether `ty` is `Default`: a primitive or `()`; a type that derives
/// it, whose derive bounds every argument of a struct by it and none of an
/// enum, whose default is a unit variant; or one its crate implements it
/// for, by an impl whose self type is `ty` and each of whose bounds is on
/// `Default` and met. A bound on any other trait, serde's included, the
/// reader does not take for met. `depth` bounds it as in [`wire_at`].
fn defaults_at(resolver: &mut Resolver<'_>, impls: &mut Impls, ty: &Ty, depth: usize) -> bool {
    if depth > DEPTH {
        return false;
    }
    let (krate, def, args) = match ty {
        // Each primitive, and `()`, is `Default`.
        Ty::Prim(_) | Ty::Unit => return true,
        Ty::Param(_) | Ty::Def(Def::Module(..), _) => return false,
        // Each is `Default` whatever it holds, but a box: it holds a
        // default value.
        Ty::Std(Std::Box, args) => return defaults_at(resolver, impls, &args[0], depth + 1),
        Ty::Std(..) => return true,
        Ty::Def(def @ Def::Type(c, ..), args) => (*c, *def, args),
    };
    let Def::Type(c, m, i) = def else {
        unreachable!("a type");
    };
    if derives_from(resolver, def, "Default") {
        let definition = &resolver.crates[c].modules[m].types[i];
        // Only a struct has fields of its own; `derive(Default)` refuses a
        // union, and an alias is followed before this.
        let enumeration = definition.fields.is_none();
        return enumeration
            || args
                .iter()
                .all(|arg| defaults_at(resolver, impls, arg, depth + 1));
    }
    let found: Vec<ImplOf> = impls.of(resolver, Traits::Default, def).to_vec();
    for of in found {
        let Some(needs) =
            matching(resolver, krate, of, ty).and_then(|env| impl_needs(resolver, krate, of, &env))
        else {
            continue;
        };
        return needs.into_iter().all(|(arg, need)| {
            !need.wire
                && (!need.default || defaults_at(resolver, impls, &arg, depth + 1))
                && (!need.sized || sized(resolver, &arg, depth + 1))
        });
    }
    false
}

/// Whether a bound of `generics` on `param` itself says `?Sized`.
fn relaxed(generics: &syn::Generics, param: &syn::Ident) -> bool {
    let maybe = |bounds: &syn::punctuated::Punctuated<syn::TypeParamBound, syn::Token![+]>| {
        bounds.iter().any(|b| {
            matches!(b, syn::TypeParamBound::Trait(t)
                if matches!(t.modifier, syn::TraitBoundModifier::Maybe(_)))
        })
    };
    generics
        .type_params()
        .any(|p| p.ident == *param && maybe(&p.bounds))
        || generics.where_clause.iter().any(|clause| {
            clause.predicates.iter().any(|predicate| {
                matches!(predicate, syn::WherePredicate::Type(t)
                    if matches!(&t.bounded_ty, syn::Type::Path(p) if p.path.is_ident(param))
                        && maybe(&t.bounds))
            })
        })
}

/// Whether `ty` is sized: a primitive or `()`; an enum, or a union; or a
/// struct whose last field is, with its parameters bound by `ty`'s
/// arguments. A field of `[T]`, `str`, or a `dyn` trait is not. A field of
/// a type the reader cannot name, from the standard library, say, is by
/// its name: `str`, `Path`, `OsStr`, and `CStr` are not, and any other
/// is, as every other type there whose name it can write is. `depth`
/// bounds it as in [`wire_at`].
fn sized(resolver: &mut Resolver<'_>, ty: &Ty, depth: usize) -> bool {
    if depth > DEPTH {
        return false;
    }
    let Ty::Def(Def::Type(c, m, i), args) = ty else {
        // Each the reader knows of the standard library's is sized.
        return matches!(ty, Ty::Prim(_) | Ty::Unit | Ty::Std(..));
    };
    let (c, m, i) = (*c, *m, *i);
    let definition = &resolver.crates[c].modules[m].types[i];
    let Some(last) = definition.fields.as_ref().and_then(|f| f.iter().last()) else {
        // An enum, a union, or a struct without fields.
        return true;
    };
    let last = last.ty.clone();
    let env: Env = definition
        .params
        .iter()
        .cloned()
        .zip(args.iter().cloned())
        .collect();
    match &last {
        syn::Type::Slice(_) | syn::Type::TraitObject(_) => false,
        syn::Type::Path(p) if p.qself.is_none() => match canonical(resolver, c, m, &last, &env) {
            Ok(ty) => sized(resolver, &ty, depth + 1),
            Err(_) => !p.path.segments.last().is_some_and(|s| {
                matches!(
                    s.ident.to_string().as_str(),
                    "str" | "Path" | "OsStr" | "CStr"
                )
            }),
        },
        // A reference, a pointer, an array, a tuple, a function: sized.
        _ => true,
    }
}

/// What serde bounds a type by.
#[derive(Clone, Copy, Default)]
pub(crate) struct Bounded {
    /// `Serialize` or `Deserialize`: the type must be a wire type.
    pub(crate) wire: bool,
    /// `Default`, for a field serde fills with its default.
    pub(crate) default: bool,
    /// `Sized`, which a hand-written impl's parameter is unless it says
    /// `?Sized`.
    pub(crate) sized: bool,
}
