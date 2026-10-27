//! The impls of serde's traits, of `Default`, and of the role traits a
//! crate holds, by the type they are for: an impl counts for the
//! instantiation its self type matches.

use std::collections::HashMap;
use std::rc::Rc;

use syn::ext::IdentExt as _;

use crate::canon::{DEPTH, Env, Ty, canonical, unify};
use crate::external::{Item, Known, known_trait, named};
use crate::reads::Counted;
use crate::resolve::{Def, Resolver, segments};

/// The impls of serde's traits and of the role traits in each crate read,
/// by the type they are for: each crate's impls of one kind are resolved
/// once, since walking them for every type costs types times impls.
#[derive(Default)]
pub(crate) struct Impls(HashMap<(usize, Traits), ByType>);

/// One crate's impls of one kind, by the type they are for.
type ByType = Counted<HashMap<Def, Vec<ImplOf>>>;

impl Impls {
    /// The impls of the traits of `traits` for `def`, in the order of its
    /// crate's modules.
    pub(crate) fn of(
        &mut self,
        resolver: &mut Resolver<'_>,
        traits: Traits,
        def: Def,
    ) -> &[ImplOf] {
        let (Def::Type(krate, ..) | Def::Module(krate, _) | Def::Trait(krate, ..)) = def else {
            return &[];
        };
        let by_type = self
            .0
            .entry((krate, traits))
            .or_insert_with(|| index(resolver, krate, traits));
        by_type.get(&def).map_or(&[], Vec::as_slice)
    }
}

/// Crate `krate`'s impls of the traits of `traits`, by the type they are
/// for. Only those impls' self types are resolved: resolving one can read
/// another crate, and a crate holds many impls of traits nothing asks
/// about (`tests/cost.rs` guards it).
fn index(resolver: &mut Resolver<'_>, krate: usize, traits: Traits) -> ByType {
    let mut by_type = ByType::default();
    for module in 0..resolver.crates[krate].modules.len() {
        let found: Vec<(usize, Item, syn::Path, syn::Type)> = resolver.crates[krate].modules
            [module]
            .impls
            .iter()
            .enumerate()
            .filter_map(|(index, i)| {
                let path = i.trait_path.as_ref()?;
                let item = named(resolver, krate, module, path)?;
                traits
                    .hold(item)
                    .then(|| (index, item, path.clone(), i.self_ty.clone()))
            })
            .collect();
        for (index, item, path, self_ty) in found {
            // A trait of serde's name, or `Default`, that the reader cannot
            // follow to serde, or to the standard library, is another: the
            // reader judges only theirs.
            let ours = match item {
                // A message that is not generic is judged by name.
                _ if traits == Traits::SerdeNamed => true,
                Item::Trait(known) => known_trait(resolver, krate, module, &path) == Some(known),
                _ => true,
            };
            if !ours {
                continue;
            }
            let target = if traits == Traits::SerdeNamed {
                // The self type resolved as a path, its arguments aside.
                let syn::Type::Path(path) = &self_ty else {
                    continue;
                };
                let (names, absolute) = segments(&path.path);
                resolver.resolve(krate, module, &names, absolute)
            } else {
                // A qualified path names an associated type, not a type
                // the reader can index under.
                let syn::Type::Path(path @ syn::TypePath { qself: None, .. }) = &self_ty else {
                    continue;
                };
                head(resolver, krate, module, &path.path)
            };
            let Some(target) = target else {
                continue;
            };
            by_type.entry(target).or_default().push(ImplOf {
                item,
                module,
                index,
            });
        }
    }
    by_type
}

/// The type an impl's self type `path`, written in module `module`, is
/// for: its head, through aliases, since an impl for an alias is an impl
/// for the type it names. An argument is resolved only where an alias
/// names its parameter as its own head, so that `Identity<Stage<u8>>`,
/// with `type Identity<T> = T`, is `Stage`, and `Wrapper<dep::Payload>` is
/// `Wrapper` without reading `dep`.
///
/// It walks aliases as `canon::canonical` does, apart from `canonical`
/// because `canonical` resolves every argument as it goes, and so reads
/// each crate an argument names: the index holds every impl of a crate,
/// most of them for types nothing asks about (`tests/cost.rs` guards it).
/// Matching reads the self type whole with `canonical`, for the types it
/// is asked about. So that the two agree, it refuses, of the types it
/// follows, what `canonical` refuses: a type with a lifetime or const
/// parameter, a const argument, a count of type arguments other than the
/// type's, which leaves an argument to its default, and an alias that
/// names itself deeper than `canon::DEPTH`.
fn head(resolver: &mut Resolver<'_>, krate: usize, module: usize, path: &syn::Path) -> Option<Def> {
    let (mut krate, mut module, mut path) = (krate, module, path.clone());
    let mut scope = Scope::default();
    for _ in 0..=DEPTH {
        if let Some(ident) = path.get_ident()
            && let Some(bound) = scope.get(&ident.unraw().to_string())
        {
            let bound = bound.clone();
            (krate, module, path, scope) = (bound.krate, bound.module, bound.path, bound.scope);
            continue;
        }
        let (names, absolute) = segments(&path);
        let def = resolver.resolve(krate, module, &names, absolute)?;
        let Def::Type(c, m, i) = def else {
            return Some(def);
        };
        let mut written: Vec<&syn::Type> = Vec::new();
        if let Some(syn::PathArguments::AngleBracketed(bracketed)) =
            path.segments.last().map(|s| &s.arguments)
        {
            for arg in &bracketed.args {
                match arg {
                    syn::GenericArgument::Type(t) => written.push(t),
                    syn::GenericArgument::Lifetime(_) => {}
                    _ => return None,
                }
            }
        }
        let alias = &resolver.crates[c].modules[m].types[i];
        if alias.other_params || alias.params.len() != written.len() {
            return None;
        }
        let Some(aliased) = alias.alias.clone() else {
            return Some(def);
        };
        let syn::Type::Path(aliased @ syn::TypePath { qself: None, .. }) = aliased else {
            return None;
        };
        // An argument that is not a path names no type to index under, so
        // a parameter it is written for stays unbound, and resolves to
        // nothing if the alias names it.
        let bound: HashMap<String, Bound> = alias
            .params
            .iter()
            .zip(written)
            .filter_map(|(param, arg)| match arg {
                syn::Type::Path(arg @ syn::TypePath { qself: None, .. }) => Some((
                    param.clone(),
                    Bound {
                        path: arg.path.clone(),
                        krate,
                        module,
                        scope: scope.clone(),
                    },
                )),
                _ => None,
            })
            .collect();
        (krate, module, path, scope) = (c, m, aliased.path, Rc::new(bound));
    }
    None
}

/// The alias parameters in scope where a path is written, each with the
/// argument written for it.
type Scope = Rc<HashMap<String, Bound>>;

/// The argument written for an alias's parameter: its path, where it is
/// written, and the parameters in scope there.
#[derive(Clone)]
struct Bound {
    path: syn::Path,
    krate: usize,
    module: usize,
    scope: Scope,
}

/// The self type of impl `index` of module `module`, read by `canonical`
/// with the impl's parameters as `Ty::Param`; none when the reader cannot
/// name it.
fn self_type(resolver: &mut Resolver<'_>, krate: usize, module: usize, index: usize) -> Option<Ty> {
    let i = &resolver.crates[krate].modules[module].impls[index];
    let (self_ty, params) = (i.self_ty.clone(), i.params());
    let env: Env = params
        .iter()
        .map(|p| (p.clone(), Ty::Param(p.clone())))
        .collect();
    canonical(resolver, krate, module, &self_ty, &env).ok()
}

/// The traits an impl index holds: serde's, for the wire check, or the
/// role traits, for a node. Indexed apart, so that a crate asked only for
/// its nodes does not resolve its serde impls, nor the other way round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Traits {
    Serde,
    /// Traits of serde's names, whatever they resolve to, for a message
    /// that is not generic: [`crate::wire::plain_wire`].
    SerdeNamed,
    Role,
    /// `Default`, which serde bounds a parameter by for a field it fills
    /// with its default.
    Default,
}

impl Traits {
    /// Whether an index of this kind holds impls of `item`.
    fn hold(self, item: Item) -> bool {
        matches!(
            (self, item),
            (
                Self::Serde | Self::SerdeNamed,
                Item::Trait(Known::Serialize | Known::Deserialize)
            ) | (Self::Role, Item::Role(_))
                | (Self::Default, Item::Trait(Known::Default))
        )
    }
}

/// An impl of a trait for a type: the trait, and the impl's module and its
/// index among the module's impls.
#[derive(Clone, Copy)]
pub(crate) struct ImplOf {
    pub(crate) item: Item,
    pub(crate) module: usize,
    pub(crate) index: usize,
}

/// The parameters of impl `of`, in crate `krate`, bound so that its self
/// type is `target`; `None` when the impl is for another type.
pub(crate) fn matching(
    resolver: &mut Resolver<'_>,
    krate: usize,
    of: ImplOf,
    target: &Ty,
) -> Option<Env> {
    let pattern = self_type(resolver, krate, of.module, of.index)?;
    let mut env = Env::new();
    unify(&pattern, target, &mut env)?;
    let params = resolver.crates[krate].modules[of.module].impls[of.index].params();
    params.iter().all(|p| env.contains_key(p)).then_some(env)
}
