//! Types by identity, not by spelling: a type written in a module resolves
//! to its definition, through `use`s and aliases, with each argument
//! resolved the same way. `Carried<Words, B>` in one crate and an alias of
//! `carrier::Carried<protocols::Words, carrier::Plain>` in another are one
//! type when they resolve to one definition and one set of arguments.
//!
//! A type is written as `core::any::type_name` writes it, which is how a
//! build names an instantiation of a generic message: each definition by
//! its definition path, the arguments after it in `<..>`, separated by
//! `, `. `extract --verify` compares the two.

use std::collections::HashMap;

use quote::ToTokens as _;
use syn::ext::IdentExt as _;

use crate::resolve::{Def, Resolver, segments};

/// A type, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ty {
    /// A struct, enum, or union, and its arguments: crate, module, and its
    /// index among the module's types.
    Def(Def, Vec<Ty>),
    /// A primitive, by its name.
    Prim(String),
    /// `()`.
    Unit,
    /// A parameter of an impl, in an impl's self type: what matching binds
    /// to the instantiation.
    Param(String),
    /// A type of the standard library's the reader knows, and its
    /// arguments.
    Std(Std, Vec<Ty>),
}

/// The types of the standard library's the reader names and judges, by
/// their names alone where the prelude or a `use` of the standard library
/// brings them (a name trick is not chased).
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
    /// The one of this name.
    fn named(name: &str) -> Option<Self> {
        Some(match name {
            "String" => Self::String,
            "Vec" => Self::Vec,
            "VecDeque" => Self::VecDeque,
            "Option" => Self::Option,
            "Box" => Self::Box,
            "HashMap" => Self::HashMap,
            "BTreeMap" => Self::BTreeMap,
            _ => return None,
        })
    }

    /// Its path, as `core::any::type_name` writes it.
    fn path(self) -> &'static str {
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
    fn arity(self) -> usize {
        match self {
            Self::String => 0,
            Self::Vec | Self::VecDeque | Self::Option | Self::Box => 1,
            Self::HashMap | Self::BTreeMap => 2,
        }
    }

    /// Whether the prelude brings it by its name.
    fn prelude(self) -> bool {
        matches!(self, Self::String | Self::Vec | Self::Option | Self::Box)
    }
}

/// The type each parameter in scope stands for.
pub(crate) type Env = HashMap<String, Ty>;

/// How deep aliases and arguments may nest before the reader stops: a
/// bound on an alias that names itself.
pub(crate) const DEPTH: usize = 64;

/// The primitives, which a build names as they are written.
const PRIMITIVES: &[&str] = &[
    "bool", "char", "f32", "f64", "i8", "i16", "i32", "i64", "i128", "isize", "u8", "u16", "u32",
    "u64", "u128", "usize",
];

impl Ty {
    /// As `type_name` writes it.
    pub(crate) fn render(&self, resolver: &Resolver<'_>) -> String {
        match self {
            Self::Def(Def::Type(krate, module, index), args) => {
                let name = &resolver.crates[*krate].modules[*module].types[*index].name;
                let path = crate::markers::public_path(resolver, *krate, *module, name);
                if args.is_empty() {
                    path
                } else {
                    let args: Vec<String> = args.iter().map(|a| a.render(resolver)).collect();
                    format!("{path}<{}>", args.join(", "))
                }
            }
            Self::Def(Def::Module(..) | Def::Trait(..) | Def::Opaque, _) => {
                unreachable!("a type is a type")
            }
            Self::Prim(name) => name.clone(),
            Self::Unit => "()".to_string(),
            Self::Param(name) => name.clone(),
            Self::Std(std, args) if args.is_empty() => std.path().to_string(),
            Self::Std(std, args) => {
                let args: Vec<String> = args.iter().map(|a| a.render(resolver)).collect();
                format!("{}<{}>", std.path(), args.join(", "))
            }
        }
    }
}

/// What `ty`, written in module `module` of crate `krate`, is, with the
/// parameters `env` binds standing for their types. An alias is followed
/// to the type it names. A type the reader cannot name (another of the
/// standard library's than [`Std`]'s, such as `Arc`, a reference, a tuple
/// other than `()`, an array, a projection) is a problem that says so.
pub(crate) fn canonical(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    ty: &syn::Type,
    env: &Env,
) -> Result<Ty, String> {
    of(resolver, krate, module, ty, env, 0)
}

fn of(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    ty: &syn::Type,
    env: &Env,
    depth: usize,
) -> Result<Ty, String> {
    if depth > DEPTH {
        return Err(format!("`{}` nests too deep to read", ty.to_token_stream()));
    }
    let path = match ty {
        syn::Type::Tuple(t) if t.elems.is_empty() => return Ok(Ty::Unit),
        syn::Type::Paren(p) => return of(resolver, krate, module, &p.elem, env, depth + 1),
        syn::Type::Group(g) => return of(resolver, krate, module, &g.elem, env, depth + 1),
        syn::Type::Path(p) if p.qself.is_none() => &p.path,
        _ => return Err(unnamed(ty)),
    };
    if let (Some(param), true) = (path.get_ident(), path.leading_colon.is_none())
        && let Some(bound) = env.get(&param.unraw().to_string())
    {
        return Ok(bound.clone());
    }
    let (names, absolute) = segments(path);
    let Some(def) = resolver.resolve(krate, module, &names, absolute) else {
        if let ([name], false) = (names.as_slice(), absolute)
            && PRIMITIVES.contains(&name.as_str())
        {
            return Ok(Ty::Prim(name.clone()));
        }
        let Some(std) = standard(resolver, krate, module, path) else {
            return Err(unnamed(ty));
        };
        let mut args = Vec::new();
        if let Some(syn::PathArguments::AngleBracketed(bracketed)) =
            path.segments.last().map(|s| &s.arguments)
        {
            for arg in &bracketed.args {
                match arg {
                    syn::GenericArgument::Type(t) => {
                        args.push(of(resolver, krate, module, t, env, depth + 1)?);
                    }
                    _ => return Err(unnamed(ty)),
                }
            }
        }
        // A default left out, or one written (a map's hasher), the reader
        // does not read.
        if args.len() != std.arity() {
            return Err(unnamed(ty));
        }
        return Ok(Ty::Std(std, args));
    };
    let Def::Type(c, m, i) = def else {
        return Err(unnamed(ty));
    };
    let mut args = Vec::new();
    if let Some(syn::PathArguments::AngleBracketed(bracketed)) =
        path.segments.last().map(|s| &s.arguments)
    {
        for arg in &bracketed.args {
            match arg {
                syn::GenericArgument::Type(t) => {
                    args.push(of(resolver, krate, module, t, env, depth + 1)?);
                }
                syn::GenericArgument::Lifetime(_) => {}
                _ => return Err(unnamed(ty)),
            }
        }
    }
    let definition = &resolver.crates[c].modules[m].types[i];
    if definition.other_params {
        return Err(format!(
            "`{}`: the reader names types with type parameters only",
            ty.to_token_stream()
        ));
    }
    if definition.params.len() != args.len() {
        return Err(format!(
            "`{}`: `{}` takes {} type arguments; write each, as the reader does not read defaults",
            ty.to_token_stream(),
            definition.name,
            definition.params.len()
        ));
    }
    match definition.alias.clone() {
        Some(target) => {
            let env: Env = definition.params.iter().cloned().zip(args).collect();
            of(resolver, c, m, &target, &env, depth + 1)
        }
        None => Ok(Ty::Def(def, args)),
    }
}

/// The type of the standard library's that `path`, written in module
/// `module` of crate `krate`, names, if the reader knows it: by its name
/// alone where the prelude brings it and nothing in the module binds the
/// name, or by a path to it from `std`, `alloc`, or `core`, or a `use` of
/// one.
fn standard(
    resolver: &mut Resolver<'_>,
    krate: usize,
    module: usize,
    path: &syn::Path,
) -> Option<Std> {
    let name = crate::markers::imported_as(resolver, krate, module, path)?;
    let std = Std::named(&name)?;
    // `Vec<T>` alone, which `is_ident` is not, for its arguments.
    let alone =
        path.leading_colon.is_none() && path.segments.len() == 1 && path.segments[0].ident == name;
    let prelude = std.prelude()
        && alone
        && crate::identity::binds(resolver, krate, module, &name) == Some(false);
    (prelude
        || crate::identity::names_of(
            resolver,
            krate,
            module,
            path,
            &name,
            &["std", "alloc", "core"],
        ))
    .then_some(std)
}

fn unnamed(ty: &syn::Type) -> String {
    format!(
        "`{}`: the reader names a crate's own types, primitives, `()`, and of the standard library's `String`, `Vec`, `VecDeque`, `Option`, `Box`, `HashMap`, and `BTreeMap`",
        ty.to_token_stream()
    )
}

/// Bind the parameters that `pattern`, an impl's self type as
/// `canonical` reads it with the impl's parameters as `Ty::Param`, holds,
/// so that it is `target`; `None` when it cannot be. Aliases are already
/// followed in both, so the two compare by structure.
pub(crate) fn unify(pattern: &Ty, target: &Ty, env: &mut Env) -> Option<()> {
    match pattern {
        Ty::Param(name) => match env.get(name) {
            Some(bound) => (bound == target).then_some(()),
            None => {
                env.insert(name.clone(), target.clone());
                Some(())
            }
        },
        Ty::Def(def, args) => {
            let Ty::Def(target_def, target_args) = target else {
                return None;
            };
            if def != target_def || args.len() != target_args.len() {
                return None;
            }
            for (pattern, target) in args.iter().zip(target_args) {
                unify(pattern, target, env)?;
            }
            Some(())
        }
        Ty::Std(std, args) => {
            let Ty::Std(target_std, target_args) = target else {
                return None;
            };
            if std != target_std || args.len() != target_args.len() {
                return None;
            }
            for (pattern, target) in args.iter().zip(target_args) {
                unify(pattern, target, env)?;
            }
            Some(())
        }
        other => (other == target).then_some(()),
    }
}
