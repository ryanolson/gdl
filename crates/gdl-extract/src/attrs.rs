//! What serde's attributes say of what its derive bounds: its bounds, its
//! skips, its defaults, and the attributes the reader reads at all.

use std::collections::HashSet;

use quote::ToTokens as _;
use syn::Meta;
use syn::ext::IdentExt as _;

use crate::identity::Known;
use crate::tree::TypeDef;
use crate::wire::need_of;

/// The serde attributes the reader reads for what serde bounds, of a
/// container, a variant, and a field. Any other, such as `from`, `into`,
/// or `remote`, may change what serde bounds, so a type with one is local;
/// a type with no arguments only for `remote` ([`remote`]).
/// `with` and its kin are among them: serde bounds nothing for such a
/// field, and the reader bounds what its type names, which refuses at
/// worst a crossing that would work.
pub(crate) const CONTAINER: &[&str] = &[
    "rename",
    "rename_all",
    "rename_all_fields",
    "deny_unknown_fields",
    "tag",
    "content",
    "untagged",
    "bound",
    "default",
    "transparent",
    "crate",
    "expecting",
];
const VARIANT: &[&str] = &[
    "rename",
    "alias",
    "rename_all",
    "skip",
    "skip_serializing",
    "skip_deserializing",
    "serialize_with",
    "deserialize_with",
    "with",
    "bound",
    "borrow",
    "other",
    "untagged",
];
const FIELD: &[&str] = &[
    "rename",
    "alias",
    "default",
    "flatten",
    "skip",
    "skip_serializing",
    "skip_deserializing",
    "skip_serializing_if",
    "serialize_with",
    "deserialize_with",
    "with",
    "borrow",
    "bound",
];

/// Whether every serde attribute of `definition`, of its variants, and of
/// its fields is one the reader reads.
pub(crate) fn judged(definition: &TypeDef) -> bool {
    only(definition.attrs.iter(), CONTAINER)
        && definition
            .variants
            .iter()
            .all(|variant| only(variant.iter(), VARIANT))
        && definition
            .members
            .iter()
            .all(|member| only(metas(&member.field), FIELD))
}

/// Whether each key of each serde attribute among `metas` is `allowed`.
pub(crate) fn only<'m>(mut metas: impl Iterator<Item = &'m Meta>, allowed: &[&str]) -> bool {
    metas.all(|meta| {
        serde_keys(meta)
            .iter()
            .all(|key| allowed.contains(&key.as_str()))
    })
}

/// Whether a serde attribute among `metas` is `remote`, which makes no
/// trait for the type itself.
pub(crate) fn remote<'m>(mut metas: impl Iterator<Item = &'m Meta>) -> bool {
    metas.any(|meta| serde_keys(meta).iter().any(|key| key == "remote"))
}

/// The keys of `#[serde(..)]`: each name that starts an entry.
fn serde_keys(meta: &Meta) -> Vec<String> {
    use proc_macro2::TokenTree;
    let Meta::List(list) = meta else {
        return Vec::new();
    };
    if !list.path.is_ident("serde") {
        return Vec::new();
    }
    let mut keys = Vec::new();
    let mut start = true;
    for token in list.tokens.clone() {
        match &token {
            TokenTree::Ident(ident) if start => keys.push(ident.to_string()),
            TokenTree::Punct(p) if p.as_char() == ',' => {
                start = true;
                continue;
            }
            _ => {}
        }
        start = false;
    }
    keys
}

/// A direction of serde's derive, which builds its where-clause on its
/// own: a bound or a skip may name one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Serialize,
    Deserialize,
}

impl Side {
    /// Whether `metas`, a field's or a variant's, skip this direction:
    /// `skip`, or the word for it alone.
    fn skips<'m>(self, mut metas: impl Iterator<Item = &'m Meta>) -> bool {
        let alone = match self {
            Self::Serialize => "skip_serializing",
            Self::Deserialize => "skip_deserializing",
        };
        metas.any(|meta| serde_word(meta, "skip") || serde_word(meta, alone))
    }
}

/// Add the parameters that serde's derive for `side` bounds, as it builds
/// its where-clause, to `wire`, and those it bounds by `Default` to
/// `default`: those each field's or variant's own bound for `side` names,
/// whether serde skips it or not; then those the container's bound for
/// `side` names, in place of the inference; else, for each field that
/// neither it nor its variant skips or bounds for `side`, those its type
/// names outside a `PhantomData`, and, deserializing, those the type of a
/// field serde fills with its default names, by `Default`. None when a
/// field's type names an associated type of a parameter, which serde
/// bounds and the reader cannot judge.
pub(crate) fn bounded_on(
    definition: &TypeDef,
    side: Side,
    wire: &mut HashSet<String>,
    default: &mut HashSet<String>,
    known: &mut dyn FnMut(&syn::Path) -> Option<Known>,
) -> Option<()> {
    let params = &definition.params;
    for member in &definition.members {
        if let Some(bounds) = serde_bounds(metas(&member.field), side) {
            named_by_bounds(&bounds, params, wire, default, known)?;
        }
    }
    for variant in &definition.variants {
        if let Some(bounds) = serde_bounds(variant.iter(), side) {
            named_by_bounds(&bounds, params, wire, default, known)?;
        }
    }
    if let Some(bounds) = serde_bounds(definition.attrs.iter(), side) {
        return named_by_bounds(&bounds, params, wire, default, known);
    }
    for member in &definition.members {
        let field = &member.field;
        let variant = member.variant.map(|v| &definition.variants[v]);
        if side == Side::Deserialize && fills_by_default(field) {
            named_by_type(&field.ty, params, default)?;
        }
        if side.skips(metas(field))
            || serde_bounds(metas(field), side).is_some()
            || variant.is_some_and(|attrs| {
                side.skips(attrs.iter()) || serde_bounds(attrs.iter(), side).is_some()
            })
        {
            continue;
        }
        named_by_type(&field.ty, params, wire)?;
    }
    Some(())
}

/// Whether serde fills `field` with `Default::default()` when it
/// deserializes: `#[serde(default)]`, or a field it does not deserialize,
/// without a `default = ".."` of its own.
fn fills_by_default(field: &syn::Field) -> bool {
    match serde_default(metas(field)) {
        Some(Fill::Default) => true,
        Some(Fill::Path) => false,
        None => Side::Deserialize.skips(metas(field)),
    }
}

/// What `#[serde(default)]` among `metas` fills a value with.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fill {
    /// `Default::default()`: the word alone.
    Default,
    /// A function of its own: `default = ".."`.
    Path,
}

/// The `default` that `metas` write, if any.
pub(crate) fn serde_default<'m>(metas: impl Iterator<Item = &'m Meta>) -> Option<Fill> {
    use proc_macro2::TokenTree;
    let mut fill = None;
    for meta in metas {
        let Meta::List(list) = meta else {
            continue;
        };
        if !list.path.is_ident("serde") {
            continue;
        }
        let tokens: Vec<TokenTree> = list.tokens.clone().into_iter().collect();
        for (at, token) in tokens.iter().enumerate() {
            if matches!(token, TokenTree::Ident(i) if i == "default") {
                fill = Some(match tokens.get(at + 1) {
                    Some(TokenTree::Punct(p)) if p.as_char() == '=' => Fill::Path,
                    _ => Fill::Default,
                });
            }
        }
    }
    fill
}

/// A field's attributes.
fn metas(field: &syn::Field) -> impl Iterator<Item = &Meta> {
    field.attrs.iter().map(|a| &a.meta)
}

/// The where-clauses for `side` that `#[serde(bound = "..")]`, for both
/// directions, or `#[serde(bound(serialize = "..", deserialize = ".."))]`,
/// for each, write among `metas`; none when no `bound` is there for
/// `side`. A bound that is not a string is `None` in the list: the reader
/// cannot say what it bounds.
pub(crate) fn serde_bounds<'m>(
    metas: impl Iterator<Item = &'m Meta>,
    side: Side,
) -> Option<Vec<Option<String>>> {
    use proc_macro2::TokenTree;
    let string = |token: Option<&TokenTree>| match token {
        Some(TokenTree::Literal(literal)) => {
            syn::parse2::<syn::LitStr>(TokenTree::Literal(literal.clone()).into())
                .ok()
                .map(|s| s.value())
        }
        _ => None,
    };
    let named = match side {
        Side::Serialize => "serialize",
        Side::Deserialize => "deserialize",
    };
    let mut bounds = None;
    for meta in metas {
        let Meta::List(list) = meta else {
            continue;
        };
        if !list.path.is_ident("serde") {
            continue;
        }
        let tokens: Vec<TokenTree> = list.tokens.clone().into_iter().collect();
        for (at, token) in tokens.iter().enumerate() {
            if !matches!(token, TokenTree::Ident(i) if i == "bound") {
                continue;
            }
            match tokens.get(at + 1) {
                Some(TokenTree::Punct(p)) if p.as_char() == '=' => {
                    bounds
                        .get_or_insert_with(Vec::new)
                        .push(string(tokens.get(at + 2)));
                }
                Some(TokenTree::Group(group)) => {
                    let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                    for (at, token) in inner.iter().enumerate() {
                        if matches!(token, TokenTree::Ident(i) if i == named)
                            && matches!(inner.get(at + 1), Some(TokenTree::Punct(p)) if p.as_char() == '=')
                        {
                            bounds
                                .get_or_insert_with(Vec::new)
                                .push(string(inner.get(at + 2)));
                        }
                    }
                }
                _ => bounds.get_or_insert_with(Vec::new).push(None),
            }
        }
    }
    bounds
}

/// Add the parameters that `bounds`, where-clauses, bound, to `wire` for
/// serde's traits and to `default` for `Default`. A bound on no parameter
/// holds for every instantiation or for none, so it is passed over. None
/// for one the reader cannot judge: a bound it cannot read, one on another
/// trait, or one on a type that names a parameter but is not one, such as
/// `Vec<T>` or `T::Out`.
fn named_by_bounds(
    bounds: &[Option<String>],
    params: &[String],
    wire: &mut HashSet<String>,
    default: &mut HashSet<String>,
    known: &mut dyn FnMut(&syn::Path) -> Option<Known>,
) -> Option<()> {
    for bound in bounds {
        let clause = bound
            .as_deref()
            .and_then(|b| syn::parse_str::<syn::WhereClause>(&format!("where {b}")).ok())?;
        for predicate in &clause.predicates {
            let predicate = match predicate {
                // A bound on serde's `'de` holds for some input lifetimes
                // alone, and `DeserializeOwned` needs every one.
                syn::WherePredicate::Lifetime(p)
                    if p.lifetime.ident == "de" || p.bounds.iter().any(|b| b.ident == "de") =>
                {
                    return None;
                }
                syn::WherePredicate::Lifetime(_) => continue,
                syn::WherePredicate::Type(predicate) => predicate,
                _ => return None,
            };
            let bounded = predicate.bounded_ty.to_token_stream();
            if !params.iter().any(|param| names(bounded.clone(), param)) {
                continue;
            }
            let param = match &predicate.bounded_ty {
                syn::Type::Path(p) if p.qself.is_none() => p.path.get_ident()?.unraw().to_string(),
                _ => return None,
            };
            let need = need_of(&predicate.bounds, known)?;
            if need.wire {
                wire.insert(param.clone());
            }
            if need.default {
                default.insert(param);
            }
        }
    }
    Some(())
}

/// Add the parameters that serde's inference bounds for a field of type
/// `ty`: each one the type names as a path of its own, at any depth, but
/// not inside a `PhantomData`, which serde bounds nothing for. None when
/// the type names an associated type of a parameter, `T::Out` or
/// `<T as Has>::Out`, which serde bounds and the reader cannot judge, or is
/// a macro, which the reader cannot expand.
fn named_by_type(ty: &syn::Type, params: &[String], found: &mut HashSet<String>) -> Option<()> {
    match ty {
        syn::Type::Path(p) => {
            if let Some(qself) = &p.qself
                && params
                    .iter()
                    .any(|param| names(qself.ty.to_token_stream(), param))
            {
                return None;
            }
            named_by_path(&p.path, params, found)
        }
        syn::Type::Array(t) => named_by_type(&t.elem, params, found),
        syn::Type::Slice(t) => named_by_type(&t.elem, params, found),
        syn::Type::Ptr(t) => named_by_type(&t.elem, params, found),
        syn::Type::Reference(t) => named_by_type(&t.elem, params, found),
        syn::Type::Paren(t) => named_by_type(&t.elem, params, found),
        syn::Type::Group(t) => named_by_type(&t.elem, params, found),
        syn::Type::Tuple(t) => t
            .elems
            .iter()
            .try_for_each(|t| named_by_type(t, params, found)),
        syn::Type::BareFn(f) => {
            for input in &f.inputs {
                named_by_type(&input.ty, params, found)?;
            }
            match &f.output {
                syn::ReturnType::Type(_, output) => named_by_type(output, params, found),
                syn::ReturnType::Default => Some(()),
            }
        }
        syn::Type::TraitObject(t) => named_by_bounds_of(&t.bounds, params, found),
        syn::Type::ImplTrait(t) => named_by_bounds_of(&t.bounds, params, found),
        syn::Type::Never(_) => Some(()),
        // A macro, or tokens syn does not parse: what they expand to, the
        // reader cannot say.
        _ => None,
    }
}

fn named_by_bounds_of<'b>(
    bounds: impl IntoIterator<Item = &'b syn::TypeParamBound>,
    params: &[String],
    found: &mut HashSet<String>,
) -> Option<()> {
    for bound in bounds {
        if let syn::TypeParamBound::Trait(t) = bound {
            named_by_path(&t.path, params, found)?;
        }
    }
    Some(())
}

fn named_by_path(path: &syn::Path, params: &[String], found: &mut HashSet<String>) -> Option<()> {
    if path
        .segments
        .last()
        .is_some_and(|s| s.ident == "PhantomData")
    {
        return Some(());
    }
    if path.leading_colon.is_none()
        && let Some(first) = path.segments.first()
        && params.contains(&first.ident.unraw().to_string())
    {
        if path.segments.len() > 1 {
            // `T::Out`: serde bounds the associated type, not `T`.
            return None;
        }
        found.insert(first.ident.unraw().to_string());
    }
    for segment in &path.segments {
        match &segment.arguments {
            syn::PathArguments::AngleBracketed(args) => {
                for arg in &args.args {
                    match arg {
                        syn::GenericArgument::Type(t) => named_by_type(t, params, found)?,
                        syn::GenericArgument::AssocType(a) => named_by_type(&a.ty, params, found)?,
                        _ => {}
                    }
                }
            }
            syn::PathArguments::Parenthesized(args) => {
                for input in &args.inputs {
                    named_by_type(input, params, found)?;
                }
                if let syn::ReturnType::Type(_, output) = &args.output {
                    named_by_type(output, params, found)?;
                }
            }
            syn::PathArguments::None => {}
        }
    }
    Some(())
}

/// Whether `meta` is `#[serde(..)]` with the word `word` in it.
fn serde_word(meta: &Meta, word: &str) -> bool {
    match meta {
        Meta::List(list) if list.path.is_ident("serde") => list
            .tokens
            .clone()
            .into_iter()
            .any(|t| matches!(t, proc_macro2::TokenTree::Ident(i) if i == word)),
        _ => false,
    }
}

/// Whether `tokens` hold the identifier `name`.
fn names(tokens: proc_macro2::TokenStream, name: &str) -> bool {
    tokens.into_iter().any(|t| match t {
        proc_macro2::TokenTree::Ident(i) => i == name,
        proc_macro2::TokenTree::Group(g) => names(g.stream(), name),
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARAMS: &[&str] = &["P", "B"];

    /// What a field of type `ty` bounds; none when the reader cannot say.
    fn by_type(ty: &str) -> Option<Vec<String>> {
        let params: Vec<String> = PARAMS.iter().map(|p| p.to_string()).collect();
        let mut found = HashSet::new();
        named_by_type(&syn::parse_str(ty).expect("a type"), &params, &mut found)?;
        let mut found: Vec<String> = found.into_iter().collect();
        found.sort();
        Some(found)
    }

    /// What a bound for `side` bounds, each parameter by its name, and by
    /// `name: Default` for `Default`; the outer none when there is no bound
    /// for `side`, the inner when the reader cannot judge it.
    fn by_attribute(attribute: &str, side: Side) -> Option<Option<Vec<String>>> {
        let params: Vec<String> = PARAMS.iter().map(|p| p.to_string()).collect();
        let meta: Meta = syn::parse_str(attribute).expect("an attribute");
        let bounds = serde_bounds(std::iter::once(&meta), side)?;
        let (mut wire, mut default) = (HashSet::new(), HashSet::new());
        // The trait by its name alone: what a path leads to, these
        // tests do not read.
        let mut known = |path: &syn::Path| match path.segments.last()?.ident.to_string().as_str() {
            "Serialize" => Some(Known::Serialize),
            "DeserializeOwned" => Some(Known::DeserializeOwned),
            "Default" => Some(Known::Default),
            _ => None,
        };
        if named_by_bounds(&bounds, &params, &mut wire, &mut default, &mut known).is_none() {
            return Some(None);
        }
        let mut found: Vec<String> = wire
            .into_iter()
            .chain(default.into_iter().map(|p| format!("{p}: Default")))
            .collect();
        found.sort();
        Some(Some(found))
    }

    /// As serde's inference: a parameter at any depth, but not inside a
    /// `PhantomData` by any path; and an associated type of a parameter
    /// is beyond the reader.
    #[test]
    fn a_field_type_bounds_the_parameters_serde_infers() {
        let named = |names: &[&str]| Some(names.iter().map(|n| n.to_string()).collect());
        assert_eq!(by_type("Vec<(P, [B; 2])>"), named(&["B", "P"]));
        assert_eq!(by_type("PhantomData<fn() -> B>"), named(&[]));
        assert_eq!(by_type("(P, std::marker::PhantomData<B>)"), named(&["P"]));
        assert_eq!(by_type("Box<dyn Fn(B) -> P>"), named(&["B", "P"]));
        // An associated type of a parameter, which serde bounds: the reader
        // cannot judge it.
        assert_eq!(by_type("P::Output"), None);
        assert_eq!(by_type("Vec<<B as Iterator>::Item>"), None);
        assert_eq!(by_type("PhantomData<P::Output>"), named(&[]));
        assert_eq!(by_type("Vec<holder!(P)>"), None);
    }

    /// A bound names the parameters it bounds, in the direction it is
    /// for: `bound = ".."` in both, `bound(deserialize = "..")` in one, and
    /// none in the other; by serde's traits, or by `Default`. One the
    /// reader cannot judge, on another trait, on a type that is not a
    /// parameter but names one, or that it cannot read, it does not judge.
    #[test]
    fn a_serde_bound_names_what_it_bounds_in_its_direction() {
        use Side::{Deserialize as De, Serialize as Ser};
        let named = |names: &[&str]| Some(Some(names.iter().map(|n| n.to_string()).collect()));
        for side in [Ser, De] {
            assert_eq!(by_attribute(r#"serde(bound = "")"#, side), named(&[]));
            assert_eq!(
                by_attribute(
                    r#"serde(bound(serialize = "P: Serialize", deserialize = "P: DeserializeOwned"))"#,
                    side
                ),
                named(&["P"])
            );
            assert_eq!(
                by_attribute(r#"serde(bound = "P: Default, u8: Serialize")"#, side),
                named(&["P: Default"])
            );
            for unjudged in [
                r#"serde(rename = "x", bound = "B: Clone")"#,
                r#"serde(bound = "Vec<B>: Serialize")"#,
                r#"serde(bound = "P::Out: Serialize")"#,
                "serde(bound = 3)",
            ] {
                assert_eq!(by_attribute(unjudged, side), Some(None), "{unjudged}");
            }
            assert_eq!(by_attribute("serde(skip)", side), None);
        }
        let deserialize = r#"serde(bound(deserialize = "P: DeserializeOwned"))"#;
        assert_eq!(by_attribute(deserialize, Ser), None);
        assert_eq!(by_attribute(deserialize, De), named(&["P"]));
        let serialize = r#"serde(bound(serialize = ""))"#;
        assert_eq!(by_attribute(serialize, Ser), named(&[]));
        assert_eq!(by_attribute(serialize, De), None);
    }
}
