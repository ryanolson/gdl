//! What serde's attributes say of what its derive bounds: its bounds, its
//! skips, its defaults, and the attributes the reader reads at all. Each
//! item's `#[serde(..)]` attributes are read once, into a [`Serde`], when
//! the reader reads the item; the wire check reads that.

use std::collections::HashSet;

use proc_macro2::TokenTree;
use quote::ToTokens as _;
use syn::Meta;
use syn::ext::IdentExt as _;

use crate::external::Known;
use crate::tree::{Member, TypeDef, reference};
use crate::wire::need_of;

/// The serde attributes the reader reads for what serde bounds, of a
/// container, a variant, and a field. Any other, such as `from`, `into`,
/// or `remote`, may change what serde bounds, so a type with one is local;
/// a type with no arguments only for `remote` ([`Serde::remote`]).
/// `with` and its kin are among them: serde bounds nothing for such a
/// field, and the reader bounds what its type names, which refuses at
/// worst a crossing that would work.
const CONTAINER: &[&str] = &[
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

/// Where serde attributes stand, which sets the keys the reader reads.
#[derive(Clone, Copy)]
pub(crate) enum Place {
    Container,
    Variant,
    Field,
}

impl Place {
    fn keys(self) -> &'static [&'static str] {
        match self {
            Self::Container => CONTAINER,
            Self::Variant => VARIANT,
            Self::Field => FIELD,
        }
    }
}

/// What the `#[serde(..)]` attributes of one container, variant, or field
/// say, read once. A key is the name that starts an entry, raw or not as
/// written. A word, such as `skip`, counts anywhere at the top of a list,
/// and so do `default` and `bound`, with what follows them. Inside a
/// group, only a direction's bound counts. Only a list named `serde` is
/// read. A word that does not start an entry is not a key only in code
/// that serde's derive rejects.
#[derive(Clone, Default)]
pub(crate) struct Serde {
    /// Whether a key is not one the reader reads in its place.
    pub(crate) unread: bool,
    /// Whether a key is `remote`, which makes no trait for the type
    /// itself.
    pub(crate) remote: bool,
    /// What `default` fills a value with; the last one written wins.
    pub(crate) default: Option<Fill>,
    skip: bool,
    skip_serializing: bool,
    skip_deserializing: bool,
    borrow: bool,
    /// The where-clauses for each direction that `bound = ".."`, for both,
    /// or `bound(serialize = "..", deserialize = "..")`, for each, write;
    /// none when no `bound` is there for the direction. A bound that is
    /// not a string is `None` in the list: the reader cannot say what it
    /// bounds.
    serialize: Option<Vec<Option<String>>>,
    deserialize: Option<Vec<Option<String>>>,
}

impl Serde {
    /// Read the serde attributes among `metas`, which stand at `place`.
    pub(crate) fn read<'m>(metas: impl IntoIterator<Item = &'m Meta>, place: Place) -> Self {
        let mut serde = Self::default();
        for meta in metas {
            let Meta::List(list) = meta else {
                continue;
            };
            if !list.path.is_ident("serde") {
                continue;
            }
            let tokens: Vec<TokenTree> = list.tokens.clone().into_iter().collect();
            let mut start = true;
            for (at, token) in tokens.iter().enumerate() {
                match token {
                    TokenTree::Punct(p) if p.as_char() == ',' => {
                        start = true;
                        continue;
                    }
                    TokenTree::Ident(ident) => {
                        if start {
                            serde.unread |= !place.keys().iter().any(|key| ident == key);
                            serde.remote |= ident == "remote";
                        }
                        serde.word(ident, &tokens[at + 1..]);
                    }
                    _ => {}
                }
                start = false;
            }
        }
        serde
    }

    /// Read word `ident`, at the top of a list, where `after` follows it.
    fn word(&mut self, ident: &proc_macro2::Ident, after: &[TokenTree]) {
        let equals = |token: Option<&TokenTree>| matches!(token, Some(TokenTree::Punct(p)) if p.as_char() == '=');
        if ident == "skip" {
            self.skip = true;
        } else if ident == "skip_serializing" {
            self.skip_serializing = true;
        } else if ident == "skip_deserializing" {
            self.skip_deserializing = true;
        } else if ident == "borrow" {
            self.borrow = true;
        } else if ident == "default" {
            self.default = Some(if equals(after.first()) {
                Fill::Path
            } else {
                Fill::Default
            });
        } else if ident == "bound" {
            match after.first() {
                Some(TokenTree::Group(group)) => {
                    let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                    for (at, token) in inner.iter().enumerate() {
                        let TokenTree::Ident(side) = token else {
                            continue;
                        };
                        if !equals(inner.get(at + 1)) {
                            continue;
                        }
                        let bounds = if side == "serialize" {
                            &mut self.serialize
                        } else if side == "deserialize" {
                            &mut self.deserialize
                        } else {
                            continue;
                        };
                        bounds
                            .get_or_insert_with(Vec::new)
                            .push(string(inner.get(at + 2)));
                    }
                }
                first => {
                    let bound = if equals(first) {
                        string(after.get(1))
                    } else {
                        None
                    };
                    for bounds in [&mut self.serialize, &mut self.deserialize] {
                        bounds.get_or_insert_with(Vec::new).push(bound.clone());
                    }
                }
            }
        }
    }

    /// The where-clauses for `side`, as [`Serde`] keeps them.
    pub(crate) fn bounds(&self, side: Side) -> Option<&[Option<String>]> {
        match side {
            Side::Serialize => self.serialize.as_deref(),
            Side::Deserialize => self.deserialize.as_deref(),
        }
    }

    /// Whether a field's or a variant's attributes skip `side`: `skip`, or
    /// the word for it alone.
    pub(crate) fn skips(&self, side: Side) -> bool {
        self.skip
            || match side {
                Side::Serialize => self.skip_serializing,
                Side::Deserialize => self.skip_deserializing,
            }
    }

    /// Whether a field of type `ty` with these attributes borrows when
    /// deserialized: a reference anywhere in its type, or `borrow`, unless
    /// serde skips deserializing it.
    pub(crate) fn borrows(&self, ty: &syn::Type) -> bool {
        !self.skips(Side::Deserialize) && (self.borrow || reference(ty))
    }
}

/// The string literal `token` is, if it is one.
fn string(token: Option<&TokenTree>) -> Option<String> {
    match token {
        Some(TokenTree::Literal(literal)) => {
            syn::parse2::<syn::LitStr>(TokenTree::Literal(literal.clone()).into())
                .ok()
                .map(|s| s.value())
        }
        _ => None,
    }
}

/// Whether every serde attribute of `definition`, of its variants, and of
/// its fields is one the reader reads.
pub(crate) fn judged(definition: &TypeDef) -> bool {
    !definition.serde.unread
        && definition.variants.iter().all(|variant| !variant.unread)
        && definition.members.iter().all(|member| !member.serde.unread)
}

/// A direction of serde's derive, which builds its where-clause on its
/// own: a bound or a skip may name one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Serialize,
    Deserialize,
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
        if let Some(bounds) = member.serde.bounds(side) {
            named_by_bounds(bounds, params, wire, default, known)?;
        }
    }
    for variant in &definition.variants {
        if let Some(bounds) = variant.bounds(side) {
            named_by_bounds(bounds, params, wire, default, known)?;
        }
    }
    if let Some(bounds) = definition.serde.bounds(side) {
        return named_by_bounds(bounds, params, wire, default, known);
    }
    for member in &definition.members {
        let variant = member.variant.map(|v| &definition.variants[v]);
        if side == Side::Deserialize && fills_by_default(member) {
            named_by_type(&member.ty, params, default)?;
        }
        if member.serde.skips(side)
            || member.serde.bounds(side).is_some()
            || variant.is_some_and(|serde| serde.skips(side) || serde.bounds(side).is_some())
        {
            continue;
        }
        named_by_type(&member.ty, params, wire)?;
    }
    Some(())
}

/// Whether serde fills `member` with `Default::default()` when it
/// deserializes: `#[serde(default)]`, or a field it does not deserialize,
/// without a `default = ".."` of its own.
fn fills_by_default(member: &Member) -> bool {
    match member.serde.default {
        Some(Fill::Default) => true,
        Some(Fill::Path) => false,
        None => member.serde.skips(Side::Deserialize),
    }
}

/// What `#[serde(default)]` fills a value with.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fill {
    /// `Default::default()`: the word alone.
    Default,
    /// A function of its own: `default = ".."`.
    Path,
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
        let serde = Serde::read([&meta], Place::Field);
        let bounds = serde.bounds(side)?;
        let (mut wire, mut default) = (HashSet::new(), HashSet::new());
        // The trait by its name alone: what a path leads to, these
        // tests do not read.
        let mut known = |path: &syn::Path| match path.segments.last()?.ident.to_string().as_str() {
            "Serialize" => Some(Known::Serialize),
            "DeserializeOwned" => Some(Known::DeserializeOwned),
            "Default" => Some(Known::Default),
            _ => None,
        };
        if named_by_bounds(bounds, &params, &mut wire, &mut default, &mut known).is_none() {
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

    /// What the reader reads of these attributes, one item's: the places
    /// whose keys all are ones it reads (`C`ontainer, `V`ariant, `F`ield),
    /// then `remote`, the `default`, each direction's bounds, its skips,
    /// and `borrow`.
    fn summary(attributes: &[&str]) -> String {
        let metas: Vec<Meta> = attributes
            .iter()
            .map(|a| syn::parse_str(a).expect("an attribute"))
            .collect();
        let mut out = String::from("read=");
        for (name, place) in [
            ("C", Place::Container),
            ("V", Place::Variant),
            ("F", Place::Field),
        ] {
            if !Serde::read(&metas, place).unread {
                out.push_str(name);
            }
        }
        let serde = Serde::read(&metas, Place::Field);
        if serde.remote {
            out.push_str(" remote");
        }
        match serde.default {
            Some(Fill::Default) => out.push_str(" default"),
            Some(Fill::Path) => out.push_str(" default=path"),
            None => {}
        }
        for (name, side) in [("ser", Side::Serialize), ("de", Side::Deserialize)] {
            if let Some(bounds) = serde.bounds(side) {
                out.push_str(&format!(" {name}={bounds:?}"));
            }
        }
        for (name, side) in [("ser", Side::Serialize), ("de", Side::Deserialize)] {
            if serde.skips(side) {
                out.push_str(&format!(" skip-{name}"));
            }
        }
        if serde.borrow {
            out.push_str(" borrow");
        }
        out
    }

    /// What the reader reads of `#[serde(..)]`, token by token: a key is
    /// the name that starts an entry, raw or not as written; a word such
    /// as `skip` counts anywhere at the top of the list, and `default` and
    /// `bound` too, by what follows them; nothing inside a group counts
    /// but a direction's bound; and only a list named `serde` is read.
    #[test]
    fn serde_attributes_read_as_they_always_have() {
        for (attributes, read) in [
            (&[r#"serde(rename = "x")"#][..], "read=CVF"),
            (
                &[r#"serde(rename_all = "camelCase", deny_unknown_fields)"#],
                "read=C",
            ),
            (&[r#"serde(r#rename = "x")"#], "read="),
            (
                &[r#"serde(rename(serialize = "a", deserialize = "b"))"#],
                "read=CVF",
            ),
            (&[r#"serde(rename(skip))"#], "read=CVF"),
            (&["serde(rename = skip)"], "read=CVF skip-ser skip-de"),
            (&["serde(rename = borrow)"], "read=CVF borrow"),
            (&["serde(rename = default)"], "read=CVF default"),
            (&[r#"serde(, rename = "x",, ::x)"#], "read=CVF"),
            (&[r#"serde(remote = "Other")"#], "read= remote"),
            (
                &[r#"serde(rename = "x", remote = "Other")"#],
                "read= remote",
            ),
            (&[r#"serde(from = "Other")"#], "read="),
            (&["serde(transparent)"], "read=C"),
            (&["serde(flatten)"], "read=F"),
            (&["serde(other)"], "read=V"),
            (
                &[r#"serde(skip_serializing_if = "Option::is_none")"#],
                "read=F",
            ),
            (&["serde(skip)"], "read=VF skip-ser skip-de"),
            (&["serde(skip_serializing)"], "read=VF skip-ser"),
            (&["serde(skip_deserializing)"], "read=VF skip-de"),
            (&[r#"serde(with = "m", borrow)"#], "read=VF borrow"),
            (&[r#"serde(borrow = "'a")"#], "read=VF borrow"),
            (&["serde(default)"], "read=CF default"),
            (&[r#"serde(default = "f")"#], "read=CF default=path"),
            (
                &[r#"serde(default, default = "f")"#],
                "read=CF default=path",
            ),
            (&[r#"serde(default = "f", default)"#], "read=CF default"),
            (
                &[r#"serde(default = "f")"#, "serde(skip_deserializing)"],
                "read=F default=path skip-de",
            ),
            (&["serde(bound)"], "read=CVF ser=[None] de=[None]"),
            (&["serde(bound())"], "read=CVF"),
            (&["serde(bound = 3)"], "read=CVF ser=[None] de=[None]"),
            (
                &[r#"serde(bound = r"T: Clone")"#],
                r#"read=CVF ser=[Some("T: Clone")] de=[Some("T: Clone")]"#,
            ),
            (
                &[r#"serde(bound(serialize = "A: S"), bound(deserialize = "B: D"))"#],
                r#"read=CVF ser=[Some("A: S")] de=[Some("B: D")]"#,
            ),
            (
                &[
                    r#"serde(bound = "A: S")"#,
                    r#"serde(bound[deserialize = 3])"#,
                ],
                r#"read=CVF ser=[Some("A: S")] de=[Some("A: S"), None]"#,
            ),
            (&["serde(bound(serialize))"], "read=CVF"),
            (&["other(skip, borrow, default, bound)"], "read=CVF"),
            (&["serde"], "read=CVF"),
            (&[r#"serde = "skip""#], "read=CVF"),
            (&["serde::x(skip)"], "read=CVF"),
        ] {
            assert_eq!(summary(attributes), read, "{attributes:?}");
        }
    }
}
