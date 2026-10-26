//! A crate's module tree, read from its source: the items the reader needs
//! in each module, with `cfg` applied.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use syn::ext::IdentExt as _;
use syn::spanned::Spanned as _;
use syn::{Item, Meta, UseTree};

use crate::cfg::Settings;
use crate::reads::Counted;

/// One crate: its modules, the root first.
pub(crate) struct Crate {
    /// The package it is the library of.
    pub(crate) package: usize,
    /// Its crate name, as Rust code names it.
    pub(crate) name: String,
    pub(crate) modules: Vec<Module>,
}

/// A module. Its path from the crate root is not stored: a copy per module
/// costs the depth of the tree times the length of its paths.
pub(crate) struct Module {
    pub(crate) parent: Option<usize>,
    /// Its name in its parent; the root's is empty.
    pub(crate) name: String,
    /// Who can see it from outside its parent.
    pub(crate) vis: Vis,
    pub(crate) children: Counted<BTreeMap<String, usize>>,
    pub(crate) types: Counted<Vec<TypeDef>>,
    pub(crate) uses: Counted<Vec<Use>>,
    pub(crate) impls: Counted<Vec<Impl>>,
    /// The types of each name, and the uses that bind it, by index in
    /// order: a lookup finds a name without scanning the module, which
    /// made resolving every name of a module cost the square of its size.
    types_named: Counted<HashMap<String, Vec<usize>>>,
    uses_named: Counted<HashMap<String, Vec<usize>>>,
    /// The uses that are globs, in order.
    globs: Counted<Vec<usize>>,
    /// The names of the traits and trait aliases it declares: a local
    /// `trait Default` hides the prelude's.
    others: Counted<HashSet<String>>,
    /// The traits it declares, which a path can resolve to, as a cell
    /// policy's trait does.
    pub(crate) traits: Counted<Vec<TraitDef>>,
    traits_named: Counted<HashMap<String, Vec<usize>>>,
}

/// A trait a module declares: its name and visibility, all a path to it
/// needs.
#[derive(Clone)]
pub(crate) struct TraitDef {
    pub(crate) name: String,
    pub(crate) vis: Vis,
}

/// A struct, an enum, a union, or a type alias.
#[derive(Clone)]
pub(crate) struct TypeDef {
    pub(crate) fields: Option<syn::Fields>,
    /// Every field of a generic struct, or of a generic enum's variants,
    /// with `cfg` applied: what serde bounds its parameters by. Empty for a
    /// type without type parameters, which has none to bound.
    pub(crate) members: Vec<Member>,
    /// The attributes of each of a generic enum's variants that `cfg`
    /// keeps, with `cfg_attr` applied: a variant's serde bound and skip
    /// hold for its fields.
    pub(crate) variants: Vec<Vec<Meta>>,
    /// Its type parameters, by name; a lifetime or const parameter is
    /// none of them.
    pub(crate) params: Vec<String>,
    /// Whether it has a lifetime or const parameter.
    pub(crate) other_params: bool,
    /// An alias's type; none for a struct, an enum, or a union.
    pub(crate) alias: Option<syn::Type>,
    pub(crate) name: String,
    pub(crate) vis: Vis,
    pub(crate) attrs: Vec<Meta>,
    /// Whether a field borrows: a reference in its type, or
    /// `#[serde(borrow)]`. Derived `Deserialize` then is not
    /// `DeserializeOwned`, and the type has no codec.
    pub(crate) borrows: bool,
    pub(crate) at: Location,
}

/// A field of a struct, or of an enum's variant.
#[derive(Clone)]
pub(crate) struct Member {
    pub(crate) field: syn::Field,
    /// Its variant's index in [`TypeDef::variants`]; none in a struct.
    pub(crate) variant: Option<usize>,
}

/// Who can see an item: anyone, its crate (`pub(crate)`, `pub(super)`,
/// `pub(in ..)`, read as the crate), or its module and the modules in it.
/// A glob imports only what its module can see.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Vis {
    Public,
    Crate,
    Private,
}

impl Vis {
    fn of(vis: &syn::Visibility) -> Self {
        match vis {
            syn::Visibility::Public(_) => Self::Public,
            syn::Visibility::Restricted(r) if r.path.is_ident("self") => Self::Private,
            syn::Visibility::Restricted(_) => Self::Crate,
            syn::Visibility::Inherited => Self::Private,
        }
    }
}

/// One name a `use` (or an `extern crate`) brings in, or one glob.
pub(crate) struct Use {
    /// The name it binds; `None` for a glob.
    pub(crate) name: Option<String>,
    pub(crate) vis: Vis,
    /// The path it imports, or the module a glob imports from.
    pub(crate) path: UsePath,
    /// Whether the path starts with `::`: an external crate.
    pub(crate) absolute: bool,
    /// The attributes of its `use` item, shared by every name the item
    /// binds: a copy per name costs names times attributes.
    pub(crate) attrs: Rc<[Meta]>,
    pub(crate) at: Location,
}

/// A path a `use` imports: its last segment, linked to the ones before
/// it. The names in one pair of braces share everything before the braces,
/// and a path inside them adds one link: a copy of the shared part per
/// name, or per nested group, costs names times path length.
#[derive(Clone)]
pub(crate) struct UsePath {
    last: Option<Rc<Link<String>>>,
}

impl UsePath {
    /// A path of these segments.
    fn of(segments: Vec<String>) -> Self {
        segments
            .into_iter()
            .fold(Self { last: None }, |path, segment| path.push(segment))
    }

    fn push(&self, segment: String) -> Self {
        Self {
            last: Some(Link::push(self.last.as_ref(), segment)),
        }
    }

    /// Its segments, first to last.
    pub(crate) fn segments(&self) -> Vec<&str> {
        let mut segments: Vec<&str> = Link::values(self.last.as_deref())
            .map(String::as_str)
            .collect();
        segments.reverse();
        segments
    }

    pub(crate) fn last(&self) -> Option<&str> {
        self.last.as_deref().map(|link| link.value.as_str())
    }
}

/// A value linked to the ones before it, so that what many chains share is
/// stored once.
struct Link<T> {
    value: T,
    up: Option<Rc<Link<T>>>,
}

impl<T> Link<T> {
    fn push(up: Option<&Rc<Self>>, value: T) -> Rc<Self> {
        Rc::new(Self {
            value,
            up: up.cloned(),
        })
    }

    /// The values of the chain that ends at `last`, last first.
    fn values(last: Option<&Self>) -> impl Iterator<Item = &T> {
        std::iter::successors(last, |link| link.up.as_deref()).map(|link| &link.value)
    }
}

impl<T> Drop for Link<T> {
    // Unlinked one at a time: a chain dropped recursively takes a stack
    // frame per link, and the source decides how long a chain is.
    fn drop(&mut self) {
        let mut up = self.up.take();
        while let Some(link) = up {
            up = match Rc::try_unwrap(link) {
                Ok(mut link) => link.up.take(),
                Err(_) => None,
            };
        }
    }
}

/// A directory, as the chain of module names and `#[path]`s that leads to
/// it from a file's directory. It is made a path only when a module file
/// is looked up under it: a path per inline module costs depth times path
/// length.
#[derive(Clone)]
struct Dir(Rc<Link<PathBuf>>);

impl Dir {
    fn at(path: PathBuf) -> Self {
        Self(Link::push(None, path))
    }

    fn join(&self, part: impl Into<PathBuf>) -> Self {
        Self(Link::push(Some(&self.0), part.into()))
    }

    fn path(&self) -> PathBuf {
        let mut parts: Vec<&PathBuf> = Link::values(Some(&self.0)).collect();
        parts.reverse();
        parts.into_iter().collect()
    }
}

pub(crate) struct Impl {
    /// Its generics, with their bounds: its type parameters, and what a
    /// serde impl written by hand bounds.
    pub(crate) generics: syn::Generics,
    pub(crate) trait_path: Option<syn::Path>,
    pub(crate) self_ty: syn::Type,
    /// Its associated types: name, type, and where.
    pub(crate) types: Vec<(String, syn::Type, Location)>,
}

impl Impl {
    /// Its type parameters, by name.
    pub(crate) fn params(&self) -> Vec<String> {
        params(&self.generics)
    }
}

/// A place in a source file.
#[derive(Clone)]
pub(crate) struct Location {
    /// Shared by every location in the file: a copy per item costs items
    /// times the length of the file's path.
    pub(crate) file: Rc<Path>,
    pub(crate) line: usize,
    pub(crate) column: usize,
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.file.display(),
            self.line,
            self.column + 1
        )
    }
}

impl Crate {
    /// Read the crate whose root file is `root`, and whose files are all
    /// under `within` and, with every other crate of the extraction, within
    /// `budget`. Files that do not parse, cannot be found, are elsewhere,
    /// or are on their own module path are problems; the rest is read.
    pub(crate) fn read(
        package: usize,
        name: &str,
        root: &Path,
        within: &Path,
        settings: &Settings,
        features: &[String],
        ledger: Ledger<'_>,
    ) -> Self {
        let mut krate = Self {
            package,
            name: name.to_string(),
            modules: Vec::new(),
        };
        let mut reader = Reader {
            settings,
            features,
            ledger,
            within: std::fs::canonicalize(within).unwrap_or_else(|_| within.to_path_buf()),
            open: Vec::new(),
        };
        krate
            .modules
            .push(Module::empty(None, String::new(), Vis::Public));
        let dir = Dir::at(root.parent().unwrap_or(Path::new("")).to_path_buf());
        if let Some(items) = reader.file(root) {
            reader.items(&mut krate, 0, &items, root, &dir, &dir);
        }
        krate
    }
}

impl Module {
    fn empty(parent: Option<usize>, name: String, vis: Vis) -> Self {
        Self {
            parent,
            name,
            vis,
            children: Default::default(),
            types: Default::default(),
            uses: Default::default(),
            impls: Default::default(),
            types_named: Default::default(),
            uses_named: Default::default(),
            globs: Default::default(),
            others: Default::default(),
            traits: Default::default(),
            traits_named: Default::default(),
        }
    }

    /// Whether it declares an item named `name` in the type namespace: a
    /// type, a module, or a trait. A `use`, `extern crate` among them, is
    /// not a declaration: [`Module::uses_named`].
    pub(crate) fn declares(&self, name: &str) -> bool {
        !self.types_named(name).is_empty()
            || self.children.contains_key(name)
            || self.others.contains(name)
    }

    /// The types named `name`, in order.
    pub(crate) fn types_named(&self, name: &str) -> &[usize] {
        self.types_named.get(name).map_or(&[], Vec::as_slice)
    }

    /// The traits named `name`, in order.
    pub(crate) fn traits_named(&self, name: &str) -> &[usize] {
        self.traits_named.get(name).map_or(&[], Vec::as_slice)
    }

    /// The uses that bind `name`, in order.
    pub(crate) fn uses_named(&self, name: &str) -> &[usize] {
        self.uses_named.get(name).map_or(&[], Vec::as_slice)
    }

    /// The uses that are globs, in order.
    pub(crate) fn globs(&self) -> &Counted<Vec<usize>> {
        &self.globs
    }

    fn add_type(&mut self, ty: TypeDef) {
        self.types_named
            .entry(ty.name.clone())
            .or_default()
            .push(self.types.len());
        self.types.push(ty);
    }

    fn add_use(&mut self, u: Use) {
        match &u.name {
            Some(name) => self
                .uses_named
                .entry(name.clone())
                .or_default()
                .push(self.uses.len()),
            None => self.globs.push(self.uses.len()),
        }
        self.uses.push(u);
    }
}

/// What one extraction shares across every crate it reads: the problems
/// met, and what it may still read, in bytes. Many files backed by one
/// blob cost a repository nothing and the reader everything, and a chain
/// of files each naming the next twice doubles at every level, so the
/// budget is for the extraction, not the crate.
pub(crate) struct Ledger<'a> {
    pub(crate) problems: &'a mut Vec<String>,
    pub(crate) budget: &'a mut u64,
}

struct Reader<'a> {
    settings: &'a Settings,
    features: &'a [String],
    ledger: Ledger<'a>,
    /// The directory the crate's files must be under: a crate read from a
    /// registry or a Git checkout may not name a file elsewhere through
    /// `#[path]`, and a device or a file of any size is not a module.
    within: PathBuf,
    /// The files whose items are being read, outermost first: a `#[path]`
    /// that names one of them is a cycle. A file named twice as two
    /// modules is read twice, as rustc reads it; the budget bounds that.
    open: Vec<PathBuf>,
}

/// The most a module file may hold. Generated bindings run to megabytes;
/// nothing a person wrote runs to sixteen.
const FILE_LIMIT: u64 = 16 << 20;

/// The most one extraction reads in all. The reader's peak is about a
/// hundred and twenty times a source byte on a crate of messages
/// (see docs/src/operations/reader-cost.md), so this bounds what it stores near
/// two gigabytes. Work that repeats a resolution chain per query is not
/// bounded by it; the composer's limits on the process that reads an
/// import bound that.
pub(crate) const TOTAL_LIMIT: u64 = 16 << 20;

/// The least a file costs the budget: a chain of tiny files is bounded by
/// their number, not their bytes.
const LEAST_CHARGE: u64 = 4 << 10;

impl Reader<'_> {
    fn file(&mut self, path: &Path) -> Option<Vec<Item>> {
        let canonical = match std::fs::canonicalize(path) {
            Ok(canonical) => canonical,
            Err(e) => {
                self.ledger
                    .problems
                    .push(format!("{}: cannot be read: {e}", path.display()));
                return None;
            }
        };
        if !canonical.starts_with(&self.within) {
            self.ledger.problems.push(format!(
                "{}: outside the package's directory {}; not read",
                path.display(),
                self.within.display()
            ));
            return None;
        }
        if self.open.contains(&canonical) {
            self.ledger.problems.push(format!(
                "{}: a module file on its own module path; the `#[path]` cycle is not read",
                path.display()
            ));
            return None;
        }
        match std::fs::metadata(&canonical) {
            Ok(meta) if !meta.is_file() => {
                self.ledger
                    .problems
                    .push(format!("{}: not a regular file", path.display()));
                return None;
            }
            Ok(meta) if meta.len() > FILE_LIMIT => {
                self.ledger.problems.push(format!(
                    "{}: larger than {} MiB; not read",
                    path.display(),
                    FILE_LIMIT >> 20
                ));
                return None;
            }
            Ok(meta) => {
                let charge = meta.len().max(LEAST_CHARGE);
                if charge > *self.ledger.budget {
                    self.ledger.problems.push(format!(
                        "{}: the files read for this extraction exceed {} MiB in all; not read",
                        path.display(),
                        TOTAL_LIMIT >> 20
                    ));
                    return None;
                }
                *self.ledger.budget -= charge;
            }
            Err(e) => {
                self.ledger
                    .problems
                    .push(format!("{}: cannot be read: {e}", path.display()));
                return None;
            }
        }
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) => {
                self.ledger
                    .problems
                    .push(format!("{}: cannot be read: {e}", path.display()));
                return None;
            }
        };
        match syn::parse_file(&text) {
            // A file's own `#![cfg]` can remove all of it.
            Ok(file) => Some(match self.settings.attributes(&file.attrs, self.features) {
                Some(_) => file.items,
                None => Vec::new(),
            }),
            Err(e) => {
                let start = e.span().start();
                self.ledger.problems.push(format!(
                    "{}:{}:{}: does not parse: {e}",
                    path.display(),
                    start.line,
                    start.column + 1
                ));
                None
            }
        }
    }

    /// Whether a field borrows when deserialized: a reference anywhere in
    /// its type, or `#[serde(borrow)]`, unless serde skips deserializing it
    /// or `cfg` removes it. `cfg_attr` applies first.
    fn borrows(&self, field: &syn::Field) -> bool {
        let Some(attrs) = self.settings.attributes(&field.attrs, self.features) else {
            return false;
        };
        let serde = |word: &str| {
            attrs.iter().any(|meta| match meta {
                Meta::List(list) if list.path.is_ident("serde") => list
                    .tokens
                    .clone()
                    .into_iter()
                    .any(|t| matches!(t, proc_macro2::TokenTree::Ident(i) if i == word)),
                _ => false,
            })
        };
        if serde("skip") || serde("skip_deserializing") {
            return false;
        }
        serde("borrow") || reference(&field.ty)
    }

    /// The fields `cfg` keeps, with `cfg_attr` applied to their attributes,
    /// of variant `variant`, or of a struct.
    fn members(&self, fields: &syn::Fields, variant: Option<usize>) -> Vec<Member> {
        fields
            .iter()
            .filter_map(|f| {
                let attrs = self.settings.attributes(&f.attrs, self.features)?;
                let mut field = f.clone();
                field.attrs = attrs
                    .into_iter()
                    .map(|meta| syn::parse_quote!(#[#meta]))
                    .collect();
                Some(Member { field, variant })
            })
            .collect()
    }

    /// Read `items` of module `module`, which is in `file`; its child
    /// modules' files are under `dir`, and a `#[path]` on one is relative
    /// to `base`: the file's directory at the top of a file, and `dir` in
    /// an inline module.
    fn items(
        &mut self,
        krate: &mut Crate,
        module: usize,
        items: &[Item],
        file: &Path,
        dir: &Dir,
        base: &Dir,
    ) {
        // The file is open while its items are read, including those of
        // the modules it holds inline, which `items_of` reads without
        // opening it again.
        self.open
            .push(std::fs::canonicalize(file).unwrap_or_else(|_| file.to_path_buf()));
        self.items_of(krate, module, items, &file.into(), dir, base);
        self.open.pop();
    }

    fn items_of(
        &mut self,
        krate: &mut Crate,
        module: usize,
        items: &[Item],
        file: &Rc<Path>,
        dir: &Dir,
        base: &Dir,
    ) {
        let at = |span: proc_macro2::Span| {
            let start = span.start();
            Location {
                file: Rc::clone(file),
                line: start.line,
                column: start.column,
            }
        };
        for item in items {
            let Some(attrs) = self.settings.attributes(item_attrs(item), self.features) else {
                continue;
            };
            match item {
                Item::Struct(s) => {
                    let params = params(&s.generics);
                    let members = if params.is_empty() {
                        Vec::new()
                    } else {
                        self.members(&s.fields, None)
                    };
                    krate.modules[module].add_type(TypeDef {
                        fields: Some(s.fields.clone()),
                        members,
                        variants: Vec::new(),
                        params,
                        other_params: other_params(&s.generics),
                        alias: None,
                        name: s.ident.unraw().to_string(),
                        vis: Vis::of(&s.vis),
                        attrs,
                        borrows: s.fields.iter().any(|f| self.borrows(f)),
                        at: at(s.ident.span()),
                    })
                }
                Item::Enum(e) => {
                    let kept: Vec<(Vec<Meta>, &syn::Fields)> = e
                        .variants
                        .iter()
                        .filter_map(|v| {
                            Some((
                                self.settings.attributes(&v.attrs, self.features)?,
                                &v.fields,
                            ))
                        })
                        .collect();
                    // A variant `cfg` removes has no fields.
                    let borrows = kept
                        .iter()
                        .flat_map(|(_, fields)| fields.iter())
                        .any(|f| self.borrows(f));
                    let params = params(&e.generics);
                    let (members, variants) = if params.is_empty() {
                        (Vec::new(), Vec::new())
                    } else {
                        let members = kept
                            .iter()
                            .enumerate()
                            .flat_map(|(index, (_, fields))| self.members(fields, Some(index)))
                            .collect();
                        (members, kept.into_iter().map(|(attrs, _)| attrs).collect())
                    };
                    krate.modules[module].add_type(TypeDef {
                        fields: None,
                        members,
                        variants,
                        params,
                        other_params: other_params(&e.generics),
                        alias: None,
                        name: e.ident.unraw().to_string(),
                        vis: Vis::of(&e.vis),
                        attrs,
                        borrows,
                        at: at(e.ident.span()),
                    })
                }
                Item::Union(u) => krate.modules[module].add_type(TypeDef {
                    fields: None,
                    members: Vec::new(),
                    variants: Vec::new(),
                    params: params(&u.generics),
                    other_params: other_params(&u.generics),
                    alias: None,
                    name: u.ident.unraw().to_string(),
                    vis: Vis::of(&u.vis),
                    attrs,
                    borrows: u.fields.named.iter().any(|f| self.borrows(f)),
                    at: at(u.ident.span()),
                }),
                Item::Type(t) => krate.modules[module].add_type(TypeDef {
                    fields: None,
                    members: Vec::new(),
                    variants: Vec::new(),
                    params: params(&t.generics),
                    other_params: other_params(&t.generics),
                    alias: Some((*t.ty).clone()),
                    name: t.ident.unraw().to_string(),
                    vis: Vis::of(&t.vis),
                    attrs,
                    borrows: false,
                    at: at(t.ident.span()),
                }),
                Item::Use(u) => {
                    let mut uses = Vec::new();
                    flatten(&u.tree, &UsePath::of(Vec::new()), &mut uses);
                    let attrs: Rc<[Meta]> = attrs.into();
                    // Once per item: `span` turns the whole item into tokens.
                    let at = at(u.span());
                    for (name, path) in uses {
                        krate.modules[module].add_use(Use {
                            name,
                            vis: Vis::of(&u.vis),
                            path,
                            absolute: u.leading_colon.is_some(),
                            attrs: attrs.clone(),
                            at: at.clone(),
                        });
                    }
                }
                Item::ExternCrate(e) => {
                    let name = e
                        .rename
                        .as_ref()
                        .map_or(&e.ident, |(_, alias)| alias)
                        .unraw()
                        .to_string();
                    // `extern crate self as name` names this crate.
                    let (path, absolute) = if e.ident == "self" {
                        (vec!["crate".to_string()], false)
                    } else {
                        (vec![e.ident.unraw().to_string()], true)
                    };
                    krate.modules[module].add_use(Use {
                        name: Some(name),
                        vis: Vis::of(&e.vis),
                        path: UsePath::of(path),
                        absolute,
                        attrs: attrs.into(),
                        at: at(e.span()),
                    });
                }
                Item::Impl(i) => {
                    let types = i
                        .items
                        .iter()
                        .filter_map(|item| match item {
                            syn::ImplItem::Type(t) => {
                                // Of two `type In` under exclusive `cfg`,
                                // the one that holds.
                                self.settings.attributes(&t.attrs, self.features)?;
                                Some((t.ident.unraw().to_string(), t.ty.clone(), at(t.ty.span())))
                            }
                            _ => None,
                        })
                        .collect();
                    krate.modules[module].impls.push(Impl {
                        generics: i.generics.clone(),
                        trait_path: i.trait_.as_ref().map(|(_, path, _)| path.clone()),
                        self_ty: (*i.self_ty).clone(),
                        types,
                    });
                }
                Item::Mod(m) => {
                    let name = m.ident.unraw().to_string();
                    let child = krate.modules.len();
                    krate
                        .modules
                        .push(Module::empty(Some(module), name.clone(), Vis::of(&m.vis)));
                    krate.modules[module].children.insert(name.clone(), child);
                    let explicit = attrs.iter().find_map(|meta| match meta {
                        Meta::NameValue(nv) if nv.path.is_ident("path") => match &nv.value {
                            syn::Expr::Lit(syn::ExprLit {
                                lit: syn::Lit::Str(s),
                                ..
                            }) => Some(s.value()),
                            _ => None,
                        },
                        _ => None,
                    });
                    match (&m.content, explicit) {
                        (Some((_, inner)), explicit) => {
                            // Its own `#[path]` is relative to `base`, as a
                            // module file's is; its name, to `dir`.
                            let inner_dir = match explicit {
                                Some(explicit) => base.join(explicit),
                                None => dir.join(name),
                            };
                            self.items_of(krate, child, inner, file, &inner_dir, &inner_dir);
                        }
                        (None, Some(explicit)) => {
                            let target = base.path().join(explicit);
                            let child_dir = Dir::at(
                                target
                                    .parent()
                                    .map_or_else(|| dir.path(), Path::to_path_buf),
                            );
                            if let Some(items) = self.file(&target) {
                                self.items(krate, child, &items, &target, &child_dir, &child_dir);
                            }
                        }
                        (None, None) => {
                            let path = dir.path();
                            let flat = path.join(format!("{name}.rs"));
                            let nested = path.join(&name).join("mod.rs");
                            let (target, base) = if flat.exists() {
                                (flat, Dir::at(path))
                            } else {
                                (nested, Dir::at(path.join(&name)))
                            };
                            if let Some(items) = self.file(&target) {
                                self.items(krate, child, &items, &target, &dir.join(&name), &base);
                            }
                        }
                    }
                }
                Item::Trait(t) => {
                    let name = t.ident.unraw().to_string();
                    let m = &mut krate.modules[module];
                    m.others.insert(name.clone());
                    m.traits_named
                        .entry(name.clone())
                        .or_default()
                        .push(m.traits.len());
                    m.traits.push(TraitDef {
                        name,
                        vis: Vis::of(&t.vis),
                    });
                }
                Item::TraitAlias(t) => {
                    krate.modules[module]
                        .others
                        .insert(t.ident.unraw().to_string());
                }
                _ => {}
            }
        }
    }
}

fn item_attrs(item: &Item) -> &[syn::Attribute] {
    match item {
        Item::Struct(i) => &i.attrs,
        Item::Enum(i) => &i.attrs,
        Item::Union(i) => &i.attrs,
        Item::Type(i) => &i.attrs,
        Item::Use(i) => &i.attrs,
        Item::ExternCrate(i) => &i.attrs,
        Item::Impl(i) => &i.attrs,
        Item::Mod(i) => &i.attrs,
        Item::Trait(i) => &i.attrs,
        Item::TraitAlias(i) => &i.attrs,
        _ => &[],
    }
}

/// The names of a declaration's type parameters.
fn params(generics: &syn::Generics) -> Vec<String> {
    generics
        .type_params()
        .map(|p| p.ident.unraw().to_string())
        .collect()
}

/// Whether a declaration has a lifetime or const parameter.
fn other_params(generics: &syn::Generics) -> bool {
    generics
        .params
        .iter()
        .any(|p| !matches!(p, syn::GenericParam::Type(_)))
}

/// Each name a use tree binds, with its full path; a glob as `None`.
fn flatten(tree: &UseTree, prefix: &UsePath, out: &mut Vec<(Option<String>, UsePath)>) {
    match tree {
        UseTree::Path(p) => flatten(&p.tree, &prefix.push(p.ident.unraw().to_string()), out),
        UseTree::Name(n) => {
            let name = n.ident.unraw().to_string();
            if name == "self" {
                if let Some(last) = prefix.last() {
                    out.push((Some(last.to_string()), prefix.clone()));
                }
            } else {
                out.push((Some(name.clone()), prefix.push(name)));
            }
        }
        UseTree::Rename(r) => {
            let name = r.ident.unraw().to_string();
            let path = if name == "self" {
                prefix.clone()
            } else {
                prefix.push(name)
            };
            let alias = r.rename.unraw().to_string();
            if alias != "_" {
                out.push((Some(alias), path));
            }
        }
        UseTree::Glob(_) => out.push((None, prefix.clone())),
        UseTree::Group(g) => {
            for tree in &g.items {
                flatten(tree, prefix, out);
            }
        }
    }
}

fn reference(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Reference(_) => true,
        syn::Type::Array(a) => reference(&a.elem),
        syn::Type::Slice(s) => reference(&s.elem),
        syn::Type::Group(g) => reference(&g.elem),
        syn::Type::Paren(p) => reference(&p.elem),
        syn::Type::Tuple(t) => t.elems.iter().any(reference),
        syn::Type::Path(p) => p.path.segments.iter().any(|s| match &s.arguments {
            syn::PathArguments::AngleBracketed(args) => args.args.iter().any(|a| match a {
                syn::GenericArgument::Type(t) => reference(t),
                _ => false,
            }),
            _ => false,
        }),
        _ => false,
    }
}
