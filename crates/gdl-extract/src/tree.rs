//! A crate's module tree, read from its source: the items the reader needs
//! in each module, with `cfg` applied.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use syn::ext::IdentExt as _;
use syn::spanned::Spanned as _;
use syn::{Item, Meta, UseTree};

use crate::cfg::Settings;

/// One crate: its modules, the root first.
pub(crate) struct Crate {
    /// The package it is the library of.
    pub(crate) package: usize,
    /// Its crate name, as Rust code names it.
    pub(crate) name: String,
    pub(crate) modules: Vec<Module>,
}

pub(crate) struct Module {
    pub(crate) parent: Option<usize>,
    /// Who can see it from outside its parent.
    pub(crate) vis: Vis,
    /// Its names from the crate root, the root's empty.
    pub(crate) path: Vec<String>,
    pub(crate) children: BTreeMap<String, usize>,
    pub(crate) types: Vec<TypeDef>,
    pub(crate) uses: Vec<Use>,
    pub(crate) impls: Vec<Impl>,
}

/// A struct, an enum, or a union.
pub(crate) struct TypeDef {
    pub(crate) fields: Option<syn::Fields>,
    pub(crate) name: String,
    pub(crate) vis: Vis,
    pub(crate) attrs: Vec<Meta>,
    /// Whether a field borrows: a reference in its type, or
    /// `#[serde(borrow)]`. Derived `Deserialize` then is not
    /// `DeserializeOwned`, and the type has no codec.
    pub(crate) borrows: bool,
    pub(crate) at: Location,
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
    pub(crate) path: Vec<String>,
    /// Whether the path starts with `::`: an external crate.
    pub(crate) absolute: bool,
    pub(crate) attrs: Vec<Meta>,
    pub(crate) at: Location,
}

pub(crate) struct Impl {
    pub(crate) trait_path: Option<syn::Path>,
    pub(crate) self_ty: syn::Type,
    /// Its associated types: name, type, and where.
    pub(crate) types: Vec<(String, syn::Type, Location)>,
}

/// A place in a source file.
#[derive(Clone)]
pub(crate) struct Location {
    pub(crate) file: PathBuf,
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
            .push(Module::empty(None, Vec::new(), Vis::Public));
        let dir = root.parent().unwrap_or(Path::new("")).to_path_buf();
        if let Some(items) = reader.file(root) {
            reader.items(&mut krate, 0, &items, root, &dir, &dir);
        }
        krate
    }
}

impl Module {
    fn empty(parent: Option<usize>, path: Vec<String>, vis: Vis) -> Self {
        Self {
            parent,
            vis,
            path,
            children: BTreeMap::new(),
            types: Vec::new(),
            uses: Vec::new(),
            impls: Vec::new(),
        }
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

/// The most one extraction reads in all. The reader keeps about forty
/// times a source byte in memory, so this is well under a gigabyte.
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
        dir: &Path,
        base: &Path,
    ) {
        // The file is open while its items are read, including those of
        // the modules it holds inline.
        self.open
            .push(std::fs::canonicalize(file).unwrap_or_else(|_| file.to_path_buf()));
        self.items_of(krate, module, items, file, dir, base);
        self.open.pop();
    }

    fn items_of(
        &mut self,
        krate: &mut Crate,
        module: usize,
        items: &[Item],
        file: &Path,
        dir: &Path,
        base: &Path,
    ) {
        let at = |span: proc_macro2::Span| {
            let start = span.start();
            Location {
                file: file.to_path_buf(),
                line: start.line,
                column: start.column,
            }
        };
        for item in items {
            let Some(attrs) = self.settings.attributes(item_attrs(item), self.features) else {
                continue;
            };
            match item {
                Item::Struct(s) => krate.modules[module].types.push(TypeDef {
                    fields: Some(s.fields.clone()),
                    name: s.ident.unraw().to_string(),
                    vis: Vis::of(&s.vis),
                    attrs,
                    borrows: s.fields.iter().any(|f| self.borrows(f)),
                    at: at(s.ident.span()),
                }),
                Item::Enum(e) => krate.modules[module].types.push(TypeDef {
                    fields: None,
                    name: e.ident.unraw().to_string(),
                    vis: Vis::of(&e.vis),
                    attrs,
                    borrows: e
                        .variants
                        .iter()
                        // A variant `cfg` removes has no fields.
                        .filter(|v| self.settings.attributes(&v.attrs, self.features).is_some())
                        .flat_map(|v| &v.fields)
                        .any(|f| self.borrows(f)),
                    at: at(e.ident.span()),
                }),
                Item::Union(u) => krate.modules[module].types.push(TypeDef {
                    fields: None,
                    name: u.ident.unraw().to_string(),
                    vis: Vis::of(&u.vis),
                    attrs,
                    borrows: u.fields.named.iter().any(|f| self.borrows(f)),
                    at: at(u.ident.span()),
                }),
                Item::Use(u) => {
                    let mut uses = Vec::new();
                    flatten(&u.tree, &mut Vec::new(), &mut uses);
                    for (name, path) in uses {
                        krate.modules[module].uses.push(Use {
                            name,
                            vis: Vis::of(&u.vis),
                            path,
                            absolute: u.leading_colon.is_some(),
                            attrs: attrs.clone(),
                            at: at(u.span()),
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
                    krate.modules[module].uses.push(Use {
                        name: Some(name),
                        vis: Vis::of(&e.vis),
                        path,
                        absolute,
                        attrs,
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
                        trait_path: i.trait_.as_ref().map(|(_, path, _)| path.clone()),
                        self_ty: (*i.self_ty).clone(),
                        types,
                    });
                }
                Item::Mod(m) => {
                    let name = m.ident.unraw().to_string();
                    let mut path = krate.modules[module].path.clone();
                    path.push(name.clone());
                    let child = krate.modules.len();
                    krate
                        .modules
                        .push(Module::empty(Some(module), path, Vis::of(&m.vis)));
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
                            self.items(krate, child, inner, file, &inner_dir, &inner_dir);
                        }
                        (None, Some(explicit)) => {
                            let target = base.join(explicit);
                            let child_dir = target.parent().unwrap_or(dir).to_path_buf();
                            if let Some(items) = self.file(&target) {
                                self.items(krate, child, &items, &target, &child_dir, &child_dir);
                            }
                        }
                        (None, None) => {
                            let flat = dir.join(format!("{name}.rs"));
                            let nested = dir.join(&name).join("mod.rs");
                            let (target, base) = if flat.exists() {
                                (flat, dir.to_path_buf())
                            } else {
                                (nested, dir.join(&name))
                            };
                            if let Some(items) = self.file(&target) {
                                self.items(krate, child, &items, &target, &dir.join(&name), &base);
                            }
                        }
                    }
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
        Item::Use(i) => &i.attrs,
        Item::ExternCrate(i) => &i.attrs,
        Item::Impl(i) => &i.attrs,
        Item::Mod(i) => &i.attrs,
        _ => &[],
    }
}

/// Each name a use tree binds, with its full path; a glob as `None`.
fn flatten(tree: &UseTree, prefix: &mut Vec<String>, out: &mut Vec<(Option<String>, Vec<String>)>) {
    match tree {
        UseTree::Path(p) => {
            prefix.push(p.ident.unraw().to_string());
            flatten(&p.tree, prefix, out);
            prefix.pop();
        }
        UseTree::Name(n) => {
            let name = n.ident.unraw().to_string();
            if name == "self" {
                if let Some(last) = prefix.last() {
                    out.push((Some(last.clone()), prefix.clone()));
                }
            } else {
                let mut path = prefix.clone();
                path.push(name.clone());
                out.push((Some(name), path));
            }
        }
        UseTree::Rename(r) => {
            let mut path = prefix.clone();
            let name = r.ident.unraw().to_string();
            if name != "self" {
                path.push(name);
            }
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
