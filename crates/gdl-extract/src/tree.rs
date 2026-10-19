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
    /// Its names from the crate root, the root's empty.
    pub(crate) path: Vec<String>,
    pub(crate) children: BTreeMap<String, usize>,
    pub(crate) types: Vec<TypeDef>,
    pub(crate) uses: Vec<Use>,
    pub(crate) impls: Vec<Impl>,
}

/// A struct, an enum, or a union.
pub(crate) struct TypeDef {
    pub(crate) name: String,
    pub(crate) attrs: Vec<Meta>,
}

/// One name a `use` (or an `extern crate`) brings in, or one glob.
pub(crate) struct Use {
    /// The name it binds; `None` for a glob.
    pub(crate) name: Option<String>,
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
    pub(crate) attrs: Vec<Meta>,
    pub(crate) at: Location,
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
    /// Read the crate whose root file is `root`. Files that do not parse or
    /// cannot be found are problems; the rest is read.
    pub(crate) fn read(
        package: usize,
        name: &str,
        root: &Path,
        settings: &Settings,
        features: &[String],
        problems: &mut Vec<String>,
    ) -> Self {
        let mut krate = Self {
            package,
            name: name.to_string(),
            modules: Vec::new(),
        };
        let mut reader = Reader {
            settings,
            features,
            problems,
        };
        krate.modules.push(Module::empty(None, Vec::new()));
        let dir = root.parent().unwrap_or(Path::new("")).to_path_buf();
        if let Some(items) = reader.file(root) {
            reader.items(&mut krate, 0, &items, root, &dir);
        }
        krate
    }
}

impl Module {
    fn empty(parent: Option<usize>, path: Vec<String>) -> Self {
        Self {
            parent,
            path,
            children: BTreeMap::new(),
            types: Vec::new(),
            uses: Vec::new(),
            impls: Vec::new(),
        }
    }
}

struct Reader<'a> {
    settings: &'a Settings,
    features: &'a [String],
    problems: &'a mut Vec<String>,
}

impl Reader<'_> {
    fn file(&mut self, path: &Path) -> Option<Vec<Item>> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) => {
                self.problems
                    .push(format!("{}: cannot be read: {e}", path.display()));
                return None;
            }
        };
        match syn::parse_file(&text) {
            Ok(file) => Some(file.items),
            Err(e) => {
                let start = e.span().start();
                self.problems.push(format!(
                    "{}:{}:{}: does not parse: {e}",
                    path.display(),
                    start.line,
                    start.column + 1
                ));
                None
            }
        }
    }

    /// Read `items` of module `module`, which is in `file`; its child
    /// modules' files are under `dir`.
    fn items(&mut self, krate: &mut Crate, module: usize, items: &[Item], file: &Path, dir: &Path) {
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
                    name: s.ident.unraw().to_string(),
                    attrs,
                }),
                Item::Enum(e) => krate.modules[module].types.push(TypeDef {
                    name: e.ident.unraw().to_string(),
                    attrs,
                }),
                Item::Union(u) => krate.modules[module].types.push(TypeDef {
                    name: u.ident.unraw().to_string(),
                    attrs,
                }),
                Item::Use(u) => {
                    let mut uses = Vec::new();
                    flatten(&u.tree, &mut Vec::new(), &mut uses);
                    for (name, path) in uses {
                        krate.modules[module].uses.push(Use {
                            name,
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
                    krate.modules[module].uses.push(Use {
                        name: Some(name),
                        path: vec![e.ident.unraw().to_string()],
                        absolute: true,
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
                                Some((t.ident.unraw().to_string(), t.ty.clone(), at(t.ty.span())))
                            }
                            _ => None,
                        })
                        .collect();
                    krate.modules[module].impls.push(Impl {
                        trait_path: i.trait_.as_ref().map(|(_, path, _)| path.clone()),
                        self_ty: (*i.self_ty).clone(),
                        types,
                        attrs,
                        at: at(i.self_ty.span()),
                    });
                }
                Item::Mod(m) => {
                    let name = m.ident.unraw().to_string();
                    let mut path = krate.modules[module].path.clone();
                    path.push(name.clone());
                    let child = krate.modules.len();
                    krate.modules.push(Module::empty(Some(module), path));
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
                        (Some((_, inner)), _) => {
                            self.items(krate, child, inner, file, &dir.join(&name));
                        }
                        (None, Some(explicit)) => {
                            let target = file.parent().unwrap_or(dir).join(explicit);
                            let child_dir = target.parent().unwrap_or(dir).to_path_buf();
                            if let Some(items) = self.file(&target) {
                                self.items(krate, child, &items, &target, &child_dir);
                            }
                        }
                        (None, None) => {
                            let flat = dir.join(format!("{name}.rs"));
                            let nested = dir.join(&name).join("mod.rs");
                            let target = if flat.exists() { flat } else { nested };
                            if let Some(items) = self.file(&target) {
                                self.items(krate, child, &items, &target, &dir.join(&name));
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
