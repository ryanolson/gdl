//! Names, resolved as Rust 2018 and later resolve them, as far as the
//! markers need: a path in a module, through the module's items, its `use`
//! declarations and globs, and the dependencies by the names Cargo gives
//! them, to a module or a type. Crates are read when a path first enters
//! them.

use std::collections::HashMap;

use crate::cfg::Settings;
use crate::metadata::Graph;
use crate::tree::{Crate, Vis};

/// What a path names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Def {
    /// A module: crate and module.
    Module(usize, usize),
    /// A type: crate, module, and its index among the module's types.
    Type(usize, usize, usize),
}

/// The crates read so far, and the means to read more.
pub(crate) struct Resolver<'a> {
    pub(crate) graph: &'a Graph,
    settings: &'a Settings,
    pub(crate) crates: Vec<Crate>,
    by_package: HashMap<usize, Option<usize>>,
    /// Problems met reading a crate's files.
    pub(crate) problems: Vec<String>,
    /// Within one query, each lookup of a name in a module by a viewer,
    /// and its answer: `None` while it is in progress. Globs can import
    /// each other, so a lookup met again on the way finds nothing new,
    /// and one met again by another path is answered once.
    known: HashMap<Lookup, Option<Def>>,
}

/// A lookup: crate, module, name, and the viewer, if a glob looks.
type Lookup = (usize, usize, String, Option<(usize, usize)>);

/// How deep `use` chains and globs may go before the reader stops: far
/// beyond what real code needs, and a bound on a cycle.
const DEPTH: usize = 64;

impl<'a> Resolver<'a> {
    pub(crate) fn new(graph: &'a Graph, settings: &'a Settings) -> Self {
        Self {
            graph,
            settings,
            crates: Vec::new(),
            by_package: HashMap::new(),
            problems: Vec::new(),
            known: HashMap::new(),
        }
    }

    /// The crate of package `package`'s library, read on first use; `None`
    /// for a package without a library.
    pub(crate) fn krate(&mut self, package: usize) -> Option<usize> {
        if let Some(&known) = self.by_package.get(&package) {
            return known;
        }
        let found = self.graph.packages[package]
            .lib
            .clone()
            .map(|(name, root)| {
                let krate = Crate::read(
                    package,
                    &name,
                    &root,
                    self.settings,
                    &self.graph.packages[package].features,
                    &mut self.problems,
                );
                self.crates.push(krate);
                self.crates.len() - 1
            });
        self.by_package.insert(package, found);
        found
    }

    /// What `path` names in module `module` of crate `krate`. `absolute`
    /// for a path that starts with `::`.
    pub(crate) fn resolve(
        &mut self,
        krate: usize,
        module: usize,
        path: &[String],
        absolute: bool,
    ) -> Option<Def> {
        self.known.clear();
        self.path(krate, module, path, absolute, 0)
    }

    fn path(
        &mut self,
        krate: usize,
        module: usize,
        path: &[String],
        absolute: bool,
        depth: usize,
    ) -> Option<Def> {
        if depth > DEPTH {
            return None;
        }
        let (first, rest) = path.split_first()?;
        let mut at = if absolute {
            self.external(krate, first)?
        } else {
            match first.as_str() {
                "crate" => Def::Module(krate, 0),
                "self" => Def::Module(krate, module),
                "super" => Def::Module(krate, self.crates[krate].modules[module].parent?),
                name => self.scope(krate, module, name, depth)?,
            }
        };
        for segment in rest {
            let Def::Module(c, m) = at else {
                return None;
            };
            at = match segment.as_str() {
                "super" => Def::Module(c, self.crates[c].modules[m].parent?),
                "self" => at,
                name => self.lookup(c, m, name, None, depth + 1)?,
            };
        }
        Some(at)
    }

    /// A name in scope in a module: its members, else a dependency.
    fn scope(&mut self, krate: usize, module: usize, name: &str, depth: usize) -> Option<Def> {
        self.lookup(krate, module, name, None, depth)
            .or_else(|| self.external(krate, name))
    }

    /// The root of the dependency that crate `krate` calls `name`.
    fn external(&mut self, krate: usize, name: &str) -> Option<Def> {
        let package = self.crates[krate].package;
        if name == self.crates[krate].name {
            return Some(Def::Module(krate, 0));
        }
        let dep = self.graph.packages[package]
            .deps
            .iter()
            .find(|(n, _)| n == name)
            .map(|&(_, p)| p)?;
        let c = self.krate(dep)?;
        Some(Def::Module(c, 0))
    }

    /// What `name` names in a module, as code outside the crate sees it:
    /// only what is `pub` along the way. A generated segment names a node from outside.
    pub(crate) fn public_member(&mut self, krate: usize, module: usize, name: &str) -> Option<Def> {
        self.known.clear();
        // No module of any crate: only a `pub` item is visible to it.
        self.lookup(krate, module, name, Some((usize::MAX, usize::MAX)), 0)
    }

    /// A name that a module holds (a type, a child module, a name a `use`
    /// binds, or a name a glob brings in), of the items that module
    /// `viewer` can see, if any: the module
    /// whose glob began the search. A glob passes on only what the module
    /// that holds it can see, and visibility only narrows along a chain of
    /// globs, so each item is checked against the first module.
    fn lookup(
        &mut self,
        krate: usize,
        module: usize,
        name: &str,
        viewer: Option<(usize, usize)>,
        depth: usize,
    ) -> Option<Def> {
        if depth > DEPTH {
            return None;
        }
        let key = (krate, module, name.to_string(), viewer);
        if let Some(&known) = self.known.get(&key) {
            return known;
        }
        self.known.insert(key.clone(), None);
        let found = self.held(krate, module, name, viewer, depth);
        self.known.insert(key, found);
        found
    }

    fn held(
        &mut self,
        krate: usize,
        module: usize,
        name: &str,
        viewer: Option<(usize, usize)>,
        depth: usize,
    ) -> Option<Def> {
        let sees = |this: &Self, vis: Vis| {
            viewer.is_none_or(|viewer| this.visible(vis, (krate, module), viewer))
        };
        let m = &self.crates[krate].modules[module];
        if let Some(i) = m
            .types
            .iter()
            .position(|t| t.name == name && sees(self, t.vis))
        {
            return Some(Def::Type(krate, module, i));
        }
        if let Some(&child) = m.children.get(name)
            && sees(self, self.crates[krate].modules[child].vis)
        {
            return Some(Def::Module(krate, child));
        }
        let named: Vec<(Vec<String>, bool)> = m
            .uses
            .iter()
            .filter(|u| u.name.as_deref() == Some(name) && sees(self, u.vis))
            .map(|u| (u.path.clone(), u.absolute))
            .collect();
        for (path, absolute) in named {
            if let Some(def) = self.path(krate, module, &path, absolute, depth + 1) {
                return Some(def);
            }
        }
        let globs: Vec<(Vec<String>, bool)> = self.crates[krate].modules[module]
            .uses
            .iter()
            .filter(|u| u.name.is_none() && sees(self, u.vis))
            .map(|u| (u.path.clone(), u.absolute))
            .collect();
        for (path, absolute) in globs {
            if let Some(Def::Module(c, m)) = self.path(krate, module, &path, absolute, depth + 1)
                && (c, m) != (krate, module)
                && let Some(def) = self.lookup(
                    c,
                    m,
                    name,
                    Some(viewer.unwrap_or((krate, module))),
                    depth + 1,
                )
            {
                return Some(def);
            }
        }
        None
    }

    /// Whether module `viewer` can see an item of module `owner` with
    /// visibility `vis`: a private item is seen in its module and the
    /// modules inside it.
    fn visible(&self, vis: Vis, owner: (usize, usize), viewer: (usize, usize)) -> bool {
        match vis {
            Vis::Public => true,
            Vis::Crate => owner.0 == viewer.0,
            Vis::Private => {
                if owner.0 != viewer.0 {
                    return false;
                }
                let modules = &self.crates[owner.0].modules;
                let mut at = Some(viewer.1);
                while let Some(m) = at {
                    if m == owner.1 {
                        return true;
                    }
                    at = modules[m].parent;
                }
                false
            }
        }
    }
}

/// A path's segments as names, `r#` dropped, and whether it starts with
/// `::`. Generic arguments are dropped.
pub(crate) fn segments(path: &syn::Path) -> (Vec<String>, bool) {
    use syn::ext::IdentExt as _;
    (
        path.segments
            .iter()
            .map(|s| s.ident.unraw().to_string())
            .collect(),
        path.leading_colon.is_some(),
    )
}
