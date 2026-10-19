//! Generates one crate per segment of a gdl graph placement.
//!
//! The input is a spec ([`Spec`]): the contract crate holding the graph's
//! `graph!` invocation, the placement of nodes into segments, and the crate
//! and type implementing each node. The generator reads the contract's
//! manifest by compiling the contract alone, validates the placement and the
//! bindings against it, and writes, per segment, a crate with a library
//! (`start`, to deploy the segment inside a program) and a binary (to run
//! it on its own). Each segment crate depends only on the implementation
//! crates of the nodes it hosts.
//!
//! The generator owns each segment crate's directory: every file in it is
//! generated, and carries a do-not-edit marker on its first line. [`write`]
//! replaces generated files and removes marked files it no longer produces;
//! [`drift`] reports any difference instead of writing.
//!
//! [`diagram`] draws the spec's placed graph as Graphviz DOT or a Mermaid
//! flowchart.
//!
//! A spec may name a graph file instead of a contract crate: the body of a
//! `graph!` invocation, with message names where `graph!` has Rust types,
//! and the crates that offer those messages: types that derive `Message`,
//! and `pub use` items marked `#[export]`. [`resolve`] checks that the
//! names agree along every edge and resolves each to exactly one message,
//! read from the crates' source by `gdl-extract`. Such a spec binds its nodes through `[nodes]`:
//! node crates that offer them, read from their source without compiling
//! those crates.

mod contract;
mod dump;
mod emit;
mod export;
mod extract;
mod names;
mod nodes;
mod paths;
mod resolve;
mod spec;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use emit::MARKER;
pub use export::export;
pub use extract::{Crate, Extracted, add_to_catalog, extract, verify};
/// The working catalogue, its entries, and what a crate offers, read from
/// its source.
pub use gdl_extract::{Catalog, Entry, Offer};
pub use resolve::Resolved;
pub use spec::Spec;

/// A generation failure, with everything wrong at once where possible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// Generated files, keyed by path relative to the output directory.
pub type Files = BTreeMap<PathBuf, String>;

/// Build the contract's manifest in `target` (a cargo target directory),
/// validate `spec` against it, and render every file for output directory
/// `out`. For a graph file, the files include the contract crate, in
/// `contract/` under `out`.
pub fn render(spec: &Spec, out: &Path, target: &Path) -> Result<Files, Error> {
    Ok(render_with(spec, out, target, emit::Deps::Relative, Offers::Read)?.0)
}

/// Where the offers of a spec's listed crates come from.
#[derive(Debug, Clone, Copy)]
pub enum Offers<'a> {
    /// Read from each crate's source, a crate listed by version fetched
    /// into the target directory first.
    Read,
    /// Given: the entries of a loaded catalogue, read when each was added.
    /// A listed crate the catalogue does not hold is an error.
    Given(&'a [Offer]),
}

/// [`render`], with dependencies named as `deps` says and offers from
/// `from`, and the graph's name.
fn render_with(
    spec: &Spec,
    out: &Path,
    target: &Path,
    deps: emit::Deps,
    from: Offers<'_>,
) -> Result<(Files, String), Error> {
    let prepared = prepare(spec, Some(out), target, deps, from)?;
    let spec = bound(spec, &prepared, target, from)?;
    let mut files = render_manifest(&spec, &prepared.contract, &prepared.manifest, out, deps)?;
    files.extend(prepared.files);
    Ok((files, prepared.manifest.name))
}

/// `spec` with its graph's nodes bound to the nodes its node crates offer,
/// in place of the `[bind.*]` tables of a contract spec, which is returned
/// as it is.
fn bound<'s>(
    spec: &'s Spec,
    prepared: &Prepared,
    target: &Path,
    from: Offers<'_>,
) -> Result<std::borrow::Cow<'s, Spec>, Error> {
    let spec::Source::Graph(source) = &spec.source else {
        return Ok(std::borrow::Cow::Borrowed(spec));
    };
    let (graph, resolution) = prepared
        .graph
        .as_ref()
        .expect("a graph spec's preparation reads its graph");
    let listed: Vec<_> = source
        .nodes
        .iter()
        .map(|c| (c.package.as_str(), c.lib.as_str(), &c.source))
        .collect();
    let offers = offers(&listed, target, from)?;
    let (bind, problems) = nodes::bind(
        graph,
        &source.nodes,
        &offers,
        &resolution.messages,
        &source.node_pins,
    );
    let mut with_binds = spec.clone();
    with_binds.bind = bind;
    with_binds.problems.extend(problems);
    Ok(std::borrow::Cow::Owned(with_binds))
}

/// A spec checked as generating it would check it, with nothing written:
/// its manifest, what one request costs under its placement, and its
/// placed graph as a Mermaid flowchart.
#[derive(Debug, Clone)]
pub struct Check {
    /// The contract's manifest.
    pub manifest: gdl_model::Manifest,
    /// The crossing report, as `crossings.txt` holds it.
    pub crossings: String,
    /// The placed graph.
    pub mermaid: String,
}

/// Check `spec` as [`render`] and [`export`] check it, with offers from
/// `from`, and write nothing. A spec that passes generates.
pub fn check(spec: &Spec, target: &Path, from: Offers<'_>) -> Result<Check, Error> {
    let prepared = prepare(spec, None, target, emit::Deps::Relative, from)?;
    let spec = bound(spec, &prepared, target, from)?;
    let layout = validate(&spec, &prepared.contract, &prepared.manifest)?;
    let mermaid = prepared
        .manifest
        .to_mermaid(Some(&layout))
        .map_err(|e| Error::new(e.to_string()))?;
    Ok(Check {
        crossings: layout.report().to_string(),
        mermaid,
        manifest: prepared.manifest,
    })
}

/// A diagram format for [`diagram`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Graphviz DOT.
    Dot,
    /// A Mermaid flowchart.
    Mermaid,
}

/// The spec's placed graph as a diagram: each segment a cluster, and each
/// edge that crosses segments marked. The contract's manifest is built in
/// `target` (a cargo target directory), as for [`render`].
pub fn diagram(spec: &Spec, target: &Path, format: Format) -> Result<String, Error> {
    let manifest = prepare(spec, None, target, emit::Deps::Relative, Offers::Read)?.manifest;
    let layout =
        gdl_model::layout(&manifest, &spec.placement).map_err(|e| Error::new(e.to_string()))?;
    match format {
        Format::Dot => manifest.to_dot(Some(&layout)),
        Format::Mermaid => manifest.to_mermaid(Some(&layout)),
    }
    .map_err(|e| Error::new(e.to_string()))
}

/// A graph file's message names, each resolved to one message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// Message name to the message it resolved to.
    pub messages: BTreeMap<String, Resolved>,
}

impl fmt::Display for Resolution {
    /// One line per name: the name, the type's path, and `wire` or `local`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self.messages.keys().map(String::len).max().unwrap_or(0);
        let paths = self
            .messages
            .values()
            .map(|m| m.path.len())
            .max()
            .unwrap_or(0);
        for (name, message) in &self.messages {
            let wire = if message.wire { "wire" } else { "local" };
            writeln!(f, "{name:names$}  {:paths$}  {wire}", message.path)?;
        }
        Ok(())
    }
}

/// Check the spec's graph file and resolve its message names against what
/// the crates `[messages]` lists offer, read from their source. The names
/// are checked before any crate is read, and nothing is built.
pub fn resolve(spec: &Spec, target: &Path) -> Result<Resolution, Error> {
    let spec::Source::Graph(source) = &spec.source else {
        return Err(Error::new(format!(
            "{}: resolving messages needs a [graph] spec; a contract crate names its own types",
            spec.file.display()
        )));
    };
    Ok(resolved(source, target, Offers::Read)?.1)
}

/// The graph file of `source`, checked, and its names resolved.
fn resolved(
    source: &spec::GraphSource,
    target: &Path,
    from: Offers<'_>,
) -> Result<(names::GraphFile, Resolution), Error> {
    let file = match &source.text {
        Some(text) => names::GraphFile::parse(text, &source.path)?,
        None => names::GraphFile::read(&source.path)?,
    };
    let listed: Vec<_> = source
        .crates
        .iter()
        .map(|c| (c.package.as_str(), c.lib.as_str(), &c.source))
        .collect();
    let offers = offers(&listed, target, from)?;
    let messages = resolve::resolve(&file.uses, &offers, &source.pins)
        .map_err(|problems| Error::new(problems.join("\n")))?;
    Ok((file, Resolution { messages }))
}

/// What the listed crates offer, read from their source: a directory as
/// it is, and a version fetched from crates.io into `target`, which builds
/// nothing. Crates of one workspace share one `cargo metadata`. Every
/// problem the reader meets is reported.
fn offers(
    listed: &[(&str, &str, &spec::CrateSource)],
    target: &Path,
    from: Offers<'_>,
) -> Result<Vec<gdl_extract::Offer>, Error> {
    if let Offers::Given(given) = from {
        return listed
            .iter()
            .map(|&(package, lib, source)| given_offer(given, package, lib, source))
            .collect::<Result<Vec<_>, Vec<String>>>()
            .map_err(|problems| Error::new(problems.join("\n")));
    }
    let mut workspaces: Vec<gdl_extract::Workspace> = Vec::new();
    let mut offers = Vec::with_capacity(listed.len());
    let mut problems = Vec::new();
    for &(package, lib, source) in listed {
        let extraction = match source {
            spec::CrateSource::Path(dir) => {
                // Crates of one workspace share its `cargo metadata`.
                match workspaces.iter().find_map(|w| w.extract_in(dir)) {
                    Some(extraction) => extraction,
                    None => {
                        let workspace = gdl_extract::Workspace::load(&dir.join("Cargo.toml"))
                            .map_err(|e| Error::new(format!("{}: {e}", dir.display())))?;
                        let extraction = workspace.extract_in(dir).ok_or_else(|| {
                            Error::new(format!(
                                "{}: cargo metadata resolves no package there",
                                dir.display()
                            ))
                        })?;
                        workspaces.push(workspace);
                        extraction
                    }
                }
            }
            spec::CrateSource::Version(version) => {
                let scratch = target
                    .join("gdl-gen")
                    .join(format!("fetch-{package}-{version}"));
                let spec = format!("{package}@{version}");
                let fail = |e: gdl_extract::Error| Error::new(format!("{spec}: {e}"));
                let workspace =
                    gdl_extract::Workspace::fetch(package, version, &scratch).map_err(fail)?;
                let located = workspace.locate(&spec).map_err(fail)?;
                // The spec named the library for the package; generated
                // code names the crate by its library.
                if located.lib.as_deref().is_some_and(|l| l != lib) {
                    return Err(Error::new(format!(
                        "{spec}: its library is `{}`, not `{lib}`; a crate listed by version must name its library for its package",
                        located.lib.unwrap_or_default()
                    )));
                }
                workspace.extract(&spec).map_err(fail)?
            }
        };
        problems.extend(extraction.problems);
        offers.push(extraction.offer);
    }
    if problems.is_empty() {
        Ok(offers)
    } else {
        Err(Error::new(problems.join("\n")))
    }
}

/// crates.io, as a lockfile names it over git and over the sparse
/// protocol: what a crate listed by version, and a manifest that depends
/// on one by version, mean.
pub const CRATES_IO: [&str; 2] = [
    "registry+https://github.com/rust-lang/crates.io-index",
    "sparse+https://index.crates.io/",
];

/// The offer of the listed crate `package` from `source` among `given`,
/// held to the rule a read holds it to: its library is `lib`.
fn given_offer(
    given: &[Offer],
    package: &str,
    lib: &str,
    source: &spec::CrateSource,
) -> Result<Offer, Vec<String>> {
    let canonical = |dir: &Path| std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let found = given.iter().find(|o| {
        o.package == package
            && match source {
                spec::CrateSource::Version(v) =>
                // A version names the crates.io package, which a lockfile
                // gives a registry source; a git package is another.
                {
                    o.version == *v && CRATES_IO.contains(&o.source.as_str())
                }
                spec::CrateSource::Path(dir) => o
                    .source
                    .strip_prefix("path+file://")
                    .is_some_and(|d| canonical(Path::new(d)) == canonical(dir)),
            }
    });
    let Some(offer) = found else {
        return Err(vec![format!(
            "`{package}` ({source}) is not in the catalogue: add it first"
        )]);
    };
    // The library's name, as the reader wrote every path of the crate.
    let library = offer
        .messages
        .iter()
        .map(|m| m.path.as_str())
        .chain(offer.exports.iter().map(|e| e.path.as_str()))
        .chain(offer.nodes.iter().map(|n| n.ty.as_str()))
        .find_map(|path| path.split("::").next());
    if let Some(library) = library.filter(|l| *l != lib) {
        return Err(vec![format!(
            "{package}@{}: its library is `{library}`, not `{lib}`; a crate listed by version must name its library for its package",
            offer.version
        )]);
    }
    Ok(offer.clone())
}

/// The contract that a spec's segments build on, its manifest, and the
/// files of the contract crate when the generator writes it.
struct Prepared {
    /// The contract, at its place under the output directory.
    contract: spec::Contract,
    manifest: gdl_model::Manifest,
    /// Empty for a contract crate the spec names.
    files: Files,
    /// A graph file's graph and its resolution: read once, so that
    /// everything generated from it, the contract and the bindings, comes
    /// from one version of it.
    graph: Option<(gdl_syntax::Graph, Resolution)>,
}

/// The spec's contract and its manifest. A graph file's contract is
/// generated: its manifest is read from a copy under `target`, so that
/// `--check` and `diagram` write nothing in the output, and its files
/// are rendered for `out/contract` when `out` is given.
fn prepare(
    spec: &Spec,
    out: Option<&Path>,
    target: &Path,
    deps: emit::Deps,
    from: Offers<'_>,
) -> Result<Prepared, Error> {
    let source = match &spec.source {
        spec::Source::Contract(contract) => {
            return Ok(Prepared {
                contract: contract.clone(),
                manifest: dump::manifest(contract, target)?,
                files: Files::new(),
                graph: None,
            });
        }
        spec::Source::Graph(source) => source,
    };
    let (file, resolution) = resolved(source, target, from)?;
    let name = file.graph.name.value();
    let valid = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !valid {
        return Err(Error::new(format!(
            "graph `{name}`: its name starts every package name, so it is a letter, then letters, digits, `-`, and `_`"
        )));
    }
    // Named as segment packages are, so the package checks of `validate`
    // compare like with like.
    let package = emit::package_name(&name, "contract");
    let lib = package.replace('-', "_");
    let clashes = library_clashes(&lib, &source.crates);
    if !clashes.is_empty() {
        return Err(Error::new(clashes.join("\n")));
    }
    let lib_rs = contract::lib_rs(&file.graph, &resolution, &source.path);
    let crates = |path: &dyn Fn(&Path) -> String| {
        contract::cargo_toml(&package, &spec.gdl, &source.crates, path)
    };

    // The manifest is built from the graph and its resolution, as the
    // contract's `graph!` would emit it. Nothing is compiled, so a spec
    // whose crates do not build here (an engine that needs CUDA) still
    // generates. A wrong reading of a wire type is caught by the segment's
    // build or by its deployment, which checks the layout against the
    // compiled contract's own manifest.
    let manifest = file
        .graph
        .manifest(&|name| {
            let resolved = resolution.messages.get(name)?;
            Some((resolved.path.clone(), resolved.wire))
        })
        .map_err(Error::new)?;
    let mut contract = spec::Contract {
        package: package.clone(),
        lib: lib.clone(),
        path: target.join("gdl-gen").join(&package),
        module: gdl_syntax::identifier(&lib),
    };

    let mut files = Files::new();
    if let Some(out) = out {
        let out = dump::absolute(out)?;
        let dir = out.join("contract");
        let path = |p: &Path| deps.path(&dir, p);
        files.insert(
            PathBuf::from("contract/Cargo.toml"),
            crates(&path) + deps.workspace(),
        );
        files.insert(PathBuf::from("contract/src/lib.rs"), lib_rs);
        contract.path = dir;
    }
    Ok(Prepared {
        contract,
        manifest,
        files,
        graph: Some((file.graph, resolution)),
    })
}

/// The message crates whose library takes a name that the generated
/// contract uses for another crate: `gdl`, or the contract's own.
fn library_clashes(contract: &str, crates: &[spec::MessageCrate]) -> Vec<String> {
    crates
        .iter()
        .filter_map(|c| {
            let whose = if c.lib == "gdl" {
                "the gdl runtime's name in the generated contract"
            } else if c.lib == contract {
                "the generated contract's name"
            } else {
                return None;
            };
            Some(format!(
                "[messages] crate `{}`: its library `{}` is {whose}",
                c.package, c.lib
            ))
        })
        .collect()
}

fn render_manifest(
    spec: &Spec,
    contract: &spec::Contract,
    manifest: &gdl_model::Manifest,
    out: &Path,
    deps: emit::Deps,
) -> Result<Files, Error> {
    let layout = validate(spec, contract, manifest)?;
    let out = dump::absolute(out)?;
    emit::files(spec, contract, manifest, &layout, &out, deps)
}

/// Check the placement and the bindings against the manifest, reporting
/// every problem found.
fn validate(
    spec: &Spec,
    contract: &spec::Contract,
    manifest: &gdl_model::Manifest,
) -> Result<gdl_model::Layout, Error> {
    let mut problems = spec.problems.clone();
    // Where a node's crate comes from, as the spec writes it: a contract
    // spec binds each node in a table, a graph spec through `[nodes]`.
    let bound_by = |node: &str| match spec.source {
        spec::Source::Contract(_) => format!("[bind.{node}]"),
        spec::Source::Graph(_) => format!("node `{node}`'s [nodes] entry"),
    };
    let layout = match gdl_model::layout(manifest, &spec.placement) {
        Ok(layout) => Some(layout),
        Err(errors) => {
            problems.push(errors.to_string());
            None
        }
    };
    if !manifest
        .name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
    {
        problems.push(format!(
            "graph `{}`: its name starts every package name, so it must start with a letter",
            manifest.name
        ));
    }
    // Package names are what Cargo sees: two segments that differ only in
    // case or in `-` against `_` would build one package twice.
    let mut packages: BTreeMap<String, &str> = BTreeMap::new();
    for (segment, nodes) in spec.placement.segments() {
        let hosted: BTreeSet<&str> = nodes.iter().map(String::as_str).collect();
        let valid = !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !valid {
            problems.push(format!(
                "segment `{segment}`: a segment name is letters, digits, `-`, and `_`"
            ));
            continue;
        }
        // The generated contract is written to `contract/`, whatever its
        // package is called; case is folded for file systems that fold it.
        if matches!(spec.source, spec::Source::Graph(_)) && segment.eq_ignore_ascii_case("contract")
        {
            problems.push(format!(
                "segment `{segment}`: the generated contract crate is `contract/`; rename the segment"
            ));
            continue;
        }
        let package = emit::package(manifest, segment);
        // A generated crate depends on the contract and on implementations
        // by package name; Cargo cannot hold two path packages of one name.
        if package == contract.package {
            problems.push(format!(
                "segment `{segment}` becomes package `{package}`, the contract's name"
            ));
        }
        for (node, bind) in &spec.bind {
            if package == bind.package {
                problems.push(match spec.source {
                    spec::Source::Contract(_) => format!(
                        "segment `{segment}` becomes package `{package}`, the name of [bind.{node}]'s crate"
                    ),
                    spec::Source::Graph(_) => format!(
                        "segment `{segment}` becomes package `{package}`, the name of node `{node}`'s [nodes] crate"
                    ),
                });
            }
        }
        // Rust code names crates by library name; each crate the segment's
        // code sees must have its own.
        let mut seen: BTreeMap<String, String> = BTreeMap::new();
        let mut see = |lib: &str, what: String, problems: &mut Vec<String>| {
            if let Some(first) = seen.get(lib) {
                if *first != what {
                    problems.push(format!(
                        "segment `{segment}`: crate name `{lib}` is both {first} and {what}"
                    ));
                }
            } else {
                seen.insert(lib.to_string(), what);
            }
        };
        see(
            &package.replace('-', "_"),
            "the segment's library".into(),
            &mut problems,
        );
        see(&contract.lib, "the contract".into(), &mut problems);
        for fixed in ["gdl", "serde", "tokio", "toml"] {
            see(fixed, format!("the `{fixed}` dependency"), &mut problems);
        }
        // Every crate sees these; a dependency of the same name would
        // shadow the standard library in the generated code.
        for builtin in ["std", "core", "alloc"] {
            see(builtin, "the standard library".into(), &mut problems);
        }
        // One package may implement several nodes: it is one crate.
        for (node, bind) in &spec.bind {
            if hosted.contains(node.as_str()) {
                see(
                    &bind.lib,
                    format!("package `{}`", bind.package),
                    &mut problems,
                );
            }
        }
        // Cargo names dependencies by package: each key in the generated
        // manifest must be distinct too, whatever the libraries are called.
        let mut keys: BTreeMap<String, String> = BTreeMap::new();
        let mut key = |name: &str, what: String, problems: &mut Vec<String>| match keys.get(name) {
            Some(first) if *first != what => problems.push(format!(
                "segment `{segment}`: dependency `{name}` is both {first} and {what}"
            )),
            Some(_) => {}
            None => {
                keys.insert(name.to_string(), what);
            }
        };
        for fixed in ["gdl", "serde", "tokio", "toml"] {
            key(fixed, format!("the `{fixed}` dependency"), &mut problems);
        }
        key(&contract.package, "the contract".into(), &mut problems);
        for (node, bind) in &spec.bind {
            if hosted.contains(node.as_str()) {
                let first = spec
                    .bind
                    .iter()
                    .find(|(n, b)| hosted.contains(n.as_str()) && b.package == bind.package)
                    .map_or(node.as_str(), |(n, _)| n.as_str());
                key(
                    &bind.package,
                    format!("{}'s package", bound_by(first)),
                    &mut problems,
                );
            }
        }
        if let Some(other) = packages.insert(package.clone(), segment) {
            problems.push(format!(
                "segments `{other}` and `{segment}` both become package `{package}`"
            ));
        }
    }
    let mut sources: BTreeMap<&str, (spec::CrateSource, &str)> = BTreeMap::new();
    for (node, bind) in &spec.bind {
        match manifest.node(node) {
            None => problems.push(format!(
                "{}: the graph has no node `{node}`",
                bound_by(node)
            )),
            Some(info) if info.role == gdl_model::Role::Source => problems.push(format!(
                "{}: `{node}` is the source, which takes no implementation",
                bound_by(node)
            )),
            Some(_) => {}
        }
        if bind.ty.trim().is_empty() {
            problems.push(format!("{}: `type` is empty", bound_by(node)));
        } else {
            match type_crate(&bind.ty) {
                None => problems.push(format!(
                    "{}: type `{}` is not a Rust type path",
                    bound_by(node),
                    bind.ty
                )),
                // The bound crate is the only one of the node's that the
                // segment depends on; a type from anywhere else cannot
                // resolve.
                Some(krate) if krate != bind.lib => problems.push(format!(
                    "{}: type `{}` is not in crate `{}`",
                    bound_by(node),
                    bind.ty,
                    bind.lib
                )),
                Some(_) => {}
            }
        }
        let path = match &bind.source {
            spec::CrateSource::Path(path) => spec::CrateSource::Path(
                dump::absolute(path).unwrap_or_else(|_| paths::normalize(path)),
            ),
            version => version.clone(),
        };
        match sources.get(bind.package.as_str()) {
            Some((first, other)) if *first != path => problems.push(format!(
                "{} and {} give package `{}` two sources",
                bound_by(other),
                bound_by(node),
                bind.package
            )),
            Some(_) => {}
            None => {
                sources.insert(&bind.package, (path, node));
            }
        }
    }
    // A graph spec has reported each node that did not bind already.
    let contract_spec = matches!(spec.source, spec::Source::Contract(_));
    for node in &manifest.nodes {
        if contract_spec
            && node.role != gdl_model::Role::Source
            && !spec.bind.contains_key(&node.name)
        {
            problems.push(format!("node `{}` has no [bind.{}]", node.name, node.name));
        }
        if node.role != gdl_model::Role::Source && node.binding.is_none() {
            problems.push(format!(
                "node `{}` has no binding name in the manifest; rebuild the contract with this gdl",
                node.name
            ));
        }
    }
    match (layout, problems.is_empty()) {
        (Some(layout), true) => Ok(layout),
        _ => Err(Error::new(problems.join("\n"))),
    }
}

/// Write `files` under `out`, and remove generated files there that are no
/// longer produced. Returns the removed paths. Refuses, writing nothing, if
/// a segment crate's directory holds a file the generator did not write:
/// whatever it is, it changes that crate's build.
pub fn write(out: &Path, files: &Files) -> Result<Vec<PathBuf>, Error> {
    let survey = survey(out, files)?;
    if !survey.foreign.is_empty() {
        let list: Vec<String> = survey
            .foreign
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        return Err(Error::new(format!(
            "these files in generated crates were not generated; move them out first:\n{}",
            list.join("\n")
        )));
    }
    for (path, text) in files {
        let path = out.join(path);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| io(dir, e))?;
        }
        if std::fs::read_to_string(&path).is_ok_and(|existing| &existing == text) {
            continue;
        }
        std::fs::write(&path, text).map_err(|e| io(&path, e))?;
    }
    for path in &survey.stale {
        let full = out.join(path);
        std::fs::remove_file(&full).map_err(|e| io(&full, e))?;
    }
    Ok(survey.stale)
}

/// How the files under `out` differ from `files`: one line per missing,
/// changed, stale (generated, no longer produced), or unexpected (in a
/// generated crate, not generated) file. Empty when they match.
pub fn drift(out: &Path, files: &Files) -> Result<Vec<String>, Error> {
    let mut found = Vec::new();
    for (path, text) in files {
        match std::fs::read_to_string(out.join(path)) {
            Err(_) => found.push(format!("missing: {}", path.display())),
            Ok(existing) if &existing != text => found.push(format!("differs: {}", path.display())),
            Ok(_) => {}
        }
    }
    let survey = survey(out, files)?;
    found.extend(
        survey
            .stale
            .iter()
            .map(|p| format!("stale: {}", p.display())),
    );
    found.extend(
        survey
            .foreign
            .iter()
            .map(|p| format!("unexpected: {}", p.display())),
    );
    Ok(found)
}

/// Files under `out` that `files` does not hold.
struct Survey {
    /// Marked as generated: output of an earlier generation.
    stale: Vec<PathBuf>,
    /// Unmarked, inside a generated crate's directory or at a path the
    /// generator writes.
    foreign: Vec<PathBuf>,
}

/// Walk `out`. The one directory skipped is a crate's own build directory,
/// `<crate>/target` holding a `CACHEDIR.TAG`, which is what building the
/// crate on its own leaves. Neither the name alone (a segment may be called
/// `target`) nor a tag alone (Cargo still builds a tagged `src/bin`) marks a
/// directory as build output. Likewise a crate's `Cargo.lock` is left to
/// whoever builds the crate on its own: it changes no source and pins only
/// what that build resolved.
fn survey(out: &Path, files: &Files) -> Result<Survey, Error> {
    let crates: BTreeSet<&Path> = files
        .keys()
        .filter(|path| path.components().count() > 1)
        .filter_map(|path| path.iter().next().map(Path::new))
        .collect();
    let mut survey = Survey {
        stale: Vec::new(),
        foreign: Vec::new(),
    };
    let mut dirs = vec![PathBuf::new()];
    while let Some(dir) = dirs.pop() {
        let full = out.join(&dir);
        let mut parts = dir.iter();
        let build_dir = match (parts.next(), parts.next(), parts.next()) {
            (Some(krate), Some(name), None) => {
                crates.contains(Path::new(krate)) && name == "target"
            }
            _ => false,
        };
        if build_dir && full.join("CACHEDIR.TAG").exists() {
            continue;
        }
        let entries = match std::fs::read_dir(&full) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io(&full, e)),
        };
        for entry in entries {
            let entry = entry.map_err(|e| io(&full, e))?;
            let path = dir.join(entry.file_name());
            let kind = entry.file_type().map_err(|e| io(&full, e))?;
            if kind.is_dir() {
                dirs.push(path);
                continue;
            }
            let lockfile = path.file_name().is_some_and(|name| name == "Cargo.lock")
                && dir.components().count() == 1
                && crates.contains(dir.as_path());
            if lockfile {
                continue;
            }
            let text = std::fs::read_to_string(out.join(&path)).unwrap_or_default();
            let marked = text
                .lines()
                .next()
                .is_some_and(|line| line.contains(MARKER));
            // A file at a path the generator writes is the generator's only
            // if it says so; a handwritten one there is never replaced.
            if files.contains_key(&path) {
                if !marked {
                    survey.foreign.push(path);
                }
                continue;
            }
            let in_crate = path
                .iter()
                .next()
                .is_some_and(|first| crates.contains(Path::new(first)));
            if marked {
                survey.stale.push(path);
            } else if in_crate {
                survey.foreign.push(path);
            }
        }
    }
    survey.stale.sort();
    survey.foreign.sort();
    Ok(survey)
}

fn io(path: &Path, error: std::io::Error) -> Error {
    Error::new(format!("{}: {error}", path.display()))
}

/// The crate a binding's type path starts in, as cargo names the library
/// (without a raw identifier's `r#`), or `None` if the text is not a Rust
/// path. Parsed rather than split: a library named like a keyword appears
/// in the path as `r#name`, and an unescaped keyword is not a path at all.
fn type_crate(ty: &str) -> Option<String> {
    use syn::ext::IdentExt as _;
    let path: syn::Path = syn::parse_str(ty.trim()).ok()?;
    let first = path.segments.first()?;
    Some(first.ident.unraw().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Bind, Contract, GdlDep, Source};
    use gdl_model::{HopInfo, Manifest, NodeInfo, Role, TypeInfo};

    /// `S => D`: a source and one sink.
    fn manifest() -> Manifest {
        let node = |name: &str, role, binding: Option<&str>| NodeInfo {
            name: name.into(),
            role,
            binding: binding.map(str::to_string),
        };
        Manifest {
            name: "tiny".into(),
            schema: 1,
            nodes: vec![
                node("S", Role::Source, None),
                node("D", Role::Sink, Some("d")),
            ],
            hops: vec![HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![TypeInfo::new("Req", true)],
                backward_types: vec![TypeInfo::new("Item", true)],
            }],
        }
    }

    fn spec(placement: gdl_model::Placement, bind: &[(&str, &str)]) -> Spec {
        Spec {
            file: "gdl.toml".into(),
            dir: "/work".into(),
            source: Source::Contract(Contract {
                package: "tiny-contract".into(),
                lib: "tiny_contract".into(),
                path: "/work/contract".into(),
                module: "tiny_contract".into(),
            }),
            gdl: GdlDep::Version("0.1".into()),
            placement,
            bind: bind
                .iter()
                .map(|(node, ty)| {
                    let bind = Bind {
                        package: "tiny-nodes".into(),
                        lib: "tiny_nodes".into(),
                        source: spec::CrateSource::Path("/work/nodes".into()),
                        ty: (*ty).into(),
                    };
                    ((*node).to_string(), bind)
                })
                .collect(),
            problems: Vec::new(),
        }
    }

    fn check(spec: &Spec, manifest: &Manifest) -> Result<gdl_model::Layout, Error> {
        validate(spec, spec.contract().expect("a contract spec"), manifest)
    }

    fn rendered(spec: &Spec, manifest: &Manifest, out: &Path) -> Result<Files, Error> {
        render_manifest(
            spec,
            spec.contract().expect("a contract spec"),
            manifest,
            out,
            emit::Deps::Relative,
        )
    }

    fn split() -> gdl_model::Placement {
        gdl_model::Placement::new()
            .segment("front", ["S"])
            .segment("engine", ["D"])
    }

    /// The contract hash covers the path each name resolves to: the same
    /// graph file, with `Chunk` pinned to one crate or the other, makes two
    /// contracts that refuse each other's traffic.
    #[test]
    fn the_contract_hash_changes_when_a_name_resolves_elsewhere() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compose");
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/gdl-gen-tests");
        let manifest = |file: &str| {
            let spec = Spec::load(&fixtures.join(file)).expect("spec");
            prepare(&spec, None, &target, emit::Deps::Relative, Offers::Read)
                .expect("prepared")
                .manifest
        };
        let (one, other) = (manifest("pinned.toml"), manifest("pinned-other.toml"));
        let chunk = |m: &Manifest| m.hops[0].backward_types[0].name.clone();
        assert_eq!(chunk(&one), ":: chat_protocols :: Chunk");
        assert_eq!(chunk(&other), ":: other_protocols :: Chunk");
        assert_ne!(one.contract_hash(), other.contract_hash());
    }

    /// A graph file's manifest, built from the graph and its resolution
    /// without compiling anything, is the one its compiled contract prints,
    /// hash and all: the composer builds it so, in a browser.
    #[test]
    fn a_graph_files_manifest_is_built_as_its_contract_prints_it() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let target = root.join("../../target/gdl-gen-tests");
        for spec in [
            root.join("../../examples/chat/gdl.toml"),
            root.join("tests/fixtures/compose/pinned.toml"),
            root.join("tests/fixtures/compose/versions-pinned.toml"),
        ] {
            let spec = Spec::load(&spec).expect("spec");
            let built = prepare(&spec, None, &target, emit::Deps::Relative, Offers::Read)
                .expect("prepared")
                .manifest;
            let compiled = compiled_manifest(&spec, &target);
            assert_eq!(built, compiled);
            assert_eq!(built.contract_hash(), compiled.contract_hash());
        }
    }

    /// What a graph spec's contract prints when it is compiled and run:
    /// the generator no longer compiles it, and this is the proof that it
    /// need not.
    fn compiled_manifest(spec: &Spec, target: &Path) -> Manifest {
        let Source::Graph(source) = &spec.source else {
            panic!("a graph spec");
        };
        let (file, resolution) = resolved(source, target, Offers::Read).expect("resolved");
        let package = emit::package_name(&file.graph.name.value(), "contract");
        let lib = package.replace('-', "_");
        let lib_rs = contract::lib_rs(&file.graph, &resolution, &source.path);
        let absolute =
            |p: &Path| dump::toml_string(&dump::absolute(p).expect("absolute").to_string_lossy());
        let toml = contract::cargo_toml(&package, &spec.gdl, &source.crates, &absolute);
        let dir = target
            .join("gdl-gen")
            .join(format!("contract-{:016x}", dump::hash(&(&toml, &lib_rs))));
        std::fs::create_dir_all(dir.join("src")).expect("dir");
        dump::write(&dir.join("Cargo.toml"), &toml).expect("manifest");
        dump::write(&dir.join("src/lib.rs"), &lib_rs).expect("lib");
        let contract = Contract {
            package,
            module: gdl_syntax::identifier(&lib),
            lib,
            path: dir,
        };
        dump::manifest(&contract, target).expect("compiled")
    }

    /// The generated contract depends on gdl and on each message crate by
    /// library name, so a message library may take neither `gdl` nor the
    /// contract's own name.
    #[test]
    fn a_message_library_may_not_take_a_name_the_contract_uses() {
        let crate_ = |lib: &str| spec::MessageCrate {
            package: format!("{lib}-package"),
            lib: lib.to_string(),
            source: spec::CrateSource::Path(PathBuf::from("/m")),
        };
        assert_eq!(
            library_clashes("chat_contract", &[crate_("chat_protocols")]),
            Vec::<String>::new()
        );
        assert_eq!(
            library_clashes("chat_contract", &[crate_("gdl"), crate_("chat_contract")]),
            [
                "[messages] crate `gdl-package`: its library `gdl` is the gdl runtime's name in the generated contract",
                "[messages] crate `chat_contract-package`: its library `chat_contract` is the generated contract's name",
            ]
        );
    }

    #[test]
    fn a_complete_spec_validates() {
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        assert!(check(&spec, &manifest()).is_ok());
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let placement = gdl_model::Placement::new()
            .segment("front.end", ["S"])
            .segment("engine", ["D"]);
        let spec = spec(placement, &[("S", "tiny_nodes::S"), ("Q", "tiny_nodes::Q")]);
        let error = check(&spec, &manifest()).expect_err("invalid").to_string();
        for expected in [
            "segment `front.end`",
            "[bind.S]: `S` is the source",
            "[bind.Q]: the graph has no node `Q`",
            "node `D` has no [bind.D]",
        ] {
            assert!(
                error.contains(expected),
                "{expected:?} missing from:\n{error}"
            );
        }
    }

    #[test]
    fn segments_that_become_one_package_are_refused() {
        let placement = gdl_model::Placement::new()
            .segment("front", ["S"])
            .segment("worker-a", ["D"])
            .segment("worker_a", Vec::<String>::new());
        let spec = spec(placement, &[("D", "tiny_nodes::D")]);
        let error = check(&spec, &manifest())
            .expect_err("collision")
            .to_string();
        assert!(
            error
                .contains("segments `worker-a` and `worker_a` both become package `tiny-worker-a`"),
            "{error}"
        );
    }

    /// A graph spec has no `[bind.*]` tables, so its errors name the node
    /// and its `[nodes]` crate instead.
    #[test]
    fn a_graph_spec_names_a_node_crate_without_bind_tables() {
        let mut spec = spec(
            gdl_model::Placement::new()
                .segment("contract", ["S"])
                .segment("nodes", ["D"]),
            &[("D", "tiny_nodes::D")],
        );
        spec.source = Source::Graph(spec::GraphSource {
            path: "/work/tiny.gdl".into(),
            text: None,
            crates: Vec::new(),
            pins: Default::default(),
            nodes: Vec::new(),
            node_pins: Default::default(),
        });
        let contract = Contract {
            package: "tiny-contract".into(),
            lib: "tiny_contract".into(),
            path: "/work/out/contract".into(),
            module: "tiny_contract".into(),
        };
        let error = validate(&spec, &contract, &manifest())
            .expect_err("collision")
            .to_string();
        assert!(
            error.contains(
                "segment `nodes` becomes package `tiny-nodes`, the name of node `D`'s [nodes] crate"
            ),
            "{error}"
        );
        assert!(!error.contains("[bind."), "{error}");
    }

    #[test]
    fn a_segment_package_may_not_take_a_dependency_name() {
        // `tiny-contract` and `tiny-nodes` are the contract and the
        // implementation; Cargo cannot build two path packages of one name.
        // A contract generated from a graph file is `<graph>-contract` in
        // `contract/`, so this check also keeps a segment out of its
        // directory.
        let placement = gdl_model::Placement::new()
            .segment("contract", ["S"])
            .segment("nodes", ["D"]);
        let spec = spec(placement, &[("D", "tiny_nodes::D")]);
        let error = check(&spec, &manifest())
            .expect_err("collision")
            .to_string();
        assert!(
            error.contains(
                "segment `contract` becomes package `tiny-contract`, the contract's name"
            ),
            "{error}"
        );
        assert!(
            error.contains(
                "segment `nodes` becomes package `tiny-nodes`, the name of [bind.D]'s crate"
            ),
            "{error}"
        );
    }

    #[test]
    fn a_dependency_may_not_share_a_segment_crate_name() {
        // Package `tiny_front` is a different package from the generated
        // `tiny-front`, but both are the crate `tiny_front` in Rust, and
        // the generated binary would see two crates of that name.
        let mut spec = spec(split(), &[("D", "tiny_nodes::D")]);
        let bind = spec.bind.get_mut("D").expect("D");
        bind.package = "tiny_engine".into();
        bind.lib = "tiny_engine".into();
        let error = check(&spec, &manifest())
            .expect_err("collision")
            .to_string();
        assert!(
            error.contains("segment `engine`: crate name `tiny_engine` is both the segment's library and package `tiny_engine`"),
            "{error}"
        );
        // An explicit library name counts, not the package name.
        let bind = spec.bind.get_mut("D").expect("D");
        bind.package = "tiny-nodes".into();
        bind.lib = "tiny_engine".into();
        assert!(check(&spec, &manifest()).is_err());
        spec.bind.get_mut("D").expect("D").lib = "tiny_nodes".into();
        assert!(check(&spec, &manifest()).is_ok());
    }

    /// A library named like a keyword is valid in cargo; Rust names it with
    /// a raw identifier, and the spec's type must too.
    #[test]
    fn a_keyword_library_takes_a_raw_type_path() {
        let mut spec = spec(split(), &[("D", "r#async::D")]);
        spec.bind.get_mut("D").expect("D").lib = "async".into();
        assert!(check(&spec, &manifest()).is_ok());

        spec.bind.get_mut("D").expect("D").ty = "async::D".into();
        let error = check(&spec, &manifest())
            .expect_err("not a path")
            .to_string();
        assert!(
            error.contains("[bind.D]: type `async::D` is not a Rust type path"),
            "{error}"
        );
    }

    #[test]
    fn a_type_must_come_from_its_bound_crate() {
        let spec = spec(split(), &[("D", "contract::D")]);
        let error = check(&spec, &manifest())
            .expect_err("foreign type")
            .to_string();
        assert!(
            error.contains("[bind.D]: type `contract::D` is not in crate `tiny_nodes`"),
            "{error}"
        );
    }

    #[test]
    fn a_dependency_may_not_take_a_reserved_key() {
        // Package `tokio`, library `engine_impl`: distinct crate names, but
        // the generated manifest would list `tokio` twice.
        let mut spec = spec(split(), &[("D", "engine_impl::D")]);
        let bind = spec.bind.get_mut("D").expect("D");
        bind.package = "tokio".into();
        bind.lib = "engine_impl".into();
        let error = check(&spec, &manifest())
            .expect_err("duplicate key")
            .to_string();
        assert!(
            error.contains("segment `engine`: dependency `tokio` is both the `tokio` dependency and [bind.D]'s package"),
            "{error}"
        );
    }

    #[test]
    fn generated_code_names_implementations_from_the_crate_root() {
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        let files = rendered(&spec, &manifest(), Path::new("/work/segments")).expect("render");
        let lib = &files[&PathBuf::from("engine/src/lib.rs")];
        let main = &files[&PathBuf::from("engine/src/main.rs")];
        assert!(lib.contains("pub d: ::tiny_nodes::D"), "{lib}");
        assert!(
            lib.contains("pub use ::tiny_contract as contract;"),
            "{lib}"
        );
        assert!(main.contains("build::<::tiny_nodes::D>"), "{main}");
    }

    /// Generated code names a crate by its library: a given offer whose
    /// paths begin with another library than the listed one is refused,
    /// as a read is.
    #[test]
    fn a_given_offer_names_its_library_for_its_package() {
        let offer = Offer {
            package: "proto".into(),
            version: "0.1.0".into(),
            source: CRATES_IO[0].into(),
            messages: vec![gdl_extract::MessageEntry {
                name: "Words".into(),
                path: "renamed::Words".into(),
                wire: true,
                public: true,
            }],
            exports: Vec::new(),
            nodes: Vec::new(),
        };
        let version = spec::CrateSource::Version("0.1.0".into());
        let error = given_offer(std::slice::from_ref(&offer), "proto", "proto", &version)
            .expect_err("renamed");
        assert_eq!(
            error,
            [
                "proto@0.1.0: its library is `renamed`, not `proto`; a crate listed by version must name its library for its package"
            ]
        );
        let mut named = offer.clone();
        named.messages[0].path = "proto::Words".into();
        assert!(given_offer(&[named], "proto", "proto", &version).is_ok());
    }

    /// A crate listed by version is the registry's: a git package of the
    /// same name and version is another crate, whatever order the catalogue
    /// gives them in.
    #[test]
    fn a_version_is_matched_to_the_registry_offer() {
        let offer = |source: &str| Offer {
            package: "proto".into(),
            version: "0.1.0".into(),
            source: source.into(),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes: Vec::new(),
        };
        let git = offer("git+https://example.com/proto#abc");
        let registry = offer("registry+https://github.com/rust-lang/crates.io-index");
        let version = spec::CrateSource::Version("0.1.0".into());
        assert!(given_offer(std::slice::from_ref(&git), "proto", "proto", &version).is_err());
        // Another registry's package of that name and version is another
        // crate: the generated manifests name crates.io.
        let elsewhere = offer("sparse+https://registry.example.com/index/");
        assert!(given_offer(std::slice::from_ref(&elsewhere), "proto", "proto", &version).is_err());
        let sparse = offer("sparse+https://index.crates.io/");
        assert!(given_offer(std::slice::from_ref(&sparse), "proto", "proto", &version).is_ok());
        for given in [
            [git.clone(), registry.clone()],
            [registry.clone(), git.clone()],
        ] {
            let found = given_offer(&given, "proto", "proto", &version).expect("the registry's");
            assert_eq!(found.source, registry.source);
        }
    }

    /// A node crate listed by version is a dependency on exactly that
    /// version, in a generated crate and in an exported one alike.
    #[test]
    fn a_crate_listed_by_version_is_depended_on_exactly() {
        let mut spec = spec(split(), &[("D", "tiny_nodes::D")]);
        spec.bind.get_mut("D").expect("D").source = spec::CrateSource::Version("0.3.1".into());
        let out = Path::new("/work/segments");
        let manifest = manifest();
        let contract = spec.contract().expect("a contract spec");
        for deps in [emit::Deps::Relative, emit::Deps::Exported] {
            let files = render_manifest(&spec, contract, &manifest, out, deps).expect("render");
            let cargo = &files[&PathBuf::from("engine/Cargo.toml")];
            assert!(
                cargo.contains("tiny-nodes = { version = \"=0.3.1\" }\n"),
                "{cargo}"
            );
        }
    }

    #[test]
    fn a_type_from_the_crate_root_is_the_same_binding() {
        let out = Path::new("/work/segments");
        let plain =
            rendered(&spec(split(), &[("D", "tiny_nodes::D")]), &manifest(), out).expect("plain");
        let rooted = rendered(
            &spec(split(), &[("D", "::tiny_nodes::D")]),
            &manifest(),
            out,
        )
        .expect("rooted");
        assert_eq!(plain, rooted);
        let spaced = rendered(
            &spec(split(), &[("D", "  ::tiny_nodes::D ")]),
            &manifest(),
            out,
        )
        .expect("spaced");
        assert_eq!(plain, spaced);
        let mut module = spec(split(), &[("D", "tiny_nodes::D")]);
        let Source::Contract(contract) = &mut module.source else {
            unreachable!("a contract spec")
        };
        contract.module = "  ::tiny_contract ".into();
        let module = rendered(&module, &manifest(), out).expect("module");
        assert_eq!(plain, module);
    }

    #[test]
    fn a_crate_may_not_take_a_standard_library_name() {
        for name in ["std", "core", "alloc"] {
            let ty = format!("{name}::D");
            let mut spec = spec(split(), &[("D", &ty)]);
            spec.bind.get_mut("D").expect("D").lib = name.into();
            let error = check(&spec, &manifest()).expect_err("shadows").to_string();
            assert!(
                error.contains(&format!("crate name `{name}` is both the standard library")),
                "{error}"
            );
        }
    }

    #[test]
    fn a_package_bound_from_two_paths_is_refused() {
        let mut manifest = manifest();
        manifest.nodes.push(NodeInfo {
            name: "F".into(),
            role: Role::Backward,
            binding: Some("f".into()),
        });
        manifest.hops[0].backward.push("F".into());
        manifest.hops[0]
            .backward_types
            .push(TypeInfo::new("Item", true));
        let placement = gdl_model::Placement::new()
            .segment("front", ["S", "F"])
            .segment("engine", ["D"]);
        let mut spec = spec(placement, &[("D", "tiny_nodes::D"), ("F", "tiny_nodes::F")]);
        spec.bind.get_mut("F").expect("F").source =
            spec::CrateSource::Path("/elsewhere/nodes".into());
        let error = check(&spec, &manifest).expect_err("two paths").to_string();
        assert!(
            error.contains("[bind.D] and [bind.F] give package `tiny-nodes` two sources"),
            "{error}"
        );
        spec.bind.get_mut("F").expect("F").source = spec::CrateSource::Path("/work/./nodes".into());
        assert!(
            check(&spec, &manifest).is_ok(),
            "the same path, spelled two ways"
        );
        // Relative: two different directories, however many steps up.
        spec.bind.get_mut("D").expect("D").source = spec::CrateSource::Path("../../nodes".into());
        spec.bind.get_mut("F").expect("F").source = spec::CrateSource::Path("nodes".into());
        assert!(
            check(&spec, &manifest).is_err(),
            "`../../nodes` is not `nodes`"
        );
    }

    #[test]
    fn a_placement_error_is_reported() {
        let placement = gdl_model::Placement::new().segment("front", ["S"]);
        let spec = spec(placement, &[("D", "tiny_nodes::D")]);
        assert!(check(&spec, &manifest()).is_err());
    }

    #[test]
    fn drift_finds_changed_missing_and_stale_files() {
        let out = std::env::temp_dir().join(format!("gdl-gen-drift-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&out);
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        let files = rendered(&spec, &manifest(), &out).expect("render");
        write(&out, &files).expect("write");
        assert_eq!(drift(&out, &files).expect("drift"), Vec::<String>::new());

        let lib = out.join("engine/src/lib.rs");
        let edited = std::fs::read_to_string(&lib).expect("read") + "\n// hand edit\n";
        std::fs::write(&lib, edited).expect("edit");
        std::fs::remove_file(out.join("front/Cargo.toml")).expect("remove");
        std::fs::create_dir_all(out.join("gone/src")).expect("dir");
        std::fs::write(out.join("gone/src/lib.rs"), format!("// {MARKER}.\n")).expect("stale");
        std::fs::write(out.join("notes.txt"), "not generated\n").expect("user file");
        // Unmarked, but inside a generated crate, where Cargo would use it.
        std::fs::write(out.join("front/build.rs"), "fn main() {}\n").expect("build script");
        assert_eq!(
            drift(&out, &files).expect("drift"),
            [
                "differs: engine/src/lib.rs",
                "missing: front/Cargo.toml",
                "stale: gone/src/lib.rs",
                "unexpected: front/build.rs",
            ]
        );
        let refused = write(&out, &files).expect_err("a foreign file in a crate");
        assert!(refused.to_string().contains("front/build.rs"), "{refused}");
        assert!(
            out.join("gone/src/lib.rs").exists(),
            "a refused write changes nothing"
        );

        std::fs::remove_file(out.join("front/build.rs")).expect("remove");
        let removed = write(&out, &files).expect("rewrite");
        assert_eq!(removed, [PathBuf::from("gone/src/lib.rs")]);
        assert_eq!(drift(&out, &files).expect("drift"), Vec::<String>::new());
        assert!(
            out.join("notes.txt").exists(),
            "a file outside the crates is kept"
        );

        // A cargo build directory is skipped, whatever its name.
        std::fs::create_dir_all(out.join("engine/target/debug")).expect("dir");
        std::fs::write(
            out.join("engine/target/CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .expect("tag");
        std::fs::write(out.join("engine/target/debug/out"), "build output\n").expect("output");
        assert_eq!(drift(&out, &files).expect("drift"), Vec::<String>::new());
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn generation_never_replaces_a_file_it_did_not_write() {
        let out = std::env::temp_dir().join(format!("gdl-gen-collide-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&out);
        std::fs::create_dir_all(out.join("front/src")).expect("dir");
        let handwritten = "[package]\nname = \"mine\"\n";
        std::fs::write(out.join("front/Cargo.toml"), handwritten).expect("write");
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        let files = rendered(&spec, &manifest(), &out).expect("render");
        let refused = write(&out, &files).expect_err("a handwritten file is in the way");
        assert!(
            refused.to_string().contains("front/Cargo.toml"),
            "{refused}"
        );
        assert_eq!(
            std::fs::read_to_string(out.join("front/Cargo.toml")).expect("read"),
            handwritten
        );
        assert!(
            !out.join("engine").exists(),
            "a refused write writes nothing"
        );
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn a_streaming_source_is_not_served_as_json_lines() {
        let out = std::path::Path::new("/work/segments");
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        let mut manifest = manifest();
        manifest.hops[0].forward_types = vec![TypeInfo::streamed("Req", true, "Chunk", true)];
        let files = rendered(&spec, &manifest, out).expect("render");
        let front = &files[&PathBuf::from("front/src/main.rs")];
        assert!(!front.contains("serve_json_lines"), "{front}");
        assert!(front.contains("streamed request"), "{front}");
        assert!(!files[&PathBuf::from("front/Cargo.toml")].contains("json-lines"));
    }

    #[test]
    fn only_the_source_segment_serves_json_lines() {
        let out = std::path::Path::new("/work/segments");
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        let files = rendered(&spec, &manifest(), out).expect("render");
        let front = &files[&PathBuf::from("front/src/main.rs")];
        let engine = &files[&PathBuf::from("engine/src/main.rs")];
        assert!(front.contains("gdl::serve_json_lines("));
        assert!(!engine.contains("serve_json_lines"));
        assert!(files[&PathBuf::from("front/Cargo.toml")].contains("json-lines"));
        assert!(!files[&PathBuf::from("engine/Cargo.toml")].contains("json-lines"));
        assert!(
            !front.contains("peer_info"),
            "generated code calls no velo method"
        );
        assert!(!files[&PathBuf::from("front/Cargo.toml")].contains("tiny-nodes"));
        assert!(files[&PathBuf::from("engine/Cargo.toml")].contains("tiny-nodes"));
    }
}
