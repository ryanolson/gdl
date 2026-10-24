//! Generates one crate per segment of a gdl graph placement.
//!
//! The input is a spec ([`Spec`]): the contract crate holding the graph's
//! `graph!` invocation, the placement of nodes into segments, and the crate
//! and type implementing each node. The generator reads the contract's
//! manifest by compiling the contract alone (a graph spec, below, compiles
//! nothing), validates the placement and the bindings against it, and
//! writes, per segment, a crate with a library
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
//! read from the crates' source by `gdl-extract`. Such a spec binds its
//! nodes through `[nodes]`: node crates that offer them, read from their
//! source. Nothing is compiled: the contract's manifest is built from the
//! graph file and the resolution, so a crate that needs CUDA, say,
//! composes on a machine without it. [`check`] validates a spec as
//! generating it would, writing nothing, with the crates' offers read from
//! source or given by a loaded catalogue.

mod contract;
mod deployment;
mod dump;
mod emit;
mod export;
mod extract;
mod fleet;
mod names;
mod nodes;
mod offers;
mod output;
mod paths;
mod resolve;
mod service;
mod spec;
mod validate;

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

pub use deployment::{ComposeBundle, compose, compose_bundle, compose_files};
use emit::MARKER;
pub use export::export;
pub use extract::{Crate, Extracted, add_to_catalog, extract, verify};
pub use fleet::check_files;
/// The working catalogue, its entries, and what a crate offers, read from
/// its source.
pub use gdl_extract::{Catalog, Entry, Offer};
use offers::offers;
pub use offers::{CRATES_IO, Offers};
pub use output::{drift, write};
pub use resolve::Resolved;
pub use spec::{Spec, runtime_at};
use validate::{library_clashes, validate};

/// A generation failure, with everything wrong at once where possible:
/// each problem, printed one to a line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(Vec<gdl_model::Problem>);

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self(vec![gdl_model::Problem::from(message.into())])
    }

    /// Every one of `problems`, at once.
    fn all<P: Into<gdl_model::Problem>>(problems: impl IntoIterator<Item = P>) -> Self {
        Self(problems.into_iter().map(Into::into).collect())
    }

    /// Each problem, those that name no file as read from `file`: the
    /// spec's problems, located in the spec.
    pub fn in_file(self, file: impl fmt::Display) -> gdl_model::Problems {
        gdl_model::Problems(self.0).in_file(file)
    }
}

impl From<gdl_model::Problems> for Error {
    fn from(problems: gdl_model::Problems) -> Self {
        Self(problems.0)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, problem) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{problem}")?;
        }
        Ok(())
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

/// [`render`], with dependencies named as `deps` says and offers from
/// `from`, and the graph's name.
fn render_with(
    spec: &Spec,
    out: &Path,
    target: &Path,
    deps: emit::Deps,
    from: Offers<'_>,
) -> Result<(Files, String), Error> {
    spec.gdl.check()?;
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
    let offers = offers(&source.nodes, target, from)?;
    let (bind, problems) = nodes::bind(
        graph,
        &source.nodes,
        &offers,
        &resolution.messages,
        &source.node_pins,
    );
    let mut with_binds = spec.clone();
    for (name, binding) in &bind {
        if let Some(node) = offers
            .iter()
            .flat_map(|o| &o.nodes)
            .find(|n| n.ty == binding.ty)
        {
            let values = spec.config.get(name).cloned().unwrap_or_default();
            match gdl_model::resolve_config(&node.config, &values) {
                Ok(values) => {
                    with_binds.config.insert(name.clone(), values);
                }
                Err(e) => with_binds.problems.push(format!("[config.{name}]: {e}")),
            }
        }
    }
    with_binds.bind = bind;
    service::bind(
        &mut with_binds,
        graph,
        &source.nodes,
        &offers,
        &resolution.messages,
    );
    for name in spec.config.keys() {
        let adapter = with_binds
            .service
            .as_ref()
            .is_some_and(|s| s.nodes().iter().any(|n| &n.name == name));
        if !with_binds.bind.contains_key(name) && !adapter {
            with_binds.problems.push(format!(
                "[config.{name}]: the graph has no bound node `{name}`"
            ));
        }
    }
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
    let check = checked(spec, target, from).0?;
    // What an export of it would refuse.
    let problems = export::refusals(spec, &check.manifest.name);
    if !problems.is_empty() {
        return Err(Error::all(problems));
    }
    Ok(check)
}

/// `spec` checked as [`render`] checks it, and its layout whenever its
/// placement fits its graph, whatever else is wrong with it: a fleet file
/// is checked against the layout even when a binding is not.
pub(crate) fn checked(
    spec: &Spec,
    target: &Path,
    from: Offers<'_>,
) -> (Result<Check, Error>, Option<gdl_model::Layout>) {
    let prepared = prepare(spec, None, target, emit::Deps::Relative, from);
    let layout = prepared
        .as_ref()
        .ok()
        .and_then(|prepared| gdl_model::layout(&prepared.manifest, &spec.placement).ok());
    let check = || {
        spec.gdl.check()?;
        let prepared = prepared?;
        let spec = bound(spec, &prepared, target, from)?;
        let layout = validate(&spec, &prepared.contract, &prepared.manifest)?;
        let mermaid = prepared
            .manifest
            .to_mermaid(Some(&layout))
            .map_err(|e| Error::new(e.to_string()))?;
        Ok(Check {
            crossings: layout.report().to_string(),
            mermaid: service::diagram(&spec, &prepared.manifest, mermaid),
            manifest: prepared.manifest,
        })
    };
    (check(), layout)
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
    let offers = offers(&source.crates, target, from)?;
    let messages = resolve::resolve(&file.uses, &offers, &source.pins).map_err(Error::all)?;
    Ok((file, Resolution { messages }))
}

/// The contract that a spec's segments build on, its manifest, and the
/// files of the contract crate when the generator writes it.
struct Prepared {
    /// The contract: a contract spec's crate, or a graph file's generated
    /// one, at its place under the output directory when the files are
    /// rendered (a placeholder under `target` otherwise, which nothing
    /// reads).
    contract: spec::Contract,
    manifest: gdl_model::Manifest,
    /// Empty for a contract crate the spec names.
    files: Files,
    /// A graph file's graph and its resolution: read once, so that
    /// everything generated from it, the contract and the bindings, comes
    /// from one version of it.
    graph: Option<(gdl_syntax::Graph, Resolution)>,
}

/// The spec's contract and its manifest. A contract spec's manifest is read
/// by compiling its crate. A graph file's contract is generated and nothing
/// is compiled: its manifest is built from the graph and the resolution,
/// and its files are rendered for `out/contract` when `out` is given.
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
        return Err(Error::all(clashes));
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
            Some(gdl_model::TypeInfo::new(
                resolved.path.clone(),
                resolved.wire,
                resolved.encoding,
            ))
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

#[cfg(test)]
mod tests;
