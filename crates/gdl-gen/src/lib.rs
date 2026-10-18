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
//! and the crates that define those messages with `derive(gdl::Message)`.
//! [`resolve`] checks that the names agree along every edge and resolves
//! each to exactly one type.

mod contract;
mod dump;
mod emit;
mod names;
mod paths;
mod resolve;
mod spec;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use emit::MARKER;
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
    let prepared = prepare(spec, Some(out), target)?;
    let mut files = render_manifest(spec, &prepared.contract, &prepared.manifest, out)?;
    files.extend(prepared.files);
    Ok(files)
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
    let manifest = prepare(spec, None, target)?.manifest;
    let layout = gdl::layout(&manifest, &spec.placement).map_err(|e| Error::new(e.to_string()))?;
    match format {
        Format::Dot => manifest.to_dot(Some(&layout)),
        Format::Mermaid => manifest.to_mermaid(Some(&layout)),
    }
    .map_err(|e| Error::new(e.to_string()))
}

/// A graph file's message names, each resolved to one type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// Message name to the type it resolved to.
    pub messages: BTreeMap<String, gdl::MessageInfo>,
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

/// Check the spec's graph file and resolve its message names against the
/// catalogue of the crates `[messages]` lists, which is built in `target`
/// (a cargo target directory), as for [`render`]. The names are checked
/// before anything is compiled.
pub fn resolve(spec: &Spec, target: &Path) -> Result<Resolution, Error> {
    let spec::Source::Graph(source) = &spec.source else {
        return Err(Error::new(format!(
            "{}: resolving messages needs a [graph] spec; a contract crate names its own types",
            spec.file.display()
        )));
    };
    Ok(resolved(spec, source, target)?.1)
}

/// The graph file of `source`, checked, and its names resolved.
fn resolved(
    spec: &Spec,
    source: &spec::GraphSource,
    target: &Path,
) -> Result<(names::GraphFile, Resolution), Error> {
    let file = names::GraphFile::read(&source.path)?;
    let (catalogue, listed) = dump::catalogue(&source.crates, &spec.gdl, target)?;
    let messages = resolve::resolve(&file.uses, &catalogue, &listed, &source.pins)
        .map_err(|problems| Error::new(problems.join("\n")))?;
    Ok((file, Resolution { messages }))
}

/// The contract that a spec's segments build on, its manifest, and the
/// files of the contract crate when the generator writes it.
struct Prepared {
    /// The contract, at its place under the output directory.
    contract: spec::Contract,
    manifest: gdl::Manifest,
    /// Empty for a contract crate the spec names.
    files: Files,
}

/// The spec's contract and its manifest. A graph file's contract is
/// generated: its manifest is read from a copy under `target`, so that
/// `--check` and `--diagram` write nothing in the output, and its files
/// are rendered for `out/contract` when `out` is given.
fn prepare(spec: &Spec, out: Option<&Path>, target: &Path) -> Result<Prepared, Error> {
    let source = match &spec.source {
        spec::Source::Contract(contract) => {
            return Ok(Prepared {
                contract: contract.clone(),
                manifest: dump::manifest(contract, target)?,
                files: Files::new(),
            });
        }
        spec::Source::Graph(source) => source,
    };
    let (file, resolution) = resolved(spec, source, target)?;
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
    let lib_rs = contract::lib_rs(&file.graph, &resolution, &source.path);
    let crates = |path: &dyn Fn(&Path) -> String| {
        contract::cargo_toml(&package, &spec.gdl, &source.crates, path)
    };

    // Read the manifest from a copy whose paths are absolute, in a
    // directory named by its content: runs of other specs never share it.
    let absolute = |p: &Path| {
        dump::toml_string(
            &dump::absolute(p)
                .unwrap_or_else(|_| p.to_path_buf())
                .to_string_lossy(),
        )
    };
    let scratch_toml = crates(&absolute);
    let scratch = target.join("gdl-gen").join(format!(
        "contract-{:016x}",
        dump::hash(&(&scratch_toml, &lib_rs))
    ));
    let src = scratch.join("src");
    std::fs::create_dir_all(&src).map_err(|e| io(&src, e))?;
    dump::write(&scratch.join("Cargo.toml"), &scratch_toml)?;
    dump::write(&src.join("lib.rs"), &lib_rs)?;
    let mut contract = spec::Contract {
        package: package.clone(),
        lib: lib.clone(),
        path: scratch,
        module: gdl_syntax::identifier(&lib),
    };
    let manifest = dump::manifest(&contract, target).map_err(|e| {
        let text = e.to_string();
        // rustc's E0603 names the private module. The contract names each
        // message where it is defined, and a re-export elsewhere does not
        // make that path public.
        let modules: Vec<&str> = text
            .split("module `")
            .skip(1)
            .filter_map(|rest| rest.split_once("` is private").map(|(m, _)| m))
            .filter(|m| !m.contains('`'))
            .collect();
        let private: Vec<&str> = resolution
            .messages
            .values()
            .map(|m| m.path.as_str())
            .filter(|path| {
                let mut segments = path.split("::");
                let _crate = segments.next();
                segments.any(|s| modules.contains(&s))
            })
            .collect();
        if private.is_empty() {
            return e;
        }
        Error::new(format!(
            "{text}\na message must be public at the path where it is defined: {}",
            private
                .iter()
                .map(|p| format!("`{p}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;

    let mut files = Files::new();
    if let Some(out) = out {
        let dir = dump::absolute(out)?.join("contract");
        let relative = |p: &Path| {
            let p = dump::absolute(p).unwrap_or_else(|_| p.to_path_buf());
            dump::toml_string(&paths::display(&paths::relative(&dir, &p)))
        };
        files.insert(PathBuf::from("contract/Cargo.toml"), crates(&relative));
        files.insert(PathBuf::from("contract/src/lib.rs"), lib_rs);
        contract.path = dir;
    }
    Ok(Prepared {
        contract,
        manifest,
        files,
    })
}

fn render_manifest(
    spec: &Spec,
    contract: &spec::Contract,
    manifest: &gdl::Manifest,
    out: &Path,
) -> Result<Files, Error> {
    let layout = validate(spec, contract, manifest)?;
    let out = dump::absolute(out)?;
    emit::files(spec, contract, manifest, &layout, &out)
}

/// Check the placement and the bindings against the manifest, reporting
/// every problem found.
fn validate(
    spec: &Spec,
    contract: &spec::Contract,
    manifest: &gdl::Manifest,
) -> Result<gdl::Layout, Error> {
    let mut problems = spec.problems.clone();
    let layout = match gdl::layout(manifest, &spec.placement) {
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
                problems.push(format!(
                    "segment `{segment}` becomes package `{package}`, the name of [bind.{node}]'s crate"
                ));
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
                    format!("[bind.{first}]'s package"),
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
    let mut sources: BTreeMap<&str, (PathBuf, &str)> = BTreeMap::new();
    for (node, bind) in &spec.bind {
        match manifest.node(node) {
            None => problems.push(format!("[bind.{node}]: the graph has no node `{node}`")),
            Some(info) if info.role == gdl::Role::Source => problems.push(format!(
                "[bind.{node}]: `{node}` is the source, which takes no implementation"
            )),
            Some(_) => {}
        }
        if bind.ty.trim().is_empty() {
            problems.push(format!("[bind.{node}]: `type` is empty"));
        } else {
            match type_crate(&bind.ty) {
                None => problems.push(format!(
                    "[bind.{node}]: type `{}` is not a Rust type path",
                    bind.ty
                )),
                // The bound crate is the only one of the node's that the
                // segment depends on; a type from anywhere else cannot
                // resolve.
                Some(krate) if krate != bind.lib => problems.push(format!(
                    "[bind.{node}]: type `{}` is not in crate `{}`",
                    bind.ty, bind.lib
                )),
                Some(_) => {}
            }
        }
        let path = dump::absolute(&bind.path).unwrap_or_else(|_| paths::normalize(&bind.path));
        match sources.get(bind.package.as_str()) {
            Some((first, other)) if *first != path => problems.push(format!(
                "[bind.{other}] and [bind.{node}] give package `{}` two paths",
                bind.package
            )),
            Some(_) => {}
            None => {
                sources.insert(&bind.package, (path, node));
            }
        }
    }
    for node in &manifest.nodes {
        if node.role != gdl::Role::Source && !spec.bind.contains_key(&node.name) {
            problems.push(format!("node `{}` has no [bind.{}]", node.name, node.name));
        }
        if node.role != gdl::Role::Source && node.binding.is_none() {
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
    use gdl::{HopInfo, Manifest, NodeInfo, Role, TypeInfo};

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

    fn spec(placement: gdl::Placement, bind: &[(&str, &str)]) -> Spec {
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
                        path: "/work/nodes".into(),
                        ty: (*ty).into(),
                    };
                    ((*node).to_string(), bind)
                })
                .collect(),
            problems: Vec::new(),
        }
    }

    fn check(spec: &Spec, manifest: &Manifest) -> Result<gdl::Layout, Error> {
        validate(spec, spec.contract().expect("a contract spec"), manifest)
    }

    fn rendered(spec: &Spec, manifest: &Manifest, out: &Path) -> Result<Files, Error> {
        render_manifest(
            spec,
            spec.contract().expect("a contract spec"),
            manifest,
            out,
        )
    }

    fn split() -> gdl::Placement {
        gdl::Placement::new()
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
            prepare(&spec, None, &target).expect("prepared").manifest
        };
        let (one, other) = (manifest("pinned.toml"), manifest("pinned-other.toml"));
        let chunk = |m: &Manifest| m.hops[0].backward_types[0].name.clone();
        assert_eq!(chunk(&one), ":: chat_protocols :: Chunk");
        assert_eq!(chunk(&other), ":: other_protocols :: Chunk");
        assert_ne!(one.contract_hash(), other.contract_hash());
    }

    #[test]
    fn a_complete_spec_validates() {
        let spec = spec(split(), &[("D", "tiny_nodes::D")]);
        assert!(check(&spec, &manifest()).is_ok());
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let placement = gdl::Placement::new()
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
        let placement = gdl::Placement::new()
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

    #[test]
    fn a_segment_package_may_not_take_a_dependency_name() {
        // `tiny-contract` and `tiny-nodes` are the contract and the
        // implementation; Cargo cannot build two path packages of one name.
        // A contract generated from a graph file is `<graph>-contract` in
        // `contract/`, so this check also keeps a segment out of its
        // directory.
        let placement = gdl::Placement::new()
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
        let placement = gdl::Placement::new()
            .segment("front", ["S", "F"])
            .segment("engine", ["D"]);
        let mut spec = spec(placement, &[("D", "tiny_nodes::D"), ("F", "tiny_nodes::F")]);
        spec.bind.get_mut("F").expect("F").path = "/elsewhere/nodes".into();
        let error = check(&spec, &manifest).expect_err("two paths").to_string();
        assert!(
            error.contains("[bind.D] and [bind.F] give package `tiny-nodes` two paths"),
            "{error}"
        );
        spec.bind.get_mut("F").expect("F").path = "/work/./nodes".into();
        assert!(
            check(&spec, &manifest).is_ok(),
            "the same path, spelled two ways"
        );
        // Relative: two different directories, however many steps up.
        spec.bind.get_mut("D").expect("D").path = "../../nodes".into();
        spec.bind.get_mut("F").expect("F").path = "nodes".into();
        assert!(
            check(&spec, &manifest).is_err(),
            "`../../nodes` is not `nodes`"
        );
    }

    #[test]
    fn a_placement_error_is_reported() {
        let placement = gdl::Placement::new().segment("front", ["S"]);
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
