//! `cargo gdl extract`: what a crate offers to graphs, read from its source
//! into a working-catalogue entry, and `--verify`, which compares that with
//! what a build of the crate registers.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use gdl_extract::{Catalog, Entry, Located, Offer, Skipped, Workspace};

use crate::Error;

/// A crate to extract: a directory, a version on its registry, or a
/// package of a Git repository at a commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Crate {
    /// The crate in this directory.
    Path(PathBuf),
    /// `name@version`, from crates.io.
    Registry {
        /// The package name.
        name: String,
        /// Its exact version.
        version: String,
    },
    /// A package of a Git repository at a commit: the one at `path` within
    /// it, which must be the one cargo resolves by `name`.
    Git {
        /// The package name.
        name: String,
        /// The repository's URL.
        url: String,
        /// The commit.
        rev: String,
        /// The package's directory within the repository; empty for the
        /// root.
        path: PathBuf,
    },
}

impl Crate {
    /// A directory, or `name@version` that is not one.
    pub fn parse(text: &str) -> Result<Self, Error> {
        Self::parse_in(Path::new(""), text)
    }

    /// [`Crate::parse`], with a directory relative to `base`: the one rule
    /// for a crate named by a command line, the composer, or a spec.
    pub(crate) fn parse_in(base: &Path, text: &str) -> Result<Self, Error> {
        let path = base.join(text);
        match text.split_once('@') {
            Some((name, version)) if !path.exists() => {
                if name.is_empty() || version.is_empty() {
                    return Err(Error::new(format!(
                        "`{text}` is neither a directory nor `name@version`"
                    )));
                }
                Ok(Self::Registry {
                    name: name.to_string(),
                    version: version.to_string(),
                })
            }
            _ => Ok(Self::Path(path)),
        }
    }
}

/// The crate read, and where it is.
pub struct Extracted {
    /// What it offers.
    pub offer: Offer,
    workspace: Workspace,
    located: Located,
}

/// Read what `krate` offers. Nothing is built; a registry crate is
/// resolved from a manifest of our own under `target`. Every problem the
/// reader meets is an error: an entry is written for a crate read whole.
pub fn extract(
    krate: &Crate,
    target: &Path,
    cargo: &gdl_extract::Cargo,
) -> Result<Extracted, Error> {
    // Cargo reports absolute directories; a relative target (the default
    // of both binaries) must compare as one.
    let target = std::path::absolute(target)
        .map_err(|e| Error::new(format!("{}: {e}", target.display())))?;
    let target = target.as_path();
    let (workspace, located, extraction) = match krate {
        Crate::Path(dir) => {
            let workspace = Workspace::load(&dir.join("Cargo.toml"), cargo)
                .map_err(|e| Error::new(format!("{}: {e}", dir.display())))?;
            let missing = || {
                Error::new(format!(
                    "{}: cargo metadata resolves no package there",
                    dir.display()
                ))
            };
            let located = workspace.locate_in(dir).ok_or_else(missing)?;
            let extraction = workspace.extract_in(dir).ok_or_else(missing)?;
            (workspace, located, extraction)
        }
        Crate::Registry { name, version } => {
            let scratch = target
                .join("gdl-gen")
                .join(format!("fetch-{name}-{version}"));
            let workspace = Workspace::fetch(name, version, &scratch, cargo)
                .map_err(|e| Error::new(format!("{name}@{version}: {e}")))?;
            let spec = format!("{name}@{version}");
            let located = workspace
                .locate(&spec)
                .map_err(|e| Error::new(e.to_string()))?;
            fetched_only(&workspace, &scratch)?;
            let extraction = workspace
                .extract(&spec)
                .map_err(|e| Error::new(e.to_string()))?;
            (workspace, located, extraction)
        }
        Crate::Git {
            name,
            url,
            rev,
            path,
        } => {
            // Cargo resolves a Git package by name, as a generated crate's
            // dependency will: the catalogue must say what the build will
            // link. So the package is read through cargo, and refused when
            // the one cargo resolves is not the directory chosen.
            let workspace = Workspace::fetch_git(name, url, rev, target, cargo)
                .map_err(|e| Error::new(e.to_string()))?;
            let located = workspace
                .locate(name)
                .map_err(|e| Error::new(e.to_string()))?;
            let chosen: PathBuf = path
                .components()
                .filter(|c| matches!(c, std::path::Component::Normal(_)))
                .collect();
            // The package cargo resolved is the one chosen when it is the
            // chosen path below the checkout's root.
            let resolved = gdl_extract::checkout_root(&located.dir)
                .and_then(|root| located.dir.strip_prefix(root).ok())
                .ok_or_else(|| {
                    Error::new(format!(
                        "{}: not where cargo keeps its checkouts",
                        located.dir.display()
                    ))
                })?;
            if resolved != chosen {
                return Err(Error::new(format!(
                    "The repository has more than one package named `{name}`: a build would link `{}`, not `{}`. Rename one, or import the one cargo resolves.",
                    resolved.display(),
                    chosen.display()
                )));
            }
            let scratch = Workspace::git_scratch(name, url, rev, target);
            fetched_only(&workspace, &scratch)?;
            let extraction = workspace
                .extract(name)
                .map_err(|e| Error::new(e.to_string()))?;
            (workspace, located, extraction)
        }
    };
    if !extraction.problems.is_empty() {
        return Err(Error::all(extraction.problems));
    }
    Ok(Extracted {
        offer: extraction.offer,
        workspace,
        located,
    })
}

/// Add the extracted crate to the working catalogue in `catalogue`, as
/// `<name>/<version>.toml`, with its checksum and the time `added`, and
/// return the entry's file. A published version already there is kept as
/// it is; a crate from a directory replaces its entry, whatever version it
/// had.
pub fn add_to_catalog(
    catalogue: &Path,
    extracted: &Extracted,
    added: std::time::SystemTime,
) -> Result<PathBuf, Error> {
    let mut catalog = Catalog::load(catalogue).map_err(|e| Error::new(e.to_string()))?;
    let (_, file) = catalog
        .add(extracted.entry(added))
        .map_err(|e| Error::new(e.to_string()))?;
    Ok(file)
}

impl Extracted {
    /// The crate as a catalogue entry, added at `added`.
    pub fn entry(&self, added: std::time::SystemTime) -> Entry {
        Entry {
            added: gdl_extract::rfc3339(added),
            checksum: self.located.checksum.clone(),
            offer: self.offer.clone(),
        }
    }
}

/// Build a program that links the extracted crate, in `target`, and
/// compare what its markers register with what the reader found. Each
/// difference is an error: something the reader missed (a type a macro
/// made, code a build script wrote) or something the build does not
/// register. An alias the reader leaves out on purpose, with the reason
/// in the offer's `skipped`, is no difference: it is returned, with that
/// reason, for the caller to report.
pub fn verify(extracted: &Extracted, target: &Path) -> Result<Vec<String>, Error> {
    let (registered, mine) = registrations(extracted, target, "verify")?;
    let problems = differences(&registered, &mine, &extracted.offer);
    if problems.is_empty() {
        Ok(left_out(&registered, &mine, &extracted.offer))
    } else {
        Err(Error::all(problems))
    }
}

/// Build the program [`verify`] builds, in `target`, and record on each
/// instantiation of a generic message the extracted crate exports whether
/// the build finds it a wire type. The build sees serde impls and bounds
/// the reader cannot judge, so its reading takes precedence. It builds the
/// crate's default features. A crate that exports no instantiation has
/// nothing to record, and nothing is built. Each instantiation the build
/// does not register keeps the reader's reading, and is returned as a note
/// for the caller to report.
pub fn probe(extracted: &mut Extracted, target: &Path) -> Result<Vec<String>, Error> {
    if !extracted.offer.exports.iter().any(|e| e.is_instantiation()) {
        return Ok(Vec::new());
    }
    let (registered, mine) = registrations(extracted, target, "probe")?;
    let mut notes = Vec::new();
    for export in extracted
        .offer
        .exports
        .iter_mut()
        .filter(|e| e.is_instantiation())
    {
        // An instantiation is compared by its path, as `--verify` compares
        // it: a build cannot name the packages of its arguments.
        let built = registered
            .exports
            .iter()
            .find(|e| mine(&e.dir) && e.path == export.path && e.origin.path == export.origin.path);
        match built {
            Some(built) => export.probed = Some(built.wire),
            None => notes.push(format!(
                "the build registers no `{}` of `{}`: its wire capability is the reader's",
                export.path, export.origin.path
            )),
        }
    }
    Ok(notes)
}

/// What a program that links the extracted crate registers, built in
/// `target`, and a test for the build directory of an entry that tells
/// the crate's own entries from those of its dependencies. `command` names
/// the caller in the error for a crate that registers nothing.
fn registrations(
    extracted: &Extracted,
    target: &Path,
    command: &str,
) -> Result<(gdl_message::Catalogue, impl Fn(&str) -> bool + use<>), Error> {
    let located = &extracted.located;
    // The crate's own `gdl-message`, the nearest one: a workspace may hold
    // another, and a dependency for another platform may reach another.
    let message = match extracted
        .workspace
        .nearest_dependencies(&located.dir, "gdl-message")
        .as_slice()
    {
        [one] => one.clone(),
        [] => {
            return Err(Error::new(format!(
                "{} does not depend on gdl-message, so it registers nothing to {command}",
                located.name
            )));
        }
        several => {
            return Err(Error::new(format!(
                "{} reaches {} packages gdl-message in as many steps ({}), so `--{command}` cannot tell which one it builds with",
                located.name,
                several.len(),
                several
                    .iter()
                    .map(|m| format!("{} from {}", m.version, m.source))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    };
    // The crate under an alias of the program's own: its library's name
    // may be a keyword, or `gdl_message`.
    let dependencies = format!(
        "{}{}{}",
        dependency("gdl-message", &message)?,
        dependency("gdl_verified", located)?,
        patches(extracted.workspace.root())
    );
    let main = "use gdl_verified as _;\n\n\
                fn main() {\n    print!(\"{}\", gdl_message::Catalogue::linked().to_toml());\n}\n";
    // The lock of the resolve the reader read: the program builds the
    // versions the reader saw, and Cargo needs no registry index for them.
    let lock = std::fs::read_to_string(extracted.workspace.root().join("Cargo.lock")).ok();
    let name = format!(
        "gdl-verify-{:016x}",
        crate::dump::hash(&(&dependencies, main, &lock))
    );
    let text = crate::dump::run(
        target,
        &name,
        "the crate's registrations",
        &dependencies,
        main,
        lock.as_deref(),
    )?;
    let registered = gdl_message::Catalogue::from_toml(&text)
        .map_err(|e| Error::new(format!("reading the crate's registrations: {e}")))?;

    // Only this crate's registrations: a dependency registers its own.
    let dir = std::fs::canonicalize(&located.dir).unwrap_or_else(|_| located.dir.clone());
    let mine = move |d: &str| std::fs::canonicalize(d).is_ok_and(|d| d == dir);
    Ok((registered, mine))
}

/// The `[dependencies]` line that takes the package `at` under `key` from
/// the source the reader's lock resolved it from: a path, a Git repository,
/// or a registry version.
///
/// A Git package keeps the reference its source names (a branch, a tag, a
/// revision, or none), never the commit: the seeded lock pins the commit,
/// and any other reference is another source to Cargo, so the program
/// would link a second copy of the package beside the crate's own. Two
/// copies of `gdl-message` declare one linker section twice, and the
/// program stops before it prints anything.
fn dependency(key: &str, at: &Located) -> Result<String, Error> {
    let package = crate::dump::toml_string(&at.name);
    if at.path {
        return Ok(format!(
            "{key} = {{ package = {package}, path = {} }}\n",
            crate::dump::toml_string(&at.dir.to_string_lossy())
        ));
    }
    let Some(git) = at.source.strip_prefix("git+") else {
        return Ok(format!(
            "{key} = {{ package = {package}, version = {} }}\n",
            crate::dump::toml_string(&format!("={}", at.version))
        ));
    };
    // `cargo metadata` names a Git source `<url>[?<kind>=<name>]#<commit>`,
    // with the name as written in the manifest, not percent-encoded.
    let git = git.rsplit_once('#').map_or(git, |(git, _commit)| git);
    let (url, reference) = match git.split_once('?') {
        None => (git, String::new()),
        Some((url, query)) => match query.split_once('=') {
            Some((kind @ ("branch" | "tag" | "rev"), name)) => (
                url,
                format!(", {kind} = {}", crate::dump::toml_string(name)),
            ),
            _ => {
                return Err(Error::new(format!(
                    "{} {}: cannot read the reference of its Git source `{}`",
                    at.name, at.version, at.source
                )));
            }
        },
    };
    Ok(format!(
        "{key} = {{ package = {package}, git = {}{reference} }}\n",
        crate::dump::toml_string(url)
    ))
}

/// The `[patch]` tables of the workspace manifest in `root`, with each
/// relative path made absolute, for the program's manifest. The lock
/// names what a patch resolved to, but Cargo applies a patch only from the
/// manifest: without it, the crate's own dependency on a patched package
/// asks the original source again.
fn patches(root: &Path) -> String {
    let Some(mut patch) = std::fs::read_to_string(root.join("Cargo.toml"))
        .ok()
        .and_then(|text| text.parse::<toml::Table>().ok())
        .and_then(|mut manifest| manifest.remove("patch"))
    else {
        return String::new();
    };
    let specs = patch
        .as_table_mut()
        .into_iter()
        .flat_map(|sources| sources.iter_mut())
        .filter_map(|(_, packages)| packages.as_table_mut())
        .flat_map(|packages| packages.iter_mut())
        .filter_map(|(_, spec)| spec.as_table_mut());
    for spec in specs {
        if let Some(toml::Value::String(path)) = spec.get_mut("path") {
            *path = root.join(&*path).to_string_lossy().into_owned();
        }
    }
    let mut table = toml::Table::new();
    table.insert("patch".into(), patch);
    format!("\n{table}")
}

/// Each difference between what a build registered for the crate whose
/// registrations `mine` picks, and what the reader found in it, `offer`.
/// Entries are compared as lines that hold everything an entry claims: a
/// message's wire capability, an export's origin and the wire capability
/// the build probes where it exports the message, and each port's
/// messages and shape.
fn differences(
    registered: &gdl_message::Catalogue,
    mine: &dyn Fn(&str) -> bool,
    offer: &Offer,
) -> Vec<String> {
    // An alias the reader leaves out on purpose is reported by
    // `left_out`, not compared.
    let ours = |dir: &str, skipped: &dyn Fn(&Skipped) -> bool| {
        mine(dir) && !offer.skipped.iter().any(skipped)
    };
    let built: BTreeSet<String> = registered
        .messages
        .iter()
        .filter(|m| mine(&m.dir))
        .map(|m| {
            format!(
                "message `{}` at `{}`, {}",
                m.name,
                m.path,
                wire(m.wire, m.encoding)
            )
        })
        .chain(
            registered
                .exports
                .iter()
                .filter(|e| ours(&e.dir, &|s| export_of(s, e)))
                .map(|e| {
                    format!(
                        "export `{}` at `{}`, of `{}` ({} {}), {}",
                        e.name,
                        e.path,
                        e.origin.path,
                        e.origin.package,
                        e.origin.version,
                        wire(e.wire, e.encoding)
                    )
                }),
        )
        .chain(
            registered
                .nodes
                .iter()
                .filter(|n| ours(&n.dir, &|s| node_of(s, n)))
                .map(|n| {
                    let roles: Vec<String> = n
                        .roles
                        .iter()
                        .map(|r| {
                            let ports: Vec<String> = r
                                .ports
                                .iter()
                                .map(|p| {
                                    port(
                                        &p.port,
                                        p.head.as_ref().map(|o| (&o.package, &o.version, &o.path)),
                                        p.streamed.then(|| {
                                            p.item
                                                .as_ref()
                                                .map(|o| (&o.package, &o.version, &o.path))
                                        }),
                                        p.nested,
                                    )
                                })
                                .collect();
                            let entries: Vec<_> = r
                                .entries
                                .iter()
                                .map(|e| {
                                    let shape = |p: &gdl_message::PortInfo| {
                                        port(
                                            "",
                                            p.head
                                                .as_ref()
                                                .map(|o| (&o.package, &o.version, &o.path)),
                                            p.streamed.then(|| {
                                                p.item
                                                    .as_ref()
                                                    .map(|o| (&o.package, &o.version, &o.path))
                                            }),
                                            p.nested,
                                        )
                                    };
                                    format!(
                                        "{} {} {} {} {}",
                                        e.name,
                                        e.path,
                                        e.enabled,
                                        shape(&e.request),
                                        shape(&e.response)
                                    )
                                })
                                .collect();
                            format!("{}({}) entries {:?}", r.role, ports.join(", "), entries)
                        })
                        .collect();
                    format!(
                        "node `{}` at `{}`: {} config {:?} metrics {:?}",
                        n.name,
                        n.path,
                        roles.join(" "),
                        n.config,
                        n.metrics
                    )
                }),
        )
        .collect();
    let read: BTreeSet<String> = offer
        .messages
        .iter()
        .map(|m| {
            format!(
                "message `{}` at `{}`, {}",
                m.name,
                m.path,
                wire(m.wire, m.encoding)
            )
        })
        .chain(offer.exports.iter().map(|e| {
            format!(
                "export `{}` at `{}`, of `{}` ({} {}), {}",
                e.name,
                e.path,
                e.origin.path,
                e.origin.package,
                e.origin.version,
                wire(e.wire, e.encoding)
            )
        }))
        .chain(offer.nodes.iter().map(|n| {
            let ports: Vec<String> = n
                .ports
                .iter()
                .map(|p| {
                    port(
                        &p.port,
                        p.head.as_ref().map(|o| (&o.package, &o.version, &o.path)),
                        p.item
                            .as_ref()
                            .map(|o| Some((&o.package, &o.version, &o.path))),
                        // The reader offers no nested stream: such a port
                        // is a problem, not an entry.
                        false,
                    )
                })
                .collect();
            let entries: Vec<_> = n
                .entries
                .iter()
                .map(|e| {
                    let shape = |p: &gdl_extract::PortEntry| {
                        port(
                            "",
                            p.head.as_ref().map(|o| (&o.package, &o.version, &o.path)),
                            p.item
                                .as_ref()
                                .map(|o| Some((&o.package, &o.version, &o.path))),
                            false,
                        )
                    };
                    format!(
                        "{} {} {} {} {}",
                        e.name,
                        e.path,
                        e.enabled,
                        shape(&e.request),
                        shape(&e.response)
                    )
                })
                .collect();
            format!(
                "node `{}` at `{}`: {}({}) entries {:?} config {:?} metrics {:?}",
                n.name,
                n.ty,
                n.role,
                ports.join(", "),
                entries,
                n.config,
                n.metrics
            )
        }))
        .collect();
    let mut problems: Vec<String> = built
        .difference(&read)
        .map(|entry| format!("registered by the build, not read from source: {entry}"))
        .collect();
    problems.extend(
        read.difference(&built)
            .map(|entry| format!("read from source, not registered by the build: {entry}")),
    );
    problems
}

/// Each alias the reader leaves out on purpose that the build registers,
/// as an export or a node of the crate `mine` picks, with the reader's
/// reason.
fn left_out(
    registered: &gdl_message::Catalogue,
    mine: &dyn Fn(&str) -> bool,
    offer: &Offer,
) -> Vec<String> {
    offer
        .skipped
        .iter()
        .filter(|s| {
            registered
                .exports
                .iter()
                .any(|e| mine(&e.dir) && export_of(s, e))
                || registered
                    .nodes
                    .iter()
                    .any(|n| mine(&n.dir) && node_of(s, n))
        })
        .map(|s| {
            format!(
                "left out by the reader on purpose, registered by the build: `{}`: {}",
                s.name, s.problem
            )
        })
        .collect()
}

/// Whether the reader left out the export the build registered as `e`.
/// The match is by path, as the build registers it: two aliases of one
/// name in two modules are two exports, and a message's alias is no
/// node's.
fn export_of(s: &Skipped, e: &gdl_message::ExportInfo) -> bool {
    s.path.as_deref() == Some(e.path.as_str())
}

/// Whether the reader left out the node the build registered as `n`, by
/// path, as [`export_of`] matches.
fn node_of(s: &Skipped, n: &gdl_message::NodeInfo) -> bool {
    s.path.as_deref() == Some(n.path.as_str())
}

fn wire(wire: bool, encoding: gdl_model::Encoding) -> String {
    format!(
        "{} (codec {})",
        if wire { "wire" } else { "local" },
        encoding.name()
    )
}

/// A port, with each message by package, version, and path. `item` is
/// `Some` for a streamed request, with its item, `None` for `()`.
type Message<'a> = (&'a String, &'a String, &'a String);

fn port(
    name: &str,
    head: Option<Message<'_>>,
    item: Option<Option<Message<'_>>>,
    nested: bool,
) -> String {
    let one = |m: Option<Message<'_>>| {
        m.map_or("()".to_string(), |(package, version, path)| {
            format!("{path} ({package} {version})")
        })
    };
    let nested = if nested { ", nested" } else { "" };
    match item {
        None => format!("{name}={}{nested}", one(head)),
        Some(item) => format!("{name}={} + stream {}{nested}", one(head), one(item)),
    }
}

/// Refuse a fetched package whose dependencies are not all fetched too: a
/// path dependency naming a directory on this machine would be read here
/// and fail to build anywhere else. Each package is judged by where cargo
/// says it comes from and where, canonically, it put it: a registry
/// package wherever cargo keeps it (a vendored directory source replaces
/// one in place); every package of one Git source under one checkout
/// root, the one holding the package cargo resolved for that source (a
/// path dependency into another checkout, or through a committed symlink
/// to a directory elsewhere, shows a second root or none); and no path
/// package but the scratch manifest that asked. Cargo gives a Git
/// package's path dependency its parent's Git source, so the source alone
/// says nothing about where a package is; and a registry package's path
/// dependency is stripped at publication, so the path arm is reached only
/// by the scratch.
fn fetched_only(workspace: &Workspace, scratch: &Path) -> Result<(), Error> {
    let mut roots: BTreeMap<&str, BTreeSet<PathBuf>> = BTreeMap::new();
    let packages: Vec<Located> = workspace.located_all().collect();
    for package in &packages {
        let kind = package.source.split('+').next().unwrap_or_default();
        let dir = std::fs::canonicalize(&package.dir).unwrap_or_else(|_| package.dir.clone());
        let fetched = match kind {
            "registry" | "sparse" => true,
            "git" => match gdl_extract::checkout_root(&dir) {
                Some(root) => {
                    roots
                        .entry(package.source.as_str())
                        .or_default()
                        .insert(root.to_path_buf());
                    true
                }
                None => false,
            },
            "path" => package.name == "gdl-extract-fetch" && package.dir.starts_with(scratch),
            _ => false,
        };
        if !fetched {
            return Err(Error::new(format!(
                "package `{}` at {} is not a fetched package: a Git or registry package may not depend on a path on this machine",
                package.name,
                package.dir.display()
            )));
        }
    }
    if let Some((source, roots)) = roots.iter().find(|(_, roots)| roots.len() > 1) {
        return Err(Error::new(format!(
            "the packages of `{source}` are in {} checkouts; a Git package may not depend on a path outside its repository",
            roots.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use gdl_extract::{ExportEntry, MessageEntry, NodeEntry, PortEntry};
    use gdl_message::{Catalogue, ExportInfo, MessageInfo, NodeInfo, PortInfo, RoleInfo};

    use super::*;

    const PROTOCOLS: &str = "/crates/protocols";
    const FACADE: &str = "/crates/facade";

    fn built_origin(path: &str) -> gdl_message::Origin {
        gdl_message::Origin {
            package: "protocols".into(),
            version: "0.1.0".into(),
            dir: PROTOCOLS.into(),
            path: path.into(),
        }
    }

    fn read_origin(path: &str) -> gdl_extract::Origin {
        gdl_extract::Origin {
            package: "protocols".into(),
            version: "0.1.0".into(),
            source: format!("path+file://{PROTOCOLS}"),
            path: path.into(),
            arguments: Vec::new(),
        }
    }

    /// A facade that exports `protocols::Words`, and has one forward node
    /// whose `In` is `Streamed<Words, Words>`, as a build registers it.
    fn registered(words_wire: bool, nested: bool) -> Catalogue {
        Catalogue {
            messages: vec![MessageInfo {
                package: "protocols".into(),
                version: "0.1.0".into(),
                dir: PROTOCOLS.into(),
                name: "Words".into(),
                path: "protocols::Words".into(),
                wire: words_wire,
                encoding: Default::default(),
            }],
            exports: vec![ExportInfo {
                package: "facade".into(),
                version: "0.1.0".into(),
                dir: FACADE.into(),
                name: "Words".into(),
                path: "facade::Words".into(),
                origin: built_origin("protocols::Words"),
                wire: words_wire,
                encoding: Default::default(),
            }],
            nodes: vec![NodeInfo {
                config: Vec::new(),
                metrics: Vec::new(),
                package: "facade".into(),
                version: "0.1.0".into(),
                dir: FACADE.into(),
                name: "Stage".into(),
                path: "facade::Stage".into(),
                roles: vec![RoleInfo {
                    entries: Vec::new(),
                    role: "forward".into(),
                    ports: vec![
                        PortInfo {
                            port: "In".into(),
                            streamed: true,
                            nested,
                            head: Some(built_origin("protocols::Words")),
                            item: Some(built_origin("protocols::Words")),
                        },
                        PortInfo {
                            port: "Out".into(),
                            streamed: false,
                            nested: false,
                            head: Some(built_origin("protocols::Words")),
                            item: None,
                        },
                    ],
                }],
            }],
        }
    }

    /// The facade as the reader finds it: `Words` is a wire type.
    fn read() -> Offer {
        Offer {
            package: "facade".into(),
            version: "0.1.0".into(),
            source: format!("path+file://{FACADE}"),
            messages: Vec::<MessageEntry>::new(),
            exports: vec![ExportEntry {
                name: "Words".into(),
                path: "facade::Words".into(),
                origin: read_origin("protocols::Words"),
                wire: true,
                encoding: Default::default(),
                public: true,
                probed: None,
            }],
            nodes: vec![NodeEntry {
                entries: Vec::new(),
                clients: None,
                config: Vec::new(),
                metrics: Vec::new(),
                name: "Stage".into(),
                role: gdl_model::Role::Forward,
                ty: "facade::Stage".into(),
                ports: vec![
                    PortEntry {
                        port: "In".into(),
                        head: Some(read_origin("protocols::Words")),
                        item: Some(read_origin("protocols::Words")),
                    },
                    PortEntry {
                        port: "Out".into(),
                        head: Some(read_origin("protocols::Words")),
                        item: None,
                    },
                ],
            }],
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
        }
    }

    fn facade(dir: &str) -> bool {
        dir == FACADE
    }

    #[test]
    fn endpoint_contracts_are_compared() {
        let mut offer = read();
        let port = offer.nodes[0].ports[1].clone();
        offer.nodes[0].entries.push(gdl_extract::EndpointEntry {
            name: "chat".into(),
            path: "/chat".into(),
            enabled: "enabled_chat".into(),
            request: port.clone(),
            response: port.clone(),
        });
        assert!(!differences(&registered(true, false), &facade, &offer).is_empty());
    }

    /// An alias the reader leaves out on purpose, which the build
    /// registers, is not a difference.
    #[test]
    fn an_alias_the_reader_leaves_out_is_no_difference() {
        let mut offer = read();
        offer.exports.clear();
        offer.skipped.push(gdl_extract::Skipped {
            name: "Words".into(),
            path: Some("facade::Words".into()),
            problem: "src/lib.rs:3:10: `Arc < Words >`: the reader names a crate's own types"
                .into(),
        });
        assert_eq!(
            differences(&registered(true, false), &facade, &offer),
            Vec::<String>::new()
        );
        assert_eq!(
            left_out(&registered(true, false), &facade, &offer),
            [
                "left out by the reader on purpose, registered by the build: `Words`: src/lib.rs:3:10: `Arc < Words >`: the reader names a crate's own types"
            ]
        );
    }

    /// An alias left out at another path does not hide one of its name
    /// that the build registers and the reader missed.
    #[test]
    fn a_left_out_alias_hides_only_its_own_path() {
        let mut offer = read();
        offer.exports.clear();
        offer.skipped.push(gdl_extract::Skipped {
            name: "Words".into(),
            path: Some("facade::other::Words".into()),
            problem: "src/other.rs:3:10: `Arc < Words >`: the reader names a crate's own types"
                .into(),
        });
        assert_eq!(
            differences(&registered(true, false), &facade, &offer),
            [
                "registered by the build, not read from source: export `Words` at `facade::Words`, of `protocols::Words` (protocols 0.1.0), wire (codec msgpack)"
            ]
        );
        assert_eq!(
            left_out(&registered(true, false), &facade, &offer),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_build_that_agrees_has_no_differences() {
        assert_eq!(
            differences(&registered(true, false), &facade, &read()),
            Vec::<String>::new()
        );
    }

    /// The reader may take a message for a wire type that is not one (its
    /// serde impls borrow, say); the export's entry claims the capability,
    /// so it is compared with the one the build probes where it exports
    /// the message, though another crate defines it.
    #[test]
    fn an_exports_wire_capability_is_compared() {
        let problems = differences(&registered(false, false), &facade, &read());
        assert_eq!(
            problems,
            [
                "registered by the build, not read from source: export `Words` at `facade::Words`, of `protocols::Words` (protocols 0.1.0), local (codec msgpack)",
                "read from source, not registered by the build: export `Words` at `facade::Words`, of `protocols::Words` (protocols 0.1.0), wire (codec msgpack)",
            ]
        );
    }

    #[test]
    fn an_exports_encoding_is_compared() {
        let mut catalogue = registered(true, false);
        catalogue.messages[0].encoding = gdl_model::Encoding::Json;
        catalogue.exports[0].encoding = gdl_model::Encoding::Json;
        let problems = differences(&catalogue, &facade, &read());
        assert_eq!(problems.len(), 2);
        assert!(problems[0].contains("codec json"), "{problems:?}");
        assert!(problems[1].contains("codec msgpack"), "{problems:?}");
    }

    /// A metric the build registers that the source does not show, as
    /// a hand-written impl would, differs.
    #[test]
    fn metrics_are_compared() {
        let mut measured = registered(true, false);
        measured.nodes[0].metrics.push(gdl_model::MetricField {
            name: "passed_total".into(),
            help: "Items passed.".into(),
            kind: gdl_model::MetricKind::Counter,
            labels: Vec::new(),
            buckets: Vec::new(),
        });
        let problems = differences(&measured, &facade, &read());
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems[0].contains("passed_total"), "{problems:?}");
        assert!(differences(&registered(true, false), &facade, &read()).is_empty());
    }

    /// A port the build finds nested, where the source shows a flat
    /// stream (a `cfg` the reader cannot evaluate chose it), differs.
    #[test]
    fn a_nested_port_differs_from_a_flat_one() {
        let problems = differences(&registered(true, true), &facade, &read());
        assert_eq!(problems.len(), 2, "{problems:#?}");
        assert!(problems[0].contains(", nested"), "{}", problems[0]);
    }

    /// The program takes the workspace's patches, with each path made
    /// absolute, since the program's manifest is in another directory.
    #[test]
    fn the_program_takes_the_workspaces_patches() {
        let temp = tempfile::Builder::new()
            .prefix("gdl-patches-")
            .tempdir()
            .expect("dir");
        let root = temp.path();
        std::fs::write(
            root.join("Cargo.toml"),
            "[workspace]\n\n[patch.crates-io]\n\
             gdl-message = { path = \"vendor/gdl-message\" }\n\n\
             [patch.\"https://example.com/gdl\"]\n\
             gdl-model = { git = \"https://example.com/fork\", branch = \"main\" }\n",
        )
        .expect("manifest");
        let text = patches(root);
        let table: toml::Table = text.parse().expect("TOML");
        let patch = &table["patch"];
        assert_eq!(
            patch["crates-io"]["gdl-message"]["path"].as_str(),
            Some(&*root.join("vendor/gdl-message").to_string_lossy()),
            "{text}"
        );
        assert_eq!(
            patch["https://example.com/gdl"]["gdl-model"]["branch"].as_str(),
            Some("main"),
            "{text}"
        );
        std::fs::write(root.join("Cargo.toml"), "[workspace]\n").expect("manifest");
        assert_eq!(patches(root), "");
    }

    /// The program takes each package from the source the lock names: a
    /// Git package under its own reference, as written, so that Cargo sees
    /// one source and links one copy.
    #[test]
    fn a_dependency_keeps_its_source() {
        let at = |path: bool, source: &str| Located {
            name: "gdl-message".into(),
            version: "0.1.0".into(),
            dir: PathBuf::from("/work/gdl-message"),
            path,
            source: source.into(),
            checksum: None,
            lib: Some("gdl_message".into()),
        };
        let line = |path: bool, source: &str| {
            dependency("m", &at(path, source)).map_err(|e| e.to_string())
        };
        for (path, source, expected) in [
            (
                true,
                "path+file:///work/gdl-message",
                r#"m = { package = "gdl-message", path = "/work/gdl-message" }"#,
            ),
            (
                false,
                "registry+https://github.com/rust-lang/crates.io-index",
                r#"m = { package = "gdl-message", version = "=0.1.0" }"#,
            ),
            (
                false,
                "git+https://example.com/gdl#0123abcd",
                r#"m = { package = "gdl-message", git = "https://example.com/gdl" }"#,
            ),
            (
                false,
                "git+file:///work/repo?branch=feat/x+y%z#0123abcd",
                r#"m = { package = "gdl-message", git = "file:///work/repo", branch = "feat/x+y%z" }"#,
            ),
            (
                false,
                "git+https://example.com/gdl?tag=v0.1.0#0123abcd",
                r#"m = { package = "gdl-message", git = "https://example.com/gdl", tag = "v0.1.0" }"#,
            ),
            (
                false,
                "git+ssh://git@github.com/NVIDIA-dev/dynamo-gdl.git?rev=88ca8d3a#88ca8d3a1b2c",
                r#"m = { package = "gdl-message", git = "ssh://git@github.com/NVIDIA-dev/dynamo-gdl.git", rev = "88ca8d3a" }"#,
            ),
        ] {
            assert_eq!(line(path, source), Ok(format!("{expected}\n")), "{source}");
        }
        let error = line(false, "git+https://example.com/gdl?head=main#0123abcd")
            .expect_err("an unknown reference");
        assert!(error.contains("?head=main"), "{error}");
    }
}
