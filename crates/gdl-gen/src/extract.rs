//! `cargo gdl extract`: what a crate offers to graphs, read from its source
//! into a working-catalogue entry, and `--verify`, which compares that with
//! what a build of the crate registers.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use gdl_extract::{Located, Offer, Workspace};

use crate::Error;

/// A crate to extract: a directory, or a version on its registry.
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl Crate {
    /// `name@version`, or a directory.
    pub fn parse(text: &str) -> Self {
        match text.split_once('@') {
            Some((name, version)) if !Path::new(text).exists() => Self::Registry {
                name: name.to_string(),
                version: version.to_string(),
            },
            _ => Self::Path(PathBuf::from(text)),
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
pub fn extract(krate: &Crate, target: &Path) -> Result<Extracted, Error> {
    let (workspace, spec) = match krate {
        Crate::Path(dir) => {
            let workspace = Workspace::load(&dir.join("Cargo.toml"))
                .map_err(|e| Error::new(format!("{}: {e}", dir.display())))?;
            let name = workspace.package_in(dir).ok_or_else(|| {
                Error::new(format!(
                    "{}: cargo metadata resolves no package there",
                    dir.display()
                ))
            })?;
            (workspace, name)
        }
        Crate::Registry { name, version } => {
            let scratch = target
                .join("gdl-gen")
                .join(format!("fetch-{name}-{version}"));
            let workspace = Workspace::fetch(name, version, &scratch)
                .map_err(|e| Error::new(format!("{name}@{version}: {e}")))?;
            (workspace, format!("{name}@{version}"))
        }
    };
    let located = workspace
        .locate(&spec)
        .map_err(|e| Error::new(e.to_string()))?;
    let extraction = workspace
        .extract(&format!("{}@{}", located.name, located.version))
        .map_err(|e| Error::new(e.to_string()))?;
    if !extraction.problems.is_empty() {
        return Err(Error::new(extraction.problems.join("\n")));
    }
    Ok(Extracted {
        offer: extraction.offer,
        workspace,
        located,
    })
}

/// Write `offer` into the working catalogue at `catalogue`, as
/// `<name>/<version>.toml`, and return the file's path. A crate version
/// never changes, so an entry is written once; writing it again writes the
/// same text.
pub fn write_entry(catalogue: &Path, offer: &Offer) -> Result<PathBuf, Error> {
    let dir = catalogue.join(&offer.package);
    std::fs::create_dir_all(&dir).map_err(|e| Error::new(format!("{}: {e}", dir.display())))?;
    let file = dir.join(format!("{}.toml", offer.version));
    crate::dump::write(&file, &offer.to_toml())?;
    Ok(file)
}

/// Build a program that links the extracted crate, in `target`, and
/// compare what its markers register with what the reader found. Each
/// difference is an error: something the reader missed (a type a macro
/// made, code a build script wrote) or something the build does not
/// register.
pub fn verify(extracted: &Extracted, target: &Path) -> Result<(), Error> {
    let located = &extracted.located;
    let message = extracted.workspace.locate("gdl-message").map_err(|_| {
        Error::new(format!(
            "{} does not depend on gdl-message, so it registers nothing to verify",
            located.name
        ))
    })?;
    let dependency = |key: &str, at: &Located| {
        if at.path {
            format!(
                "{key} = {{ package = {}, path = {} }}\n",
                crate::dump::toml_string(&at.name),
                crate::dump::toml_string(&at.dir.to_string_lossy())
            )
        } else {
            format!(
                "{key} = {{ package = {}, version = {} }}\n",
                crate::dump::toml_string(&at.name),
                crate::dump::toml_string(&format!("={}", at.version))
            )
        }
    };
    // The crate under an alias of the program's own: its library's name
    // may be a keyword, or `gdl_message`.
    let dependencies = format!(
        "{}{}",
        dependency("gdl-message", &message),
        dependency("gdl_verified", located)
    );
    let main = "use gdl_verified as _;\n\n\
                fn main() {\n    print!(\"{}\", gdl_message::Catalogue::linked().to_toml());\n}\n";
    let name = format!(
        "gdl-verify-{:016x}",
        crate::dump::hash(&(&dependencies, main))
    );
    let text = crate::dump::run(
        target,
        &name,
        "the crate's registrations",
        &dependencies,
        main,
    )?;
    let registered = gdl_message::Catalogue::from_toml(&text)
        .map_err(|e| Error::new(format!("reading the crate's registrations: {e}")))?;

    // Only this crate's registrations: a dependency registers its own.
    let dir = std::fs::canonicalize(&located.dir).unwrap_or_else(|_| located.dir.clone());
    let mine = |d: &str| std::fs::canonicalize(d).is_ok_and(|d| d == dir);
    let built: BTreeSet<String> = registered
        .messages
        .iter()
        .filter(|m| mine(&m.dir))
        .map(|m| format!("message `{}` at `{}`, {}", m.name, m.path, wire(m.wire)))
        .chain(registered.exports.iter().filter(|e| mine(&e.dir)).map(|e| {
            format!(
                "export `{}` at `{}`, of `{}` ({} {})",
                e.name, e.path, e.origin.path, e.origin.package, e.origin.version
            )
        }))
        .chain(registered.nodes.iter().filter(|n| mine(&n.dir)).map(|n| {
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
                                p.item.as_ref().map(|o| (&o.package, &o.version, &o.path)),
                            )
                        })
                        .collect();
                    format!("{}({})", r.role, ports.join(", "))
                })
                .collect();
            format!("node `{}` at `{}`: {}", n.name, n.path, roles.join(" "))
        }))
        .collect();
    let offer = &extracted.offer;
    let read: BTreeSet<String> = offer
        .messages
        .iter()
        .map(|m| format!("message `{}` at `{}`, {}", m.name, m.path, wire(m.wire)))
        .chain(offer.exports.iter().map(|e| {
            format!(
                "export `{}` at `{}`, of `{}` ({} {})",
                e.name, e.path, e.origin.path, e.origin.package, e.origin.version
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
                        p.item.as_ref().map(|o| (&o.package, &o.version, &o.path)),
                    )
                })
                .collect();
            format!(
                "node `{}` at `{}`: {}({})",
                n.name,
                n.ty,
                n.role,
                ports.join(", ")
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
    if problems.is_empty() {
        Ok(())
    } else {
        Err(Error::new(problems.join("\n")))
    }
}

fn wire(wire: bool) -> &'static str {
    if wire { "wire" } else { "local" }
}

/// A port, with each message by package, version, and path.
fn port(
    name: &str,
    head: Option<(&String, &String, &String)>,
    item: Option<(&String, &String, &String)>,
) -> String {
    let one = |m: Option<(&String, &String, &String)>| {
        m.map_or("()".to_string(), |(package, version, path)| {
            format!("{path} ({package} {version})")
        })
    };
    match item {
        None => format!("{name}={}", one(head)),
        Some(_) => format!("{name}={} + stream {}", one(head), one(item)),
    }
}
