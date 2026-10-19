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
    let problems = differences(&registered, &mine, &extracted.offer);
    if problems.is_empty() {
        Ok(())
    } else {
        Err(Error::new(problems.join("\n")))
    }
}

/// Each difference between what a build registered for the crate whose
/// registrations `mine` picks, and what the reader found in it, `offer`.
/// Entries are compared as lines that hold everything an entry claims: a
/// message's wire capability, an export's origin and that message's wire
/// capability, and each port's messages and shape.
fn differences(
    registered: &gdl_message::Catalogue,
    mine: &dyn Fn(&str) -> bool,
    offer: &Offer,
) -> Vec<String> {
    // An export's capability is its message's, registered by the crate
    // that defines it, which the program links too.
    let capability = |origin: &gdl_message::Origin| {
        registered
            .messages
            .iter()
            .find(|m| m.dir == origin.dir && m.path == origin.path)
            .map_or("unregistered", |m| wire(m.wire))
    };
    let built: BTreeSet<String> = registered
        .messages
        .iter()
        .filter(|m| mine(&m.dir))
        .map(|m| format!("message `{}` at `{}`, {}", m.name, m.path, wire(m.wire)))
        .chain(registered.exports.iter().filter(|e| mine(&e.dir)).map(|e| {
            format!(
                "export `{}` at `{}`, of `{}` ({} {}), {}",
                e.name,
                e.path,
                e.origin.path,
                e.origin.package,
                e.origin.version,
                capability(&e.origin)
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
                                p.streamed.then(|| {
                                    p.item.as_ref().map(|o| (&o.package, &o.version, &o.path))
                                }),
                                p.nested,
                            )
                        })
                        .collect();
                    format!("{}({})", r.role, ports.join(", "))
                })
                .collect();
            format!("node `{}` at `{}`: {}", n.name, n.path, roles.join(" "))
        }))
        .collect();
    let read: BTreeSet<String> = offer
        .messages
        .iter()
        .map(|m| format!("message `{}` at `{}`, {}", m.name, m.path, wire(m.wire)))
        .chain(offer.exports.iter().map(|e| {
            format!(
                "export `{}` at `{}`, of `{}` ({} {}), {}",
                e.name,
                e.path,
                e.origin.path,
                e.origin.package,
                e.origin.version,
                wire(e.wire)
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
    problems
}

fn wire(wire: bool) -> &'static str {
    if wire { "wire" } else { "local" }
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
            }],
            exports: vec![ExportInfo {
                package: "facade".into(),
                version: "0.1.0".into(),
                dir: FACADE.into(),
                name: "Words".into(),
                path: "facade::Words".into(),
                origin: built_origin("protocols::Words"),
            }],
            nodes: vec![NodeInfo {
                package: "facade".into(),
                version: "0.1.0".into(),
                dir: FACADE.into(),
                name: "Stage".into(),
                path: "facade::Stage".into(),
                roles: vec![RoleInfo {
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
            }],
            nodes: vec![NodeEntry {
                name: "Stage".into(),
                role: "forward".into(),
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
        }
    }

    fn facade(dir: &str) -> bool {
        dir == FACADE
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
    /// so it is compared with the build's, though another crate defines
    /// the message.
    #[test]
    fn an_exports_wire_capability_is_compared() {
        let problems = differences(&registered(false, false), &facade, &read());
        assert_eq!(
            problems,
            [
                "registered by the build, not read from source: export `Words` at `facade::Words`, of `protocols::Words` (protocols 0.1.0), local",
                "read from source, not registered by the build: export `Words` at `facade::Words`, of `protocols::Words` (protocols 0.1.0), wire",
            ]
        );
    }

    /// A port the build finds nested, where the source shows a flat
    /// stream (a `cfg` the reader cannot evaluate chose it), differs.
    #[test]
    fn a_nested_port_differs_from_a_flat_one() {
        let problems = differences(&registered(true, true), &facade, &read());
        assert_eq!(problems.len(), 2, "{problems:#?}");
        assert!(problems[0].contains(", nested"), "{}", problems[0]);
    }
}
