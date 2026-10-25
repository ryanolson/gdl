//! Where the offers of a spec's listed crates come from: read from each
//! crate's source, or given by a loaded catalogue and held to the rules a
//! read is held to.

use std::path::Path;

use gdl_extract::Offer;

use crate::{Error, spec};

/// Where the offers of a spec's listed crates come from.
#[derive(Debug, Clone, Copy)]
pub enum Offers<'a> {
    /// Read from each crate's source, a crate listed by version fetched
    /// into the target directory first.
    Read,
    /// Given: the entries of a loaded catalogue, read when each was added.
    /// A listed crate the catalogue does not hold is an error.
    Given(&'a [&'a Offer]),
}

/// What the listed crates offer, read from their source: a directory as
/// it is, and a version fetched from crates.io into `target`, which builds
/// nothing. Crates of one workspace share one `cargo metadata`. Every
/// problem the reader meets is reported.
pub(crate) fn offers(
    listed: &[spec::ListedCrate],
    target: &Path,
    from: Offers<'_>,
) -> Result<Vec<gdl_extract::Offer>, Error> {
    if let Offers::Given(given) = from {
        return listed
            .iter()
            .map(|c| given_offer(given, &c.package, &c.lib, &c.source))
            .collect::<Result<Vec<_>, Vec<String>>>()
            .map_err(Error::all);
    }
    let mut workspaces: Vec<gdl_extract::Workspace> = Vec::new();
    let mut offers = Vec::with_capacity(listed.len());
    let mut problems = Vec::new();
    for listed in listed {
        let (package, lib, source) = (listed.package.as_str(), listed.lib.as_str(), &listed.source);
        let extraction = match source {
            spec::CrateSource::Git { url, rev } => {
                let workspace = gdl_extract::Workspace::fetch_git(
                    package,
                    url,
                    rev,
                    target,
                    &gdl_extract::Cargo::trusted(),
                )
                .map_err(|e| Error::new(e.to_string()))?;
                let located = workspace
                    .locate(package)
                    .map_err(|e| Error::new(e.to_string()))?;
                if located.lib.as_deref().is_some_and(|name| name != lib) {
                    return Err(Error::new(format!(
                        "{package}: its library does not match `{lib}`"
                    )));
                }
                workspace
                    .extract(package)
                    .map_err(|e| Error::new(e.to_string()))?
            }
            spec::CrateSource::Path(dir) => {
                // Crates of one workspace share its `cargo metadata`.
                match workspaces.iter().find_map(|w| w.extract_in(dir)) {
                    Some(extraction) => extraction,
                    None => {
                        let workspace = gdl_extract::Workspace::load(
                            &dir.join("Cargo.toml"),
                            &gdl_extract::Cargo::trusted(),
                        )
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
                let workspace = gdl_extract::Workspace::fetch(
                    package,
                    version,
                    &scratch,
                    &gdl_extract::Cargo::trusted(),
                )
                .map_err(fail)?;
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
        Err(Error::all(problems))
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
pub(crate) fn given_offer(
    given: &[&Offer],
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
                spec::CrateSource::Git { url, rev } => {
                    o.source == format!("git+{url}?rev={rev}#{rev}")
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
    Ok((*offer).clone())
}

#[cfg(test)]
mod tests {
    use super::*;

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
                encoding: Default::default(),
                public: true,
            }],
            exports: Vec::new(),
            nodes: Vec::new(),
            skipped: Vec::new(),
        };
        let version = spec::CrateSource::Version("0.1.0".into());
        let error = given_offer(&[&offer], "proto", "proto", &version).expect_err("renamed");
        assert_eq!(
            error,
            [
                "proto@0.1.0: its library is `renamed`, not `proto`; a crate listed by version must name its library for its package"
            ]
        );
        let mut named = offer.clone();
        named.messages[0].path = "proto::Words".into();
        assert!(given_offer(&[&named], "proto", "proto", &version).is_ok());
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
            skipped: Vec::new(),
        };
        let git = offer("git+https://example.com/proto#abc");
        let registry = offer("registry+https://github.com/rust-lang/crates.io-index");
        let version = spec::CrateSource::Version("0.1.0".into());
        assert!(given_offer(&[&git], "proto", "proto", &version).is_err());
        // Another registry's package of that name and version is another
        // crate: the generated manifests name crates.io.
        let elsewhere = offer("sparse+https://registry.example.com/index/");
        assert!(given_offer(&[&elsewhere], "proto", "proto", &version).is_err());
        let sparse = offer("sparse+https://index.crates.io/");
        assert!(given_offer(&[&sparse], "proto", "proto", &version).is_ok());
        for given in [[&git, &registry], [&registry, &git]] {
            let found = given_offer(&given, "proto", "proto", &version).expect("the registry's");
            assert_eq!(found.source, registry.source);
        }
    }
}
