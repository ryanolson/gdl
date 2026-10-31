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
                spec::CrateSource::Git { url, rev } => git_source_is(&o.source, url, rev),
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
        .chain(offer.policies.iter().map(|p| p.ty.as_str()))
        .chain(offer.selectors.iter().map(|s| s.ty.as_str()))
        .find_map(|path| path.split("::").next());
    if let Some(library) = library.filter(|l| *l != lib) {
        return Err(vec![match source {
            spec::CrateSource::Version(_) => format!(
                "{package}@{}: its library is `{library}`, not `{lib}`; a crate listed by version must name its library for its package",
                offer.version
            ),
            _ => format!("{package}: its library is `{library}`, not `{lib}`"),
        }]);
    }
    Ok((*offer).clone())
}

/// Whether `source`, as a lockfile names a git package, is `url` at
/// `rev`. Cargo writes `git+<url>?rev=<as bound>#<full commit>`, so a
/// revision bound short or in full names the offer's commit when it begins
/// that commit; two revisions that name different commits never match.
fn git_source_is(source: &str, url: &str, rev: &str) -> bool {
    let Some((bound, commit)) = source
        .strip_prefix("git+")
        .and_then(|s| s.strip_prefix(url))
        .and_then(|s| s.strip_prefix("?rev="))
        .and_then(|s| s.split_once('#'))
    else {
        return false;
    };
    !rev.is_empty() && commit.starts_with(rev) && commit.starts_with(bound)
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
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
            selectors: Vec::new(),
            skipped_selectors: Vec::new(),
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

    /// A crate that offers only a pool policy or only a selector is held
    /// to its library too: generated code names the router's type by it,
    /// so one bound under another library would not build.
    #[test]
    fn a_given_router_crate_names_its_library_for_its_package() {
        let entry = gdl_extract::PolicyEntry {
            name: "ByText".into(),
            ty: "router_library::ByText".into(),
            head: gdl_extract::PolicyHead::Encoded,
            needs: Vec::new(),
        };
        let empty = Offer {
            package: "selector-router".into(),
            version: "0.1.0".into(),
            source: CRATES_IO[0].into(),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes: Vec::new(),
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
            selectors: Vec::new(),
            skipped_selectors: Vec::new(),
        };
        let policy = Offer {
            policies: vec![entry.clone()],
            ..empty.clone()
        };
        let selector = Offer {
            selectors: vec![entry],
            ..empty
        };
        let version = spec::CrateSource::Version("0.1.0".into());
        for offer in [&policy, &selector] {
            let error = given_offer(&[offer], "selector-router", "selector_router", &version)
                .expect_err("another library");
            assert!(
                error[0].contains("its library is `router_library`, not `selector_router`"),
                "{error:?}"
            );
            assert!(given_offer(&[offer], "selector-router", "router_library", &version).is_ok());
        }
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
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
            selectors: Vec::new(),
            skipped_selectors: Vec::new(),
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

    /// A git package is matched by its commit, whether the spec binds the
    /// revision short or in full: cargo records the source as the revision
    /// as bound, then the full commit, and pool policies and selectors
    /// share the match.
    #[test]
    fn a_short_revision_and_its_full_form_match_one_offer() {
        const URL: &str = "https://example.com/router";
        const COMMIT: &str = "dec58f81ac7f4b7a9c0d1e2f3a4b5c6d7e8f9a0b";
        let git = |short: &str| spec::CrateSource::Git {
            url: URL.into(),
            rev: short.into(),
        };
        let entry = gdl_extract::PolicyEntry {
            name: "ByText".into(),
            ty: "router::ByText".into(),
            head: gdl_extract::PolicyHead::Encoded,
            needs: Vec::new(),
        };
        let empty = Offer {
            package: "router".into(),
            version: "0.1.0".into(),
            source: format!("git+{URL}?rev={}#{COMMIT}", &COMMIT[..12]),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes: Vec::new(),
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
            selectors: Vec::new(),
            skipped_selectors: Vec::new(),
        };
        let policy = Offer {
            policies: vec![entry.clone()],
            ..empty.clone()
        };
        let selector = Offer {
            selectors: vec![entry],
            ..empty
        };
        let full = Offer {
            source: format!("git+{URL}?rev={COMMIT}#{COMMIT}"),
            ..policy.clone()
        };
        for offer in [&policy, &selector, &full] {
            for rev in [&COMMIT[..12], &COMMIT[..8], COMMIT] {
                let found = given_offer(&[offer], "router", "router", &git(rev));
                assert_eq!(
                    found.as_ref().map(|o| &o.source),
                    Ok(&offer.source),
                    "{rev}"
                );
            }
            // Another commit, short or full, is another offer.
            for rev in [
                "0123456789ab",
                "dec58f81ac7e",
                "dec58f81ac7f4b7a9c0d1e2f3a4b5c6d7e8f9a0c",
                "",
            ] {
                assert!(
                    given_offer(&[offer], "router", "router", &git(rev)).is_err(),
                    "{rev}"
                );
            }
            // Another repository at the same commit is another offer.
            let elsewhere = spec::CrateSource::Git {
                url: "https://example.com/other".into(),
                rev: COMMIT.into(),
            };
            assert!(given_offer(&[offer], "router", "router", &elsewhere).is_err());
        }
    }

    #[test]
    #[ignore = "F1: the catalogue accepts unresolved and ambiguous commit prefixes"]
    fn a_given_git_revision_must_be_resolvable_and_unambiguous() {
        let repo = GitFixture::new();
        let first = repo.offer(&repo.first);
        let second = repo.offer(&repo.second);
        let mut catalog =
            gdl_extract::Catalog::load(&repo.dir.path().join("catalog")).expect("catalogue");
        for offer in [first.clone(), second.clone()] {
            catalog
                .add(gdl_extract::Entry {
                    added: "2026-10-30T00:00:00Z".into(),
                    checksum: None,
                    offer,
                })
                .expect("different versions can both be in the catalogue");
        }
        let mut given: Vec<_> = catalog.entries().map(|entry| &entry.offer).collect();
        for offer in [&first, &second] {
            let rev = offer.source.rsplit_once('#').expect("commit").1;
            assert_eq!(
                offers(&[repo.listed(rev)], repo.dir.path(), Offers::Given(&given))
                    .expect("a full commit selects its offer")[0]
                    .source,
                offer.source
            );
        }
        let mut accepted = Vec::new();
        for len in [1, 2, 3, 4] {
            let rev = &repo.first[..len];
            let listed = [repo.listed(rev)];
            let error = offers(&listed, repo.dir.path(), Offers::Read)
                .expect_err("Cargo must reject an unresolved or ambiguous abbreviation");
            assert!(error.to_string().contains("code=Ambiguous (-5)"), "{error}");
            for _ in 0..2 {
                if let Ok(found) = offers(&listed, repo.dir.path(), Offers::Given(&given)) {
                    accepted.push(format!("{rev}: {}", found[0].source));
                }
                given.reverse();
            }
        }
        assert!(
            accepted.is_empty(),
            "the catalogue accepts revisions that Cargo rejects, and order chooses the commit:\n{}",
            accepted.join("\n")
        );
    }

    struct GitFixture {
        dir: tempfile::TempDir,
        url: String,
        first: String,
        second: String,
    }

    impl GitFixture {
        fn new() -> Self {
            let dir = tempfile::Builder::new()
                .prefix("gdl-revision-")
                .tempdir()
                .expect("fixture directory");
            let path = dir.path().join("repo");
            std::fs::create_dir_all(path.join("src")).expect("source directory");
            let manifest = "[package]\nname = \"review-router\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n";
            std::fs::write(path.join("Cargo.toml"), manifest).expect("manifest");
            std::fs::write(path.join("src/lib.rs"), "pub const VALUE: u32 = 1;\n").expect("source");
            let mut repo = Self {
                url: format!(
                    "file://{}",
                    path.canonicalize().expect("repository").display()
                ),
                dir,
                first: String::new(),
                second: String::new(),
            };
            repo.git(&["init", "--quiet", "--object-format=sha1"]);
            repo.git(&["add", "."]);
            repo.git(&["commit", "--quiet", "-m", "first"]);
            repo.first = repo.git(&["rev-parse", "HEAD"]);
            std::fs::write(path.join("Cargo.toml"), manifest.replace("0.1.0", "0.2.0"))
                .expect("second manifest");
            std::fs::write(path.join("src/lib.rs"), "pub const VALUE: u32 = 2;\n")
                .expect("second source");
            repo.git(&["add", "."]);
            // A fixed collision avoids mining commit hashes during the test.
            repo.git(&["commit", "--quiet", "-m", "collision-68092"]);
            repo.second = repo.git(&["rev-parse", "HEAD"]);
            assert_ne!(repo.first, repo.second);
            assert_eq!(&repo.first[..4], &repo.second[..4]);
            repo
        }

        fn git(&self, args: &[&str]) -> String {
            let output = std::process::Command::new("git")
                .current_dir(self.dir.path().join("repo"))
                .args(["-c", "commit.gpgsign=false", "-c", "core.autocrlf=false"])
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_SYSTEM", "/dev/null")
                .env("GIT_AUTHOR_NAME", "gdl-test")
                .env("GIT_AUTHOR_EMAIL", "gdl@example.invalid")
                .env("GIT_COMMITTER_NAME", "gdl-test")
                .env("GIT_COMMITTER_EMAIL", "gdl@example.invalid")
                .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
                .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
                .args(args)
                .output()
                .expect("git");
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout)
                .expect("git output")
                .trim()
                .to_string()
        }

        fn listed(&self, rev: &str) -> spec::ListedCrate {
            spec::ListedCrate {
                package: "review-router".into(),
                lib: "review_router".into(),
                source: spec::CrateSource::Git {
                    url: self.url.clone(),
                    rev: rev.into(),
                },
            }
        }

        fn offer(&self, rev: &str) -> Offer {
            offers(&[self.listed(rev)], self.dir.path(), Offers::Read)
                .expect("read the Git fixture")
                .remove(0)
        }
    }
}
