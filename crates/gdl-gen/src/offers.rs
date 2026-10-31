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
/// `rev`. Cargo writes `git+<url>?rev=<as bound>#<resolved commit>` and
/// identifies a git source by the revision as written, not by the commit
/// it names: a short revision and the full one for the same commit are two
/// sources, which the generated manifests must not mix. So the revision
/// must equal the offer's text for text. The last `#` ends it, since the
/// revision may be a tag that holds one.
fn git_source_is(source: &str, url: &str, rev: &str) -> bool {
    source
        .strip_prefix("git+")
        .and_then(|s| s.strip_prefix(url))
        .and_then(|s| s.strip_prefix("?rev="))
        .and_then(|s| s.rsplit_once('#'))
        .is_some_and(|(bound, _)| bound == rev)
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

    /// Cargo identifies a git source by the revision as written: a spec
    /// matches the offer catalogued at that same text, short or full, and
    /// a short revision and the full one for the same commit are two
    /// bindings, each needing its own offer. Pool policies and selectors
    /// share the match.
    #[test]
    fn a_git_revision_matches_the_offer_catalogued_at_that_text() {
        const URL: &str = "https://example.com/router";
        const COMMIT: &str = "dec58f81ac7f4b7a9c0d1e2f3a4b5c6d7e8f9a0b";
        let short = &COMMIT[..12];
        let git = |rev: &str| spec::CrateSource::Git {
            url: URL.into(),
            rev: rev.into(),
        };
        let entry = gdl_extract::PolicyEntry {
            name: "ByText".into(),
            ty: "router::ByText".into(),
            head: gdl_extract::PolicyHead::Encoded,
            needs: Vec::new(),
        };
        let empty = |bound: &str| Offer {
            package: "router".into(),
            version: "0.1.0".into(),
            source: format!("git+{URL}?rev={bound}#{COMMIT}"),
            messages: Vec::new(),
            exports: Vec::new(),
            nodes: Vec::new(),
            policies: Vec::new(),
            skipped: Vec::new(),
            skipped_policies: Vec::new(),
            selectors: Vec::new(),
            skipped_selectors: Vec::new(),
        };
        for (what, offer) in [
            (
                "policy",
                Offer {
                    policies: vec![entry.clone()],
                    ..empty(short)
                },
            ),
            (
                "selector",
                Offer {
                    selectors: vec![entry.clone()],
                    ..empty(short)
                },
            ),
        ] {
            let found = given_offer(&[&offer], "router", "router", &git(short));
            assert_eq!(
                found.as_ref().map(|o| &o.source),
                Ok(&offer.source),
                "{what}"
            );
            // The same commit, bound in full, is another source.
            assert!(
                given_offer(&[&offer], "router", "router", &git(COMMIT)).is_err(),
                "{what}"
            );
        }
        let full = empty(COMMIT);
        assert!(given_offer(&[&full], "router", "router", &git(COMMIT)).is_ok());
        assert!(given_offer(&[&full], "router", "router", &git(short)).is_err());
        // Another revision, a prefix of the bound one, or none, is another offer.
        let offer = empty(short);
        for rev in ["0123456789ab", "dec58f81ac7e", &short[..8], ""] {
            assert!(
                given_offer(&[&offer], "router", "router", &git(rev)).is_err(),
                "{rev}"
            );
        }
        // Another repository at the same revision is another offer.
        let elsewhere = spec::CrateSource::Git {
            url: "https://example.com/other".into(),
            rev: short.into(),
        };
        assert!(given_offer(&[&offer], "router", "router", &elsewhere).is_err());
    }

    #[test]
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

    #[test]
    fn a_hex_named_ref_must_not_select_a_different_commit() {
        let repo = GitFixture::new();
        let rev = &repo.first[..12];
        repo.git(&["tag", rev, &repo.second]);
        let by_ref = repo.offer(rev);
        assert_eq!(by_ref.version, "0.2.0");
        assert_eq!(
            by_ref.source,
            format!("git+{}?rev={rev}#{}", repo.url, repo.second)
        );
        let other = repo.offer(&repo.first);
        assert_eq!(other.version, "0.1.0");
        let found = offers(
            &[repo.listed(rev)],
            repo.dir.path(),
            Offers::Given(&[&other]),
        );
        assert!(
            found.is_err(),
            "rev {rev} resolves to {}, but the catalogue substitutes {:?}",
            by_ref.source,
            found.as_ref().map(|offers| &offers[0].source)
        );
    }

    #[test]
    fn a_hash_inside_a_git_ref_must_not_replace_the_resolved_commit() {
        let repo = GitFixture::new();
        let rev = format!("{}#{}", &repo.first[..12], repo.first);
        repo.git(&["tag", &rev, &repo.second]);
        let tagged = repo.offer(&rev);
        assert_eq!(tagged.version, "0.2.0");
        assert_eq!(
            tagged.source,
            format!("git+{}?rev={rev}#{}", repo.url, repo.second)
        );
        let expected = repo.offer(&repo.first);
        assert_eq!(expected.version, "0.1.0");
        let found = offers(
            &[repo.listed(&repo.first)],
            repo.dir.path(),
            Offers::Given(&[&tagged]),
        );
        assert!(
            found.is_err(),
            "revision {} resolves to {}, but the catalogue substitutes {:?}",
            repo.first,
            expected.source,
            found.as_ref().map(|offers| &offers[0].source)
        );
    }

    #[test]
    fn a_rebound_git_offer_must_preserve_message_origin_checks() {
        // Comparing both providers requires repeated Git imports and source reads.
        let repo = GitFixture::new();
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root");
        let path = |path: std::path::PathBuf| toml::Value::String(path.display().to_string());
        std::fs::write(
            repo.dir.path().join("repo/Cargo.toml"),
            format!(
                "[package]\nname = \"review-router\"\nversion = \"0.2.0\"\nedition = \"2024\"\n\n[workspace]\n[dependencies]\ngdl = {{ path = {} }}\nfutures = \"0.3\"\n",
                path(root.join("crates/gdl"))
            ),
        )
        .expect("manifest");
        std::fs::write(
            repo.dir.path().join("repo/src/lib.rs"),
            r#"
#[derive(Debug, gdl::Message)]
pub struct Prompt;
#[derive(Debug, gdl::Message)]
pub struct Answer;
#[derive(gdl::Node)]
pub struct Model;
impl gdl::Sink for Model {
    type Req = Prompt;
    type Item = Answer;
    async fn generate(&self, _: gdl::Cx, _: Prompt) -> gdl::Result<gdl::Items<Answer>> {
        Ok(gdl::items(futures::stream::empty()))
    }
}
impl gdl::Build for Model {
    type Config = ();
    type Metrics = ();
    async fn build(_: (), _: ()) -> gdl::Result<Self> { Ok(Self) }
}
"#,
        )
        .expect("source");
        repo.git(&["add", "."]);
        repo.git(&["commit", "--quiet", "-m", "messages and node"]);
        let full = repo.git(&["rev-parse", "HEAD"]);
        let short = &full[..12];
        let offer = repo.offer(short);
        assert_eq!(offer.messages.len(), 2);
        assert_eq!(offer.nodes.len(), 1);
        let text = format!(
            "[graph]\npath = \"identity.gdl\"\n[messages]\ncrates = [{{ package = \"review-router\", git = {url:?}, rev = {full:?} }}]\n[nodes]\ncrates = [{{ package = \"review-router\", git = {url:?}, rev = {short:?} }}]\n[gdl]\npath = {runtime}\n[segment.main]\nnodes = [\"Front\", \"Model\"]\n",
            url = repo.url,
            runtime = path(root.join("crates/gdl-runtime")),
        );
        let graph = "name = \"identity\", schema = 1;\nsource Front: Prompt -> stream Answer;\nsink Model: Prompt -> stream Answer;\ntopology: Front => Model;\n";
        let control = crate::Spec::from_text(&text.replace(&full, short), graph, repo.dir.path())
            .expect("one Git reference");
        crate::check(&control, repo.dir.path(), Offers::Read)
            .expect("a graph whose message and node use one Git source");
        let spec = crate::Spec::from_text(&text, graph, repo.dir.path()).expect("spec");
        let error = crate::check(&spec, repo.dir.path(), Offers::Read)
            .expect_err("different Cargo references give different message origins");
        assert!(error.to_string().contains("port `Req`"), "{error}");
        assert!(
            crate::check(&spec, repo.dir.path(), Offers::Given(&[&offer])).is_err(),
            "Given accepted a graph that Read rejects:\n{error}"
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
