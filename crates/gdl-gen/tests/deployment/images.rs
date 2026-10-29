//! A fleet's images: one per registry, platform, and repository, each
//! tagged with a digest of what it is built from, built and pushed through
//! a fake Docker that stands in for each registry. The same tags are what
//! `k8s` names without Docker, and the digests a run records are what it
//! names with them.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::k8s::{container, deployment, fleet, fleet_text, objects, spec};
use super::{Mirror, SPLIT, WHOLE, nest, placed};

/// What the fake Docker was asked to do.
#[derive(Debug, Clone, PartialEq)]
enum Call {
    /// Whether the registry holds `reference`.
    Probe(String),
    /// A build of `reference` for `platform`, pushed or loaded.
    Build {
        reference: String,
        platform: String,
        push: bool,
    },
}

/// The files of a build's context, by their path in it.
type Context = BTreeMap<PathBuf, String>;

/// A Docker whose registries are a map from reference to digest: a push
/// puts the image there, and a probe reads it.
#[derive(Default)]
struct Fake {
    registry: BTreeMap<String, String>,
    calls: Vec<Call>,
    /// Each build's context, by its reference.
    contexts: BTreeMap<String, Context>,
    builds: u32,
}

impl Fake {
    fn builds(&self) -> Vec<(&str, &str, bool)> {
        self.calls
            .iter()
            .filter_map(|call| match call {
                Call::Build {
                    reference,
                    platform,
                    push,
                } => Some((reference.as_str(), platform.as_str(), *push)),
                Call::Probe(_) => None,
            })
            .collect()
    }

    fn probes(&self) -> usize {
        self.calls
            .iter()
            .filter(|call| matches!(call, Call::Probe(_)))
            .count()
    }
}

impl gdl_gen::Docker for Fake {
    fn digest(&mut self, reference: &str) -> Result<Option<String>, gdl_gen::Error> {
        self.calls.push(Call::Probe(reference.to_string()));
        Ok(self.registry.get(reference).cloned())
    }

    fn build(
        &mut self,
        context: &Path,
        platform: &str,
        reference: &str,
        push: bool,
    ) -> Result<String, gdl_gen::Error> {
        self.calls.push(Call::Build {
            reference: reference.to_string(),
            platform: platform.to_string(),
            push,
        });
        self.contexts
            .insert(reference.to_string(), read_dir(context));
        self.builds += 1;
        let digest = format!("sha256:{:064x}", self.builds);
        if push {
            self.registry.insert(reference.to_string(), digest.clone());
        }
        Ok(digest)
    }
}

/// Every file under `dir`, by its path from it.
fn read_dir(dir: &Path) -> Context {
    let mut files = Context::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("a context") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let text = std::fs::read_to_string(&path).expect("a text file");
                files.insert(path.strip_prefix(dir).expect("inside").to_path_buf(), text);
            }
        }
    }
    files
}

fn run(
    mirror: &Mirror,
    spec: &gdl_gen::Spec,
    fleet: &gdl_model::Fleet,
    push: bool,
    docker: &mut Fake,
) -> Vec<gdl_gen::Built> {
    gdl_gen::images(spec, fleet, &mirror.target(), push, docker).expect("images")
}

/// A reference's repository and tag: `<registry>/<repository>:<tag>`.
fn split(reference: &str) -> (&str, &str, &str) {
    let (rest, tag) = reference.rsplit_once(':').expect("a tag");
    let (registry, repository) = rest.rsplit_once('/').expect("a repository");
    (registry, repository, tag)
}

/// The fleet of the k8s tests with `euw1`'s registry and platform given.
fn with_euw1(registry: &str, platform: &str) -> gdl_model::Fleet {
    let text = fleet_text()
        .replace(
            "registry = \"registry.example/gdl-eu\"",
            &format!("registry = \"{registry}\""),
        )
        .replace(
            "platform = \"linux/arm64\"",
            &format!("platform = \"{platform}\""),
        );
    fleet(&text)
}

/// Each cluster gets the hub and each segment its pools run, built for its
/// platform and pushed to its registry, each tagged with a sha256 of what
/// it is built from. Nothing is in a registry yet, so each is probed, then
/// built and pushed.
#[test]
fn each_cluster_gets_its_images_for_its_platform() {
    let mirror = Mirror::new();
    let mut docker = Fake::default();
    let built = run(
        &mirror,
        &spec(&mirror),
        &fleet(&fleet_text()),
        true,
        &mut docker,
    );
    let mut seen: Vec<(&str, &str, &str)> = docker
        .builds()
        .iter()
        .map(|(reference, platform, push)| {
            assert!(push, "{reference}");
            let (registry, repository, tag) = split(reference);
            assert_eq!(tag.len(), 64, "{reference}");
            assert!(tag.chars().all(|c| c.is_ascii_hexdigit()), "{reference}");
            (registry, repository, *platform)
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        [
            ("registry.example/gdl", "api-engine", "linux/amd64"),
            ("registry.example/gdl", "api-front", "linux/amd64"),
            ("registry.example/gdl", "api-hub", "linux/amd64"),
            ("registry.example/gdl-eu", "api-engine", "linux/arm64"),
            ("registry.example/gdl-eu", "api-front", "linux/arm64"),
            ("registry.example/gdl-eu", "api-hub", "linux/arm64"),
        ]
    );
    assert_eq!(docker.probes(), 6, "each image is probed first");
    // Each image's digest is recorded, by its reference.
    assert_eq!(built.len(), 6);
    for image in &built {
        assert_eq!(
            image.outcome,
            gdl_gen::Outcome::Pushed,
            "{}",
            image.reference
        );
        assert_eq!(docker.registry[&image.reference], image.digest);
    }
}

/// Two clusters that share a registry and a platform share each image:
/// it is built and pushed once.
#[test]
fn clusters_that_share_a_registry_and_platform_build_once() {
    let mirror = Mirror::new();
    let mut docker = Fake::default();
    let fleet = with_euw1("registry.example/gdl", "linux/amd64");
    run(&mirror, &spec(&mirror), &fleet, true, &mut docker);
    let repositories: Vec<&str> = docker
        .builds()
        .iter()
        .map(|(reference, _, _)| split(reference).1)
        .collect();
    assert_eq!(repositories.len(), 3, "{repositories:?}");
    let unique: BTreeSet<&str> = repositories.iter().copied().collect();
    assert_eq!(unique.len(), 3, "{repositories:?}");
}

/// One registry that two platforms pull from holds each image twice,
/// under two tags: the platform is part of what the tag digests.
#[test]
fn one_registry_for_two_platforms_holds_two_tags() {
    let mirror = Mirror::new();
    let mut docker = Fake::default();
    let fleet = with_euw1("registry.example/gdl", "linux/arm64");
    run(&mirror, &spec(&mirror), &fleet, true, &mut docker);
    let builds = docker.builds();
    assert_eq!(builds.len(), 6, "{builds:?}");
    let mut tags: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (reference, _, _) in &builds {
        let (_, repository, tag) = split(reference);
        tags.entry(repository).or_default().insert(tag);
    }
    for (repository, tags) in &tags {
        assert_eq!(tags.len(), 2, "{repository}: {tags:?}");
    }
}

/// A second run with nothing changed only asks each registry: every image
/// is there under its tag, so nothing is built or pushed, and each digest
/// is the one the first run recorded.
#[test]
fn a_second_run_with_no_change_pushes_nothing() {
    let mirror = Mirror::new();
    let spec = spec(&mirror);
    let fleet = fleet(&fleet_text());
    let mut docker = Fake::default();
    let first = run(&mirror, &spec, &fleet, true, &mut docker);
    docker.calls.clear();
    let second = run(&mirror, &spec, &fleet, true, &mut docker);
    assert!(docker.builds().is_empty(), "{:?}", docker.calls);
    assert_eq!(docker.probes(), 6);
    let digests = |built: &[gdl_gen::Built]| -> BTreeMap<String, String> {
        built
            .iter()
            .map(|image| (image.reference.clone(), image.digest.clone()))
            .collect()
    };
    assert_eq!(digests(&first), digests(&second));
    for image in &second {
        assert_eq!(
            image.outcome,
            gdl_gen::Outcome::Present,
            "{}",
            image.reference
        );
    }
}

/// Without `--push`, no registry is asked: each image is built and loaded
/// into the local Docker.
#[test]
fn without_push_each_image_is_built_and_loaded() {
    let mirror = Mirror::new();
    let mut docker = Fake::default();
    let built = run(
        &mirror,
        &spec(&mirror),
        &fleet(&fleet_text()),
        false,
        &mut docker,
    );
    assert_eq!(docker.probes(), 0);
    assert_eq!(docker.builds().len(), 6);
    assert!(docker.builds().iter().all(|(_, _, push)| !push));
    assert!(docker.registry.is_empty());
    for image in &built {
        assert_eq!(
            image.outcome,
            gdl_gen::Outcome::Loaded,
            "{}",
            image.reference
        );
    }
}

/// An image holds only its binary: no settings, which each pod reads from
/// its pool's ConfigMap. Its build context is exactly what the tag
/// digests: a segment's holds the contract and its own crate, built with
/// mimalloc, and a hub's only its Dockerfile. The binary is built on the
/// build host's platform for the image's, so no emulated compiler runs.
#[test]
fn an_image_holds_its_binary_and_no_settings() {
    let mirror = Mirror::new();
    let mut docker = Fake::default();
    run(
        &mirror,
        &spec(&mirror),
        &fleet(&fleet_text()),
        true,
        &mut docker,
    );
    for (reference, context) in &docker.contexts {
        let (_, repository, _) = split(reference);
        let dockerfile = &context[Path::new("Dockerfile")];
        assert!(
            dockerfile.contains("FROM --platform=$BUILDPLATFORM "),
            "{reference}:\n{dockerfile}"
        );
        assert!(
            dockerfile.ends_with("ENTRYPOINT [\"/usr/local/bin/segment\"]\n"),
            "{reference}:\n{dockerfile}"
        );
        let paths: Vec<&Path> = context.keys().map(PathBuf::as_path).collect();
        for path in &paths {
            let name = path.file_name().expect("a name");
            assert!(
                name != "config.toml" && name != "deployment.json",
                "{reference}: {}",
                path.display()
            );
        }
        if repository == "api-hub" {
            assert_eq!(paths, [Path::new("Dockerfile")], "{reference}");
            assert!(dockerfile.contains(" gdl-hub "), "{dockerfile}");
            continue;
        }
        let segment = repository.strip_prefix("api-").expect("a segment");
        assert!(
            paths.iter().all(|path| path.starts_with("contract")
                || path.starts_with(segment)
                || *path == Path::new("Dockerfile")),
            "{reference}: {paths:?}"
        );
        let manifest = &context[&Path::new(segment).join("Cargo.toml")];
        assert!(manifest.contains("\nmimalloc = "), "{manifest}");
        assert!(
            context[&Path::new(segment).join("src/main.rs")].contains("#[global_allocator]"),
            "{reference}"
        );
    }
}

/// Each tag is a digest of what the image is built from: a change to a
/// segment's settings, which the image does not hold, moves no tag.
#[test]
fn a_change_to_settings_alone_moves_no_tag() {
    let mirror = Mirror::new();
    let fleet = fleet(&fleet_text());
    let references = |spec: &gdl_gen::Spec| -> BTreeSet<String> {
        let mut docker = Fake::default();
        run(&mirror, spec, &fleet, true, &mut docker)
            .into_iter()
            .map(|image| image.reference)
            .collect()
    };
    let before = references(&spec(&mirror));
    let deployment = deployment();
    let changed = deployment.replace("drain_ms = 90000", "drain_ms = 45000");
    assert_ne!(deployment, changed);
    let changed = placed(&mirror, &format!("{SPLIT}{changed}"));
    assert_eq!(before, references(&changed));
}

/// One hub image serves every layout: its pod reads the deployment from
/// the ConfigMap. It is built once per registry and platform, and the
/// pools of both layouts name it at one tag.
#[test]
fn the_hub_has_one_tag_for_every_layout() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, &(nest("split", SPLIT) + &nest("whole", WHOLE)));
    let fleet = fleet(
        "[fleet]\nname = \"api\"\n\
         [cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-api\"\nregion = \"us-east-1\"\n\
         registry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n\
         [class.global]\nready = { front = 1 }\n[class.model_x]\nready = { engine = 1 }\n\
         [pool.global-us]\nlayout = \"split\"\nclass = \"global\"\ncluster = \"use1\"\n\
         [pool.model-x-us]\nlayout = \"split\"\nclass = \"model_x\"\ncluster = \"use1\"\nhome = \"global-us\"\n\
         [pool.global-eu]\nlayout = \"whole\"\nclass = \"global\"\ncluster = \"use1\"\n",
    );
    let mut docker = Fake::default();
    let built = run(&mirror, &spec, &fleet, true, &mut docker);
    let hubs: Vec<&str> = docker
        .builds()
        .iter()
        .map(|(reference, _, _)| *reference)
        .filter(|reference| split(reference).1 == "api-hub")
        .collect();
    assert_eq!(hubs.len(), 1, "{hubs:?}");
    let digests = gdl_gen::Digests::of(&built);
    let files =
        gdl_gen::k8s_files(&spec, &fleet, &mirror.target(), None, Some(&digests)).expect("objects");
    let objects = objects(&files);
    let hub = |pool: &str| {
        container(&objects[&("Deployment".to_string(), pool.to_string())])["image"]
            .as_str()
            .expect("an image")
            .to_string()
    };
    assert_eq!(hub("global-us"), hub("global-eu"));
    assert_eq!(hub("global-us"), hub("model-x-us"));
}

/// `k8s` computes each tag without Docker: the objects name the very
/// references `images` pushed. Given the digests a run recorded, each
/// container names its image by digest too, in its own cluster's registry.
#[test]
fn the_objects_name_the_pushed_digests() {
    let mirror = Mirror::new();
    let spec = spec(&mirror);
    let fleet = fleet(&fleet_text());
    let mut docker = Fake::default();
    let built = run(&mirror, &spec, &fleet, true, &mut docker);
    let pushed: BTreeMap<&str, &str> = built
        .iter()
        .map(|image| (image.reference.as_str(), image.digest.as_str()))
        .collect();
    let images = |digests: Option<&gdl_gen::Digests>| -> BTreeSet<String> {
        let files =
            gdl_gen::k8s_files(&spec, &fleet, &mirror.target(), None, digests).expect("objects");
        objects(&files)
            .iter()
            .filter(|((kind, _), _)| kind == "Deployment")
            .map(|(_, deployment)| {
                container(deployment)["image"]
                    .as_str()
                    .expect("an image")
                    .to_string()
            })
            .collect()
    };
    let by_tag: BTreeSet<String> = pushed.keys().map(|r| r.to_string()).collect();
    assert_eq!(images(None), by_tag);
    let by_digest: BTreeSet<String> = pushed
        .iter()
        .map(|(reference, digest)| format!("{reference}@{digest}"))
        .collect();
    let digests = gdl_gen::Digests::of(&built);
    assert_eq!(images(Some(&digests)), by_digest);
    // The digests survive their file.
    let read = gdl_gen::Digests::from_json(&digests.to_json()).expect("the digests");
    assert_eq!(images(Some(&read)), by_digest);
}

/// An image that the digests do not name is refused by name: the objects
/// would run an image that no run of `images` pushed.
#[test]
fn an_image_missing_from_the_digests_is_refused() {
    let mirror = Mirror::new();
    let spec = spec(&mirror);
    let fleet = fleet(&fleet_text());
    let mut docker = Fake::default();
    let built = run(&mirror, &spec, &fleet, true, &mut docker);
    let kept: Vec<gdl_gen::Built> = built
        .into_iter()
        .filter(|image| !image.reference.contains("/api-engine:"))
        .collect();
    let digests = gdl_gen::Digests::of(&kept);
    let error = gdl_gen::k8s_files(&spec, &fleet, &mirror.target(), None, Some(&digests))
        .expect_err("refused");
    let error = error.to_string();
    assert!(error.contains("/api-engine:"), "{error}");
    assert!(error.contains("cargo gdl images"), "{error}");
}

/// jemalloc fixes the page size at build time, so an image built on one
/// host would not start on a node of larger pages: a spec that chooses it
/// is refused, with the reason.
#[test]
fn jemalloc_is_refused() {
    let mirror = Mirror::new();
    let spec = placed(
        &mirror,
        &format!(
            "{SPLIT}{}[binary]\nallocator = \"jemalloc\"\n",
            deployment()
        ),
    );
    let mut docker = Fake::default();
    let error = gdl_gen::images(
        &spec,
        &fleet(&fleet_text()),
        &mirror.target(),
        true,
        &mut docker,
    )
    .expect_err("refused")
    .to_string();
    assert!(error.contains("jemalloc"), "{error}");
    assert!(error.contains("page size"), "{error}");
    assert!(docker.calls.is_empty());
}

/// An image is built on the build host's platform for its own: only the
/// platforms whose Rust target and cross linker the build installs are
/// taken.
#[test]
fn a_platform_without_a_cross_build_is_refused() {
    let mirror = Mirror::new();
    let mut docker = Fake::default();
    let fleet = with_euw1("registry.example/gdl-eu", "linux/s390x");
    let error = gdl_gen::images(&spec(&mirror), &fleet, &mirror.target(), true, &mut docker)
        .expect_err("refused")
        .to_string();
    assert!(error.contains("linux/s390x"), "{error}");
    assert!(docker.calls.is_empty());
}
