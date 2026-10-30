//! A fleet's images: the hub's, and one per layout and segment that a pool
//! runs, each built for every platform and registry of the clusters that
//! run it. An image holds only its binary, at `/usr/local/bin/segment`:
//! each pod reads its settings from its pool's ConfigMap. So one hub image
//! serves every layout.
//!
//! Each image is tagged with a sha256 of what it is built from: its
//! platform and every file of its build context, its Dockerfile included.
//! `k8s` computes the tags without Docker, and `images` skips an image
//! whose tag its registry already holds. The binary is built on the build
//! host's platform for the image's, with a cross linker when the two
//! differ, so that no compiler runs under emulation. See
//! `docs/src/guides/kubernetes.md`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use gdl_model::{Fleet, PoolSettings};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::k8s::Prepared;
use super::{io, quote};
use crate::{Error, Files, emit};

mod buildx;

pub use buildx::Buildx;

/// The platforms an image is built for: those whose Rust target and C
/// cross compiler [`TOOLCHAIN`] installs.
const PLATFORMS: [&str; 3] = ["linux/amd64", "linux/arm64", "linux/arm64/v8"];

/// Builds images, and asks registries what they hold. [`Buildx`] runs
/// `docker buildx` with the Docker login the user already has; tests give
/// a fake.
pub trait Docker {
    /// The manifest digest (`sha256:<hex>`) of `reference` in its
    /// registry, or `None` when the registry does not hold it.
    fn digest(&mut self, reference: &str) -> Result<Option<String>, Error>;

    /// Build the image whose context is the directory `context`, with its
    /// `Dockerfile` there, for `platform`, tagged `reference`. With `push`
    /// it goes to its registry, else into the local image store. Returns
    /// its manifest digest.
    fn build(
        &mut self,
        context: &Path,
        platform: &str,
        reference: &str,
        push: bool,
    ) -> Result<String, Error>;
}

/// One image that `images` built, or found in its registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Built {
    /// `<registry>/<repository>:<tag>`.
    pub reference: String,
    /// Its manifest digest, `sha256:<hex>`.
    pub digest: String,
    /// Whether it was pushed, found, or loaded.
    pub outcome: Outcome,
}

/// What `images` did with one image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Built and pushed to its registry.
    Pushed,
    /// Its registry already held its tag: neither built nor pushed.
    Present,
    /// Built into the local image store, without `--push`.
    Loaded,
}

/// The digest of each image in its registry, by its reference: what a run
/// of `images` with `--push` recorded, for `k8s` to name each image by.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Digests(BTreeMap<String, String>);

impl Digests {
    /// The digests of the images of `built` that are in their registries.
    pub fn of(built: &[Built]) -> Self {
        Self(
            built
                .iter()
                .filter(|image| image.outcome != Outcome::Loaded)
                .map(|image| (image.reference.clone(), image.digest.clone()))
                .collect(),
        )
    }

    /// The digests a file holds: a JSON object from each reference to its
    /// digest. Refused unless each digest is `sha256:` and 64 hex digits.
    pub fn from_json(text: &str) -> Result<Self, Error> {
        let digests: Self = serde_json::from_str(text).map_err(|e| {
            Error::new(format!("the digests are not a JSON object of strings: {e}"))
        })?;
        for (reference, digest) in &digests.0 {
            check_digest(digest).map_err(|why| Error::new(format!("`{reference}`: {why}")))?;
        }
        Ok(digests)
    }

    /// The digests as [`Digests::from_json`] reads them.
    pub fn to_json(&self) -> String {
        let mut text = serde_json::to_string_pretty(&self.0).expect("a map of strings");
        text.push('\n');
        text
    }

    pub(super) fn get(&self, reference: &str) -> Option<&str> {
        self.0.get(reference).map(String::as_str)
    }
}

/// Refuse a digest that is not `sha256:` and 64 lowercase hex digits.
pub(super) fn check_digest(digest: &str) -> Result<(), String> {
    let hex = digest.strip_prefix("sha256:").unwrap_or_default();
    if hex.len() == 64
        && hex
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    {
        return Ok(());
    }
    Err(format!(
        "`{digest}` is not a digest: `sha256:` and 64 hex digits"
    ))
}

/// Build `fleet`'s images for `spec` with `docker`, after the checks that
/// `k8s` makes. With `push`, an image whose tag its registry holds is
/// neither built nor pushed, and each other one is built and pushed;
/// without it, no registry is asked, and each is built into the local
/// image store. Returns each image, in reference order.
pub fn images(
    spec: &crate::Spec,
    fleet: &Fleet,
    target: &Path,
    push: bool,
    docker: &mut dyn Docker,
) -> Result<Vec<Built>, Error> {
    let plan = super::k8s::judged(spec, fleet, target)?;
    let unsupported: Vec<String> = plan
        .iter()
        .map(|image| image.platform.as_str())
        .filter(|platform| !PLATFORMS.contains(platform))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|platform| {
            format!(
                "platform `{platform}`: `images` builds for {} only, whose Rust target and cross linker it installs",
                PLATFORMS.join(", ")
            )
        })
        .collect();
    if !unsupported.is_empty() {
        return Err(Error::all(unsupported));
    }
    let scratch = tempfile::Builder::new()
        .prefix("gdl-images-")
        .tempdir()
        .map_err(io)?;
    let mut built = Vec::new();
    for (index, image) in plan.iter().enumerate() {
        let reference = image.reference();
        if push && let Some(digest) = docker.digest(&reference)? {
            check_digest(&digest).map_err(|why| Error::new(format!("`{reference}`: {why}")))?;
            built.push(Built {
                reference,
                digest,
                outcome: Outcome::Present,
            });
            continue;
        }
        let context = scratch.path().join(index.to_string());
        std::fs::create_dir(&context).map_err(io)?;
        crate::write(&context, &image.context)?;
        let digest = docker.build(&context, &image.platform, &reference, push)?;
        check_digest(&digest).map_err(|why| Error::new(format!("`{reference}`: {why}")))?;
        built.push(Built {
            reference,
            digest,
            outcome: if push {
                Outcome::Pushed
            } else {
                Outcome::Loaded
            },
        });
    }
    Ok(built)
}

/// One image of a fleet: what it is built from, for which platform, and
/// where it goes.
pub(super) struct Image {
    pub(super) registry: String,
    pub(super) repository: String,
    pub(super) platform: String,
    /// The sha256 of the platform and the context, as hex.
    pub(super) tag: String,
    /// Its build context, the `Dockerfile` included, shared by every
    /// registry it goes to.
    context: Rc<Files>,
}

impl Image {
    pub(super) fn reference(&self) -> String {
        super::k8s::image(&self.registry, &self.repository, &self.tag, None)
    }
}

/// Every image the pools of `fleet` run, once for each registry and
/// platform of their clusters, in reference order. [`Prepared`] has
/// checked that each pool has a cluster, and each cluster a registry and a
/// platform.
pub(super) fn plan(prepared: &Prepared, fleet: &Fleet) -> Result<Vec<Image>, Error> {
    let bundle = &prepared.bundle;
    let graph = &bundle.graph;
    let hub = Rc::new(Files::from([(
        PathBuf::from("Dockerfile"),
        hub_dockerfile(bundle, fleet),
    )]));
    let hub_repository = emit::package_name(graph, "hub");
    let pools: Vec<PoolSettings> = fleet.pools().collect();
    // By registry, repository, and platform: two clusters that share a
    // registry and a platform share each image.
    let mut images = BTreeMap::new();
    let mut add = |cluster: &gdl_model::Cluster, repository: &str, context: &Rc<Files>| {
        let registry = cluster.registry.clone().unwrap_or_default();
        let platform = cluster.platform.clone().unwrap_or_default();
        images
            .entry((registry.clone(), repository.to_string(), platform.clone()))
            .or_insert_with(|| Image {
                tag: tag(&platform, context),
                registry,
                repository: repository.to_string(),
                platform,
                context: Rc::clone(context),
            });
    };
    for laid in prepared.spec.layouts() {
        let layout = prepared
            .layouts
            .get(laid.name)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::new(laid.named("the placement does not fit the graph".into())))?;
        let mut contexts: BTreeMap<&str, Rc<Files>> = BTreeMap::new();
        for pool in pools
            .iter()
            .filter(|pool| pool.layout.as_deref().unwrap_or(laid.name) == laid.name)
        {
            let (_, cluster) = fleet
                .cluster_of(&pool.name)
                .expect("Prepared requires every pool's cluster");
            add(cluster, &hub_repository, &hub);
            for (segment, _) in laid.placement.segments() {
                if layout.class_of(segment) != Some(pool.class.as_str()) {
                    continue;
                }
                let context = contexts.entry(segment).or_insert_with(|| {
                    let package = laid.package(graph, segment);
                    Rc::new(segment_context(bundle, &laid.dir().join(segment), &package))
                });
                let context = Rc::clone(context);
                add(cluster, &laid.package(graph, segment), &context);
            }
        }
    }
    // The DynamoDB directory's tables Job runs the hub's image in the
    // directory's cluster, which may run no pool.
    if let Some(cluster) = fleet
        .directory()
        .filter(|directory| directory.local)
        .and_then(|directory| directory.cluster.as_deref())
        .and_then(|name| fleet.cluster(name))
    {
        add(cluster, &hub_repository, &hub);
    }
    let mut images: Vec<Image> = images.into_values().collect();
    images.sort_by_key(Image::reference);
    Ok(images)
}

/// The tag of an image built for `platform` from `context`: the sha256,
/// as hex, of the platform and each file's path and text, in path order,
/// each with its length, so that no two contexts read alike.
fn tag(platform: &str, context: &Files) -> String {
    let mut hash = Sha256::new();
    let mut field = |bytes: &[u8]| {
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    };
    field(b"gdl image");
    field(platform.as_bytes());
    for (path, text) in context {
        field(path.to_string_lossy().as_bytes());
        field(text.as_bytes());
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The build context of the segment whose crate is in `crate_dir`, which
/// builds package `package`: the contract crate, the segment's crate
/// without its settings, each file with no absolute path ([`placeless`]),
/// and a Dockerfile.
fn segment_context(bundle: &super::Bundle, crate_dir: &Path, package: &str) -> Files {
    let settings = crate_dir.join("config.toml");
    let mut files: Files = bundle
        .files
        .iter()
        .filter(|(path, _)| {
            path.starts_with("contract") || (path.starts_with(crate_dir) && **path != settings)
        })
        .map(|(path, text)| (path.clone(), placeless(text)))
        .collect();
    let settings = &bundle.settings;
    let segment = crate_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let base = settings
        .segment
        .get(&segment)
        .and_then(|image| image.base_image.as_ref())
        .unwrap_or(&settings.base_image);
    let manifest = format!("/app/{}/Cargo.toml", crate_dir.display());
    let build = format!(
        "cargo build --release --target \"$GDL_TARGET\" --manifest-path {} && cp \"$CARGO_TARGET_DIR/$GDL_TARGET/release/{package}\" /gdl/segment",
        quote(&manifest)
    );
    files.insert(
        "Dockerfile".into(),
        dockerfile(&settings.build_image, base, true, &build),
    );
    files
}

/// `text`, a generated file, with its `Regenerate with:` comment naming
/// the command that builds images instead. The generated comment names a
/// spec loaded from a file by its absolute path, which would make the tag
/// depend on where the checkout is. The comment changes no binary. The
/// generator writes the path on one line, its line breaks escaped.
fn placeless(text: &str) -> String {
    const PREFIXES: [&str; 2] = ["# Regenerate with: ", "// Regenerate with: "];
    text.split_inclusive('\n')
        .map(
            |line| match PREFIXES.iter().find(|prefix| line.starts_with(*prefix)) {
                Some(prefix) => format!("{prefix}cargo gdl images --spec <spec> --fleet <fleet>\n"),
                None => line.to_string(),
            },
        )
        .collect()
}

/// The Dockerfile of the hub, installed as the spec's `[gdl]` names it.
fn hub_dockerfile(bundle: &super::Bundle, fleet: &Fleet) -> String {
    let settings = &bundle.settings;
    // The DynamoDB directory, and its tables step, are a feature of the
    // hub, off by default, since it links the AWS SDK.
    let features = match fleet.directory() {
        Some(_) => " --features dynamodb",
        None => "",
    };
    let build = format!(
        "cargo install {}{features} --locked --root /build-tools --target \"$GDL_TARGET\" gdl-hub && cp /build-tools/bin/gdl-hub /gdl/segment",
        bundle.hub_install
    );
    dockerfile(
        &settings.build_image,
        settings.hub_image.as_ref().unwrap_or(&settings.base_image),
        false,
        &build,
    )
}

/// A Dockerfile that runs `build` in `builder`, on the build host's
/// platform, after [`TOOLCHAIN`], with the context at `/app` when
/// `copies`, and copies the binary `build` leaves at `/gdl/segment` into
/// `base`, of the image's platform. The final stage runs nothing, so it
/// needs no emulation.
fn dockerfile(builder: &str, base: &str, copies: bool, build: &str) -> String {
    let copy = if copies {
        "WORKDIR /app\nCOPY . /app\n"
    } else {
        ""
    };
    format!(
        "# {marker}\n\
         FROM --platform=$BUILDPLATFORM {builder} AS build\n\
         ARG TARGETARCH\n\
         RUN {TOOLCHAIN}\n\
         {copy}\
         ENV CARGO_TARGET_DIR=/build\n\
         RUN . /gdl/target.env && {build}\n\
         FROM {base}\n\
         COPY --from=build /gdl/segment /usr/local/bin/segment\n\
         ENTRYPOINT [\"/usr/local/bin/segment\"]\n",
        marker = emit::MARKER,
    )
}

/// The build stage's toolchain for the image's architecture: its Rust
/// target in `GDL_TARGET`, and, when the build host's differs, the target's
/// standard library, a C cross compiler and the target's C library, which
/// link the binary and build mimalloc. `/gdl/target.env` sets them for
/// the build. Debian names the packages, as the default build image is.
const TOOLCHAIN: &str = "set -eu; mkdir -p /gdl; \\
    case \"$TARGETARCH\" in \\
      amd64) target=x86_64-unknown-linux-gnu; gnu=x86_64-linux-gnu; packages=\"gcc-x86-64-linux-gnu libc6-dev-amd64-cross\"; linker=CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER; cc=CC_x86_64_unknown_linux_gnu ;; \\
      arm64) target=aarch64-unknown-linux-gnu; gnu=aarch64-linux-gnu; packages=\"gcc-aarch64-linux-gnu libc6-dev-arm64-cross\"; linker=CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER; cc=CC_aarch64_unknown_linux_gnu ;; \\
      *) echo \"no Rust target for architecture $TARGETARCH\" >&2; exit 1 ;; \\
    esac; \\
    echo \"export GDL_TARGET=$target\" > /gdl/target.env; \\
    if [ \"$(rustc -vV | sed -n 's/^host: //p')\" != \"$target\" ]; then \\
      apt-get update && apt-get install -y --no-install-recommends $packages && rm -rf /var/lib/apt/lists/*; \\
      rustup target add \"$target\"; \\
      echo \"export $linker=$gnu-gcc $cc=$gnu-gcc\" >> /gdl/target.env; \\
    fi";

#[cfg(test)]
mod tests {
    use super::*;

    /// A tag digests the platform and every path and byte of the context:
    /// changing any one moves it, and moving a byte from a file's text to
    /// its path does too.
    #[test]
    fn a_tag_digests_the_platform_and_each_file() {
        let context = |files: &[(&str, &str)]| -> Files {
            files
                .iter()
                .map(|(path, text)| (PathBuf::from(path), text.to_string()))
                .collect()
        };
        let base = context(&[("Dockerfile", "FROM x\n"), ("a/b.rs", "fn main() {}\n")]);
        let tag = tag("linux/amd64", &base);
        assert_eq!(tag.len(), 64);
        assert_eq!(tag, super::tag("linux/amd64", &base.clone()));
        for (platform, other) in [
            ("linux/arm64", base.clone()),
            (
                "linux/amd64",
                context(&[("Dockerfile", "FROM y\n"), ("a/b.rs", "fn main() {}\n")]),
            ),
            (
                "linux/amd64",
                context(&[("Dockerfile", "FROM x\n"), ("a/c.rs", "fn main() {}\n")]),
            ),
            ("linux/amd64", context(&[("Dockerfile", "FROM x\n")])),
            (
                "linux/amd64",
                context(&[("Dockerfile", "FROM x\na/b.rs"), ("", "fn main() {}\n")]),
            ),
        ] {
            assert_ne!(tag, super::tag(platform, &other), "{platform} {other:?}");
        }
    }

    /// The comment that names the spec by its path names none, in either
    /// comment form, and every other line is kept as it is.
    #[test]
    fn a_context_names_no_path() {
        let text = "# @generated\n# Regenerate with: cargo gdl export --spec \"/home/a/gdl.toml\" --out <archive.tar.gz>\n[package]\nname = \"x\"\n";
        assert_eq!(
            placeless(text),
            "# @generated\n# Regenerate with: cargo gdl images --spec <spec> --fleet <fleet>\n[package]\nname = \"x\"\n"
        );
        let rust = "// Regenerate with: cargo gdl export --spec \"/srv/b/gdl.toml\" --out <archive.tar.gz>\nfn main() {}";
        assert_eq!(
            placeless(rust),
            "// Regenerate with: cargo gdl images --spec <spec> --fleet <fleet>\nfn main() {}"
        );
        let other = "let s = \"# Regenerate with: kept\";\n";
        assert_eq!(placeless(other), other);
    }

    /// A digest is `sha256:` and 64 lowercase hex digits, and a file of
    /// digests holds only such.
    #[test]
    fn a_digest_is_shaped() {
        let good = format!("sha256:{}", "ab".repeat(32));
        assert!(check_digest(&good).is_ok());
        for bad in [
            String::new(),
            "sha256:".into(),
            "ab".repeat(32),
            format!("sha512:{}", "ab".repeat(32)),
            format!("sha256:{}", "AB".repeat(32)),
            format!("sha256:{}", "ab".repeat(31)),
        ] {
            assert!(check_digest(&bad).is_err(), "{bad}");
        }
        let file = format!("{{\"r.example/a:t\": \"{good}\"}}");
        let digests = Digests::from_json(&file).expect("digests");
        assert_eq!(digests.get("r.example/a:t"), Some(good.as_str()));
        assert_eq!(
            Digests::from_json(&digests.to_json()).expect("digests"),
            digests
        );
        assert!(Digests::from_json("{\"r.example/a:t\": \"latest\"}").is_err());
        assert!(Digests::from_json("[]").is_err());
    }

    /// The build stage runs on the build host's platform, and only the
    /// final stage, which runs nothing, is of the image's.
    #[test]
    fn only_the_final_stage_is_of_the_images_platform() {
        let text = dockerfile("builder", "base", true, "true");
        let froms: Vec<&str> = text.lines().filter(|l| l.starts_with("FROM ")).collect();
        assert_eq!(
            froms,
            [
                "FROM --platform=$BUILDPLATFORM builder AS build",
                "FROM base"
            ]
        );
        let last = text.split("FROM base\n").nth(1).expect("a final stage");
        assert!(!last.contains("RUN"), "{last}");
    }
}
