//! A fleet's images: the hub's, and one per layout and segment that a pool
//! runs, each built for every platform and registry of the clusters that
//! run it. An image holds only its binary, at `/usr/local/bin/segment`:
//! each pod reads its settings from its pool's ConfigMap. So one hub image
//! serves every layout.
//!
//! Each image is tagged with a sha256 of what it is built from: its
//! platform and every file of its build context, its Dockerfile included.
//! With local sources ([`Sources::Local`]) every image builds from one
//! tree ([`super::tree`]): gdl's workspace and each crate the spec names by
//! path, as Git holds them, with the bundle placed in it as in gdl's
//! workspace, and each segment's lock settled there. So each tag covers
//! those sources, and a change to one moves every tag.
//! `k8s` computes the tags without Docker, and `images` skips an image
//! whose tag its registry already holds. The binary is built on the build
//! host's platform for the image's, with a cross linker when the two
//! differ, so that no compiler runs under emulation. See
//! `docs/src/guides/kubernetes.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use gdl_model::{Fleet, FleetBackend, PoolSettings};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::k8s::Prepared;
use super::{Sources, io, quote};
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
/// `k8s` makes of the fleet and its DGD files, `dgds`. With `push`, an
/// image whose tag its registry holds is neither built nor pushed, and each other one is built and pushed;
/// without it, no registry is asked, and each is built into the local
/// image store. Returns each image, in reference order.
pub fn images(
    spec: &crate::Spec,
    fleet: &Fleet,
    dgds: &super::Dgds,
    target: &Path,
    push: bool,
    sources: Sources,
    docker: &mut dyn Docker,
) -> Result<Vec<Built>, Error> {
    let plan = super::k8s::judged(spec, fleet, dgds, target, sources)?;
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
        let context = match &image.context {
            Source::Files(files) => {
                let context = scratch.path().join(index.to_string());
                std::fs::create_dir(&context).map_err(io)?;
                crate::write(&context, files)?;
                context
            }
            // The tree is shared: its Dockerfile is this image's while it
            // builds.
            Source::Tree { tree, dockerfile } => {
                std::fs::write(tree.dir.path().join("Dockerfile"), dockerfile).map_err(io)?;
                tree.dir.path().to_path_buf()
            }
        };
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
    /// The clusters whose pools run it, by name.
    pub(super) clusters: BTreeSet<String>,
    /// Its build context, shared by every registry it goes to.
    context: Source,
}

/// What an image builds from.
#[derive(Clone)]
enum Source {
    /// Its own files, the `Dockerfile` among them.
    Files(Rc<Files>),
    /// A tree of local sources that every image of the spec shares, and
    /// this image's Dockerfile, written at the tree's root for its build.
    Tree { tree: Rc<Tree>, dockerfile: String },
}

impl Source {
    /// The sha256, as hex, of `platform` and what this source holds.
    fn tag(&self, platform: &str) -> String {
        match self {
            Self::Files(files) => tag(platform, files),
            Self::Tree { tree, dockerfile } => tag(
                platform,
                &Files::from([
                    (PathBuf::from("Dockerfile"), dockerfile.clone()),
                    (PathBuf::from("sources"), tree.digest.clone()),
                ]),
            ),
        }
    }
}

/// The tree of local sources that a spec's images build from, with the
/// bundle in it, and the sha256 of what it holds.
struct Tree {
    dir: tempfile::TempDir,
    digest: String,
}

impl Image {
    pub(super) fn reference(&self) -> String {
        super::k8s::image(&self.registry, &self.repository, &self.tag, None)
    }
}

/// Every image the pools of `fleet` run, once for each registry, platform,
/// and base image of their clusters, in reference order, each with the
/// clusters that run it. [`Prepared`] has checked that each pool has a
/// cluster, and each cluster a registry and a platform. A Slurm site's
/// images are built on its own base image ([`Bases::of`]).
pub(super) fn plan(prepared: &Prepared, fleet: &Fleet) -> Result<Vec<Image>, Error> {
    let bundle = &prepared.bundle;
    let graph = &bundle.graph;
    let local = match &bundle.context {
        Some(context) => Some(Local::new(prepared, context)?),
        None => None,
    };
    // The hub's build context, by the image it is built on.
    let mut hubs: BTreeMap<String, Source> = BTreeMap::new();
    let mut hub = |base: &str| {
        hubs.entry(base.to_string())
            .or_insert_with(|| {
                let dockerfile = hub_dockerfile(bundle, fleet, base);
                match &local {
                    None => Source::Files(Rc::new(Files::from([(
                        PathBuf::from("Dockerfile"),
                        dockerfile,
                    )]))),
                    Some(local) => local.source(dockerfile),
                }
            })
            .clone()
    };
    let hub_repository = emit::package_name(graph, "hub");
    let pools: Vec<PoolSettings> = fleet.pools().collect();
    // By registry, repository, platform, and tag: two clusters that share
    // a registry, a platform, and a base image share each image.
    let mut images = BTreeMap::new();
    let mut add = |name: &str, cluster: &gdl_model::Cluster, repository: &str, context: &Source| {
        let registry = cluster.registry().unwrap_or_default().to_string();
        let platform = cluster.platform().unwrap_or_default().to_string();
        let tag = context.tag(&platform);
        images
            .entry((
                registry.clone(),
                repository.to_string(),
                platform.clone(),
                tag.clone(),
            ))
            .or_insert_with(|| Image {
                tag,
                registry,
                repository: repository.to_string(),
                platform,
                clusters: BTreeSet::new(),
                context: context.clone(),
            })
            .clusters
            .insert(name.to_string());
    };
    for laid in prepared.spec.layouts() {
        let layout = prepared
            .layouts
            .get(laid.name)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::new(laid.named("the placement does not fit the graph".into())))?;
        let mut contexts: BTreeMap<(&str, &str, String), Source> = BTreeMap::new();
        for pool in pools
            .iter()
            .filter(|pool| pool.layout.as_deref().unwrap_or(laid.name) == laid.name)
        {
            let (name, cluster) = fleet
                .cluster_of(&pool.name)
                .expect("Prepared requires every pool's cluster");
            let bases = Bases::of(&bundle.settings, cluster);
            add(name, cluster, &hub_repository, &hub(bases.hub));
            for (segment, _) in laid.placement.segments() {
                if layout.class_of(segment) != Some(pool.class.as_str()) {
                    continue;
                }
                // By architecture: `[deployment.codegen.<arch>]` reaches
                // the Dockerfile of its own architecture's images only.
                let arch = arch(cluster.platform().unwrap_or_default());
                let base = bases.segment(&bundle.settings, segment).to_string();
                let context =
                    contexts
                        .entry((segment, arch, base.clone()))
                        .or_insert_with(|| {
                            let package = laid.package(graph, segment);
                            let crate_dir = laid.dir().join(segment);
                            let codegen = codegen(bundle, segment, arch);
                            match &local {
                                None => Source::Files(Rc::new(segment_context(
                                    bundle, &crate_dir, &package, &codegen, &base,
                                ))),
                                Some(local) => local.source(local.segment_dockerfile(
                                    bundle, &crate_dir, &package, &codegen, &base,
                                )),
                            }
                        });
                let context = context.clone();
                add(name, cluster, &laid.package(graph, segment), &context);
            }
        }
    }
    // The DynamoDB directory's tables step runs the hub's image in the
    // directory's cluster, which may run no pool; so does a Slurm site's
    // etcd job, whose processes run in the job's container.
    if let Some(directory) = fleet.directory().filter(|d| d.runs_store())
        && let Some(name) = directory.cluster.as_deref()
        && let Some(cluster) = fleet.cluster(name)
        && match directory.backend {
            FleetBackend::Dynamodb { .. } => true,
            FleetBackend::Etcd => cluster.slurm().is_some(),
        }
    {
        let bases = Bases::of(&bundle.settings, cluster);
        add(name, cluster, &hub_repository, &hub(bases.hub));
    }
    let mut images: Vec<Image> = images.into_values().collect();
    images.sort_by_key(Image::reference);
    Ok(images)
}

/// The images a cluster's images are built on. A Kubernetes cluster's are
/// the spec's `[deployment] base_image` and `hub_image`. A Slurm site's
/// are its own `base_image` and `hub_image`, or
/// [`gdl_model::SlurmSite::DEFAULT_BASE_IMAGE`]: srt-slurm runs every
/// command under `bash -c`, and the spec's default base has no shell. A
/// segment's own `[deployment.segment.<name>] base_image` wins on either.
struct Bases<'a> {
    base: &'a str,
    hub: &'a str,
}

impl<'a> Bases<'a> {
    fn of(settings: &'a super::Settings, cluster: &'a gdl_model::Cluster) -> Self {
        match cluster.slurm() {
            None => Self {
                base: &settings.base_image,
                hub: settings.hub_image.as_ref().unwrap_or(&settings.base_image),
            },
            Some(site) => {
                let base = site
                    .base_image
                    .as_deref()
                    .unwrap_or(gdl_model::SlurmSite::DEFAULT_BASE_IMAGE);
                Self {
                    base,
                    hub: site.hub_image.as_deref().unwrap_or(base),
                }
            }
        }
    }

    /// The image segment `segment`'s image is built on.
    fn segment(&self, settings: &'a super::Settings, segment: &str) -> &'a str {
        settings
            .segment
            .get(segment)
            .and_then(|image| image.base_image.as_deref())
            .unwrap_or(self.base)
    }
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
/// and a Dockerfile, whose binary is copied into `base`.
fn segment_context(
    bundle: &super::Bundle,
    crate_dir: &Path,
    package: &str,
    codegen: &str,
    base: &str,
) -> Files {
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
    let manifest = format!("/app/{}/Cargo.toml", crate_dir.display());
    let build = format!(
        "{codegen}cargo build --release --target \"$GDL_TARGET\" --manifest-path {} && cp \"$CARGO_TARGET_DIR/$GDL_TARGET/release/{package}\" /gdl/segment",
        quote(&manifest)
    );
    files.insert(
        "Dockerfile".into(),
        dockerfile(
            &settings.build_image,
            base,
            true,
            &build,
            &super::segment_tunables_env(),
        ),
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

/// The tree of local sources a spec's images build from, once it is made.
struct Local {
    tree: Rc<Tree>,
    /// The bundle's directory in the tree.
    bundle: PathBuf,
}

impl Local {
    /// Build the tree of `context`'s sources ([`super::tree`]), with
    /// `prepared`'s bundle at the bundle's place there, each segment's
    /// manifest patched to build against gdl's sources, and its lock
    /// settled from gdl's, as `compose --local` does.
    fn new(prepared: &Prepared, context: &super::context::Context) -> Result<Self, Error> {
        let dir = tempfile::Builder::new()
            .prefix("gdl-images-local-")
            .tempdir()
            .map_err(io)?;
        let mut files: Files = prepared
            .bundle
            .files
            .iter()
            .filter(|(path, _)| path.file_name().is_none_or(|name| name != "config.toml"))
            .map(|(path, text)| (path.clone(), placeless(text)))
            .collect();
        let segments: Vec<PathBuf> = files
            .keys()
            .filter(|path| path.file_name().is_some_and(|name| name == "Cargo.toml"))
            .filter_map(|path| path.parent())
            .filter(|dir| !dir.starts_with("contract"))
            .map(Path::to_path_buf)
            .collect();
        for crate_dir in &segments {
            super::localize(&mut files, context, crate_dir)?;
        }
        let digest = super::tree::build(dir.path(), context, &files, &segments)?;
        Ok(Self {
            tree: Rc::new(Tree { dir, digest }),
            bundle: context.bundle().to_path_buf(),
        })
    }

    fn source(&self, dockerfile: String) -> Source {
        Source::Tree {
            tree: Rc::clone(&self.tree),
            dockerfile,
        }
    }

    /// The Dockerfile of the segment whose crate is `crate_dir` in the
    /// bundle, which builds package `package` by its settled lock, and
    /// copies it into `base`.
    fn segment_dockerfile(
        &self,
        bundle: &super::Bundle,
        crate_dir: &Path,
        package: &str,
        codegen: &str,
        base: &str,
    ) -> String {
        let settings = &bundle.settings;
        let manifest = format!("/app/{}/Cargo.toml", self.bundle.join(crate_dir).display());
        let build = format!(
            "{codegen}cargo build --locked --release --target \"$GDL_TARGET\" --manifest-path {} && cp \"$CARGO_TARGET_DIR/$GDL_TARGET/release/{package}\" /gdl/segment",
            quote(&manifest)
        );
        dockerfile(
            &settings.build_image,
            base,
            true,
            &build,
            &super::segment_tunables_env(),
        )
    }
}

/// The Dockerfile of the hub, installed as the spec's `[gdl]` names it,
/// on `base`: with local sources, from the hub's crate in the context it
/// copies.
fn hub_dockerfile(bundle: &super::Bundle, fleet: &Fleet, base: &str) -> String {
    let settings = &bundle.settings;
    // The directory's backend, and the DynamoDB tables step, are a feature
    // of the hub, off by default.
    let (needs, features) = super::hub_feature(fleet.directory().map(|d| &d.backend));
    let build = format!(
        "{needs}cargo install {}{features} --locked --root /build-tools --target \"$GDL_TARGET\" gdl-hub && cp /build-tools/bin/gdl-hub /gdl/segment",
        bundle.hub_install
    );
    dockerfile(
        &settings.build_image,
        base,
        bundle.context.is_some(),
        &build,
        "",
    )
}

/// A Dockerfile that runs `build` in `builder`, on the build host's
/// platform, after [`TOOLCHAIN`], with the context at `/app` when
/// `copies`, and copies the binary `build` leaves at `/gdl/segment` into
/// `base`, of the image's platform, with the build stage's CA bundle
/// ([`CA_BUNDLE`]), which a slim base lacks. The final stage runs nothing, so
/// it needs no emulation.
/// A segment's image carries `env`, [`super::segment_tunables_env`]; the
/// hub's carries nothing more.
fn dockerfile(builder: &str, base: &str, copies: bool, build: &str, env: &str) -> String {
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
         RUN {CA_CERTIFICATES}\n\
         {copy}\
         ENV CARGO_TARGET_DIR=/build\n\
         RUN . /gdl/target.env && {build}\n\
         FROM {base}\n\
         {env}\
         COPY --from=build {CA_BUNDLE} {CA_BUNDLE}\n\
         COPY --from=build /gdl/segment /usr/local/bin/segment\n\
         ENTRYPOINT [\"/usr/local/bin/segment\"]\n",
        marker = emit::MARKER,
    )
}

/// The architecture of `platform`, `<os>/<arch>[/<variant>]`, as Docker
/// names it in `TARGETARCH`: `arm64` for `linux/arm64/v8`.
pub(super) fn arch(platform: &str) -> &str {
    platform.split('/').nth(1).unwrap_or_default()
}

/// What the build of segment `segment`'s image for architecture `arch`
/// runs before cargo, from the spec's `[deployment.codegen.<arch>]`
/// table: nothing without one, so that a spec without one builds as it
/// did before the table existed.
fn codegen(bundle: &super::Bundle, segment: &str, arch: &str) -> String {
    match bundle.settings.codegen(arch) {
        None => String::new(),
        Some(codegen) => codegen_guard(segment, arch, codegen),
    }
}

/// The checks and the flags of `codegen`, the table of `arch`, in the
/// build of segment `segment`'s image, each followed by `&&` and before
/// cargo, after `/gdl/target.env`. Each check ends the build with one line
/// that names the table:
///
/// - The image's architecture (`TARGETARCH`) must be `arch`: a build of
///   this Dockerfile for another platform would apply the table to an
///   image it does not name.
/// - `RUSTFLAGS` and `CARGO_ENCODED_RUSTFLAGS` must be empty or unset: a
///   flag there could set LTO, which rustc reads after the profile's thin
///   LTO, and fat LTO runs the features before `main`. The step cannot
///   tell which flags do (`-C`, `--codegen`, `_` for `-`, `@file`), so it
///   reads none of them. `RUSTC`, `RUSTC_WRAPPER`,
///   `RUSTC_WORKSPACE_WRAPPER`, and the build image's Cargo configuration
///   are trusted, as the build image itself is.
/// - Each feature must be one the build image's rustc knows
///   for the target: rustc only warns of an unknown one, and builds
///   without it. A feature counts only in rustc's own list, where a name
///   matches a whole line as fixed text: as a pattern, `lse.` would match
///   `lse2`.
///
/// `RUSTFLAGS` is then exactly the table's flags. Cargo reads it in place
/// of rustflags in its configuration or environment (`build.rustflags`,
/// `CARGO_BUILD_RUSTFLAGS`, and their `target` forms). Cargo builds with
/// `--target`, so the flags do not reach build scripts and proc macros,
/// which run on the build host.
///
/// The build uses thin LTO, not the profile's fat LTO: fat LTO compiles
/// std's start-up with the flags too, and it then runs an instruction of
/// the features before `main`, so a CPU without them faults before the
/// binary's start check (`gdl_runtime::check_cpu_features`) can refuse
/// it. See `docs/src/development/codegen-design.md`.
fn codegen_guard(segment: &str, arch: &str, codegen: &super::Codegen) -> String {
    let table = format!("[deployment.codegen.{arch}]");
    let fail = |why: String| format!("{{ echo \"segment {segment}: {why}\" >&2; exit 1; }}");
    let mut steps = vec![format!(
        "{{ [ \"$TARGETARCH\" = {arch} ] || {}; }}",
        fail(format!(
            "this build applies {table}, but its image is for $TARGETARCH"
        ))
    )];
    // Any flag the build image sets is refused, not read: rustc takes
    // codegen options in several spellings, with `_` for `-`, and from an
    // `@file`, so no list of the ones that set LTO is complete. An empty
    // `CARGO_ENCODED_RUSTFLAGS` is unset, since cargo would read it in
    // place of `RUSTFLAGS`.
    for variable in ["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"] {
        steps.push(format!(
            "{{ [ -z \"${{{variable}:-}}\" ] || {}; }}",
            fail(format!(
                "the build image sets {variable}=${variable}, which this build step cannot read safely: an image with {table} builds with exactly the table's flags"
            ))
        ));
    }
    steps.push("unset CARGO_ENCODED_RUSTFLAGS".to_string());
    let features: Vec<&str> = codegen.feature_names().collect();
    if !features.is_empty() {
        steps.push(
            "features=$(rustc --print target-features --target \"$GDL_TARGET\" | sed -n '/^Features supported by rustc/,/^$/s/^    \\([^ ]*\\).*/\\1/p')".to_string(),
        );
        for feature in features {
            steps.push(format!(
                "{{ printf '%s\\n' \"$features\" | grep -qxF -- {feature} || {}; }}",
                fail(format!(
                    "{table} names target feature {feature}, which the build image's rustc does not know for $GDL_TARGET"
                ))
            ));
        }
    }
    steps.push("export CARGO_PROFILE_RELEASE_LTO=thin".to_string());
    steps.push(format!("export RUSTFLAGS=\"{}\"", codegen.rustflags()));
    steps
        .into_iter()
        .map(|step| format!("{step} && \\\n    "))
        .collect()
}

/// The build stage's toolchain for the image's architecture: its Rust
/// target in `GDL_TARGET`, and, when the build host's differs, the target's
/// standard library, a C and a C++ cross compiler, and the target's C
/// library, which link the binary and build mimalloc and any crate that
/// builds C or C++ with the `cc` crate, such as a tokenizer's `esaxx-rs`.
/// The `cc` crate reads `CC_<target>` and `CXX_<target>`; without the
/// latter it derives `<gnu>-g++` from the C compiler, which must then be
/// installed. `/gdl/target.env` sets them for the build. Debian names the
/// packages, as the default build image is.
const TOOLCHAIN: &str = "set -eu; mkdir -p /gdl; \\
    case \"$TARGETARCH\" in \\
      amd64) target=x86_64-unknown-linux-gnu; gnu=x86_64-linux-gnu; packages=\"gcc-x86-64-linux-gnu g++-x86-64-linux-gnu libc6-dev-amd64-cross\"; linker=CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER; cc=CC_x86_64_unknown_linux_gnu; cxx=CXX_x86_64_unknown_linux_gnu ;; \\
      arm64) target=aarch64-unknown-linux-gnu; gnu=aarch64-linux-gnu; packages=\"gcc-aarch64-linux-gnu g++-aarch64-linux-gnu libc6-dev-arm64-cross\"; linker=CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER; cc=CC_aarch64_unknown_linux_gnu; cxx=CXX_aarch64_unknown_linux_gnu ;; \\
      *) echo \"no Rust target for architecture $TARGETARCH\" >&2; exit 1 ;; \\
    esac; \\
    echo \"export GDL_TARGET=$target\" > /gdl/target.env; \\
    if [ \"$(rustc -vV | sed -n 's/^host: //p')\" != \"$target\" ]; then \\
      apt-get update && apt-get install -y --no-install-recommends $packages && rm -rf /var/lib/apt/lists/*; \\
      rustup target add \"$target\"; \\
      echo \"export $linker=$gnu-gcc $cc=$gnu-gcc $cxx=$gnu-g++\" >> /gdl/target.env; \\
    fi";

/// Where rustls' native roots read the CA bundle on Debian, and where the
/// final stage puts the build stage's copy. See
/// `docs/src/guides/kubernetes.md` (Images).
const CA_BUNDLE: &str = "/etc/ssl/certs/ca-certificates.crt";

/// What the build stage runs so that it has a [`CA_BUNDLE`] to copy: nothing
/// when its image has one, as the official Rust images do, else Debian's
/// package, and a rebuild of the bundle from it, since an image can hold the
/// package and lack the generated bundle. An image with neither the bundle nor
/// `apt-get` ends the build with one line that says so.
const CA_CERTIFICATES: &str = "[ -s /etc/ssl/certs/ca-certificates.crt ] || { \\
    command -v apt-get >/dev/null || { echo \"the build image has no /etc/ssl/certs/ca-certificates.crt and no apt-get to install ca-certificates\" >&2; exit 1; }; \\
    apt-get update && apt-get install -y --no-install-recommends ca-certificates && update-ca-certificates && rm -rf /var/lib/apt/lists/*; }";

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
        let text = dockerfile("builder", "base", true, "true", "");
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

    /// A slim base has no root CA certificates, and the final stage runs
    /// nothing to install them, so it copies the build stage's bundle to the
    /// path rustls' native roots read on Debian, and the build stage makes
    /// sure it has one.
    #[test]
    fn the_final_stage_copies_the_build_stages_ca_bundle() {
        let text = dockerfile("builder", "debian:bookworm-slim", true, "true", "");
        let (build, last) = text
            .split_once("FROM debian:bookworm-slim\n")
            .expect("a final stage");
        let copy = "COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt\n";
        assert!(last.contains(copy), "{last}");
        assert!(!last.contains("RUN"), "{last}");
        assert!(
            build.contains(&format!("RUN {CA_CERTIFICATES}\n")),
            "{build}"
        );
    }

    /// The build stage installs `ca-certificates` only when its image has
    /// no bundle: a build image of the user's may be any Debian-family one.
    #[cfg(unix)]
    #[test]
    fn the_build_stage_installs_ca_certificates_only_without_a_bundle() {
        for (present, installs) in [(false, true), (true, false)] {
            let dir = tempfile::Builder::new()
                .prefix("gdl-cacerts-")
                .tempdir()
                .expect("a directory");
            let bin = dir.path().join("bin");
            std::fs::create_dir(&bin).expect("a directory");
            let log = dir.path().join("apt.log");
            let bundle = dir.path().join("ca-certificates.crt");
            // `apt-get` leaves a bundle that is not there as it was, as it
            // does when the package is installed already; only
            // `update-ca-certificates` writes one.
            for (name, body) in [
                ("apt-get", "echo \"$@\" >> \"$LOG\""),
                (
                    "update-ca-certificates",
                    "echo update >> \"$LOG\"; echo bundle > \"$BUNDLE\"",
                ),
            ] {
                let path = bin.join(name);
                std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("a stand-in");
                std::fs::set_permissions(
                    &path,
                    std::os::unix::fs::PermissionsExt::from_mode(0o755),
                )
                .expect("executable");
            }
            if present {
                std::fs::write(&bundle, "-----BEGIN CERTIFICATE-----\n").expect("a bundle");
            }
            let script = CA_CERTIFICATES
                .replace("\\\n", "\n")
                .replace(CA_BUNDLE, "\"$BUNDLE\"")
                .replace("rm -rf /var/lib/apt/lists/*", "true");
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg(&script)
                .env("LOG", &log)
                .env("BUNDLE", &bundle)
                .env(
                    "PATH",
                    format!(
                        "{}:{}",
                        bin.display(),
                        std::env::var("PATH").unwrap_or_default()
                    ),
                )
                .output()
                .expect("sh runs");
            assert!(output.status.success(), "{present}: {output:?}");
            let ran = std::fs::read_to_string(&log).unwrap_or_default();
            assert_eq!(
                ran.contains("install -y --no-install-recommends ca-certificates"),
                installs,
                "bundle present {present}: {ran}"
            );
            assert!(bundle.is_file(), "a bundle after the step: {ran}");
        }
    }

    /// A build image with neither a bundle nor `apt-get` ends the build with
    /// one line that names the missing bundle, not apt's error.
    #[cfg(unix)]
    #[test]
    fn a_build_image_with_no_bundle_and_no_apt_get_is_refused_by_name() {
        let dir = tempfile::Builder::new()
            .prefix("gdl-cacerts-")
            .tempdir()
            .expect("a directory");
        let script = CA_CERTIFICATES
            .replace("\\\n", "\n")
            .replace(CA_BUNDLE, "\"$BUNDLE\"");
        let output = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(&script)
            .env("BUNDLE", dir.path().join("ca-certificates.crt"))
            // A path with no `apt-get`: only the shell's own commands run.
            .env("PATH", dir.path())
            .output()
            .expect("sh runs");
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(stderr.lines().count(), 1, "{stderr}");
        assert!(
            stderr.contains("ca-certificates.crt") && stderr.contains("apt-get"),
            "{stderr}"
        );
    }

    /// The build stage's checks and flags of a `[deployment.codegen.arm64]`
    /// table, run with a stand-in `rustc` that knows features `lse` and
    /// `lse2`, and an LLVM-only feature. A build for another architecture,
    /// a build image that sets `RUSTFLAGS`, and a name rustc does not know
    /// each end the build with a line that names the segment and the table.
    /// A good build sets thin LTO and exactly the table's flags.
    #[cfg(unix)]
    #[test]
    fn the_codegen_guard_refuses_by_name_and_sets_its_flags() {
        let dir = tempfile::Builder::new()
            .prefix("gdl-codegen-")
            .tempdir()
            .expect("a directory");
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).expect("a directory");
        let rustc = bin.join("rustc");
        std::fs::write(
            &rustc,
            "#!/bin/sh\n\
             case \"$2\" in\n\
             target-features) printf 'Features supported by rustc for this target:\\n    lse                 - LSE.\\n    lse2                - LSE2.\\n\\nCode-generation features supported by LLVM for this target:\\n    llvmonly            - LLVM.\\n' ;;\n\
             esac\n",
        )
        .expect("a stand-in");
        std::fs::set_permissions(&rustc, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .expect("executable");
        let path = format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let run = |table: &str, env: &[(&str, &str)]| {
            let codegen: super::super::Codegen = toml::from_str(table).expect("a table");
            let script = format!(
                "{}printf '%s %s' \"$CARGO_PROFILE_RELEASE_LTO\" \"$RUSTFLAGS\"",
                codegen_guard("engine", "arm64", &codegen)
            );
            let mut command = std::process::Command::new("sh");
            command
                .arg("-c")
                .arg(&script)
                .env_clear()
                .env("PATH", &path)
                .env("GDL_TARGET", "aarch64-unknown-linux-gnu")
                .env("TARGETARCH", "arm64");
            for (key, value) in env {
                command.env(key, value);
            }
            let output = command.output().expect("sh runs");
            (
                output.status.success(),
                String::from_utf8_lossy(&output.stdout).into_owned(),
                String::from_utf8_lossy(&output.stderr).into_owned(),
            )
        };
        let good = "target_features = [\"+lse\"]\n";
        assert_eq!(
            run(good, &[]),
            (true, "thin -Ctarget-feature=+lse".into(), String::new())
        );
        assert_eq!(
            run("target_features = [\"+lse\", \"-lse2\"]\n", &[]),
            (
                true,
                "thin -Ctarget-feature=+lse,-lse2".into(),
                String::new()
            )
        );
        for (table, env, line) in [
            (
                good,
                &[("TARGETARCH", "amd64")][..],
                "segment engine: this build applies [deployment.codegen.arm64], but its image is for amd64",
            ),
            (
                good,
                &[("RUSTFLAGS", "--cfg rustix_use_libc")][..],
                "segment engine: the build image sets RUSTFLAGS=--cfg rustix_use_libc, which this build step cannot read safely: an image with [deployment.codegen.arm64] builds with exactly the table's flags",
            ),
            (
                "target_features = [\"+lse128\"]\n",
                &[][..],
                "segment engine: [deployment.codegen.arm64] names target feature lse128, which the build image's rustc does not know for aarch64-unknown-linux-gnu",
            ),
            (
                "target_features = [\"+ls\"]\n",
                &[][..],
                "segment engine: [deployment.codegen.arm64] names target feature ls, which the build image's rustc does not know for aarch64-unknown-linux-gnu",
            ),
            (
                "target_features = [\"+llvmonly\"]\n",
                &[][..],
                "segment engine: [deployment.codegen.arm64] names target feature llvmonly, which the build image's rustc does not know for aarch64-unknown-linux-gnu",
            ),
        ] {
            assert_eq!(
                run(table, env),
                (false, String::new(), format!("{line}\n")),
                "{table} {env:?}"
            );
        }
    }

    /// The build stage's toolchain script, run on a host of another
    /// architecture than the image's, with stand-ins for `rustc`, `apt-get`,
    /// and `rustup`: it installs a C and a C++ cross compiler for the
    /// image's architecture, with its C library, and names both to cargo
    /// and to the `cc` crate, so that a crate that builds C++, such as
    /// `esaxx-rs`, finds `<gnu>-g++` rather than the host's compiler.
    #[cfg(unix)]
    #[test]
    fn a_cross_build_installs_and_names_a_c_and_a_cpp_compiler() {
        for (arch, host, gnu, var) in [
            (
                "amd64",
                "aarch64-unknown-linux-gnu",
                "x86_64-linux-gnu",
                "x86_64_unknown_linux_gnu",
            ),
            (
                "arm64",
                "x86_64-unknown-linux-gnu",
                "aarch64-linux-gnu",
                "aarch64_unknown_linux_gnu",
            ),
        ] {
            let dir = tempfile::Builder::new()
                .prefix("gdl-toolchain-")
                .tempdir()
                .expect("a directory");
            let bin = dir.path().join("bin");
            std::fs::create_dir(&bin).expect("a directory");
            let log = dir.path().join("installed");
            for (name, body) in [
                ("rustc", format!("echo \"host: {host}\"")),
                ("apt-get", format!("echo \"$@\" >> '{}'", log.display())),
                ("rustup", "true".to_string()),
            ] {
                let path = bin.join(name);
                std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("a stand-in");
                std::fs::set_permissions(
                    &path,
                    std::os::unix::fs::PermissionsExt::from_mode(0o755),
                )
                .expect("executable");
            }
            let gdl = dir.path().join("gdl");
            let script = TOOLCHAIN
                .replace("\\\n", "\n")
                .replace("/gdl", &gdl.display().to_string())
                .replace("rm -rf /var/lib/apt/lists/*", "true");
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg(&script)
                .env("TARGETARCH", arch)
                .env(
                    "PATH",
                    format!(
                        "{}:{}",
                        bin.display(),
                        std::env::var("PATH").unwrap_or_default()
                    ),
                )
                .output()
                .expect("sh runs");
            assert!(output.status.success(), "{arch}: {output:?}");
            let installed = std::fs::read_to_string(&log).expect("apt-get ran");
            for package in [
                format!("gcc-{}", gnu.replace('_', "-")),
                format!("g++-{}", gnu.replace('_', "-")),
            ] {
                assert!(
                    installed.contains(&package),
                    "{arch}: {package} in {installed}"
                );
            }
            let env = std::fs::read_to_string(gdl.join("target.env")).expect("target.env");
            for set in [
                format!("CC_{var}={gnu}-gcc"),
                format!("CXX_{var}={gnu}-g++"),
                format!("CARGO_TARGET_{}_LINKER={gnu}-gcc", var.to_uppercase()),
            ] {
                assert!(env.contains(&set), "{arch}: {set} in {env}");
            }
        }
    }
}
