use gdl_model::Fleet;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Settings {
    base_image: String,
    build_image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    hub_image: Option<String>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    segment: std::collections::BTreeMap<String, Image>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    secrets: std::collections::BTreeMap<String, Secret>,
}
/// `[deployment] base_image` when the spec does not set it: the image a
/// segment, and the hub unless `hub_image` says otherwise, runs in. It
/// holds glibc and the C runtime and no shell, and runs as a user that is
/// not root.
pub(crate) const DEFAULT_BASE_IMAGE: &str = "gcr.io/distroless/cc-debian12:nonroot";

/// `[deployment] build_image` when the spec does not set it: the image
/// every binary is built in. It is the toolchain `rust-toolchain.toml`
/// pins, on Debian 12, the release the default base image is built from,
/// so that a binary finds the glibc it was linked against.
pub(crate) const DEFAULT_BUILD_IMAGE: &str = "rust:1.93.1-slim-bookworm";

impl Settings {
    /// The segments that `[deployment.segment.<name>]` tables name.
    pub(crate) fn segments(&self) -> impl Iterator<Item = &str> {
        self.segment.keys().map(String::as_str)
    }

    /// What is wrong with the secrets the settings declare and the
    /// segments name, each with its table.
    pub(crate) fn secret_problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        for (name, secret) in &self.secrets {
            if !secret_name(name) {
                problems.push(format!(
                    "[deployment.secrets.{name}]: a secret's name is letters, digits, `_`, `.`, and `-`, and not `.` or `..`, so that it names one file in `/run/secrets`"
                ));
            }
            if !Path::new(&secret.file).is_absolute() {
                problems.push(format!(
                    "[deployment.secrets.{name}]: file `{}` is not an absolute path: Compose reads a relative one from the bundle, which every image build copies",
                    secret.file
                ));
            }
        }
        for (segment, image) in &self.segment {
            let mut named = std::collections::BTreeSet::new();
            for name in &image.secrets {
                if !self.secrets.contains_key(name) {
                    problems.push(format!(
                        "[deployment.segment.{segment}]: secret `{name}` is not declared in a `[deployment.secrets.{name}]` table"
                    ));
                }
                if !named.insert(name) {
                    problems.push(format!(
                        "[deployment.segment.{segment}]: secret `{name}` is named twice"
                    ));
                }
            }
        }
        problems
    }
}

/// Whether `name` can name a secret: Compose mounts it at
/// `/run/secrets/<name>`, which must be one file there.
fn secret_name(name: &str) -> bool {
    !matches!(name, "" | "." | "..")
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            base_image: DEFAULT_BASE_IMAGE.into(),
            build_image: DEFAULT_BUILD_IMAGE.into(),
            hub_image: None,
            segment: Default::default(),
            secrets: Default::default(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Image {
    #[serde(skip_serializing_if = "Option::is_none")]
    base_image: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    ports: Vec<String>,
    #[serde(default, skip_serializing_if = "toml::Table::is_empty")]
    config: toml::Table,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    secrets: Vec<String>,
}

/// A secret a segment can mount: a file on the Docker host, which the
/// generator names and never opens.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Secret {
    file: String,
}
fn validate_image(image: &str) -> Result<(), Error> {
    if image.is_empty()
        || image.starts_with('-')
        || !image
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/_.:@-".contains(&b))
    {
        return Err(Error::new(format!(
            "invalid container image reference `{image}`"
        )));
    }
    Ok(())
}
use crate::paths::display;
use crate::{
    Error, Files, Offers, Spec, emit,
    spec::{CrateSource, GdlDep, Source},
};

mod cells;
pub(crate) mod context;
pub(crate) mod locks;
mod variables;

/// Where a bundle's images take the crates they build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sources {
    /// Cargo fetches every crate from Git or crates.io in the image build,
    /// so the bundle builds from its own files, on any Docker host. The
    /// composer's only mode.
    Fetched,
    /// The images build the crates the spec names by path, and the gdl
    /// runtime and hub of its `[gdl] path`, as they are on this machine,
    /// from a build context that holds them and the bundle. For testing
    /// code that is not pushed: the bundle builds only on this machine.
    /// [`compose`] writes each segment's lock on this machine, and each
    /// image builds by it.
    Local,
}

/// Render a Docker Compose deployment, its images built from `sources`.
/// Without `fleet`, it deploys the spec's one placement with one hub. With
/// it, it deploys by cell: one hub per cell, and each cell's segments of
/// its class in its layout ([`cells`]). A spec with classes or several
/// layouts deploys only by cell, and a fleet is checked against the spec
/// first; a fleet for a spec without classes can have no cell, and the
/// spec deploys as without one.
pub fn compose_files(
    spec: &Spec,
    fleet: Option<&Fleet>,
    out: &Path,
    target: &Path,
    sources: Sources,
) -> Result<Files, Error> {
    Ok(bundle_files(spec, fleet, out, target, sources)?.0)
}

/// [`compose_files`], and the build context of a bundle of local sources.
fn bundle_files(
    spec: &Spec,
    fleet: Option<&Fleet>,
    out: &Path,
    target: &Path,
    sources: Sources,
) -> Result<(Files, Option<context::Context>), Error> {
    let by_cell = cells::by_cell(spec);
    if let (None, Some(why)) = (fleet, &by_cell) {
        return Err(Error::new(format!(
            "`compose` deploys a spec {why} by cell, as its fleet file says: give it `--fleet <fleet.toml>`"
        )));
    }
    let (spec, bundle) = Bundle::prepare(spec, out, target, sources)?;
    bundle.check_secrets(out)?;
    let local = bundle.context.clone();
    let mut files = match fleet {
        None => single(&spec, bundle, target)?,
        Some(fleet) => {
            let (checked, layouts) = crate::checked(&spec, target, Offers::Read);
            let checked = checked?;
            fleet
                .check(Some(&layouts))
                .map_err(|errors| Error::from(gdl_model::Problems::from(errors)))?;
            if by_cell.is_none() {
                single(&spec, bundle, target)?
            } else {
                let stdin = stdin_source(&spec, &checked.manifest)?;
                cells::bundle(&spec, fleet, bundle, &checked.manifest, &layouts, stdin)?
            }
        }
    };
    if local.is_some() {
        mark_local(&mut files)?;
    }
    if let Some(compose) = files.get_mut(Path::new("compose.yaml")) {
        *compose = yaml_safe(compose);
    }
    Ok((files, local))
}

/// `compose.yaml`, which is JSON, with each character that Compose's YAML
/// parser refuses or reads as a line break written as a `\u` escape, which
/// JSON and YAML read alike: the C1 controls, U+2028, U+2029, the byte
/// order mark, U+FFFE, and U+FFFF. Only strings, such as a path, can hold
/// them.
fn yaml_safe(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    for c in json.chars() {
        if matches!(
            c,
            '\u{80}'..='\u{9f}' | '\u{2028}' | '\u{2029}' | '\u{feff}' | '\u{fffe}' | '\u{ffff}'
        ) {
            out.push_str(&format!("\\u{:04x}", u32::from(c)));
        } else {
            out.push(c);
        }
    }
    out
}

/// Say, in the bundle's `compose.yaml` and its README, that its images
/// build local sources, and so build only on this machine. `compose.yaml`
/// is JSON, which has no comments, so it says so in an extension field,
/// which Compose ignores.
fn mark_local(files: &mut Files) -> Result<(), Error> {
    const NOTE: &str = "This bundle builds local sources: its images build from a context outside the bundle, which holds every crate it builds from by path. It builds only on the machine that wrote it, and every change to those crates rebuilds its images.";
    let path = Path::new("compose.yaml");
    let mut compose: serde_json::Value =
        serde_json::from_str(files.get(path).map_or("{}", String::as_str)).map_err(io)?;
    if let Some(compose) = compose.as_object_mut() {
        compose.insert("x-gdl-local-sources".into(), NOTE.into());
    }
    files.insert(
        path.into(),
        serde_json::to_string_pretty(&compose).map_err(io)?,
    );
    if let Some(readme) = files.get_mut(Path::new("README.md")) {
        readme.push_str(&format!("\n{NOTE}\n"));
    }
    Ok(())
}

/// What every bundle of a spec starts from: its deployment settings, how
/// its hub is installed, its generated crates, its graph's name, and the
/// build context of a bundle that builds local sources.
struct Bundle {
    settings: Settings,
    hub_install: String,
    files: Files,
    graph: String,
    context: Option<context::Context>,
}

impl Bundle {
    /// `spec` as a bundle deploys it, and what its bundle starts from:
    /// refused unless every crate it builds can be fetched, or, with local
    /// sources, unless a build context holds them and the bundle in `out`.
    fn prepare(
        spec: &Spec,
        out: &Path,
        target: &Path,
        sources: Sources,
    ) -> Result<(Spec, Self), Error> {
        // A deployment discovers its segments through the hub, whether or
        // not the spec says anything else about it.
        let mut spec = spec.clone();
        let settings = spec.deployment.get_or_insert_with(Default::default).clone();
        let Source::Graph(source) = &spec.source else {
            return Err(Error::new(
                "Deploy requires a graph definition with Git or crates.io packages, not a local contract crate",
            ));
        };
        let local: Vec<_> = source
            .crates
            .iter()
            .chain(&source.nodes)
            .filter(|listed| matches!(listed.source, CrateSource::Path(_)))
            .map(|listed| listed.package.as_str())
            .chain(
                spec.bind
                    .values()
                    .chain(spec.cell_policies.values())
                    .filter(|binding| matches!(binding.source, CrateSource::Path(_)))
                    .map(|binding| binding.package.as_str()),
            )
            .collect();
        if !local.is_empty() && sources == Sources::Fetched {
            return Err(Error::new(format!(
                "Deploy requires Git or crates.io node, message, and cell policy packages. Re-import these local packages from Git or crates.io: {}",
                local.join(", ")
            )));
        }
        // Refused before the render, which fetches the spec's crates: the
        // refusal needs only the spec, and a fetch can fail first and hide it.
        let runtime = match (sources, &spec.gdl) {
            (Sources::Local, GdlDep::Path(runtime)) => Some(runtime),
            (Sources::Local, _) => {
                return Err(Error::new(
                    "`--local` builds gdl and its hub from the sources [gdl] path names; this spec names gdl by Git or version, so build it without `--local`",
                ));
            }
            (Sources::Fetched, _) => None,
        };
        let deps = match sources {
            Sources::Fetched => emit::Deps::Exported,
            Sources::Local => emit::Deps::Local,
        };
        let (files, graph) = crate::render_with(&spec, out, target, deps, Offers::Read)?;
        // The manifests name their crates from `out` resolved, so the
        // context must be found from it too.
        let context = match runtime {
            Some(runtime) => Some(context::Context::find(
                &crate::dump::absolute(out)?,
                &files,
                Some(runtime),
                context::Mode::Image,
            )?),
            None => None,
        };
        let hub_install = match (&spec.gdl, &context) {
            (_, Some(context)) => format!("--path {}", quote(&format!("/app/{}", context.hub()))),
            (GdlDep::Git { url, rev }, None) => {
                format!("--git {} --rev {}", quote(url), quote(rev))
            }
            (GdlDep::Version(version), None) => format!("--version {}", quote(version)),
            (GdlDep::Path(_), None) => {
                return Err(Error::new(
                    "Deploy requires GDL from Git or crates.io; start the server with --gdl-git and --gdl-rev",
                ));
            }
        };
        let bundle = Self {
            settings,
            hub_install,
            files,
            graph,
            context,
        };
        Ok((spec, bundle))
    }

    /// The `build` of a service whose image the Dockerfile at `dockerfile`,
    /// a path in the bundle, builds; and that Dockerfile's ignore file, for
    /// a bundle of local sources.
    fn build(&mut self, dockerfile: &Path) -> serde_json::Value {
        let Some(context) = &self.context else {
            return json!({"context": ".", "dockerfile": display(dockerfile)});
        };
        let mut ignore = dockerfile.as_os_str().to_owned();
        ignore.push(".dockerignore");
        self.files.insert(ignore.into(), context.ignore());
        // Compose reads `$` as a variable, and `$$` as `$`. [`compose`]
        // refuses the `${` that no escape keeps.
        let dockerfile = context.in_bundle(dockerfile).replace('$', "$$");
        json!({"context": context.seen_from_bundle(), "dockerfile": dockerfile})
    }

    /// `path`, a path in the bundle, as the image build sees it under
    /// `/app`.
    fn in_build(&self, path: &Path) -> String {
        match &self.context {
            Some(context) => context.in_bundle(path),
            None => display(path),
        }
    }

    /// Every image the settings name, checked.
    fn validate_images(&self) -> Result<(), Error> {
        let settings = &self.settings;
        for image in [&settings.base_image, &settings.build_image]
            .into_iter()
            .chain(settings.hub_image.iter())
            .chain(
                settings
                    .segment
                    .values()
                    .filter_map(|s| s.base_image.as_ref()),
            )
        {
            validate_image(image)?;
        }
        Ok(())
    }

    /// The Dockerfile of a hub that admits the deployment in
    /// `deployment`, a path in the bundle, with an empty directory at
    /// `state_dir` that any user can write, if it names one.
    fn hub_dockerfile(&self, deployment: &Path, state_dir: Option<&str>) -> String {
        let settings = &self.settings;
        let install = format!(
            "cargo install {} --locked --root /build-tools gdl-hub",
            self.hub_install
        );
        let Some(state_dir) = state_dir else {
            return dockerfile(
                &install,
                "/build-tools/bin/gdl-hub",
                &self.in_build(deployment),
                &settings.build_image,
                settings.hub_image.as_ref().unwrap_or(&settings.base_image),
            );
        };
        // A new named volume takes the owner and mode of the directory it
        // is mounted on. The runtime image's user is not known here, and
        // the default one is not root, so the directory is writable by
        // all, as `/tmp` is. COPY keeps the mode of a directory only
        // inside what it copies, so the directory is copied with its
        // parent's contents.
        let (parent, name) = state_dir
            .rsplit_once('/')
            .expect("the state directory is an absolute path");
        let staged = format!("/build-tools/state/{name}");
        let mut text = dockerfile(
            &format!("{install} && mkdir -p {staged} && chmod 1777 {staged}"),
            "/build-tools/bin/gdl-hub",
            &self.in_build(deployment),
            &settings.build_image,
            settings.hub_image.as_ref().unwrap_or(&settings.base_image),
        );
        text.push_str(&format!(
            "COPY --from=build /build-tools/state/ {parent}/\n"
        ));
        text
    }

    /// Segment `segment`'s image, from its crate in `dir`, which builds
    /// package `package`: its Dockerfile and its settings, with `[hub]
    /// host` when `hub` names it, and no wait for the instances of segment
    /// `unawaited` when it names one. Returns how long Docker must let a
    /// container of it stop, in seconds.
    fn segment_image(
        &mut self,
        dir: &Path,
        segment: &str,
        package: &str,
        hub: Option<&str>,
        unawaited: Option<&str>,
    ) -> Result<u64, Error> {
        let settings = &self.settings;
        let crate_dir = dir.join(segment);
        let path = crate_dir.join("config.toml");
        let manifest = format!("/app/{}/Cargo.toml", self.in_build(&crate_dir));
        // A segment of local sources builds by the lock [`locks::write`]
        // writes on the host, which records its patch: a resolve in the
        // image build would fetch the patched source, with no SSH agent,
        // or move a Git dependency to its newest commit.
        let locked = match &self.context {
            Some(context) => {
                let patch =
                    context.patch(&self.files, std::slice::from_ref(&crate_dir), &crate_dir)?;
                if let Some(manifest) = self.files.get_mut(&crate_dir.join("Cargo.toml")) {
                    manifest.push_str(&patch);
                    // The host's Rust may be newer than the image's: Cargo
                    // picks each new dependency for the oldest Rust the
                    // segment says it builds with, which is gdl's.
                    if let Some(version) = context.rust_version() {
                        *manifest = manifest.replacen(
                            "publish = false\n",
                            &format!(
                                "publish = false\nrust-version = {}\n",
                                crate::dump::toml_string(version)
                            ),
                            1,
                        );
                    }
                }
                "--locked "
            }
            None => "",
        };
        let build = format!(
            "cargo build {locked}--release --manifest-path {}",
            quote(&manifest)
        );
        let text = dockerfile(
            &build,
            &format!("/build/release/{package}"),
            &self.in_build(&path),
            &settings.build_image,
            settings
                .segment
                .get(segment)
                .and_then(|s| s.base_image.as_ref())
                .unwrap_or(&settings.base_image),
        );
        let files = &mut self.files;
        files.insert(crate_dir.join("Dockerfile"), text);
        let hub = hub.map_or(String::new(), |host| {
            format!("[hub]\nhost = \"{host}\"\n\n")
        });
        // The source that reads stdin runs once per `docker compose run`,
        // so the others start without it; a launch that waited for it
        // would give up after `rendezvous_ms`.
        let unawaited = unawaited.map_or(String::new(), |source| {
            format!("\n[min_replicas]\n{source} = 0\n")
        });
        files.insert(
            path.clone(),
            format!(
                "# {}\nlisten = \"0.0.0.0:9000\"\nrendezvous_ms = 60000\n\n{hub}[system]\nport = 9090\n{unawaited}",
                emit::MARKER
            ),
        );
        if let Some(image) = settings.segment.get(segment) {
            let mut config: toml::Table = toml::from_str(&files[&path]).map_err(io)?;
            for (key, value) in &image.config {
                // Where a segment finds its peers is the bundle's, and its
                // cell the fleet's.
                if ["hub", "roster", "instances", "cell"].contains(&key.as_str()) {
                    return Err(Error::new(format!("deployment cannot override `{key}`")));
                }
                config.insert(key.clone(), value.clone());
            }
            files.insert(path.clone(), toml::to_string_pretty(&config).map_err(io)?);
        }
        // Docker kills a container 10 s after it stops it unless told otherwise; a segment may
        // drain for its bound, read here from its settings as generated (an environment
        // override at run time is not seen).
        let config: toml::Table = toml::from_str(&files[&path]).map_err(io)?;
        Ok(drain_bound(&config).as_secs() + 30)
    }

    /// Refuse a secret whose file lies in the build context, the bundle or
    /// the context of its local sources: every image build would copy it
    /// into an image.
    fn check_secrets(&self, out: &Path) -> Result<(), Error> {
        let context = match &self.context {
            Some(context) => context.root().to_path_buf(),
            None => crate::dump::absolute(out)?,
        };
        for (name, secret) in &self.settings.secrets {
            let file = crate::paths::resolve(Path::new(&secret.file)).map_err(io)?;
            if file.starts_with(&context) {
                return Err(Error::new(format!(
                    "[deployment.secrets.{name}]: file `{}` lies in the build context {}, which every image build copies: keep the file outside it",
                    secret.file,
                    display(&context)
                )));
            }
        }
        Ok(())
    }

    /// Mount the secrets segment `segment` names in `service`, and note in
    /// `used` that the segment mounts them.
    fn mount_secrets(&self, segment: &str, service: &mut serde_json::Value, used: &mut Secrets) {
        let names = self
            .settings
            .segment
            .get(segment)
            .map_or(&[][..], |s| s.secrets.as_slice());
        if names.is_empty() {
            return;
        }
        service["secrets"] = json!(names);
        for name in names {
            used.entry(name.clone())
                .or_default()
                .insert(segment.to_string());
        }
    }

    /// The top-level `secrets` of a `compose.yaml` whose services mount
    /// those in `used`: each from its file, with `$`, which Compose reads
    /// as a variable, written `$$`.
    fn declare_secrets(&self, used: &Secrets) -> serde_json::Value {
        used.keys()
            .map(|name| {
                let file = self.settings.secrets[name].file.replace('$', "$$");
                (name.clone(), json!({"file": file}))
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    /// What a bundle's README says about the secrets in `used`.
    fn secrets_readme(&self, used: &Secrets) -> String {
        if used.is_empty() {
            return String::new();
        }
        let mut text = String::from("\n## Secrets\n\n");
        for (name, segments) in used {
            let segments: Vec<String> = segments.iter().map(|s| format!("`{s}`")).collect();
            text.push_str(&format!(
                "- `{name}`, from `{}` on the Docker host, at `/run/secrets/{name}` in segment {}.\n",
                self.settings.secrets[name].file,
                segments.join(", ")
            ));
        }
        text.push_str("\nThe bundle names each file and holds none of them. Each file must exist on the Docker host before `docker compose up`. Compose mounts it with its owner and mode on the host, and ignores `uid` and `mode` outside Swarm, so the user a container runs as must be able to read it. The default image runs as uid 65532: let that user read a file only you can read with `setfacl -m u:65532:r <file>`.\n");
        text
    }

    /// The ports segment `segment` publishes.
    fn ports(&self, segment: &str) -> &[String] {
        self.settings
            .segment
            .get(segment)
            .map_or(&[], |s| s.ports.as_slice())
    }
}

/// The secrets a bundle's services mount: by name, the segments that mount
/// each.
type Secrets = std::collections::BTreeMap<String, std::collections::BTreeSet<String>>;

/// The bundle of a spec's one placement: one hub, and each segment.
fn single(spec: &Spec, mut bundle: Bundle, target: &Path) -> Result<Files, Error> {
    // A bundle deploys one placement, and its hub admits only that one.
    let laid = spec.only_layout("`compose`")?;
    let placement = laid.placement;
    let checked = crate::check(spec, target, Offers::Read)?;
    let stdin = stdin_source(spec, &checked.manifest)?;
    let deployment = gdl_model::DeploymentSpec::new(checked.manifest, placement.clone())
        .map_err(|e| Error::new(e.to_string()))?;
    bundle.files.insert(
        "deployment.json".into(),
        serde_json::to_string_pretty(&deployment).map_err(|e| Error::new(e.to_string()))?,
    );
    bundle.validate_images()?;
    let mut services = serde_json::Map::new();
    let build = bundle.build(Path::new("hub/Dockerfile"));
    services.insert(
        "hub".into(),
        json!({"build":build,"healthcheck":hub_healthcheck()}),
    );
    let hub = bundle.hub_dockerfile(Path::new("deployment.json"), None);
    bundle.files.insert("hub/Dockerfile".into(), hub);
    let source = placement
        .segments()
        .find(|(_, nodes)| hosts(nodes, stdin.as_ref()))
        .map(|(segment, _)| segment);
    let mut reads_stdin = None;
    let mut secrets = Secrets::new();
    for (segment, nodes) in placement.segments() {
        check_segment_name(segment)?;
        let package = emit::package_name(&bundle.graph, segment);
        let grace = bundle.segment_image(Path::new(""), segment, &package, Some("hub"), source)?;
        // Health is reported, never waited on: no segment depends on another's, since segments
        // whose paths return wait for each other, and an order over them deadlocks. The start
        // period covers loading models.
        let build = bundle.build(&Path::new(segment).join("Dockerfile"));
        let mut service = json!({"build":build,"depends_on":{"hub":{"condition":"service_healthy"}},"init":true,"stop_grace_period":format!("{grace}s"),"healthcheck":segment_healthcheck(),"ports":bundle.ports(segment)});
        if hosts(nodes, stdin.as_ref()) {
            keep_stdin_open(&mut service);
            reads_stdin = Some(segment);
        }
        bundle.mount_secrets(segment, &mut service, &mut secrets);
        services.insert(segment.into(), service);
    }
    let mut compose = json!({"services":services});
    if !secrets.is_empty() {
        compose["secrets"] = bundle.declare_secrets(&secrets);
    }
    let secrets_readme = bundle.secrets_readme(&secrets);
    let mut files = bundle.files;
    files.insert(
        "compose.yaml".into(),
        serde_json::to_string_pretty(&compose).map_err(|e| Error::new(e.to_string()))?,
    );
    let mut readme = String::from(
        "# Docker Compose deployment\n\nRun `docker compose up --build` from this directory. The hub starts first; every segment registers its compiled contract and waits for all handlers to be ready.\n\nConfigure images, runtime settings, and published ports in the web composer Deploy tab. Review the generated files, then download a new bundle after changes. Runtime images must provide the libraries needed by their executables.\n\nEach segment takes its roster from the hub when it starts: the loaded instances of every segment, at least `min_replicas` of each (one by default). An instance that starts later joins: once it is ready, the segments already running route to it, so `docker compose up --scale <segment>=N` adds instances. Scaling works only for a segment that publishes no host port: two containers cannot bind one port. An instance that is stopped retires: the others stop routing to it, and it leaves once it has drained. Kubernetes and srt-slurm are not generated.\n",
    );
    if let Some(source) = reads_stdin {
        readme.push_str(&stdin_readme(source));
    }
    readme.push_str(&secrets_readme);
    files.insert("README.md".into(), readme);
    Ok(files)
}

/// The source whose segment reads JSON requests on stdin, or none when a
/// frontend is pinned on a source. A graph spec pins in its `[nodes]`
/// table, a contract spec in `[bind]`. A source with neither is refused:
/// its segment would serve nothing that a container can reach.
fn stdin_source(spec: &Spec, manifest: &gdl_model::Manifest) -> Result<Option<String>, Error> {
    let pinned = |name: &str| {
        spec.bind.contains_key(name)
            || matches!(&spec.source, Source::Graph(g) if g.node_pins.contains_key(name))
    };
    reads_stdin(manifest, pinned)
}

/// [`stdin_source`], with the pins as `pinned` says.
fn reads_stdin(
    manifest: &gdl_model::Manifest,
    pinned: impl Fn(&str) -> bool,
) -> Result<Option<String>, Error> {
    if manifest
        .nodes
        .iter()
        .any(|n| n.role == gdl_model::Role::Source && pinned(&n.name))
    {
        return Ok(None);
    }
    match emit::json_lines_source(manifest) {
        Some(source) => Ok(Some(source.to_string())),
        None => Err(Error::new(
            "Deploy requires a frontend pinned on a source, or a source that reads JSON requests on stdin: the graph's only source, whose request is one value, and whose request and item cross segments",
        )),
    }
}

/// Whether a segment of `nodes` hosts the source that reads stdin.
fn hosts(nodes: &[String], stdin: Option<&String>) -> bool {
    stdin.is_some_and(|source| nodes.contains(source))
}

/// Keep the service's stdin open, so that its source waits for requests
/// under `up` rather than reading the end of its input and stopping.
/// `docker compose run -T` gives it the input instead.
fn keep_stdin_open(service: &mut serde_json::Value) {
    if let Some(service) = service.as_object_mut() {
        service.insert("stdin_open".into(), true.into());
    }
}

/// What a bundle's README says about the source service `service`, which
/// reads JSON requests on stdin.
fn stdin_readme(service: &str) -> String {
    format!(
        "\nService `{service}` reads JSON requests on stdin, one per line, and writes each answer's items as JSON lines. Start the other services, then run it with the requests as its input:\n\n```bash\ndocker compose up --build --detach --scale {service}=0\ndocker compose run --rm -T {service} < requests.jsonl\n```\n\nEach run is one more instance of `{service}`: it joins through the hub, answers, and leaves at the end of its input.\n"
    )
}

/// Whether `segment` can name a segment's directory and service.
fn check_segment_name(segment: &str) -> Result<(), Error> {
    if segment == "hub" {
        return Err(Error::new("segment name `hub` is reserved by Compose"));
    }
    if !segment
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error::new(format!(
            "segment `{segment}` cannot be a Compose service name"
        )));
    }
    Ok(())
}

/// A hub's health check: its own liveness, answered by its binary. Never
/// a cell's health, which a parent cell answers 503 until its children are
/// ready: a wait on it would deadlock a fleet that starts together.
fn hub_healthcheck() -> serde_json::Value {
    json!({"test":["CMD","/usr/local/bin/segment","--healthcheck"],"interval":"2s","timeout":"2s","retries":30})
}

/// A segment's health check, which its binary answers from its settings.
fn segment_healthcheck() -> serde_json::Value {
    json!({"test":["CMD","/usr/local/bin/segment","--healthcheck","/etc/gdl/config"],"interval":"5s","timeout":"3s","retries":3,"start_period":"1800s"})
}

/// How long a segment with these settings may drain: `runtime.drain_ms`,
/// else `runtime.default_deadline_ms`, else the runtime's defaults.
fn drain_bound(config: &toml::Table) -> std::time::Duration {
    let runtime = config.get("runtime").and_then(toml::Value::as_table);
    let ms = |key: &str| {
        runtime
            .and_then(|runtime| runtime.get(key))
            .and_then(toml::Value::as_integer)
            .and_then(|ms| u64::try_from(ms).ok())
    };
    ms("drain_ms").or_else(|| ms("default_deadline_ms")).map_or(
        gdl_model::DEFAULT_DEADLINE,
        std::time::Duration::from_millis,
    )
}

/// A Dockerfile that runs `build` in `builder`, and copies the `binary`
/// it builds and the settings at `config`, a path in the build context,
/// into `base`.
fn dockerfile(build: &str, binary: &str, config: &str, builder: &str, base: &str) -> String {
    // The JSON form keeps a path with spaces, which a local bundle's can
    // hold, one source. Docker still reads the source as a word, where `\`,
    // `$`, and quotes are syntax, and then as a pattern. `./` keeps a path
    // such as `git@host:dir` from reading as a Git source.
    let config: String = context::pattern(&format!("./{config}"))
        .chars()
        .flat_map(|c| match c {
            '\\' | '$' | '"' | '\'' => vec!['\\', c],
            c => vec![c],
        })
        .collect();
    let config = serde_json::Value::from(config);
    format!(
        "# {}\nFROM {builder} AS build\nWORKDIR /app\nCOPY . /app\nENV CARGO_TARGET_DIR=/build\nRUN {build}\nFROM {base}\nCOPY --from=build {binary} /usr/local/bin/segment\nCOPY [{config}, \"/etc/gdl/config\"]\nENTRYPOINT [\"/usr/local/bin/segment\",\"/etc/gdl/config\"]\n",
        emit::MARKER,
    )
}
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Write a Compose bundle into `out`, which must not exist, its images
/// built from `sources`. Returns the root of the build context of a bundle
/// of local sources, which the bundle names only relative to itself.
/// Refuses a directory that Docker Compose cannot load a project from.
pub fn compose(
    spec: &Spec,
    fleet: Option<&Fleet>,
    out: &Path,
    target: &Path,
    sources: Sources,
) -> Result<Option<PathBuf>, Error> {
    // Compose loads the project from the directory as the shell names it,
    // which can be a symlink's name; a later `cd` may take the resolved one.
    loadable(&as_given(out)?)?;
    loadable(&crate::dump::absolute(out)?)?;
    write(spec, fleet, out, target, sources)
}

/// `out` absolute as a shell spells it: a relative `out` from `PWD`, which
/// keeps the symlinks the shell went through, when `PWD` names the
/// working directory. Compose takes its working directory the same way.
fn as_given(out: &Path) -> Result<PathBuf, Error> {
    let here = std::env::current_dir().and_then(|d| d.canonicalize()).ok();
    let absolute = match std::env::var_os("PWD").map(PathBuf::from) {
        Some(pwd) if out.is_relative() && pwd.is_absolute() && pwd.canonicalize().ok() == here => {
            pwd.join(out)
        }
        _ => std::path::absolute(out).map_err(io)?,
    };
    Ok(crate::paths::normalize(&absolute))
}

/// [`compose`], whatever the path.
fn write(
    spec: &Spec,
    fleet: Option<&Fleet>,
    out: &Path,
    target: &Path,
    sources: Sources,
) -> Result<Option<PathBuf>, Error> {
    if out.exists() {
        return Err(Error::new(
            "Compose output already exists; choose a fresh directory",
        ));
    }
    let out = crate::dump::absolute(out)?;
    let (files, context) = bundle_files(spec, fleet, &out, target, sources)?;
    // Claim `out` only now, and atomically: another process may have made
    // it while the bundle was prepared. A failure below then removes only
    // what this run made, so compose writes all of a bundle or none.
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).map_err(io)?;
    }
    match std::fs::create_dir(&out) {
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(Error::new(
                "Compose output already exists; choose a fresh directory",
            ));
        }
        other => other.map_err(io)?,
    }
    let written = crate::write(&out, &files)
        .map(drop)
        .and_then(|()| match &context {
            Some(context) => locks::write(&out, &files, context),
            None => Ok(()),
        });
    if let Err(error) = written {
        let _ = std::fs::remove_dir_all(&out);
        return Err(error);
    }
    Ok(context.map(|context| context.root().to_path_buf()))
}

/// Refuse an output directory, absolute, from which Docker Compose cannot
/// run the bundle with `docker compose up`. `buildx bake`, which Compose v5
/// always builds with, reads the build context's path, which is absolute,
/// as UTF-8 in Unicode normal form C, and changes a path that is not. It
/// also reads `${` and `%{` in that path as templates. Compose makes a
/// `file:` URL of the directory, which a control character or a `%` that
/// does not start an escape breaks; `%{` is such a `%`. And Compose names
/// the project from the directory's name, keeping only `a-z`, `0-9`, `_`,
/// and `-`.
fn loadable(out: &Path) -> Result<(), Error> {
    let refuse = |what: &str| {
        Err(Error::new(format!(
            "Compose output {:?} {what}; choose another directory",
            out.as_os_str()
        )))
    };
    let Some(text) = out.to_str() else {
        return refuse("is not UTF-8, which `buildx bake` replaces in the build context's path");
    };
    if !icu_normalizer::ComposingNormalizerBorrowed::new_nfc().is_normalized(text) {
        return refuse(
            "is not in Unicode normal form C, to which `buildx bake` changes the build context's path, so that it names another directory",
        );
    }
    if text.chars().any(|c| c.is_ascii_control()) {
        return refuse(
            "holds a control character, which Docker Compose cannot load a project from",
        );
    }
    let bytes = text.as_bytes();
    let bare = bytes.iter().enumerate().any(|(i, &b)| {
        b == b'%'
            && !bytes
                .get(i + 1..i + 3)
                .is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit))
    });
    if bare {
        return refuse(
            "holds a `%` that two hex digits do not follow, which Docker Compose cannot load a project from",
        );
    }
    if text.contains("${") {
        return refuse(
            "holds `${`, which `buildx bake` reads as a template in the build context's path, and no escape in `compose.yaml` reads as `${` in each of Compose's builders",
        );
    }
    let name = out
        .file_name()
        .map(|name| name.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    if !name.chars().any(|c| c.is_ascii_alphanumeric()) {
        return refuse(
            "has a name with no ASCII letter or digit, from which Docker Compose derives an empty project name",
        );
    }
    Ok(())
}
fn io(e: impl std::fmt::Display) -> Error {
    Error::new(e.to_string())
}

/// Generated files and their self-contained build archive from one snapshot.
pub struct ComposeBundle {
    /// Generated text files.
    pub files: Files,
    /// Gzip-compressed tar containing these exact files.
    pub archive: Vec<u8>,
}

/// Capture the generated files once for preview and download.
pub fn compose_bundle(spec: &Spec, target: &Path) -> Result<ComposeBundle, Error> {
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-")
        .tempdir()
        .map_err(io)?;
    // Not checked: the bundle runs where its archive is unpacked.
    let out = temp.path().join("deployment");
    write(spec, None, &out, target, Sources::Fetched)?;
    let mut files = Files::new();
    let mut pending = vec![out.clone()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            let path = entry.path();
            let relative = path.strip_prefix(&out).map_err(io)?;
            if entry.file_type().map_err(io)?.is_dir() {
                pending.push(path);
            } else {
                files.insert(
                    relative.to_path_buf(),
                    std::fs::read_to_string(&path).map_err(io)?,
                );
            }
        }
    }
    let gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut archive = tar::Builder::new(gzip);
    archive.append_dir_all("deployment", &out).map_err(io)?;
    let archive = archive.into_inner().map_err(io)?.finish().map_err(io)?;
    Ok(ComposeBundle { files, archive })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gdl_model::{HopInfo, Manifest, NodeInfo, Role, TypeInfo};

    /// A graph of one hop, source `S` to sink `D`, whose request and item
    /// are `request` and `item`.
    fn graph(request: TypeInfo, item: TypeInfo) -> Manifest {
        let node = |name: &str, role| NodeInfo {
            name: name.into(),
            role,
            binding: None,
        };
        Manifest {
            name: "g".into(),
            schema: 1,
            nodes: vec![node("S", Role::Source), node("D", Role::Sink)],
            hops: vec![HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![request],
                backward_types: vec![item],
            }],
        }
    }

    fn wire(wire: bool) -> TypeInfo {
        TypeInfo::new("T", wire, Default::default())
    }

    /// A source reads stdin when its binary can serve it there; a pinned
    /// frontend takes its place; and a source with neither serves nothing a
    /// container can reach, so it is refused.
    #[test]
    fn a_source_is_served_by_a_frontend_or_on_stdin() {
        let served = graph(wire(true), wire(true));
        assert_eq!(reads_stdin(&served, |_| false), Ok(Some("S".into())));
        assert_eq!(reads_stdin(&served, |name| name == "S"), Ok(None));
        let local = graph(wire(false), wire(true));
        let refused = reads_stdin(&local, |_| false).expect_err("no way in");
        assert!(
            refused
                .to_string()
                .contains("a source that reads JSON requests on stdin"),
            "{refused}"
        );
        assert_eq!(reads_stdin(&local, |name| name == "S"), Ok(None));
    }
}
