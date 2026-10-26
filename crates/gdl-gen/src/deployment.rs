use gdl_model::Fleet;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Settings {
    base_image: String,
    build_image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    hub_image: Option<String>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    segment: std::collections::BTreeMap<String, Image>,
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
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            base_image: DEFAULT_BASE_IMAGE.into(),
            build_image: DEFAULT_BUILD_IMAGE.into(),
            hub_image: None,
            segment: Default::default(),
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
use crate::{
    Error, Files, Offers, Spec, emit,
    spec::{CrateSource, GdlDep, Source},
};

mod cells;
mod variables;

/// Render a Docker Compose deployment from Git or registry dependencies.
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
) -> Result<Files, Error> {
    let by_cell = cells::by_cell(spec);
    if let (None, Some(why)) = (fleet, &by_cell) {
        return Err(Error::new(format!(
            "`compose` deploys a spec {why} by cell, as its fleet file says: give it `--fleet <fleet.toml>`"
        )));
    }
    let (spec, bundle) = Bundle::prepare(spec, out, target)?;
    let Some(fleet) = fleet else {
        return single(&spec, bundle, target);
    };
    let (checked, layouts) = crate::checked(&spec, target, Offers::Read);
    let checked = checked?;
    fleet
        .check(Some(&layouts))
        .map_err(|errors| Error::from(gdl_model::Problems::from(errors)))?;
    if by_cell.is_none() {
        return single(&spec, bundle, target);
    }
    frontend(&spec, &checked.manifest)?;
    cells::bundle(&spec, fleet, bundle, &checked.manifest, &layouts)
}

/// What every bundle of a spec starts from: its deployment settings, how
/// its hub is installed, its generated crates, and its graph's name.
struct Bundle {
    settings: Settings,
    hub_install: String,
    files: Files,
    graph: String,
}

impl Bundle {
    /// `spec` as a bundle deploys it, and what its bundle starts from:
    /// refused unless every crate it builds can be fetched.
    fn prepare(spec: &Spec, out: &Path, target: &Path) -> Result<(Spec, Self), Error> {
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
                    .filter(|binding| matches!(binding.source, CrateSource::Path(_)))
                    .map(|binding| binding.package.as_str()),
            )
            .collect();
        if !local.is_empty() {
            return Err(Error::new(format!(
                "Deploy requires Git or crates.io node and message packages. Re-import these local packages from Git or crates.io: {}",
                local.join(", ")
            )));
        }
        let hub_install = match &spec.gdl {
            GdlDep::Git { url, rev } => format!("--git {} --rev {}", quote(url), quote(rev)),
            GdlDep::Version(version) => format!("--version {}", quote(version)),
            GdlDep::Path(_) => {
                return Err(Error::new(
                    "Deploy requires GDL from Git or crates.io; start the server with --gdl-git and --gdl-rev",
                ));
            }
        };
        let (files, graph) =
            crate::render_with(&spec, out, target, emit::Deps::Exported, Offers::Read)?;
        let bundle = Self {
            settings,
            hub_install,
            files,
            graph,
        };
        Ok((spec, bundle))
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
    /// `deployment`, a path in the bundle.
    fn hub_dockerfile(&self, deployment: &str) -> String {
        let settings = &self.settings;
        dockerfile(
            &self.hub_install,
            "gdl-hub",
            deployment,
            true,
            &settings.build_image,
            settings.hub_image.as_ref().unwrap_or(&settings.base_image),
        )
    }

    /// Segment `segment`'s image, from its crate in `dir`, which builds
    /// package `package`: its Dockerfile and its settings, with `[hub]
    /// host` when `hub` names it. Returns how long Docker must let a
    /// container of it stop, in seconds.
    fn segment_image(
        &mut self,
        dir: &Path,
        segment: &str,
        package: &str,
        hub: Option<&str>,
    ) -> Result<u64, Error> {
        let settings = &self.settings;
        let files = &mut self.files;
        let crate_dir = dir.join(segment);
        let path = crate_dir.join("config.toml");
        files.insert(
            crate_dir.join("Dockerfile"),
            dockerfile(
                &format!("/app/{}/Cargo.toml", crate_dir.display()),
                package,
                &path.display().to_string(),
                false,
                &settings.build_image,
                settings
                    .segment
                    .get(segment)
                    .and_then(|s| s.base_image.as_ref())
                    .unwrap_or(&settings.base_image),
            ),
        );
        let hub = hub.map_or(String::new(), |host| {
            format!("[hub]\nhost = \"{host}\"\n\n")
        });
        files.insert(
            path.clone(),
            format!(
                "# {}\nlisten = \"0.0.0.0:9000\"\nrendezvous_ms = 60000\n\n{hub}[system]\nport = 9090\n",
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

    /// The ports segment `segment` publishes.
    fn ports(&self, segment: &str) -> &[String] {
        self.settings
            .segment
            .get(segment)
            .map_or(&[], |s| s.ports.as_slice())
    }
}

/// The bundle of a spec's one placement: one hub, and each segment.
fn single(spec: &Spec, mut bundle: Bundle, target: &Path) -> Result<Files, Error> {
    // A bundle deploys one placement, and its hub admits only that one.
    let laid = spec.only_layout("`compose`")?;
    let placement = laid.placement;
    let checked = crate::check(spec, target, Offers::Read)?;
    frontend(spec, &checked.manifest)?;
    let deployment = gdl_model::DeploymentSpec::new(checked.manifest, placement.clone())
        .map_err(|e| Error::new(e.to_string()))?;
    bundle.files.insert(
        "deployment.json".into(),
        serde_json::to_string_pretty(&deployment).map_err(|e| Error::new(e.to_string()))?,
    );
    bundle.validate_images()?;
    let mut services = serde_json::Map::new();
    services.insert(
        "hub".into(),
        json!({"build":{"context":".","dockerfile":"hub/Dockerfile"},"healthcheck":hub_healthcheck()}),
    );
    let hub = bundle.hub_dockerfile("deployment.json");
    bundle.files.insert("hub/Dockerfile".into(), hub);
    for (segment, _) in placement.segments() {
        check_segment_name(segment)?;
        let package = emit::package_name(&bundle.graph, segment);
        let grace = bundle.segment_image(Path::new(""), segment, &package, Some("hub"))?;
        // Health is reported, never waited on: no segment depends on another's, since segments
        // whose paths return wait for each other, and an order over them deadlocks. The start
        // period covers loading models.
        services.insert(segment.into(),json!({"build":{"context":".","dockerfile":format!("{segment}/Dockerfile")},"depends_on":{"hub":{"condition":"service_healthy"}},"init":true,"stop_grace_period":format!("{grace}s"),"healthcheck":segment_healthcheck(),"ports":bundle.ports(segment)}));
    }
    let mut files = bundle.files;
    files.insert(
        "compose.yaml".into(),
        serde_json::to_string_pretty(&json!({"services":services}))
            .map_err(|e| Error::new(e.to_string()))?,
    );
    files.insert("README.md".into(), "# Docker Compose deployment\n\nRun `docker compose up --build` from this directory. The hub starts first; every segment registers its compiled contract and waits for all handlers to be ready.\n\nConfigure images, runtime settings, and published ports in the web composer Deploy tab. Review the generated files, then download a new bundle after changes. Runtime images must provide the libraries needed by their executables.\n\nEach segment takes its roster from the hub when it starts: the loaded instances of every segment, at least `min_replicas` of each (one by default). An instance that starts later joins: once it is ready, the segments already running route to it, so `docker compose up --scale <segment>=N` adds instances. Scaling works only for a segment that publishes no host port: two containers cannot bind one port. An instance that is stopped retires: the others stop routing to it, and it leaves once it has drained. Kubernetes and srt-slurm are not generated.\n".into());
    Ok(files)
}

/// A source without a frontend is served on stdin, which a container has
/// none of: the segment would read end of file and stop, ready. A graph
/// spec pins in its `[nodes]` table, a contract spec in `[bind]`.
fn frontend(spec: &Spec, manifest: &gdl_model::Manifest) -> Result<(), Error> {
    let pinned = |name: &str| {
        spec.bind.contains_key(name)
            || matches!(&spec.source, Source::Graph(g) if g.node_pins.contains_key(name))
    };
    if !manifest
        .nodes
        .iter()
        .any(|n| n.role == gdl_model::Role::Source && pinned(&n.name))
    {
        return Err(Error::new(
            "Deploy requires a frontend pinned on a source: without one, the source segment reads requests from stdin, which a container does not provide",
        ));
    }
    Ok(())
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

fn dockerfile(
    manifest: &str,
    package: &str,
    config: &str,
    hub: bool,
    builder: &str,
    base: &str,
) -> String {
    let build = if hub {
        format!("cargo install {manifest} --locked --root /build-tools gdl-hub")
    } else {
        format!("cargo build --release --manifest-path {}", quote(manifest))
    };
    let binary = if hub {
        format!("/build-tools/bin/{package}")
    } else {
        format!("/build/release/{package}")
    };
    format!(
        "# {}\nFROM {builder} AS build\nWORKDIR /app\nCOPY . /app\nENV CARGO_TARGET_DIR=/build\nRUN {build}\nFROM {base}\nCOPY --from=build {binary} /usr/local/bin/segment\nCOPY {config} /etc/gdl/config\nENTRYPOINT [\"/usr/local/bin/segment\",\"/etc/gdl/config\"]\n",
        emit::MARKER,
    )
}
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Write a Compose build context whose dependencies are fetched by Cargo.
pub fn compose(spec: &Spec, fleet: Option<&Fleet>, out: &Path, target: &Path) -> Result<(), Error> {
    if out.exists() {
        return Err(Error::new(
            "Compose output already exists; choose a fresh directory",
        ));
    }
    let out = crate::dump::absolute(out)?;
    let files = compose_files(spec, fleet, &out, target)?;
    crate::write(&out, &files)?;
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
    let out = temp.path().join("deployment");
    compose(spec, None, &out, target)?;
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
