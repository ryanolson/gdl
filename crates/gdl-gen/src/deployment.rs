use serde::Deserialize;
use serde_json::json;
use std::path::Path;
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Settings {
    base_image: String,
    build_image: String,
    hub_image: Option<String>,
    segment: std::collections::BTreeMap<String, Image>,
}
/// `[deployment] base_image` when the spec does not set it: the image a
/// segment, and the hub unless `hub_image` says otherwise, runs in.
pub(crate) const DEFAULT_BASE_IMAGE: &str = "debian:bookworm-slim";

/// `[deployment] build_image` when the spec does not set it: the image
/// every binary is built in.
pub(crate) const DEFAULT_BUILD_IMAGE: &str = "rust:1.93-slim-bookworm";

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
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Image {
    base_image: Option<String>,
    #[serde(default)]
    ports: Vec<String>,
    #[serde(default)]
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

/// Render a Docker Compose deployment from Git or registry dependencies.
pub fn compose_files(spec: &Spec, out: &Path, target: &Path) -> Result<Files, Error> {
    // A deployment discovers its segments through the hub, whether or not
    // the spec says anything else about it.
    let mut spec = spec.clone();
    let settings = spec.deployment.get_or_insert_with(Default::default).clone();
    let spec = &spec;
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
    let (mut files, graph) =
        crate::render_with(spec, out, target, emit::Deps::Exported, Offers::Read)?;
    let checked = crate::check(spec, target, Offers::Read)?;
    // A source without a frontend is served on stdin, which a container
    // has none of: the segment would read end of file and stop, ready.
    // A graph spec pins in its `[nodes]` table, a contract spec in `[bind]`.
    let pinned = |name: &str| {
        spec.bind.contains_key(name)
            || matches!(&spec.source, Source::Graph(g) if g.node_pins.contains_key(name))
    };
    if !checked
        .manifest
        .nodes
        .iter()
        .any(|n| n.role == gdl_model::Role::Source && pinned(&n.name))
    {
        return Err(Error::new(
            "Deploy requires a frontend pinned on a source: without one, the source segment reads requests from stdin, which a container does not provide",
        ));
    }
    let deployment = gdl_model::DeploymentSpec::new(checked.manifest, spec.placement.clone())
        .map_err(|e| Error::new(e.to_string()))?;
    files.insert(
        "deployment.json".into(),
        serde_json::to_string_pretty(&deployment).map_err(|e| Error::new(e.to_string()))?,
    );
    let settings = &settings;
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
    for name in settings.segment.keys() {
        if !spec
            .placement
            .segments()
            .any(|(segment, _)| segment == name)
        {
            return Err(Error::new(format!(
                "[deployment.segment.{name}]: the placement has no segment `{name}`"
            )));
        }
    }
    let mut services = serde_json::Map::new();
    services.insert("hub".into(),json!({"build":{"context":".","dockerfile":"hub/Dockerfile"},"healthcheck":{"test":["CMD","/usr/local/bin/segment","--healthcheck"],"interval":"2s","timeout":"2s","retries":30}}));
    files.insert(
        "hub/Dockerfile".into(),
        dockerfile(
            &hub_install,
            "gdl-hub",
            "deployment.json",
            true,
            &settings.build_image,
            settings.hub_image.as_ref().unwrap_or(&settings.base_image),
        ),
    );
    for (segment, _) in spec.placement.segments() {
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
        let package = emit::package_name(&graph, segment);
        files.insert(
            Path::new(segment).join("Dockerfile"),
            dockerfile(
                &format!("/app/{segment}/Cargo.toml"),
                &package,
                &format!("{segment}/config.toml"),
                false,
                &settings.build_image,
                settings
                    .segment
                    .get(segment)
                    .and_then(|s| s.base_image.as_ref())
                    .unwrap_or(&settings.base_image),
            ),
        );
        files.insert(
            Path::new(segment).join("config.toml"),
            format!(
                "# {}\nlisten = \"0.0.0.0:9000\"\nrendezvous_ms = 60000\n\n[hub]\nhost = \"hub\"\n\n[system]\nport = 9090\n",
                emit::MARKER
            ),
        );
        if let Some(image) = settings.segment.get(segment) {
            let path = Path::new(segment).join("config.toml");
            let mut config: toml::Table = toml::from_str(&files[&path]).map_err(io)?;
            for (key, value) in &image.config {
                if ["hub", "roster", "instances"].contains(&key.as_str()) {
                    return Err(Error::new(format!("deployment cannot override `{key}`")));
                }
                config.insert(key.clone(), value.clone());
            }
            files.insert(path, toml::to_string_pretty(&config).map_err(io)?);
        }
        // Docker kills a container 10 s after it stops it unless told otherwise; a segment may
        // drain for its bound, read here from its settings as generated (an environment
        // override at run time is not seen).
        let config: toml::Table =
            toml::from_str(&files[&Path::new(segment).join("config.toml")]).map_err(io)?;
        let grace = drain_bound(&config).as_secs() + 30;
        // Health is reported, never waited on: no segment depends on another's, since segments
        // whose paths return wait for each other, and an order over them deadlocks. The start
        // period covers loading models.
        services.insert(segment.into(),json!({"build":{"context":".","dockerfile":format!("{segment}/Dockerfile")},"depends_on":{"hub":{"condition":"service_healthy"}},"init":true,"stop_grace_period":format!("{grace}s"),"healthcheck":{"test":["CMD","/usr/local/bin/segment","--healthcheck","/etc/gdl/config"],"interval":"5s","timeout":"3s","retries":3,"start_period":"1800s"},"ports":settings.segment.get(segment).map(|s| s.ports.clone()).unwrap_or_default()}));
    }
    files.insert(
        "compose.yaml".into(),
        serde_json::to_string_pretty(&json!({"services":services}))
            .map_err(|e| Error::new(e.to_string()))?,
    );
    files.insert("README.md".into(), "# Docker Compose deployment\n\nRun `docker compose up --build` from this directory. The hub starts first; every segment registers its compiled contract and waits for all handlers to be ready.\n\nConfigure images, runtime settings, and published ports in the web composer Deploy tab. Review the generated files, then download a new bundle after changes. Runtime images must provide the libraries needed by their executables.\n\nEach segment takes its roster from the hub when it starts: the loaded instances of every segment, at least `min_replicas` of each (one by default). An instance that starts later joins: once it is ready, the segments already running route to it, so `docker compose up --scale <segment>=N` adds instances. Scaling works only for a segment that publishes no host port: two containers cannot bind one port. An instance that is stopped retires: the others stop routing to it, and it leaves once it has drained. Kubernetes and srt-slurm are not generated.\n".into());
    Ok(files)
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
pub fn compose(spec: &Spec, out: &Path, target: &Path) -> Result<(), Error> {
    if out.exists() {
        return Err(Error::new(
            "Compose output already exists; choose a fresh directory",
        ));
    }
    let out = crate::dump::absolute(out)?;
    let files = compose_files(spec, &out, target)?;
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
    compose(spec, &out, target)?;
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
