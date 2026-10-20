use serde::Deserialize;
use serde_json::json;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::Command,
};
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Settings {
    base_image: String,
    build_image: String,
    hub_image: Option<String>,
    segment: std::collections::BTreeMap<String, Image>,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            base_image: "debian:bookworm-slim".into(),
            build_image: "rust:1.93-slim-bookworm".into(),
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

/// Render a Docker Compose deployment. Local dependencies are staged separately by [`compose`].
pub fn compose_files(spec: &Spec, out: &Path, target: &Path) -> Result<Files, Error> {
    let GdlDep::Path(gdl) = &spec.gdl else {
        return Err(Error::new(
            "Compose currently requires a local GDL checkout",
        ));
    };
    let root = gdl
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| Error::new("GDL workspace path is missing"))?;
    let (mut files, graph) =
        crate::render_with(spec, out, target, emit::Deps::Exported, Offers::Read)?;
    let checked = crate::check(spec, target, Offers::Read)?;
    let deployment = gdl_model::DeploymentSpec::new(checked.manifest, spec.placement.clone())
        .map_err(|e| Error::new(e.to_string()))?;
    files.insert(
        "deployment.json".into(),
        serde_json::to_string_pretty(&deployment).map_err(|e| Error::new(e.to_string()))?,
    );
    let settings = &spec.deployment;
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
                "image override names unknown segment `{name}`"
            )));
        }
    }
    let mut services = serde_json::Map::new();
    services.insert("hub".into(),json!({"build":{"context":".","dockerfile":"hub/Dockerfile"},"healthcheck":{"test":["CMD","/usr/local/bin/segment","--healthcheck"],"interval":"2s","timeout":"2s","retries":30}}));
    files.insert(
        "hub/Dockerfile".into(),
        dockerfile(
            &format!("{}/Cargo.toml", root.display()),
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
                "# {}\nlisten = \"0.0.0.0:9000\"\nhub = \"hub\"\nrendezvous_ms = 60000\n",
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
        services.insert(segment.into(),json!({"build":{"context":".","dockerfile":format!("{segment}/Dockerfile")},"depends_on":{"hub":{"condition":"service_healthy"}},"init":true,"ports":settings.segment.get(segment).map(|s| s.ports.clone()).unwrap_or_default()}));
    }
    files.insert(
        "compose.yaml".into(),
        serde_json::to_string_pretty(&json!({"services":services}))
            .map_err(|e| Error::new(e.to_string()))?,
    );
    files.insert("README.md".into(), "# Docker Compose deployment\n\nRun `docker compose up --build` from this directory. The hub starts first; every segment registers its compiled contract and waits for all handlers to be ready.\n\nConfigure images, runtime settings, and published ports in the web composer Deploy tab. Review the generated files, then download a new bundle after changes. Runtime images must provide the libraries needed by their executables.\n\nThis deployment uses one replica per segment and a startup roster. Restart the whole stack after replacing a segment. Kubernetes and srt-slurm are not generated.\n".into());
    Ok(files)
}

fn dockerfile(
    manifest: &str,
    package: &str,
    config: &str,
    hub: bool,
    builder: &str,
    base: &str,
) -> String {
    let package_arg = if hub { " -p gdl-hub" } else { "" };
    format!(
        "# {}\nFROM {builder} AS build\nWORKDIR /app\nCOPY sources /\nCOPY . /app\nENV CARGO_TARGET_DIR=/build\nRUN cargo build --release --manifest-path {}{}\nFROM {base}\nCOPY --from=build /build/release/{package} /usr/local/bin/segment\nCOPY {config} /etc/gdl/config\nENTRYPOINT [\"/usr/local/bin/segment\",\"/etc/gdl/config\"]\n",
        emit::MARKER,
        quote(manifest),
        package_arg
    )
}
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Snapshot local Git worktrees and generated crates into a new Compose build context.
/// Ignored files, including target directories and local credentials, are not copied.
pub fn compose(spec: &Spec, out: &Path, target: &Path) -> Result<(), Error> {
    if out.exists() {
        return Err(Error::new(
            "Compose output already exists; choose a fresh directory",
        ));
    }
    let out = crate::dump::absolute(out)?;
    let files = compose_files(spec, &out, target)?;
    let mut paths = Vec::new();
    if let GdlDep::Path(path) = &spec.gdl {
        paths.push(path.clone());
    }
    match &spec.source {
        Source::Graph(source) => {
            for listed in source.crates.iter().chain(&source.nodes) {
                if let CrateSource::Path(path) = &listed.source {
                    paths.push(path.clone());
                }
            }
        }
        Source::Contract(contract) => paths.push(contract.path.clone()),
    }
    for binding in spec.bind.values() {
        if let CrateSource::Path(path) = &binding.source {
            paths.push(path.clone());
        }
    }
    let mut roots = BTreeSet::new();
    for path in paths {
        let output = Command::new("git")
            .arg("-C")
            .arg(&path)
            .args(["rev-parse", "--show-toplevel"])
            .output()
            .map_err(io)?;
        if !output.status.success() {
            return Err(Error::new(format!(
                "local dependency {} must belong to a Git worktree",
                path.display()
            )));
        }
        roots.insert(PathBuf::from(
            String::from_utf8(output.stdout).map_err(io)?.trim(),
        ));
    }
    let mut sources = Vec::new();
    for root in roots {
        let output = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args([
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ])
            .output()
            .map_err(io)?;
        if !output.status.success() {
            return Err(Error::new("could not list local source files"));
        }
        for name in output
            .stdout
            .split(|b| *b == 0)
            .filter(|name| !name.is_empty())
        {
            let name = std::str::from_utf8(name).map_err(io)?;
            let source = root.join(name);
            if source.starts_with(&out) || !source.exists() {
                continue;
            }
            if source
                .symlink_metadata()
                .map_err(io)?
                .file_type()
                .is_symlink()
            {
                continue;
            }
            sources.push(source);
        }
    }
    crate::write(&out, &files)?;
    for source in sources {
        let destination = out
            .join("sources")
            .join(source.strip_prefix("/").map_err(io)?);
        std::fs::create_dir_all(
            destination
                .parent()
                .ok_or_else(|| Error::new("missing source parent"))?,
        )
        .map_err(io)?;
        std::fs::copy(&source, destination).map_err(io)?;
    }
    Ok(())
}
fn io(e: impl std::fmt::Display) -> Error {
    Error::new(e.to_string())
}

/// Generated files and their self-contained build archive from one snapshot.
pub struct ComposeBundle {
    /// Generated text files, excluding captured dependency sources.
    pub files: Files,
    /// Gzip-compressed tar containing these files and dependency sources.
    pub archive: Vec<u8>,
}

/// Capture the generated files and sources once for preview and download.
pub fn compose_bundle(spec: &Spec, target: &Path) -> Result<ComposeBundle, Error> {
    let temp = tempfile::tempdir().map_err(io)?;
    let out = temp.path().join("deployment");
    compose(spec, &out, target)?;
    let mut files = Files::new();
    let mut pending = vec![out.clone()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            let path = entry.path();
            let relative = path.strip_prefix(&out).map_err(io)?;
            if relative.starts_with("sources") {
                continue;
            }
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
