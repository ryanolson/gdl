//! `cargo gdl export`: the generated crates as one `.tar.gz`, to expand
//! and build anywhere on the machine that made it.
//!
//! The archive holds what `generate` writes (the contract crate of a graph
//! file and one crate per segment), each manifest its own workspace root,
//! with every dependency outside the archive named by its absolute path or
//! as the spec names it (a version, a git revision). `build-all.sh` builds
//! each segment with its own `cargo build`, so one segment's dependencies
//! never reach another's, into one shared target directory.
//!
//! When every crate the archive depends on can be fetched anywhere (a
//! version, or a git revision), the archive also holds a `Dockerfile` per
//! segment and `build-images.sh`: one multi-stage image build per segment,
//! from the archive alone. A directory on this machine is not in an image's
//! build context, so an archive that names one holds no image builds.
//!
//! The bytes depend only on the files: entries in a fixed order, with zero
//! times and owners, so two exports of one spec are equal.

use std::path::{Path, PathBuf};

use crate::spec::{CrateSource, GdlDep, Source};
use crate::{Error, Offers, Spec, emit};

/// The spec's crates as a `.tar.gz`, under a directory named for the graph.
/// The contract's manifest is built in `target`, as for [`crate::render`].
pub fn export(spec: &Spec, target: &Path, from: Offers<'_>) -> Result<Vec<u8>, Error> {
    // The output's place only decides which paths are inside it; nothing
    // is written there.
    let out = crate::dump::absolute(&spec.dir)?.join(".gdl-export");
    let (files, graph) = crate::render_with(spec, &out, target, emit::Deps::Exported, from)?;
    let segments: Vec<&str> = spec.placement.segments().map(|(name, _)| name).collect();

    let mut entries: Vec<(PathBuf, Vec<u8>, u32)> = files
        .into_iter()
        .map(|(path, text)| (path, text.into_bytes(), 0o644))
        .collect();
    entries.push((
        "README.md".into(),
        readme(&graph, &segments, portable(spec)).into_bytes(),
        0o644,
    ));
    entries.push((
        "build-all.sh".into(),
        build_all(&segments).into_bytes(),
        0o755,
    ));
    // A spec given as text (a composer's draft) exists nowhere else: the
    // archive holds it and its graph file, and exports again from them.
    // Its paths are absolute or versions, so they hold there too.
    if let (Some(text), Source::Graph(source)) = (&spec.text, &spec.source) {
        let graph = source.text.as_deref().unwrap_or_default();
        let name = source.path.file_name().unwrap_or_default();
        entries.push(("gdl.toml".into(), text.clone().into_bytes(), 0o644));
        entries.push((PathBuf::from(name), graph.as_bytes().to_vec(), 0o644));
    }
    let problems = refusals(spec, &graph);
    if !problems.is_empty() {
        return Err(Error::new(problems.join("\n")));
    }
    if portable(spec) {
        for segment in &segments {
            let package = emit::package_name(&graph, segment);
            entries.push((
                Path::new(segment).join("Dockerfile"),
                dockerfile(&graph, segment, &package).into_bytes(),
                0o644,
            ));
        }
        entries.push((
            "build-images.sh".into(),
            build_images(&graph, &segments).into_bytes(),
            0o755,
        ));
    }

    let gzip = flate2::GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), flate2::Compression::default());
    let mut archive = tar::Builder::new(gzip);
    for (path, data, mode) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(mode);
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        header.set_entry_type(tar::EntryType::Regular);
        archive
            .append_data(&mut header, Path::new(&graph).join(&path), data.as_slice())
            .map_err(|e| Error::new(format!("archiving {}: {e}", path.display())))?;
    }
    let gzip = archive
        .into_inner()
        .map_err(|e| Error::new(format!("archiving: {e}")))?;
    gzip.finish()
        .map_err(|e| Error::new(format!("compressing: {e}")))
}

/// What an export of `spec`, whose graph is `graph`, refuses beyond what
/// rendering it does: a check refuses the same, so a spec that checks
/// exports.
pub(crate) fn refusals(spec: &Spec, graph: &str) -> Vec<String> {
    if !portable(spec) {
        return Vec::new();
    }
    let segments: Vec<&str> = spec.placement.segments().map(|(name, _)| name).collect();
    image_problems(graph, &segments)
}

/// Whether every crate the archive depends on can be fetched anywhere: gdl
/// and each listed crate by a version or a git revision. A contract spec's
/// contract is a directory, so it never is.
fn portable(spec: &Spec) -> bool {
    let Source::Graph(source) = &spec.source else {
        return false;
    };
    !matches!(spec.gdl, GdlDep::Path(_))
        && source
            .crates
            .iter()
            .map(|c| &c.source)
            .chain(source.nodes.iter().map(|c| &c.source))
            .all(|s| matches!(s, CrateSource::Version(_) | CrateSource::Git { .. }))
}

/// A segment's image: built in the Rust image, from the archive's root
/// (the contract and the segment), and run in a slim one with only its
/// binary. The Rust version is the one gdl needs.
fn dockerfile(graph: &str, segment: &str, package: &str) -> String {
    let json = |s: &str| format!("{s:?}");
    format!(
        "# {marker}.\n\
         # Segment `{segment}` of graph `{graph}` as an image, built from the\n\
         # archive's root, which holds the contract it depends on:\n\
         #\n\
         #   docker build -f {segment}/Dockerfile -t {package} .\n\
         ARG RUST_VERSION={rust}\n\
         FROM rust:${{RUST_VERSION}}-slim-bookworm AS build\n\
         WORKDIR /src\n\
         COPY [\"contract\", \"contract\"]\n\
         COPY [{segment_json}, {segment_json}]\n\
         RUN [\"cargo\", \"build\", \"--release\", \"--manifest-path\", {manifest}]\n\
         \n\
         FROM debian:bookworm-slim\n\
         COPY --from=build [{binary}, \"/usr/local/bin/{package}\"]\n\
         ENTRYPOINT [\"/usr/local/bin/{package}\"]\n",
        marker = emit::MARKER,
        rust = env!("CARGO_PKG_RUST_VERSION"),
        segment_json = json(segment),
        // `./`: a segment named `-front` is a path to cargo, not an option.
        manifest = json(&format!("./{segment}/Cargo.toml")),
        binary = json(&format!("/src/{segment}/target/release/{package}")),
    )
}

/// Why a segment's image name would be refused: Docker takes a name of
/// lowercase letters, digits, and separators that starts and ends with a
/// letter or digit, and a package name may end with `-`.
fn image_problems(graph: &str, segments: &[&str]) -> Vec<String> {
    segments
        .iter()
        .filter_map(|segment| {
            let name = emit::package_name(graph, segment);
            let edge = |c: Option<char>| c.is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
            (!edge(name.chars().next()) || !edge(name.chars().last())).then(|| {
                format!(
                    "segment `{segment}`: its image would be named `{name}`, which Docker refuses; name the segment with a letter or digit last"
                )
            })
        })
        .collect()
}

/// Builds one image per segment, each from its own `Dockerfile`.
fn build_images(graph: &str, segments: &[&str]) -> String {
    let mut text = String::from(
        "#!/bin/sh\n\
         # Builds one image per segment, each from its own Dockerfile, so an\n\
         # image holds only its segment's dependencies. Each is tagged with its\n\
         # package's name, after GDL_IMAGE_PREFIX (a registry and path, empty by\n\
         # default). Publishing them is left to you.\n\
         #\n\
         #   sh build-images.sh [docker build arguments]\n\
         set -eu\n\
         cd \"$(dirname \"$0\")\"\n",
    );
    for segment in segments {
        let dockerfile = quoted(&format!("./{segment}/Dockerfile"));
        let package = quoted(&emit::package_name(graph, segment));
        text.push_str(&format!(
            "docker build -f {dockerfile} -t \"${{GDL_IMAGE_PREFIX:-}}\"{package} \"$@\" .\n"
        ));
    }
    text
}

/// Builds every segment, each on its own, into one target directory.
fn build_all(segments: &[&str]) -> String {
    let mut text = String::from(
        "#!/bin/sh\n\
         # Builds each segment crate with its own `cargo build`, so the\n\
         # dependencies of one segment never reach another's. They share one\n\
         # target directory, which caches what they have in common.\n\
         #\n\
         #   sh build-all.sh [cargo build arguments]\n\
         #\n\
         # GDL_PROFILE picks the profile (release by default); CARGO_TARGET_DIR,\n\
         # the target directory (target/ here by default).\n\
         set -eu\n\
         cd \"$(dirname \"$0\")\"\n\
         export CARGO_TARGET_DIR=\"${CARGO_TARGET_DIR:-$PWD/target}\"\n",
    );
    for segment in segments {
        let manifest = quoted(&format!("./{segment}/Cargo.toml"));
        text.push_str(&format!(
            "cargo build --profile \"${{GDL_PROFILE:-release}}\" --manifest-path {manifest} \"$@\"\n"
        ));
    }
    text
}

/// `word` as one `sh` word: in single quotes, each `'` closed, escaped,
/// and reopened.
fn quoted(word: &str) -> String {
    format!("'{}'", word.replace('\'', "'\\''"))
}

fn readme(graph: &str, segments: &[&str], images: bool) -> String {
    let list: String = segments.iter().map(|s| format!("- `{s}/`\n")).collect();
    let images = if images {
        "\n`sh build-images.sh` builds one image per segment, each from its own `Dockerfile`, from this directory alone: every crate comes from crates.io or git. `GDL_IMAGE_PREFIX` goes before each image's name, for the registry you push to.\n"
    } else {
        ""
    };
    format!(
        "# {graph}\n\
         \n\
         The segment crates of graph `{graph}`, generated by `cargo gdl export`. Each is its own Cargo workspace, so a segment links only the node implementations it hosts:\n\
         \n\
         {list}\n\
         `sh build-all.sh` builds them all. Each segment's binary is then in `target/release/`, named for its package; run it with the segment's configuration file. `GDL_PROFILE=dev sh build-all.sh` builds without optimizations.\n\
         \n\
         A dependency outside this directory is named by its absolute path on the machine that made the archive, so the archive builds there. Crates named by a version or a git revision build anywhere.\n{images}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A segment may be named anything a TOML key holds: each manifest
    /// path is one quoted word, and never an option to cargo.
    #[test]
    fn the_build_script_quotes_each_segment() {
        let script = build_all(&["-front", "it's"]);
        assert!(
            script.contains(" --manifest-path './-front/Cargo.toml' \"$@\"\n"),
            "{script}"
        );
        assert!(
            script.contains(" --manifest-path './it'\\''s/Cargo.toml' \"$@\"\n"),
            "{script}"
        );
    }

    /// The Dockerfile's cargo reads a segment's manifest path as a path,
    /// never an option, however the segment is named.
    #[test]
    fn the_dockerfile_names_the_manifest_as_a_path() {
        let dockerfile = dockerfile("g", "-front", "g--front");
        assert!(
            dockerfile.contains("\"--manifest-path\", \"./-front/Cargo.toml\"]"),
            "{dockerfile}"
        );
    }

    /// An image is named for its segment's package, which Docker must accept:
    /// a name that would end in a separator is refused by segment.
    #[test]
    fn image_names_are_names_docker_accepts() {
        assert_eq!(
            image_problems("imaged", &["engine", "front"]),
            Vec::<String>::new()
        );
        assert_eq!(
            image_problems("imaged", &["engine_"]),
            [
                "segment `engine_`: its image would be named `imaged-engine-`, which Docker refuses; name the segment with a letter or digit last"
            ]
        );
    }
}
