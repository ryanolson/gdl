//! Container deployment output contains an ordered hub and one image per segment.
use std::path::Path;
#[test]
fn compose_starts_segments_only_after_hub_health() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let spec = portable_spec(&root).expect("valid test fixture");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-test-")
        .tempdir()
        .expect("valid test fixture");
    let files = gdl_gen::compose_files(&spec, temp.path(), &root.join("target"))
        .expect("valid test fixture");
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("valid test fixture");
    for name in ["front", "engine"] {
        assert_eq!(
            compose["services"][name]["depends_on"]["hub"]["condition"],
            "service_healthy"
        );
        assert!(files.contains_key(&Path::new(name).join("Dockerfile")));
        let config = &files[&Path::new(name).join("config.toml")];
        assert!(config.contains("[hub]\nhost = \"hub\""));
        // The dynamo operator's port; the probe asks the binary itself, so
        // an image needs no HTTP client.
        assert!(config.contains("[system]\nport = 9090"), "{config}");
        let check = &compose["services"][name]["healthcheck"];
        assert_eq!(
            check["test"],
            serde_json::json!([
                "CMD",
                "/usr/local/bin/segment",
                "--healthcheck",
                "/etc/gdl/config"
            ])
        );
        assert!(check["start_period"].is_string(), "{check}");
        // The drain's bound, the request deadline by default, and a margin:
        // Docker otherwise kills a container 10 s after stopping it.
        assert_eq!(compose["services"][name]["stop_grace_period"], "630s");
        // Health is reported, never waited on between segments.
        let depends = compose["services"][name]["depends_on"]
            .as_object()
            .expect("depends_on");
        assert_eq!(depends.keys().collect::<Vec<_>>(), ["hub"]);
    }
    assert!(
        files[Path::new("hub/Dockerfile")]
            .contains("cargo install --git 'https://github.com/ryanolson/gdl'")
    );
    assert!(files.values().all(|text| !text.contains("COPY sources")));
    assert!(
        files[Path::new("front/Cargo.toml")].contains("git = \"https://github.com/ryanolson/gdl\"")
    );
    assert!(files.contains_key(Path::new("deployment.json")));
    // Instances join and retire while the others serve, and the README
    // says when Compose can add them.
    let readme = &files[Path::new("README.md")];
    assert!(
        readme.contains("docker compose up --scale <segment>=N"),
        "{readme}"
    );
    assert!(
        readme.contains("Scaling works only for a segment that publishes no host port"),
        "{readme}"
    );
    assert!(!readme.contains("not routed to"), "{readme}");
}

#[test]
fn each_image_can_override_the_runtime_base_without_changing_the_builder() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text =
        std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("valid test fixture");
    let graph =
        std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("valid test fixture");
    let text = portable_text(&text);
    let text = format!(
        "{text}\n[deployment]\nbase_image = \"ubuntu:24.04\"\nbuild_image = \"rust:1.93-bookworm\"\nhub_image = \"debian:bookworm-slim\"\n[deployment.segment.engine]\nbase_image = \"nvcr.io/nvidia/cuda:12.8.1-runtime-ubuntu24.04\"\n[deployment.segment.engine.config.runtime]\ndrain_ms = 90000\n"
    );
    let spec = gdl_gen::Spec::from_text(&text, &graph, &root.join("examples/api"))
        .expect("valid test fixture");
    let files = gdl_gen::compose_files(
        &spec,
        &root.join("target/deployment-test"),
        &root.join("target"),
    )
    .expect("valid test fixture");
    assert!(files[Path::new("front/Dockerfile")].contains("FROM ubuntu:24.04\n"));
    assert!(
        files[Path::new("engine/Dockerfile")]
            .contains("FROM nvcr.io/nvidia/cuda:12.8.1-runtime-ubuntu24.04\n")
    );
    assert!(files[Path::new("engine/Dockerfile")].contains("FROM rust:1.93-bookworm AS build"));
    // A drain bound set in the deployment's settings sets the grace.
    let compose: serde_json::Value =
        serde_json::from_str(&files[Path::new("compose.yaml")]).expect("valid test fixture");
    assert_eq!(compose["services"]["engine"]["stop_grace_period"], "120s");
    assert_eq!(compose["services"]["front"]["stop_grace_period"], "630s");
}

#[test]
fn preview_files_are_the_exact_downloaded_snapshot() {
    use std::io::Read;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let spec = portable_spec(&root).expect("valid deployment fixture");
    let bundle =
        gdl_gen::compose_bundle(&spec, &root.join("target")).expect("valid deployment fixture");
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bundle.archive.as_slice()));
    let mut matched = 0;
    for entry in archive.entries().expect("valid deployment fixture") {
        let mut entry = entry.expect("valid deployment fixture");
        let path = entry
            .path()
            .expect("valid deployment fixture")
            .strip_prefix("deployment")
            .expect("valid deployment fixture")
            .to_path_buf();
        if let Some(expected) = bundle.files.get(&path) {
            let mut actual = String::new();
            entry
                .read_to_string(&mut actual)
                .expect("valid deployment fixture");
            assert_eq!(&actual, expected, "{}", path.display());
            matched += 1;
        }
    }
    assert_eq!(matched, bundle.files.len());
    assert!(bundle.files.contains_key(Path::new("compose.yaml")));
    assert!(bundle.files.contains_key(Path::new("engine/Dockerfile")));
}

#[test]
fn deployment_rejects_local_node_and_message_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let spec = gdl_gen::Spec::load(&root.join("examples/api/gdl.toml")).expect("fixture");
    let result = gdl_gen::compose_files(&spec, &root.join("target/rejected"), &root.join("target"));
    assert!(result.is_err(), "local packages must not be deployable");
    assert!(
        result
            .expect_err("rejection")
            .to_string()
            .contains("api-frontend")
    );
}

/// The mirror commit that `examples/api/deploy.toml` pins gdl and the
/// example package to, read from that file so that a pin is moved in one
/// place.
fn pinned_rev() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/deploy.toml")).expect("deploy spec");
    let spec: toml::Table = toml::from_str(&text).expect("deploy spec");
    spec["gdl"]["rev"].as_str().expect("[gdl] rev").to_string()
}

fn portable_text(text: &str) -> String {
    let rev = pinned_rev();
    text.replace("crates = [\".\"]", &format!("crates = [{{ package = \"api-frontend\", git = \"https://github.com/ryanolson/gdl\", rev = \"{rev}\" }}]"))
        .replace("path = \"../../crates/gdl-runtime\"", &format!("git = \"https://github.com/ryanolson/gdl\"\nrev = \"{rev}\""))
}

/// The pinned commit carries what generated crates depend on, and the
/// example package comes from the same commit. A pin that predates
/// `gdl-runtime` compiles here, where only text is compared, and fails
/// when cargo resolves the generated manifests in the image build. This
/// reads the local repository. A pin that was never pushed to the mirror
/// fails the tests above, which fetch the example package at the pin.
#[test]
fn the_deployment_pin_carries_the_runtime() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let rev = pinned_rev();
    let text = std::fs::read_to_string(root.join("examples/api/deploy.toml")).expect("deploy spec");
    assert_eq!(
        text.matches(&format!("rev = \"{rev}\"")).count(),
        3,
        "{text}"
    );
    let show = std::process::Command::new("git")
        .arg("-C")
        .arg(&root)
        .args([
            "cat-file",
            "-e",
            &format!("{rev}:crates/gdl-runtime/Cargo.toml"),
        ])
        .status()
        .expect("git");
    assert!(show.success(), "{rev} has no crates/gdl-runtime");
}

fn portable_spec(root: &Path) -> Result<gdl_gen::Spec, gdl_gen::Error> {
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
    gdl_gen::Spec::from_text(&portable_text(&text), &graph, &root.join("examples/api"))
}

/// A spec's classes reach the hub's `deployment.json` and every segment's
/// embedded placement, so all of them agree on the classed hash.
#[test]
fn a_classed_bundle_agrees_on_the_placement_hash() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
    let classes =
        "\n[class.global]\nsegments = [\"front\"]\n[class.model_x]\nsegments = [\"engine\"]\n";
    let spec = gdl_gen::Spec::from_text(
        &format!("{}{classes}", portable_text(&text)),
        &graph,
        &root.join("examples/api"),
    )
    .expect("a classed spec");
    let expected = gdl_model::Placement::from_toml(&format!(
        "[segment.front]\nnodes = [\"Http\"]\n[segment.engine]\nnodes = [\"Engine\"]\n{classes}"
    ))
    .expect("a placement")
    .hash();
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-classes-")
        .tempdir()
        .expect("a directory");
    let files = gdl_gen::compose_files(&spec, temp.path(), &root.join("target")).expect("a bundle");
    let deployment: gdl_model::DeploymentSpec =
        serde_json::from_str(&files[Path::new("deployment.json")]).expect("the hub's spec");
    assert_eq!(deployment.placement.hash(), expected);
    let mut embedded = 0;
    for (path, text) in &files {
        let Some((_, rest)) = text.split_once("pub const PLACEMENT: &str = r#\"") else {
            continue;
        };
        let (placement, _) = rest.split_once("\"#;").expect("a raw string");
        let placement = gdl_model::Placement::from_toml(placement).expect("an embedded placement");
        assert_eq!(placement.hash(), expected, "{}", path.display());
        embedded += 1;
    }
    assert_eq!(embedded, 2, "one embedded placement per segment");
}
