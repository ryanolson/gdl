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

fn portable_text(text: &str) -> String {
    text.replace("crates = [\".\"]", "crates = [{ package = \"api-frontend\", git = \"https://github.com/ryanolson/gdl\", rev = \"5840801a303434b5977aab5d1d4c134df329598d\" }]")
        .replace("path = \"../../crates/gdl\"", "git = \"https://github.com/ryanolson/gdl\"\nrev = \"5840801a303434b5977aab5d1d4c134df329598d\"")
}
fn portable_spec(root: &Path) -> Result<gdl_gen::Spec, gdl_gen::Error> {
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
    gdl_gen::Spec::from_text(&portable_text(&text), &graph, &root.join("examples/api"))
}
