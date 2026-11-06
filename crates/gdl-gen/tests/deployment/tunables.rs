//! Compose must keep tunables supplied by an override's environment file.

use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::{Value, json};

use super::{Mirror, ONE_FLEET, SPLIT, placed, portable_spec};

const OWN: &str = "glibc.malloc.arena_max=4";

/// An explicit environment file must replace the image's default just as
/// an explicit environment mapping does. Only Compose parses the files, so
/// the test reads them with `docker compose config`, and says it skipped
/// where that is missing. gdl's services set no `GLIBC_TUNABLES`: the
/// image's `ENV` line is the default, and Compose ranks both of the
/// user's places above it.
#[test]
fn an_override_env_file_keeps_the_users_tunables() {
    let version = Command::new("timeout")
        .args(["10", "docker", "compose", "version"])
        .stdin(Stdio::null())
        .output();
    if !version.is_ok_and(|output| output.status.success()) {
        eprintln!("skipped: `docker compose version` fails");
        return;
    }
    let mirror = Mirror::new();
    let fleet = gdl_model::Fleet::from_toml(ONE_FLEET).expect("a fleet");
    let mut replaced = Vec::new();
    for (spec, fleet, segments) in [
        (
            portable_spec(&mirror).expect("a spec"),
            None,
            vec!["front", "engine"],
        ),
        (
            placed(&mirror, SPLIT),
            Some(&fleet),
            vec!["global-us_front", "model-x-01_engine", "model-x-02_engine"],
        ),
    ] {
        let dir = tempfile::Builder::new()
            .prefix("gdl-compose-tunables-")
            .tempdir()
            .expect("a directory");
        let files = gdl_gen::compose_files(
            &spec,
            fleet,
            dir.path(),
            &mirror.target(),
            gdl_gen::Sources::Fetched,
        )
        .expect("a bundle");
        std::fs::write(
            dir.path().join("compose.yaml"),
            &files[Path::new("compose.yaml")],
        )
        .expect("the generated Compose file");
        std::fs::write(dir.path().join(".env"), format!("GLIBC_TUNABLES={OWN}\n"))
            .expect("the user's environment file");

        let read = |entry: Value| -> Value {
            let services: serde_json::Map<String, Value> = segments
                .iter()
                .map(|name| (name.to_string(), entry.clone()))
                .collect();
            std::fs::write(
                dir.path().join("compose.override.yaml"),
                json!({"services": services}).to_string(),
            )
            .expect("the user's override");
            let output = Command::new("timeout")
                .args([
                    "10",
                    "docker",
                    "compose",
                    "--project-name",
                    "gdl-tunables",
                    "--file",
                    "compose.yaml",
                    "--file",
                    "compose.override.yaml",
                    "config",
                    "--format",
                    "json",
                ])
                .env_remove("GLIBC_TUNABLES")
                .current_dir(dir.path())
                .stdin(Stdio::null())
                .output()
                .expect("docker compose config");
            assert!(output.status.success(), "{output:?}");
            serde_json::from_slice(&output.stdout).expect("the resolved Compose configuration")
        };

        let explicit = read(json!({"environment": {"GLIBC_TUNABLES": "${GLIBC_TUNABLES}"}}));
        for segment in &segments {
            assert_eq!(
                explicit["services"][segment]["environment"]["GLIBC_TUNABLES"],
                OWN
            );
        }

        let from_file = read(json!({"env_file": ".env"}));
        for segment in segments {
            let value = from_file["services"][segment]["environment"]["GLIBC_TUNABLES"].clone();
            if value != OWN {
                replaced.push((segment, value));
            }
        }
    }
    assert!(replaced.is_empty(), "user tunables replaced: {replaced:#?}");
}
