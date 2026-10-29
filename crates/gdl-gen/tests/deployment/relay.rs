//! A parent pool's hub in a fleet's bundle can write its state volume.

use std::path::Path;
use std::process::{Command, Output};

use super::{Mirror, ONE_FLEET, SPLIT, fleet_bundle, placed};

/// A parent's hub, as the bundle builds it, runs as the base image's
/// user, which by default is not root, and must still store a child's
/// record in its new volume. The test builds the generated hub image with
/// the hub's build replaced by a probe that writes as the hub would, and
/// checks root's write as a control.
#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_parent_can_store_a_record_in_its_new_volume() {
    let mirror = Mirror::new();
    let fleet = gdl_model::Fleet::from_toml(ONE_FLEET).expect("a fleet");
    let (files, mut compose) = fleet_bundle(&mirror, &placed(&mirror, SPLIT), &fleet);
    let temp = tempfile::Builder::new()
        .prefix("gdl-relay-volume-")
        .tempdir()
        .expect("a build context");
    for (path, text) in &files {
        let path = temp.path().join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("the directory");
        std::fs::write(path, text).expect("a bundle file");
    }

    // Replace only the application build. Keep the generated runtime stage,
    // its user, and the Compose volume, so image setup controls its access.
    let dockerfile = &files[Path::new("hub/Dockerfile")];
    let start = dockerfile.find("cargo install ").expect("the hub build");
    let suffix = " --locked --root /build-tools gdl-hub";
    let end = start
        + dockerfile[start..]
            .find(suffix)
            .expect("the install target")
        + suffix.len();
    let dockerfile = dockerfile.replacen(
        &dockerfile[start..end],
        "mkdir -p /build-tools/bin && rustc /app/probe.rs -o /build-tools/bin/gdl-hub",
        1,
    );
    std::fs::write(temp.path().join("hub/Dockerfile"), dockerfile).expect("the probe build");
    std::fs::write(
        temp.path().join("probe.rs"),
        r#"fn main() {
    let root = std::path::PathBuf::from(std::env::var("GDL_POOL__STATE_DIR").expect("state_dir"));
    let class = root.join("model_x");
    std::fs::create_dir_all(&class).expect("make the child's class directory");
    std::fs::write(class.join("model-x-01.json"), b"{}").expect("store the child's record");
}
"#,
    )
    .expect("the storage probe");

    let name = temp
        .path()
        .file_name()
        .expect("a name")
        .to_str()
        .expect("UTF-8")
        .to_ascii_lowercase();
    compose["services"]["global-us"]["image"] = name.clone().into();
    std::fs::write(
        temp.path().join("compose.yaml"),
        serde_json::to_vec_pretty(&compose).expect("Compose JSON"),
    )
    .expect("the Compose file");
    let project = Project {
        name,
        root: temp.path().to_owned(),
    };
    let built = project.run("120", &["build", "global-us"]);
    assert!(built.status.success(), "image build: {built:?}");
    let stored = project.run("30", &["run", "--rm", "--no-deps", "global-us"]);
    let control = project.run(
        "30",
        &["run", "--rm", "--no-deps", "--user", "0:0", "global-us"],
    );
    assert!(control.status.success(), "root can write: {control:?}");
    assert!(
        stored.status.success(),
        "the default runtime user cannot store a child's record in its new volume:\n{}",
        String::from_utf8_lossy(&stored.stderr)
    );
}

struct Project {
    name: String,
    root: std::path::PathBuf,
}

impl Project {
    fn run(&self, seconds: &str, args: &[&str]) -> Output {
        Command::new("timeout")
            .args([seconds, "docker", "compose", "--project-name", &self.name])
            .args(args)
            .current_dir(&self.root)
            .output()
            .expect("docker compose")
    }
}

impl Drop for Project {
    fn drop(&mut self) {
        let _ = self.run("30", &["down", "--volumes", "--remove-orphans"]);
        let _ = Command::new("timeout")
            .args(["30", "docker", "image", "rm", &self.name])
            .output();
    }
}
