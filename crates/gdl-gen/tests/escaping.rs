//! Local bundle paths must build or fail before the bundle is written.

mod common;

use common::{root, target};
use std::process::Command;

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_bundle_with_url_syntax_builds_or_is_refused_before_writing() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-percent-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let spec = gdl_gen::Spec::load(&root().join("examples/http/gdl.toml")).expect("the spec");
    let mut failures = Vec::new();
    for name in [
        "bundle%41",
        "bundle%",
        "bundle%GG",
        "bundle\t",
        "bundle\u{1b}",
        "bundle\u{7f}",
    ] {
        let out = holder.path().join(name);
        if let Err(error) = gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Local,
        ) {
            assert_ne!(name, "bundle%41", "the control path must work: {error}");
            assert!(!out.exists(), "refusal must precede writing");
            assert!(error.to_string().starts_with("Compose output"), "{error}");
            continue;
        }
        let config = Command::new("timeout")
            .args([
                "15",
                "docker",
                "compose",
                "--project-name",
                "gdl-review-percent",
                "config",
                "--quiet",
            ])
            .current_dir(&out)
            .output()
            .expect("docker compose config");
        if !config.status.success() {
            failures.push(format!(
                "accepted {name:?}, but Compose cannot read the written bundle:\n{}",
                String::from_utf8_lossy(&config.stderr)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_context_with_template_syntax_builds_or_is_refused_before_writing() {
    use std::path::Path;

    fn copy_sources(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("the copied directory");
        for entry in std::fs::read_dir(from).expect("the sources") {
            let entry = entry.expect("a source");
            if entry.file_name() == "target" || entry.file_name() == ".git" {
                continue;
            }
            let into = to.join(entry.file_name());
            if entry.file_type().expect("the source type").is_dir() {
                copy_sources(&entry.path(), &into);
            } else {
                std::fs::copy(entry.path(), into).expect("the copied source");
            }
        }
    }

    struct Image(String);
    impl Drop for Image {
        fn drop(&mut self) {
            let _ = Command::new("timeout")
                .args(["15", "docker", "image", "rm", "--force", &self.0])
                .output();
        }
    }

    let holder = tempfile::Builder::new()
        .prefix("gdl-ancestry-")
        .tempdir()
        .expect("a source directory");
    for (case, name) in ["plain", "checkout${GDL_REVIEW_PARENT}"].iter().enumerate() {
        let checkout = holder.path().join(name);
        std::fs::create_dir(&checkout).expect("the checkout");
        for file in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"] {
            std::fs::copy(root().join(file), checkout.join(file)).expect("a workspace file");
        }
        copy_sources(&root().join("crates"), &checkout.join("crates"));
        copy_sources(
            &root().join("examples/http"),
            &checkout.join("examples/http"),
        );
        let spec = gdl_gen::Spec::load(&checkout.join("examples/http/gdl.toml")).expect("the spec");
        let out = checkout.join("target/bundle");
        let context = match gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Local,
        ) {
            Ok(context) => context.expect("the local context"),
            Err(error) => {
                assert_ne!(*name, "plain", "the control checkout must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                assert!(error.to_string().starts_with("Compose output"), "{error}");
                continue;
            }
        };
        assert_eq!(context, checkout);
        let dockerfile = out.join("hub/Dockerfile");
        let generated = std::fs::read_to_string(&dockerfile).expect("the generated Dockerfile");
        let copy = generated
            .lines()
            .find(|line| line.starts_with("COPY ["))
            .expect("the settings copy");
        std::fs::write(&dockerfile, format!("FROM scratch\n{copy}\n")).expect("the copy probe");
        let project = format!("gdl-review-ancestry-{}-{case}", std::process::id());
        let _image = Image(format!("{project}-hub"));
        let build = Command::new("timeout")
            .args([
                "30",
                "docker",
                "compose",
                "--project-name",
                &project,
                "build",
                "hub",
            ])
            .env_remove("GDL_REVIEW_PARENT")
            .current_dir(&out)
            .output()
            .expect("docker compose build");
        assert!(
            build.status.success(),
            "accepted checkout {name:?} with bundle target/bundle, but Compose cannot build it:\n{}",
            String::from_utf8_lossy(&build.stderr)
        );
    }
}

#[cfg(unix)]
#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_fetched_bundle_with_non_utf8_ancestry_builds_or_is_refused_before_writing() {
    use std::os::unix::ffi::OsStrExt;

    struct Image(String);
    impl Drop for Image {
        fn drop(&mut self) {
            let _ = Command::new("timeout")
                .args(["15", "docker", "image", "rm", "--force", &self.0])
                .output();
        }
    }

    let holder = tempfile::Builder::new()
        .prefix("gdl-review-bytes-")
        .tempdir()
        .expect("a bundle directory");
    let spec = gdl_gen::Spec::load(&root().join("examples/api/deploy.toml")).expect("the spec");
    let mut failures = Vec::new();
    for (case, name) in [
        b"bundle".as_slice(),
        b"bundle-\xff".as_slice(),
        b"parent-\xff/bundle".as_slice(),
    ]
    .iter()
    .enumerate()
    {
        let out = holder.path().join(std::ffi::OsStr::from_bytes(name));
        match gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Fetched,
        ) {
            Ok(_) => {}
            Err(error) => {
                assert_ne!(case, 0, "the control bundle must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                assert!(error.to_string().starts_with("Compose output"), "{error}");
                assert!(error.to_string().contains("UTF-8"), "{error}");
                continue;
            }
        }
        let dockerfile = out.join("hub/Dockerfile");
        let generated = std::fs::read_to_string(&dockerfile).expect("the generated Dockerfile");
        let copy = generated
            .lines()
            .find(|line| line.starts_with("COPY ["))
            .expect("the settings copy");
        std::fs::write(&dockerfile, format!("FROM scratch\n{copy}\n"))
            .expect("the settings copy probe");
        let project = format!("gdl-review-bytes-{}-{case}", std::process::id());
        let _image = Image(format!("{project}-hub"));
        let build = Command::new("timeout")
            .args([
                "30",
                "docker",
                "compose",
                "--project-name",
                &project,
                "build",
                "hub",
            ])
            .current_dir(&out)
            .output()
            .expect("docker compose build");
        if !build.status.success() {
            assert_ne!(case, 0, "the control bundle must build");
            failures.push(format!(
                "accepted {:?}, but Compose cannot build the fetched bundle:\n{}",
                out.as_os_str(),
                String::from_utf8_lossy(&build.stderr)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_unicode_bundle_works_without_an_explicit_project_name_or_is_refused() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-project-name-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let spec = gdl_gen::Spec::load(&root().join("examples/http/gdl.toml")).expect("the spec");
    let mut failures = Vec::new();
    for name in ["bundle", "部署", "---"] {
        let out = holder.path().join(name);
        if let Err(error) = gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Local,
        ) {
            assert_ne!(name, "bundle", "the control path must work: {error}");
            assert!(!out.exists(), "refusal must precede writing");
            assert!(error.to_string().starts_with("Compose output"), "{error}");
            continue;
        }
        let config = Command::new("timeout")
            .args(["15", "docker", "compose", "config", "--quiet"])
            .env_remove("COMPOSE_PROJECT_NAME")
            .current_dir(&out)
            .output()
            .expect("docker compose config");
        if !config.status.success() {
            failures.push(format!(
                "accepted {name:?}, but Compose cannot read the written bundle without --project-name:\n{}",
                String::from_utf8_lossy(&config.stderr)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_bundle_under_a_git_directory_builds_or_is_refused_before_writing() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-ignored-path-")
        .tempdir_in(root())
        .expect("a directory in the context");
    let spec = gdl_gen::Spec::load(&root().join("examples/http/gdl.toml")).expect("the spec");
    for name in ["bundle", "nested/.git/bundle"] {
        let out = holder.path().join(name);
        let context = match gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Local,
        ) {
            Ok(context) => context.expect("the local context"),
            Err(error) => {
                assert_ne!(name, "bundle", "the control path must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                assert!(error.to_string().starts_with("Compose output"), "{error}");
                continue;
            }
        };
        let staging = tempfile::Builder::new()
            .prefix("gdl-ignore-context-")
            .tempdir()
            .expect("a minimal context");
        let exported = tempfile::Builder::new()
            .prefix("gdl-ignore-export-")
            .tempdir()
            .expect("the exported image");
        let relative = out
            .strip_prefix(&context)
            .expect("the bundle is in its context");
        let source = staging.path().join(relative).join("deployment.json");
        std::fs::create_dir_all(source.parent().expect("a parent")).expect("the source directory");
        let settings = std::fs::read(out.join("deployment.json")).expect("the generated settings");
        std::fs::write(source, &settings).expect("the settings in the minimal context");
        let generated =
            std::fs::read_to_string(out.join("hub/Dockerfile")).expect("the generated Dockerfile");
        let copy = generated
            .lines()
            .find(|line| line.starts_with("COPY ["))
            .expect("the settings copy");
        std::fs::write(
            staging.path().join("Dockerfile"),
            format!("FROM scratch\n{copy}\n"),
        )
        .expect("the settings copy probe");
        std::fs::copy(
            out.join("hub/Dockerfile.dockerignore"),
            staging.path().join("Dockerfile.dockerignore"),
        )
        .expect("the generated ignore file");
        let build = Command::new("timeout")
            .args(["30", "docker", "build", "--network=none", "--output"])
            .arg(format!("type=local,dest={}", exported.path().display()))
            .arg(staging.path())
            .output()
            .expect("docker build");
        assert!(
            build.status.success(),
            "accepted {name:?}, but the generated ignore file prevents its settings COPY:\n{}",
            String::from_utf8_lossy(&build.stderr)
        );
        assert_eq!(
            std::fs::read(exported.path().join("etc/gdl/config")).expect("the copied settings"),
            settings
        );
    }
}

fn build_settings(out: &std::path::Path, project: &str) -> std::process::Output {
    struct Image(String);
    impl Drop for Image {
        fn drop(&mut self) {
            let _ = Command::new("timeout")
                .args(["15", "docker", "image", "rm", "--force", &self.0])
                .output();
        }
    }

    let dockerfile = out.join("hub/Dockerfile");
    let generated = std::fs::read_to_string(&dockerfile).expect("the generated Dockerfile");
    let copy = generated
        .lines()
        .find(|line| line.starts_with("COPY ["))
        .expect("the settings copy");
    std::fs::write(&dockerfile, format!("FROM scratch\n{copy}\n"))
        .expect("the settings copy probe");
    let _image = Image(format!("{project}-hub"));
    let built = Command::new("timeout")
        .args([
            "30",
            "docker",
            "compose",
            "--project-name",
            project,
            "build",
            "hub",
        ])
        .current_dir(out)
        .output()
        .expect("docker compose build");
    if !built.status.success() {
        return built;
    }
    eprintln!("built {project}; checking the copied settings");
    let created = Command::new("timeout")
        .args(["15", "docker", "create", &_image.0, "none"])
        .output()
        .expect("docker create");
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    struct Container(String);
    impl Drop for Container {
        fn drop(&mut self) {
            let _ = Command::new("timeout")
                .args(["15", "docker", "rm", "--force", &self.0])
                .output();
        }
    }
    let container = Container(
        String::from_utf8(created.stdout)
            .expect("the container ID")
            .trim()
            .to_owned(),
    );
    let exported = tempfile::Builder::new()
        .prefix("gdl-review-settings-")
        .tempdir()
        .expect("the exported settings");
    let destination = exported.path().join("settings");
    let copied = Command::new("timeout")
        .args(["15", "docker", "cp"])
        .arg(format!("{}:/etc/gdl/config", container.0))
        .arg(&destination)
        .output()
        .expect("docker cp");
    if copied.status.success() {
        assert_eq!(
            std::fs::read(destination).expect("the copied settings"),
            std::fs::read(out.join("deployment.json")).expect("the generated settings")
        );
    }
    copied
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_local_bundle_with_a_leading_bang_builds_or_is_refused_before_writing() {
    let spec = gdl_gen::Spec::load(&root().join("examples/http/gdl.toml")).expect("the spec");
    for (case, prefix) in ["gdl-review-bang-", "!gdl-review-bang-"].iter().enumerate() {
        let holder = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir_in(root())
            .expect("a directory in the context");
        let out = holder.path().join("bundle");
        match gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Local,
        ) {
            Ok(_) => {}
            Err(error) => {
                assert_ne!(case, 0, "the control bundle must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                assert!(error.to_string().contains('!'), "{error}");
                continue;
            }
        }
        let project = format!("gdl-review-bang-{}-{case}", std::process::id());
        let build = build_settings(&out, &project);
        assert!(
            build.status.success(),
            "accepted {:?}, but Docker cannot copy the bundle's settings:\n{}",
            out.as_os_str(),
            String::from_utf8_lossy(&build.stderr)
        );
    }
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_bundle_with_decomposed_unicode_builds_or_is_refused_before_writing() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-review-unicode-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let mut failures = Vec::new();
    for (mode, (spec_path, sources)) in [
        ("examples/http/gdl.toml", gdl_gen::Sources::Local),
        ("examples/api/deploy.toml", gdl_gen::Sources::Fetched),
    ]
    .iter()
    .enumerate()
    {
        let spec = gdl_gen::Spec::load(&root().join(spec_path)).expect("the spec");
        for (case, name) in [
            "control-caf\u{e9}",
            "bundle-cafe\u{301}",
            "bundle-\u{212a}",
            "parent-cafe\u{301}/bundle",
        ]
        .iter()
        .enumerate()
        {
            let out = holder.path().join(mode.to_string()).join(name);
            match gdl_gen::compose(&spec, None, &out, &target("gdl-gen-tests"), *sources) {
                Ok(_) => {}
                Err(error) => {
                    assert_ne!(case, 0, "the composed control must work: {error}");
                    assert!(!out.exists(), "refusal must precede writing");
                    assert!(error.to_string().starts_with("Compose output"), "{error}");
                    continue;
                }
            }
            let project = format!("gdl-review-unicode-{}-{mode}-{case}", std::process::id());
            let build = build_settings(&out, &project);
            if !build.status.success() {
                assert_ne!(
                    case,
                    0,
                    "the composed control must build:\n{}",
                    String::from_utf8_lossy(&build.stderr)
                );
                failures.push(format!(
                    "accepted {name:?} with {sources:?}, but Compose cannot build the bundle:\n{}",
                    String::from_utf8_lossy(&build.stderr)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_local_bundle_with_scp_syntax_builds_or_is_refused_before_writing() {
    let spec = gdl_gen::Spec::load(&root().join("examples/http/gdl.toml")).expect("the spec");
    for (case, prefix) in ["gdl-review-scp-", "git@gdl-review-scp:"]
        .iter()
        .enumerate()
    {
        let holder = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir_in(root())
            .expect("a directory in the context");
        let out = holder.path().join("bundle");
        match gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Local,
        ) {
            Ok(_) => {}
            Err(error) => {
                assert_ne!(case, 0, "the control bundle must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                assert!(error.to_string().contains("git@gdl-review-scp:"), "{error}");
                continue;
            }
        }
        let project = format!("gdl-review-scp-{}-{case}", std::process::id());
        let build = build_settings(&out, &project);
        assert!(
            build.status.success(),
            "accepted {:?}, but Docker reads the settings path as a Git source:\n{}",
            out.as_os_str(),
            String::from_utf8_lossy(&build.stderr)
        );
    }
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_local_bundle_with_leading_whitespace_builds_or_is_refused_before_writing() {
    let spec = gdl_gen::Spec::load(&root().join("examples/http/gdl.toml")).expect("the spec");
    let mut failures = Vec::new();
    for (case, prefix) in [
        "gdl-review-spaces-",
        " gdl-review-spaces-",
        "\u{a0}gdl-review-spaces-",
    ]
    .iter()
    .enumerate()
    {
        let holder = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir_in(root())
            .expect("a directory in the context");
        let out = holder.path().join("bundle");
        match gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Local,
        ) {
            Ok(_) => {}
            Err(error) => {
                assert_ne!(case, 0, "the control bundle must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                continue;
            }
        }
        let project = format!("gdl-review-spaces-{}-{case}", std::process::id());
        let build = build_settings(&out, &project);
        if !build.status.success() {
            assert_ne!(case, 0, "the control bundle must build");
            failures.push(format!(
                "accepted {:?}, but Docker cannot copy the settings:\n{}",
                out.as_os_str(),
                String::from_utf8_lossy(&build.stderr)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_local_bundle_with_a_caret_copies_its_settings_or_is_refused_before_writing() {
    let spec = gdl_gen::Spec::load(&root().join("examples/http/gdl.toml")).expect("the spec");
    let mut failures = Vec::new();
    for (location, parent) in ["", "target"].iter().enumerate() {
        for (case, name) in ["bundle[dev]", "bundle^caret", "bundle^caret[dev]"]
            .iter()
            .enumerate()
        {
            let holder = tempfile::Builder::new()
                .prefix(&format!("gdl-review-{name}-"))
                .tempdir_in(root().join(parent))
                .expect("a directory in the context");
            let out = holder.path().join("bundle");
            match gdl_gen::compose(
                &spec,
                None,
                &out,
                &target("gdl-gen-tests"),
                gdl_gen::Sources::Local,
            ) {
                Ok(_) => {}
                Err(error) => {
                    assert!(case > 1, "the control bundle must work: {error}");
                    assert!(!out.exists(), "refusal must precede writing");
                    continue;
                }
            }
            let project = format!("gdl-review-caret-{}-{location}-{case}", std::process::id());
            let copied = build_settings(&out, &project);
            if !copied.status.success() {
                assert!(
                    case > 1,
                    "the control bundle must copy its settings:\n{}",
                    String::from_utf8_lossy(&copied.stderr)
                );
                failures.push(format!(
                    "accepted {name:?} under {parent:?}, but its image does not hold the generated settings:\n{}",
                    String::from_utf8_lossy(&copied.stderr)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_local_bundle_with_yaml_characters_builds_or_is_refused_before_writing() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-review-yaml-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let spec = gdl_gen::Spec::load(&root().join("examples/http/gdl.toml")).expect("the spec");
    let mut failures = Vec::new();
    for (case, name) in [
        "bundle",
        "bundle\u{85}suffix",
        "bundle\u{fffe}suffix",
        "bundle\u{ffff}suffix",
    ]
    .iter()
    .enumerate()
    {
        let out = holder.path().join(name);
        if let Err(error) = gdl_gen::compose(
            &spec,
            None,
            &out,
            &target("gdl-gen-tests"),
            gdl_gen::Sources::Local,
        ) {
            assert_ne!(case, 0, "the control bundle must work: {error}");
            assert!(!out.exists(), "refusal must precede writing");
            assert!(
                error.to_string().starts_with("Compose output")
                    || error.to_string().starts_with("`--local`"),
                "{error}"
            );
            continue;
        }
        let project = format!("gdl-review-yaml-{}-{case}", std::process::id());
        let built = build_settings(&out, &project);
        if !built.status.success() {
            assert_ne!(case, 0, "the control bundle must build");
            failures.push(format!(
                "accepted {name:?}, but Compose changes or rejects its Dockerfile path:\n{}",
                String::from_utf8_lossy(&built.stderr)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
#[ignore = "F2: a 255-byte bundle basename makes Compose derive an invalid image repository name"]
fn a_long_bundle_basename_builds_or_is_refused_before_writing() {
    struct Image(String);
    impl Drop for Image {
        fn drop(&mut self) {
            let _ = Command::new("timeout")
                .args(["15", "docker", "image", "rm", "--force", &self.0])
                .output();
        }
    }

    let holder = tempfile::Builder::new()
        .prefix("gdl-review-length-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let prefix = format!("gdl-review-length-{}-", std::process::id());
    let names = [
        format!("{prefix}control"),
        format!("{prefix}{}", "a".repeat(255 - prefix.len())),
    ];
    let mut failures = Vec::new();
    for (mode, (path, sources)) in [
        ("examples/http/gdl.toml", gdl_gen::Sources::Local),
        ("examples/api/deploy.toml", gdl_gen::Sources::Fetched),
    ]
    .iter()
    .enumerate()
    {
        let spec = gdl_gen::Spec::load(&root().join(path)).expect("the spec");
        for (case, name) in names.iter().enumerate() {
            let out = holder.path().join(mode.to_string()).join(name);
            if let Err(error) =
                gdl_gen::compose(&spec, None, &out, &target("gdl-gen-tests"), *sources)
            {
                assert_ne!(case, 0, "the control bundle must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                assert!(error.to_string().starts_with("Compose output"), "{error}");
                continue;
            }
            let dockerfile = out.join("hub/Dockerfile");
            let generated = std::fs::read_to_string(&dockerfile).expect("the generated Dockerfile");
            let copy = generated
                .lines()
                .find(|line| line.starts_with("COPY ["))
                .expect("the settings copy");
            std::fs::write(&dockerfile, format!("FROM scratch\n{copy}\n"))
                .expect("the settings copy probe");
            let compose: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(out.join("compose.yaml")).expect("compose.yaml"),
            )
            .expect("the Compose configuration");
            let project = compose["name"].as_str().unwrap_or(name);
            let _image = Image(
                compose["services"]["hub"]["image"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("{project}-hub")),
            );
            let built = Command::new("timeout")
                .args(["30", "docker", "compose", "build", "hub"])
                .env_remove("COMPOSE_PROJECT_NAME")
                .current_dir(&out)
                .output()
                .expect("docker compose build");
            if !built.status.success() {
                assert_ne!(case, 0, "the control bundle must build");
                failures.push(format!(
                    "accepted a {}-byte bundle basename with {sources:?}, but its default image name cannot build:\n{}",
                    name.len(),
                    String::from_utf8_lossy(&built.stderr)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[cfg(unix)]
#[test]
#[ignore = "needs docker; run with --ignored"]
fn a_bundle_through_a_percent_symlink_runs_or_is_refused_before_writing() {
    let holder = tempfile::Builder::new()
        .prefix("gdl-review-symlink-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let mut failures = Vec::new();
    for (mode, (path, sources)) in [
        ("examples/http/gdl.toml", gdl_gen::Sources::Local),
        ("examples/api/deploy.toml", gdl_gen::Sources::Fetched),
    ]
    .iter()
    .enumerate()
    {
        let spec = gdl_gen::Spec::load(&root().join(path)).expect("the spec");
        let parent = holder.path().join(mode.to_string());
        std::fs::create_dir(&parent).expect("the source mode directory");
        for (case, name) in ["alias", "alias%"].iter().enumerate() {
            let physical = parent.join(format!("physical-{case}"));
            std::fs::create_dir(&physical).expect("the physical directory");
            let alias = parent.join(name);
            std::os::unix::fs::symlink(&physical, &alias).expect("the directory symlink");
            let out = alias.join("bundle");
            if let Err(error) =
                gdl_gen::compose(&spec, None, &out, &target("gdl-gen-tests"), *sources)
            {
                assert_ne!(case, 0, "the control bundle must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                assert!(error.to_string().starts_with("Compose output"), "{error}");
                assert!(error.to_string().contains('%'), "{error}");
                continue;
            }
            // A shell's `cd` preserves the symlink spelling in PWD, which Compose reads.
            let config = Command::new("timeout")
                .args(["15", "docker", "compose", "config", "--quiet"])
                .env("PWD", &out)
                .env_remove("COMPOSE_PROJECT_NAME")
                .current_dir(&out)
                .output()
                .expect("docker compose config");
            if !config.status.success() {
                assert_ne!(case, 0, "the control bundle must load");
                failures.push(format!(
                    "accepted output through {name:?} with {sources:?}, but Compose cannot load it after a shell cd:\n{}",
                    String::from_utf8_lossy(&config.stderr)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Compose accepts project separators that Docker refuses in the image
/// name it derives from them. No explicit project name masks that path.
#[test]
#[ignore = "F1: Compose derives an invalid image name from trailing or mixed underscores in a bundle name"]
fn a_bundle_with_project_separators_builds_or_is_refused_before_writing() {
    struct Image(String);
    impl Drop for Image {
        fn drop(&mut self) {
            let _ = Command::new("timeout")
                .args(["15", "docker", "image", "rm", "--force", &self.0])
                .output();
        }
    }

    let holder = tempfile::Builder::new()
        .prefix("gdl-review-separators-")
        .tempdir_in(root().join("target"))
        .expect("a directory in the context");
    let mut failures = Vec::new();
    for (mode, (path, sources)) in [
        ("examples/http/gdl.toml", gdl_gen::Sources::Local),
        ("examples/api/deploy.toml", gdl_gen::Sources::Fetched),
    ]
    .iter()
    .enumerate()
    {
        let spec = gdl_gen::Spec::load(&root().join(path)).expect("the spec");
        for (case, suffix) in [
            "control",
            "double__middle",
            "trailing_",
            "triple___middle",
            "mixed_-middle",
        ]
        .iter()
        .enumerate()
        {
            let name = format!(
                "gdl-review-separators-{}-{mode}-{suffix}",
                std::process::id()
            );
            let out = holder.path().join(&name);
            if let Err(error) =
                gdl_gen::compose(&spec, None, &out, &target("gdl-gen-tests"), *sources)
            {
                assert!(case > 1, "the control bundle must work: {error}");
                assert!(!out.exists(), "refusal must precede writing");
                assert!(error.to_string().starts_with("Compose output"), "{error}");
                continue;
            }
            let dockerfile = out.join("hub/Dockerfile");
            let generated = std::fs::read_to_string(&dockerfile).expect("the generated Dockerfile");
            let copy = generated
                .lines()
                .find(|line| line.starts_with("COPY ["))
                .expect("the settings copy");
            std::fs::write(&dockerfile, format!("FROM scratch\n{copy}\n"))
                .expect("the settings copy probe");
            let _image = Image(format!("{name}-hub"));
            let built = Command::new("timeout")
                .args(["30", "docker", "compose", "build", "hub"])
                .env_remove("COMPOSE_PROJECT_NAME")
                .current_dir(&out)
                .output()
                .expect("docker compose build");
            if !built.status.success() {
                assert!(
                    case > 1,
                    "the control bundle must build:\n{}",
                    String::from_utf8_lossy(&built.stderr)
                );
                failures.push(format!(
                    "accepted {suffix:?} with {sources:?}, but Compose derives an invalid image name:\n{}",
                    String::from_utf8_lossy(&built.stderr)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
