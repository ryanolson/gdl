//! `[deployment.codegen.<arch>]`: target features for the
//! segment images of one architecture. The k8s tests' fleet has a cluster
//! of `linux/amd64` and one of `linux/arm64`, so each segment has an image
//! of each.

use super::*;

/// The checks and flags that the build step of the api example's arm64
/// engine image runs before cargo, with [`LSE`].
fn lse_guard(mirror: &Mirror) -> String {
    let images = images(mirror, &with(mirror, LSE));
    let image = &images[&("api-engine".into(), "linux/arm64".into())];
    image
        .dockerfile()
        .split("RUN . /gdl/target.env && ")
        .nth(1)
        .expect("the build step")
        .split("cargo build ")
        .next()
        .expect("the guard")
        .to_string()
}

/// Nor can the build image bring fat LTO back through cargo's settings:
/// its `CARGO_PROFILE_RELEASE_LTO`, which the step sets to thin last; a
/// `[profile.release]` in its Cargo configuration, below that variable; or
/// rustflags in its configuration or environment (`build.rustflags`,
/// `target.<triple>.rustflags`, `CARGO_BUILD_RUSTFLAGS`,
/// `CARGO_TARGET_<TRIPLE>_RUSTFLAGS`), which cargo reads only when
/// `RUSTFLAGS` is unset, and the step sets it. Each is a real cargo build
/// of an empty crate after the step, about a second each: it gets
/// `-C lto=thin` and no `-C lto=fat`.
#[cfg(unix)]
#[test]
fn a_build_images_cargo_settings_cannot_restore_fat_lto() {
    let mirror = Mirror::new();
    let guard = lse_guard(&mirror);
    let rustc = std::process::Command::new("rustc")
        .arg("-vV")
        .output()
        .expect("rustc runs");
    let host = String::from_utf8_lossy(&rustc.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("host: ").map(str::to_string))
        .expect("the host triple");
    let host_env = host.to_uppercase().replace('-', "_");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    for (name, config, env) in [
        (
            "profile",
            "[profile.release]\nlto = \"fat\"\n".to_string(),
            None,
        ),
        (
            "build.rustflags",
            "[build]\nrustflags = [\"-Clto=fat\"]\n".to_string(),
            None,
        ),
        (
            "target.rustflags",
            format!("[target.{host}]\nrustflags = [\"-Clto=fat\"]\n"),
            None,
        ),
        (
            "CARGO_BUILD_RUSTFLAGS",
            String::new(),
            Some(("CARGO_BUILD_RUSTFLAGS".to_string(), "-Clto=fat")),
        ),
        (
            "CARGO_TARGET_RUSTFLAGS",
            String::new(),
            Some((format!("CARGO_TARGET_{host_env}_RUSTFLAGS"), "-Clto=fat")),
        ),
    ] {
        let dir = tempfile::Builder::new()
            .prefix("gdl-cargo-lto-")
            .tempdir()
            .expect("a directory");
        let krate = dir.path().join("crate");
        std::fs::create_dir_all(krate.join("src")).expect("a crate");
        std::fs::create_dir_all(krate.join(".cargo")).expect("a configuration");
        std::fs::write(
            krate.join("Cargo.toml"),
            "[package]\nname = \"probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n[workspace]\n",
        )
        .expect("a manifest");
        std::fs::write(krate.join("src/main.rs"), "fn main() {}\n").expect("a main");
        std::fs::write(krate.join(".cargo/config.toml"), &config).expect("a configuration");
        let mut command = std::process::Command::new("timeout");
        command
            .args(["120", "sh", "-c"])
            .arg(format!(
                "{guard}\"$GDL_CARGO\" build --release -v --offline"
            ))
            .current_dir(&krate)
            .env("GDL_CARGO", &cargo)
            .env("TARGETARCH", "arm64")
            .env("GDL_TARGET", "aarch64-unknown-linux-gnu")
            .env("CARGO_PROFILE_RELEASE_LTO", "fat")
            .env("CARGO_TARGET_DIR", dir.path().join("target"))
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .stdin(std::process::Stdio::null());
        if let Some((key, value)) = &env {
            command.env(key, value);
        }
        let output = command.output().expect("the build step and cargo run");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{name}: {stderr}");
        assert!(stderr.contains("-C lto=thin"), "{name}: {stderr}");
        assert!(!stderr.contains("lto=fat"), "{name}: {stderr}");
    }
}

/// A name with a second sign, or one that rustc's list holds only as a
/// pattern (`lse.` would match `lse2`), is refused by `check` or by the build
/// step, before rustc, which would only warn of it and build without it.
#[cfg(all(unix, target_arch = "aarch64"))]
#[test]
fn a_name_rustc_does_not_know_exactly_is_refused() {
    let mirror = Mirror::new();
    let guard = |table: &str| {
        let spec = with(&mirror, &format!("[deployment.codegen.arm64]\n{table}\n"));
        if gdl_gen::check(&spec, &mirror.target(), gdl_gen::Offers::Read).is_err() {
            return None;
        }
        let images = images(&mirror, &spec);
        let image = &images[&("api-engine".into(), "linux/arm64".into())];
        let guard = image
            .dockerfile()
            .split("RUN . /gdl/target.env && ")
            .nth(1)
            .expect("the build step")
            .split("cargo build ")
            .next()
            .expect("the guard");
        Some(
            std::process::Command::new("timeout")
                .args(["30", "sh", "-c"])
                .arg(format!(
                    "{guard}rustc --print cfg --target \"$GDL_TARGET\" $RUSTFLAGS"
                ))
                .env("TARGETARCH", "arm64")
                .env("GDL_TARGET", "aarch64-unknown-linux-gnu")
                .env("RUSTFLAGS", "")
                .env_remove("CARGO_ENCODED_RUSTFLAGS")
                .stdin(std::process::Stdio::null())
                .output()
                .expect("the generated build guard runs"),
        )
    };
    let good = guard("target_features = [\"+lse\"]").expect("a valid feature checks");
    assert!(good.status.success(), "{good:?}");
    assert!(String::from_utf8_lossy(&good.stdout).contains("target_feature=\"lse\""));
    let mut accepted = Vec::new();
    for table in [
        "target_features = [\"+-lse\"]",
        "target_features = [\"--lse\"]",
        "target_features = [\"+lse.\"]",
    ] {
        if let Some(output) = guard(table)
            && output.status.success()
        {
            accepted.push(format!(
                "{table}: exit={:?}, lse={}, {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stdout).contains("target_feature=\"lse\""),
                String::from_utf8_lossy(&output.stderr)
                    .lines()
                    .next()
                    .unwrap_or("")
            ));
        }
    }
    assert!(
        accepted.is_empty(),
        "unknown names pass the guard:\n{}",
        accepted.join("\n")
    );
}

/// An opted-in build refuses any `RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS`
/// the build image sets, whatever it holds, with one line that gives the
/// variable and its value, and then builds with exactly the table's flags.
/// A list of the flags that set LTO cannot be complete: rustc takes
/// `-C`, `--codegen`, and `--codegen=` forms, `_` for `-` in a key, and
/// options from an `@file`, and each of them once passed such a list. A
/// build with fat LTO runs `+lse` in std's start-up, and an Armv8.0 core
/// faults before the start check (`scripts/qemu-startcheck.sh` shows it).
#[cfg(unix)]
#[test]
fn an_opted_in_build_refuses_any_inherited_rustflags() {
    let mirror = Mirror::new();
    let guard = lse_guard(&mirror);
    let dir = tempfile::Builder::new()
        .prefix("gdl-inherited-rustflags-")
        .tempdir()
        .expect("a directory");
    let file = dir.path().join("flags");
    std::fs::write(&file, "lto=fat\n").expect("an argument file");
    let file = file.display();
    // Each variable as the build image sets it: `None` leaves it unset.
    let run = |rustflags: Option<&str>, encoded: Option<&str>| {
        let mut command = std::process::Command::new("timeout");
        command
            .args(["30", "sh", "-c"])
            .arg(format!(
                "{guard}printf '%s|%s|%s' \"$CARGO_PROFILE_RELEASE_LTO\" \"$RUSTFLAGS\" \"${{CARGO_ENCODED_RUSTFLAGS-unset}}\""
            ))
            .env("TARGETARCH", "arm64")
            .env("GDL_TARGET", "aarch64-unknown-linux-gnu")
            // The build image's own profile setting, which the step replaces.
            .env("CARGO_PROFILE_RELEASE_LTO", "fat")
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .stdin(std::process::Stdio::null());
        if let Some(value) = rustflags {
            command.env("RUSTFLAGS", value);
        }
        if let Some(value) = encoded {
            command.env("CARGO_ENCODED_RUSTFLAGS", value);
        }
        command.output().expect("the build step's guard runs")
    };
    let refusal = |variable: &str, value: &str| {
        format!(
            "segment engine: the build image sets {variable}={value}, which this build step cannot read safely: an image with [deployment.codegen.arm64] builds with exactly the table's flags\n"
        )
    };
    for rustflags in [
        "-Clto=fat".to_string(),
        "-C lto=fat".into(),
        "--codegen lto=fat".into(),
        "--codegen=lto=fat".into(),
        format!("@{file}"),
        format!("-C @{file}"),
        format!("--codegen @{file}"),
        "-C embed_bitcode=no".into(),
        "-C linker_plugin_lto".into(),
        "-C linker-plugin_lto=yes".into(),
        "-C".into(),
        // Harmless flags too: the step does not read them.
        "--cfg rustix_use_libc".into(),
        " ".into(),
    ] {
        let output = run(Some(&rustflags), None);
        assert!(!output.status.success(), "{rustflags}: {output:?}");
        assert_eq!(
            String::from_utf8_lossy(&output.stderr),
            refusal("RUSTFLAGS", &rustflags),
            "{rustflags}"
        );
    }
    for encoded in ["-Clto=fat", "--cfg\u{1f}x"] {
        let output = run(None, Some(encoded));
        assert!(!output.status.success(), "{encoded}: {output:?}");
        assert_eq!(
            String::from_utf8_lossy(&output.stderr),
            refusal("CARGO_ENCODED_RUSTFLAGS", encoded),
            "{encoded}"
        );
    }
    // Unset or empty passes. An empty `CARGO_ENCODED_RUSTFLAGS` is unset,
    // since cargo would read it in place of `RUSTFLAGS`. The flags are the
    // table's alone, and the profile's LTO is thin.
    for (rustflags, encoded) in [
        (None, None),
        (Some(""), None),
        (None, Some("")),
        (Some(""), Some("")),
    ] {
        let output = run(rustflags, encoded);
        assert!(
            output.status.success(),
            "{rustflags:?} {encoded:?}: {output:?}"
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            "thin|-Ctarget-feature=+lse|unset",
            "{rustflags:?} {encoded:?}"
        );
    }
}

/// A table for arm64 only.
const LSE: &str = "[deployment.codegen.arm64]\ntarget_features = [\"+lse\"]\n";

/// The k8s tests' spec with `codegen` after its `[deployment]` tables.
fn with(mirror: &Mirror, codegen: &str) -> gdl_gen::Spec {
    placed(mirror, &format!("{SPLIT}{}{codegen}", deployment()))
}

/// One image of a run: its repository, platform, tag, and build context.
struct Image {
    repository: String,
    platform: String,
    tag: String,
    context: Context,
}

/// Every image `images` builds for `spec`, by its repository and platform.
fn images(mirror: &Mirror, spec: &gdl_gen::Spec) -> BTreeMap<(String, String), Image> {
    let mut docker = Fake::default();
    run(mirror, spec, &fleet(&fleet_text()), true, &mut docker);
    docker
        .builds()
        .into_iter()
        .map(|(reference, platform, _)| {
            let (_, repository, tag) = split(reference);
            let image = Image {
                repository: repository.to_string(),
                platform: platform.to_string(),
                tag: tag.to_string(),
                context: docker.contexts[reference].clone(),
            };
            ((image.repository.clone(), image.platform.clone()), image)
        })
        .collect()
}

impl Image {
    fn dockerfile(&self) -> &str {
        &self.context[Path::new("Dockerfile")]
    }

    /// The segment it runs, or `None` for the hub's.
    fn segment(&self) -> Option<&str> {
        self.repository
            .strip_prefix("api-")
            .filter(|segment| *segment != "hub")
    }

    fn main_rs(&self) -> &str {
        let segment = self.segment().expect("a segment's image");
        &self.context[&Path::new(segment).join("src/main.rs")]
    }
}

/// A segment's Dockerfile with no `[deployment.codegen]` table, as it was
/// before the table existed: `<segment>` and `<package>` stand for its
/// segment and package.
const PINNED: &str = "# @generated by gdl-gen; do not edit\n\
FROM --platform=$BUILDPLATFORM rust:1.93.1-slim-bookworm AS build\n\
ARG TARGETARCH\n\
RUN <toolchain>\n\
ENV CARGO_TARGET_DIR=/build\n\
RUN . /gdl/target.env && cargo build --release --target \"$GDL_TARGET\" --manifest-path '/app/<segment>/Cargo.toml' && cp \"$CARGO_TARGET_DIR/$GDL_TARGET/release/<package>\" /gdl/segment\n\
FROM gcr.io/distroless/cc-debian12:nonroot\n\
COPY --from=build /gdl/segment /usr/local/bin/segment\n\
ENTRYPOINT [\"/usr/local/bin/segment\"]\n";

/// The toolchain step of a Dockerfile, which the unit tests of `images`
/// run, as `<toolchain>`.
fn without_toolchain(dockerfile: &str) -> String {
    let start = dockerfile.find("RUN set -eu;").expect("a toolchain step") + 4;
    let end = start + dockerfile[start..].find("\nENV ").expect("the step's end");
    format!("{}<toolchain>{}", &dockerfile[..start], &dockerfile[end..])
}

/// With no table, each segment's Dockerfile is the one before the table
/// existed, its binary does not check its CPU, and its image is the same
/// for both platforms but for the platform itself.
#[test]
fn without_codegen_the_image_build_is_as_before() {
    let mirror = Mirror::new();
    let images = images(&mirror, &spec(&mirror));
    assert_eq!(images.len(), 6);
    for image in images.values() {
        let Some(segment) = image.segment() else {
            continue;
        };
        let pinned = PINNED
            .replace("<segment>", segment)
            .replace("<package>", &image.repository);
        assert_eq!(
            without_toolchain(image.dockerfile()),
            pinned,
            "{}",
            image.repository
        );
        assert!(
            !image.main_rs().contains("check_cpu_features"),
            "{}",
            image.repository
        );
        let other = if image.platform == "linux/amd64" {
            "linux/arm64"
        } else {
            "linux/amd64"
        };
        let twin = &images[&(image.repository.clone(), other.to_string())];
        assert_eq!(image.context, twin.context, "{}", image.repository);
    }
}

/// A table for arm64 reaches the arm64 segment images only: their build
/// checks the image's architecture and adds the features to `RUSTFLAGS`.
/// The amd64 images build as without the table, and the hub's as before.
/// Every segment binary checks its CPU first in `main`, since one crate
/// serves both platforms.
#[test]
fn codegen_for_arm64_reaches_arm64_images_only() {
    let mirror = Mirror::new();
    let before = images(&mirror, &spec(&mirror));
    let after = images(&mirror, &with(&mirror, LSE));
    assert_eq!(after.len(), 6);
    for (key, image) in &after {
        let was = &before[key];
        let Some(segment) = image.segment() else {
            assert_eq!(image.context, was.context, "{key:?}");
            assert_eq!(image.tag, was.tag, "{key:?}");
            continue;
        };
        let dockerfile = image.dockerfile();
        if image.platform == "linux/arm64" {
            for part in [
                "{ [ \"$TARGETARCH\" = arm64 ] || ",
                "[deployment.codegen.arm64]",
                "grep -qxF -- lse ",
                "export CARGO_PROFILE_RELEASE_LTO=thin && \\\n    ",
                "export RUSTFLAGS=\"-Ctarget-feature=+lse\" && \\\n    cargo build ",
            ] {
                assert!(
                    dockerfile.contains(part),
                    "{key:?}: {part} in\n{dockerfile}"
                );
            }
        } else {
            assert_eq!(dockerfile, was.dockerfile(), "{key:?}");
            assert!(!dockerfile.contains("RUSTFLAGS"), "{key:?}");
            assert!(!dockerfile.contains("LTO"), "{key:?}");
        }
        let main = image.main_rs();
        let body = main
            .split("fn main() -> std::process::ExitCode {\n")
            .nth(1)
            .expect("a main");
        let first = body
            .lines()
            .find(|line| !line.trim_start().starts_with("//"))
            .expect("a statement");
        assert_eq!(
            first,
            format!("    if let Err(code) = gdl_runtime::check_cpu_features({segment:?}) {{"),
            "{key:?}:\n{main}"
        );
    }
}

/// A table moves every segment's tag: every segment binary then checks
/// its CPU, and the arm64 images build with the feature. The hub's tags
/// do not move.
#[test]
fn a_tag_moves_with_its_architectures_setting() {
    let mirror = Mirror::new();
    let tags = |codegen: &str| -> BTreeMap<(String, String), String> {
        let spec = if codegen.is_empty() {
            spec(&mirror)
        } else {
            with(&mirror, codegen)
        };
        images(&mirror, &spec)
            .into_iter()
            .map(|(key, image)| (key, image.tag))
            .collect()
    };
    let none = tags("");
    let lse = tags(LSE);
    for key in none.keys() {
        let (repository, _) = key;
        if repository == "api-hub" {
            assert_eq!(none[key], lse[key], "{key:?}");
        } else {
            assert_ne!(none[key], lse[key], "{key:?}");
        }
    }
}

/// A feature that `scripts/qemu-startcheck.sh` does not prove is refused,
/// by `check` and by `images`, with one line that names it. Built with
/// `+avx2` and thin LTO, std's `lang_start_internal` runs `vmovaps` before
/// its first call, so a core without AVX faults before the start check:
/// only arm64 `+lse` is proven.
#[test]
fn a_feature_not_proven_to_reach_the_start_check_is_refused() {
    let mirror = Mirror::new();
    for (table, feature) in [
        ("amd64", "+avx2"),
        ("arm64", "+crc"),
        ("arm64", "-lse"),
        ("amd64", "+lse"),
    ] {
        let codegen = format!("[deployment.codegen.{table}]\ntarget_features = [\"{feature}\"]\n");
        let spec = with(&mirror, &codegen);
        let line = format!(
            "[deployment.codegen.{table}]: target feature `{feature}` is not proven to reach the start check: a core without it could fault before the binary checks its CPU. Only `+lse` for arm64 is, by `scripts/qemu-startcheck.sh` (docs/src/development/codegen-design.md)"
        );
        let checked = gdl_gen::check(&spec, &mirror.target(), gdl_gen::Offers::Read)
            .map(drop)
            .expect_err(&codegen)
            .to_string();
        assert!(checked.contains(&line), "{codegen}: {checked}");
        let built = gdl_gen::images(
            &spec,
            &fleet(&fleet_text()),
            &gdl_gen::Dgds::default(),
            &mirror.target(),
            false,
            gdl_gen::Sources::Fetched,
            &mut Fake::default(),
        )
        .map(drop)
        .expect_err(&codegen)
        .to_string();
        assert!(built.contains(&line), "{codegen}: {built}");
    }
}

/// `compose` and `export` build for the Docker host, not for a cluster's
/// platform: each refuses a spec with a table, and names `cargo gdl
/// images`, which builds with it.
#[test]
fn compose_and_export_refuse_a_codegen_table() {
    let mirror = Mirror::new();
    let spec = with(&mirror, LSE);
    let out = tempfile::Builder::new()
        .prefix("gdl-codegen-compose-")
        .tempdir()
        .expect("a directory");
    let refusals = [
        gdl_gen::compose_files(
            &spec,
            None,
            &out.path().join("deployment"),
            &mirror.target(),
            gdl_gen::Sources::Fetched,
        )
        .map(|_| ())
        .expect_err("compose refuses it"),
        gdl_gen::compose_bundle(&spec, &mirror.target())
            .map(|_| ())
            .expect_err("the composer's preview refuses it"),
        gdl_gen::export(&spec, &mirror.target(), gdl_gen::Offers::Read)
            .map(|_| ())
            .expect_err("export refuses it"),
    ];
    for refusal in refusals {
        let text = refusal.to_string();
        for part in ["`[deployment.codegen.arm64]`", "`cargo gdl images`"] {
            assert!(text.contains(part), "{part} in {text}");
        }
    }
}

/// `cargo gdl check` refuses a table that `images` could not build with:
/// an architecture `images` does not build for, a table that sets
/// nothing, a name that is not one, and a key the form does not have,
/// such as `target_cpu`. Each refusal names its table. A good table
/// checks.
#[test]
fn check_refuses_a_table_images_cannot_build_with() {
    let mirror = Mirror::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = std::fs::read_to_string(root.join("examples/api/api.gdl")).expect("graph");
    let text = mirror.portable(&text).replace(SEGMENTS, SPLIT);
    let check = |codegen: &str| {
        gdl_gen::Spec::from_text(
            &format!("{text}{codegen}"),
            &graph,
            &root.join("examples/api"),
        )
        .and_then(|spec| gdl_gen::check(&spec, &mirror.target(), gdl_gen::Offers::Read))
        .map(drop)
    };
    check(LSE).expect("a good table checks");
    for (codegen, expected) in [
        (
            "[deployment.codegen.aarch64]\ntarget_features = [\"+lse\"]\n",
            "[deployment.codegen.aarch64]: `aarch64` is not an architecture `cargo gdl images` builds for: name `amd64` or `arm64` as a cluster's `platform` names them (`aarch64` is `arm64` there)",
        ),
        (
            "[deployment.codegen.riscv64]\ntarget_features = [\"+lse\"]\n",
            "[deployment.codegen.riscv64]: `riscv64` is not an architecture",
        ),
        (
            "[deployment.codegen.arm64]\n",
            "[deployment.codegen.arm64]: the table sets no `target_features`",
        ),
        (
            "[deployment.codegen.arm64]\ntarget_features = [\"lse\"]\n",
            "[deployment.codegen.arm64]: target feature `lse` is not one `+` or `-` and a name",
        ),
        (
            "[deployment.codegen.arm64]\ntarget_features = [\"+lse,+crc\"]\n",
            "[deployment.codegen.arm64]: target feature `+lse,+crc` is not",
        ),
        (
            "[deployment.codegen.arm64]\ntarget_features = [\"+lse\"]\ntarget_cpu = \"neoverse-n1\"\n",
            "[deployment.codegen.arm64] target_cpu: unknown field `target_cpu`, expected `target_features`",
        ),
        (
            "[deployment.codegen.arm64]\ntarget_feature = [\"+lse\"]\n",
            "unknown field `target_feature`",
        ),
    ] {
        let error = check(codegen).expect_err(codegen).to_string();
        assert!(error.contains(expected), "{codegen}: {error}");
    }
}
