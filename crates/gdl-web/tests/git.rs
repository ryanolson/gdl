//! Exercise the Git import over a local smart-HTTP server, without external services.

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    response::Response,
};
use http_body_util::BodyExt;
use serde_json::json;
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tower::ServiceExt;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(dir)
        .args([
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .output()
        .expect("git");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git output")
        .trim()
        .into()
}

async fn backend(State(root): State<PathBuf>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = body.collect().await.expect("request body").to_bytes();
    let bytes = tokio::task::spawn_blocking(move || {
        let mut child = Command::new("git")
            .arg("http-backend")
            .env("GIT_PROJECT_ROOT", root)
            .env("GIT_HTTP_EXPORT_ALL", "1")
            .env("PATH_INFO", parts.uri.path())
            .env("QUERY_STRING", parts.uri.query().unwrap_or(""))
            .env("REQUEST_METHOD", parts.method.as_str())
            .env(
                "CONTENT_TYPE",
                parts
                    .headers
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or(""),
            )
            .env("CONTENT_LENGTH", body.len().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("git backend");
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(&body)
            .expect("write body");
        let output = child.wait_with_output().expect("git response");
        assert!(output.status.success());
        output.stdout
    })
    .await
    .expect("backend thread");
    let split = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("CGI headers");
    let mut response = Response::builder();
    for line in std::str::from_utf8(&bytes[..split])
        .expect("headers")
        .lines()
    {
        let (name, value) = line.split_once(':').expect("header");
        if name.eq_ignore_ascii_case("status") {
            response = response.status(
                value
                    .trim()
                    .split(' ')
                    .next()
                    .expect("status")
                    .parse::<u16>()
                    .expect("status number"),
            );
        } else {
            response = response.header(name, value.trim());
        }
    }
    response
        .body(Body::from(bytes[split + 4..].to_vec()))
        .expect("response")
}

#[tokio::test]
async fn import_a_branch_and_subdirectory_over_http_pins_the_commit() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let work = temp.path().join("work");
    std::fs::create_dir_all(work.join("protocols/src")).expect("package");
    std::fs::write(
        work.join("Cargo.toml"),
        "[workspace]\nmembers=['protocols']\nresolver='3'\n",
    )
    .expect("workspace");
    std::fs::write(
        work.join("protocols/Cargo.toml"),
        "[package]\nname='protocols'\nversion='0.1.0'\nedition='2024'\n",
    )
    .expect("manifest");
    std::fs::write(
        work.join("protocols/src/lib.rs"),
        "#[derive(gdl_message::Message)]\npub struct Prompt;\n",
    )
    .expect("source");
    git(&work, &["init", "--quiet", "--initial-branch=demo"]);
    git(&work, &["add", "."]);
    git(&work, &["commit", "--quiet", "-m", "fixture"]);
    let commit = git(&work, &["rev-parse", "HEAD"]);
    git(
        temp.path(),
        &["clone", "--quiet", "--bare", "work", "repo.git"],
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let url = format!(
        "http://{}/repo.git",
        listener.local_addr().expect("address")
    );
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(backend)
                .with_state(temp.path().to_path_buf()),
        )
        .into_future(),
    );
    let app = gdl_web::router(gdl_web::Config {
        catalog: temp.path().join("catalog"),
        target: temp.path().join("target"),
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        gdl: gdl_web::Gdl::Path(Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl")),
    })
    .expect("composer");
    let request = Request::builder()
        .method("POST")
        .uri("/api/catalog")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"source":"git","url":url,"revision":"demo","path":"protocols"}).to_string(),
        ))
        .expect("request");
    let response = tokio::time::timeout(std::time::Duration::from_secs(15), app.oneshot(request))
        .await
        .expect("import deadline")
        .expect("response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let catalog = gdl_extract::Catalog::load(&temp.path().join("catalog")).expect("catalog");
    let entry = catalog.entries().next().expect("entry");
    assert_eq!(
        entry.offer.source,
        format!("git+{url}?rev={commit}#{commit}")
    );
    assert_eq!(entry.offer.messages[0].name, "Prompt");
    server.abort();
}

/// A repository, served over HTTP, with `packages` written by `write`,
/// and the composer's router over a catalogue in `temp`.
async fn served(
    temp: &Path,
    write: impl FnOnce(&Path),
) -> (
    String,
    tokio::task::JoinHandle<std::io::Result<()>>,
    axum::Router,
) {
    let work = temp.join("work");
    std::fs::create_dir_all(&work).expect("work");
    write(&work);
    git(&work, &["init", "--quiet", "--initial-branch=demo"]);
    git(&work, &["add", "."]);
    git(&work, &["commit", "--quiet", "-m", "fixture"]);
    git(temp, &["clone", "--quiet", "--bare", "work", "repo.git"]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let url = format!(
        "http://{}/repo.git",
        listener.local_addr().expect("address")
    );
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(backend)
                .with_state(temp.to_path_buf()),
        )
        .into_future(),
    );
    let app = gdl_web::router(gdl_web::Config {
        catalog: temp.join("catalog"),
        target: temp.join("target"),
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        gdl: gdl_web::Gdl::Path(Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl")),
    })
    .expect("composer");
    (url, server, app)
}

/// One package directory with a `Message` named `message`.
fn package(dir: &Path, name: &str, message: &str, extra_manifest: &str) {
    std::fs::create_dir_all(dir.join("src")).expect("package");
    std::fs::write(
        dir.join("Cargo.toml"),
        format!("[package]\nname='{name}'\nversion='0.1.0'\nedition='2024'\n[workspace]\n{extra_manifest}"),
    )
    .expect("manifest");
    std::fs::write(
        dir.join("src/lib.rs"),
        format!("#[derive(gdl_message::Message)]\npub struct {message};\n"),
    )
    .expect("source");
}

async fn import(app: axum::Router, url: &str, path: &str) -> (u16, String) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/catalog")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"source":"git","url":url,"revision":"demo","path":path}).to_string(),
        ))
        .expect("request");
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), app.oneshot(request))
        .await
        .expect("import deadline")
        .expect("response");
    let status = response.status().as_u16();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Cargo resolves a Git package by name, so a build links whichever
/// package of that name it finds first; an import of the other directory
/// would put one package in the catalogue and another in the build. It is
/// refused, naming both.
#[tokio::test]
async fn an_import_of_a_namesake_cargo_does_not_resolve_is_refused() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("a/protocols"), "protocols", "FromA", "");
        package(&work.join("b/protocols"), "protocols", "FromB", "");
    })
    .await;
    let (status, body) = import(app.clone(), &url, "b/protocols").await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body.contains("more than one package named `protocols`") && body.contains("a/protocols"),
        "{body}"
    );
    // The one cargo resolves imports, and is the one a build would link.
    let (status, body) = import(app, &url, "a/protocols").await;
    assert_eq!(status, 200, "{body}");
    let catalog = gdl_extract::Catalog::load(&temp.path().join("catalog")).expect("catalog");
    let names: Vec<_> = catalog
        .entries()
        .flat_map(|e| e.offer.messages.iter().map(|m| m.name.clone()))
        .collect();
    assert_eq!(names, ["FromA"]);
    server.abort();
}

/// Everything a Git import records comes from the Git source: the
/// package's own source, and the origin of what it re-exports from a
/// sibling package in the repository. A path from a checkout would name a
/// directory that is gone.
#[tokio::test]
async fn a_git_import_records_git_origins_throughout() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("protocols"), "protocols", "Words", "");
        std::fs::create_dir_all(work.join("facade/src")).expect("facade");
        std::fs::write(
            work.join("facade/Cargo.toml"),
            "[package]\nname='facade'\nversion='0.1.0'\nedition='2024'\n[workspace]\n[dependencies]\nprotocols = { path = \"../protocols\" }\n",
        )
        .expect("manifest");
        std::fs::write(
            work.join("facade/src/lib.rs"),
            "#[gdl_message::export]\npub use protocols::Words;\n",
        )
        .expect("source");
    })
    .await;
    let (status, body) = import(app, &url, "facade").await;
    assert_eq!(status, 200, "{body}");
    let catalog = gdl_extract::Catalog::load(&temp.path().join("catalog")).expect("catalog");
    let entry = catalog.entries().next().expect("entry");
    assert!(
        entry.offer.source.starts_with("git+"),
        "{}",
        entry.offer.source
    );
    let export = entry.offer.exports.first().expect("the export");
    assert_eq!(export.name, "Words");
    assert!(
        export.origin.source.starts_with(&format!("git+{url}")),
        "{}",
        export.origin.source
    );
    server.abort();
}

/// Cargo runs from the server's scratch directory, never from the
/// checkout, so the repository's `rust-toolchain.toml` cannot choose the
/// `cargo` that reads it, and its `.cargo/config.toml` does not apply. The
/// config here is not even TOML: a cargo that read it would fail, and so
/// would the import.
#[tokio::test]
async fn a_repository_cannot_choose_the_cargo_that_reads_it() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let fake = temp.path().join("fake");
    let marker = temp.path().join("marker");
    std::fs::create_dir_all(fake.join("bin")).expect("fake toolchain");
    for tool in ["cargo", "rustc"] {
        let script = fake.join("bin").join(tool);
        std::fs::write(
            &script,
            format!("#!/bin/sh\necho ran > {}\nexit 1\n", marker.display()),
        )
        .expect("script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
                .expect("executable");
        }
    }
    let toolchain = format!("[toolchain]\npath = {:?}\n", fake.display());
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("protocols"), "protocols", "Prompt", "");
        std::fs::write(work.join("rust-toolchain.toml"), &toolchain).expect("toolchain");
        std::fs::write(work.join("protocols/rust-toolchain.toml"), &toolchain).expect("toolchain");
        for dir in [work.to_path_buf(), work.join("protocols")] {
            std::fs::create_dir_all(dir.join(".cargo")).expect("config dir");
            std::fs::write(dir.join(".cargo/config.toml"), "not = = toml\n").expect("config");
        }
    })
    .await;
    let (status, body) = import(app, &url, "protocols").await;
    assert_eq!(status, 200, "{body}");
    assert!(!marker.exists(), "the repository's toolchain ran");
    server.abort();
}

/// The package's own Git dependencies are fetched by cargo under the same
/// rule as the package: HTTP(S) only. Another transport fails, and the
/// import says so.
#[tokio::test]
async fn a_dependency_over_another_transport_fails_the_import() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let secret = temp.path().join("secret");
    package(&secret, "secret", "Hidden", "");
    git(&secret, &["init", "--quiet", "--initial-branch=main"]);
    git(&secret, &["add", "."]);
    git(&secret, &["commit", "--quiet", "-m", "secret"]);
    let dependency = format!(
        "[dependencies]\nsecret = {{ git = \"file://{}\" }}\n",
        secret.display()
    );
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("protocols"), "protocols", "Prompt", &dependency);
    })
    .await;
    let (status, body) = import(app, &url, "protocols").await;
    assert_eq!(status, 422, "{body}");
    assert!(body.contains("secret"), "{body}");
    server.abort();
}

/// A fetched package may not reach outside the repository: a path
/// dependency naming a directory on this machine would be read here and
/// fail to build anywhere else, and so would a `#[path]` module.
#[tokio::test]
async fn a_git_package_cannot_reach_outside_its_repository() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let outside = temp.path().join("outside");
    package(&outside, "secret", "Hidden", "");
    std::fs::write(outside.join("leak.rs"), "pub struct Leaked;\n").expect("leak");
    let dependency = format!(
        "[dependencies]\nsecret = {{ path = {:?} }}\n",
        outside.display()
    );
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("dependent"), "dependent", "Prompt", &dependency);
        package(&work.join("including"), "including", "Prompt", "");
        std::fs::write(
            work.join("including/src/lib.rs"),
            format!(
                "#[path = {:?}]\npub mod leak;\n#[derive(gdl_message::Message)]\npub struct Prompt;\n",
                outside.join("leak.rs").display()
            ),
        )
        .expect("source");
    })
    .await;
    let (status, body) = import(app.clone(), &url, "dependent").await;
    assert_eq!(status, 422, "{body}");
    assert!(body.contains("not a fetched package"), "{body}");
    let (status, body) = import(app, &url, "including").await;
    assert_eq!(status, 422, "{body}");
    assert!(body.contains("outside the package's directory"), "{body}");
    server.abort();
}

/// The manifest is read from the fetched objects: a symlink there is its
/// link text, refused by its mode rather than followed to whatever it
/// names on this machine.
#[tokio::test]
async fn a_symlinked_manifest_is_refused() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let outside = temp.path().join("outside.toml");
    std::fs::write(
        &outside,
        "[package]\nname='secret-name-from-outside'\nversion='0.0.0'\nedition='2024'\n",
    )
    .expect("outside");
    let (url, server, app) = served(temp.path(), |work| {
        std::fs::create_dir_all(work.join("linked/src")).expect("package");
        std::fs::write(work.join("linked/src/lib.rs"), "").expect("source");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, work.join("linked/Cargo.toml")).expect("symlink");
    })
    .await;
    let (status, body) = import(app, &url, "linked").await;
    assert_eq!(status, 422, "{body}");
    assert!(body.contains("not a regular file"), "{body}");
    server.abort();
}

/// Both binaries default to a relative target directory; cargo reports
/// absolute ones, and the two must compare as one.
#[tokio::test]
async fn a_relative_target_directory_imports() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let (url, server, _) = served(temp.path(), |work| {
        package(&work.join("protocols"), "protocols", "Prompt", "");
    })
    .await;
    let relative = PathBuf::from(format!("target-relative-{}", std::process::id()));
    let app = gdl_web::router(gdl_web::Config {
        catalog: temp.path().join("catalog"),
        target: relative.clone(),
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        gdl: gdl_web::Gdl::Path(Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl")),
    })
    .expect("composer");
    let (status, body) = import(app, &url, "protocols").await;
    let _ = std::fs::remove_dir_all(&relative);
    assert_eq!(status, 200, "{body}");
    server.abort();
}

/// A repository's packages may share its files and depend on each other
/// by path, wherever in the repository they are, even under a directory
/// named `registry`.
#[tokio::test]
async fn a_repository_may_share_files_among_its_packages() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("crates/registry/shared"), "shared", "Words", "");
        std::fs::write(work.join("crates/registry/common.rs"), "pub struct Common;\n").expect("shared file");
        package(
            &work.join("crates/registry/client"),
            "client",
            "Prompt",
            "[dependencies]\nshared = { path = \"../shared\" }\n",
        );
        std::fs::write(
            work.join("crates/registry/client/src/lib.rs"),
            "#[path = \"../../common.rs\"]\npub mod common;\n#[gdl_message::export]\npub use shared::Words;\n#[derive(gdl_message::Message)]\npub struct Prompt;\n",
        )
        .expect("source");
    })
    .await;
    let (status, body) = import(app, &url, "crates/registry/client").await;
    assert_eq!(status, 200, "{body}");
    server.abort();
}

/// A manifest is read only up to a megabyte: one past that is refused
/// before it is read.
#[tokio::test]
async fn a_manifest_over_the_limit_is_refused() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("protocols"), "protocols", "Prompt", "");
        let mut manifest =
            std::fs::read_to_string(work.join("protocols/Cargo.toml")).expect("manifest");
        manifest.push_str("# ");
        manifest.push_str(&"x".repeat(1 << 20));
        manifest.push('\n');
        std::fs::write(work.join("protocols/Cargo.toml"), manifest).expect("manifest");
    })
    .await;
    let (status, body) = import(app, &url, "protocols").await;
    assert_eq!(status, 422, "{body}");
    assert!(body.contains("larger than 1 MiB"), "{body}");
    server.abort();
}

/// A registry dependency is accepted wherever cargo keeps it: an operator
/// who replaces crates.io with a vendored directory still imports Git
/// packages that depend on the registry.
#[tokio::test]
async fn a_vendored_registry_dependency_is_a_fetched_package() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let registry = Path::new(env!("CARGO_MANIFEST_DIR")).join("../gdl-gen/tests/fixtures/registry");
    std::fs::create_dir_all(temp.path().join(".cargo")).expect("config dir");
    std::fs::write(
        temp.path().join(".cargo/config.toml"),
        format!(
            "[source.crates-io]\nreplace-with = \"fixture\"\n\n[source.fixture]\ndirectory = {:?}\n",
            registry.canonicalize().expect("registry").display()
        ),
    )
    .expect("config");
    let (url, server, app) = served(temp.path(), |work| {
        package(
            &work.join("dependent"),
            "dependent",
            "Prompt",
            "[dependencies]\nregproto = \"0.1.0\"\n",
        );
    })
    .await;
    let (status, body) = import(app, &url, "dependent").await;
    assert_eq!(status, 200, "{body}");
    server.abort();
}

/// A Git package's path dependency into another checkout is judged by
/// provenance: the packages of one Git source must share one checkout
/// root, and a dependency elsewhere shows a second.
#[tokio::test]
async fn a_git_package_cannot_reach_another_checkout() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let other = temp
        .path()
        .join("elsewhere/git/checkouts/other-0123456789abcdef/abcdef0");
    std::fs::create_dir_all(&other).expect("other");
    std::fs::write(other.join(".cargo-ok"), "{\"v\":1}").expect("ok");
    package(&other.join("secret"), "secret", "Hidden", "");
    let dependency = format!(
        "[dependencies]\nsecret = {{ path = {:?} }}\n",
        other.join("secret").display()
    );
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("dependent"), "dependent", "Prompt", &dependency);
    })
    .await;
    let (status, body) = import(app, &url, "dependent").await;
    assert_eq!(status, 422, "{body}");
    assert!(body.contains("checkouts"), "{body}");
    server.abort();
}

/// A directory cargo marks complete under `registry/src` is not a Git
/// checkout, whatever a path dependency into it claims.
#[tokio::test]
async fn a_git_package_cannot_reach_a_registry_directory() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let other = temp
        .path()
        .join("elsewhere/registry/src/index-0123/secret-0.1.0");
    package(&other, "secret", "Hidden", "");
    std::fs::write(other.join(".cargo-ok"), "{\"v\":1}").expect("ok");
    let dependency = format!(
        "[dependencies]\nsecret = {{ path = {:?} }}\n",
        other.display()
    );
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("dependent"), "dependent", "Prompt", &dependency);
    })
    .await;
    let (status, body) = import(app, &url, "dependent").await;
    assert_eq!(status, 422, "{body}");
    assert!(body.contains("not a fetched package"), "{body}");
    server.abort();
}

/// A committed symlink to a directory elsewhere does not bring that
/// directory into the repository: the package behind it is judged where
/// it really is.
#[tokio::test]
async fn a_symlinked_path_dependency_is_refused() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let outside = temp.path().join("outside");
    package(&outside, "secret", "Hidden", "");
    let (url, server, app) = served(temp.path(), |work| {
        package(
            &work.join("dependent"),
            "dependent",
            "Prompt",
            "[dependencies]\nsecret = { path = \"escape\" }\n",
        );
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, work.join("dependent/escape")).expect("symlink");
    })
    .await;
    let (status, body) = import(app, &url, "dependent").await;
    assert_eq!(status, 422, "{body}");
    server.abort();
}

/// A crate that the parser cannot read without running out of stack (a
/// thousand nested references) is refused, and the server goes on: the
/// crate is read in a child, which is what gives out.
#[tokio::test]
async fn a_crate_the_reader_cannot_survive_is_refused_not_fatal() {
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("deep"), "deep", "Prompt", "");
        std::fs::write(
            work.join("deep/src/lib.rs"),
            format!("pub struct S({}u8);\n", "&".repeat(1000)),
        )
        .expect("source");
        package(&work.join("fine"), "fine", "Prompt", "");
    })
    .await;
    let (status, body) = import(app.clone(), &url, "deep").await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body.contains("could not be read") || body.contains("stack"),
        "{body}"
    );
    // The server is still here, and reads the next crate.
    let (status, body) = import(app, &url, "fine").await;
    assert_eq!(status, 200, "{body}");
    server.abort();
}

/// An import abandoned by its client ends the child that reads it, and
/// with the child the cargo and git it started: the connections a silent
/// dependency host held are closed, rather than kept by orphans holding
/// cargo's lock for every later import.
#[tokio::test]
async fn an_abandoned_import_ends_its_child_and_cargo() {
    use std::io::Read;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a silent port");
    let silent = format!(
        "http://{}/slow.git",
        listener.local_addr().expect("address")
    );
    let held: std::sync::Arc<std::sync::Mutex<Vec<std::net::TcpStream>>> = Default::default();
    let accepted = std::sync::Arc::clone(&held);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            accepted.lock().expect("held").push(stream);
        }
    });
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let dependency = format!("[dependencies]\nslow = {{ git = {silent:?} }}\n");
    let (url, server, app) = served(temp.path(), |work| {
        package(&work.join("dependent"), "dependent", "Prompt", &dependency);
    })
    .await;
    // The server's own fetch of the repository succeeds; the child's cargo
    // then fetches the dependency and waits on the silent host.
    let abandoned = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        import(app, &url, "dependent"),
    )
    .await;
    assert!(abandoned.is_err(), "a silent dependency answered");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let streams = loop {
        let streams = std::mem::take(&mut *held.lock().expect("held"));
        if !streams.is_empty() || std::time::Instant::now() > deadline {
            break streams;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert!(
        !streams.is_empty(),
        "cargo's git never reached the dependency"
    );
    for mut stream in streams {
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .expect("a read timeout");
        let mut rest = Vec::new();
        stream
            .read_to_end(&mut rest)
            .expect("a connection the child's git held is still open");
    }
    server.abort();
}

/// A child whose cargo reaches its deadline reports, then ends its own
/// process group: the cargo and the git it leaves die with it, and the
/// connections they held are closed. The child is run here as the server
/// runs it, in a group of its own, with a short deadline.
#[tokio::test]
async fn a_child_past_its_cargo_deadline_ends_its_group() {
    use std::io::Read;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a silent port");
    let silent = format!(
        "http://{}/slow.git",
        listener.local_addr().expect("address")
    );
    let held: std::sync::Arc<std::sync::Mutex<Vec<std::net::TcpStream>>> = Default::default();
    let accepted = std::sync::Arc::clone(&held);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            accepted.lock().expect("held").push(stream);
        }
    });
    let temp = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .expect("temp");
    let request = json!({
        "krate": {"Git": {"name": "slow", "url": silent, "rev": "0123456789abcdef0123456789abcdef01234567", "path": ""}},
        "target": temp.path().join("target"),
        "cargo_deadline_secs": 2,
    })
    .to_string();
    let mut command = std::process::Command::new("sh");
    command
        .args(["-c", "exec \"$0\" extract-child \"$1\""])
        .arg(env!("CARGO_BIN_EXE_gdl-web"))
        .arg(request)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    // Bounded at ten times cargo's deadline: a child that outlives it is
    // the defect, and ends here with its group, not with the test runner.
    let output = tokio::task::spawn_blocking(move || {
        let mut child = command.spawn().expect("the child");
        let pid = child.id();
        let started = std::time::Instant::now();
        loop {
            if child.try_wait().expect("its state").is_some() {
                break child.wait_with_output().expect("its end");
            }
            if started.elapsed() > std::time::Duration::from_secs(20) {
                gdl_extract::kill_process_group(pid);
                let _ = child.kill();
                panic!("the child outlived cargo's deadline");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    })
    .await
    .expect("waited");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("did not finish within"), "{stderr}");
    let streams = std::mem::take(&mut *held.lock().expect("held"));
    assert!(
        !streams.is_empty(),
        "cargo's git never reached the dependency"
    );
    for mut stream in streams {
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .expect("a read timeout");
        let mut rest = Vec::new();
        stream
            .read_to_end(&mut rest)
            .expect("a connection the child's git held is still open");
    }
}
