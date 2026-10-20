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
    let temp = tempfile::tempdir().expect("temp");
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
