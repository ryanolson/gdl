//! The composer's server, driven in process: the catalogue it serves, the
//! drafts it checks and exports, and a crate it adds, offline, from the
//! vendored registry of `gdl-gen`'s tests. No test compiles a crate.

use std::path::{Path, PathBuf};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tower::ServiceExt as _;

fn crates() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// A scratch directory whose `.cargo/config.toml` takes crates.io from the
/// vendored registry, offline.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gdl-web-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join(".cargo")).expect("dir");
    let registry = crates().join("gdl-gen/tests/fixtures/registry");
    std::fs::write(
        dir.join(".cargo/config.toml"),
        format!(
            "[net]\noffline = true\n[source.crates-io]\nreplace-with = \"fixture\"\n[source.fixture]\ndirectory = {:?}\n",
            registry.display().to_string()
        ),
    )
    .expect("config");
    dir
}

/// The server over `catalog`, with its target directory in `dir`.
fn router(dir: &Path, catalog: PathBuf) -> axum::Router {
    gdl_web::router(gdl_web::Config {
        catalog,
        target: dir.join("target"),
        gdl: gdl_web::Gdl::Path(crates().join("gdl")),
    })
    .expect("router")
}

fn fixture_catalog() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/catalog")
}

async fn call(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .expect("request");
    let response = router.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, bytes.to_vec())
}

async fn json_call(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let (status, bytes) = call(router, method, uri, body).await;
    (status, serde_json::from_slice(&bytes).expect("json"))
}

fn origin(path: &str) -> Value {
    json!({
        "package": "regproto",
        "version": "0.1.0",
        "source": "registry+https://github.com/rust-lang/crates.io-index",
        "path": path,
    })
}

/// A source of `request`, into the registry's node `node`, placed by
/// `segments`.
fn draft(request: &str, node: &str, segments: Value) -> Value {
    json!({
        "name": "composed",
        "source": {
            "name": "A",
            "request": { "head": origin(request) },
            "response": origin("regproto::Token"),
        },
        "nodes": [{ "name": node, "entry": "regnodes@0.1.0", "node": node }],
        "hop": { "inner": node },
        "segments": segments,
    })
}

#[tokio::test]
async fn the_catalogue_lists_its_entries() {
    let dir = scratch("list");
    let router = router(&dir, fixture_catalog());
    let (status, body) = json_call(&router, "GET", "/api/catalog", None).await;
    assert_eq!(status, StatusCode::OK);
    let keys: Vec<String> = body["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| {
            format!(
                "{}@{}",
                e["package"].as_str().expect("package"),
                e["version"].as_str().expect("version")
            )
        })
        .collect();
    assert_eq!(keys, ["regnodes@0.1.0", "regproto@0.1.0"]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A draft becomes a graph file and a spec, which check as generation
/// would: a cut on an edge of wire types is a crossing.
#[tokio::test]
async fn a_draft_checks_as_its_spec_would_generate() {
    let dir = scratch("check");
    let router = router(&dir, fixture_catalog());
    let one = draft(
        "regproto::Words",
        "Engine",
        json!({ "all": ["A", "Engine"] }),
    );
    let (_, body) = json_call(&router, "POST", "/api/check", Some(one)).await;
    assert_eq!(body["ok"], true, "{body:#}");
    assert_eq!(
        body["graph"],
        "name = \"composed\", schema = 1;\nsource A: Words -> stream Token;\nsink Engine: Words -> stream Token;\ntopology: A => Engine;\n"
    );
    // A check writes nothing.
    assert!(!dir.join("target/gdl-web").exists());
    let spec = body["spec"].as_str().expect("spec");
    assert!(spec.contains("crates = [\"regproto@0.1.0\"]"), "{spec}");
    assert!(spec.contains("crates = [\"regnodes@0.1.0\"]"), "{spec}");
    let cut = draft(
        "regproto::Words",
        "Engine",
        json!({ "front": ["A"], "engine": ["Engine"] }),
    );
    let (_, body) = json_call(&router, "POST", "/api/check", Some(cut)).await;
    assert_eq!(body["ok"], true, "{body:#}");
    assert!(
        body["crossings"]
            .as_str()
            .expect("crossings")
            .contains("per request: 2 crossings"),
        "{body:#}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// What generation refuses, a check refuses, with the reason: a cut on an
/// edge of a type without a codec, ports of two messages, and a node the
/// catalogue does not hold.
#[tokio::test]
async fn a_check_refuses_what_generation_refuses() {
    let dir = scratch("refuse");
    let router = router(&dir, fixture_catalog());
    let refused = |draft: Value| {
        let router = router.clone();
        async move {
            let (_, body) = json_call(&router, "POST", "/api/check", Some(draft)).await;
            assert_eq!(body["ok"], false, "{body:#}");
            body["problems"]
                .as_array()
                .expect("problems")
                .iter()
                .map(|p| p.as_str().expect("a problem").to_string())
                .collect::<Vec<_>>()
                .join("\n")
        }
    };
    let local = refused(draft(
        "regproto::Local",
        "Quiet",
        json!({ "front": ["A"], "engine": ["Quiet"] }),
    ))
    .await;
    assert_eq!(
        local,
        "edge `A` -> `Quiet` crosses a segment boundary, but its type `:: regproto :: Local` is not serializable"
    );
    let mismatched = refused(draft(
        "regproto::Local",
        "Engine",
        json!({ "all": ["A", "Engine"] }),
    ))
    .await;
    assert!(
        mismatched.contains("`Engine` takes `Words`, but `A` sends `Local`"),
        "{mismatched}"
    );
    let mut missing = draft(
        "regproto::Words",
        "Engine",
        json!({ "all": ["A", "Engine"] }),
    );
    missing["nodes"][0]["entry"] = json!("nosuch@1.0.0");
    let missing = refused(missing).await;
    assert_eq!(
        missing,
        "node `Engine`: the catalogue has no node `Engine` in `nosuch@1.0.0`"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A draft that checks exports: the archive `cargo gdl export` writes,
/// from the catalogue's offers, with nothing fetched or built.
#[tokio::test]
async fn a_checked_draft_exports() {
    let dir = scratch("export");
    let router = router(&dir, fixture_catalog());
    let cut = draft(
        "regproto::Words",
        "Engine",
        json!({ "front": ["A"], "engine": ["Engine"] }),
    );
    let (_, body) = json_call(&router, "POST", "/api/check", Some(cut.clone())).await;
    assert_eq!(body["ok"], true, "{body:#}");
    let (status, bytes) = call(&router, "POST", "/api/export", Some(cut)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    // The archive holds the contract, each segment, and the build script,
    // and each segment depends on the registry crate at its version.
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let mut names = Vec::new();
    for entry in archive.entries().expect("entries") {
        let mut entry = entry.expect("entry");
        let name = entry.path().expect("path").display().to_string();
        if name == "composed/engine/Cargo.toml" {
            let mut text = String::new();
            std::io::Read::read_to_string(&mut entry, &mut text).expect("text");
            assert!(
                text.contains("regnodes = { version = \"=0.1.0\" }"),
                "{text}"
            );
        }
        names.push(name);
    }
    for name in [
        "composed/contract/Cargo.toml",
        "composed/engine/Cargo.toml",
        "composed/front/Cargo.toml",
        "composed/build-all.sh",
    ] {
        assert!(names.iter().any(|n| n == name), "{name} in {names:?}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Adding a crate fetches it, reads it, and serves it; one that cannot be
/// fetched adds nothing and says why.
#[tokio::test]
async fn a_crate_is_added_from_its_registry() {
    let dir = scratch("add");
    let router = router(&dir, dir.join("catalogue"));
    let (status, body) = json_call(
        &router,
        "POST",
        "/api/catalog",
        Some(json!({ "crate": "regproto@0.1.0" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let (_, body) = json_call(&router, "GET", "/api/catalog", None).await;
    assert_eq!(body["entries"][0]["package"], "regproto");
    let (status, body) = json_call(
        &router,
        "POST",
        "/api/catalog",
        Some(json!({ "crate": "nosuch@1.0.0" })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body:#}");
    assert!(!body["problems"].as_array().expect("problems").is_empty());
    let (_, body) = json_call(&router, "GET", "/api/catalog", None).await;
    assert_eq!(body["entries"].as_array().expect("entries").len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The page is served over a socket, as a browser would ask for it.
#[tokio::test]
async fn the_page_is_served() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let dir = scratch("page");
    let router = router(&dir, fixture_catalog());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    stream
        .write_all(b"GET / HTTP/1.1\r\nhost: composer\r\nconnection: close\r\n\r\n")
        .await
        .expect("request");
    let mut response = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_to_string(&mut response),
    )
    .await
    .expect("an answer within 5 s")
    .expect("read");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.contains("<title>gdl composer</title>"),
        "{response}"
    );
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A crate is a name and a version, and nothing else: one that would name
/// a path outside the target directory writes nothing there.
#[tokio::test]
async fn adding_a_crate_writes_only_under_the_target() {
    let dir = scratch("escape");
    let victim = dir.join("victim");
    std::fs::create_dir_all(victim.join("src")).expect("dir");
    std::fs::write(victim.join("Cargo.toml"), "sentinel").expect("sentinel");
    std::fs::write(victim.join("src/lib.rs"), "sentinel").expect("sentinel");
    let router = router(&dir, dir.join("catalogue"));
    let (status, body) = json_call(
        &router,
        "POST",
        "/api/catalog",
        Some(json!({ "crate": "regproto@x/../../../victim" })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body:#}");
    assert_eq!(
        std::fs::read_to_string(victim.join("Cargo.toml")).expect("kept"),
        "sentinel"
    );
    assert_eq!(
        std::fs::read_to_string(victim.join("src/lib.rs")).expect("kept"),
        "sentinel"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Identical drafts checked at once all check: each is read from its own
/// text, and nothing is shared between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_checks_at_once_all_succeed() {
    let dir = scratch("concurrent");
    let router = router(&dir, fixture_catalog());
    let one = draft(
        "regproto::Words",
        "Engine",
        json!({ "all": ["A", "Engine"] }),
    );
    let checks: Vec<_> = (0..32)
        .map(|_| {
            let (router, one) = (router.clone(), one.clone());
            tokio::spawn(async move { json_call(&router, "POST", "/api/check", Some(one)).await })
        })
        .collect();
    for check in checks {
        let (_, body) = check.await.expect("check");
        assert_eq!(body["ok"], true, "{body:#}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A graph node named otherwise than its catalogue node is bound by a pin.
#[tokio::test]
async fn a_renamed_node_is_pinned() {
    let dir = scratch("renamed");
    let router = router(&dir, fixture_catalog());
    let mut renamed = draft(
        "regproto::Words",
        "Engine",
        json!({ "all": ["A", "Model"] }),
    );
    renamed["nodes"][0]["name"] = json!("Model");
    renamed["hop"]["inner"] = json!("Model");
    let (_, body) = json_call(&router, "POST", "/api/check", Some(renamed)).await;
    assert_eq!(body["ok"], true, "{body:#}");
    let spec = body["spec"].as_str().expect("spec");
    assert!(spec.contains("Model = \"regnodes::Engine\""), "{spec}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Adds of one crate at once (a double click, or two users) share the
/// fetch's scratch directory: each must read a whole manifest there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adds_of_one_crate_at_once_all_succeed() {
    // The race showed in about one run in ten of eight adds; several
    // rounds make a regression fail here, not now and then.
    for round in 0..6 {
        let dir = scratch(&format!("adds-{round}"));
        let router = router(&dir, dir.join("catalogue"));
        let adds: Vec<_> = (0..8)
            .map(|_| {
                let router = router.clone();
                tokio::spawn(async move {
                    json_call(
                        &router,
                        "POST",
                        "/api/catalog",
                        Some(json!({ "crate": "regproto@0.1.0" })),
                    )
                    .await
                })
            })
            .collect();
        for add in adds {
            let (status, body) = add.await.expect("add");
            assert_eq!(status, StatusCode::OK, "round {round}: {body:#}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
