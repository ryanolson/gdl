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
fn scratch(name: &str) -> tempfile::TempDir {
    let temp = tempfile::Builder::new()
        .prefix(&format!("gdl-web-{name}-"))
        .tempdir()
        .expect("dir");
    let dir = temp.path();
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
    temp
}

/// The server over `catalog`, with its target directory in `dir`.
fn router(dir: &Path, catalog: PathBuf) -> axum::Router {
    gdl_web::router(gdl_web::Config {
        catalog,
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        deploy: gdl_web::Deploy::default(),
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
    let temp = scratch("list");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
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
}

/// A draft becomes a graph file and a spec, which check as generation
/// would: a cut on an edge of wire types is a crossing.
#[tokio::test]
async fn a_draft_checks_as_its_spec_would_generate() {
    let temp = scratch("check");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
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
}

/// What generation refuses, a check refuses, with the reason: a cut on an
/// edge of a type without a codec, ports of two messages, and a node the
/// catalogue does not hold.
#[tokio::test]
async fn a_check_refuses_what_generation_refuses() {
    let temp = scratch("refuse");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
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
}

/// A draft that checks exports: the archive `cargo gdl export` writes,
/// from the catalogue's offers, with nothing fetched or built.
#[tokio::test]
async fn a_checked_draft_exports() {
    let temp = scratch("export");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
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
}

/// Adding a crate fetches it, reads it, and serves it; one that cannot be
/// fetched adds nothing and says why.
#[tokio::test]
async fn a_crate_is_added_from_its_registry() {
    let temp = scratch("add");
    let dir = temp.path();
    let router = router(dir, dir.join("catalogue"));
    let (status, body) = json_call(
        &router,
        "POST",
        "/api/catalog",
        Some(json!({ "source":"registry", "name":"regproto", "version":"0.1.0" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let (_, body) = json_call(&router, "GET", "/api/catalog", None).await;
    assert_eq!(body["entries"][0]["package"], "regproto");
    let (status, body) = json_call(
        &router,
        "POST",
        "/api/catalog",
        Some(json!({ "source":"registry", "name":"nosuch", "version":"1.0.0" })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body:#}");
    assert!(!body["problems"].as_array().expect("problems").is_empty());
    let (_, body) = json_call(&router, "GET", "/api/catalog", None).await;
    assert_eq!(body["entries"].as_array().expect("entries").len(), 1);
}

/// The page is served over a socket, as a browser would ask for it.
#[tokio::test]
async fn the_page_is_served() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let temp = scratch("page");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    stream
        .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
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
}

#[tokio::test]
async fn the_editor_assets_are_served() {
    let temp = scratch("assets");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
    for (path, content_type, marker) in [
        ("/style.css", "text/css", "--green:#83d100"),
        ("/app.js", "text/javascript", "function sourcePort()"),
        (
            "/vendor/mermaid.js",
            "text/javascript",
            "globalThis[\"mermaid\"]",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], content_type);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert!(std::str::from_utf8(&body).expect("text").contains(marker));
    }
}

/// A crate is a name and a version, and nothing else: one that would name
/// a path outside the target directory writes nothing there.
#[tokio::test]
async fn adding_a_crate_writes_only_under_the_target() {
    let temp = scratch("escape");
    let dir = temp.path();
    let victim = dir.join("victim");
    std::fs::create_dir_all(victim.join("src")).expect("dir");
    std::fs::write(victim.join("Cargo.toml"), "sentinel").expect("sentinel");
    std::fs::write(victim.join("src/lib.rs"), "sentinel").expect("sentinel");
    let router = router(dir, dir.join("catalogue"));
    let (status, body) = json_call(
        &router,
        "POST",
        "/api/catalog",
        Some(json!({ "source":"registry", "name":"regproto", "version":"x/../../../victim" })),
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
}

/// Identical drafts checked at once all check: each is read from its own
/// text, and nothing is shared between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_checks_at_once_all_succeed() {
    let temp = scratch("concurrent");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
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
}

/// A graph node named otherwise than its catalogue node is bound by a pin.
#[tokio::test]
async fn a_renamed_node_is_pinned() {
    let temp = scratch("renamed");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
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
}

/// Adds of one crate at once (a double click, or two users) share the
/// fetch's scratch directory: each must read a whole manifest there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adds_of_one_crate_at_once_all_succeed() {
    // The race showed in about one run in ten of eight adds; several
    // rounds make a regression fail here, not now and then.
    for round in 0..6 {
        let temp = scratch(&format!("adds-{round}"));
        let dir = temp.path();
        let router = router(dir, dir.join("catalogue"));
        let adds: Vec<_> = (0..8)
            .map(|_| {
                let router = router.clone();
                tokio::spawn(async move {
                    json_call(
                        &router,
                        "POST",
                        "/api/catalog",
                        Some(json!({ "source":"registry", "name":"regproto", "version":"0.1.0" })),
                    )
                    .await
                })
            })
            .collect();
        for add in adds {
            let (status, body) = add.await.expect("add");
            assert_eq!(status, StatusCode::OK, "round {round}: {body:#}");
        }
    }
}

/// The archive holds the draft as its spec and graph file, so the download
/// is the save: expanded, it exports again from what it holds.
#[tokio::test]
async fn an_export_holds_its_spec_and_graph_file() {
    use std::io::Read as _;
    let temp = scratch("saved");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
    let one = draft(
        "regproto::Words",
        "Engine",
        json!({ "all": ["A", "Engine"] }),
    );
    let (status, bytes) = call(&router, "POST", "/api/export", Some(one)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let unpacked = dir.join("unpacked");
    tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()))
        .unpack(&unpacked)
        .expect("expanded");
    let root = unpacked.join("composed");
    let mut cargo = String::new();
    std::fs::File::open(root.join("all/Cargo.toml"))
        .expect("segment")
        .read_to_string(&mut cargo)
        .expect("text");
    assert_eq!(
        cargo.lines().nth(1),
        Some("# Regenerate with: cargo gdl export --spec ../gdl.toml --out <archive.tar.gz>")
    );
    assert!(root.join("graph.gdl").exists());
    gdl_gen::Spec::load(&root.join("gdl.toml")).expect("the archived spec loads");
}

/// When every crate can be fetched anywhere, an export builds images, and
/// a check refuses what that export would: a segment whose image name
/// Docker would refuse.
#[tokio::test]
async fn a_check_refuses_an_image_name_the_export_would() {
    let temp = scratch("image-name");
    let dir = temp.path();
    let router = gdl_web::router(gdl_web::Config {
        catalog: fixture_catalog(),
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        deploy: gdl_web::Deploy::default(),
        target: dir.join("target"),
        gdl: gdl_web::Gdl::Git {
            url: "https://example.com/gdl".into(),
            rev: "abc123".into(),
        },
    })
    .expect("router");
    let named = draft(
        "regproto::Words",
        "Engine",
        json!({ "front": ["A"], "engine_": ["Engine"] }),
    );
    let (_, body) = json_call(&router, "POST", "/api/check", Some(named.clone())).await;
    assert_eq!(body["ok"], false, "{body:#}");
    assert_eq!(
        body["problems"][0],
        "segment `engine_`: its image would be named `composed-engine-`, which Docker refuses; name the segment with a letter or digit last"
    );
    let (status, _) = call(&router, "POST", "/api/export", Some(named)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

/// The demo, without the page: the chat example's four crates added by
/// their directories, its graph composed, carved into three segments,
/// checked, and exported. Nothing is fetched or built.
#[tokio::test]
async fn the_chat_example_composes_from_its_directories() {
    let temp = scratch("chat");
    let dir = temp.path();
    let router = local_router(dir, &crates().join(".."));
    let chat = crates().join("../examples/chat");
    for krate in ["protocols", "front-nodes", "engine-mock", "post-nodes"] {
        let path = chat.join(krate).display().to_string();
        let (status, body) = json_call(
            &router,
            "POST",
            "/api/catalog",
            Some(json!({ "source":"local", "path": path })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{krate}: {body:#}");
    }
    let (_, catalogue) = json_call(&router, "GET", "/api/catalog", None).await;
    let protocols = catalogue["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|e| e["package"] == "chat-protocols")
        .expect("chat-protocols")
        .clone();
    let message = |name: &str| {
        json!({
            "package": "chat-protocols",
            "version": protocols["version"],
            "source": protocols["source"],
            "path": format!("chat_protocols::{name}"),
        })
    };
    let node = |name: &str, entry: &str| json!({ "name": name, "entry": entry, "node": name });
    let draft = json!({
        "name": "chat",
        "source": {
            "name": "Frontend",
            "request": { "head": message("Prompt") },
            "response": message("Chunk"),
        },
        "nodes": [
            node("Preprocess", "chat-front-nodes@0.0.0"),
            node("Truncate", "chat-front-nodes@0.0.0"),
            node("Engine", "chat-engine-mock@0.0.0"),
            node("Detokenize", "chat-post-nodes@0.0.0"),
            node("Format", "chat-post-nodes@0.0.0"),
            node("Number", "chat-front-nodes@0.0.0"),
        ],
        "hop": {
            "inner": "Preprocess",
            "nested": { "forward": ["Truncate"], "inner": "Engine", "backward": ["Detokenize", "Format"] },
            "backward": ["Number"],
        },
        "segments": {
            "front": ["Frontend", "Preprocess", "Truncate", "Number"],
            "engine": ["Engine"],
            "post": ["Detokenize", "Format"],
        },
    });
    let (_, body) = json_call(&router, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(body["ok"], true, "{body:#}");
    assert!(
        body["crossings"]
            .as_str()
            .expect("crossings")
            .contains("per request: 3 crossings"),
        "{body:#}"
    );
    let (status, bytes) = call(&router, "POST", "/api/export", Some(draft)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let names: Vec<String> = archive
        .entries()
        .expect("entries")
        .map(|e| {
            e.expect("entry")
                .path()
                .expect("path")
                .display()
                .to_string()
        })
        .collect();
    for segment in ["front", "engine", "post"] {
        assert!(
            names.contains(&format!("chat/{segment}/Cargo.toml")),
            "{names:?}"
        );
    }
}

/// A crate added from a directory, edited, and added again is served as it
/// is now, not as it was first read.
#[tokio::test]
async fn an_edited_directory_crate_is_read_again() {
    let temp = scratch("edited");
    let dir = temp.path();
    let krate = dir.join("edited");
    std::fs::create_dir_all(krate.join("src")).expect("crate");
    std::fs::write(
        krate.join("Cargo.toml"),
        "[package]\nname = \"edited\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .expect("manifest");
    let write = |messages: &[&str]| {
        let text: String = messages
            .iter()
            .map(|m| format!("#[derive(gdl_message::Message)]\npub struct {m};\n"))
            .collect();
        std::fs::write(krate.join("src/lib.rs"), text).expect("source");
    };
    let router = local_router(dir, dir);
    let add = json!({ "source":"local", "path": krate.display().to_string() });
    let names = |body: &Value| -> Vec<String> {
        body["entries"][0]["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .map(|m| m["name"].as_str().expect("name").to_string())
            .collect()
    };
    write(&["One"]);
    let (status, body) = json_call(&router, "POST", "/api/catalog", Some(add.clone())).await;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let (_, body) = json_call(&router, "GET", "/api/catalog", None).await;
    assert_eq!(names(&body), ["One"]);
    write(&["One", "Two"]);
    let (status, body) = json_call(&router, "POST", "/api/catalog", Some(add.clone())).await;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let (_, body) = json_call(&router, "GET", "/api/catalog", None).await;
    assert_eq!(names(&body), ["One", "Two"]);
    // A new version of the directory is still the one package: its entry
    // replaces the old version's, so the catalogue never offers both.
    std::fs::write(
        krate.join("Cargo.toml"),
        "[package]\nname = \"edited\"\nversion = \"0.2.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .expect("manifest");
    write(&["Uno"]);
    let (status, body) = json_call(&router, "POST", "/api/catalog", Some(add)).await;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let (_, body) = json_call(&router, "GET", "/api/catalog", None).await;
    assert_eq!(
        body["entries"].as_array().expect("entries").len(),
        1,
        "{body:#}"
    );
    assert_eq!(body["entries"][0]["version"], "0.2.0");
    assert_eq!(names(&body), ["Uno"]);
}

/// A crate is named as a spec names one: a name with no version, or a
/// version with no name, is refused by the one rule, before any fetch.
#[tokio::test]
async fn a_crate_without_a_name_or_a_version_is_refused() {
    let temp = scratch("unnamed");
    let dir = temp.path();
    let router = router(dir, dir.join("catalogue"));
    for text in ["@1.2.3", "regproto@"] {
        let (status, body) = json_call(
            &router,
            "POST",
            "/api/catalog",
            Some(json!({ "source":"registry", "name":text.split_once('@').expect("test fixture").0, "version":text.split_once('@').expect("test fixture").1 })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body:#}");
        assert_eq!(
            body["problems"][0],
            format!("`{text}` is neither a directory nor `name@version`")
        );
    }
}

#[tokio::test]
async fn local_imports_and_browsing_are_disabled_by_default() {
    let temp = scratch("local-disabled");
    let dir = temp.path();
    let app = router(dir, dir.join("catalog"));
    let (status, catalog) = json_call(&app, "GET", "/api/catalog", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(catalog["local_enabled"], false);
    let (status, _) = call(&app, "GET", "/api/local", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = call(
        &app,
        "POST",
        "/api/catalog",
        Some(json!({"source":"local", "path":dir})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

fn local_router(dir: &Path, root: &Path) -> axum::Router {
    gdl_web::router(gdl_web::Config {
        catalog: dir.join("catalogue"),
        target: dir.join("target"),
        gdl: gdl_web::Gdl::Path(crates().join("gdl")),
        local_root: Some(root.to_path_buf()),
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        deploy: gdl_web::Deploy::default(),
    })
    .expect("local router")
}

#[tokio::test]
async fn browsing_is_confined_to_the_local_root() {
    let temp = scratch("browse");
    let dir = temp.path();
    let root = dir.join("allowed");
    std::fs::create_dir_all(root.join("package/src")).expect("test fixture");
    std::fs::write(
        root.join("package/Cargo.toml"),
        "[package]\nname='example'\nversion='0.1.0'\n",
    )
    .expect("test fixture");
    let app = local_router(dir, &root);
    let (status, body) = json_call(&app, "GET", "/api/local", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["directories"][0]["name"], "package");
    assert_eq!(body["parent"], Value::Null);
    let (status, body) = json_call(&app, "GET", "/api/local?path=package", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["package"], "example");
    let (status, _) = call(&app, "GET", "/api/local?path=..", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(dir, root.join("escape")).expect("test fixture");
        let (status, _) = call(&app, "GET", "/api/local?path=escape", None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let (status, _) = call(
        &app,
        "POST",
        "/api/catalog",
        Some(json!({"source":"local", "path":"../"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn git_catalog_entries_export_fetchable_dependencies() {
    let temp = scratch("git-export");
    let dir = temp.path();
    let commit = "0123456789abcdef0123456789abcdef01234567ab";
    let source = format!("git+https://example.com/packages?rev={commit}#{commit}");
    let replace = |value: String| {
        value.replace(
            "registry+https://github.com/rust-lang/crates.io-index",
            &source,
        )
    };
    let mut catalog = gdl_extract::Catalog::load(&dir.join("catalog")).expect("test fixture");
    for entry in gdl_extract::Catalog::load(&fixture_catalog())
        .expect("test fixture")
        .entries()
    {
        let entry = serde_json::from_str(&replace(
            serde_json::to_string(entry).expect("test fixture"),
        ))
        .expect("test fixture");
        catalog.add(entry).expect("test fixture");
    }
    let app = gdl_web::router(gdl_web::Config {
        catalog: dir.join("catalog"),
        target: dir.join("target"),
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        deploy: gdl_web::Deploy::default(),
        gdl: gdl_web::Gdl::Git {
            url: "https://example.com/gdl".into(),
            rev: commit.into(),
        },
    })
    .expect("test fixture");
    let draft = draft(
        "regproto::Words",
        "Engine",
        json!({"front":["A"],"engine":["Engine"]}),
    );
    let draft: Value = serde_json::from_str(&replace(draft.to_string())).expect("test fixture");
    let (status, checked) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(checked["ok"], true, "{checked:#}");
    assert!(
        checked["spec"]
            .as_str()
            .expect("test fixture")
            .contains(commit)
    );
    let (status, bytes) = call(&app, "POST", "/api/export", Some(draft)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let mut dockerfiles = 0;
    let mut git_dependencies = 0;
    for file in archive.entries().expect("test fixture") {
        use std::io::Read;
        let mut file = file.expect("test fixture");
        let path = file
            .path()
            .expect("test fixture")
            .to_string_lossy()
            .into_owned();
        if path.ends_with("Dockerfile") {
            dockerfiles += 1;
        }
        if path.ends_with("Cargo.toml") {
            let mut text = String::new();
            file.read_to_string(&mut text).expect("test fixture");
            if text.contains("git = \"https://example.com/packages\"") {
                assert!(text.contains(commit));
                git_dependencies += 1;
            }
        }
    }
    assert_eq!(dockerfiles, 2);
    assert!(git_dependencies >= 2);
}

#[tokio::test]
async fn git_import_rejects_non_http_sources_and_escaping_paths() {
    let temp = scratch("git-refused");
    let dir = temp.path();
    let app = router(dir, dir.join("catalog"));
    for (url, path) in [
        ("file:///tmp/repo", ""),
        ("ssh://example.com/repo", ""),
        ("https://example.com/repo", "../elsewhere"),
    ] {
        let (status, body) = json_call(
            &app,
            "POST",
            "/api/catalog",
            Some(json!({"source":"git","url":url,"revision":"main","path":path})),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body:#}");
    }
}

#[tokio::test]
async fn a_catalog_frontend_sets_the_source_binding_and_exports_its_service() {
    let temp = scratch("frontend");
    let dir = temp.path();
    let app = local_router(dir, &crates().join(".."));
    let (status, body) = json_call(
        &app,
        "POST",
        "/api/catalog",
        Some(json!({"source":"local","path":"examples/http"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let (_, catalog) = json_call(&app, "GET", "/api/catalog", None).await;
    let entry = &catalog["entries"][0];
    let frontend = entry["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .find(|n| n["name"] == "HttpFrontend")
        .expect("frontend");
    assert_eq!(frontend["role"], "source");
    let message = |name: &str| json!({"package":"http-frontend","version":"0.1.0","source":entry["source"],"path":format!("http_frontend::{name}")});
    let mut draft = json!({"name":"http","source":{"name":"Http","request":{"head":message("Prompt")},"response":message("Chunk")},"nodes":[
        {"name":"Http","entry":"http-frontend@0.1.0","node":"HttpFrontend"},
        {"name":"Engine","entry":"http-frontend@0.1.0","node":"Echo"}
    ],"hop":{"inner":"Engine"},"segments":{"front":["Http"],"engine":["Engine"]}});
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(check["ok"], true, "{check:#}");
    let (status, bytes) = call(&app, "POST", "/api/export", Some(draft.clone())).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let mut frontend_main = false;
    for file in archive.entries().expect("archive") {
        use std::io::Read;
        let mut file = file.expect("file");
        let path = file.path().expect("path").to_string_lossy().into_owned();
        if path.ends_with("/src/lib.rs") && !path.contains("contract") {
            let mut text = String::new();
            file.read_to_string(&mut text).expect("source");
            if path.contains("/front/") {
                assert!(
                    text.contains(".build::<::http_frontend::HttpFrontend>(\"Http\", frontend)"),
                    "{text}"
                );
                assert!(
                    text.contains("gdl::Frontend::serve(frontend, segment,"),
                    "{text}"
                );
                assert!(!text.contains("gdl::serve_json_lines"));
                frontend_main = true;
            } else {
                assert!(!text.contains("gdl::Frontend::serve"));
            }
        }
    }
    assert!(frontend_main);
    let (status, result) = json_call(&app, "POST", "/api/deploy", Some(draft.clone())).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(result["problems"].to_string().contains("http-frontend"));
    draft["source"]["request"]["head"] = message("Chunk");
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft)).await;
    assert_eq!(check["ok"], false, "frontend contract mismatch must fail");
}

#[tokio::test]
async fn service_flags_control_required_typed_adapters_and_export_one_shared_pipeline() {
    let temp = scratch("service");
    let dir = temp.path();
    let app = local_router(dir, &crates().join(".."));
    let (status, body) = json_call(
        &app,
        "POST",
        "/api/catalog",
        Some(json!({"source":"local","path":"examples/api"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let (_, catalog) = json_call(&app, "GET", "/api/catalog", None).await;
    let entry = &catalog["entries"][0];
    let service = entry["nodes"]
        .as_array()
        .expect("test result")
        .iter()
        .find(|n| n["name"] == "ApiFrontend")
        .expect("test result");
    assert_eq!(service["entries"].as_array().expect("test result").len(), 3);
    assert_eq!(service["config"].as_array().expect("test result").len(), 3);
    assert!(!service["config"].to_string().contains("listen"));
    let message = |name: &str| json!({"package":"api-frontend","version":"0.1.0","source":entry["source"],"path":format!("api_frontend::{name}")});
    let adapter = |name: &str| json!({"operator":{"name":name,"entry":"api-frontend@0.1.0","node":name},"nested":{"target":"Http"}});
    let mut draft = json!({"name":"api","source":{"name":"Http","request":{"head":message("Prompt")},"response":message("Chunk")},"nodes":[
        {"name":"Http","entry":"api-frontend@0.1.0","node":"ApiFrontend"},
        {"name":"Engine","entry":"api-frontend@0.1.0","node":"Echo"}
    ],"hop":{"inner":"Engine"},"segments":{"front":["Http"],"engine":["Engine"]},"entries":{}});
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(check["ok"], false, "enabled entries must be wired");
    draft["entries"] = json!({"chat":adapter("ChatAdapter"),"responses":adapter("ResponsesAdapter"),"anthropic":adapter("AnthropicAdapter")});
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(check["ok"], true, "{check:#}");
    let mut independent = draft.clone();
    independent["nodes"]
        .as_array_mut()
        .expect("nodes")
        .push(json!({"name":"SecondEngine","entry":"api-frontend@0.1.0","node":"Echo"}));
    independent["pipelines"] = json!([{"source":{"name":"SecondPipeline","request":{"head":message("Prompt")},"response":message("Chunk")},"hop":{"inner":"SecondEngine"}}]);
    independent["entries"]["responses"]["nested"]["target"] = json!("SecondPipeline");
    independent["segments"]["front"] = json!(["Http", "SecondPipeline"]);
    independent["segments"]["second"] = json!(["SecondEngine"]);
    let (_, check) = json_call(&app, "POST", "/api/check", Some(independent.clone())).await;
    assert_eq!(check["ok"], true, "independent pipelines: {check:#}");
    assert!(
        check["graph"]
            .as_str()
            .expect("graph")
            .contains("topology: SecondPipeline => SecondEngine")
    );
    let (status, bytes) = call(&app, "POST", "/api/export", Some(independent.clone())).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let generated = dir.join("independent");
    tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()))
        .unpack(&generated)
        .expect("generated files");
    println!("independent export: {}", generated.display());
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let mut found = false;
    for file in archive.entries().expect("archive") {
        use std::io::Read;
        let mut file = file.expect("file");
        if file.path().expect("path").ends_with("front/src/lib.rs") {
            let mut text = String::new();
            file.read_to_string(&mut text).expect("read");
            assert!(text.contains("roots.0.clone()"), "{text}");
            assert!(text.contains("roots.1.0.clone()"), "{text}");
            found = true;
        }
    }
    assert!(found);
    let mut shared_sink = independent.clone();
    shared_sink["pipelines"][0]["hop"]["inner"] = json!("Engine");
    shared_sink["nodes"]
        .as_array_mut()
        .expect("nodes")
        .retain(|n| n["name"] != "SecondEngine");
    shared_sink["segments"]
        .as_object_mut()
        .expect("segments")
        .remove("second");
    let (_, check) = json_call(&app, "POST", "/api/check", Some(shared_sink.clone())).await;
    assert_eq!(check["ok"], true, "shared sink: {check:#}");
    let graph = check["graph"].as_str().expect("graph");
    assert_eq!(graph.matches("sink Engine:").count(), 1);
    assert_eq!(graph.matches("=> Engine;").count(), 2);
    let mut four = shared_sink.clone();
    four["identities"] = json!({"ProcessingOne":{"role":"operator","request":{"head":message("Prompt")},"response":message("Chunk")},"ProcessingTwo":{"role":"forward","request":{"head":message("Prompt")}}});
    four["hop"] = json!({"inner":"ProcessingOne","nested":{"inner":"Engine"}});
    four["pipelines"][0]["hop"] = json!({"forward":["ProcessingTwo"],"inner":"Engine"});
    four["segments"]["processing-one"] = json!(["ProcessingOne"]);
    four["segments"]["processing-two"] = json!(["ProcessingTwo"]);
    let (_, checked) = json_call(&app, "POST", "/api/check", Some(four.clone())).await;
    assert_eq!(checked["ok"], true, "four segments: {checked:#}");
    assert!(
        checked["graph"]
            .as_str()
            .expect("valid identity fixture")
            .contains("identity ProcessingTwo: request Prompt")
    );
    let (status, archive) = call(&app, "POST", "/api/export", Some(four.clone())).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&archive)
    );
    tar::Archive::new(flate2::read::GzDecoder::new(archive.as_slice()))
        .unpack(dir.join("four"))
        .expect("valid identity fixture");
    println!("four segment export: {}", dir.join("four/api").display());
    for name in ["front", "processing-one", "processing-two", "engine"] {
        assert!(dir.join("four/api").join(name).join("Cargo.toml").exists());
    }
    four["identities"]["ProcessingTwo"] = json!({"role":"backward","response":message("Chunk")});
    four["pipelines"][0]["hop"] = json!({"inner":"Engine","backward":["ProcessingTwo"]});
    let (_, checked) = json_call(&app, "POST", "/api/check", Some(four.clone())).await;
    assert_eq!(checked["ok"], true, "response-only adapter: {checked:#}");
    assert!(
        checked["graph"]
            .as_str()
            .expect("valid identity fixture")
            .contains("identity ProcessingTwo: response Chunk")
    );
    let (status, archive) = call(&app, "POST", "/api/export", Some(four)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&archive)
    );
    tar::Archive::new(flate2::read::GzDecoder::new(archive.as_slice()))
        .unpack(dir.join("response-only"))
        .expect("valid identity fixture");
    let (status, bytes) = call(&app, "POST", "/api/export", Some(shared_sink)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()))
        .unpack(dir.join("shared-sink"))
        .expect("shared sink export");
    independent["entries"]["responses"]["nested"]["target"] = json!("Missing");
    let (_, check) = json_call(&app, "POST", "/api/check", Some(independent.clone())).await;
    assert_eq!(check["ok"], false, "missing pipeline must fail");
    independent["entries"]["responses"]["nested"]
        .as_object_mut()
        .expect("nested")
        .remove("target");
    let (_, check) = json_call(&app, "POST", "/api/check", Some(independent)).await;
    assert_eq!(check["ok"], false, "no implicit sharing");
    draft["entries"]["chat"] = adapter("ResponsesAdapter");
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(
        check["ok"], false,
        "public request and response types must match"
    );
    draft["entries"] = json!({"chat":adapter("ChatAdapter")});
    draft["nodes"][0]["config"] =
        json!({"enabled_chat":true,"enabled_responses":false,"enabled_anthropic":false});
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(check["ok"], true, "{check:#}");
    let (status, bytes) = call(&app, "POST", "/api/export", Some(draft.clone())).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    let mut checked = false;
    for file in archive.entries().expect("test result") {
        use std::io::Read;
        let mut file = file.expect("test result");
        let path = file
            .path()
            .expect("test result")
            .to_string_lossy()
            .into_owned();
        if path.ends_with("front/src/lib.rs") {
            let mut text = String::new();
            file.read_to_string(&mut text).expect("test result");
            assert!(text.contains("gdl::Service::serve"), "{text}");
            // One segment, shared through the runtime with every entry.
            assert_eq!(
                text.matches("Serving::new(segment, |segment, shutdown|")
                    .count(),
                1,
                "{text}"
            );
            assert!(!text.contains("Arc::new(segment)"), "{text}");
            assert!(text.contains("responses: None"));
            assert!(text.contains("anthropic: None"));
            assert!(text.contains("builder: &gdl::Builder"), "{text}");
            checked = true;
        }
    }
    assert!(checked);
    let single = |name: &str| json!({"name":name,"entry":"api-frontend@0.1.0","node":name});
    draft["entries"]["chat"] = json!({"forward":[single("ExtractChat")],"backward":[single("FormatChat")],"target":"Http"});
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(
        check["ok"], true,
        "separate request and response transforms join the shared pipeline: {check:#}"
    );
    draft["entries"]["chat"]["backward"] = json!([]);
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(
        check["ok"], false,
        "an unrepaired response mismatch must fail"
    );
    draft["nodes"][0]["config"]["listen"] = json!("private");
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft)).await;
    assert_eq!(
        check["ok"], false,
        "runtime-only fields are refused in the builder"
    );
}

#[tokio::test]
async fn request_router_policy_is_checked_and_embedded_in_exported_segments() {
    let temp = scratch("request-router");
    let dir = temp.path();
    let app = router(dir, fixture_catalog());
    let mut cut = draft(
        "regproto::Words",
        "Engine",
        json!({"front":["A"],"engine":["Engine"]}),
    );
    cut["routing"] = json!({"Engine":"random"});
    let (_, body) = json_call(&app, "POST", "/api/check", Some(cut.clone())).await;
    assert_eq!(body["ok"], true, "{body}");
    let (status, bytes) = call(&app, "POST", "/api/export", Some(cut.clone())).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let unpacked = dir.join("unpacked");
    tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()))
        .unpack(&unpacked)
        .expect("router export fixture");
    let root = unpacked.join("composed");
    let spec = std::fs::read_to_string(root.join("gdl.toml")).expect("router export fixture");
    let placement = gdl_model::Placement::from_toml(&spec).expect("router export fixture");
    assert!(
        toml::to_string(&placement)
            .expect("router export fixture")
            .contains("random")
    );
    let segment =
        std::fs::read_to_string(root.join("engine/src/lib.rs")).expect("router export fixture");
    assert!(segment.contains("random"));
    cut["routing"] = json!({"A":"random"});
    let (_, body) = json_call(&app, "POST", "/api/check", Some(cut.clone())).await;
    assert_ne!(
        body["ok"], true,
        "source cannot be a request routing destination"
    );

    // A selector, gdl's own or another, exports as a table; a name no
    // selector can have is refused.
    cut["routing"] = json!({"Engine":{"selector":"least_loaded"}});
    let (_, body) = json_call(&app, "POST", "/api/check", Some(cut.clone())).await;
    assert_eq!(body["ok"], true, "{body}");
    let (status, bytes) = call(&app, "POST", "/api/export", Some(cut.clone())).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let unpacked = dir.join("selected");
    tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()))
        .unpack(&unpacked)
        .expect("selector export fixture");
    let spec = std::fs::read_to_string(unpacked.join("composed/gdl.toml"))
        .expect("selector export fixture");
    let placement = gdl_model::Placement::from_toml(&spec).expect("selector export fixture");
    let routed = gdl_model::Placement::new()
        .segment("front", ["A"])
        .segment("engine", ["Engine"])
        .route(
            "Engine",
            gdl_model::RequestRouter::Selector("least_loaded".into()),
        );
    assert_eq!(placement.hash(), routed.hash(), "{spec}");
    cut["routing"] = json!({"Engine":{"selector":"Least-Loaded"}});
    let (_, body) = json_call(&app, "POST", "/api/check", Some(cut)).await;
    assert_ne!(body["ok"], true, "a bad selector name was accepted");
}

#[tokio::test]
async fn the_catalogue_lists_gdls_own_selectors() {
    let temp = scratch("selectors");
    let app = router(temp.path(), fixture_catalog());
    let (_, catalog) = json_call(&app, "GET", "/api/catalog", None).await;
    let names: Vec<&str> = catalog["selectors"]
        .as_array()
        .expect("a list of selectors")
        .iter()
        .map(|s| s["name"].as_str().expect("a name"))
        .collect();
    assert_eq!(names, ["affinity", "least_loaded"]);
    assert!(catalog["selectors"][0]["summary"].is_string());
}

#[tokio::test]
async fn endpoint_nodes_export_as_four_independent_segments() {
    let temp = scratch("endpoint-segments");
    let dir = temp.path();
    let app = local_router(dir, &crates().join(".."));
    let (status, body) = json_call(
        &app,
        "POST",
        "/api/catalog",
        Some(json!({"source":"local","path":"examples/api"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let (_, catalog) = json_call(&app, "GET", "/api/catalog", None).await;
    let origin = &catalog["entries"][0]["source"];
    let message = |name: &str| json!({"package":"api-frontend","version":"0.1.0","source":origin,"path":format!("api_frontend::{name}")});
    let draft = json!({"name":"ports","source":{"name":"Http","request":{"head":message("ChatRequest")},"response":message("ChatReply")},
        "nodes":[{"name":"Http","entry":"api-frontend@0.1.0","node":"ApiFrontend"},{"name":"ExtractChat","entry":"api-frontend@0.1.0","node":"ExtractChat"},{"name":"FormatChat","entry":"api-frontend@0.1.0","node":"FormatChat"},{"name":"ResponsesAdapter","entry":"api-frontend@0.1.0","node":"ResponsesAdapter"},{"name":"AnthropicAdapter","entry":"api-frontend@0.1.0","node":"AnthropicAdapter"},{"name":"Echo","entry":"api-frontend@0.1.0","node":"Echo"}],
        "hop":{"forward":["ExtractChat"],"inner":"Echo","backward":["FormatChat"]},
        "pipelines":[{"source":{"name":"Responses","request":{"head":message("ResponseRequest")},"response":message("ResponseReply")},"hop":{"inner":"ResponsesAdapter","nested":{"inner":"Echo"}}},{"source":{"name":"Anthropic","request":{"head":message("AnthropicRequest")},"response":message("AnthropicReply")},"hop":{"inner":"AnthropicAdapter","nested":{"inner":"Echo"}}}],
        "entries":{"chat":{"target":"Http"},"responses":{"target":"Responses"},"anthropic":{"target":"Anthropic"}},
        "segments":{"s1":["Http","Responses","Anthropic","FormatChat","AnthropicAdapter"],"extract":["ExtractChat"],"responses":["ResponsesAdapter"],"sink":["Echo"]}});
    let (_, check) = json_call(&app, "POST", "/api/check", Some(draft.clone())).await;
    assert_eq!(check["ok"], true, "{check:#}");
    let (status, bytes) = call(&app, "POST", "/api/export", Some(draft)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let generated = dir.join("generated");
    tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()))
        .unpack(&generated)
        .expect("unpack segments");
    for (segment, node) in [
        ("extract", "ExtractChat"),
        ("responses", "ResponsesAdapter"),
        ("sink", "Echo"),
        ("s1", "FormatChat"),
    ] {
        let text =
            std::fs::read_to_string(generated.join("ports").join(segment).join("src/lib.rs"))
                .expect("segment main");
        assert!(
            text.contains(&format!("build::<::api_frontend::{node}>")),
            "{segment}: {text}"
        );
        if segment != "s1" {
            assert!(!text.contains("build::<::api_frontend::ApiFrontend>"));
        }
    }
    let front =
        std::fs::read_to_string(generated.join("ports/s1/src/lib.rs")).expect("frontend library");
    assert!(!front.contains("build::<::api_frontend::ExtractChat>"));
    assert!(!front.contains("build::<::api_frontend::ResponsesAdapter>"));
    eprintln!("endpoint segments: {}", generated.join("ports").display());
}

#[tokio::test]
async fn deployment_rejects_local_packages_without_local_access() {
    let temp = scratch("deploy-access");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
    let (status, _) = call(
        &router,
        "POST",
        "/api/deploy",
        Some(draft(
            "regproto::Words",
            "Engine",
            json!({"front":["A"],"engine":["Engine"]}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

/// The server over the fixture catalogue, deploying with a fake `docker`
/// in `dir` that answers the probes and records any other call in `calls`.
/// A child `sh` writes the script: were this process to hold it open for
/// writing, a test forking on another thread would hold it too, and running
/// it would fail with "Text file busy".
fn deploying_router(dir: &Path, apply: bool) -> axum::Router {
    use std::io::Write as _;
    let docker = dir.join("docker");
    let script = format!(
        "#!/bin/sh\ncase \"$1 $2\" in\n  \"compose version\") echo 5.2.0 ;;\n  \"info --format\") echo 29.6.2 ;;\n  *) echo \"$@\" >> {:?} ;;\nesac\n",
        dir.join("calls").display().to_string()
    );
    let mut writer = std::process::Command::new("sh")
        .args(["-c", "cat > \"$0\" && chmod 755 \"$0\""])
        .arg(&docker)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("sh");
    writer
        .stdin
        .take()
        .expect("stdin")
        .write_all(script.as_bytes())
        .expect("script");
    assert!(writer.wait().expect("sh").success());
    gdl_web::router(gdl_web::Config {
        catalog: fixture_catalog(),
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        deploy: gdl_web::Deploy {
            docker,
            kubectl: dir.join("no-kubectl"),
            apply,
        },
        target: dir.join("target"),
        gdl: gdl_web::Gdl::Path(crates().join("gdl")),
    })
    .expect("router")
}

#[tokio::test]
async fn targets_report_compose_and_whether_this_server_applies() {
    let temp = scratch("targets");
    for apply in [false, true] {
        let app = deploying_router(temp.path(), apply);
        let (status, targets) = json_call(&app, "GET", "/api/targets", None).await;
        assert_eq!(status, StatusCode::OK, "{targets:#}");
        assert_eq!(targets["compose"]["version"], "5.2.0");
        assert_eq!(targets["compose"]["available"], true);
        assert_eq!(targets["compose"]["apply"], apply);
        assert_eq!(targets["kubernetes"]["kubectl"], false);
    }
}

#[tokio::test]
async fn apply_needs_the_docker_flag() {
    let temp = scratch("apply-flag");
    let app = deploying_router(temp.path(), false);
    let draft = draft(
        "regproto::Words",
        "Engine",
        json!({"front":["A"],"engine":["Engine"]}),
    );
    let (status, body) = json_call(&app, "POST", "/api/deploy/apply", Some(draft)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:#}");
    assert!(
        body["problems"].to_string().contains("--docker"),
        "{body:#}"
    );
    assert!(!temp.path().join("calls").exists(), "docker was not run");
}

#[tokio::test]
async fn apply_refuses_what_deploy_refuses_and_runs_nothing() {
    let temp = scratch("apply-local");
    let app = deploying_router(temp.path(), true);
    let draft = draft(
        "regproto::Words",
        "Engine",
        json!({"front":["A"],"engine":["Engine"]}),
    );
    let (deploy, _) = json_call(&app, "POST", "/api/deploy", Some(draft.clone())).await;
    let (status, body) = json_call(&app, "POST", "/api/deploy/apply", Some(draft.clone())).await;
    assert_eq!(status, deploy, "{body:#}");
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body:#}");
    assert!(!temp.path().join("calls").exists(), "docker was not run");
    assert!(
        !temp.path().join("target/deployments/gdl-composed").exists(),
        "nothing was placed"
    );
    // A refusal ends the apply: the next one is not told one is running.
    let (status, _) = json_call(&app, "POST", "/api/deploy/apply", Some(draft)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn clear_catalog_archives_entries_and_reopens_empty() {
    let temp = scratch("clear-catalog");
    let dir = temp.path();
    let catalog = dir.join("catalogue");
    let entry = gdl_extract::Catalog::load(&fixture_catalog())
        .expect("fixture")
        .entries()
        .next()
        .expect("entry")
        .clone();
    gdl_extract::Catalog::load(&catalog)
        .expect("catalog")
        .add(entry)
        .expect("add");
    let app = router(dir, catalog.clone());
    let (status, body) = json_call(&app, "POST", "/api/catalog/clear", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let backup = body["backup"].as_str().expect("backup path");
    assert_eq!(
        gdl_extract::Catalog::load(Path::new(backup))
            .expect("backup")
            .entries()
            .count(),
        1
    );
    assert_eq!(
        gdl_extract::Catalog::load(&catalog)
            .expect("empty catalog")
            .entries()
            .count(),
        0
    );
    let (_, body) = json_call(&app, "GET", "/api/catalog", None).await;
    assert_eq!(body["entries"].as_array().expect("entries").len(), 0);
}

#[tokio::test]
async fn examples_use_the_configured_runtime_revision() {
    let temp = scratch("example-revision");
    let dir = temp.path();
    let app = gdl_web::router(gdl_web::Config {
        catalog: dir.join("catalogue"),
        target: dir.join("target"),
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        deploy: gdl_web::Deploy::default(),
        gdl: gdl_web::Gdl::Git {
            url: "https://github.com/ryanolson/gdl".into(),
            rev: "test-revision".into(),
        },
    })
    .expect("router");
    let (_, body) = json_call(&app, "GET", "/api/catalog", None).await;
    assert_eq!(body["example"]["revision"], "test-revision");
    assert_eq!(body["example"]["path"], "examples/api");
}

/// A browser sends `Origin` with a cross-origin `POST` even when no
/// preflight is needed; a page elsewhere must not be able to change the
/// catalogue through the user's browser.
#[tokio::test]
async fn a_request_from_another_origin_cannot_change_the_catalogue() {
    let temp = scratch("origin");
    let dir = temp.path();
    let router = router(dir, dir.join("catalog"));
    let send = |origin: Option<&str>, site: Option<&str>| {
        let mut request = Request::builder()
            .method("POST")
            .uri("/api/catalog/clear")
            .header("host", "127.0.0.1:8080")
            .header("content-type", "text/plain");
        if let Some(origin) = origin {
            request = request.header("origin", origin);
        }
        if let Some(site) = site {
            request = request.header("sec-fetch-site", site);
        }
        let request = request.body(Body::empty()).expect("request");
        let router = router.clone();
        async move { router.oneshot(request).await.expect("response").status() }
    };
    assert_eq!(
        send(Some("https://evil.example"), None).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send(Some("http://127.0.0.1:8080"), Some("cross-site")).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(send(Some("null"), None).await, StatusCode::FORBIDDEN);
    assert_eq!(
        send(Some("http://127.0.0.1:8080"), Some("same-origin")).await,
        StatusCode::OK
    );
    // A request without browser headers, as a script sends one.
    assert_eq!(send(None, None).await, StatusCode::OK);
    // A DNS name pointed at this machine makes a page elsewhere same-origin
    // with the server; the server answers to no such name, on any method.
    for (host, expected) in [
        ("evil.example:8080", StatusCode::FORBIDDEN),
        ("localhost:8080", StatusCode::OK),
        ("[::1]:8080", StatusCode::OK),
        ("composer.internal", StatusCode::OK),
    ] {
        let request = Request::builder()
            .method("GET")
            .uri("/api/catalog")
            .header("host", host)
            .body(Body::empty())
            .expect("request");
        let named = gdl_web::router(gdl_web::Config {
            catalog: dir.join("catalog"),
            local_root: None,
            hosts: vec!["composer.internal".into()],
            extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
            deploy: gdl_web::Deploy::default(),
            target: dir.join("target"),
            gdl: gdl_web::Gdl::Path(crates().join("gdl")),
        })
        .expect("router");
        let status = named.oneshot(request).await.expect("response").status();
        assert_eq!(status, expected, "{host}");
    }
}

/// The example package is offered from gdl's own repository at the
/// revision the server takes gdl from, and not at all from a path.
#[tokio::test]
async fn the_example_follows_the_configured_gdl_revision() {
    let temp = scratch("example");
    let dir = temp.path();
    let (_, body) = json_call(
        &router(dir, dir.join("catalog")),
        "GET",
        "/api/catalog",
        None,
    )
    .await;
    assert_eq!(body["example"], Value::Null);
    let git = gdl_web::router(gdl_web::Config {
        catalog: dir.join("catalog"),
        local_root: None,
        hosts: Vec::new(),
        extractor: PathBuf::from(env!("CARGO_BIN_EXE_gdl-web")),
        deploy: gdl_web::Deploy::default(),
        target: dir.join("target"),
        gdl: gdl_web::Gdl::Git {
            url: "https://example.com/gdl".into(),
            rev: "abc123".into(),
        },
    })
    .expect("router");
    let (_, body) = json_call(&git, "GET", "/api/catalog", None).await;
    assert_eq!(
        body["example"],
        json!({"source": "git", "url": "https://example.com/gdl", "revision": "abc123", "path": "examples/api"})
    );
}
