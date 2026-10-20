//! The composer: a page that builds a graph from the nodes and messages
//! of a working catalogue, and the server that checks and exports it.
//!
//! ```text
//! page --GET /api/catalog---------------> the catalogue's entries
//!      --POST /api/catalog {source,...}-> fetch and read a package, then add it
//!      --POST /api/check {draft}--------> graph file + spec, checked: problems, or crossings and diagram
//!      --POST /api/export {draft}-------> the generated crates as a .tar.gz
//! ```
//!
//! The page offers only ports of one message to join, but decides nothing:
//! every verdict is the server's, from the same rules `cargo gdl` applies.
//! Nothing is compiled, so a crate that needs CUDA composes like any other.

mod draft;
mod imports;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::Router;
use axum::extract::{Json, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use gdl_extract::Catalog;
use serde_json::json;

pub use draft::{Draft, Gdl, Rendered};

/// What the server serves from.
pub struct Config {
    /// The working catalogue's directory.
    pub catalog: PathBuf,
    /// Where crates are fetched.
    pub target: PathBuf,
    /// Where generated crates take gdl from.
    pub gdl: Gdl,
    /// Directory exposed by local browsing and imports; disabled when absent.
    pub local_root: Option<PathBuf>,
}

struct Composer {
    config: Config,
    catalog: RwLock<Catalog>,
    http: reqwest::Client,
}

/// The server's routes, over the catalogue `config` names, loaded now.
pub fn router(mut config: Config) -> Result<Router, String> {
    config.local_root = config
        .local_root
        .map(|root| {
            let root = root.canonicalize().map_err(|e| e.to_string())?;
            if !root.is_dir() {
                return Err("local root must be a directory".to_string());
            }
            Ok(root)
        })
        .transpose()?;
    let http = reqwest::Client::builder()
        .user_agent(concat!("gdl-web/", env!("CARGO_PKG_VERSION")))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| e.to_string())?;
    let catalog = Catalog::load(&config.catalog).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&config.target)
        .map_err(|e| format!("{}: {e}", config.target.display()))?;
    let composer = Arc::new(Composer {
        config,
        http,
        catalog: RwLock::new(catalog),
    });
    Ok(Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../assets/index.html")) }),
        )
        .route("/app.js", get(app_js))
        .route(
            "/vendor/mermaid.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript")],
                    include_str!("../assets/vendor/mermaid.js"),
                )
            }),
        )
        .route(
            "/style.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css")],
                    include_str!("../assets/style.css"),
                )
            }),
        )
        .route("/api/catalog", get(entries).post(imports::add))
        .route("/api/catalog/clear", axum::routing::post(clear_catalog))
        .route("/api/local", get(imports::browse))
        .route("/api/crates", get(imports::search))
        .route("/api/check", axum::routing::post(check))
        .route("/api/export", axum::routing::post(export))
        .route("/api/deploy", axum::routing::post(deploy))
        .with_state(composer))
}

async fn app_js() -> Response {
    (
        [(header::CONTENT_TYPE, "text/javascript")],
        include_str!("../assets/app.js"),
    )
        .into_response()
}

type Shared = State<Arc<Composer>>;

/// Every entry, as its file holds it.
async fn clear_catalog(State(composer): Shared) -> Response {
    let result = tokio::task::spawn_blocking(move || -> Result<Option<PathBuf>, String> {
        let mut catalog = composer.catalog.write().map_err(|e| e.to_string())?;
        let path = &composer.config.catalog;
        let backup = if path.exists() {
            let suffix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_nanos();
            let backup = path.with_extension(format!("cleared-{suffix}"));
            std::fs::rename(path, &backup).map_err(|e| e.to_string())?;
            Some(backup)
        } else {
            None
        };
        *catalog = Catalog::load(path).map_err(|e| e.to_string())?;
        Ok(backup)
    })
    .await;
    match result {
        Ok(Ok(backup)) => Json(json!({"backup":backup})).into_response(),
        Ok(Err(error)) => problems(StatusCode::INTERNAL_SERVER_ERROR, vec![error]),
        Err(error) => problems(StatusCode::INTERNAL_SERVER_ERROR, vec![error.to_string()]),
    }
}

async fn entries(State(composer): Shared) -> Response {
    let catalog = composer.catalog.read().expect("the catalogue lock");
    let entries: Vec<&gdl_extract::Entry> = catalog.entries().collect();
    let url = "https://github.com/ryanolson/gdl";
    let revision = match &composer.config.gdl {
        Gdl::Git {
            url: configured,
            rev,
        } if configured.trim_end_matches(".git") == url => rev.as_str(),
        _ => "e2813e7f9af25a3a09f078b83360e0668d785b44",
    };
    Json(
        json!({ "entries": entries, "local_enabled": composer.config.local_root.is_some(),
        "example": {"source":"git", "url":url, "revision":revision, "path":"examples/api"} }),
    )
    .into_response()
}

/// A draft's graph file and spec, and what generating them would find:
/// every problem, or the crossing report and the placed graph.
async fn check(State(composer): Shared, Json(draft): Json<Draft>) -> Response {
    let checked = tokio::task::spawn_blocking(move || {
        let (rendered, spec) = prepare(&composer, &draft)?;
        let check = with_offers(&composer, |offers| {
            gdl_gen::check(&spec, &composer.config.target, offers)
        })
        .map_err(|e| {
            (
                Some(rendered.clone()),
                e.to_string().lines().map(str::to_string).collect(),
            )
        })?;
        Ok::<_, (Option<Rendered>, Vec<String>)>((rendered, check))
    })
    .await
    .expect("the check's thread");
    match checked {
        Ok((rendered, check)) => Json(json!({
            "ok": true,
            "graph": rendered.graph,
            "spec": rendered.spec,
            "crossings": check.crossings,
            "mermaid": check.mermaid,
        }))
        .into_response(),
        Err((rendered, problems)) => Json(json!({
            "ok": false,
            "graph": rendered.as_ref().map(|r| r.graph.clone()),
            "spec": rendered.as_ref().map(|r| r.spec.clone()),
            "problems": problems,
        }))
        .into_response(),
    }
}

/// The draft's generated crates, as `cargo gdl export` writes them.
async fn export(State(composer): Shared, Json(draft): Json<Draft>) -> Response {
    let exported = tokio::task::spawn_blocking(move || {
        let (_, spec) = prepare(&composer, &draft).map_err(|(_, problems)| problems)?;
        with_offers(&composer, |offers| {
            gdl_gen::export(&spec, &composer.config.target, offers)
        })
        .map_err(|e| {
            e.to_string()
                .lines()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .map(|bytes| (draft.name, bytes))
    })
    .await
    .expect("the export's thread");
    match exported {
        Ok((name, bytes)) => (
            [
                (header::CONTENT_TYPE, "application/gzip".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{name}.tar.gz\""),
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(found) => problems(StatusCode::UNPROCESSABLE_ENTITY, found),
    }
}

/// Preview and archive of a Compose build context with fetchable dependencies.
async fn deploy(State(composer): Shared, Json(draft): Json<Draft>) -> Response {
    let exported = tokio::task::spawn_blocking(move || {
        let (_, spec) = prepare(&composer, &draft).map_err(|(_, problems)| problems)?;
        gdl_gen::compose_bundle(&spec, &composer.config.target)
            .map_err(|error| vec![error.to_string()])
    })
    .await;
    match exported {
        Ok(Ok(bundle)) => {
            use base64::Engine;
            Json(serde_json::json!({
                "files": bundle.files,
                "archive": base64::engine::general_purpose::STANDARD.encode(bundle.archive),
            }))
            .into_response()
        }
        Ok(Err(errors)) => problems(StatusCode::UNPROCESSABLE_ENTITY, errors),
        Err(error) => problems(StatusCode::INTERNAL_SERVER_ERROR, vec![error.to_string()]),
    }
}

/// The draft rendered, and its spec read from the text: nothing is
/// written, so drafts leave nothing behind. The target directory stands for
/// the spec's own; paths in the spec are absolute or versions.
fn prepare(
    composer: &Composer,
    draft: &Draft,
) -> Result<(Rendered, gdl_gen::Spec), (Option<Rendered>, Vec<String>)> {
    let rendered = {
        let catalog = composer.catalog.read().expect("the catalogue lock");
        draft
            .render(&catalog, &composer.config.gdl)
            .map_err(|p| (None, p))?
    };
    match gdl_gen::Spec::from_text(&rendered.spec, &rendered.graph, &composer.config.target) {
        Ok(spec) => Ok((rendered, spec)),
        Err(error) => Err((Some(rendered), vec![error.to_string()])),
    }
}

/// `f` with the catalogue's offers, lent under its read lock: each check
/// and export reads the catalogue as one version of it, and copies nothing.
fn with_offers<T>(composer: &Composer, f: impl FnOnce(gdl_gen::Offers<'_>) -> T) -> T {
    let catalog = composer.catalog.read().expect("the catalogue lock");
    let offers: Vec<&gdl_gen::Offer> = catalog.entries().map(|e| &e.offer).collect();
    f(gdl_gen::Offers::Given(&offers))
}

fn problems(status: StatusCode, problems: Vec<String>) -> Response {
    (status, Json(json!({ "ok": false, "problems": problems }))).into_response()
}
