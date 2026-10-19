//! The composer: a page that builds a graph from the nodes and messages
//! of a working catalogue, and the server that checks and exports it.
//!
//! ```text
//! page --GET /api/catalog---------------> the catalogue's entries
//!      --POST /api/catalog {crate}------> read (a directory, or a version fetched first), add
//!      --POST /api/check {draft}--------> graph file + spec, checked: problems, or crossings and diagram
//!      --POST /api/export {draft}-------> the generated crates as a .tar.gz
//! ```
//!
//! The page offers only ports of one message to join, but decides nothing:
//! every verdict is the server's, from the same rules `cargo gdl` applies.
//! Nothing is compiled, so a crate that needs CUDA composes like any other.

mod draft;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::Router;
use axum::extract::{Json, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use gdl_extract::Catalog;
use serde::Deserialize;
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
}

struct Composer {
    config: Config,
    catalog: RwLock<Catalog>,
}

/// The server's routes, over the catalogue `config` names, loaded now.
pub fn router(config: Config) -> Result<Router, String> {
    let catalog = Catalog::load(&config.catalog).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&config.target)
        .map_err(|e| format!("{}: {e}", config.target.display()))?;
    let composer = Arc::new(Composer {
        config,
        catalog: RwLock::new(catalog),
    });
    Ok(Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../assets/index.html")) }),
        )
        .route("/app.js", get(app_js))
        .route("/api/catalog", get(entries).post(add))
        .route("/api/check", axum::routing::post(check))
        .route("/api/export", axum::routing::post(export))
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
async fn entries(State(composer): Shared) -> Response {
    let catalog = composer.catalog.read().expect("the catalogue lock");
    let entries: Vec<&gdl_extract::Entry> = catalog.entries().collect();
    Json(json!({ "entries": entries })).into_response()
}

#[derive(Deserialize)]
struct Add {
    /// `name@version` on crates.io, or a directory on the server's machine.
    #[serde(rename = "crate")]
    krate: String,
}

/// Read a crate, fetching a version first, and add it. A crate read with
/// problems adds nothing, and the problems are the answer. A directory is
/// one on the server's machine: the composer serves the machine it runs on.
async fn add(State(composer): Shared, Json(add): Json<Add>) -> Response {
    let krate = gdl_gen::Crate::parse(&add.krate);
    // Fetching runs cargo, and adding writes a file: off the async threads.
    // The add is made to the served catalogue, under its lock, so two adds
    // at once both stay served; `Catalog::add` keeps whichever entry was
    // written first.
    let added = tokio::task::spawn_blocking(move || {
        let extracted =
            gdl_gen::extract(&krate, &composer.config.target).map_err(|e| e.to_string())?;
        let entry = extracted.entry(std::time::SystemTime::now());
        let mut catalog = composer.catalog.write().expect("the catalogue lock");
        catalog.add(entry).map(|_| ()).map_err(|e| e.to_string())
    })
    .await
    .expect("the add's thread");
    match added {
        Ok(()) => Json(json!({ "added": add.krate })).into_response(),
        Err(error) => problems(
            StatusCode::UNPROCESSABLE_ENTITY,
            error.lines().map(str::to_string).collect(),
        ),
    }
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
