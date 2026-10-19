//! The composer: a page that builds a graph from the nodes and messages
//! of a working catalogue, and the server that checks and exports it.
//!
//! ```text
//! page --GET /api/catalog---------------> the catalogue's entries
//!      --POST /api/catalog {crate}------> fetch, read, add (the only call that fetches)
//!      --POST /api/check {draft}--------> graph file + spec, checked: problems, or crossings and diagram
//!      --POST /api/export {draft}-------> the generated crates as a .tar.gz
//! ```
//!
//! The page offers only ports of one message to join, but decides nothing:
//! every verdict is the server's, from the same rules `cargo gdl` applies.
//! Nothing is compiled, so a crate that needs CUDA composes like any other.

mod draft;

use std::path::{Path, PathBuf};
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
    /// Where crates are fetched, and drafts written.
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
    /// `name@version`.
    #[serde(rename = "crate")]
    krate: String,
}

/// Fetch a crate version, read it, and add it. A crate read with problems
/// adds nothing, and the problems are the answer.
async fn add(State(composer): Shared, Json(add): Json<Add>) -> Response {
    let Some((name, version)) = add.krate.split_once('@') else {
        return problems(
            StatusCode::BAD_REQUEST,
            vec![format!("`{}` is not `name@version`", add.krate)],
        );
    };
    let krate = gdl_gen::Crate::Registry {
        name: name.to_string(),
        version: version.to_string(),
    };
    let blocking = Arc::clone(&composer);
    // Fetching runs cargo, and adding writes a file: off the async threads.
    let added = tokio::task::spawn_blocking(move || {
        let extracted =
            gdl_gen::extract(&krate, &blocking.config.target).map_err(|e| e.to_string())?;
        gdl_gen::add_to_catalog(
            &blocking.config.catalog,
            &extracted,
            std::time::SystemTime::now(),
        )
        .map_err(|e| e.to_string())?;
        Catalog::load(&blocking.config.catalog).map_err(|e| e.to_string())
    })
    .await
    .expect("the add's thread");
    match added {
        Ok(catalog) => {
            *composer.catalog.write().expect("the catalogue lock") = catalog;
            Json(json!({ "added": add.krate })).into_response()
        }
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
        let catalog = composer.catalog.read().expect("the catalogue lock");
        let offers: Vec<gdl_gen::Offer> = catalog.entries().map(|e| e.offer.clone()).collect();
        drop(catalog);
        let check = gdl_gen::check(
            &spec,
            &composer.config.target,
            gdl_gen::Offers::Given(&offers),
        )
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
        let catalog = composer.catalog.read().expect("the catalogue lock");
        let offers: Vec<gdl_gen::Offer> = catalog.entries().map(|e| e.offer.clone()).collect();
        drop(catalog);
        gdl_gen::export(
            &spec,
            &composer.config.target,
            gdl_gen::Offers::Given(&offers),
        )
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

/// The draft rendered, and written where its spec can be loaded: a
/// directory named for its content, so one draft is always one path.
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
    let dir = drafts(&composer.config.target).join(format!("{:016x}", hash(&rendered)));
    let write = |path: &Path, text: &str| {
        std::fs::write(path, text).map_err(|e| format!("{}: {e}", path.display()))
    };
    let written = std::fs::create_dir_all(&dir)
        .map_err(|e| format!("{}: {e}", dir.display()))
        .and_then(|()| write(&dir.join("graph.gdl"), &rendered.graph))
        .and_then(|()| write(&dir.join("gdl.toml"), &rendered.spec))
        .and_then(|()| gdl_gen::Spec::load(&dir.join("gdl.toml")).map_err(|e| e.to_string()));
    match written {
        Ok(spec) => Ok((rendered, spec)),
        Err(error) => Err((Some(rendered), vec![error])),
    }
}

fn drafts(target: &Path) -> PathBuf {
    target.join("gdl-web").join("drafts")
}

fn hash(rendered: &Rendered) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    rendered.graph.hash(&mut hasher);
    rendered.spec.hash(&mut hasher);
    hasher.finish()
}

fn problems(status: StatusCode, problems: Vec<String>) -> Response {
    (status, Json(json!({ "ok": false, "problems": problems }))).into_response()
}
