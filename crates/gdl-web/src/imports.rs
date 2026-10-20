use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::extract::{Json, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::{Composer, Shared, problems};

#[derive(Deserialize)]
#[serde(tag = "source", rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum Add {
    Registry {
        name: String,
        version: String,
    },
    Local {
        path: PathBuf,
    },
    Git {
        url: String,
        revision: String,
        #[serde(default)]
        path: PathBuf,
    },
}

pub(crate) async fn add(State(composer): Shared, Json(add): Json<Add>) -> Response {
    let entry = match add {
        Add::Local { path } => {
            let path = match local_path(&composer, &path) {
                Ok(path) => path,
                Err(error) => return problems(StatusCode::FORBIDDEN, vec![error.into()]),
            };
            extract(composer.clone(), gdl_gen::Crate::Path(path)).await
        }
        Add::Registry { name, version } => {
            if name.is_empty() || version.is_empty() {
                return problems(
                    StatusCode::BAD_REQUEST,
                    vec![format!(
                        "`{name}@{version}` is neither a directory nor `name@version`"
                    )],
                );
            }
            extract(composer.clone(), gdl_gen::Crate::Registry { name, version }).await
        }
        Add::Git {
            url,
            revision,
            path,
        } => git(&composer, &url, &revision, &path).await,
    };
    let entry = match entry {
        Ok(entry) => entry,
        Err(error) => return problems(StatusCode::UNPROCESSABLE_ENTITY, vec![error]),
    };
    let added = entry.key();
    let saved = tokio::task::spawn_blocking(move || {
        composer
            .catalog
            .write()
            .expect("catalog lock")
            .add(entry)
            .map(|_| ())
            .map_err(|e| e.to_string())
    })
    .await;
    match saved {
        Ok(Ok(_)) => Json(json!({"added":added})).into_response(),
        Ok(Err(error)) => problems(StatusCode::UNPROCESSABLE_ENTITY, vec![error]),
        Err(error) => problems(StatusCode::INTERNAL_SERVER_ERROR, vec![error.to_string()]),
    }
}

async fn extract(
    composer: Arc<Composer>,
    krate: gdl_gen::Crate,
) -> Result<gdl_extract::Entry, String> {
    tokio::task::spawn_blocking(move || {
        gdl_gen::extract(&krate, &composer.config.target)
            .map(|extracted| extracted.entry(std::time::SystemTime::now()))
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}

fn local_path(composer: &Composer, path: &Path) -> Result<PathBuf, &'static str> {
    let refuse = || "Local imports require --local-root and must stay inside that directory.";
    let root = composer.config.local_root.as_ref().ok_or_else(refuse)?;
    let path = root.join(path).canonicalize().map_err(|_| refuse())?;
    if !path.starts_with(root) || !path.is_dir() {
        return Err(refuse());
    }
    Ok(path)
}

#[derive(Deserialize)]
pub(crate) struct Browse {
    #[serde(default)]
    path: PathBuf,
}

pub(crate) async fn browse(State(composer): Shared, Query(query): Query<Browse>) -> Response {
    let path = match local_path(&composer, &query.path) {
        Ok(path) => path,
        Err(error) => return problems(StatusCode::FORBIDDEN, vec![error.into()]),
    };
    let root = composer.config.local_root.clone().expect("enabled root");
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let relative = path.strip_prefix(&root).map_err(|e| e.to_string())?;
        let mut directories = Vec::new();
        for entry in std::fs::read_dir(&path).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || name == "target" { continue; }
            let Ok(resolved) = entry.path().canonicalize() else { continue; };
            if resolved.is_dir() && resolved.starts_with(&root) {
                directories.push(json!({"name":name,"path":relative.join(entry.file_name())}));
            }
        }
        directories.sort_by(|a,b| a["name"].as_str().cmp(&b["name"].as_str()));
        let package = std::fs::read_to_string(path.join("Cargo.toml")).ok()
            .and_then(|text| toml::from_str::<toml::Value>(&text).ok())
            .and_then(|v| v.get("package")?.get("name")?.as_str().map(str::to_string));
        Ok(json!({"path":relative,"parent":relative.parent(),"package":package,"directories":directories}))
    }).await;
    match result {
        Ok(Ok(body)) => Json(body).into_response(),
        _ => problems(
            StatusCode::INTERNAL_SERVER_ERROR,
            vec!["Could not read this directory.".into()],
        ),
    }
}

#[derive(Deserialize)]
pub(crate) struct Search {
    q: String,
}

pub(crate) async fn search(State(composer): Shared, Query(query): Query<Search>) -> Response {
    let query = query.q.trim();
    if query.len() < 2 {
        return Json(json!({"crates":[]})).into_response();
    }
    if query.len() > 100 {
        return problems(
            StatusCode::BAD_REQUEST,
            vec!["Search is limited to 100 characters.".into()],
        );
    }
    let result = async {
        composer
            .http
            .get("https://crates.io/api/v1/crates")
            .query(&[("q", query), ("per_page", "10")])
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await
    }
    .await;
    match result {
        Ok(body) => Json(body).into_response(),
        Err(_) => problems(
            StatusCode::BAD_GATEWAY,
            vec!["crates.io search is unavailable. Try again.".into()],
        ),
    }
}

async fn git(
    composer: &Arc<Composer>,
    url: &str,
    revision: &str,
    path: &Path,
) -> Result<gdl_extract::Entry, String> {
    let parsed = reqwest::Url::parse(url).map_err(|_| "Enter an HTTP(S) repository URL.")?;
    if !matches!(parsed.scheme(), "https" | "http")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(
            "Enter an HTTP(S) repository URL without credentials, query, or fragment.".into(),
        );
    }
    if revision.is_empty() || revision.starts_with('-') || revision.chars().any(char::is_whitespace)
    {
        return Err("Enter a branch or commit.".into());
    }
    if path.components().any(|part| {
        !matches!(
            part,
            std::path::Component::Normal(_) | std::path::Component::CurDir
        )
    }) {
        return Err("Package path must be relative to the repository.".into());
    }
    let checkout = tempfile::tempdir().map_err(|e| e.to_string())?;
    git_command(checkout.path(), &["init", "--quiet"]).await?;
    git_fetch(checkout.path(), url, revision).await?;
    git_command(
        checkout.path(),
        &["checkout", "--quiet", "--detach", "FETCH_HEAD"],
    )
    .await?;
    let commit = git_command(checkout.path(), &["rev-parse", "HEAD"])
        .await?
        .trim()
        .to_string();
    let package = checkout
        .path()
        .join(path)
        .canonicalize()
        .map_err(|_| "Package directory does not exist in this revision.")?;
    if !package.starts_with(checkout.path().canonicalize().map_err(|e| e.to_string())?) {
        return Err("Package directory points outside the repository.".into());
    }
    let manifest = std::fs::read_to_string(package.join("Cargo.toml")).map_err(
        |_| "No Cargo.toml at this path; choose the package directory within the repository.",
    )?;
    let manifest: toml::Value =
        toml::from_str(&manifest).map_err(|e: toml::de::Error| e.to_string())?;
    let name = manifest
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(toml::Value::as_str)
        .ok_or("This is a workspace root. Enter the path to a package within it.")?
        .to_string();
    let url = url.to_string();
    let target = composer.config.target.clone();
    tokio::task::spawn_blocking(move || {
        let workspace = gdl_extract::Workspace::fetch_git(&name, &url, &commit, &target)
            .map_err(|e| e.to_string())?;
        let extracted = workspace.extract(&name).map_err(|e| e.to_string())?;
        if !extracted.problems.is_empty() {
            return Err(extracted.problems.join("\n"));
        }
        Ok(gdl_extract::Entry {
            added: gdl_extract::rfc3339(std::time::SystemTime::now()),
            checksum: None,
            offer: extracted.offer,
        })
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn git_fetch(dir: &Path, url: &str, revision: &str) -> Result<String, String> {
    // An exact identity rewrite wins over workstation-wide HTTPS-to-SSH prefixes.
    // Keep the import on its validated HTTP transport without changing user config.
    let rewrite = format!("url.{url}.insteadOf={url}");
    git_command(
        dir,
        &[
            "-c",
            &rewrite,
            "fetch",
            "--quiet",
            "--depth=1",
            "--",
            url,
            revision,
        ],
    )
    .await
}

async fn git_command(dir: &Path, args: &[&str]) -> Result<String, String> {
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        tokio::process::Command::new("git")
            .current_dir(dir)
            .env("GIT_TERMINAL_PROMPT", "0")
            .args([
                "-c",
                "protocol.allow=never",
                "-c",
                "protocol.https.allow=always",
                "-c",
                "protocol.http.allow=always",
            ])
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| "Git operation timed out.".to_string())?
    .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn https_import_overrides_ssh_url_rewrites() {
        let dir = tempfile::tempdir().expect("directory");
        super::git_command(dir.path(), &["init", "--quiet"])
            .await
            .expect("init");
        super::git_command(
            dir.path(),
            &[
                "config",
                "url.ssh://example.invalid/.insteadOf",
                "http://127.0.0.1:",
            ],
        )
        .await
        .expect("rewrite");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("port");
        let url = format!("http://{}/repo", listener.local_addr().expect("address"));
        drop(listener);
        let error = super::git_fetch(dir.path(), &url, "main")
            .await
            .expect_err("closed port");
        assert!(!error.contains("transport 'ssh'"), "{error}");
        assert!(
            error.contains("Failed to connect") || error.contains("Couldn't connect"),
            "{error}"
        );
    }
}
