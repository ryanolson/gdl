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

/// What the child reads: the crate, where cargo may fetch, and how long
/// cargo may take.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ChildRequest {
    pub(crate) krate: gdl_gen::Crate,
    pub(crate) target: PathBuf,
    pub(crate) cargo_deadline_secs: u64,
}

/// How much a child reading one crate may take: enough for any crate a
/// person wrote, and the end of one a repository made to exhaust the
/// reader. The memory limit is advisory where the shell cannot set it.
const CHILD_MEMORY_KIB: u64 = 4 << 20;
const CHILD_CPU_SECONDS: u64 = 600;
const CHILD_DEADLINE: std::time::Duration = std::time::Duration::from_secs(360);
/// How long the child's cargo may take, within the child's own deadline.
const CARGO_DEADLINE_SECS: u64 = 300;

/// Read `krate` in a child process, with its memory and time limited, in
/// its own process group: a crate that makes the reader run out of stack
/// or memory, or never finish, is refused, and the server goes on.
async fn extract(
    composer: Arc<Composer>,
    krate: gdl_gen::Crate,
) -> Result<gdl_extract::Entry, String> {
    let request = serde_json::to_string(&ChildRequest {
        krate,
        target: composer.config.target.clone(),
        cargo_deadline_secs: CARGO_DEADLINE_SECS,
    })
    .map_err(|e| e.to_string())?;
    let mut command = tokio::process::Command::new("sh");
    command.args([
        "-c",
        &format!(
            "ulimit -v {CHILD_MEMORY_KIB} 2>/dev/null; ulimit -t {CHILD_CPU_SECONDS} 2>/dev/null; exec \"$0\" extract-child \"$1\""
        ),
    ]);
    command.arg(&composer.config.extractor).arg(request);
    let output = run_bounded(command, CHILD_DEADLINE).await.map_err(|e| {
        if e.starts_with(OUTPUT_OVERFLOW) {
            format!(
                "The crate offers more than an import may hold ({} MiB).",
                gdl_extract::OUTPUT_LIMIT >> 20
            )
        } else {
            e
        }
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            "The crate could not be read: the reader was stopped before it finished, so the crate needs more memory or time than an import is allowed.".to_string()
        } else {
            stderr
        });
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("the reader's answer did not parse: {e}"))
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
    // The manifest is read from the fetched objects, not from a working
    // tree: nothing is checked out, so a symlink is its link text (and
    // refused by its mode), and the blob's size is known before it is read.
    let repo = tempfile::Builder::new()
        .prefix("gdl-web-git-")
        .tempdir()
        .map_err(|e| e.to_string())?;
    git_command(repo.path(), &["init", "--quiet"]).await?;
    git_fetch(repo.path(), url, revision).await?;
    let commit = git_command(repo.path(), &["rev-parse", "FETCH_HEAD"])
        .await?
        .trim()
        .to_string();
    let manifest_path = path
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .chain(["Cargo.toml"])
        .collect::<Vec<_>>()
        .join("/");
    let listed = git_command(
        repo.path(),
        &["ls-tree", "FETCH_HEAD", "--", &manifest_path],
    )
    .await?;
    let mode = listed.split_whitespace().next().unwrap_or_default();
    match mode {
        "100644" | "100755" => {}
        "" => {
            return Err(
                "No Cargo.toml at this path; choose the package directory within the repository."
                    .into(),
            );
        }
        _ => return Err("Cargo.toml is not a regular file in this revision.".into()),
    }
    let object = format!("FETCH_HEAD:{manifest_path}");
    let size: u64 = git_command(repo.path(), &["cat-file", "-s", &object])
        .await?
        .trim()
        .parse()
        .map_err(|_| "Cargo.toml has no size.".to_string())?;
    if size > MANIFEST_LIMIT {
        return Err("Cargo.toml is larger than 1 MiB.".into());
    }
    let manifest = git_command(repo.path(), &["cat-file", "-p", &object]).await?;
    let manifest: toml::Value =
        toml::from_str(&manifest).map_err(|e: toml::de::Error| e.to_string())?;
    let name = manifest
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(toml::Value::as_str)
        .ok_or("This is a workspace root. Enter the path to a package within it.")?
        .to_string();
    drop(repo);
    let krate = gdl_gen::Crate::Git {
        name,
        url: url.to_string(),
        rev: commit,
        path: path.to_path_buf(),
    };
    extract(composer.clone(), krate).await
}

async fn git_fetch(dir: &Path, url: &str, revision: &str) -> Result<String, String> {
    // An exact identity rewrite wins over any HTTPS-to-SSH prefix that
    // reached git despite its configuration being set aside. The package's
    // own Git dependencies are fetched by cargo, which the server's child
    // confines the same way (`gdl_extract::Cargo::confined_as_child`).
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

/// The most a manifest may hold: a `Cargo.toml` is a page, not a megabyte.
const MANIFEST_LIMIT: u64 = 1 << 20;

/// Run git on a repository the user named, under the same confinement as
/// cargo's git: no configuration or credential helper of the user's, no
/// prompt, HTTP(S) only; in its own process group, which dies with the
/// command, at the deadline or when the request is abandoned, so that a
/// transfer no one waits for does not keep its `remote-http` helper.
async fn git_command(dir: &Path, args: &[&str]) -> Result<String, String> {
    let mut command = tokio::process::Command::new("git");
    command
        .current_dir(dir)
        .envs(gdl_extract::confined_git_env())
        .args(args);
    let output = run_bounded(command, std::time::Duration::from_secs(120)).await?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Run `command` to completion within `deadline`. Its answer, on stdout,
/// is refused past `gdl_extract::OUTPUT_LIMIT` rather than cut; its
/// messages, on stderr, are kept to the limit and the rest discarded as it
/// arrives, so a host that never stops talking fills a pipe, not the
/// server's memory. The command's process group ends with the command,
/// whatever ended it: when it finishes, at the deadline, or when this
/// future is dropped before it finished.
async fn run_bounded(
    mut command: tokio::process::Command,
    deadline: std::time::Duration,
) -> Result<std::process::Output, String> {
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().map_err(|e| e.to_string())?;
    let group = Group(child.id());
    let finished = async {
        let (stdout, stderr, status) = tokio::join!(
            keep(child.stdout.take()),
            keep(child.stderr.take()),
            child.wait()
        );
        status.map(|status| std::process::Output {
            status,
            stdout,
            stderr,
        })
    };
    let mut output = match tokio::time::timeout(deadline, finished).await {
        Ok(output) => output.map_err(|e| e.to_string())?,
        Err(_) => return Err(format!("The operation timed out after {deadline:?}.")),
    };
    // Whatever ended the command, nothing it started outlives its answer:
    // a child killed from outside while its cargo ran would otherwise
    // leave that cargo, with no deadline, holding cargo's lock.
    drop(group);
    // An answer that was cut would be parsed as something else; a message
    // that was cut is still a message.
    if output.stdout.len() as u64 > gdl_extract::OUTPUT_LIMIT {
        return Err(format!(
            "{OUTPUT_OVERFLOW} {} MiB.",
            gdl_extract::OUTPUT_LIMIT >> 20
        ));
    }
    output.stderr.truncate(gdl_extract::OUTPUT_LIMIT as usize);
    Ok(output)
}

const OUTPUT_OVERFLOW: &str = "The answer was larger than";

/// The first `OUTPUT_LIMIT` bytes of `stream` and one more, so that the
/// caller can tell a stream that was cut from one that fit; the rest is
/// read and dropped.
async fn keep<R: tokio::io::AsyncRead + Unpin>(stream: Option<R>) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut kept = Vec::new();
    if let Some(mut stream) = stream {
        let _ = (&mut stream)
            .take(gdl_extract::OUTPUT_LIMIT + 1)
            .read_to_end(&mut kept)
            .await;
        let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
    }
    kept
}

/// The process group of a git command, ended when the command's future
/// is dropped before the command finished: at the deadline, or when the
/// request it served was abandoned.
struct Group(Option<u32>);

impl Drop for Group {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            gdl_extract::kill_process_group(pid);
        }
    }
}

#[cfg(test)]
mod tests {
    /// A command's error output is kept only up to the limit, the rest read
    /// and dropped, so a host that never stops talking costs a pipe, not
    /// the server's memory; an answer past the limit is refused as such,
    /// never cut and parsed.
    #[tokio::test]
    async fn a_command_that_never_stops_talking_is_bounded() {
        let mut command = tokio::process::Command::new("sh");
        command.args([
            "-c",
            "head -c 50000000 /dev/zero | tr '\\0' x >&2; echo fits",
        ]);
        let output = super::run_bounded(command, std::time::Duration::from_secs(60))
            .await
            .expect("ran");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"fits\n");
        assert_eq!(output.stderr.len() as u64, gdl_extract::OUTPUT_LIMIT);
        let mut command = tokio::process::Command::new("sh");
        command.args(["-c", "head -c 50000000 /dev/zero | tr '\\0' y"]);
        let Err(error) = super::run_bounded(command, std::time::Duration::from_secs(60)).await
        else {
            panic!("fifty megabytes of answer were kept or cut");
        };
        assert!(error.starts_with(super::OUTPUT_OVERFLOW), "{error}");
    }

    /// A fetch abandoned with its request takes its helpers with it: the
    /// connections git held are closed once the future is dropped.
    #[tokio::test]
    async fn an_abandoned_fetch_ends_its_process_group() {
        use std::io::Read;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a silent port");
        let url = format!(
            "http://{}/silent.git",
            listener.local_addr().expect("address")
        );
        let held: std::sync::Arc<std::sync::Mutex<Vec<std::net::TcpStream>>> = Default::default();
        let accepted = std::sync::Arc::clone(&held);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                accepted.lock().expect("held").push(stream);
            }
        });
        let dir = tempfile::Builder::new()
            .prefix("gdl-web-git-")
            .tempdir()
            .expect("directory");
        super::git_command(dir.path(), &["init", "--quiet"])
            .await
            .expect("init");
        let abandoned = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            super::git_fetch(dir.path(), &url, "main"),
        )
        .await;
        assert!(abandoned.is_err(), "a silent host answered");
        let streams = std::mem::take(&mut *held.lock().expect("held"));
        assert!(!streams.is_empty(), "git never connected");
        for mut stream in streams {
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .expect("a read timeout");
            let mut rest = Vec::new();
            stream
                .read_to_end(&mut rest)
                .expect("the connection git held is still open");
        }
    }

    #[tokio::test]
    async fn https_import_overrides_ssh_url_rewrites() {
        let dir = tempfile::Builder::new()
            .prefix("gdl-web-git-")
            .tempdir()
            .expect("directory");
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
