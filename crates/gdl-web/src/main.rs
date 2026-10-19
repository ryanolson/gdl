//! `gdl-web`: serve the composer.
//!
//! ```text
//! gdl-web --catalog <dir> [--listen <address>] [--target-dir <dir>]
//!         (--gdl-path <dir> | --gdl-git <url> --gdl-rev <rev>)
//! ```
//!
//! It listens on `127.0.0.1:8080` unless told otherwise, prints the
//! address it serves, and adds crates to the catalogue in `--catalog`.
//! Crates are fetched, and drafts written, under `--target-dir`
//! (`target` here by default). Generated crates take gdl from the path or
//! git revision given.

use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "usage: gdl-web --catalog <dir> [--listen <address>] [--target-dir <dir>] (--gdl-path <dir> | --gdl-git <url> --gdl-rev <rev>)";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gdl-web: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let (mut catalog, mut listen, mut target) = (None, "127.0.0.1:8080".to_string(), None);
    let (mut path, mut git, mut rev) = (None, None, None);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or_else(|| USAGE.to_string());
        match arg.as_str() {
            "--catalog" => catalog = Some(PathBuf::from(value()?)),
            "--listen" => listen = value()?,
            "--target-dir" => target = Some(PathBuf::from(value()?)),
            "--gdl-path" => path = Some(PathBuf::from(value()?)),
            "--gdl-git" => git = Some(value()?),
            "--gdl-rev" => rev = Some(value()?),
            _ => return Err(format!("unexpected argument {arg:?}\n{USAGE}")),
        }
    }
    let gdl = match (path, git, rev) {
        (Some(path), None, None) => gdl_web::Gdl::Path(
            std::fs::canonicalize(&path).map_err(|e| format!("{}: {e}", path.display()))?,
        ),
        (None, Some(url), Some(rev)) => gdl_web::Gdl::Git { url, rev },
        _ => return Err(USAGE.to_string()),
    };
    let config = gdl_web::Config {
        catalog: catalog.ok_or_else(|| USAGE.to_string())?,
        target: target.unwrap_or_else(|| PathBuf::from("target")),
        gdl,
    };
    let router = gdl_web::router(config)?;
    let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind(&listen)
            .await
            .map_err(|e| format!("{listen}: {e}"))?;
        let address = listener.local_addr().map_err(|e| e.to_string())?;
        println!("gdl-web: serving http://{address}/");
        axum::serve(listener, router)
            .await
            .map_err(|e| e.to_string())
    })
}
