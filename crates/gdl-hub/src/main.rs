//! Run one graph discovery hub: `gdl-hub deployment.json`. A pool's hub
//! takes its pool's settings from `GDL_POOL__*` variables. The hub listens
//! on the deployment's ports unless `GDL_HUB__PUBLIC_PORT` and
//! `GDL_HUB__CONTROL_PORT` name others (0 lets the OS choose), the
//! variables a segment reads for where its hub is. Once it serves, it
//! prints the addresses it bound on stderr. Log events go to stderr, at
//! the level `RUST_LOG` sets (`warn` by default).
//!
//! `gdl-hub --tables`, in a build with the `dynamodb` feature, is the
//! DynamoDB directory's tables step, which a deployment runs once before
//! its hubs: it makes the tables that are absent and checks the keys of
//! those that exist, from the `GDL_POOL__*` variables of the `dynamodb`
//! backend, and exits.
//!
//! `gdl-hub --resolve <class>/<pool> --write <file>` is for a job that
//! joins a running pool: it reads the pool's record from the directory the
//! `GDL_POOL__*` variables name (`dir` or `dynamodb`), waits for the
//! record's heartbeat, and writes `[hub]` with its `host` and
//! `public_port`, which the segment takes as an override file. A pool it
//! cannot join writes nothing, and exits non-zero with one line.
use gdl_model::{DeploymentSpec, HUB_CONTROL_PORT, HUB_PUBLIC_PORT};
use tokio::signal::unix::{SignalKind, signal};
use velo_hub::HubConfig;

/// The port that variable `name` gives, else `default`.
fn port(name: &str, default: u16) -> Result<u16, String> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|e| format!("{name}={value:?}: not a port: {e}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(format!("{name}: {e}")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The binary, not the `gdl-hub` library, installs the subscriber: a
    // program that serves the hub through the library keeps its own.
    // Colour off, though another crate may turn on the `ansi` feature:
    // escape codes are noise in a container's log.
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::WARN.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: gdl-hub deployment.json")?;
    if path == "--healthcheck" {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect_timeout(
            &(
                std::net::Ipv4Addr::LOCALHOST,
                port("GDL_HUB__PUBLIC_PORT", HUB_PUBLIC_PORT)?,
            )
                .into(),
            std::time::Duration::from_secs(1),
        )?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(1)))?;
        stream.write_all(b"GET /v1/health HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
        let mut response = [0; 12];
        stream.read_exact(&mut response)?;
        if !response.starts_with(b"HTTP/1.0 200") && !response.starts_with(b"HTTP/1.1 200") {
            return Err("hub is not healthy".into());
        }
        return Ok(());
    }
    if path == "--tables" {
        return tables().await;
    }
    if path == "--resolve" {
        return resolve().await;
    }
    let spec: DeploymentSpec = serde_json::from_slice(&std::fs::read(path)?)?;
    let spec = DeploymentSpec::new(spec.graph, spec.placement)?;
    let pool = gdl_hub::pool_from_env(&spec)?;
    let mut config = HubConfig::default();
    config.server.public_port = port("GDL_HUB__PUBLIC_PORT", HUB_PUBLIC_PORT)?;
    config.server.control_port = port("GDL_HUB__CONTROL_PORT", HUB_CONTROL_PORT)?;
    // SIGTERM as well as SIGINT, both taken before the hub serves. Docker
    // stops a container with SIGTERM, and the hub is the container's first
    // process, for which a signal it does not handle does nothing: a hub
    // that stopped only on SIGINT ran until Docker killed it, 10 s later.
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let hub = gdl_hub::hub_builder(config, spec, pool)
        .await?
        .serve()
        .await?;
    eprintln!(
        "gdl-hub: serving, public {}, control {}",
        hub.public_addr(),
        hub.control_addr()
    );
    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }
    hub.shutdown().await?;
    Ok(())
}

#[cfg(feature = "dynamodb")]
async fn tables() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for line in gdl_hub::ensure_tables_from_vars(std::env::vars_os()).await? {
        eprintln!("{line}");
    }
    Ok(())
}

#[cfg(not(feature = "dynamodb"))]
async fn tables() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Err("gdl-hub --tables: this gdl-hub is built without its `dynamodb` feature, which links the AWS SDK; install it with `cargo install gdl-hub --features dynamodb`".into())
}

/// `--resolve`: one line on stderr, and exit 1 when nothing was written.
async fn resolve() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match gdl_hub::resolve_from_args(std::env::args_os().skip(2), std::env::vars_os()).await {
        Ok(line) => {
            eprintln!("{line}");
            Ok(())
        }
        Err(error) => {
            eprintln!("gdl-hub --resolve: {error}");
            std::process::exit(1);
        }
    }
}
