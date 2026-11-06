//! Run one graph discovery hub: `gdl-hub deployment.json`. A pool's hub
//! takes its pool's settings from `GDL_POOL__*` variables. The hub listens
//! on the deployment's ports unless `GDL_HUB__PUBLIC_PORT` and
//! `GDL_HUB__CONTROL_PORT` name others (0 lets the OS choose), and keeps
//! an instance's registration 30 s without a heartbeat, pruning every 10 s,
//! unless `GDL_HUB__INSTANCE_LEASE_MS` and `GDL_HUB__PRUNE_MS` say
//! otherwise: the variables a segment reads as its `[hub]` table
//! ([`gdl_hub::hub_config_from_env`]). Once it serves, it
//! prints the addresses it bound on stderr. Log events go to stderr, at
//! the level `RUST_LOG` sets (`warn` by default).
//!
//! The hub stops on its first SIGTERM or SIGINT. With
//! `GDL_POOL__DRAIN_ON_STOP=true`, it first stops its pool
//! ([`gdl_hub::PoolStop::run`]), for at most `GDL_POOL__STOP_WAIT_MS`,
//! and says on stderr how that ended; a second signal cuts that short.
//! Once the hub has shut down, the process exits within
//! `BLOCKING_GRACE` even when a directory call has not returned.
//!
//! `gdl-hub --tables`, in a build with the `dynamodb` feature, is the
//! DynamoDB directory's tables step, which a deployment runs once before
//! its hubs: it makes the tables that are absent and checks the keys of
//! those that exist, from the `GDL_POOL__*` variables of the `dynamodb`
//! backend, and exits.
//!
//! `gdl-hub --resolve <class>/<pool> --write <file>` is for a job that
//! joins a running pool: it reads the pool's record from the directory the
//! `GDL_POOL__*` variables name (`dir`, `dynamodb`, or `etcd`), waits for the
//! record's heartbeat, and writes `[hub]` with its `host` and
//! `public_port`, which the segment takes as an override file. A pool it
//! cannot join writes nothing, and exits non-zero with one line.
use gdl_model::DeploymentSpec;
use tokio::signal::unix::{SignalKind, signal};

/// How long the hub's runtime waits, once the hub has shut down, for
/// the blocking calls still running, such as a `dir` directory's read of
/// a mount that does not answer. Such a call cannot be cancelled, and a
/// runtime that waited for it would keep the process alive past its stop
/// and past a second signal.
const BLOCKING_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime = tokio::runtime::Runtime::new()?;
    let served = runtime.block_on(serve());
    runtime.shutdown_timeout(BLOCKING_GRACE);
    served
}

async fn serve() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
                gdl_hub::hub_config_from_env()?.server.public_port,
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
    let drain_on_stop = pool.as_ref().and_then(|pool| pool.drain_on_stop);
    // Refused before the hub serves: a hub that cannot run its stop
    // before it must be gone must not start. Read only with
    // `drain_on_stop`, which the pool's settings require for it.
    let leave_at = match pool
        .as_ref()
        .and_then(|pool| pool.leave_by.zip(drain_on_stop))
    {
        Some((leave, wait)) => Some((leave_at(leave, wait)?, leave)),
        None => None,
    };
    let config = gdl_hub::hub_config_from_env()?;
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
    let left = async {
        match leave_at {
            Some((at, _)) => tokio::time::sleep_until(at).await,
            None => std::future::pending().await,
        }
    };
    // The leave point starts the stop a first signal starts, and is never
    // a second signal: one after it, such as a batch system's own stop at
    // the job's end, must not cut the stop it began.
    let mut signals = tokio::select! {
        _ = terminate.recv() => 1,
        _ = interrupt.recv() => 1,
        () = left => {
            if let Some((_, leave)) = leave_at {
                eprintln!(
                    "gdl-hub: stopping: `pool.leave_by` {} is the stop's wait and the margin away",
                    leave.unix_ms
                );
            }
            0
        }
    };
    let stop = hub
        .extension::<gdl_hub::GraphAdmission>()
        .and_then(|admission| admission.pool_stop());
    match (drain_on_stop, stop) {
        (Some(wait), Some(stop)) => {
            // A hub with a leave point ends its stop by the end that point
            // set, its margin before `leave_by`, even when it takes the
            // stop late: it has only the time left before then.
            let wait = match leave_at {
                Some((at, _)) => {
                    wait.min((at + wait).saturating_duration_since(tokio::time::Instant::now()))
                }
                None => wait,
            };
            eprintln!(
                "gdl-hub: stopping: stopping the pool, for at most {wait:?}; a second signal stops the hub at once"
            );
            let run = stop.run(wait);
            tokio::pin!(run);
            loop {
                tokio::select! {
                    stopped = &mut run => {
                        eprintln!("gdl-hub: stopped: {stopped}");
                        break;
                    }
                    _ = async {
                        tokio::select! {
                            _ = terminate.recv() => {}
                            _ = interrupt.recv() => {}
                        }
                    } => {
                        signals += 1;
                        if signals >= 2 {
                            eprintln!("gdl-hub: a second signal cut the pool's stop");
                            break;
                        }
                        eprintln!(
                            "gdl-hub: a first signal, after the leave point began the stop; a second stops the hub at once"
                        );
                    }
                }
            }
        }
        (Some(_), None) => eprintln!("gdl-hub: stopping at once: the hub runs no pool to stop"),
        (None, _) => {}
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

/// When a hub that must be gone by `leave` starts its stop, on the
/// monotonic clock: by the one rule an instance uses
/// ([`gdl_model::LeaveBy::begin_in`]), with the default clock skew, since a
/// hub has no setting for it, and the stop's `wait` in place of a drain.
fn leave_at(
    leave: gdl_model::LeaveBy,
    wait: std::time::Duration,
) -> Result<tokio::time::Instant, gdl_model::Error> {
    let before = tokio::time::Instant::now();
    let wall = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|wall| u64::try_from(wall.as_nanos()).ok())
        .unwrap_or(0);
    let now = tokio::time::Instant::now();
    let begin = leave.begin_in(
        gdl_model::LeaveBy::wall_since(wall, now - before),
        gdl_model::DEFAULT_CLOCK_SKEW,
        wait,
        "pool",
        "stop_wait_ms",
    )?;
    Ok(now + begin)
}
