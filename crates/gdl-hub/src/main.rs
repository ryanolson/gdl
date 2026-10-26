//! Run one graph discovery hub: `gdl-hub deployment.json`. A cell's hub
//! takes its cell's settings from `GDL_CELL__*` variables. The hub listens
//! on the deployment's ports unless `GDL_HUB__PUBLIC_PORT` and
//! `GDL_HUB__CONTROL_PORT` name others (0 lets the OS choose), the
//! variables a segment reads for where its hub is. Once it serves, it
//! prints the addresses it bound on stderr.
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
    let spec: DeploymentSpec = serde_json::from_slice(&std::fs::read(path)?)?;
    let spec = DeploymentSpec::new(spec.graph, spec.placement)?;
    let cell = gdl_hub::cell_from_env(&spec)?;
    let mut config = HubConfig::default();
    config.server.public_port = port("GDL_HUB__PUBLIC_PORT", HUB_PUBLIC_PORT)?;
    config.server.control_port = port("GDL_HUB__CONTROL_PORT", HUB_CONTROL_PORT)?;
    // SIGTERM as well as SIGINT, both taken before the hub serves. Docker
    // stops a container with SIGTERM, and the hub is the container's first
    // process, for which a signal it does not handle does nothing: a hub
    // that stopped only on SIGINT ran until Docker killed it, 10 s later.
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let hub = gdl_hub::hub_builder(config, spec, cell)?.serve().await?;
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
