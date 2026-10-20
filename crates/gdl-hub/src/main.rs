//! Run one graph discovery hub.
use gdl_hub::GraphAdmission;
use gdl_model::{DeploymentSpec, HUB_CONTROL_PORT, HUB_PUBLIC_PORT};
use velo_hub::{Hub, HubConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: gdl-hub deployment.json")?;
    if path == "--healthcheck" {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect_timeout(
            &(std::net::Ipv4Addr::LOCALHOST, HUB_PUBLIC_PORT).into(),
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
    let mut config = HubConfig::default();
    config.server.public_port = HUB_PUBLIC_PORT;
    config.server.control_port = HUB_CONTROL_PORT;
    Hub::builder(config)
        .extension(GraphAdmission::new(spec))
        .serve()
        .await?
        .run_until_signal()
        .await?;
    Ok(())
}
