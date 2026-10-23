//! A mock receiver that prints the usage events it accepts.
use acme_gdl_nodes::UsageEvent;
use axum::{Json, Router, http::StatusCode, routing::post};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8091".into())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!(
        "Mock accounting receiver: http://{}/usage",
        listener.local_addr()?
    );
    axum::serve(listener, Router::new().route("/usage", post(receive)))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn receive(Json(event): Json<UsageEvent>) -> StatusCode {
    println!(
        "{}",
        serde_json::to_string(&event).expect("usage event contains only strings and integers")
    );
    StatusCode::ACCEPTED
}
