use crate::{Chunk, Prompt};
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use futures::StreamExt;
use gdl::{Build, Cx, Frontend, Next};
use serde::Deserialize;
use std::{future::Future, net::SocketAddr, time::Duration};
use tokio::net::TcpListener;

/// Hosts a typed `/generate` endpoint with an NDJSON response stream.
#[derive(gdl::Node)]
#[gdl(config = FrontendConfig)]
pub struct HttpFrontend {
    listener: TcpListener,
}

impl HttpFrontend {
    /// The bound address, including an OS-assigned port when configured with 0.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl Build for HttpFrontend {
    type Config = FrontendConfig;
    type Metrics = ();
    async fn build(config: FrontendConfig, (): ()) -> gdl::Result<Self> {
        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(|error| gdl::Error::node(error.to_string()))?;
        Ok(Self { listener })
    }
}

impl Frontend for HttpFrontend {
    type Req = Prompt;
    type Item = Chunk;
    async fn serve<N: Next<Prompt, Chunk>>(
        self,
        next: N,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> gdl::Result<()> {
        let app = Router::new()
            .route("/generate", post(generate::<N>))
            .with_state(next);
        axum::serve(self.listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|error| gdl::Error::node(error.to_string()))
    }
}

/// Runtime-only HTTP listener settings.
#[derive(Debug, Deserialize, gdl::Config)]
#[serde(deny_unknown_fields)]
pub struct FrontendConfig {
    /// HTTP bind address; the demo defaults to loopback port 8082.
    #[serde(default = "listen")]
    pub listen: SocketAddr,
}
fn listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8082))
}

async fn generate<N: Next<Prompt, Chunk>>(
    State(next): State<N>,
    Json(request): Json<Prompt>,
) -> Response {
    let stream = match next
        .generate(Cx::new().with_timeout(Duration::from_secs(30)), request)
        .await
    {
        Ok(stream) => stream,
        Err(error) => return (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    };
    let body = Body::from_stream(stream.map(|item| {
        let mut bytes = match item {
            Ok(chunk) => serde_json::to_vec(&chunk),
            Err(error) => serde_json::to_vec(&serde_json::json!({"error": error.to_string()})),
        }
        .map_err(std::io::Error::other)?;
        bytes.push(b'\n');
        Ok::<_, std::io::Error>(bytes)
    }));
    ([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response()
}
