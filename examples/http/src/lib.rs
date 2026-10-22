//! An Axum source frontend with streamed and collected HTTP responses.

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
use serde::{Deserialize, Serialize};
use std::{future::Future, net::SocketAddr, time::Duration};
use tokio::net::TcpListener;

/// Hosts `/generate` and forwards typed requests into a graph.
#[derive(gdl::Node)]
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
    type Config = Config;
    type Metrics = ();

    async fn build(config: Config, (): ()) -> gdl::Result<Self> {
        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(|e| gdl::Error::node(e.to_string()))?;
        Ok(Self { listener })
    }
}

impl Frontend for HttpFrontend {
    type Req = Prompt;
    type Item = Chunk;

    async fn serve<N>(
        self,
        next: N,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> gdl::Result<()>
    where
        N: Next<Prompt, Chunk>,
    {
        let app = Router::new()
            .route("/generate", post(generate::<N>))
            .with_state(next);
        axum::serve(self.listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|e| gdl::Error::node(e.to_string()))
    }
}

/// Listener settings for the source node.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// HTTP bind address; defaults to loopback port 8081.
    #[serde(default = "listen")]
    pub listen: SocketAddr,
}

fn listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8081))
}

/// The graph request, independent of the external response format.
#[derive(Clone, Serialize, Deserialize, gdl::Message)]
pub struct Prompt {
    /// Input text.
    pub text: String,
}

/// One response item from the graph.
#[derive(Clone, Serialize, Deserialize, gdl::Message)]
pub struct Chunk {
    /// One word of the input.
    pub text: String,
}

/// A sample implementation behind the HTTP frontend.
#[derive(gdl::Node)]
pub struct Echo;

impl Build for Echo {
    type Config = std::collections::BTreeMap<String, String>;
    type Metrics = ();
    async fn build(_: Self::Config, (): ()) -> gdl::Result<Self> {
        Ok(Self)
    }
}

impl gdl::Sink for Echo {
    type Req = Prompt;
    type Item = Chunk;
    async fn generate(&self, _: Cx, request: Prompt) -> gdl::Result<gdl::Items<Chunk>> {
        let chunks: Vec<_> = request
            .text
            .split_whitespace()
            .map(|word| Ok(Chunk { text: word.into() }))
            .collect();
        Ok(gdl::items(futures::stream::iter(chunks)))
    }
}

/// The local version of the same graph described by `http.gdl`.
pub mod graph {
    use super::{Chunk, Prompt};
    gdl_runtime::graph! {
        name = "http", schema = 1;
        source Http: Prompt -> stream Chunk;
        sink Engine: Prompt -> stream Chunk;
        topology: Http => Engine;
    }
}

#[derive(Deserialize)]
struct Request {
    text: String,
    #[serde(default = "streaming")]
    stream: bool,
}
fn streaming() -> bool {
    true
}

async fn generate<N: Next<Prompt, Chunk>>(
    State(next): State<N>,
    Json(request): Json<Request>,
) -> Response {
    // A request owns its deadline and cancellation scope. Dropping the HTTP
    // body drops the graph stream, so a disconnected client stops its work.
    let cx = Cx::new().with_timeout(Duration::from_secs(30));
    let mut items = match next.generate(cx, Prompt { text: request.text }).await {
        Ok(items) => items,
        Err(error) => return (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    };
    if request.stream {
        let body = Body::from_stream(items.map(|item| {
            let value = match item {
                Ok(item) => serde_json::json!({"item":item}),
                Err(error) => serde_json::json!({"error":error.to_string()}),
            };
            Ok::<_, std::convert::Infallible>(format!("{value}\n"))
        }));
        return ([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response();
    }
    let mut text = String::new();
    while let Some(item) = items.next().await {
        let item = match item {
            Ok(item) => item,
            Err(error) => return (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
        };
        // Collected output has an explicit memory limit; streaming has no
        // aggregate buffer and uses HTTP backpressure.
        if text.len() + item.text.len() + 1 > 1024 * 1024 {
            return (
                StatusCode::BAD_GATEWAY,
                "Collected response exceeds 1 MiB; use streaming.",
            )
                .into_response();
        }
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(&item.text);
    }
    Json(serde_json::json!({"text":text})).into_response()
}
