//! Three typed public entry paths sharing one internal graph.
use axum::{Json, Router, body::Body, extract::State, response::Response, routing::post};
use futures::StreamExt;
use gdl::{Build, Cx, Next};
use serde::{Deserialize, Serialize};
use std::{future::Future, net::SocketAddr};

/// Listener and endpoint selection.
#[derive(Deserialize, gdl::Config)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    /// Public chat request/response path.
    #[gdl(
        graph,
        default = true,
        label = "Chat Completions",
        subtext = "OpenAI chat endpoint"
    )]
    pub enabled_chat: bool,
    /// Public responses request/response path.
    #[gdl(
        graph,
        default = true,
        label = "Responses",
        subtext = "OpenAI responses endpoint"
    )]
    pub enabled_responses: bool,
    /// Public messages request/response path.
    #[gdl(
        graph,
        default = true,
        label = "Messages",
        subtext = "Anthropic messages endpoint"
    )]
    pub enabled_anthropic: bool,
    /// Runtime-only listener setting.
    #[serde(default = "listen")]
    pub listen: SocketAddr,
}
fn listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8082))
}

/// One service owning all enabled protocol entries.
#[derive(gdl::Node)]
#[gdl(config = ApiConfig)]
pub struct ApiFrontend {
    listener: tokio::net::TcpListener,
    config: ApiConfig,
}
impl ApiFrontend {
    /// The bound address, including an OS-assigned port.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}
impl Build for ApiFrontend {
    type Config = ApiConfig;
    async fn build(config: ApiConfig) -> gdl::Result<Self> {
        let listener = tokio::net::TcpListener::bind(config.listen)
            .await
            .map_err(|e| gdl::Error::node(e.to_string()))?;
        Ok(Self { listener, config })
    }
}

/// Each entry is either wired with its exact types or disabled.
#[derive(gdl::Endpoints)]
pub struct ApiClients {
    /// Chat protocol client.
    #[gdl(endpoint = "/v1/chat/completions", enabled = "enabled_chat")]
    pub chat: Option<gdl::Client<ChatRequest, ChatReply>>,
    /// Responses protocol client.
    #[gdl(endpoint = "/v1/responses", enabled = "enabled_responses")]
    pub responses: Option<gdl::Client<ResponseRequest, ResponseReply>>,
    /// Messages protocol client.
    #[gdl(endpoint = "/v1/messages", enabled = "enabled_anthropic")]
    pub anthropic: Option<gdl::Client<AnthropicRequest, AnthropicReply>>,
}
impl gdl::Service for ApiFrontend {
    type Clients = ApiClients;
    async fn serve(
        self,
        clients: ApiClients,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> gdl::Result<()> {
        if (
            self.config.enabled_chat,
            self.config.enabled_responses,
            self.config.enabled_anthropic,
        ) != (
            clients.chat.is_some(),
            clients.responses.is_some(),
            clients.anthropic.is_some(),
        ) {
            return Err(gdl::Error::node(
                "enabled endpoints and wired clients disagree",
            ));
        }
        let mut app = Router::new();
        if let Some(client) = clients.chat {
            app = app.merge(
                Router::new()
                    .route("/v1/chat/completions", post(call::<ChatRequest, ChatReply>))
                    .with_state(client),
            );
        }
        if let Some(client) = clients.responses {
            app = app.merge(
                Router::new()
                    .route(
                        "/v1/responses",
                        post(call::<ResponseRequest, ResponseReply>),
                    )
                    .with_state(client),
            );
        }
        if let Some(client) = clients.anthropic {
            app = app.merge(
                Router::new()
                    .route(
                        "/v1/messages",
                        post(call::<AnthropicRequest, AnthropicReply>),
                    )
                    .with_state(client),
            );
        }
        axum::serve(self.listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|e| gdl::Error::node(e.to_string()))
    }
}
async fn call<R: serde::de::DeserializeOwned + Send + 'static, I: Serialize + Send + 'static>(
    State(client): State<gdl::Client<R, I>>,
    Json(request): Json<R>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let stream = client
        .generate(
            Cx::new().with_timeout(std::time::Duration::from_secs(30)),
            request,
        )
        .await
        .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e.to_string()))?;
    let body = stream.map(|item| {
        let mut bytes = match item {
            Ok(item) => serde_json::to_vec(&item).expect("response"),
            Err(e) => {
                serde_json::to_vec(&serde_json::json!({"error":e.to_string()})).expect("error")
            }
        };
        bytes.push(b'\n');
        Ok::<_, std::convert::Infallible>(bytes)
    });
    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(body))
        .expect("response headers"))
}

/// Shared internal request.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Prompt {
    pub text: String,
}
/// Shared internal response item.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Chunk {
    pub text: String,
}
/// Sample chat input, deliberately smaller than a production API schema.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct ChatRequest {
    pub messages: Vec<String>,
}
/// Sample chat output.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct ChatReply {
    pub delta: String,
}
/// Sample responses input.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct ResponseRequest {
    pub input: String,
}
/// Sample responses output.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct ResponseReply {
    pub output_text: String,
}
/// Sample messages input.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct AnthropicRequest {
    pub messages: Vec<String>,
    pub max_tokens: usize,
}
/// Sample messages output.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct AnthropicReply {
    pub text: String,
}

/// Chat-to-IR adapter.
#[derive(gdl::Node)]
pub struct ChatAdapter;
impl Build for ChatAdapter {
    type Config = std::collections::BTreeMap<String, String>;
    async fn build(_: Self::Config) -> gdl::Result<Self> {
        Ok(Self)
    }
}
impl gdl::Operator for ChatAdapter {
    type UpReq = ChatRequest;
    type UpItem = ChatReply;
    type DownReq = Prompt;
    type DownItem = Chunk;
    async fn generate<N: Next<Prompt, Chunk>>(
        &self,
        cx: Cx,
        req: ChatRequest,
        next: N,
    ) -> gdl::Result<gdl::Items<ChatReply>> {
        Ok(gdl::items(
            next.generate(
                cx,
                Prompt {
                    text: req.messages.join(" "),
                },
            )
            .await?
            .map(|v| v.map(|v| ChatReply { delta: v.text })),
        ))
    }
}
/// Responses-to-IR adapter.
#[derive(gdl::Node)]
pub struct ResponsesAdapter;
impl Build for ResponsesAdapter {
    type Config = std::collections::BTreeMap<String, String>;
    async fn build(_: Self::Config) -> gdl::Result<Self> {
        Ok(Self)
    }
}
impl gdl::Operator for ResponsesAdapter {
    type UpReq = ResponseRequest;
    type UpItem = ResponseReply;
    type DownReq = Prompt;
    type DownItem = Chunk;
    async fn generate<N: Next<Prompt, Chunk>>(
        &self,
        cx: Cx,
        req: ResponseRequest,
        next: N,
    ) -> gdl::Result<gdl::Items<ResponseReply>> {
        Ok(gdl::items(
            next.generate(cx, Prompt { text: req.input })
                .await?
                .map(|v| {
                    v.map(|v| ResponseReply {
                        output_text: v.text,
                    })
                }),
        ))
    }
}
/// Messages-to-IR adapter with a request-specific response limit.
#[derive(gdl::Node)]
pub struct AnthropicAdapter;
impl Build for AnthropicAdapter {
    type Config = std::collections::BTreeMap<String, String>;
    async fn build(_: Self::Config) -> gdl::Result<Self> {
        Ok(Self)
    }
}
impl gdl::Operator for AnthropicAdapter {
    type UpReq = AnthropicRequest;
    type UpItem = AnthropicReply;
    type DownReq = Prompt;
    type DownItem = Chunk;
    async fn generate<N: Next<Prompt, Chunk>>(
        &self,
        cx: Cx,
        req: AnthropicRequest,
        next: N,
    ) -> gdl::Result<gdl::Items<AnthropicReply>> {
        Ok(gdl::items(
            next.generate(
                cx,
                Prompt {
                    text: req.messages.join(" "),
                },
            )
            .await?
            .take(req.max_tokens)
            .map(|v| v.map(|v| AnthropicReply { text: v.text })),
        ))
    }
}
/// Shared pipeline sink.
#[derive(gdl::Node)]
pub struct Echo;
impl Build for Echo {
    type Config = std::collections::BTreeMap<String, String>;
    async fn build(_: Self::Config) -> gdl::Result<Self> {
        Ok(Self)
    }
}
impl gdl::Sink for Echo {
    type Req = Prompt;
    type Item = Chunk;
    async fn generate(&self, _: Cx, req: Prompt) -> gdl::Result<gdl::Items<Chunk>> {
        let chunks: Vec<_> = req
            .text
            .split_whitespace()
            .map(|s| Ok(Chunk { text: s.into() }))
            .collect();
        Ok(gdl::items(futures::stream::iter(chunks)))
    }
}

/// Convert chat requests without coupling request and response transforms.
#[derive(gdl::Node)]
pub struct ExtractChat;
impl Build for ExtractChat {
    type Config = std::collections::BTreeMap<String, String>;
    async fn build(_: Self::Config) -> gdl::Result<Self> {
        Ok(Self)
    }
}
impl gdl::Forward for ExtractChat {
    type In = ChatRequest;
    type Out = Prompt;
    async fn forward(&self, _: &Cx, request: ChatRequest) -> gdl::Result<Prompt> {
        Ok(Prompt {
            text: request.messages.join(" "),
        })
    }
}

/// Convert internal response items back to the public chat contract.
#[derive(gdl::Node)]
pub struct FormatChat;
impl Build for FormatChat {
    type Config = std::collections::BTreeMap<String, String>;
    async fn build(_: Self::Config) -> gdl::Result<Self> {
        Ok(Self)
    }
}
impl gdl::Backward for FormatChat {
    type In = Chunk;
    type Out = ChatReply;
    fn backward(&self, _: &Cx, input: gdl::Items<Chunk>) -> gdl::Items<ChatReply> {
        gdl::items(input.map(|item| item.map(|chunk| ChatReply { delta: chunk.text })))
    }
}
