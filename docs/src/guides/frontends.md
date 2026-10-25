# Hosted frontends and service entries

A graph's source is its entry point. By default it is programmatic: code calls the graph through a typed client, and a generated source binary reads JSON lines on stdin when the source's messages are wire types. This guide shows how to make the source a hosted service instead: an HTTP, gRPC, or other server that owns its listener and calls the graph for each external request. It covers one contract (`Frontend`), several typed contracts (`Service`), graphs with several roots, shared sinks, and identity nodes.

## Host one contract with `Frontend`

`gdl::Frontend` extends `Build` and declares the graph's `Req` and `Item`. Its `serve` takes a typed `Next<Req, Item>` client and a shutdown future.

```rust
#[derive(gdl::Node)]
pub struct HttpFrontend { listener: tokio::net::TcpListener }   // bound in Build::build

impl gdl::Frontend for HttpFrontend {
    type Req = Prompt;
    type Item = Chunk;
    async fn serve<N: Next<Prompt, Chunk>>(
        self, next: N, shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> gdl::Result<()> { /* call next.generate(cx, req) for each external request */ }
}
```

1. Bind the listener in `Build::build`, so that a bind error fails the start. The settings come from the segment's `[frontend]` table.
2. In `serve`, give each external request a fresh `Cx` with its own deadline.
3. Drop the graph stream when the client disconnects. Dropping it cancels that request.
4. Stop taking requests when `shutdown` resolves, then drain on your protocol's terms. The implementation owns its drain policy.

Graph responses are always streams. A frontend can stream them over its protocol, or collect them into one external response. There is no single-output graph contract.

To deploy it, pin the graph's source to the frontend in the spec:

```toml
[nodes]
pin = { Http = "http_frontend::HttpFrontend", Engine = "http_frontend::Echo" }
```

The generated source binary calls `serve` only after every segment is ready, and resolves `shutdown` when the segment stops ([Segment lifecycle](../concepts/lifecycle.md)). Other segments do not build or run the frontend. `examples/http` is a complete frontend on axum, with streamed NDJSON and collected responses. Its test drives real HTTP requests on an OS-assigned port.

## Host several contracts with `Service`

A service with several public endpoints implements `gdl::Service`. Its `Clients` type derives `gdl::Endpoints`: one optional typed client per endpoint, with its path and the graph setting that enables it.

```rust
#[derive(gdl::Endpoints)]
pub struct ApiClients {
    #[gdl(endpoint = "/v1/chat/completions", enabled = "enabled_chat")]
    pub chat: Option<gdl::Client<ChatRequest, ChatReply>>,
    #[gdl(endpoint = "/v1/responses", enabled = "enabled_responses")]
    pub responses: Option<gdl::Client<ResponseRequest, ResponseReply>>,
}

impl gdl::Service for ApiFrontend {
    type Clients = ApiClients;   // serve(self, clients, shutdown): one listener for every Some(client)
}
```

- Each `enabled` names a `bool` graph setting of the service's `derive(gdl::Config)` type ([Write nodes](write-nodes.md#expose-settings-to-the-composer)). A disabled endpoint gets `None`.
- Endpoint flags are part of the graph. Runtime settings cannot enable an unwired endpoint or disable a wired one. Change `[config.<Source>]` and generate again.
- `gdl::Client` erases the pipeline's type at the service boundary. It costs one boxed future per external call. The pipeline's stages keep their concrete types.

Each enabled endpoint is an entry arm in the spec, `[entry.<field>]`. An arm lists `forward` and `backward` nodes in execution order. A leaf arm names the graph root it calls with `target`. An arm with an `operator` owns a `nested` arm instead.

```toml
[entry.chat]
target = "Http"
forward = ["api_frontend::ExtractChat"]
backward = ["api_frontend::FormatChat"]
[entry.responses]
operator = "api_frontend::ResponsesAdapter"
nested = { target = "Http" }
[config.Http]
enabled_chat = true
```

Generation checks every edge of each arm, and the response type returned to each endpoint. It refuses an enabled entry with no arm, an arm for a disabled or unknown entry, a missing `target`, and a service with no wired entry. Matching types permit a connection. They never create one. Arm nodes in a service spec run in the frontend's segment. Their settings tables are `[adapter.<name>]`, where the name is built from the source and the entry, such as `Http_chat_f0` for the first forward node. `examples/api` serves three endpoints with distinct types through one shared engine, placed across two segments.

## Several roots and shared sinks

A graph can declare several sources, each with its own `topology`. Each root keeps its own request and response path. A sink named in two topologies is one node: one declaration, one settings table, one binding, and one placement. Each process that hosts it builds it once. Declaring two sink names makes two instances. Only sinks can be shared.

```text
name = "processing", schema = 1;
source ChatProcessing: Prompt -> stream Chunk;
source ResponsesProcessing: Prompt -> stream Chunk;
sink Engine: Prompt -> stream Chunk;
topology: ChatProcessing => Engine;
topology: ResponsesProcessing => Engine;
```

- A hosted graph with several roots needs a `Service`. A `Frontend` serves one root.
- Service roots start in the frontend's segment. Their other nodes can be placed in any segment.
- In code, `local` and `assemble` return the roots as a nested tuple in topology order: `(chat, (responses, ()))`. A running `Arc<Segment<_>>` gives a typed client per root, such as `segment.client(|roots| roots.1.0.clone())`. Each client keeps the segment's handlers alive.
- Each response follows its own call's stream anchor back. No request id map is used.
- The crossing report counts one request through each root.
- `[pipeline_names]` maps root names to display names, for example `ChatProcessing = "processing-pipeline-1"`.

The composer lowers each enabled endpoint to its own public root, followed by its adapters as ordinary graph nodes. Those adapters can then be placed in other segments, unlike arm nodes in a service spec. [Compose in the browser](composer.md) shows how to draw this.

## Identity nodes

An identity node passes values through unchanged. It needs no crate and no binding: the runtime supplies `gdl::IdentityNode<T>` and `gdl::IdentityOperator<Req, Item>`, and generated code builds them. Use one to give a processing stage its own segment before its real implementation exists.

```text
identity ProcessingOne: Prompt -> stream Chunk;   // an operator: sees both paths
identity ProcessingTwo: request Prompt;           // forward only
identity ResponsePass: response Chunk;            // backward only, after the inner node
topology: Chat => ProcessingOne { Echo };
topology: Responses => ProcessingTwo => Echo;
```

A request identity can also carry `stream Chunk` or `Header + stream Chunk`. A request identity sees no responses, so the response of `Responses` above returns from `Echo` straight to the frontend. The response of `Chat` returns through `ProcessingOne`.
