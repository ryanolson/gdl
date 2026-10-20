# Service with three typed entry paths

This example hosts `/v1/chat/completions`, `/v1/responses`, and `/v1/messages` on one Axum listener. Each entry has distinct request and response types. The chat arm uses separate request and response transforms. The other arms use operators. All three call one shared `Prompt`/`Chunk` pipeline. The messages adapter keeps its request's response limit.

These are small demonstration schemas with NDJSON output. They do not implement the full OpenAI or Anthropic APIs. Dynamo packages can supply their actual protocol types and services through the same contracts.

## Compose

Import `examples/api` into the web catalog and select `ApiFrontend`. Open Source settings. The builder labels the three flags Chat Completions, Responses, and Messages. It does not expose the runtime-only `listen` setting.

The builder infers the shared `Prompt`/`Chunk` contract when the catalog offers one common adapter contract. Use the request “+” on Chat Completions to add ExtractChat. The response “+” stays red until FormatChat converts Chunk to ChatReply. Use ResponsesAdapter and AnthropicAdapter for the other arms. Select Echo as the shared sink. Disabled arms keep their nodes for later restoration.

Use “+” or drag a matching catalog node to extend an arm. Operators open a downstream arm. A matching terminal contract joins the shared pipeline. Settings and port details open from each node icon. Zoom controls and background dragging navigate the workspace.

Entry nodes run beside the frontend. The shared pipeline can span segments. Independent placement of entry nodes and saved pipeline insertion are not implemented.

## Generate and run

From the repository root:

```bash
cargo run -p gdl-gen -- generate --spec examples/api/gdl.toml --out /tmp/gdl-api-segments
```

Create `/tmp/api-front.toml`:

```toml
listen = "127.0.0.1:0"
roster = "/tmp/api-roster"
[frontend]
listen = "127.0.0.1:8082"
```

Create `/tmp/api-engine.toml`:

```toml
listen = "127.0.0.1:0"
roster = "/tmp/api-roster"
```

Use a fresh roster directory for each deployment. Run these commands in separate terminals:

```bash
cargo run --manifest-path /tmp/gdl-api-segments/engine/Cargo.toml -- /tmp/api-engine.toml
cargo run --manifest-path /tmp/gdl-api-segments/front/Cargo.toml -- /tmp/api-front.toml
```

Call each path:

```bash
curl -N http://127.0.0.1:8082/v1/chat/completions -H 'Content-Type: application/json' -d '{"messages":["hello chat"]}'
curl -N http://127.0.0.1:8082/v1/responses -H 'Content-Type: application/json' -d '{"input":"hello responses"}'
curl -N http://127.0.0.1:8082/v1/messages -H 'Content-Type: application/json' -d '{"messages":["hello messages"],"max_tokens":1}'
```

## Configuration

`ApiConfig` derives `gdl::Config`. Only fields marked `#[gdl(graph, default = true)]` are exposed to composition. The optional `label` and `subtext` values supply display text; doc comments supply help. The source's resolved graph settings are in `[config.Http]` in the graph spec. Each `[entry.name]` table has `forward` and `backward` lists. An `operator` requires a `nested` table for its downstream arm. An arm without an operator requires an explicit `target`. This example selects `Http` for every leaf, so all three arms share one processing pipeline. Separate source roots and topology declarations allow independent pipelines. Several roots can reference the same sink node.

Generated binaries accept multiple TOML files. Later files override earlier files. `GDL_` environment variables override files; `__` separates nested keys. For example, `GDL_FRONTEND__LISTEN=127.0.0.1:8083` changes the listener. An endpoint flag that conflicts with the generated graph fails before service startup.

Runtime-only entry settings use instance names, such as `[adapter.Http_chat_f0]` and `[adapter.Http_responses_op]`. The names append `_fN`, `_bN`, or `_op` to the source and entry name. Nested arms append `_next` before their node suffix. Shared pipeline settings go under `[node.Engine]`. The demonstration adapters and Echo need no settings.

## Validation

```bash
cargo test --manifest-path examples/api/Cargo.toml --no-run
timeout 60 cargo test --manifest-path examples/api/Cargo.toml
```

The pinned velo 0.19.0 dependency currently fails to compile on macOS because it calls a Linux-only socket API. Validation on that host requires fixing the upstream dependency or using an isolated validation patch; this example does not alter the pin.
