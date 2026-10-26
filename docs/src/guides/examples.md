# Run the examples

The examples are in `examples/`. Each is its own Cargo workspace, outside the root workspace, as a deployment's would be. Run the commands from the repository root. [Getting started](getting-started.md) walks through `chat`.

| Example | Shows | Test |
|---|---|---|
| `examples/chat` | A graph file, a protocols crate, node crates with configuration and metrics, three segments | `crates/gdl-gen/tests/chat.rs` |
| `examples/asr` | A streamed request: audio goes in as a stream, transcripts come out | `crates/gdl-gen/tests/asr.rs` |
| `examples/http` | A hosted `Frontend` on axum that serves a graph over HTTP | `examples/http/tests` |
| `examples/api` | One `Service` with three typed entry points that share one engine | `examples/api/tests` |
| `examples/film` | Two reference and vendor node crates, with configuration, metrics, and a usage tap | `crates/gdl-gen/tests/film.rs` |

## chat

Three segments, `front`, `engine`, and `post`, from `chat.gdl` and `gdl.toml`. Regenerate its segments with the command in [Getting started](getting-started.md). The segment crates in `segments/` are generated, so do not edit them.

## asr

`examples/asr` is a speech recognition pipeline with a mock recognizer. Its request is a session's settings and a stream of audio chunks:

```
source   Client: Session + stream Audio -> stream Transcript;
operator Gateway: (Session + stream Audio -> stream Transcript) over (Session + stream Audio -> stream Partial);
forward  Resample: Session + stream Audio -> Session + stream Pcm;
sink     Recognizer: Session + stream Pcm -> stream Partial;
backward Punctuate: Partial -> Partial;
```

The placement in `gdl.toml` puts `Resample` in its own segment, so the audio crosses two times. Regenerate the segments with:

```bash
cargo run -p gdl-gen -- generate --spec examples/asr/gdl.toml --out examples/asr/segments
```

The generated binary of the source's segment has no JSON-lines server, because one line cannot carry a streamed request. Call the source through the segment library instead. The `asr-demo` crate starts the three segments in one process, sends audio, and receives transcripts while the audio streams in:

```bash
(cd examples/asr && timeout 300 cargo nextest run -p asr-demo)
```

See [Streamed requests](../concepts/streamed-requests.md).

## http

`HttpFrontend` implements `gdl::Frontend` and hosts an axum service. Its graph is `Prompt -> stream Chunk`, and the `Echo` sink emits one chunk per input word:

```bash
cargo run --manifest-path examples/http/Cargo.toml -- 127.0.0.1:8081
curl http://127.0.0.1:8081/generate -H 'content-type: application/json' -d '{"text":"hello source"}'
curl http://127.0.0.1:8081/generate -H 'content-type: application/json' -d '{"text":"hello source","stream":false}'
```

The first request returns NDJSON, one `item` for each word. The second returns `{"text":"hello source"}`. See [Hosted frontends and service entries](frontends.md) and `examples/http/README.md`.

## api

`ApiFrontend` hosts `/v1/chat/completions`, `/v1/responses`, and `/v1/messages` on one listener. Each entry has its own request and response types, and all three call one shared `Prompt`/`Chunk` pipeline. The schemas are small demonstrations, not the full OpenAI or Anthropic APIs. `examples/api/README.md` has the steps to generate the segments, write the two config files, and call each path. `examples/api/deploy.toml` is the deployment spec for `cargo gdl compose` and the composer's Deploy tab.

## film

`examples/film` has two node crates that depend on `gdl` and never link velo. `gdl-demo-nodes` has an HTTP frontend and a mock engine. `acme-gdl-nodes` has a response-only usage tap and marked re-exports of the `Chunk` and `Usage` messages. Three specs use the same graph: `gdl.toml` (separate segments), `compact.toml` (one process), and `deploy.toml` (pinned Git packages for Compose). `examples/film/README.md` has the node settings and metrics. `crates/gdl-gen/tests/film.rs` runs it natively.
