# Run the examples

The examples are in `examples/`. Each is its own Cargo workspace, outside the root workspace, as a deployment's would be. Each has a graph file and a spec, and `cargo gdl build` builds its segments into its `target` directory. No example commits generated crates. Run the commands from the repository root. [Getting started](getting-started.md) walks through `chat`.

| Example | Shows | Test |
|---|---|---|
| `examples/chat` | A graph file, a protocols crate, node crates with configuration and metrics, three segments | `crates/gdl-gen/tests/chat.rs` |
| `examples/asr` | A streamed request: audio goes in as a stream, transcripts come out | `crates/gdl-gen/tests/asr.rs` |
| `examples/http` | A hosted `Frontend` on axum that serves a graph over HTTP | `examples/http/tests` |
| `examples/api` | One `Service` with three typed entry points that share one engine | `examples/api/tests` |
| `examples/film` | Two reference and vendor node crates, with configuration, metrics, and a usage tap | `crates/gdl-gen/tests/film.rs` |

## chat

Three segments, `front`, `engine`, and `post`, from `chat.gdl` and `gdl.toml`. Build them with:

```bash
cargo run -p gdl-gen -- build --spec examples/chat/gdl.toml
```

[Getting started](getting-started.md) runs them.

## asr

`examples/asr` is a speech recognition pipeline with a mock recognizer. Its request is a session's settings and a stream of audio chunks:

```
source   Client: Session + stream Audio -> stream Transcript;
operator Gateway: (Session + stream Audio -> stream Transcript) over (Session + stream Audio -> stream Partial);
forward  Resample: Session + stream Audio -> Session + stream Pcm;
sink     Recognizer: Session + stream Pcm -> stream Partial;
backward Punctuate: Partial -> Partial;
```

The graph is `asr.gdl`, the messages are in `protocols`, and the node crates mark their nodes with `derive(gdl::Node)`. The source, `Client`, binds to no node: the program that calls the front segment is the client. The placement in `gdl.toml` puts `Resample` in its own segment, so the audio crosses two times. Build the segments with:

```bash
cargo run -p gdl-gen -- build --spec examples/asr/gdl.toml
```

The generated binary of the source's segment has no JSON-lines server, because one line cannot carry a streamed request. Call the source through the segment library instead. The demo in `examples/asr/demo` starts the three segments in one process, sends audio, and receives transcripts while the audio streams in. It links the generated segment libraries, so it is not a member of the example's workspace: a test generates the segment crates in the target directory and builds the demo beside them. Run it with:

```bash
cargo nextest run -p gdl-gen --test asr --no-run
timeout 300 cargo nextest run -p gdl-gen --test asr
```

A program of your own that links a segment library takes the crates from `cargo gdl generate --out` into its own workspace, as the test does.

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

`examples/film` has two node crates that depend on `gdl` and never link velo. `gdl-demo-nodes` has an HTTP frontend and a mock engine. `acme-gdl-nodes` has a response-only usage tap and marked re-exports of the `Chunk` and `Usage` messages. Three specs use the same graph: `gdl.toml` (separate segments), `compact.toml` (one process, with mimalloc as its allocator), and `deploy.toml` (pinned Git packages for Compose). `examples/film/README.md` has the node settings and metrics. `crates/gdl-gen/tests/film.rs` runs it natively.
