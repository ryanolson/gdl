# Film demonstration nodes

This workspace supplies two crates for the GDL film. Both crates depend on `gdl`. Neither crate links `gdl-runtime` or velo.

| Crate | Nodes and messages |
| --- | --- |
| `gdl-demo-nodes` | `HttpFrontend`, `MockEngine`, and the `Prompt`, `Chunk`, and `Usage` wire messages |
| `acme-gdl-nodes` | Response-only `UsageTap`, marked re-exports of `Chunk` and `Usage`, and a mock accounting receiver |

`HttpFrontend` accepts `POST /generate` with a JSON `Prompt`: `{"text":"hello graph"}`. It returns an NDJSON stream of `Chunk` values. `MockEngine` emits one text chunk per whitespace-delimited word, followed by one usage chunk. These demonstration counts do not use a model tokenizer.

```json
{"kind":"text","index":0,"text":"hello"}
{"kind":"text","index":1,"text":"graph"}
{"kind":"usage","model":"mock-echo","input_tokens":2,"output_tokens":2}
```

The engine's graph settings expose `model` and `uppercase`. Its runtime settings also accept `delay_ms`. `HttpFrontend` accepts a runtime `listen` address, which defaults to `127.0.0.1:8082`.

`UsageTap` exposes graph settings for `tenant` and `enabled`. Its runtime `accounting_url` defaults to `http://127.0.0.1:8091/usage`. It forwards every input item unchanged. The first usage chunk triggers one accounting POST per response stream. Duplicate usage chunks do not trigger another POST. Missing usage and disabled accounting produce no POST.

The tap reuses one HTTP client per node. It waits for the accounting attempt before it forwards the first usage chunk. Each attempt has a one-second timeout. A failed request increments `accounting_errors_total` and leaves the response unchanged. The tap does not retry, persist events, or guarantee durable billing.

| Metric | Meaning |
| --- | --- |
| `mock_requests_total` | Requests accepted by `MockEngine` |
| `mock_chunks_total` | Emitted chunks, including usage chunks |
| `accounting_events_total` | Accounting POSTs accepted with a successful HTTP status |
| `accounting_errors_total` | Failed accounting attempts |

## Run the mock receiver

Run this command from the repository root:

```bash
cargo run --manifest-path examples/film/Cargo.toml -p acme-gdl-nodes --bin accounting -- 127.0.0.1:8091
```

The receiver prints each accepted `UsageEvent` as JSON. It returns HTTP 202. A known two-word response produces this event with the default tenant:

```json
{"tenant":"acme-film","usage":{"model":"mock-echo","input_tokens":2,"output_tokens":2}}
```

The graph specification, generated segments, and deployment placement select how these nodes run. The crates use relative path dependencies so the workspace also builds from a Git checkout.

| Spec | Arrangement |
| --- | --- |
| [gdl.toml](gdl.toml) | Local packages in separate frontend, engine, and usage segments |
| [compact.toml](compact.toml) | The same graph and graph settings in one serving process |
| [deploy.toml](deploy.toml) | Pinned public Git packages and Compose runtime settings |

The [public snapshot](https://github.com/ryanolson/gdl-demo/tree/d0d911b4424aa6b6d33b73b164017ba20f304293) includes the accounting receiver's Dockerfile and Compose overlay. Its README records the complete Docker rehearsal. Container execution still needs a Docker host.

## Extract the catalog

Extract each package separately. The virtual workspace root does not identify a package:

```bash
cargo run -p gdl-gen -- extract examples/film/gdl-demo-nodes --catalog tools/video/work/film-catalog
cargo run -p gdl-gen -- extract examples/film/acme-gdl-nodes --catalog tools/video/work/film-catalog
```

The composer's Message types view lists message declarations but does not display marked re-exports. The resolver still reads ACME's marked exports and preserves their reference identity. The reference messages live at the crate root so the composer can select their public canonical types.

## Check the workspace

Build the tests before the bounded run:

```bash
cargo test --manifest-path examples/film/Cargo.toml --no-run
timeout 60 cargo test --manifest-path examples/film/Cargo.toml
cargo fmt --manifest-path examples/film/Cargo.toml --all --check
cargo clippy --manifest-path examples/film/Cargo.toml --workspace --all-targets -- -D warnings
```

The tests cover deterministic output, known usage, graph settings, wire identity, HTTP service, and accounting success and failure. Test listeners use OS-assigned ports. Readiness comes from bound listeners and completed HTTP requests.
