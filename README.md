# dynamo-gdl

gdl is a graph definition language and runtime for request/response streaming pipelines. A request goes into a graph of nodes, and a stream of items comes back. The same graph runs in one process, or split into segments that run as separate programs and talk over [velo](https://github.com/ai-dynamo/velo).

You define the graph once, with its message types. A placement puts each node in a segment. The generator, `cargo gdl generate`, writes one crate per segment, and each segment crate depends only on the implementations of the nodes that it hosts.

NOTE: gdl is experimental. Its design and tests are still in progress, and its APIs are not stable.

**Documentation: the gdl book.** The source is in [`docs/src/`](docs/src/SUMMARY.md). Each push to `main` publishes it to a private GitHub Pages site, <https://crispy-adventure-qq4pemj.pages.github.io/>, which anyone who can read this repository can open. To build it locally, run `scripts/build-book.sh`, then open `docs/book/html/index.html`.

## Highlights

- **One graph, any placement.** Move a node to another segment without changing the graph or the node code. See [Specs, segments, and layouts](docs/src/concepts/placement.md).
- **Nodes read, not built.** The composer and generator read what a crate offers from its source, without building it. See [The catalogue and the source reader](docs/src/concepts/catalogue.md).
- **Streams in both directions.** A request can carry a stream of items, and every response is a stream. See [Streamed requests](docs/src/concepts/streamed-requests.md).
- **Live rosters.** Instances join, retire, and drain while the others serve. See [Segment lifecycle](docs/src/concepts/lifecycle.md).
- **Selectors and offers.** Route a crossing by round robin, random, or a selector you write, and fail over when an instance declines. See [Routing, selectors, and offers](docs/src/concepts/routing.md).
- **Dynamo-shaped health and metrics.** See [Health and metrics](docs/src/operations/observability.md).
- **Cells.** One graph across several deployments, each with its own hub. A cell policy, gdl's or one you register, chooses the cell for each request. See [Classes and cells](docs/src/concepts/cells.md) and [Write a cell policy](docs/src/guides/write-a-cell-policy.md).
- **A composer.** Draw a graph in a browser, place it, and deploy it with Compose. See [Compose in the browser](docs/src/guides/composer.md).

## Crates

gdl is two layers. A crate that implements nodes depends on `gdl`: the node roles, the request context, selectors, cell policies, the derives, and local composition, so that it can run its nodes in one process. It does not link velo. The runtime, `gdl-runtime`, runs those nodes: segments, crossings over velo, discovery, and the segment binaries. Contract crates, generated segment crates, and programs that run a graph depend on `gdl-runtime`, which re-exports the `gdl` it was built with as `gdl_runtime::gdl`. The derives expand to `::gdl::...`, so a crate that uses one depends on `gdl` itself, also when it depends on `gdl-runtime`.

| Crate | For | Contents |
|---|---|---|
| `gdl` | Authors of nodes and routers | The request context, role traits, local composition, selectors, cell policies, metrics, and the derives. No velo |
| `gdl-runtime` | Programs that run a graph | Segments, crossings over velo, discovery, the segment binary, and `graph!`. Re-exports `gdl` and the model |
| `gdl-model` | Tools | The manifest, placement, layout, diagrams, and the fleet file, without the runtime. Builds for wasm |
| `gdl-syntax` | The macro and the generator | The graph parser and checked model |
| `gdl-message` | Authors of messages | `derive(Message)` and the message catalogue, without the runtime |
| `gdl-macros` | Internal | `graph!` and the derives |
| `gdl-extract` | Tools | The source reader and the working catalogue |
| `gdl-gen` | Users | The `cargo-gdl` binary: `extract`, `catalog`, `resolve`, `generate`, `check`, `diagram`, `compose`, `export` |
| `gdl-web` | Users | The composer: a page and its server |
| `gdl-cells` | `gdl`, hub, and runtime | The cell plane's shared types: the cell record, a cell's state and load summary, and the `CellDirectory` backends. No velo |
| `gdl-watch` | Hub and runtime | The watches a cell's hub serves, as typed events, and their client |
| `gdl-hub` | Deployments | The discovery hub, and a cell's quorum, health, record, and follows |

The examples in `examples/` are separate workspaces. See [Run the examples](docs/src/guides/examples.md).

## Quick start

Install the generator:

```bash
cargo install --path crates/gdl-gen
```

A graph file names the nodes, their message types, and the topology (`examples/chat/chat.gdl`):

```
name = "chat", schema = 1;
source   Frontend: Prompt -> stream Chunk;
operator Preprocess: (Prompt -> stream Chunk) over (Words -> stream Text);
forward  Truncate: Words -> Words;
sink     Engine: Words -> stream Token;
backward Detokenize: Token -> Piece;
backward Format: Piece -> Text;
backward Number: Chunk -> Chunk;
topology: Frontend => Preprocess { Truncate => Engine => Detokenize => Format } => Number;
```

A spec places the nodes in segments (`examples/chat/gdl.toml`):

```toml
[graph]
path = "chat.gdl"
[messages]
crates = ["protocols"]
[nodes]
crates = ["front-nodes", "engine-mock", "post-nodes"]
[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number"]
[segment.engine]
nodes = ["Engine"]
[segment.post]
nodes = ["Detokenize", "Format"]
```

Generate the segment crates and build the three binaries:

```bash
cargo run -p gdl-gen -- generate --spec examples/chat/gdl.toml --out examples/chat/segments
cargo build --manifest-path examples/chat/Cargo.toml --bins
```

[Getting started](docs/src/guides/getting-started.md) writes the three config files, starts the segments, and sends a request.

A graph that is not cut costs no encoding. A cut costs one encode and one decode for each message on each edge that crosses. See [Crossing performance](docs/src/operations/crossing-performance.md) for measurements.

## Build and test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
rustup target add wasm32-unknown-unknown            # once: gdl-model's tests check the wasm build
cargo test --workspace --no-run                     # build first: the timeout then counts only the tests
timeout 300 cargo test --workspace
node --test crates/gdl-web/tests/editor.cjs         # the composer's page
bash scripts/build-book.sh                          # needs mdbook, mdbook-mermaid, mdbook-linkcheck
```

The tests start velo nodes in one process on ports that the OS assigns. The example workspaces are outside the root workspace, so run their tests from their directories. See [Testing](docs/src/development/testing.md).

## Known limitations

See [Known limitations](docs/src/operations/limitations.md).

## Contributing

See [Documentation](docs/src/development/documentation.md) for where text goes and how to build the book. See [Workspace crates](docs/src/development/architecture.md) for the crate rules.
