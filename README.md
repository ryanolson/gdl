# dynamo-gdl

gdl is a graph definition language and runtime for request/response streaming pipelines. A request goes into a graph of nodes, and a stream of items comes back. The same graph runs in one process, or split into segments that run as separate programs and talk over [velo](https://github.com/ai-dynamo/velo).

You define the graph once, with its message types. A placement puts each node in a segment. The generator writes one crate per segment, and each segment crate depends only on the implementations of the nodes that it hosts. `cargo gdl build` generates the crates in the target directory and builds them. `cargo gdl generate` writes them into your tree.

NOTE: gdl is experimental. Its design and tests are still in progress, and its APIs are not stable.

**Documentation: the gdl book.** The source is in [`docs/src/`](docs/src/SUMMARY.md). Each push to `main` publishes it to a private GitHub Pages site, <https://crispy-adventure-qq4pemj.pages.github.io/>, which anyone who can read this repository can open. To build it locally, run `scripts/build-book.sh`, then open `docs/book/html/index.html`.

## Highlights

- **One graph, any placement.** Move a node to another segment without changing the graph or the node code. See [Specs, segments, and layouts](docs/src/concepts/placement.md).
- **Nodes read, not built.** The composer and generator read what a crate offers from its source, without building it. See [The catalogue and the source reader](docs/src/concepts/catalogue.md).
- **Streams in both directions.** A request can carry a stream of items, and every response is a stream. See [Streamed requests](docs/src/concepts/streamed-requests.md).
- **Live rosters.** Instances join, retire, and drain while the others serve. A node can register a hook that hands off state as its instance leaves, and each hook's outcome is on `/metrics`. A controller can retire an instance through its pool's hub, and the instance then holds until the controller removes it. See [Segment lifecycle](docs/src/concepts/lifecycle.md).
- **Selectors and offers.** Route a crossing by round robin, random, or a selector: gdl's, or one from your crate that the spec binds or your code registers. Fail over when an instance declines. See [Routing, selectors, and offers](docs/src/concepts/routing.md).
- **Dynamo-shaped health and metrics.** A segment binary and the hub binary also write their logs to stderr, at the level `RUST_LOG` sets. Each segment instance shows its phase and what it still holds (admitted requests, egress permits, open anchors), so an operator can follow its drain, and, when asked, velo's own metrics. A chooser shows the load, state, and offer round trips of each pool it sends to, and a pool's hub serves its pool's load and its follows' links, the figures a pool policy is built from. Every segment and hub serves its own process's CPU and memory, and its cgroup's CPU use to the microsecond, throttling, quota, and memory peak. See [Health and metrics](docs/src/operations/observability.md).
- **Pools.** One graph across several deployments, each with its own hub. A pool policy chooses the pool for each request: gdl's, or one from your crate that the spec binds or your code registers. A node beside the chooser can read the same pools, which were drained on purpose, and which pools list each property. An entry whose nodes say which property each request wants declines a request for one it does not have, and the chooser offers it to another pool. A policy can keep a request with a pool that is full: the chooser waits for room there until the request's deadline. The graph declares each property kind and who provides and needs it, and `cargo gdl check` refuses a class that a reader needs a kind from when nothing supplies it. See [Classes and pools](docs/src/concepts/pools.md), [Declare properties](docs/src/guides/define-a-graph.md#declare-properties), [Write a pool policy](docs/src/guides/write-a-pool-policy.md), [What a node reads](docs/src/concepts/pool-routing.md#what-a-node-reads), and [The property index](docs/src/concepts/pool-routing.md#the-property-index).
- **Docker Compose bundles.** `cargo gdl compose` writes a hub and a service per segment, and mounts the file secrets a spec names without reading them. See [Deploy with Docker Compose](docs/src/guides/compose-deploy.md).
- **Kubernetes objects.** `cargo gdl k8s` writes a fleet's objects, one directory per cluster: a hub and a Deployment per segment for each pool, with the Kubernetes Secrets a cluster names mounted, never read. Each pod says where a Prometheus scrapes it, and a fleet can ask for a PodMonitor in each cluster. A cluster can keep the fleet's pods on a node group of their own, by a node selector and tolerations, and give every pod CPU and memory requests and limits. `cargo gdl images` builds the hub's image and each segment's for each cluster's platform, and pushes them to its registry, tagged by what they are built from; `k8s --images` then names each by digest. `cargo gdl deploy` runs both, applies the objects with each cluster's kube context, prunes and drains the pools the fleet no longer places, and waits for each pool's health. With `--local`, the images build gdl and the spec's crates from this machine's sources as Git holds them, so a change that is not published is what runs. `cargo gdl down` drains and deletes them. A cluster can ask the gdl operator to scale its segments: an HPA scales each through a `GdlScaler`, and on a scale-down the operator removes the instance it chooses by load, out of its entry Service first, and lets it drain. See [Deploy a fleet on Kubernetes](docs/src/guides/kubernetes.md) and [Scale a segment with the operator](docs/src/guides/operator.md).
- **A composer.** Draw a graph in a browser, place it, and deploy it with Compose. See [Compose in the browser](docs/src/guides/composer.md).

## Crates

gdl is two layers. A crate that implements nodes depends on `gdl`: the node roles, the request context, selectors, pool policies, the derives, and local composition, so that it can run its nodes in one process. It does not link velo. The runtime, `gdl-runtime`, runs those nodes: segments, crossings over velo, discovery, and the segment binaries. Contract crates, generated segment crates, and programs that run a graph depend on `gdl-runtime`, which re-exports the `gdl` it was built with as `gdl_runtime::gdl`. The derives expand to `::gdl::...`, so a crate that uses one depends on `gdl` itself, also when it depends on `gdl-runtime`.

| Crate | For | Contents |
|---|---|---|
| `gdl` | Authors of nodes and routers | The request context, role traits, local composition, selectors, pool policies, the followed pools' view and property index, the pool hub's record tables, metrics, and the derives. No velo |
| `gdl-runtime` | Programs that run a graph | Segments, crossings over velo, discovery, the segment binary, and `graph!`. Re-exports `gdl` and the model |
| `gdl-model` | Tools | The manifest, placement, layout, diagrams, and the fleet file, without the runtime. Builds for wasm |
| `gdl-syntax` | The macro and the generator | The graph parser and checked model |
| `gdl-message` | Authors of messages | `derive(Message)` and the message catalogue, without the runtime |
| `gdl-macros` | Internal | `graph!`, the generated segments' `segment!`, and the derives |
| `gdl-extract` | Tools | The source reader and the working catalogue |
| `gdl-gen` | Users | The `cargo-gdl` binary: `extract`, `catalog`, `resolve`, `generate`, `build`, `check`, `diagram`, `compose`, `k8s`, `export` |
| `gdl-k8s` | `cargo gdl k8s` and the operator | A pool's Kubernetes objects, rendered from the pool alone. No velo |
| `gdl-web` | Users | The composer: a page and its server |
| `gdl-pools` | `gdl`, hub, and runtime | The pool plane's shared types: the pool record, a pool's state, load summary, presence, and properties, the `PoolDirectory` backends, and an operator's commands for a pool whose hub is gone. No velo |
| `gdl-pools-dynamodb` | Deployments across regions | A pool directory in DynamoDB global tables, which the `gdl-hub` binary takes with its `dynamodb` feature |
| `gdl-cgroup` | Hub and runtime | A process's own CPU and memory, and its cgroup's CPU use and throttling, quota, and memory, read at each scrape |
| `gdl-watch` | Hub and runtime | The watches a pool's hub serves, as typed events, and their client |
| `gdl-hub` | Deployments | The discovery hub, and a pool's quorum, health, record, follows, and keyed records |
| `gdl-operator` | Deployments on Kubernetes | The operator: `GdlPool` owns a pool's objects and stops the pool in order through its finalizer, and `GdlScaler` scales a pool's segment through `/scale`, removing the instance it chooses by load once it is out of its entry Service. No velo |

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

Generate the segment crates and build the three binaries, in `examples/chat/target`:

```bash
cargo run -p gdl-gen -- build --spec examples/chat/gdl.toml
```

[Getting started](docs/src/guides/getting-started.md) writes the three config files, starts the segments, and sends a request.

A graph that is not cut costs no encoding. A cut costs one encode and one decode for each message on each edge that crosses. See [Crossing performance](docs/src/operations/crossing-performance.md) for measurements.

## Build and test

The Python scripts (the book's heading check and the benchmark runners) run with [uv](https://docs.astral.sh/uv/getting-started/installation/), which reads each script's header and picks the interpreter. Install uv once; no other Python setup is needed.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
rustup target add wasm32-unknown-unknown            # once: gdl-model's tests check the wasm build
cargo install cargo-nextest --locked                 # once: the test runner
cargo nextest run --workspace --no-run              # build first: the timeout then counts only the tests
timeout 300 cargo nextest run --workspace
timeout 60 cargo test --workspace --doc             # doctests: nextest does not run them
node --test crates/gdl-web/tests/editor.cjs         # the composer's page
bash scripts/build-book.sh                          # needs mdbook, mdbook-mermaid, mdbook-linkcheck, uv
bash scripts/dynamodb-local.sh                      # the DynamoDB directory and the hub on it against DynamoDB Local; needs Docker
bash scripts/kubeconform.sh                         # the pinned Kubernetes objects against pinned schemas; INSTALL_TOOLS=1 fetches them
```

The tests start velo nodes in one process on ports that the OS assigns. The example workspaces are outside the root workspace, so run their tests from their directories. See [Testing](docs/src/development/testing.md).

## Known limitations

See [Known limitations](docs/src/operations/limitations.md).

## Contributing

See [Documentation](docs/src/development/documentation.md) for where text goes and how to build the book. See [Workspace crates](docs/src/development/architecture.md) for the crate rules.
