# dynamo-gdl

gdl is a graph definition language and runtime for request/response streaming pipelines. A request goes into a graph of nodes, and a stream of items comes back. The same graph runs in one process, or split into segments that run as separate programs and talk over [velo](https://github.com/ai-dynamo/velo).

You define the graph once, with its message types. A placement puts each node in a segment. The generator, `gdl-gen`, writes one crate per segment. Each segment crate depends only on the implementations of the nodes that it hosts.

## The model

A graph has one source and a nested chain of hops. Each node has one of five roles:

| Role | Sees | Does |
|---|---|---|
| source | nothing | Sends the request into the graph and receives the item stream. It is the caller, not an implementation. |
| operator | the request and the response | Calls the rest of the graph through `Next`, zero or more times (for example, to retry), and returns a stream. |
| forward | the request only | Changes the request on its way in. |
| sink | the request | Produces the item stream. |
| backward | the response stream only | Changes the item stream on its way out. |

A hop is an outer end (the source or an operator), its forward nodes, an inner end (a sink or an operator), and its backward nodes. An operator is the inner end of one hop and the outer end of the next hop.

## Define a graph

A contract crate holds the message types and one `gdl::graph!` invocation:

```rust
gdl::graph! {
    name = "chat", schema = 1;
    source   Frontend: Prompt -> stream Chunk;
    operator Preprocess: (Prompt -> stream Chunk) over (Words -> stream Text);
    forward  Truncate: Words -> Words;
    sink     Engine: Words -> stream Token;
    backward Detokenize: Token -> Piece;
    backward Format: Piece -> Text;
    backward Number: Chunk -> Chunk;
    topology: Frontend => Preprocess { Truncate => Engine => Detokenize => Format } => Number;
}
```

In the topology, each hop lists its forward nodes, then its inner end, then its backward nodes in the order that the response goes through them. An operator owns its hop in braces.

The macro checks the topology at compile time. An implementation with the wrong role or types for its node does not compile. The macro generates:

- `Bindings`: one slot for each node other than the source, filled with `Bindings::new().preprocess(...).engine(...)`.
- `local(bindings)`: the whole graph as nested calls in one process.
- `assemble(&deployment, bindings)`: the part of the graph that one segment runs. It fails if the deployment was made for another contract, or if a node that the segment hosts has no binding.
- `manifest()`: the graph as data, with the wire capability of each edge type.
- `SourceReq` and `SourceItem`: the source's request and item types.

Bump `schema` when the wire form of a message type changes.

## Split a graph into segments

A spec file names the contract, the placement, and the implementation of each node. `examples/chat/gdl.toml` is a full spec:

```toml
[contract]
path = "contract"

[gdl]
path = "../../crates/gdl"

[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number"]

[segment.engine]
nodes = ["Engine"]

[bind.Engine]
crate = "chat-engine-mock"
path = "engine-mock"
type = "chat_engine_mock::Engine"
```

The generator compiles only the contract crate to read its manifest. Then it checks the placement and the bindings against the manifest, and it reports every error that it finds. A message type that crosses between segments must implement `Serialize` and `Deserialize`.

For each segment, the generator writes a crate with:

- A library. `start(node, roster, config, nodes)` deploys the segment inside a program.
- A binary. It builds each hosted node through `gdl::Build`, meets the other segments in a shared roster directory, and waits until every instance that it can send to is serving. Then it serves until Ctrl-C.
- On the segment that hosts the source, the binary instead reads one JSON request per line on stdin, writes the items as JSON lines (`gdl::serve_json_lines`), and stops at the end of the input. It does this only if the source's request type and item type both implement `Serialize` and `Deserialize`. For other source types, the binary serves until Ctrl-C, and your own program calls the graph through the generated library.

The generator owns the directory of each segment crate. Each generated file starts with a do-not-edit marker. `gdl-gen --check` writes nothing. It fails if a generated file is missing, changed, or stale, or if a segment crate holds a file that the generator did not write.

## Draw a graph

`gdl-gen --diagram` writes the placed graph to stdout as Graphviz DOT or as a Mermaid flowchart. It writes no files.

```bash
cargo run -p gdl-gen -- --spec examples/chat/gdl.toml --diagram mermaid
cargo run -p gdl-gen -- --spec examples/chat/gdl.toml --diagram dot | dot -Tsvg -o chat.svg
```

- Request edges are solid. Response edges are dashed.
- Each edge has the name of its message type. If the type has no codec, the label says `(no codec)`, because a placement cannot cut that edge.
- Each segment is a cluster. An edge that crosses segments is thick and red.

In a program, `Manifest::to_dot` and `Manifest::to_mermaid` draw a contract's `manifest()`. Give them `None` to draw the graph without segments, or a `Layout` from `gdl::layout` to draw it placed. They return an error if the layout was made for another contract.

## Run the example

1. Generate the segment crates:

   ```bash
   cargo run -p gdl-gen -- --spec examples/chat/gdl.toml --out examples/chat/segments
   ```

2. Build the three segment binaries:

   ```bash
   cargo build --manifest-path examples/chat/Cargo.toml --bins
   ```

3. Write one config file for each segment. Each config file names a listen address and a roster directory. The roster directory must be fresh for each deployment, and all segments must share it.

   ```toml
   listen = "127.0.0.1:0"
   roster = "/tmp/chat-roster"
   [node.Engine]
   uppercase = true
   ```

   If a segment runs more than one instance, give the count of instances in an `[instances]` table, for example `engine = 2`, in every config file. All instances must agree on these counts.

4. Start `chat-engine` and `chat-post`, each with its config file.
5. Start `chat-front` with its config file, and write a request to its stdin:

   ```bash
   echo '{"text": "hello big wide world", "max_words": 3}' | examples/chat/target/debug/chat-front front.toml
   ```

`crates/gdl-gen/tests/chat.rs` does these steps as a test.

## Across segments

- **Responses.** Each response stream goes to an anchor that its consumer created. The runtime never matches a response to a request by a request id.
- **Stop and cancel.** A stop ends the stream after the items that are already buffered. A cancel ends it at once. Both reach the remote producer, also when the consumer holds the stream and does not read it.
- **Deadlines.** A deadline crosses segments as wall-clock time with a clock-skew allowance. Work can end early by up to two times the skew, but never late. A request without a deadline gets `RuntimeConfig::default_deadline` (600 s) before it leaves its segment.
- **Admission.** Each instance admits a limited number of root requests. A request that enters the same instance more than one time uses one admission.
- **Egress.** Each instance limits the number of streams that it produces at the same time. A full instance refuses new work.
- **Failure.** An error item ends a stream. A stream that ends without a finish from its producer ends with an error, not as a success.

The design and its rulings are in `agent-docs/plan.md`.

## Cost

A graph that is not cut costs no encoding. A local graph is nested calls, and the nodes of one segment pass values to each other directly. A cut costs one encode and one decode for each message on each edge that crosses segments. There is one more case. If the request of a hop leaves the segment of the hop's outer end, the response items return to the outer end through an anchor. Each item is then encoded and decoded one time, also when no response edge crosses segments. `crates/gdl/tests/crossings.rs` counts these encodes.

On one aarch64 workstation, over loopback TCP:

- **Per request.** The seven-node reference graph takes about 26 us in one process and about 223 us over two segments. A request with one hop over two segments takes about the same time as the same request written directly on velo (about 200 us to 250 us; the run-to-run variance is larger than the difference). gdl adds two tasks and 48 allocations to each request.
- **Per item, at each crossing.** Encoding and decoding cost 123 ns (16 B), 223 ns (1 KiB), and 811 ns (16 KiB). Of that, 80 ns to 400 ns is more than velo direct, which also encodes.
- **Throughput.** Item throughput is 0% to 11% lower than direct velo, about the size of the run-to-run variance. velo's rate for a single stream, about 11 K items/s here, limits both.

The measurements and the method are in `agent-docs/perf/m3-remote.md`.

## Repository layout

| Path | Contents |
|---|---|
| `crates/gdl` | Request context, node roles, local composition, manifest, placement, and the velo runtime |
| `crates/gdl-macros` | The `graph!` macro |
| `crates/gdl-gen` | The generator |
| `examples/chat` | A contract, its node implementations, and the generated segment crates. It is a separate Cargo workspace. |
| `agent-docs/` | The plan, review records, and measurements |

## Build and test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
timeout 900 cargo test --workspace --all-features
```

The tests start velo nodes in one process on ports that the OS assigns. The example test builds the example workspace in `target/examples`.
