# Getting started

This guide runs the chat example as three segments on one machine. You generate the segment crates, build them, and send one request.

You need a checkout of this repository and Rust 1.93.1, which `rust-toolchain.toml` pins. velo comes from Git, so the first build needs the network.

## Install `cargo-gdl`

```bash
cargo install --path crates/gdl-gen
```

This installs the `cargo-gdl` binary, so `cargo gdl <command>` works. Without installing, run `cargo run -p gdl-gen -- <command>` from the repository root. The commands below use the second form. `cargo gdl --help` lists every command. See [Split, generate, and draw](split-and-generate.md).

## The graph

`examples/chat/chat.gdl` defines the graph. Each line is a node with a role and its message types, and the last line is the topology:

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

The message types are in the `examples/chat/protocols` crate. The node implementations are in `front-nodes`, `engine-mock`, and `post-nodes`. See [The graph model](../concepts/model.md).

## The placement

`examples/chat/gdl.toml` is the spec. It names the graph file (`[graph]`), the crates that hold the messages (`[messages]`) and the nodes (`[nodes]`), the runtime to build against (`[gdl]`), and which segment hosts which node:

```toml
[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number"]
[segment.engine]
nodes = ["Engine"]
[segment.post]
nodes = ["Detokenize", "Format"]
```

Check the spec and draw the graph with its segments:

```bash
cargo run -p gdl-gen -- check --spec examples/chat/gdl.toml
cargo run -p gdl-gen -- diagram --spec examples/chat/gdl.toml --format mermaid
```

`check` prints `cargo gdl: no problems`. The diagram shows the three segments and the edges that cross them. See [Specs, segments, and layouts](../concepts/placement.md).

## Generate and build the segments

```bash
cargo run -p gdl-gen -- generate --spec examples/chat/gdl.toml --out examples/chat/segments
cargo build --manifest-path examples/chat/Cargo.toml --bins
```

`generate` writes a contract crate and one crate per segment under `segments/`, and a `crossings.txt` that counts the crossings of one request. Do not edit `segments/` by hand. Regenerate it. The build makes the binaries `chat-front`, `chat-engine`, and `chat-post` in `examples/chat/target/debug`. See [Code generation](../concepts/generation.md).

## Configure and start the segments

Each segment reads a TOML file that names its listen address and a roster directory. All segments of one deployment share the roster directory, and it must be new for each deployment:

```bash
REPO=$PWD
mkdir -p /tmp/chat && cd /tmp/chat
for s in front engine post; do
  printf 'listen = "127.0.0.1:0"\nroster = "/tmp/chat/roster"\n' > $s.toml
done
printf '[node.Engine]\nuppercase = true\n' >> engine.toml
```

Start the engine and the post segment in the background, then send a request to the front segment. The front segment reads one JSON line from stdin for each request:

```bash
BIN=$REPO/examples/chat/target/debug
$BIN/chat-engine engine.toml &
$BIN/chat-post post.toml &
echo '{"text": "hello big wide world", "max_words": 3}' | $BIN/chat-front front.toml
```

The output is one JSON line for each item, then a `done` line. The engine answers with the words in reverse order and upper case:

```
{"item":{"index":0,"text":"[WIDE]"}}
{"item":{"index":1,"text":"[BIG]"}}
{"item":{"index":2,"text":"[HELLO]"}}
{"done":null}
```

Stop the background segments with `kill %1 %2`. A stopped segment drains before it exits. See [Segment lifecycle](../concepts/lifecycle.md).

## Next

- Add a `[system]` table with a `port` to a segment's file and read `GET /metrics` there. See [Health and metrics](../operations/observability.md).
- `crates/gdl-gen/tests/chat.rs` runs these steps as a test.
- More examples: [Run the examples](examples.md). Your own graph: [Define a graph](define-a-graph.md) and [Write nodes](write-nodes.md).
