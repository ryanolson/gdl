# Getting started

This guide runs the chat example as three segments on one machine. You build the segments with one command, and send one request.

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

## Build the segments

```bash
cargo run -p gdl-gen -- build --spec examples/chat/gdl.toml
```

`build` generates a contract crate and one crate per segment in `examples/chat/target/gdl/chat/gdl/workspace/`, and a `crossings.txt` that counts the crossings of one request. Then it builds them and prints the paths of the binaries `chat-engine`, `chat-front`, and `chat-post`, in `examples/chat/target/gdl/chat/gdl/bin/debug`. It writes nothing outside the target directory. Run it again after a change: it rebuilds only what the change reaches. See [Split, generate, and draw](split-and-generate.md#build) and [Code generation](../concepts/generation.md).

## Run the graph

`run` builds the segments as `build` does, then runs all three on this machine. It needs no Docker. It runs on Unix only.

1. **Start.** Run:

   ```bash
   cargo run -p gdl-gen -- run --spec examples/chat/gdl.toml
   ```

   Each segment prints a line such as `engine: ready` on stderr. The `front` segment then reads one JSON request per line from stdin.

2. **Send.** Type a request and press Enter:

   ```
   {"text": "hello big wide world", "max_words": 3}
   ```

   The output is one JSON line for each item, then a `done` line. The engine answers with the words in reverse order and in upper case, and `post` puts each word in angle brackets:

   ```
   {"item":{"index":0,"text":"<WIDE>"}}
   {"item":{"index":1,"text":"<BIG>"}}
   {"item":{"index":2,"text":"<HELLO>"}}
   {"done":null}
   ```

3. **Stop.** Press Ctrl-D to end the input. The `front` segment exits, and `run` sends one SIGTERM to each of the other segments. Ctrl-C stops all segments at once: the terminal sends SIGINT to each of them, and `run` sends no stop of its own. Each segment drains before it exits. `run` fails, and names the segment, if a segment fails or exits before `front` does. See [Segment lifecycle](../concepts/lifecycle.md).

To send requests from a script, pipe them in. `run` ends when its input ends:

```bash
echo '{"text": "hello big wide world", "max_words": 3}' | cargo run -p gdl-gen -- run --spec examples/chat/gdl.toml
```

The engine's and the post segment's settings come from the spec's `[settings.<segment>]` tables. `compose` writes the same tables into its bundle:

```toml
[settings.engine.node.Engine]
load_ms = 200
uppercase = true

[settings.post.node.Format]
open = "<"
close = ">"
```

`run` writes each segment's settings to a new temporary directory, and removes the directory when it ends. Each segment listens on a free port on `127.0.0.1`, and all segments share one roster directory. See [Segment settings](../operations/configuration.md).

### Run the segments yourself

`run` is a shortcut. A deployment starts each binary with its own settings file. Each file names a listen address and a roster directory, which must be new for each deployment. Start the engine and the post segment, then pipe requests to the front segment:

```bash
BIN=examples/chat/target/gdl/chat/gdl/bin/debug
D=$(mktemp -d) && mkdir $D/roster
for s in front engine post; do
  printf 'listen = "127.0.0.1:0"\nroster = "%s/roster"\n' "$D" > $D/$s.toml
done
$BIN/chat-engine $D/engine.toml &
$BIN/chat-post $D/post.toml &
echo '{"text": "hello big wide world", "max_words": 3}' | $BIN/chat-front $D/front.toml
kill %1 %2
```

These files have no node tables, so the answer is in lower case with square brackets: `[wide]`. `kill %1 %2` stops the two background segments, and each drains before it exits.

## Next

- Add a `[system]` table with a `port` to a segment's settings, in its `[settings.<segment>]` table, and read `GET /metrics` there. See [Health and metrics](../operations/observability.md).
- `crates/gdl-gen/tests/chat.rs` runs these steps as a test.
- More examples: [Run the examples](examples.md). Your own graph: [Define a graph](define-a-graph.md) and [Write nodes](write-nodes.md).
