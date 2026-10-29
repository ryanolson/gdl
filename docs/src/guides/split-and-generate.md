# Split, generate, and draw

This guide takes a graph from a spec to checked, generated segment crates and built segment binaries. [Specs, segments, and layouts](../concepts/placement.md) explains what a placement means. The generator is the `cargo-gdl` binary of `crates/gdl-gen`. Install it with `cargo install --path crates/gdl-gen`, or run it from a checkout with `cargo run -p gdl-gen --`.

## The commands

`cargo gdl --help` prints every form. A bad argument prints it too. The command line has no per-command help, so the usage line is the reference.

| Command | What it does | Writes |
|---|---|---|
| `extract <dir \| name@version> [--catalog <dir>] [--verify] [--probe]` | Reads a crate's messages, exports, and nodes from its source, building nothing. `--catalog` adds the crate to a working catalogue. `--verify` also builds the crate and fails if what its markers register differs from what was read. `--probe` also builds the crate and records the build's wire capability for each instantiation of a generic message. | stdout, or a catalogue entry |
| `catalog <dir>` | Prints a working catalogue's index. | stdout |
| `resolve --spec <gdl.toml>` | Checks a graph file's names and prints the type each one resolves to. | stdout |
| `generate --spec <gdl.toml> --out <dir> [--check]` | Writes the contract crate of a graph file and one crate per segment. With `--check` it writes nothing and fails on drift. | `--out` |
| `build --spec <gdl.toml> [--out <dir>] [--release]` | Generates the same crates as one workspace in the target directory, builds every segment, and copies each binary out. Prints each binary's path. | the target directory, and `--out` |
| `check --spec <gdl.toml> [--fleet <fleet.toml>]` | Checks the spec as `generate` does, and a fleet file against it. Prints every problem. | nothing |
| `diagram --spec <gdl.toml> --format dot\|mermaid` | Draws the placed graph. Takes a spec of one layout. | stdout |
| `export --spec <gdl.toml> --out <file.tar.gz>` | Writes the generated crates as one archive with `build-all.sh`. Takes a spec of one layout. | the archive |
| `compose --spec <gdl.toml> --out <dir> [--fleet <fleet.toml>] [--local]` | Writes a Docker Compose bundle. A spec with classes or several layouts needs `--fleet`. With `--local`, the images build local sources, and the bundle builds only on this machine. | `--out` |

Every command that builds or fetches takes `--target-dir <dir>`. The default is `$CARGO_TARGET_DIR`, else `target` beside the spec (or beside the crate, for `extract`). Each command exits non-zero on a problem.

- [The catalogue](../concepts/catalogue.md) covers `extract` and `catalog`.
- [Deploy with Docker Compose](compose-deploy.md) and [Deploy a fleet of cells](fleet.md) cover `compose`.

## Write the spec

A spec names the graph, the crates that implement its nodes, and the placement. `examples/chat/gdl.toml`:

```toml
[graph]
path = "chat.gdl"

[messages]
crates = ["protocols"]

[nodes]
crates = ["front-nodes", "engine-mock", "post-nodes"]

[gdl]
path = "../../crates/gdl-runtime"

[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number"]

[segment.engine]
nodes = ["Engine"]

[segment.post]
nodes = ["Detokenize", "Format"]
```

A node crate marks each node type with `#[derive(gdl::Node)]`. The node's name is the type's name, and the role trait it implements gives its role and messages. Each graph node binds to the node of its name at a listed crate's root. If two crates offer one name, add `pin = { Engine = "chat_engine_mock::Engine" }` to `[nodes]`. [Write nodes](write-nodes.md) shows the node side.

To split a graph, move node names between segments. Any edge between two segments becomes a crossing, and its message must be a wire type. The segment count and the cut change the cost of a request: read `crossings.txt` after you generate.

A spec can also group segments into classes, set a cell policy, or hold several layouts. [The spec reference](../appendix/spec-reference.md) lists every key.

## Check

```bash
cargo gdl check --spec examples/chat/gdl.toml
```

`check` writes nothing. It prints each problem on its own line with its file, table, and key, and the line when the parser has one, then the count. With no problem it prints `cargo gdl: no problems`. Adding `--fleet fleet.toml` checks the fleet file against the spec's layouts as well.

```text
gdl.toml:14: [segment.engine] node: unknown field `node`, expected `nodes`
gdl.toml: [cells.zz]: cell policy `[cells.zz]`: the placement has no class `zz`
fleet.toml: [class.model_x] ready.front: not a segment of class `model_x`
cargo gdl: 3 problems
```

Every table refuses a key it does not know. A table of names (a segment in `ready`, a class in `requires`, a node in `[config]` or `[nodes] pin`) refuses a name that is not in the spec. A file that does not parse is one problem, and the other file is still checked. When the spec does not read, `check` judges only what needs no layout in the fleet file and says once that the cells' children were not checked. `generate`, `resolve`, `diagram`, and `export` report the spec's problems the same way.

## Build

```bash
cargo gdl build --spec examples/chat/gdl.toml
```

`build` is the short way from a spec to binaries. It generates the contract and segment crates into `<target-dir>/gdl/<graph>/<spec stem>/workspace/`, as one Cargo workspace, and runs one `cargo build` there. It never writes the source tree. It copies each segment's binary to `--out`, else to `bin/<profile>/` beside that workspace, and prints one path per line:

```text
/home/me/dynamo-gdl/examples/chat/target/gdl/chat/gdl/bin/debug/chat-engine
/home/me/dynamo-gdl/examples/chat/target/gdl/chat/gdl/bin/debug/chat-front
/home/me/dynamo-gdl/examples/chat/target/gdl/chat/gdl/bin/debug/chat-post
```

- Run it again after any change. It renders every file, but writes only the files whose text changed, so Cargo rebuilds only the crates a change reaches. A run with no change compiles nothing and copies nothing.
- A segment the spec no longer has is removed, with its binary. The generator removes only files it wrote. A file of yours in the workspace is kept, or the run fails and names it, as for `generate`. In `--out`, it writes over or removes only the binaries it copied there.
- `--release` builds with one codegen unit and fat LTO, the profile of the `export` and `compose` images.
- With several layouts, each layout's crates go in `<layout>/`, as for `generate`, and every segment of every layout is built.
- Cargo runs in the spec's directory, so the `.cargo/config.toml` there applies. `CARGO_NET_OFFLINE` and the other Cargo variables apply too. The build uses the Cargo and Rust that run `cargo gdl`, as every command does. Run it in your project, so that rustup picks the project's toolchain.

[Code generation](../concepts/generation.md#the-build-workspace) says how the workspace takes your crates, patches, and lock.

## Generate

Use `generate` when you want the crates in your own tree, to read them, or to build them in your own workspace.

```bash
cargo gdl generate --spec examples/chat/gdl.toml --out chat-segments
```

The output has `contract/` (for a graph file), one directory per segment, and `crossings.txt`. With several layouts, the segments go in `<out>/<layout>/<segment>`. Never edit a generated file. [Code generation](../concepts/generation.md) lists what each crate holds.

`generate --check` regenerates in memory and fails if the files in `--out` differ from the output. Run it in CI to catch a spec change whose output was not committed.

```bash
cargo gdl generate --spec examples/chat/gdl.toml --out chat-segments --check
```

It reports a missing, changed, or stale file, and a file in a segment crate that the generator did not write. To build the crates, list them in your workspace's `members`. A profile in their manifests would be ignored there, so `generate` writes none, and your workspace's root profile applies.

## Draw

```bash
cargo gdl diagram --spec examples/chat/gdl.toml --format mermaid
cargo gdl diagram --spec examples/chat/gdl.toml --format dot | dot -Tsvg -o chat.svg
```

The command writes the placed graph to stdout and no file. Request edges are solid and response edges are dashed. Each edge carries its message type's name, or `(no codec)` when the type has none, because no placement can cut that edge. Each segment is a cluster, and an edge that crosses segments is thick and red. In a program, `Manifest::to_dot` and `Manifest::to_mermaid` draw a contract's `manifest()`, with `None` for no segments or a `Layout` for a placed graph.

## Export

```bash
cargo gdl export --spec examples/chat/gdl.toml --out chat.tar.gz
tar xzf chat.tar.gz
sh chat/build-all.sh          # one `cargo build` per segment, into chat/target
```

`GDL_PROFILE` picks the profile (`release` by default), and the script passes its arguments to `cargo build`. Each segment's manifest sets the release profile to one codegen unit and fat LTO, so a release build of the archive takes longer than a dev build. A crate outside the archive is named by its absolute path, so the archive builds on the machine that wrote it. A spec whose crates all come from crates.io or git also gets images: `sh chat/build-images.sh` builds one image per segment, named for its package after `GDL_IMAGE_PREFIX`. [Code generation](../concepts/generation.md#export-and-images) says what the archive holds.
