# dynamo-gdl

gdl is a graph definition language and runtime for request/response streaming pipelines. A request goes into a graph of nodes, and a stream of items comes back. The same graph runs in one process, or split into segments that run as separate programs and talk over [velo](https://github.com/ai-dynamo/velo).

You define the graph once, with its message types. A placement puts each node in a segment. The generator, `cargo gdl generate`, writes one crate per segment. Each segment crate depends only on the implementations of the nodes that it hosts.

gdl is two crates. A crate that implements nodes depends on `gdl`: the node roles, the request context, the selector trait, the derives, and local composition, so that it can run its nodes in one process. It does not link velo. The runtime, `gdl-runtime`, runs those nodes: segments, crossings over velo, discovery, and the segment binaries. Contract crates, generated segment crates, and programs that run a graph depend on `gdl-runtime`, which re-exports the `gdl` it was built with as `gdl_runtime::gdl`. The derives expand to `::gdl::...`, so a crate that uses one depends on `gdl` itself, also when it depends on `gdl-runtime`.


Known limitations, with what lifts each, are in [LIMITATIONS.md](LIMITATIONS.md).
## The model

A graph has one or more source roots, each with a nested chain of hops. A sink can receive requests from several roots through explicit connections. Each node has one of five roles:

| Role | Sees | Does |
|---|---|---|
| source | `Frontend` for a hosted service | Sends typed requests into the graph and receives response streams. It can also remain a programmatic entry point. |
| operator | the request and the response | Calls the rest of the graph through `Next`, zero or more times (for example, to retry), and returns a stream. |
| forward | the request only | Changes the request on its way in. |
| sink | the request | Produces the item stream. |
| backward | the response stream only | Changes the item stream on its way out. |

A hop is an outer end (the source or an operator), its forward nodes, an inner end (a sink or an operator), and its backward nodes. An operator is the inner end of one hop and the outer end of the next hop.

## Define a graph

A contract crate holds the message types and one `gdl_runtime::graph!` invocation:

```rust
gdl_runtime::graph! {
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

This is the same graph as a diagram, from `manifest().to_mermaid(None)`. Solid edges carry requests, and dashed edges carry responses. Each edge has the name of its message type.

```mermaid
---
title: "chat"
---
flowchart LR
    n0(["Frontend<br/>source"])
    n1[["Preprocess<br/>operator"]]
    n2["Truncate<br/>forward"]
    n3[("Engine<br/>sink")]
    n4("Detokenize<br/>backward")
    n5("Format<br/>backward")
    n6("Number<br/>backward")
    n0 -->|"Prompt"| n1
    n1 -.->|"Chunk"| n6
    n6 -.->|"Chunk"| n0
    n1 -->|"Words"| n2
    n2 -->|"Words"| n3
    n3 -.->|"Token"| n4
    n4 -.->|"Piece"| n5
    n5 -.->|"Text"| n1
```

The macro checks the topology at compile time. An implementation with the wrong role or types for its node does not compile. The macro generates:

- `Bindings`: one slot for each node other than the source, filled with `Bindings::new().preprocess(...).engine(...)`.
- `local(bindings)`: the whole graph as nested calls in one process.
- `assemble(&deployment, bindings)`: the part of the graph that one segment runs. It fails if the deployment was made for another contract, or if a node that the segment hosts has no binding.
- `manifest()`: the graph as data, with the wire capability of each edge type.
- `SourceReq` and `SourceItem`: the source's request and item types.

Bump `schema` when a message's serde representation changes. Declared codec changes update the contract hash automatically.

### Streamed requests

A request can also be a stream. In a request position, `Head + stream Item` declares a request that has a head value and a stream of items, and `stream Item` declares one with a head of `()`. The node receives a `gdl::Streamed<Head, Item>`. Its items end after their first error. The response streams back while the request still streams in.

```rust
sink Recognizer: Session + stream Audio -> stream Transcript;
```

A streamed request can cross segments like any other request. The stage that receives it creates an anchor for its items, and the sender streams the items into that anchor while the response comes back.

### Compose a graph from message names

A graph file holds the body of a `graph!` invocation. Where `graph!` has Rust types, the file has message names. In a message position, the file accepts a bare name such as `Prompt`, or `()`.

The message types are in one or more protocols crates. A protocols crate depends on `gdl-message`, a small crate without the gdl runtime, and offers a type to graph files with `derive(gdl_message::Message)`. The type must be `Send + 'static`, and it must not be generic:

```rust
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
pub struct Prompt {
    pub text: String,
}
```

A crate can also offer a message that is defined in another module or another crate, under a path of its own. Mark the `pub use` with `#[gdl_message::export]`:

```rust
#[gdl_message::export]
pub use chat_protocols::Prompt;
```

A crate that depends on gdl can write `gdl::Message` and `gdl::export` instead.

Messages cross segments as named-field MessagePack by default. This encoding preserves omitted optional fields, including fields in nested structs. A message can select its encoding:

```rust
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "json")]
pub struct ChatRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    pub prompt: String,
}
```

| Annotation | Encoding |
|------------|----------|
| Omitted, or `#[gdl(codec = "msgpack")]` | MessagePack maps with field names |
| `#[gdl(codec = "msgpack_array")]` | Positional MessagePack arrays. Fields must not be conditionally omitted. |
| `#[gdl(codec = "json")]` | JSON |

The codec applies to the complete message, including nested values. Nested message annotations do not change codecs during serialization. A streamed request selects the head and item codecs separately. A third-party type can use a local wrapper with `#[serde(transparent)]` and `derive(Message)`.

The catalogue records each message's encoding. The contract hash includes the encoding and its wire revision, so changing the codec changes the deployment identity. Receivers and typed routing selectors use the selected decoder. Messages need `Serialize` and `DeserializeOwned` to cross. These traits do not prove that arbitrary custom serde implementations round-trip.

JSON writes nonfinite floats as `null` and rejects map keys such as tuples. Use `msgpack` for these values. Positional arrays reduce encoded size for fixed layouts, but require every field to stay present. See [codec limits](LIMITATIONS.md#graphs-and-types).

MessagePack reserves a small output buffer to avoid repeated growth for token-sized values. [Codec measurements](agent-docs/perf/codecs.md) record CPU cost, allocation counts, and payload sizes for all three encodings.

Catalogue entries without encoding metadata require re-extraction. Remove the entry named by the load error first. With the current `cargo-gdl`, re-add directory or registry crates using `cargo gdl extract` and the same `--catalog` directory. Re-import Git crates through the composer after removing their entries.

`cargo gdl extract <crate dir | name@version>` prints what a crate offers (its messages, exports, and nodes), read from its source without building it. A `name@version` is fetched from crates.io with `cargo metadata`, which builds nothing. With `--catalog <dir>`, it adds the crate to a working catalogue instead: `<dir>/<name>/<version>.toml`, with the time it was added and the registry's checksum of the crate. A published version already there is kept, because it never changes. A crate from a directory has one entry, which adding it again replaces, since it changes as it is edited. `cargo gdl catalog <dir>` prints the catalogue's index: each entry, each message name with the messages of that name, and each node with the messages at its ports. With `--verify`, it also builds the crate and fails if what the markers register differs from what it read, for example a message that a macro makes. The generator is the `cargo-gdl` binary of `crates/gdl-gen`: install it with `cargo install --path crates/gdl-gen`, or run it with `cargo run -p gdl-gen --`.

A spec names the graph file instead of a contract crate, and lists the crates that offer its messages. A listed crate is a directory, `name@version` on crates.io, or a Git table: `{ package = "protocols", git = "https://example.com/repo", rev = "<commit>" }`. A Git table can also specify `lib` when the library name differs from the package name. The generator fetches and reads remote crates. Generated crates depend on such a crate at exactly that version:

```toml
[graph]
path = "chat.gdl"

[messages]
crates = ["protocols"]
pin = { Chunk = "chat_protocols::Chunk" }   # optional

[gdl]
path = "../../crates/gdl-runtime"
```

`cargo gdl resolve --spec gdl.toml` prints the type that each message name resolves to, and whether it is a wire type. It fetches a crate listed by version into `--target-dir` (`target` beside the spec by default):

```text
Chunk   chat_protocols::Chunk          wire
Piece   chat_protocols::Piece          local
Prompt  chat_protocols::Prompt         wire
```

First, the generator makes sure that the names agree on each edge. Each error gives the line and column in the graph file. Then it reads the source of the listed crates with `gdl-extract`. It builds nothing, so a crate whose build needs a toolkit that is not installed can still be read. A listed crate offers the messages that it defines and the messages that it exports. Each name must resolve to exactly one message. A message that two listed crates offer, one by its definition and one by an export, is one message. If a name resolves to two different messages, the generator reports an error. Add a `pin` entry to choose one. The generator reports all errors at once.

From a `[graph]` spec, the generator also generates the contract crate, in `contract/` under the output directory. Its `graph!` invocation names each message by a public path through a listed crate: an export's path if there is one, else the definition's. The contract hash covers that path, so it changes when a name resolves to another message. A message that is defined in a private module must be exported: resolution reports it, read from source. The chat example uses a graph file. The ASR example uses a contract crate that holds its own `graph!` invocation.

## Split a graph into segments

A spec file names the graph, the placement, and the implementation of each node. `examples/chat/gdl.toml` is a full spec. It names the graph file, the crate that holds its messages, the crates that implement its nodes, and the placement in three segments:

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

A node crate marks each node type with `#[derive(gdl::Node)]`. The node's name is the type's name, and its role and messages come from the role trait that the type implements:

```rust
#[derive(gdl::Node)]
pub struct Engine { /* .. */ }

impl gdl::Sink for Engine {
    type Req = Words;
    type Item = Token;
    // ..
}
```

Each graph node binds to the node of its name that a listed crate offers. Its implementation is the type of that name at the crate's root, here `chat_engine_mock::Engine`. If two crates offer one name, add `pin = { Engine = "chat_engine_mock::Engine" }` to `[nodes]` to choose one. The node must have the role that the graph gives it, and at each port the message that the graph's name for the port resolves to. The generator reads the node crates' source and does not compile them, so a crate that needs CUDA, for example, does not have to build there. When the node crate compiles, the derive makes sure that the type is `pub` and sits at the crate root.

A spec with a contract crate binds each node with a table instead:

```toml
[bind.Engine]
crate = "chat-engine-mock"
path = "engine-mock"
type = "chat_engine_mock::Engine"
```

A spec can also group its segments into classes: sets of segments that deploy together. For the chat graph, with the engine in a class of its own, Preprocess's hop crosses into that class, so its backward stages move into Preprocess's segment (the boundary rule below):

```toml
[segment.front]
nodes = ["Frontend", "Preprocess", "Truncate", "Number", "Detokenize", "Format"]

[segment.engine]
nodes = ["Engine"]

[class.global]
segments = ["front"]

[class.model_x]
segments = ["engine"]
```

When a spec has a class, each of its segments must be in exactly one class. A class name is lowercase letters and digits in words joined by single `_`s. Classes are part of the placement's identity: adding, renaming, or changing a class changes the placement hash. A spec without classes keeps its hash.

A hop crosses a class boundary when one of its stages is in a class other than its outer end's. Such a hop may cross only at its inner end: its inner end is in the other class, every other stage of the hop is in the outer end's segment, and every hop nested under the inner end stays in the inner end's class. The boundaries may not form a cycle: no class crosses into a class that crosses back into it. Put the boundary at an operator, as in `Http => Pre { Route { Model { Engine => Detok } } }` with `Route` in one class and `Model`, `Engine`, and `Detok` in another. With classes, the crossing report also counts the WAN round trips each boundary adds when a request spills to a cell in another region: 1 for a request value, 2 for a streamed request, whose items wait for the input anchor. Nothing deploys by class yet (see `LIMITATIONS.md`).

A class that a boundary enters can have a cell policy: how the hop's outer end picks a cell, one running deployment of the class, to send to.

```toml
[cells.model_x]
policy = "home_first"   # the policy's name; round_robin by default
```

The key is optional, and no other key is accepted: `spill_rtt_ms` and `max_near`, which chose near cells, are refused, since a chooser offers only to its own cell's children. A policy name has a selector name's shape. The spec does not check it against a list, since a custom policy can be registered later; a chooser that cannot run it refuses to start (the runtime provides `home_first` and `round_robin`). A value equal to its default is not recorded: a spec that spells out the default has the same placement and hash as one that leaves it unsaid. The placement hash covers each class's effective policy, the default included, so a placement with classes that names no policy hashes as one that names `round_robin`, and a change of default would change its hash rather than what it means. Any other value changes the placement hash. A `[cells.<class>]` table must name a class of the spec that a class boundary enters. A chooser runs the policy (see the hub's cells, below).

One graph can be split differently in different places: three classes in one region, say, and two in another. Each split is a layout, with its own segments, `[routing]`, classes, and cell policies, and a spec can hold several layouts of its graph, each in a `[layout.<name>]` table that holds what a placement holds at the top of a spec. This spec places `crates/gdl-gen/tests/fixtures/layouts/trio.gdl`, the chat graph's nodes in two roots (`Ask => Preprocess { Truncate => Engine => Detokenize => Format } => Number` and `Tell => Spare`), in three classes in layout `regional` and in two in layout `compact`:

```toml
[layout.regional.segment.front]
nodes = ["Ask", "Preprocess", "Truncate", "Detokenize", "Format", "Number"]
[layout.regional.segment.side]
nodes = ["Tell"]
[layout.regional.segment.engine]
nodes = ["Engine", "Spare"]
[layout.regional.class.global]
segments = ["front"]
[layout.regional.class.batch]
segments = ["side"]
[layout.regional.class.model_x]
segments = ["engine"]

[layout.compact.segment.front]
nodes = ["Ask", "Preprocess", "Truncate", "Detokenize", "Format", "Number"]
[layout.compact.segment.side]
nodes = ["Tell"]
[layout.compact.segment.engine]
nodes = ["Engine", "Spare"]
[layout.compact.class.global]
segments = ["front", "side"]
[layout.compact.class.model_x]
segments = ["engine"]
```

Every layout places the same graph, under the same contract and contract hash, and nothing else is shared between layouts. A layout name has a class name's shape. Each layout is checked on its own as a complete graph: every node placed, the classes partitioning its segments, the boundary rule, and no cycle of classes. Its problems name its tables (``gdl.toml: [layout.compact.class]: segment `side` is in no class``), and a problem of its in no table, such as a type that cannot cross where the layout makes it cross, names the layout (`[layout.compact]`). Each layout has its own placement hash, the hash of its placement as if it were the spec's only one: two layouts that place the graph alike have the same hash, and layouts that place it differently have different ones. A spec with `[layout]` tables has no `[segment]`, `[routing]`, `[class]`, or `[cells]` table at its top level. A spec without `[layout]` tables has one layout, named `default`, and keeps its form and its hash. `[deployment.segment.<name>]` names a segment of any layout, and serves it in each layout that has it.

The cells themselves are not part of the spec: cells are runtime topology, like replicas, and do not change the placement hash. A fleet file, `fleet.toml`, lists them, with each class's default quorum (the usable instances of each segment a cell needs to be ready) and what its cells' health requires of the classes it sends to:

```toml
[class.global]
ready = { front = 2 }             # default quorum of a cell of this class, per segment
requires = { model_x = 1 }        # optional: the cell's health needs this many ready children of each class it sends to; 1 of each when unsaid

[class.model_x]
ready = { engine = 4 }

[cell.global-us-east-1]
class = "global"
region = "us-east-1"

[cell.model-x-01-us-east-1]
class = "model_x"
region = "us-east-1"
home = "global-us-east-1"         # the parent cell, required when another class sends to this one: whose hub follows it
ready = { engine = 8 }            # optional: overrides the class's default
```

A cell runs one layout of the spec, which its `layout` names: required when the spec has more than one, and optional, meaning the only one, when it has one (a spec without `[layout]` tables calls its one layout `default`). Its class is a class of that layout, and its quorum is that class's segments'. A cell's placement hash is its layout's, and a hub follows only cells whose contract and placement hashes equal its own, so cells of different layouts never follow or offer to each other; the runtime needs no layout name to keep them apart, and a cell's hub publishes none.

```toml
[cell.model-x-01-eu-west-1]
layout = "compact"
class = "model_x"
region = "eu-west-1"
home = "global-eu-west-1"         # a cell of layout `compact`
```

A cell's `home` is its parent cell: the cell of a class that sends to this one that follows it and offers it work. It is required for every cell of a class that another class sends to, and refused for a cell of a class that nothing sends to, such as a global cell. Its cell's record names it, and the parent's hub follows the cell for that: a hub follows its children, the cells whose record names its own cell as their `home`, and no other cell (see the hub's cells, below). A deployment is complete per parent cell: every cell whose class sends to another class has at least one child of each class its class sends to, in its own layout. With three classes in a chain (two roots, one crossing from the top class into the middle one and another from the middle one into the bottom), this holds at every level: a middle cell is a child of a top cell and a parent of a bottom one.

`gdl_model::Fleet` reads the file and checks each cell against the layout it runs, reporting every problem: a cell without the `layout` that a spec of several layouts needs, a layout the spec does not have, a class its layout does not have, a `ready` key that is not a segment of the class, a `requires` key that is not a class it sends to, a count of 0, a cell without the `home` its class needs, a `home` on a cell that can have none, a `home` that names no cell of a parent class, a `home` that runs another layout than its cell, a parent cell without a child of a class its class sends to (one problem per class, at the parent's table, naming its layout), and an unknown key. A `[class.<name>]` table applies to the class's cells in every layout with the class, and is checked against each. A cell or region name is a DNS label: lowercase letters, digits, and `-`, starting with a letter, ending with a letter or digit, at most 63 characters. `Fleet::cell` gives a cell's own settings (`gdl_model::CellSettings`), with its quorum resolved from its overrides and its class's defaults, and its class's `requires`: unsaid when its class leaves it unsaid. An explicit `requires` replaces the default whole: it can raise a count or name a subset of the classes sent to, but a count of 0 and a class that the class does not send to are refused. `cargo gdl compose --fleet` deploys the fleet from it, on one host (see [A fleet's bundle](#a-fleets-bundle)).

### Check a spec and a fleet file

`cargo gdl check --spec gdl.toml --fleet fleet.toml` checks the spec as `generate` does (the graph, each layout's placement, classes, and boundary rule, and the bindings) and the fleet file against the spec's layouts, and writes nothing. It prints every problem in both files on its own line, then their count, and exits non-zero when there is any; with none, it prints `cargo gdl: no problems`. `--fleet` is optional. Each problem names its file, the table and key it is in, and the line when the parser gives one; `generate`, `resolve`, `diagram`, and `export` name the spec's problems the same way:

```text
gdl.toml:14: [segment.engine] node: unknown field `node`, expected `nodes`
gdl.toml: [cells.zz]: cell policy `[cells.zz]`: the placement has no class `zz`
fleet.toml: [class.model_x] ready.front: not a segment of class `model_x`
fleet.toml: [cell.model-x-02-us-east-1] home: missing; a cell of class `model_x` names its parent, a cell of a class that sends to it: `global`
fleet.toml: [cell.global-eu-west-1]: no child of class `model_x` in layout `default`; a cell of a class that sends to `model_x` has a child of it, a cell whose `home` is this one
cargo gdl: 5 problems
```

Every table of the spec, the fleet file, and a cell's settings refuses a key it does not know, and a table whose keys are names (a segment's in `ready`, a class's in `requires`, a node's in `[config]` and `[nodes] pin`, a message's in `[messages] pin`) refuses a name that is not one of the spec's. A file that does not parse is one problem, and the other file is still checked: a fleet file whose spec does not read is checked for what needs no layout (its cell names, regions, and that each `home` is one of its cells). Which class sends to which is a layout's. So when the spec does not read, or a cell's layout has problems, the check does not judge that cell's children. It then says so once, in a line of the fleet file's own (``fleet.toml: the cells' children are not checked: ...``), beside the spec's problems. `cargo gdl compose --fleet fleet.toml` runs the same check first, and writes nothing when it finds a problem.

### Defaults

Every default of the spec's placement and deployment tables, the fleet file, and a cell's settings is a named constant:

| Key | Default | Constant |
|---|---|---|
| spec `[routing] <node>` | `"round_robin"` | `RequestRouter::DEFAULT` |
| spec `[cells.<class>] policy` | `"round_robin"` | `CellPolicy::DEFAULT_POLICY` |
| spec `[deployment] base_image` | `"debian:bookworm-slim"` | `DEFAULT_BASE_IMAGE` |
| spec `[deployment] build_image` | `"rust:1.93-slim-bookworm"` | `DEFAULT_BUILD_IMAGE` |
| fleet `ready.<segment>`, `[cell] ready.<segment>` | `1` | `CellSettings::DEFAULT_QUORUM` |
| fleet `requires.<class>`, `[cell] requires.<class>`, for each class the cell's class sends to, when `requires` is unsaid | `1` | `CellSettings::DEFAULT_REQUIRES` |
| `[cell] directory` | `"relay"` | `DirectoryBackend::DEFAULT` |
| `[cell] summary_ms` | `10000` | `CellSettings::DEFAULT_SUMMARY` |
| `[cell] summary_ms`, its least | `1000` | `CellSettings::MIN_SUMMARY` |

The constants are `gdl_model`'s, and the two images are the generator's (`gdl-gen`). A fleet cell's quorum for a segment is its own `ready`, else its class's, else the default. A placement and a fleet file leave a default unsaid when they are written, and a spec that spells a default of its placement out has the placement, and the hash, of one that leaves it unsaid. Every other optional key has no value unless it is set, except `[deployment] hub_image`, which is the `base_image`, and `[contract] module`, which is the contract library's name.

For a graph spec, the generator compiles nothing: it builds the contract's manifest from the graph file and what the source reader found, including which messages are wire types. For a contract spec, it compiles only the contract crate to read its manifest. Then it checks the placement and the bindings against the manifest, and it reports every error that it finds. A message type that crosses between segments must implement `Serialize` and `Deserialize`. If the reader takes a type for a wire type that is not one, the segment's build or its deployment, which checks the placement against the compiled contract, fails. If it misses a wire type's serde impls (for example, impls that a macro writes), the generator refuses a cut that would work. `cargo gdl extract --verify` finds both.

For each segment, the generator writes a crate with:

- A library. `start(node, roster, config, nodes)` deploys the segment inside a program. `Binary` implements `gdl_runtime::SegmentBinary` with what differs between segments: the settings tables, how the hosted implementations are built, and what the segment serves.
- A binary. Its `main` is `gdl_runtime::segment_main::<Binary>()`: the lifecycle that every segment shares is the runtime's, and it is tested once there. It builds each hosted node through `gdl::Build`, meets the other segments in a shared roster directory, and waits until every instance that it can send to is serving; a request that reaches it earlier waits at its roster. Then it serves until SIGINT or SIGTERM, or until its hub lease is lost. A stop retires the instance: its peers stop routing to it while another instance of its segment serves, and it drains while the others serve on. A source's instance finishes the requests it holds, and every instance keeps serving until the instances that send to it have promised to avoid it (then for the runtime's setup timeout), or until each source instance that can reach it has drained or left, whichever comes first; then it holds no request, and each stream it sent has been handed on by velo, before it exits. All of this is within `[runtime] drain_ms` (the request deadline by default). Stopping every instance at once is the same path: once every instance of a segment is retired, peers route to them until the sources drain. Under a hub, an instance in a cell also stops when its cell drains (see the hub of a cell, below).
- With a pinned `Frontend`, the source binary builds and runs that service using `[frontend]` configuration. Without a source pin, it instead reads one JSON request per line on stdin, writes the items as JSON lines (`gdl_runtime::serve_json_lines`), and stops at the end of the input. A stop signal ends reading but lets the answer being written finish; a second signal ends the process at once. It does this only if the source's request type and item type both implement `Serialize` and `Deserialize`, and the request is not streamed. For other source types, the binary serves until it is stopped, and your own program calls the graph through the generated library.

A segment binary takes its settings from TOML files, each overriding the ones before it, then from `GDL_` environment variables, with `__` between nested keys: `GDL_RUNTIME__ADMISSION_LIMIT=64`, or `GDL_NODE__TRUNCATE__MAX=3` for `[node.Truncate] max = 3`. A key set in the environment is matched without regard to case (a file's keys are as spelled), and only the settings' own keys are read, so the build scripts' `GDL_PROFILE` and `GDL_IMAGE_PREFIX` do not stop a segment from starting. A misspelled key is an error. The settings name `listen` (under a hub, an unspecified address such as `0.0.0.0:9000` advertises the address the container's `HOSTNAME` resolves to, and a concrete one is bound as given), and exactly one of `roster` (a directory every instance shares, fresh for each deployment, where `[instances]` says how many of each segment to wait for) or a `[hub]` table with the discovery hub's `host` (and, optionally, its `public_port`, `control_port`, and the lease's `heartbeat_ms`), where `[min_replicas]` says how many loaded instances of each segment to wait for. In a deployment with cells, an optional `[cell]` table says which cell the instance belongs to: its `name`, `class`, and `region`, and optionally its `layout` (`GDL_CELL__LAYOUT`, the spec's layout the cell runs; it is read and its name's shape checked, and nothing else in an instance or a hub reads it, since the placement hash keeps layouts apart), its `home`, its `directory` (`relay`, the default; `memory`, one directory shared by every hub in a process, for tests; or `dir`, one file per cell under the directory that `dir` names, shared by every hub on one host; the `dir` key is required with `directory = "dir"` and refused with the others), the address at which other cells' hubs reach the cell's hub (`hub`, as `host:port`; an instance ignores it), its quorum (`[cell.ready]`, `GDL_CELL__READY__<SEGMENT>`; a segment it does not name needs 1), how many ready cells of each class its class sends to its health requires (`[cell.requires]`, `GDL_CELL__REQUIRES__<CLASS>`: one of each when unsaid, and a table that is set replaces that default whole; an instance ignores it), and the load summary's period (`summary_ms`, `GDL_CELL__SUMMARY_MS`: 10 s by default, at least 1 s). An instance refuses to start when its `[cell]` does not fit its deployment: the placement has no classes, the class is not one of the placement's, the instance's segment is not in it, a `ready` key is not a segment of it, or a `requires` key is not a class it sends to or has a count of 0. Every problem with `[cell]`, found as the settings are read or as the instance launches, names where its key was set: the file and the key (``config.toml: [cell] ready.back: not a segment of class `global` ``), or the variable (``GDL_CELL__READY__BACK: not a segment of class `global` ``). An instance in a cell waits at its launch only for the segments of its own class: `[min_replicas]` and `[instances]` may name no other, and its routing barrier waits for no instance of another class, so a global cell's instances serve with no model cell running, and a model cell's with no global instance. An instance in a cell whose segment is an entry segment of its class (it holds the inner end of a hop into the class) reports its admission to its hub once every `summary_ms`, from one period after it is ready until it retires; a report reads counters the admission keeps, so a request's path gains nothing. Under a hub, an instance in a cell whose class a hop enters watches its hub's cell watch for the cell's drain (below). Nothing else in an instance reads `[cell]` yet; a cell's hub reads the same settings (below). Every node's `Build::ready` is awaited before the segment announces itself; a node that loads a model after construction resolves it then. The binary stops on SIGINT or SIGTERM, and a frontend drains the requests it holds first; a second signal ends the drain. It takes the signals from its start, so a stop while its nodes load ends the launch rather than killing the process.

A `[system]` table turns on the system listener, which answers dynamo's probes: `GET /live` answers 200 from the start, and `GET /health` answers 200 only while the instance serves (after the barrier, before a stop or a lost lease) and 503 otherwise. Both bodies are dynamo's JSON, `{"status": "ready" | "notready", "uptime": ...}`, with gdl's `"phase": "starting" | "ready" | "draining"` added. `GET /metrics` serves the process's metrics in Prometheus's text format (see [Node metrics](#node-metrics)). It is off unless `port` is set; dynamo's variables `DYN_SYSTEM_HOST`, `DYN_SYSTEM_PORT` (negative is off, 0 an ephemeral port), `DYN_SYSTEM_LIVE_PATH`, and `DYN_SYSTEM_HEALTH_PATH` set it too, so the dynamo operator's probes find it, and `GDL_SYSTEM__*` variables override them. `<binary> --healthcheck <config.toml> ...` asks a running instance's health path, dialling loopback for an unspecified host, so an image needs no HTTP client. Under a hub the lease is kept by a heartbeat every `heartbeat_ms` (5 s by default) and lost after five misses: a segment whose lease is lost drains and stops, and the deployment starts it again. A segment name is lowercase and at most 58 characters, so that a count set in the environment names it and a hub can name its readiness services.

With several layouts, each layout's segment crates and its `crossings.txt` are written in a directory named for the layout, `<out>/<layout>/<segment>`, beside the one contract crate of a graph spec, `<out>/contract`, and each segment's package is named for its layout, `<graph>-<layout>-<segment>`, so that one workspace can hold every layout's crates. With one layout, in a `[layout]` table or not, the output is as without layouts. A layout of a graph spec with several layouts is never named `contract`. `cargo gdl export` and `cargo gdl diagram` take a spec of one layout, and refuse one of several, naming its layouts. `cargo gdl compose` deploys a spec of several layouts only by cell, from its fleet file (see [A fleet's bundle](#a-fleets-bundle)). The composer places its graph in one layout, and refuses a draft with a key it does not read, such as `layout`, rather than drop it (see `LIMITATIONS.md`).

The generator owns the directory of each segment crate. Each generated file starts with a do-not-edit marker. `cargo gdl generate --check` writes nothing. It fails if a generated file is missing, changed, or stale, or if a segment crate holds a file that the generator did not write.

`[gdl]` names the runtime that the generated crates build on, `gdl-runtime`, by `path`, by `version`, or by `git` with `rev`. Generation refuses a `path` whose package is not `gdl-runtime`, such as the definitions crate `gdl` beside it. Resolving and drawing read the path as text.

### Export the segments

`cargo gdl export --spec gdl.toml --out chat.tar.gz` writes the same crates as one archive, with a `README.md` and a `build-all.sh`:

```bash
tar xzf chat.tar.gz
sh chat/build-all.sh          # one `cargo build` per segment, into chat/target
```

Each crate in the archive is its own Cargo workspace, so each segment's build resolves only the dependencies of the nodes that it hosts. The builds share one target directory. `GDL_PROFILE` picks the profile (`release` by default), and the script passes its arguments on to `cargo build`. A crate outside the archive is named by its absolute path on the machine that wrote the archive, so the archive builds on that machine. A crate named by a version or a git revision builds anywhere. Two exports of one spec are the same bytes.

When every crate comes from crates.io or git (a graph spec whose crates use exact versions or Git revisions, with `[gdl] git`), the archive also builds images: a `Dockerfile` per segment, a multi-stage build from the archive alone, and `sh build-images.sh`, which builds one image per segment, named for its package after `GDL_IMAGE_PREFIX`. The archive does not push images anywhere.

## Docker Compose deployment

The web composer's **Deploy** tab generates and previews `compose.yaml`, the hub Dockerfile, and each segment's Dockerfile and runtime config. Settings changes regenerate the preview. Download stays disabled until generation succeeds. The downloaded archive contains the exact previewed files; download does not regenerate them.


The **Deploy** tab follows **Graph** and **Segments**. It checks the placement, then offers Docker Compose settings and a build bundle to download. Set the default runtime image, the build image, and any per-segment overrides. Published ports and runtime TOML are configured per segment. Deploy accepts only Git or crates.io node and message packages. Start the server with `--gdl-git https://github.com/ryanolson/gdl --gdl-rev <commit>` to make the runtime fetchable too. Local packages remain available for graph editing. The tab also reports what the server's host offers: Docker Compose, if `docker compose` and the Docker daemon answer, and Kubernetes, if `kubectl` has a kubeconfig and a current context (reported only). If the server was started with `--docker`, **Build and apply** generates the previewed bundle again on the server. It writes the bundle to `<target-dir>/deployments/gdl-<graph>/` and runs `docker compose -p gdl-<graph> up --build --detach` there. The page shows the output while the command runs. A second apply replaces that directory and updates the same project. One apply runs at a time. Graph and Segments retain separate zoom levels; action tooltips appear after a short hover.

The CLI produces the same build directory:

```bash
cargo run -p gdl-gen -- compose --spec examples/api/deploy.toml --out /tmp/deployment
# Copy this directory to the Docker host, then:
cd /path/to/deployment
docker compose up --build
```

The output contains a Dockerfile and runtime config for each segment, a hub Dockerfile, `deployment.json`, and `compose.yaml`. The output directory must be new. A spec with classes, or with several layouts, deploys by cell and needs its fleet file: without `--fleet`, compose refuses it (``cargo gdl: `compose` deploys a spec with classes by cell, as its fleet file says: give it `--fleet <fleet.toml>` ``); see [A fleet's bundle](#a-fleets-bundle). Cargo fetches Git and registry dependencies during the image build. The bundle contains no local dependency source snapshot. The public example in `examples/api/deploy.toml` pins both the example package and GDL to the same Git commit. That commit must carry the runtime the generator emits for (`gdl_runtime::ProcessConfig` and `gdl_runtime::Hosted`), so bump the pin whenever the generator and the runtime change together; a bundle built against an older gdl does not compile.


Image settings belong in the graph spec:

```toml
[deployment]
base_image = "debian:bookworm-slim"
build_image = "rust:1.93-slim-bookworm"
hub_image = "debian:bookworm-slim"

[deployment.segment.s1]
base_image = "ubuntu:24.04"
ports = ["8082:8082"]

[deployment.segment.s1.config.frontend]
listen = "0.0.0.0:8082"

[deployment.segment.sink]
base_image = "nvcr.io/nvidia/cuda:12.8.1-runtime-ubuntu24.04"
```

Each segment inherits `base_image` unless it sets an override. The hub also inherits it unless `hub_image` is set. The runtime image must support the executable built in `build_image` and supply its runtime libraries. The generator installs no packages into custom runtime images.

```mermaid
flowchart TD
    Hub[Hub loads graph and placement] --> Health[Hub health check passes]
    Health --> Register[All segments start and register compiled identities]
    Register --> Validate[Hub checks graph and placement hashes]
    Validate --> Roster[Segments discover their peers]
    Roster --> Ready[Install handlers and announce readiness]
    Ready --> Serve[Frontend accepts requests]
```

The hub is velo's, at the revision `Cargo.toml` pins, with admission restricted to one deployment: a segment built for another graph or placement is refused. Each segment registers as soon as its nodes are built, so the hub knows it is live while they load; once every node is ready it announces itself as loaded, waits for at least `[min_replicas]` loaded instances of each segment (one by default), and routes to those. It then installs its handlers, announces that it is serving, and waits for the instances in its roster that it sends to before the frontend accepts a request; a request that reaches a segment before its wait is over waits at its roster. How many instances a segment has is not fixed. An instance that starts while the others serve joins: once it has passed its own wait and announced that it is ready, every instance that sends to its segment adds it. An instance that is stopped retires: its peers stop routing to it while its segment has another instance, say so through the hub, and it leaves once it has drained. A roster directory has exact counts, and nothing joins it; an instance stopped there retires the same way. Kubernetes and srt-slurm generation are not implemented.

The hub of a cell takes its cell's settings from `GDL_CELL__*` variables, since the hub has no settings file: the keys of a segment's `[cell]` table, with `hub`, the address at which other cells' hubs reach it, required, and `directory = "dir"` the only backend a hub process publishes to. A variable that names no setting, or whose value does not fit its setting or the deployment, is refused before the hub starts, and every refusal names its variable (`GDL_CELL__HONE: unknown field`). For each segment of the cell's class it counts the instances usable for fresh work, those that are ready and have not retired, and compares them with the cell's quorum. The cell is `unavailable` while a segment has none, `degraded` while a segment is below its quorum and also while every quorum is met but has not yet held for 2 s, and `ready` once every quorum has held for 2 s. A drop below a quorum during those 2 s starts them again. `GET /v1/ext/gdl-cell/health` on the hub's public listener answers 200 when the cell is ready and 503 otherwise, with a JSON body: `{"state": "ready", "segments": {"d": {"usable": 2, "quorum": 2}}}`. A cell whose class sends to other classes requires cells of them: by default one of each class its class sends to, or what its settings say (`GDL_CELL__REQUIRES__<CLASS>`). It answers 200 only when, besides, for each required class, at least that many of the cells its hub follows (below), its own children, have sent their whole snapshot and last said they are `ready`. Its body adds each required class with the count required and the count met, `"requires": {"model_x": {"required": 1, "met": 0}}`, and names the classes not met, `"missing": ["model_x"]`. `missing` is absent when every class is met. A reader of an earlier build ignores it, and a body from an earlier hub has none, so a reader judges by the counts. The body's `state` stays the cell's own, which its record carries. The hub publishes the cell's record to the directory when the state changes and every 10 s, with a 30 s lease. The record names the cell's parent, its `home`, unless no class sends to the cell's class, in which case it names none. `POST /v1/ext/gdl-cell/drain` on the hub's control listener starts the cell's drain: from then the cell is `draining` whatever its quorum, on its health endpoint (503), its record, and its watch, by the time the drain is answered (`202`, with the health body): the answer waits for the directory to store the draining record, for up to 5 s. A second drain is answered the same way, and a drain cannot be cancelled. A cell whose class no hop enters, such as a global cell, is refused with `409`. Once no instance of the cell's class is a member of the hub, the hub withdraws the cell's record, after a last write that says it is draining, publishes it no more, and ends the cell's watches, so that the hubs that follow the cell drop it. From then it refuses every registration with 409, so an instance that would join the drained cell fails its launch instead of serving a cell nothing publishes; to serve the cell again, its hub is started again. Each instance of the cell learns of the drain from its hub's cell watch; an event there of a kind it does not know makes it watch again, so that the next watch's first event says what the cell's state is. An entry instance keeps taking new work, ready and reporting its load, for a bound after it learns of the drain, the runtime's `setup_timeout_ms` and twice `summary_ms`, so that an offer from a chooser whose view of the cell is stale still lands, or until its hub says that no other cell follows the cell, whichever comes first: a global cell's hub lets go of a cell only once its choosers have acknowledged that they hold no offer to it (below), and the cell's hub counts its followers by the watches that name them, until each is released. Then it declines the offers that reach it from other cells, which a chooser takes as a decline and offers to another cell, finishes the requests it holds, and announces that it has drained. It still takes the hand-offs inside the cell, which carry the requests the cell's entry instances took, until every entry instance has drained or left; then it closes and leaves. It does not retire: the cell's state already keeps choosers from it. An interior instance serves until every entry instance of the cell has drained or left, then drains as after a stop. While its cell drains, the entry instances are an interior instance's drain sources, whatever stopped it, since the sources outside the cell are not members of its hub. Each usable entry instance of the cell reports its admission to the hub once a load summary period (`summary_ms`, `GDL_CELL__SUMMARY_MS`: 10 s by default, at least 1 s; the hub and its instances read the same setting): `POST /v1/ext/gdl-cell/instances/{id}/load` on the control listener, with the instance's own credential, carrying the roots it holds admitted, its admission limit, and the hand-offs it declined since its last report the hub took. A report from any other instance (of another segment, not ready, retired, or gone) is refused with 409. Once a period the hub sums the latest report of each usable entry instance, dropping one older than three periods, into the cell's load summary: `admitted` and `limit` summed, `instances` the usable entry instances, `refusals` the declines summed, and `period_ms` the summary period, so that a reader can tell when the summary is stale. There is no summary while no report counts, and none for a class that no hop enters. The record carries the summary as it is when it is written. It also serves the cell's watch, `GET /v1/ext/gdl-cell/watch`, for the hubs that will follow the cell and for the cell's own instances, which learn of its drain from it: server-sent events, each a JSON value tagged by `event`. A watch begins with the cell (`cell`: its name, class, epoch, state, and load summary) and its entry instances, the instances of the segments that a hop from another class enters: first those that retired (`retired`), then those that are ready (`ready`, with their velo `PeerInfo` as `/v1/ext/gdl/members` shows it), then `synced`. After it come each change of state (`state`), the load summary once a period while there is one, changed or not, so that its age says how fresh it is, and once when there is none (`load`), each entry instance that is ready, retires, or leaves the hub (`left`), and, once, when the cell drains and no other cell follows it any more, its `state` again with `unfollowed: true`, which every later snapshot carries after the cell: a field of a known event, which a reader that does not know it ignores, rather than a kind of its own, which would make an earlier global's hub fail closed for the draining cell and read it again for as long as it drains. A hub that follows the cell names the watch in the query, its own cell, the run of that hub (a number it draws each time it starts), and the follow's generation (`?class=<class>&cell=<cell>&run=<run>&generation=<generation>`), and the watch counts among the cell's followers until the follower releases it (`POST /v1/ext/gdl-cell/release` on the public listener, with the same four), which a global's hub does once it has closed that follow; a watch that closes without a release still counts, and once the cell's lease has passed since it closed it no longer shows as a follower, but it is never taken as a release, so the drain then waits out its bound. A watch that names none counts for nothing. A watcher that falls 1024 events behind has its watch ended, and watches again. A watch that arrives before the hub has first published the cell's record waits for it, and is refused with 503 after 5 s; every watch ends when the hub stops, or once a drained cell's record is withdrawn, and a watch after that is refused. `gdl_watch::CellWatcher` reads it. The hub of a cell whose class sends to another class follows its children in that class: for each class it sends to, it watches the directory, and follows every cell of the class whose record names the hub's own cell as its parent (`home`), of its deployment (the same contract and placement hashes; a cell with others is ignored), and is `ready` or `degraded`, over that cell's watch, one connection per cell. Region plays no part: a child in another region is followed, and a cell of the hub's region whose parent is another cell, or that names none, is not. It relays what it follows on `GET /v1/ext/gdl-cell/follows`, server-sent events tagged by `event`: for each followed cell, `follow` (its class, name, generation, region, state, and load), then its retired entry instances (`retired`), then the ready ones it exposes (`exposed`), then `synced`; after it, each change (`state`, `load`, `offering`, `exposed`, `retired`, `left`, and `dropped` when a follow closes). Every event but `synced` names its cell, by its class and its name within the class as the directory names it, and the follow's generation, which the hub numbers each time it follows the cell. `GET /v1/ext/gdl-cell/follows/{class}/{cell}` watches one follow alone: the cell's follows as a watch of the relay begins with them, its closing generations first and then its current one, each with its `offering` while it is not offered, then `synced` (only `synced` when the hub has no follow of the cell installed), then each event of the relay that names the cell, of whichever generation; its snapshot and the events after it are ordered as the relay's are. A reader ignores a field of a known event that it does not know, so a later build adds to an event as an optional field. An event of a kind a reader does not know makes it fail closed for the cell the event concerns, and only that cell, until it has read the cell again: on a cell's watch the watched cell, on the relay the follow the event names; a relay event of an unknown kind that names no follow ends the watch, as an event that is not JSON, or a known event whose fields do not parse, ends either watch. So a new kind of event costs each earlier reader its cell until it has read it again, and needs no lockstep. A hub whose follow of a cell meets one withholds the cell: the relay shows it `unavailable` at once, which hides its instances and keeps it out of what a requirement counts, and so not offered, at the follow's next revision (`offering`), though the follow does not close while it is withheld, however its choosers acknowledge; and the hub reads the cell again from a new watch of its hub, which replaces the one that sent the event: nothing that watch sends after it is applied, and the new watch's snapshot and the events after it are ordered by the cell's hub. The follow and its generation go on. A new watch that does not begin, ends before its snapshot is complete, or finds another cell or epoch ends the follow, as a watch that ends does; one whose snapshot has an event of a kind the hub does not know leaves the cell withheld, and the cell is read again 250 ms later (the refollow wait). `gdl_hub::CellReadiness::unknown_events` and `gdl_watch::CellWatcher::unknown_events` count such events. A cell's instances are exposed only while the state it last sent is `ready` or `degraded`: a state that exposes nothing hides them all, and one that exposes them again sends each. A follow is offered to the hub's choosers while its link lives and the state it last sent is `ready` or `degraded`, and each change between offered and not offered is the follow's next revision: `offering`, with the revision and whether it is offered. A follow is offered at revision 1 as it is installed, and its snapshot shows `offering` only while it is not offered, so that a reader that does not know `offering` keeps from the cell while it is not offered and takes it again once it is. A follow that is not offered closes once every chooser of its class among the hub's members has acknowledged its current revision, or left. A member counts as a chooser once it is an instance of a segment that holds the outer end of a hop into the class and has said that it runs a follower of the relay (`POST /v1/ext/gdl-cell/instances/{id}/chooser` on the control listener, with its own credential), which a chooser does before it first watches the relay; an instance without `[cell]` runs none, never offers to another cell, and is not waited for. It acknowledges with `POST /v1/ext/gdl-cell/instances/{id}/unoffered` on the control listener, with the chooser's own credential, naming the follow's class, cell, generation, and revision (`gdl_watch::Unoffered`). One that names another revision counts for nothing, and one from an instance that is not a chooser is refused with 409. Then the hub drops the follow from the relay (`dropped`) and ends its link. A follow whose link ends is not offered from then on, and is kept, with its instances, beside the cell's later follows until it closes the same way; the cell is followed again 250 ms after its link ended, as a new generation, if its record still makes it a child that takes work. A follow the hub closed is followed again only from a later write of the cell's record that makes it one. A record that expires or is withdrawn ends no follow and changes nothing about whether it is offered, and a newer epoch in the directory ends the follow of the hub it replaces, which then closes as one whose link ended. A chooser reads the relay: an instance in a cell, under a hub, whose segment holds the outer end of a hop into another class. It opens one watch of its own hub's relay for its life, and watches again 100 ms after one ends, from a fresh snapshot, keeping its tables meanwhile. An event of a kind it does not know on the relay leaves the cell it names unread (`FollowedCell::unread`): it is not offered, as one its hub marks not offered is (`FollowedCell::offered`), so its tables offer none of the cell's instances and its follow refuses leases, though a lease taken before is kept until its response ends, until the chooser has read the cell from a watch of its own, `GET /v1/ext/gdl-cell/follows/{class}/{cell}`, which it opens at once. From then that watch carries the cell, and what the relay's watch sends of it is not applied, since the two are not ordered with each other; the relay's watch, and every other cell, go on. The cell's own snapshot replaces what the chooser holds of it, its closing generations included, and a follow shown again in its generation keeps its leases and its acknowledgement. Being unread is acknowledged to nothing: the chooser acknowledges only the hub's not-offered revisions, since being unread is its own ignorance of the cell, which has no revision. A new event of a kind the chooser does not know on the cell's own watch leaves it unread again, and a new own watch replaces it, at once, or 100 ms later if the event was in its snapshot, which the chooser then shows unread; an own watch that ends or cannot begin leaves the cell unread, and another is opened 100 ms later. The chooser holds one more connection to its hub for each cell it reads so, until its next watch of the relay, whose snapshot carries every cell again. `Cells::unknown_events` counts such events. For each entry segment its hops enter and each followed cell it keeps a table of the cell's current generation: its state, its load summary with the time it arrived on the chooser's clock (`FollowedCell::load` and `load_age`; the summary is sent every period, so its age is the time since its hub last made it), its exposed entry instances, and those that retired, which are never routed to again; a newer generation replaces the cell's table at once, and the older one keeps its instances until the hub drops it, and only its `offering` and `dropped` change anything. A follow the hub marks not offered shows no eligible instance (`FollowedCell::offered` and `revision`) and is never chosen. Each offer to a cell takes a lease on the cell's follow, one count, held until the response stream ends or is dropped, as a selector's lease is (`FollowedCell::leases`); a choice that finds the follow no longer offered once it is made, a selector's included, is made again. Once the chooser has applied a not-offered revision and holds no lease on the follow, it acknowledges that revision to its own hub. It looks for such a follow every 50 ms while it has one, and sends an acknowledgement its hub did not take again 500 ms later. Each entry instance is registered with the chooser's velo node before a table holds it, and unregistered `drain_ms` after no follow holds it (a follow holds the instances it exposed, those that a state that takes no work hid included, until they retire and leave, or the hub drops the follow), unless the instance's own membership routes to it or has yet to finish its own drain of it; the membership, in turn, does not unregister a peer that a table holds, so one velo instance in both is unregistered by whichever lets go of it last. `Launch::cells` reads the tables (`gdl_runtime::Cells`). A chooser routes by them: each hand-off of a hop into another class goes to a cell chosen for that attempt by the class's cell policy, then to an entry instance of that cell chosen by the hop's own router (`[routing]`) among that cell's eligible instances alone, with a round robin's turn kept per entry segment and cell. `round_robin`, the default, gives each attempt the next cell in the chooser's table, by class and cell, of those that are `ready`, or `degraded` while none is, that have an entry instance to route to, and that have not declined the request; it reads no load summary, so a cell is chosen from the moment it is followed, and a cell's region plays no part. `home_first`, which a placement names, chooses by each cell's load summary, and never a cell that is `unavailable` or `draining`, has no entry instance to route to, or has no summary younger than three of its periods. Of the rest it takes the first non-empty tier: `ready` home cells (of the chooser's own `[cell] region`) that are not saturated (a cell is saturated when its admitted roots reach its admission limit or its last period had refusals); `ready` cells elsewhere whose admitted roots are under 80% of their limit; saturated `ready` home cells; then the `degraded` cells in the same three tiers. Within the tier it draws two cells at random and chooses the one with the lower ratio of admitted roots to limit. A cell that declined a request is not offered it again, and the chooser damps it: it is not chosen for 1 s, then for twice as long each time it declines again once its window has passed, up to 30 s, until it accepts an offer; at most half of the cells otherwise eligible, rounded down, are damped at once, those whose windows end soonest being chosen as if they were not. The thresholds and the windows are constants of the policy. Admission counts roots, not engine work, so this is an admission heuristic. A selector is offered the chosen cell's entry instances only; if the instance it chose leaves the cell's table while it waits, or the cell stops taking work, or, under `home_first`, its summary goes or goes stale meanwhile, the cell and the instance are chosen again. With no cell to choose, the request fails at once with `Routing`, naming the class: it never waits for a cell to appear. The hand-off is an offer the cell may decline. When its entry answers, before taking the request, that it has no room (`Overloaded`: admission or egress refused, or the instance retired) or that the hand-off is not of its deployment (`Placement`), or when velo refuses the send before it leaves the chooser (the instance is not registered with the chooser's velo node, or its handshake fails), the chooser offers the request to another cell in the same call: one chosen by the class's cell policy among the cells not yet offered it, at most `[runtime] cell_failovers` times (2 by default; 0 offers each request to one cell). Nothing of a declined offer needs sending again: the entry took nothing, and a streamed request's input is sent only to the cell that accepts it. A selector's lease and meta from a declined choice are let go before the next choice. When every cell declines, the request fails with the last refusal, so a full fleet answers `Overloaded`. A hand-off whose answer does not come within the setup timeout, or whose send fails after velo took it, may have run in the cell: the request fails at once with `OutcomeUnknown`, and no other cell is offered it; a retry is the operator's. A refusal is not remembered past its request (see `LIMITATIONS.md`). An instance without `[cell]`, and every instance of a placement without classes, opens no such watch. A hub without `GDL_CELL__` variables runs as before.

### A fleet's bundle

`cargo gdl compose --spec gdl.toml --fleet fleet.toml --out <directory>` writes one bundle that deploys the whole fleet on one host. It first checks the spec and the fleet file together as `cargo gdl check` does, and writes nothing if either has a problem. A fleet without cells deploys nothing and is refused, and a fleet for a spec without classes can have no cell, so that spec composes as it does without `--fleet`.

```bash
cargo run -p gdl-gen -- compose --spec gdl.toml --fleet fleet.toml --out /tmp/fleet
cd /tmp/fleet
docker compose up --build
docker compose down -v        # also removes the cell directory's volume
```

For every cell, the bundle has one hub, the service named for the cell, and a service for each segment of the cell's class in the cell's layout, named `<cell>_<segment>`:

```text
global-us-east-1                hub of cell global-us-east-1, image api-hub (built here)
global-us-east-1_front          segment front, image api-front (built here)
model-x-01-us-east-1            hub, image api-hub
model-x-01-us-east-1_engine     segment engine, image api-engine (built here)
model-x-02-us-east-1            hub, image api-hub
model-x-02-us-east-1_engine     segment engine, image api-engine
```

Each service is configured as its cell through the environment, so that cells of one layout and class share one image: the hub and the segments of a cell take the `GDL_CELL__*` variables that `Fleet::cell` gives the cell, with `GDL_CELL__DIRECTORY=dir`, `GDL_CELL__DIR`, the one directory every hub shares, and `GDL_CELL__HUB`, where other cells' hubs reach the cell's hub on the bundle's network (`<cell>:1337`). A segment also takes its cell's hub as `GDL_HUB__HOST`. Each value is written so that the reader takes it back as it is: a string that would read as another value, such as a cell named `true`, is quoted (`"true"`), and a table with a key that a variable's name cannot spell, such as a quorum of a segment named `engine__work`, whose `__` the name would split, is one variable holding the table (`GDL_CELL__READY={"engine__work"=2}`). An excerpt of `compose.yaml`, the second model cell's hub and its segment, which runs the image the first model cell's segment builds:

```json
"model-x-02-us-east-1": {
  "container_name": "api-model-x-02-us-east-1",
  "environment": {
    "GDL_CELL__CLASS": "model_x",
    "GDL_CELL__DIR": "/var/lib/gdl/cells",
    "GDL_CELL__DIRECTORY": "dir",
    "GDL_CELL__HOME": "global-us-east-1",
    "GDL_CELL__HUB": "model-x-02-us-east-1:1337",
    "GDL_CELL__NAME": "model-x-02-us-east-1",
    "GDL_CELL__READY__ENGINE": "2",
    "GDL_CELL__REGION": "us-east-1",
    "GDL_CELL__SUMMARY_MS": "10000"
  },
  "healthcheck": {
    "interval": "2s",
    "retries": 30,
    "test": [
      "CMD",
      "/usr/local/bin/segment",
      "--healthcheck"
    ],
    "timeout": "2s"
  },
  "image": "api-hub",
  "volumes": [
    "cells:/var/lib/gdl/cells"
  ]
},
"model-x-02-us-east-1_engine": {
  "depends_on": {
    "model-x-02-us-east-1": {
      "condition": "service_healthy"
    }
  },
  "environment": {
    "GDL_CELL__CLASS": "model_x",
    "GDL_CELL__DIR": "/var/lib/gdl/cells",
    "GDL_CELL__DIRECTORY": "dir",
    "GDL_CELL__HOME": "global-us-east-1",
    "GDL_CELL__HUB": "model-x-02-us-east-1:1337",
    "GDL_CELL__NAME": "model-x-02-us-east-1",
    "GDL_CELL__READY__ENGINE": "2",
    "GDL_CELL__REGION": "us-east-1",
    "GDL_CELL__SUMMARY_MS": "10000",
    "GDL_HUB__HOST": "model-x-02-us-east-1"
  },
  "healthcheck": {
    "interval": "5s",
    "retries": 3,
    "start_period": "1800s",
    "test": [
      "CMD",
      "/usr/local/bin/segment",
      "--healthcheck",
      "/etc/gdl/config"
    ],
    "timeout": "3s"
  },
  "image": "api-engine",
  "init": true,
  "ports": [],
  "stop_grace_period": "630s"
}
```

- **One hub per cell.** A cell's record has one writer, its hub, so a cell's hub must never run as two at once. A hub's service names its container (`<graph>-<cell>`), and Compose refuses to scale a service whose container is named. Container names are global on a Docker host, so a host runs one copy of a bundle at a time. Compose's default recreate stops the old container before it starts the new one, so a `docker compose up` after a change replaces a hub without overlap.
- **Waits are on liveness.** A segment depends on its own cell's hub being healthy, and on nothing else. A hub's health check is its own liveness (`--healthcheck`), never the cell health endpoint: a parent cell's health answers 503 until its children are ready, so a wait on it would deadlock a fleet that starts together. No service waits for another cell.
- **One directory, one host.** Every hub publishes to the `dir` cell directory, the only one a hub process publishes to, on the volume `cells`, which every hub of the bundle mounts. So the bundle is for one host; cells on several machines wait for the `relay` backend.
- **Images per layout and segment.** Each image is built once, by the first service in cell order that runs it, and the other cells of its layout and class run it by its name. A segment's image is named for its package, as `generate` names it: `<graph>-<segment>` for a spec of one layout, and `<graph>-<layout>-<segment>` for one of several, whose crates and Dockerfiles are in `<layout>/<segment>/`. A hub's image is `<graph>-hub`, tagged with its layout for a spec of several (`api-hub:split`), since each layout's hub admits its own placement: its `deployment.json` is `<layout>/deployment.json`, beside `<layout>/hub/Dockerfile`. A spec of one layout keeps the names of a bundle without cells: `deployment.json`, `hub/Dockerfile`, and `<segment>/Dockerfile`.
- **Ports.** A segment that publishes a host port it names (`[deployment.segment.<name>] ports`, as `HOST:CONTAINER` or `IP:HOST:CONTAINER`) may run in one cell only, since two containers on one host cannot bind one host port; a fleet that runs it in several is refused. A port whose host port Docker chooses (`CONTAINER`, or `IP::CONTAINER`) binds a free one for each container, and runs in any number of cells. An image's settings are every cell's: `[deployment.segment.<name>] config` cannot set `[cell]`, which the fleet sets.

The composer's **Deploy** tab writes a bundle without cells only, and refuses a spec with classes or several layouts. Kubernetes, networks that span hosts, and TLS are not generated (see `LIMITATIONS.md`).

## Compose in a browser

An empty catalog offers **Load examples** to import the public API frontend package from Git. With the public GDL repository configured, the import uses the server's runtime revision. **Clear catalog** asks for confirmation, archives the catalog entries on the server, and clears the current graph. It does not delete package sources.


`gdl-web` serves the composer: a page that builds a graph from the nodes and messages of a working catalogue.

```bash
cargo run -p gdl-web -- --catalog catalogue --gdl-path crates/gdl-runtime
# gdl-web: serving http://127.0.0.1:8080/
```

The package, Git-branch, and folder icons open the crates.io, Git, and local dialogs. Hover over an icon to see its label. **Add from crates.io** searches after a 350 ms typing pause. Select a result, then use its current version or enter another exact version. **Add from Git** takes an HTTP(S) repository URL, a branch or commit, and an optional package path within the repository. The import pins the resolved commit. Generated dependencies retain that Git URL and commit.

The server answers to `localhost`, IP addresses, and each name given with `--host <name>`, and refuses every other `Host`: a DNS name an attacker points at your machine would otherwise make their page same-origin with the composer in your browser. It also refuses a state-changing request whose `Origin` is not its own.

Local imports are disabled by default, in both the page and API. To enable the directory browser, launch with `--local-root <directory>`, for example `--local-root "$PWD"`. **Add from local** then browses only that directory and its descendants. Choose a package directory and press **Add package**. The server reads source without compiling it, in a child process whose memory and time are limited: a crate the reader cannot handle is refused, and the server goes on.

Building and starting a deployment on the server's host is disabled by default. Launch with `--docker` to enable **Build and apply** in the Deploy tab. The tab reports what the host offers either way.

Expand a package in **Catalog** to inspect its messages and nodes. Messages show their Rust paths, visibility, and network codec availability. Nodes show their roles and ports. Search filters package names, message and node names, and Rust paths. It does not search serialized metadata.

Use the pencil beside **Graph Name** to edit the graph name. Select **Save**, press Enter, or leave the field to save. Press Escape to cancel.

In the source box, choose a catalog frontend or drop a source node onto the card. Its `Req` and `Item` ports set the graph contract. For a programmatic source, choose **Single** or **Stream** input and select its message. Stream input can also have an optional header message. Output is **Stream**; Single output is not available. You can select messages by name or drag them from the catalog onto the input and output cards.

After you select both messages, connect nodes through the offered slots. Each slot accepts a matching node from its selector or from the catalog. Requests flow down to each hop's inner end. Responses flow back through the backward nodes. Operators and sinks span the request and response columns. An operator contains another two-column hop beneath it.

The server checks complete flows automatically. **Segments** becomes available after the graph passes. Switch to that tab to assign nodes to segments. Choose **New segment…** beside a node, or select an existing segment. Each move updates its request and response boundaries together. Clicking an edge opens a placement dialog for a connected node. Edges with a scissors icon open the boundary dialog. Edges without a network codec stay local; hover over them for the message type. Node labels and colors show segment membership. Endpoint adapters are ordinary graph nodes and can be assigned to separate segments. The summary counts segments and actual network crossings. Expand **Node placement** to move nodes, or **Rename segments** to change names. **Undo** and **Redo** restore graph edits and placement changes during the current session. Use Ctrl/Command+Z to undo and Ctrl/Command+Shift+Z to redo outside text fields. Return to **Graph** to change nodes or source messages. **Validation & generated files** shows a rendered Mermaid diagram, problems, crossings, the graph file, and its spec. **Download segments** exports the current validated graph. In Graph mode, the export contains one segment. In Segments mode, it uses the selected placement.

Request routers select a replica when a request crosses a segment boundary. In **Segments**, click the **Round robin** control on a cut request connection to select **Round robin**, **Random**, or one of gdl's selectors (`affinity`, `least_loaded`), which the server lists with the catalogue. A selector from another crate is written in the spec by hand. A request stream stays with the selected replica. Responses return through their response/stream anchors; the request router does not select their destination. Undo/redo includes router changes.

The deployment specification stores policies by the node where the remote request run begins:

```toml
[routing]
Engine = "random"
```

Unspecified boundaries use `round_robin`. Each dispatching process has its own selection sequence. Policies apply to the replicas in the runtime roster. A router target must start a remote request run; local nodes and response-only boundaries cannot have a request router.

A boundary can also name a selector: `Engine = { selector = "least_loaded" }`. A selector is a type that implements `gdl::Selector`. It chooses one of the destination segment's eligible replicas for each request, from the same routing table that the policies use, so it never sees a replica that its sender has stopped routing to. It sees the request's context and its head: the head's own type, which the segment checks against the boundary's edge when it starts, or `gdl::Encoded` for the head's bytes. Its choice can carry a lease, which the sending segment holds while it holds the request, and metadata, which the receiving segment sees in `cx.meta()`. A selector can wait, until the request's deadline. A selector that cannot choose returns `Error::routing`. Two selectors come with gdl, and need no registration:

- `affinity`: requests with the same key go to the same replica, from every sender. The key is the value of the metadata entry `[selector.affinity] key` names, or else the request's head.
- `least_loaded`: each request goes to the replica where this sender holds the fewest leases.

A selector is built as a node is, through `gdl::Build`: its settings are the segment's `[selector.<name>]` table, its metrics carry its name as `dynamo_endpoint`, and the instance announces that it is loaded only after the selector is ready. A segment binary builds gdl's own selectors from their tables, `[selector.affinity] key = "session"` for example; a table for a selector that the binary does not register is refused. A hand-written `SegmentBinary` registers another selector with `builder.selector::<S>("<name>")` in its `build`, and an in-process deployment adds it to `RuntimeConfig::selectors`. A selector name is lowercase letters and digits in words joined by single `_`s, and is part of the placement's identity. A selector costs a load of the routing table, a decode of the head when it reads a type, and its own work; `round_robin` and `random` do not go through it.


Generated crates take the gdl runtime from `--gdl-path`, or from `--gdl-git <url> --gdl-rev <rev>`. The editor uses the dashboard's dark surfaces and green accents. On narrow screens, the catalog appears above the canvas.

The binary embeds Mermaid 12.2.0 for local diagram rendering, without a CDN. `crates/gdl-web/assets/vendor/mermaid.js` is `dist/mermaid.min.js` from the npm package `mermaid@12.2.0`. Its license is in the same directory.

Run the editor behavior tests with Node.js:

```bash
timeout 60 node --test crates/gdl-web/tests/editor.cjs
```

## Hosted source nodes

`gdl::Frontend` is the implementation contract for a source that hosts an HTTP, gRPC, or other service. It extends `Build` and declares `Req` and `Item`. Its `serve` method takes a typed `Next<Req, Item>` graph client and a shutdown future. `Build` prepares the listener. The generated binary starts serving after every segment is ready, and signals shutdown on SIGINT or SIGTERM, or when its hub lease is lost.

Mark the implementation with `#[derive(gdl::Node)]` to expose a `source` node in the catalog. Pin the graph source to that implementation under `[nodes] pin`. The generated source binary reads its configuration from `[frontend]`. Other segments do not build or run the frontend. Pipeline `Bindings` still contain only the nodes behind the source.

Graph responses remain streams. A frontend can stream them over its protocol or collect them into one external response. This does not introduce a Single-output graph contract. Each external request needs a fresh `Cx`, and the frontend must release its graph stream on disconnect.

[The Axum example](examples/http/README.md) provides a working source, a local graph, and a graph spec for generated deployment. Dynamo packages can implement the same frontend contract for their existing services.

## Configurable service entries

A frontend with several public contracts implements `gdl::Service`. Its client bundle derives `gdl::Endpoints`: one optional typed client per endpoint, annotated with its path and enabling configuration field. Each enabled entry builds an arm of forward nodes, operators, and backward nodes. Each arm explicitly selects a named pipeline through its leaf `target`. Matching contracts permit a connection; they do not create one. Arms can select separate pipelines or the same pipeline. Disabled entries have no client.

Use `#[derive(gdl::Config)]` on an implementation's deserializable configuration type. The derive is the only way to implement `gdl::ConfigSchema`: the source reader takes the fields from its attributes, so a hand-written impl does not build. Mark builder-visible fields with `#[gdl(graph, default = ...)]`, and add `#[gdl(config = Settings)]` to the node. Unmarked fields stay runtime-only. Graph controls support `bool`, `i64`, and `String` with literal defaults. Optional `label` and `subtext` annotations name each setting in the UI. Doc comments supply expandable help in settings dialogs. Generated graph specs retain resolved values under `[config.NodeName]`.

`gdl_runtime::build_node` extracts `Build::Config` from a Figment, checks its graph-affecting values against the built graph, and passes the node's metrics to `Build::build`. Application code can use any Figment provider and explicit `merge`/`join` order. Generated binaries merge their TOML arguments in order, then apply `GDL_` environment overrides with `__` between nested keys. A conflicting graph value prevents startup.

The [three-entry Axum example](examples/api/README.md) includes typed adapters, endpoint flags, and a shared pipeline placed across two segments. Entry adapters declared in service specs execute locally. The composer emits its adapters as ordinary graph nodes, which can span segments. Each named pipeline can span segments. Several pipelines can reference one sink node; the deployment builds that sink once per hosting segment instance. Composer endpoint nodes support independent placement. See the [design and implementation record](agent-docs/frontends.md).

### Independent pipelines and shared sinks

Each pipeline has an input/output contract and one `topology` declaration. Repeating a sink reference shares that node; declaring two sink names creates two instances.

```text
name = "processing", schema = 1;
source ChatProcessing: Prompt -> stream Chunk;
source ResponsesProcessing: Prompt -> stream Chunk;
sink Engine: Prompt -> stream Chunk;
topology: ChatProcessing => Engine;
topology: ResponsesProcessing => Engine;
```

A hosted graph with multiple roots requires a `Service` frontend; a `Frontend` serves one root. Crossing reports for multiple roots total one request through each root.

For multiple roots, `local` and `assemble` return a nested tuple in topology order: `(chat, (responses, ()))`. A running `Arc<Segment<_>>` exposes typed clients through `segment.client(|roots| roots.0.clone())` and `segment.client(|roots| roots.1.0.clone())`. Each client keeps the segment's handlers alive. Responses follow their own call anchors.

In a service spec, each leaf entry arm requires `target = "ChatProcessing"` or another declared source root. An operator instead owns a `nested` arm. All service pipeline entry roots stay beside the frontend; their processing nodes have independent segment placement. Optional `[pipeline_names]` labels map root identifiers to display names such as `processing-pipeline-1`.

In the composer, a dangling **+** offers **Add node** and **Connect existing**. Sink choices must match both the request and response types. Connecting an existing sink shares one node instance across arms; the canvas displays that sink once. The composer creates the required internal pipeline contracts without a separate naming step. In Segments mode, the catalog shows router policies instead of nodes and messages.

The composer emits one public request/response root per enabled endpoint and includes its adapters in the graph topology. The frontend hosts those roots; ExtractChat, FormatChat, ResponsesAdapter, and other processing nodes have independent placement. For example, ExtractChat can run in `extract`, ResponsesAdapter in `responses`, and Echo in `sink`, while FormatChat remains with the frontend in `s1`. Echo sends chat responses directly to FormatChat, bypassing ExtractChat.

In Segments, select a scissors connection and choose **Only this node** or **Downstream chain**. The latter stops at existing boundaries. Non-serializable crossings are rejected. Each segment has one process box containing all assigned nodes. Nodes retain endpoint lanes and flow order within each box. Blue arrows show requests; purple dashed arrows show responses. Arrows use orthogonal routes around nodes and segment headings, with separate attachment points for shared destinations. Routing reserves 40 canvas units at each port and requires 24 units between parallel tracks. These distances scale with zoom. A route that cannot satisfy clearance is marked instead of drawn through another node. Hover over an arrow to highlight its route. Select an arrow to edit its boundary. Expand a node’s connections to inspect message types and routers, or use **Move** to change its assignment.

When inserting a conversion chain, incomplete type matches remain editable. A chain such as `X → Y → Z → X` preserves its downstream node while intermediate conversions are added. Validation prevents deployment until the types align.

### Built-in identity nodes

Choose **Identity** at a `+`, then choose a single-message node or request/response operator and its message types. Identity nodes pass values through unchanged and need no catalog package or implementation binding. Request identities do not receive responses. Response identities do not receive requests. Identity operators retain both paths.

```text
name = "four", schema = 1;
source Chat: Prompt -> stream Chunk;
source Responses: Prompt -> stream Chunk;
identity ProcessingOne: Prompt -> stream Chunk;
identity ProcessingTwo: request Prompt;
sink Echo: Prompt -> stream Chunk;
topology: Chat => ProcessingOne { Echo };
topology: Responses => ProcessingTwo => Echo;
```

Place `Chat` and `Responses` in `s1`, `ProcessingOne` in `processing-1`, `ProcessingTwo` in `processing-2`, and `Echo` in `sink`. The generator emits four segment crates. The first response path is `Echo → ProcessingOne → s1`. The second is `Echo → s1`.

A response-only identity is declared as `identity ResponsePass: response Chunk;` and placed after the inner node in the topology. A request identity can also carry `stream Chunk` or `Header + stream Chunk`. The runtime supplies `IdentityNode<T>` and `IdentityOperator<Req, Item>`; the graph macro constructs these nodes automatically.

A scissors cut moves the selected node and its downstream nodes to the destination segment. Traversal stops at existing segment boundaries and does not move an upstream hop owner. Shared sinks keep one placement. Undo and redo restore the whole cut.

## Node metrics

A node's metrics are declared as its configuration is, and handed to it at construction. `#[derive(gdl::Metrics)]` turns a struct of prometheus metric fields into the node's metrics: the field's name is the metric's name, its doc comment the help, `#[gdl(buckets = [...])]` a histogram's buckets, and `#[gdl(labels = [...])]` a vector's label names. Name the types through `gdl::prometheus`, the version the runtime registers and serves.

```rust
#[derive(gdl::Metrics)]
pub struct EngineMetrics {
    /// Tokens generated.
    tokens_total: gdl::prometheus::IntCounter,
    /// Time to the first token.
    #[gdl(buckets = [0.005, 0.05, 0.5, 5.0])]
    time_to_first_token_seconds: gdl::prometheus::Histogram,
}

#[derive(gdl::Node)]
#[gdl(metrics = EngineMetrics)]
pub struct Engine { metrics: EngineMetrics }

impl gdl::Build for Engine {
    type Config = Settings;
    type Metrics = EngineMetrics;
    async fn build(config: Settings, metrics: EngineMetrics) -> gdl::Result<Self> {
        Ok(Self { metrics })
    }
}
```

A node without metrics writes `type Metrics = ();` and takes `(): ()`. The derive follows dynamo's naming rules: a counter's name ends in `_total` and no other does, a duration or latency is in `_seconds`, and the unit is not `counter`, `gauge`, `size`, or `time`, nor `sum`, `count`, or `bucket`, which a histogram's own series use. It refuses the names the runtime records (below), dynamo's `request_duration_seconds`, which the runtime reserves, and the labels the runtime adds. The derive is the only way to implement `gdl::Metrics`, so the source reader, which reads the definitions from the derive's attributes, sees what the build registers; `#[gdl(metrics = T)]` on `derive(gdl::Node)` names the type for the catalogue, and must be the node's `Build::Metrics`.

A segment binary keeps one registry, serves it on the system listener's `GET /metrics`, and registers each node's metrics there before `build`, with the node's name as `dynamo_endpoint`. Every series also carries dynamo's identity labels: `dynamo_namespace` (the graph's name and the first eight hex digits of the placement hash), `dynamo_component` (the segment), and `worker_id` (the instance's velo id), under the prefix `[system] metrics_prefix`, `dynamo_component` by default, so dynamo's scrape configuration and dashboards take them as they are. The registry holds `uptime_seconds` as well. Two nodes of one segment may define a metric of the same name only if they agree on its kind, help, labels, and buckets.

The runtime records its own figures for each segment a request passes through, not for each node. Per node, the durations nest: a node's request lasts until its response ends, so the front's duration includes the engine's. Per segment, the durations add up. Each figure is labelled with `dynamo_endpoint`, the node where the request entered the segment: the source, the node a hand-off from another segment arrives at, or the first node of a backward run.

| Part | Starts | Ends | Figures |
|---|---|---|---|
| Request path | the source's call into the graph, or a hand-off this segment admits | the request is sent on to the next segment (and accepted), or a sink here starts its stream | `requests_total`, `errors_total{error_type}`, `cancellation_total`, `request_path_duration_seconds` |
| Response path | each stream the segment sends on: the response to the source's caller, a sink's stream, a backward run | its end | `responses_total`, `response_errors_total{error_type}`, `response_cancellation_total`, `response_time_to_first_item_seconds` (from its first item in from another segment, or its sink's start, to its first item out), `response_time_after_first_item_seconds` |
| End to end | the source's call | the end of its response | `end_to_end_requests_total`, `end_to_end_errors_total{error_type}`, `end_to_end_cancellation_total`, `end_to_end_time_to_first_item_seconds`, `end_to_end_duration_seconds` |

`inflight_requests` counts what dynamo counts: the requests that entered the segment at that node and that the segment still has work for. A request counts from its arrival (or, for a backward run, from the open that prepares it) until the segment hands it on, until the stream the segment sends for it ends, or until it is cancelled or fails. A request that waits counts too: for a roster, for its stream to start, or for a consumer that does not read. At the source, it is the number of requests in flight in the graph.

So the front's request path, the engine's request path, the engine's time to its first item, and the time each later segment adds to that first item sum to the time to the first item end to end, less the wire. An error is counted once, in the segment that raised it (`error_type` is the error's kind: `node`, `deadline_exceeded`, `overloaded`, and more). A segment that only passes the error on does not count it again. The end-to-end figure counts the outcome of each request once. A response dropped before its end is a cancellation, or `deadline_exceeded` once the deadline has passed. The request-path counters keep dynamo's work-handler names; the durations do not, because dynamo's `request_duration_seconds` spans the whole handler, its stream included. Durations use dynamo's buckets (5 ms to 10 minutes) with five more below them, down to 100 µs. Nothing is counted per item, and each figure is kept per thread and summed when `/metrics` is scraped, so requests in flight on several threads do not contend. A segment binary records them only when its system listener is on; an embedding sets `RuntimeConfig::metrics`.

## Draw a graph

`cargo gdl diagram` writes the placed graph to stdout as Graphviz DOT or as a Mermaid flowchart. It writes no files.

```bash
cargo run -p gdl-gen -- diagram --spec examples/chat/gdl.toml --format mermaid
cargo run -p gdl-gen -- diagram --spec examples/chat/gdl.toml --format dot | dot -Tsvg -o chat.svg
```

- Request edges are solid. Response edges are dashed.
- Each edge has the name of its message type. If the type has no codec, the label says `(no codec)`, because a placement cannot cut that edge.
- Each segment is a cluster. An edge that crosses segments is thick and red.

For the chat spec, `--format mermaid` writes this diagram. Three edges cross segments: `Words` into `engine`, `Token` out of `engine`, and `Text` out of `post`.

```mermaid
---
title: "chat"
---
flowchart LR
    subgraph s0["engine"]
        n3[("Engine<br/>sink")]
    end
    subgraph s1["front"]
        n0(["Frontend<br/>source"])
        n1[["Preprocess<br/>operator"]]
        n2["Truncate<br/>forward"]
        n6("Number<br/>backward")
    end
    subgraph s2["post"]
        n4("Detokenize<br/>backward")
        n5("Format<br/>backward")
    end
    n0 -->|"Prompt"| n1
    n1 -.->|"Chunk"| n6
    n6 -.->|"Chunk"| n0
    n1 -->|"Words"| n2
    n2 -->|"Words"| n3
    n3 -.->|"Token"| n4
    n4 -.->|"Piece"| n5
    n5 -.->|"Text"| n1
    linkStyle 4,5,7 stroke:#d62728,stroke-width:3px
```

In a program, `Manifest::to_dot` and `Manifest::to_mermaid` draw a contract's `manifest()`. Give them `None` to draw the graph without segments, or a `Layout` from `gdl_runtime::layout` to draw it placed. They return an error if the layout was made for another contract.

## Run the example

1. Generate the segment crates:

   ```bash
   cargo run -p gdl-gen -- generate --spec examples/chat/gdl.toml --out examples/chat/segments
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

To see a segment's metrics, add a `[system]` table with a `port` to its config file and read `GET /metrics` there. The engine reports `dynamo_component_words_total`, which its node defines, beside the runtime's figures for the segment, labelled by the node where each request entered it.

`crates/gdl-gen/tests/chat.rs` does these steps as a test, and `crates/gdl-gen/tests/lifecycle.rs` reads a running engine's metrics.

### A graph with a streamed request

`examples/asr` is a speech recognition pipeline with a mock recognizer. Its request is a session's settings and a stream of audio chunks:

```rust
source   Client: Session + stream Audio -> stream Transcript;
operator Gateway: (Session + stream Audio -> stream Transcript) over (Session + stream Audio -> stream Partial);
forward  Resample: Session + stream Audio -> Session + stream Pcm;
sink     Recognizer: Session + stream Pcm -> stream Partial;
backward Punctuate: Partial -> Partial;
```

Its placement puts `Resample` in a segment of its own, so the audio crosses two times. The generated binary of the source's segment has no JSON-lines server, because one line cannot carry a streamed request. Call the source through the segment library. `examples/asr/demo` starts the three generated segments in one process, sends audio, and receives transcripts while the audio streams in. `crates/gdl-gen/tests/asr.rs` runs it as a test.

## Across segments

- **Responses.** Each response stream goes to an anchor that its consumer created. The runtime never matches a response to a request by a request id.
- **Stop and cancel.** A stop ends the stream after the items that are already buffered. A cancel ends it at once. Both reach the remote producer, also when the consumer holds the stream and does not read it.
- **Deadlines.** A deadline crosses segments as wall-clock time with a clock-skew allowance. Work can end early by up to two times the skew, but never late. A request without a deadline gets `RuntimeConfig::default_deadline` (600 s) before it leaves its segment.
- **Admission.** Each instance admits a limited number of root requests. A request that enters the same instance more than one time uses one admission.
- **Egress.** Each instance limits the number of streams that it produces at the same time. A full instance refuses new work.
- **Failure.** An error item ends a stream. A stream that ends without a finish from its producer ends with an error, not as a success.
- **Streamed requests.** The items of a streamed request go to an anchor that the receiving stage created. The input ends cleanly only when its sender finishes it. A stage that stops reading its input ends its sender, and the response still decides the result. A stop does not end the input. A stop from the caller reaches every stage that has handed the request on or produces a stream, and a stage can end its input when it sees the stop. A stop that a node asks for on its own context reaches the same stages, with one exception: a stage that relays the request reaches the stages after it only while its run lasts. A stop that it asks for after its own input has ended, or just before it ends its own output, does not reach them. In another segment, a forward node that waits for a stop inside `forward`, before it hands the request on, is not reached either. In both cases the deadline ends the request. A stage that relays a streamed request keeps its admission until it has sent all of the items on.

The design and its rulings are in `agent-docs/plan.md`.

Request-only segments hold their reply reservation until downstream acceptance or failure. One task per runtime renews held reservations in batches at `RuntimeConfig::reservation_renew_interval` (30 seconds by default, configured as `reservation_renew_interval_ms`). Velo defaults to 120 seconds before ordinary attachment and 180 seconds for a prepared slot. `RuntimeConfig::anchor_timeout` optionally overrides the ordinary lifetime; `None` inherits Velo's default. Choose a renewal interval shorter than the reservation lifetime. GDL also bounds each renewal call by that interval. Velo does not schedule renewal. Renewal does not extend request deadlines or replace attached-stream heartbeats.

## Cost

A graph that is not cut costs no encoding. A local graph is nested calls, and the nodes of one segment pass values to each other directly. A cut costs one encode and one decode for each message on each edge that crosses segments. There is one more case. If the request of a hop leaves the segment of the hop's outer end, the response items return to the outer end through an anchor. Each item is then encoded and decoded one time, also when no response edge crosses segments. A streamed request's head and each of its items are encoded once on each crossing that they go through. `crates/gdl-runtime/tests/crossings.rs` counts these encodes.

On one aarch64 workstation, over loopback TCP:

- **Per request.** The seven-node reference graph takes about 26 us in one process and about 223 us over two segments. A request with one hop over two segments, whose request is one value, takes about the same time as the same request written directly on velo (about 200 us to 250 us; the run-to-run variance is larger than the difference). gdl adds two tasks and 48 allocations to each request.
- **Per item, at each crossing.** Encoding and decoding cost 123 ns (16 B), 223 ns (1 KiB), and 811 ns (16 KiB). Of that, 80 ns to 400 ns is more than velo direct, which also encodes.
- **Throughput.** Item throughput is 0% to 11% lower than direct velo, about the size of the run-to-run variance. velo's rate for a single stream, about 11 K items/s here, limits both.

- **Streamed requests.** Items of a streamed request cross at velo's rate, the same as on velo directly. A one-item upload over two segments takes about 524 us, against 403 us for the same exchange written directly on velo.

The measurements and the method are in `agent-docs/perf/m3-remote.md` and `agent-docs/perf/streamed.md`.

## Repository layout

| Path | Contents |
|---|---|
| `crates/gdl` | What a node crate depends on, without velo: the request context, node roles, local composition, selectors, metrics, and the derives |
| `crates/gdl-runtime` | The runtime: segments and crossings over velo, discovery, the segment binary, and `graph!`; under a hub, a chooser's tables of the cells its hub follows, and its routing across the class boundary by them, offering the request to another cell when one declines it, with a lease on each offer, by which it acknowledges a cell its hub no longer offers; it re-exports `gdl` and the model |
| `crates/gdl-model` | The model without the runtime: manifest, placement, layout, diagrams, and the names of a segment's hub services, which segments and hubs share. It builds for wasm, so the composer can run the same rules in a browser |
| `crates/gdl-cells` | The cell plane's shared types, without velo: the record each cell's hub publishes, the `CellDirectory` trait its backends implement, the `memory` backend (one process) and the `dir` backend (one file per cell under a directory shared by the hubs on one host, polled every 50 ms), the reader's view, which keeps each cell's newest record and enforces its lease on the reader's clock, and the conformance suite every backend runs (the `conformance` feature) |
| `crates/gdl-hub` | The discovery hub: velo-hub with admission restricted to one deployment, and, for a cell's hub, the `gdl-cell` extension, which judges the cell's state from its quorum, serves the cell health endpoint (with the cells it requires of the classes it sends to), publishes the cell's record, sums its entry instances' load reports into the cell's load summary, serves the cell's watch, follows its children, the cells of the classes it sends to whose record names its cell as their parent, and relays them to its instances, and closes a follow once its choosers acknowledge that they hold no offer to it |
| `crates/gdl-watch` | The watches a cell's hub serves, the cell watch and the relay of what the hub follows, as typed events, and the client that reads them; the hub serves them and a segment instance reads the relay and its own cell's watch through it |
| `crates/gdl-syntax` | The graph syntax: the parser and checked model that `graph!` and the generator share, and a graph file's manifest built without compiling its contract |
| `crates/gdl-message` | `derive(Message)`, the `Message` and `Shape` traits, and the message catalogue, without the runtime |
| `crates/gdl-extract` | The source reader: what a crate offers to graphs (messages, exports, and nodes), read from its source without building it; and the working catalogue of such offers |
| `crates/gdl-macros` | The `graph!` macro, `derive(Message)`, `derive(Node)`, and `#[export]` |
| `crates/gdl-web` | The composer: a page and its server, which check and export graphs built from a working catalogue |
| `crates/gdl-gen` | The generator, `cargo gdl`: `extract`, `catalog`, `resolve`, `generate`, `check`, `diagram`, `compose`, and `export` |
| `examples/chat` | A graph file, the protocols crate that holds its messages, its node implementations, and the generated contract and segment crates. It is a separate Cargo workspace. |
| `examples/asr` | A graph with a streamed request: a contract, its node implementations, the generated segment crates, and a demo that runs them in one process. It is a separate Cargo workspace. |
| [`examples/film`](examples/film/README.md) | Two node crates with graph settings, custom metrics, a mock engine, and a vendor usage tap. Distributed and compact specs use the same graph. |
| `agent-docs/` | The plan, review records, and measurements |

## Build and test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --no-run
timeout 300 cargo test --workspace --all-features
```

Build the tests first (`--no-run`): the `timeout` then counts only the tests, which take about a minute. It turns a hung test into a failure; set it to two or three times the run you expect.

The tests start velo nodes in one process on ports that the OS assigns. The example tests build the example workspaces in `target/examples`.
