# Define a graph

You can write a graph in two forms. A graph file names messages and leaves the types to protocols crates. A contract crate holds Rust types and one `graph!` invocation. Use a graph file unless you need the graph inside a crate you already build. [The graph model](../concepts/model.md) explains the roles and the grammar.

## Write a graph file

A graph file holds the body of a `graph!` invocation. Where `graph!` has a Rust type, the file has a message name or `()`. This is `examples/chat/chat.gdl`:

```text
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

In the topology, each hop lists its forward nodes, then its inner end, then its backward nodes in the order the response goes through them. An operator owns its hop in braces.

[Split, generate, and draw](split-and-generate.md#draw) draws it. A request position can be a stream: `Head + stream Item`, or `stream Item` for a head of `()`. [Streamed requests](../concepts/streamed-requests.md) says what a node receives.

Keep these rules in mind:

- The file accepts a bare name or `()` in a message position. A path, a generic, or a reference is a Rust type and is refused.
- `schema` is a number you own. Bump it when a message's serde representation changes. A change of codec changes the contract hash on its own.
- `identity N: request Req;` and its two siblings declare a built-in node that needs no implementation. [Hosted frontends](frontends.md) uses them.

## Offer the messages

A protocols crate depends on `gdl-message` and derives `Message` for each type the graph names, as [Messages and encodings](../concepts/messages.md#offering-a-message) shows. Every type needs `Serialize` and `DeserializeOwned` to cross segments, and must be `Send + 'static`.

To offer a type that is defined in another module or crate, mark a `pub use` with `#[gdl_message::export]`. A message defined in a private module must be exported, because the generated contract names a public path.

Choose the encoding per message. The default, named MessagePack, is right for most types. Write `#[gdl(codec = "json")]` for a message that other tools read, and `msgpack_array` only for a fixed layout whose fields are never skipped. The derive and `cargo gdl check` refuse a `msgpack_array` message whose field serde can skip in one direction. [Messages and encodings](../concepts/messages.md#encodings) has the table and the cases each one cannot keep.

A type you do not own can be wrapped in a local type with `#[serde(transparent)]`. For a generic type, export an alias of each instantiation the graph uses: [Generic messages and nodes](../concepts/messages.md#generic-messages-and-nodes).

## List the crates in the spec

The spec names the graph file and the crates that offer its messages. An entry is a directory, `name@version` on crates.io, or a Git table. A Git table can add `lib` when the library name differs from the package name.

```toml
[graph]
path = "chat.gdl"

[messages]
crates = ["protocols"]
# crates = [{ package = "protocols", git = "https://example.com/repo", rev = "<commit>" }]
pin = { Chunk = "chat_protocols::Chunk" }   # only to break a tie

[gdl]
path = "../../crates/gdl-runtime"
```

The generator fetches and reads a remote crate. It does not build it. Generated crates depend on a crate listed by version at exactly that version. Its library must be named for its package, because the spec cannot know the library name before the fetch.

## Resolve the names

```bash
cargo gdl resolve --spec gdl.toml
```

The command checks that the names agree on every edge, then prints the type each name resolves to and whether it is a wire type. It reports every problem at once.

- A name that no listed crate offers lists the names that the crates do offer, with near matches.
- A name with two candidates lists them. Add a `pin` to choose one by its public path. A pin must name a message the graph uses.
- A crate that offers nothing is a problem. Derive `Message` on its message types, or export them.

A message offered by two listed crates, one by definition and one by export, is one message. [Messages and encodings](../concepts/messages.md#naming-a-message-in-a-graph-file) says how identity works.

## Write the contract by hand instead

A contract crate holds the message types and one invocation:

```rust
gdl_runtime::graph! {
    name = "asr", schema = 1;
    source Client: Session + stream Audio -> stream Transcript;
    // ... more nodes, then the topology
}
```

The spec then uses `[contract]` with `path` (and `module` when `graph!` is not at the crate root) and binds each node in a `[bind.<Node>]` table. `examples/asr` uses this form, and `examples/chat` uses a graph file, so the tests cover both. [The spec reference](../appendix/spec-reference.md) lists the keys.

Next: [split the graph and generate the crates](split-and-generate.md).
