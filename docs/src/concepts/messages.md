# Messages and encodings

A graph names its messages. A message is a Rust type that a crate offers to graphs. This chapter covers what makes a type a message, how it is encoded when it crosses segments, how a name in a graph file becomes one type, and how a generic type is offered. [Define a graph](../guides/define-a-graph.md) gives the steps.

## Offering a message

A protocols crate offers a type with `derive(gdl_message::Message)`. `gdl-message` is a small crate without the runtime, so a protocols crate builds without velo or tokio. A crate that depends on `gdl` can write `gdl::Message` instead.

```rust
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
pub struct Prompt {
    pub text: String,
}
```

The type must be `Send + 'static`. The derive reports a type that is not, at the type's definition, so the error does not wait for a contract that uses it.

A crate can also offer a type that is defined elsewhere. `#[gdl_message::export]` on a `pub use` keeps the `use` and offers the type under the public path of the `pub use`. `gdl::export` is the same attribute through `gdl`.

```rust
#[gdl_message::export]
pub use chat_protocols::Prompt;
```

## Wire capability

A message is a wire type when it implements `Serialize` and `DeserializeOwned`. Only a wire type can cross segments. A type that is not one can still travel inside one segment, so a graph can name it as long as no cut separates its two ends.

`graph!` decides this by a probe that it expands at the concrete type. A probe inside a generic helper would always take the fallback, so the macro writes it only at concrete types. A graph file has no compiler to run the probe. The source reader decides instead, and the segment's build checks the answer: [Generated crates](generation.md#the-crossing-assertions).

A type that deserializes only by borrowing from its input is not classified. Its bound fails after method selection, so the probe is a compile error. Message types own their data.

The traits do not prove that a value survives a round trip. A custom `Serialize` impl can still fail. [Known limitations](../operations/limitations.md) lists the encodings' cases.

## Encodings

A message selects its encoding with `#[gdl(codec = "...")]`. The default is `msgpack`.

| Annotation | Encoding |
|---|---|
| none, or `codec = "msgpack"` | MessagePack maps with field names |
| `codec = "msgpack_array"` | positional MessagePack arrays. No field may be omitted in one direction only. |
| `codec = "json"` | JSON |

- The annotation applies to the complete value, nested fields included. A nested type's own annotation does not change how it serializes inside its parent.
- A streamed request selects the head's and the item's codecs separately, because they are two messages.
- Named fields keep an omitted optional field in nested structs. Positional arrays are smaller for a fixed layout, and break when a field is skipped. JSON writes a non-finite float as `null` and refuses a map key such as a tuple. Use `msgpack` for those values.
- `msgpack_array` refuses a message with a field that serde can omit in one direction: `skip_serializing_if`, or `skip_serializing` or `skip_deserializing` alone. An array has no field names, so the fields after it would shift, and the value would fail to decode or decode into the wrong fields. `skip`, or both one-sided skips, omits the field in both directions and is accepted. So is the field of a newtype struct or a `transparent` struct, whose skips serde ignores; `skip_serializing_if` on a newtype variant's field, which serde also ignores; and a field of a variant that serde skips, which serde refuses before it reads the fields. `derive(Message)` refuses at compile time, and the source reader refuses the same message, so `cargo gdl check` reports it with the field's name before a build. The check runs once per message type, so encoding a value costs nothing more. It does not look inside a field's type: [Known limitations](../operations/limitations.md#positional-messagepack-checks-only-the-messages-own-fields-for-omissions).
- A third-party type can be wrapped in a local type with `#[serde(transparent)]` and `derive(Message)`.
- The envelope and control frames use their own encoding. The codec here is for application messages only. See [Crossing segments](crossings.md) for where the codec applies and for the rule that decides which edges need one.
- A sender and a typed routing selector use the same dispatch. The edge picks monomorphized function pointers once. Each item calls them without a lookup or a boxed value.

The contract hash covers each message's encoding and the encoding's wire revision (1 today). A codec change therefore changes the deployment's identity without a `schema` bump, and an instance rejects an envelope from a build with another codec. A change to a message's serde representation still needs a `schema` bump, because Rust type names cannot fingerprint a serde form. [Cost of each codec](../operations/crossing-performance.md) has the measurements.

## Naming a message in a graph file

A graph file uses a bare name where `graph!` uses a Rust type: `Prompt`, or `()`. A path, a generic, or a reference is refused, because those are Rust types. The generator resolves each name to one message before it generates anything: first the names are checked against the graph's edges, then each listed crate is read, then each name must match one message.

- Names are checked first, before any crate is read. A streaming disagreement is structure and is reported for every edge.
- A listed crate offers what it defines and what it exports. A crate that offers nothing is a problem.
- A name must resolve to exactly one message. A missing name lists what the crates offer. An ambiguous one lists its candidates. A `pin` in `[messages]` chooses a type by its public path, for a name with two candidates or a name that differs from its type's.
- Search order never decides. A type added to an earlier crate would otherwise rebind a graph without warning.
- Every error is reported at once.

`cargo gdl resolve --spec gdl.toml` prints the result: one line per name with its type and `wire` or `local`.

```text
Chunk   chat_protocols::Chunk          wire
Piece   chat_protocols::Piece          local
Prompt  chat_protocols::Prompt         wire
```

### Identity is the origin

A message is identified by where it is defined: the package, its version and source, and the definition path. Not by the path it is offered under. Three results follow.

- A type offered once by its crate and once by an export in another crate is one message.
- Two versions of a package, or one version from two sources, are two messages. The graph and a node can only connect on equal origins.
- The generated contract names each message by a public path through a listed crate: the export's path if there is one, else the definition's. The contract hash covers that rooted path, so a name that resolves to another message changes the hash. A message defined in a private module must be exported, and resolution says so.

The package version is not in the hash. A version that changes a wire form needs a `schema` bump, as any change does.

## Generic messages and nodes

One generic type can serve many graphs: a carrier generic over the request it carries, or a node written once for every builder an endpoint picks. A graph names one concrete type per message and node, so the generic type offers nothing itself. A `pub type` marked `#[export]` names one instantiation, and the alias's name is what the graph file writes.

```rust
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "json")]
pub struct Ingress<P, RB> { pub request: P, pub builder: RB }

#[gdl_message::export]
pub type MessagesIngress = Ingress<MessagesRequest, InferenceRequestBuilder>;
```

- A generic type takes type parameters only. The derive registers nothing for it. It implements a hidden trait for every instantiation, which the alias reads where the instantiation is concrete.
- An alias of a message offers the instantiation under the alias's path and name, as an export does. Its codec is the generic type's `#[gdl(codec)]`.
- An alias of a node, through `gdl::export`, offers a node of the alias's name. [Write nodes](../guides/write-nodes.md) shows the node side.
- The alias takes no parameters of its own and writes every argument.

### An instantiation is identified by its type

An instantiation's identity is its generic type's package, its type, each argument by the path where it is defined, and the package that defines each argument. A port, an alias, and an export that name the same instantiation agree however each spells it. Two aliases of one instantiation are one message under two names.

The rejected alternative was the alias's own path as the identity. It made a node crate's port match only an alias that the reader found in some crate it had read, and it made two aliases of one type two messages. The package of each argument was added because the argument's path alone is the same for two versions of a package, so a port of one version would have matched a node of the other.

### Wire capability per instantiation

An instantiation is a wire type when serde's derive would bound it to wire types, for each direction on its own. The reader applies serde's inference: an argument is a wire type if a field that serde does not skip names its parameter outside `PhantomData`. A `#[serde(bound)]` replaces the inference for its direction. A direction written by hand bounds what its impl bounds.

The reader fails closed. It takes an instantiation for a wire type only where it can show that it is one. Anything it cannot judge, such as another trait as a bound, a bound on a type that is not a parameter, an associated type, or a serde attribute it does not read, makes the instantiation local. A local instantiation refuses at worst a crossing that would work, and `cargo gdl extract --verify` reports it. `cargo gdl extract --probe` records the build's reading in a catalogue entry, where it takes precedence over the reader's ([The catalogue and the source reader](catalogue.md#--probe-the-builds-wire-capability)). The opposite error, a wrong "wire", would let the generator accept a cut that does not build.

The bar is capped on purpose. Crashes, and a wrong answer for code that real crates write, are always fixed. A deliberate name trick, such as a local `mod serde` or a dependency renamed `serde`, is documented and not chased, because the segment's crossing assertion catches it. See [Catalogue design](../development/catalogue-design.md) for the reader and [Known limitations](../operations/limitations.md) for what it refuses.
