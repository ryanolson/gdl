# Write nodes

This guide shows how to write a node crate: the types that implement a graph's nodes. A node crate depends on `gdl`, and on the protocols crate that holds its messages. It does not depend on `gdl-runtime`, so it does not link velo. `crates/gdl/tests/dependencies.rs` fails the build if `gdl` ever links velo.

## Mark a node type

1. Define a `pub` type at the crate root, named as the graph names the node.
2. Add `#[derive(gdl::Node)]` to it.
3. Implement exactly one role trait for it, and `gdl::Build`.

```rust
#[derive(gdl::Node)]
pub struct Engine { /* .. */ }

impl gdl::Sink for Engine {
    type Req = Words;
    type Item = Token;
    async fn generate(&self, cx: Cx, req: Words) -> Result<Items<Token>> { /* .. */ }
}
```

The derive checks, when the crate compiles, that the type is `pub` and that `crate::Engine` names it. Generated segments name the type at the crate root. A type re-exported at the root from a module also passes. The source reader finds the role trait impl anywhere in the crate. A type with no role trait impl, or with two, is refused by the reader.

Each port type must be a message that derives `Message`, `()`, or `gdl::Streamed<H, T>` of messages. Write `Streamed` out at the port. A type alias of it gets no streaming codec. A crate that uses any gdl derive must depend on `gdl` itself. The derives expand to `::gdl::...`, so `gdl_runtime::gdl::Node` does not build in a crate that depends on `gdl-runtime` alone.

## Pick a role

| Trait | Sees | Ports | Method |
|---|---|---|---|
| `Sink` | the request, returns the response stream | `Req`, `Item` | `generate(&self, cx, req)` |
| `Forward` | the request only | `In`, `Out` | `forward(&self, &cx, req)` |
| `Backward` | the response stream only | `In`, `Out` | `backward(&self, &cx, items)` |
| `Operator` | both, and calls the rest of the graph through `Next` | `UpReq`, `UpItem`, `DownReq`, `DownItem` | `generate(&self, cx, req, next)` |
| `Around` | both, as a forward half and a backward half of one hop, with a value kept between them | `UpReq`, `UpItem`, `DownReq`, `DownItem`, `State` | `forward(&self, &cx, req)` and `backward(&self, &cx, state, items)` |
| `Frontend`, `Service` | hosts a service that calls the graph | see [Hosted frontends](frontends.md) | `serve(self, ..)` |

[The graph model](../concepts/model.md) explains hops, inner ends, and edge nodes. Two rules apply to every role:

- Report a failure as an `Err` item, or as the call's `Err`. Do not cancel your own `Cx` to fail.
- End your work and your stream when the `Cx` is cancelled or asked to stop. The context is how the runtime stops a node.

A `Backward` node's returned stream must own what it uses, because it outlives the call. An `Operator` may call `next` zero or more times. Each call is one attempt with its own cancel scope.

Use an `Around` instead of an `Operator` when the node would call `next` exactly once, and every value its response side needs from its request side belongs to that attempt. It costs what a forward node and a backward node cost ([The graph model](../concepts/model.md#around-nodes)):

- Return the value its backward half needs beside the request: `Ok((req, state))`. A retry runs `forward` again, with a new value.
- Make the checks that only need the request in `forward`. An `Err` there is the call's error. In `backward`, the same failure is an error item in the stream.
- A node may hold any value its constructor builds, such as a handle to the hop below that hand-written composition code builds. Work sent through it runs outside the attempt ([Known limitations](../operations/limitations.md#work-sent-through-a-handle-a-node-holds-runs-outside-the-attempt)).

## Build and readiness

A segment binary builds each node it hosts from that node's settings table, `[node.<Name>]`:

```rust
impl Build for Engine {
    type Config = Settings;          // any DeserializeOwned type
    type Metrics = EngineMetrics;    // a derive(Metrics) type, or ()

    async fn build(settings: Settings, metrics: EngineMetrics) -> Result<Self> { /* .. */ }
    async fn ready(&self) -> Result<()> { /* load weights, open connections */ }
}
```

- A `build` that compiles in its own crate compiles in any segment that hosts the node, however deeply its future nests. The segment boxes it.
- Return from `build` quickly. The segment registers with its deployment once every node is built, so the deployment knows that the process is live while the nodes load.
- Do slow work in `ready`. The segment announces that it is loaded, and takes requests, only after every node it hosts is ready. The default `ready` returns at once.
- A node without metrics writes `type Metrics = ();` and takes `(): ()`. A node with metrics names the type in the derive, `#[gdl(metrics = EngineMetrics)]`. [Define node metrics](node-metrics.md) covers that type. [Segment lifecycle](../concepts/lifecycle.md) describes the launch phases.

## Run code when the instance leaves

Take `gdl::Lifecycle::current()` in `Build::build`, and register a hook to hand off state or flush it:

```rust
impl gdl::Build for Engine {
    type Config = EngineConfig;
    type Metrics = ();

    async fn build(config: EngineConfig, (): ()) -> gdl::Result<Self> {
        let store = Store::open(&config)?;
        let held = store.clone();
        gdl::Lifecycle::current().on_leave("kv", move |leave: gdl::Leave| async move {
            // Other instances stop picking this one first.
            match leave.routed().await {
                gdl::Routing::Confirmed | gdl::Routing::SourcesDone if !leave.external() => {}
                _ => return gdl::Handoff::Nothing,
            }
            tokio::select! {
                moved = held.hand_off(leave.fence()) => moved,
                _ = leave.cx().cancelled() => gdl::Handoff::Failed("out of time".into()),
            }
        })?;
        Ok(Self { store })
    }
}
```

- Give each hook its own name. The name is the label of its metrics. A second hook of the same name is an error.
- Register before the instance serves. A later `on_leave` is an error.
- Several hooks can register. They run at the same time, under one deadline, each with its own outcome.
- Return `Done`, `Partial { moved, left }`, `Failed(reason)`, or `Nothing`. A hook that passes its deadline is recorded `timed_out`.
- Wait for `leave.drained()` before you destroy state. A hand-off sent before a peer's promise can still arrive after `routing()` moves.
- Do not make a request wait on work that a hook does after `leave.drained()`. That wait lasts until the drain's cutoff.
- Stop at `leave.cx()`. The runtime does not wait for a hook past its deadline.

[Leaving](../concepts/lifecycle.md#leaving) says when each signal moves. `[runtime] leave_ms` sets the deadline ([Segment settings](../operations/configuration.md)).

## Expose settings to the composer

By default, a node's `Config` is a plain `Deserialize` type, and the composer shows none of it. To show some fields in the composer and fix them in the graph, derive `gdl::Config`:

```rust
#[derive(serde::Deserialize, gdl::Config)]
pub struct ApiConfig {
    /// Public chat request/response path.
    #[gdl(graph, default = true, label = "Chat Completions", subtext = "OpenAI chat endpoint")]
    pub enabled_chat: bool,
    pub listen: std::net::SocketAddr,   // runtime-only: not marked
}
#[derive(gdl::Node)]
#[gdl(config = ApiConfig)]
pub struct ApiFrontend { /* .. */ }
```

- Mark a field with `#[gdl(graph, default = ...)]` to make it a graph setting. Unmarked fields stay runtime-only.
- A graph setting is a `bool`, an `i64`, or a `String`, with a literal default.
- `label` and `subtext` name the setting in the composer. The doc comment becomes its help text. Neither changes the serialized key.
- `#[gdl(config = T)]` on the node must name the node's `Build::Config`. The derive checks this.
- `derive(gdl::Config)` is the only way to implement `gdl::ConfigSchema`. The trait is sealed, and a hand-written impl fails to build with a message that names the derive. The reader takes the fields from the derive's attributes, and it cannot run a hand-written impl.

A generated spec keeps the resolved values under `[config.<Node>]`. At start, `gdl_runtime::build_node` merges the node's settings with those values. A settings value that conflicts with the checked graph stops the start. To change a graph setting, change the graph and generate again.

## Write a generic node

One generic type can serve many graphs, for example a stage written once for every request builder. A graph names one type per node, so the generic type offers nothing itself. Name each instantiation that a graph uses with an exported alias at the crate root:

```rust
#[derive(gdl::Node)]
#[gdl(config = SignalSettings, metrics = SignalMetrics)]
pub struct ClaudeCodeSignals<RB>(std::marker::PhantomData<fn() -> RB>);

impl<RB: RequestBuilder> gdl::Forward for ClaudeCodeSignals<RB> { /* In = Out = Ingress<MessagesRequest, RB> */ }

#[gdl::export]
pub type MessagesSignals = ClaudeCodeSignals<InferenceRequestBuilder>;
```

- The graph file names the alias: `forward MessagesSignals: MessagesIngress -> MessagesIngress;`.
- The generic type takes type parameters only. Lifetime and const parameters are refused. A type cannot derive both `Message` and `Node`.
- The alias takes no parameters of its own and writes every argument. It must be at the crate root. The reader refuses one that is not, though the alias's build does not check it.
- The node's role and ports come from the role impl whose self type matches the instantiation, with the impl's parameters bound by the alias's arguments. An impl for one instantiation serves only that one.
- The `config` and `metrics` the derive names must be the `Build` types of every instantiation an alias names. They may be the type's parameters, `#[gdl(config = C, metrics = M)]`, so that each instantiation has its own. An alias that binds `M` to `()` has no metrics.
- Use `gdl::export` for a node's alias. `gdl_message::export` offers messages only.

[Messages and encodings](../concepts/messages.md) describes generic messages and how an instantiation is identified. `crates/gdl-gen/tests/generic.rs` generates and runs a graph of a generic carrier and a generic stage, each instantiated twice.

## Test nodes in one process

`gdl` composes nodes as nested calls in one process, with no runtime and no transport. Use this to test a node crate:

```rust
// forward chain, inner end, backward chain
let graph = Source::new(Hop::new((Arc::new(Upper), ()), SinkStage::new(Arc::new(Spell)), (Arc::new(Mark), ())));
let items = graph.generate(Cx::new(), "ab".to_string()).await?;
```

`OperatorStage` puts an operator at the inner end, and `Join` runs another composed graph there. `crates/gdl/tests/local.rs` has complete examples. Before you publish, run `cargo gdl extract <dir>` to see what the reader finds, and `--verify` in CI ([The catalogue and the source reader](../concepts/catalogue.md#--verify-the-backstop)).
