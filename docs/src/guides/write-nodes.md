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
| `Unary` | one request, and answers one value. It is not in a hop: a node calls it through a `UnaryClient` | `Req`, `Resp` | `call(&self, &cx, req)` |
| `Frontend`, `Service` | hosts a service that calls the graph | see [Hosted frontends](frontends.md) | `serve(self, ..)` |

[The graph model](../concepts/model.md) explains hops, inner ends, and edge nodes. Two rules apply to every role:

- Report a failure as an `Err` item, or as the call's `Err`. Do not cancel your own `Cx` to fail.
- End your work and your stream when the `Cx` is cancelled or asked to stop. The context is how the runtime stops a node.

A `Backward` node's returned stream must own what it uses, because it outlives the call. A `Backward` node that may take items without passing one on (a filter, a batcher) sets `const FILTERS: bool = true`, so that a cancel or a client's detach reaches the request's edge while it reads ([Calls and attempts](../concepts/requests.md#calls-and-attempts)).

An `Operator` may call `next` zero or more times. A call with the operator's own context runs under the request's scope: it is cheap, its nodes see the request's stop and cancel through their contexts, and the request's edge drops the whole request on a cancel. But the call has no scope of its own. Set `const CALL_SCOPE: bool = true` on the operator when it needs one:

- It hedges or abandons calls, and expects a dropped call's nodes to see a cancel while the request goes on. Under the request scope, a dropped loser's `cx.cancelled()` resolves only when the whole request ends, so an operator, or a sink below it, that waits on it to clean up must set the constant.
- It relies on its `next`'s stream ending at the first `Err`. Under the request scope, a producer's items after an `Err` reach the operator.
- It turns its `next`'s end into output, such as closing replies, and must tell a cancel from a clean end. Under the request scope, a call whose request is over ends quietly, with an empty stream.
- It filters or batches an always-ready local source, or calls `next` from a task it spawned.

A call that passes a context the operator derived, `cx.child()` or another deadline, gets its own scope without the constant.

Use an `Around` instead of an `Operator` when the node would call `next` exactly once, and every value its response side needs from its request side belongs to that call. It costs what a forward node and a backward node cost ([The graph model](../concepts/model.md#around-nodes)):

- Return the value its backward half needs beside the request: `Ok((req, state))`. A retry runs `forward` again, with a new value.
- Box a large value (`type State = Box<...>`). The value travels down the hop's forward chain inside the futures of the forward halves, by value, and the compiler may copy it more than once on the way: a 512 B value was copied 2.6 times per around node per request ([Feature costs](../operations/feature-costs.md#stages-in-callgrind-ir)).
- Make the checks that only need the request in `forward`. An `Err` there is the call's error. In `backward`, the same failure is an error item in the stream.
- A node may hold any value its constructor builds, such as a handle to the hop below that hand-written composition code builds. Work sent through it runs outside the attempt ([Known limitations](../operations/limitations.md#work-sent-through-a-handle-a-node-holds-runs-outside-the-attempt)).

## Write a unary service

A unary service answers one request with one value. Write a sync service as an `async fn` with no `.await`: its future is ready at the first poll.

```rust
#[derive(gdl::Node)]
pub struct Tokenize { /* .. */ }

impl gdl::Unary for Tokenize {
    type Req = Text;
    type Resp = Tokens;
    async fn call(&self, cx: &Cx, req: Text) -> Result<Tokens> { /* .. */ }
}
```

- Keep the service's future to 256 bytes or less, aligned to 8 or less. A call builds such a future inside its own and makes no allocation of its own. A larger future is boxed, one allocation per call ([The graph model](../concepts/model.md#unary-services)).
- Box a large value that the service holds across an `.await`. It is part of the future's size.
- A caller takes its client once in `Build::build`, with `UnaryClient::<Text, Tokens>::current("Tokenize")?`, keeps it on the node, and calls `self.tokenize.call(&cx, text).await` per request. Its `calls` must name the service.
- Do not call a service in `Build::build`: the call fails with `Routing`, "called during build". A call from `Build::ready` to a service in the node's segment waits for the service's own `ready`; one to another segment fails, since the segment has not started ([Unary services](../concepts/unary.md#build-order-and-readiness)).
- A unary call has no hop and no `CALL_SCOPE`: it runs under its caller's context. A caller that wants a private scope for one call passes `cx.child()`; the service sees that scope's cancel cooperatively, with no race, and dropping the call's future ends it. A call to a service in another segment runs on there until its bound ([Known limitations](../operations/limitations.md#a-dropped-remote-call-runs-on-until-its-bound)).
- A `Backward` node's stream must own what it uses. Move a clone of the client and of the context into the stream once, and call per item:

```rust
fn backward(&self, cx: &Cx, input: Items<Token>) -> Items<Text> {
    let state = (input, self.detok.clone(), cx.clone());
    gdl::items(futures::stream::unfold(state, |(mut input, detok, cx)| async move {
        let item = match input.next().await? {
            Ok(token) => detok.call(&cx, token).await,
            Err(e) => Err(e),
        };
        Some((item, (input, detok, cx)))
    }))
}
```

## Box a large request

A request goes through a hop by value. Each hop copies it 4 times, each forward node and around node about 3 times more, and each operator stage about 6. A small request costs nothing to copy, but a large one does: a 2 KiB request was copied 19 times through one hop with 5 forward nodes, about 300 instructions each ([Feature costs](../operations/feature-costs.md#copies-of-a-request)).

For a request of a few hundred bytes or more, make the port's message type a box, so that the hop moves a pointer:

```rust
impl gdl::Forward for Translate {
    type In = Box<ChatRequest>;
    type Out = Box<EngineRequest>;
    async fn forward(&self, cx: &Cx, req: Box<ChatRequest>) -> Result<Box<EngineRequest>> { /* .. */ }
}
```

- A box costs one allocation per request. At 5 forward nodes with a 2 KiB request it brought a request from 19 copies and 13,056 instructions to none and 8,036. At one hop with no nodes, a box costs about 520 instructions more than a `u64` request.
- Build the request in its box where you can, as `Box::new(ChatRequest { .. })`. A request built on the stack and then boxed, as one deserialized into a value is, is copied once into its box.
- A node that changes the request type makes a new box, one more allocation.
- An operator that retries keeps its request and clones it for each attempt. A boxed request clones into a new box: one allocation and one copy.
- gdl does not box a request for you. A node takes its request by value, so a box inside gdl would be copied out into every node, and a small request would pay an allocation for nothing.

## Return an operator's body future as it is

gdl builds an operator's future in its box, on every request. It can do that only with the future that `generate` returns. When `generate` wraps the future of another `async fn`, the compiler builds that future on the stack and copies it whole:

- A combinator such as `.map(..)` or `.map_err(..)` takes the future by value. It is copied into the box when gdl builds the operator's future.
- An `async` block or an `async fn generate` that awaits it copies it into its own future when it starts.

The copy includes the room that the future keeps for large values it awaits later, which nothing has written yet. In one dynamo operator that was 9,808 B per request. If the body has a different error type, convert it inside the body, with `.map_err(..)?` or a `match`, and return the body's future itself:

```rust
impl gdl::Operator for Migrate {
    // ..
    fn generate<N: Next<Req, Item>>(&self, cx: Cx, req: Req, next: N) -> impl Future<Output = Result<Items<Item>>> + Send {
        self.migrate(req, Downstream::new(cx, next))
    }
}

impl Migrate {
    async fn migrate<N: Next<Req, Item>>(&self, req: Req, down: Downstream<N>) -> Result<Items<Item>> {
        let manager = RetryManager::build(req, down).await.map_err(into_gdl)?;
        Ok(manager.into_items())
    }
}
```

A large future that the body awaits is still copied once into place when the body starts. gdl cannot reach that copy ([Known limitations](../operations/limitations.md#a-large-future-that-a-nodes-body-awaits-is-copied-when-the-body-starts)). The numbers are in [Feature costs](../operations/feature-costs.md#an-operators-body-future).

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
