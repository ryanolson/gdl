# The graph model

A gdl graph is a set of typed nodes joined into nested hops. The graph fixes each node's role and the message type on each edge. It does not say where a node runs: [Specs, segments, and layouts](placement.md) does that. This chapter describes the shape of a graph, the role traits that a node implements, and the rules that every response stream follows.

## Roles

Each node has one of seven roles:

| Role | Sees | Does |
|---|---|---|
| source | the request and the response | Sends typed requests into the graph and receives response streams. A hosted source implements `Frontend`. A source can also be a programmatic entry point with no implementation. |
| operator | the request and the response | Calls the rest of the graph through `Next`, zero or more times, and returns a stream. |
| forward | the request only | Changes the request on its way in. |
| sink | the request | Produces the item stream. |
| backward | the response stream only | Changes the item stream on its way out. |
| around | the request, then the response stream | A forward half and a backward half in one hop, with a value kept from one to the other for each attempt. See [Around nodes](#around-nodes). |
| unary | one request, then one response | Answers a request with one value. It is not in a hop: another node calls it through a `UnaryClient`. See [Unary services](#unary-services). The graph declares it with `unary`, and each caller says `calls` ([Declare unary nodes](../guides/define-a-graph.md#declare-unary-nodes)). |

An operator and a source keep per-request state across both directions in one task. So each of them is placed whole, in one segment. An around node keeps a value across directions too, and both its halves must run in one task: with the hop's outer end, or with its inner end ([Specs, segments, and layouts](placement.md#what-is-checked-and-when)). A forward or backward node holds no state across directions, so a placement can put it anywhere.

## Hops

A hop is an outer end (a source or an operator), its forward nodes, an inner end (a sink or an operator), and its backward nodes. An operator is the inner end of one hop and the outer end of the next. The grammar is:

```text
Graph    := Source Hop
Hop      := Forward* Inner Backward*
Inner    := Sink | Operator Hop | Around Hop
```

An around node opens no hop of its own. In `A => P { Rest } => B`, its forward half comes after `A`'s forward nodes and before those in `Rest`, and its backward half after the backward nodes in `Rest`. The braces must hold the hop's inner end.

The reference graph that the runtime tests use has two hops. Requests go right along the top line, and responses go left along the bottom line:

```text
A ----------> B --------> C -----> D
A <--- G <--- B <--- E <--- F <--- D
```

Hop 0 has outer end A (the source), inner end B (an operator), and backward node G. Hop 1 has outer end B, forward node C, inner end D (a sink), and backward nodes F, then E.

A stage is one position in a hop: a forward node, the inner end, or a backward node. A graph can have several source roots, and a sink can take requests from several roots through explicit connections. [Hosted frontends and service entries](../guides/frontends.md) describes those shapes. The graph syntax is in [Define a graph](../guides/define-a-graph.md).

## Properties

A graph also declares the kinds of property its pools serve, such as the model a pool of engines has loaded. A property is not on an edge. A node in an entry segment of a class says that its instance has properties of a kind (`provides`). A node, an entry adapter, or a pool policy reads them from the pools of a class (`needs`). The graph declares each kind with the message type of its keys.

So the graph says two things on two axes: what a request and its response carry along the hops, and which properties each class's pools must supply. `cargo gdl check` and `cargo gdl build` refuse a class that a reader needs a kind from when nothing supplies the kind to it. [Define a graph](../guides/define-a-graph.md#declare-properties) gives the syntax, and [Classes and pools](pools.md#properties) says how a pool publishes its properties.

## Role traits

A node implementation implements one role trait from the `gdl` crate. Each method returns `impl Future + Send`. On the local path, only a node's output stream and a streamed request's items are boxed, and a unary service's future when it does not fit its call's buffer ([Unary services](#unary-services)).

```rust,ignore
pub type Items<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send>>;
pub trait Sink: Send + Sync + 'static {
    type Req: Send + 'static; type Item: Send + 'static;
    fn generate(&self, cx: Cx, req: Self::Req) -> impl Future<Output = Result<Items<Self::Item>>> + Send;
}
pub trait Forward: Send + Sync + 'static {
    type In: Send + 'static; type Out: Send + 'static;
    fn forward(&self, cx: &Cx, req: Self::In) -> impl Future<Output = Result<Self::Out>> + Send;
}
pub trait Backward: Send + Sync + 'static {
    type In: Send + 'static; type Out: Send + 'static;
    fn backward(&self, cx: &Cx, input: Items<Self::In>) -> Items<Self::Out>;
}
pub trait Next<Req, Item>: Clone + Send + Sync + 'static {
    fn generate(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Item>>> + Send;
}
pub trait Operator: Send + Sync + 'static {
    type UpReq: Send + 'static; type UpItem: Send + 'static;
    type DownReq: Send + 'static; type DownItem: Send + 'static;
    fn generate<N>(&self, cx: Cx, req: Self::UpReq, next: N) -> impl Future<Output = Result<Items<Self::UpItem>>> + Send
    where N: Next<Self::DownReq, Self::DownItem>;
}
pub trait Around: Send + Sync + 'static {
    type UpReq: Send + 'static; type UpItem: Send + 'static;
    type DownReq: Send + 'static; type DownItem: Send + 'static;
    type State: Send + 'static;
    fn forward(&self, cx: &Cx, req: Self::UpReq) -> impl Future<Output = Result<(Self::DownReq, Self::State)>> + Send;
    fn backward(&self, cx: &Cx, state: Self::State, input: Items<Self::DownItem>) -> Items<Self::UpItem>;
}
pub trait Unary: Send + Sync + 'static {
    type Req: Send + 'static; type Resp: Send + 'static;
    fn call(&self, cx: &Cx, req: Self::Req) -> impl Future<Output = Result<Self::Resp>> + Send;
}
pub trait Frontend: Build {
    type Req: Send + 'static; type Item: Send + 'static;
    fn serve<N>(self, next: N, shutdown: impl Future<Output = ()> + Send + 'static) -> impl Future<Output = Result<()>> + Send
    where N: Next<Self::Req, Self::Item>;
}
```

A returned stream is `'static`. It owns what it uses and cannot borrow `&self` or `&Cx`. Each call of `Next::generate` is one attempt. [Requests, attempts, and deadlines](requests.md) gives the attempt rules.

### Around nodes

An `Around` node is an operator that calls the rest of its hop exactly once, folded into the hop. It has a forward half and a backward half. The forward half changes the request and keeps a value, its `State`. The backward half of the same attempt receives that value by value, with the response stream. So a stage whose response side needs what its request side computed, such as a tokenizer's state, costs what a forward node and a backward node cost, not an operator's hop of its own ([Feature costs](../operations/feature-costs.md#stages-in-callgrind-ir)).

In a graph file, an around node takes an operator's signature and braces around the rest of its hop ([Define a graph](../guides/define-a-graph.md#write-a-graph-file)). In local composition, `gdl::around(node)` returns the two halves, `ForwardHalf` and `BackwardHalf`. The first goes in a hop's forward chain and the second in the same hop's backward chain:

```rust,ignore
let (ahead, behind) = gdl::around(Arc::new(backend));
let hop = Hop::new((ahead, ()), engine, (behind, ()));
```

- **The value belongs to one attempt.** It moves down the forward chain with the request, then into the stream the backward half returns. It is never stored where another attempt could reach it, and nothing looks it up. A retry is a new attempt with a new value.
- **Halves pair by nesting, as brackets do.** The forward chain pushes each value on a typed stack, and the backward chain takes them in the reverse order. A half with no partner, or two pairs of different node types that cross, do not compile. Two pairs of the same node type that cross are not caught ([Known limitations](../operations/limitations.md#two-around-nodes-of-one-type-can-cross-in-a-hand-built-hop)).
- **The halves get a forward node's and a backward node's contexts.** The forward half gets the attempt's context. A backward half that is the last node of a local backward chain gets the attempt's own context, so its stop reaches the inner end. Any other backward half gets a scope of its own below the attempt.
- **If `forward` fails, the call fails.** A failure in `backward` is an error item in the stream. A check that only needs the request belongs in `forward`, so that it keeps its kind and its place as the call's error.

A generated segment binary builds each implementation through `Build`, from that node's section of the segment settings. `build` gets the node's configuration and its metrics, already registered. It must return quickly, because the segment registers with its deployment only after every node is built. `ready` resolves when the node can take work, for example when a model is loaded. It resolves at once by default. A node without metrics writes `type Metrics = ();`. [Write nodes](../guides/write-nodes.md) shows each trait in use, and [Segment lifecycle](lifecycle.md) shows where `build` and `ready` run.

### Unary services

A unary node answers one request with one value. It is not part of a hop. Another node calls it through a `gdl::UnaryClient<Req, Resp>`, which the node keeps and calls per request:

```rust,ignore
let tokens = self.tokenize.call(&cx, text).await?;
```

- **A client is one `Arc`.** A node clones it into a stream it returns, with a clone of its context, and calls it per item.
- **A call borrows the context.** The service reads the deadline, the request id, and the metadata from it. The call makes no child context and checks no deadline itself. Dropping the call's future cancels the call.
- **The service's future is built inside the call's future.** The call's future has one size for every service, with a buffer of 256 bytes aligned to 8. At its first poll, it builds the service's future in that buffer and polls it there. A future that is larger, or aligned to more than 8, is boxed. So a call makes no allocation of its own for a future that fits: a sync service's future of 56 bytes, or a lookup behind a tokio mutex of 160 bytes ([Feature costs](../operations/feature-costs.md#a-local-unary-call)).
- **The call's future is not `Unpin`.** After its first poll, the service's future lives inside it and must not move. Await the call, or pin it.
- **A service's error is the call's error**, with its kind and its source.

A boxed future was rejected: it costs an allocation and about 140 more instructions per call. So was a buffer returned by value, as the `stackfuture` crate does: the whole buffer then moves into the caller's future on every call. A client generic over the service would cost what a direct call costs, but it makes every caller generic.

A client calls a service in the same process only ([Known limitations](../operations/limitations.md#a-unary-service-is-called-only-in-its-callers-process)).

## Streams and errors

- **Every response is a stream.** A unary response is a stream of one item.
- **Errors are terminal.** A stream item is `Result<T, gdl::Error>`. Every runtime boundary delivers the first `Err`, then ends the stream and cancels its upstream. A recoverable application error belongs inside `T`.
- **Success needs an explicit end.** A stream that crossed a segment ends successfully only if its producer finished it. Any other ending (the producer detached, was dropped, stopped responding, or the stream just ended) becomes a terminal `Err` of kind `Truncated`.
- **A node reports failure with an `Err` item.** Its context is how the runtime stops it. A node that cancels its own context must still end its stream, and the runtime reports that ending as `Cancelled`.

`gdl::Error` is serializable: a kind and a message. The kinds are `Node`, `Cancelled`, `DeadlineExceeded`, `Truncated`, `NotHosted` (placement checks prevent it), `Overloaded` ([Admission and egress](admission.md)), `Transport`, `Codec`, `Placement`, `Routing`, and `OutcomeUnknown` ([Routing, selectors, and offers](routing.md)). An error that arrives from a peer with a kind this build does not know reads as `Transport`. The rustdoc of `gdl::ErrorKind` describes each kind.

An error can also hold a source: `Error::with_source` attaches any `std::error::Error`, and `std::error::Error::source` returns it, so a caller can downcast it or walk its chain. The source is local to the process. An error passes through nested calls in one segment by value and keeps its source. A source is never serialized, so an error that crosses a segment arrives without it, as its kind and its message alone. Decide by the kind on anything that can cross. A source is not part of the error's equality or its `Display`, and it adds nothing to the error's size: `gdl::Error` and `Result<(), gdl::Error>` stay 32 bytes.
