# The graph model

A gdl graph is a set of typed nodes joined into nested hops. The graph fixes each node's role and the message type on each edge. It does not say where a node runs: [Specs, segments, and layouts](placement.md) does that. This chapter describes the shape of a graph, the role traits that a node implements, and the rules that every response stream follows.

## Roles

Each node has one of five roles:

| Role | Sees | Does |
|---|---|---|
| source | the request and the response | Sends typed requests into the graph and receives response streams. A hosted source implements `Frontend`. A source can also be a programmatic entry point with no implementation. |
| operator | the request and the response | Calls the rest of the graph through `Next`, zero or more times, and returns a stream. |
| forward | the request only | Changes the request on its way in. |
| sink | the request | Produces the item stream. |
| backward | the response stream only | Changes the item stream on its way out. |

An operator and a source keep per-request state across both directions in one task. So each of them is placed whole, in one segment. A forward or backward node holds no state across directions, so a placement can put it anywhere.

## Hops

A hop is an outer end (a source or an operator), its forward nodes, an inner end (a sink or an operator), and its backward nodes. An operator is the inner end of one hop and the outer end of the next. The grammar is:

```text
Graph    := Source Hop
Hop      := Forward* Inner Backward*
Inner    := Sink | Operator Hop
```

The reference graph that the runtime tests use has two hops. Requests go right along the top line, and responses go left along the bottom line:

```text
A ----------> B --------> C -----> D
A <--- G <--- B <--- E <--- F <--- D
```

Hop 0 has outer end A (the source), inner end B (an operator), and backward node G. Hop 1 has outer end B, forward node C, inner end D (a sink), and backward nodes F, then E.

A stage is one position in a hop: a forward node, the inner end, or a backward node. A graph can have several source roots, and a sink can take requests from several roots through explicit connections. [Hosted frontends and service entries](../guides/frontends.md) describes those shapes. The graph syntax is in [Define a graph](../guides/define-a-graph.md).

## Role traits

A node implementation implements one role trait from the `gdl` crate. Each method returns `impl Future + Send`. On the local path, only a node's output stream and a streamed request's items are boxed.

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
pub trait Frontend: Build {
    type Req: Send + 'static; type Item: Send + 'static;
    fn serve<N>(self, next: N, shutdown: impl Future<Output = ()> + Send + 'static) -> impl Future<Output = Result<()>> + Send
    where N: Next<Self::Req, Self::Item>;
}
```

A returned stream is `'static`. It owns what it uses and cannot borrow `&self` or `&Cx`. Each call of `Next::generate` is one attempt. [Requests, attempts, and deadlines](requests.md) gives the attempt rules.

A generated segment binary builds each implementation through `Build`, from that node's section of the segment settings. `build` gets the node's configuration and its metrics, already registered. It must return quickly, because the segment registers with its deployment only after every node is built. `ready` resolves when the node can take work, for example when a model is loaded. It resolves at once by default. A node without metrics writes `type Metrics = ();`. [Write nodes](../guides/write-nodes.md) shows each trait in use, and [Segment lifecycle](lifecycle.md) shows where `build` and `ready` run.

## Streams and errors

- **Every response is a stream.** A unary response is a stream of one item.
- **Errors are terminal.** A stream item is `Result<T, gdl::Error>`. Every runtime boundary delivers the first `Err`, then ends the stream and cancels its upstream. A recoverable application error belongs inside `T`.
- **Success needs an explicit end.** A stream that crossed a segment ends successfully only if its producer finished it. Any other ending (the producer detached, was dropped, stopped responding, or the stream just ended) becomes a terminal `Err` of kind `Truncated`.
- **A node reports failure with an `Err` item.** Its context is how the runtime stops it. A node that cancels its own context must still end its stream, and the runtime reports that ending as `Cancelled`.

`gdl::Error` is serializable: a kind and a message. The kinds are `Node`, `Cancelled`, `DeadlineExceeded`, `Truncated`, `NotHosted` (placement checks prevent it), `Overloaded` ([Admission and egress](admission.md)), `Transport`, `Codec`, `Placement`, `Routing`, and `OutcomeUnknown` ([Routing, selectors, and offers](routing.md)). An error that arrives from a peer with a kind this build does not know reads as `Transport`. The rustdoc of `gdl::ErrorKind` describes each kind.
