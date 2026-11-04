use std::future::Future;

use serde::de::DeserializeOwned;

use crate::cx::Cx;
use crate::error::Result;
use crate::items::Items;

/// A hosted entry point: an HTTP, gRPC, or other service that calls a graph.
///
/// `Build` prepares the service (including binding its listener). `serve`
/// starts accepting requests once the graph is ready. Each external request
/// gets a fresh `Cx`; dropping its response stream cancels that request.
/// The shutdown future stops admission and asks the service to finish. The
/// implementation owns its protocol's draining policy and returns any failure.
/// Graph responses remain streams even when a protocol collects one response.
pub trait Frontend: Build {
    /// The request sent into the graph, optionally `Streamed<Head, Item>`.
    type Req: Send + 'static;
    /// The response item received from the graph.
    type Item: Send + 'static;

    /// Serve external requests until shutdown or a service error.
    fn serve<N>(
        self,
        next: N,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> impl Future<Output = Result<()>> + Send
    where
        N: Next<Self::Req, Self::Item>;
}

/// The inner end of a graph: takes a request, returns a response stream.
///
/// The context each node receives is how the runtime stops the node: when it
/// is cancelled or asked to stop, the node should end its work and its
/// stream. A node reports a failure by emitting an `Err` item, not by
/// cancelling its context; a node that cancels its own context must still
/// end its stream, and the runtime reports that ending as `Cancelled`.
pub trait Sink: Send + Sync + 'static {
    /// The request type.
    type Req: Send + 'static;
    /// The response item type.
    type Item: Send + 'static;

    /// Whether this is the `Absent` placeholder: a segment refuses to
    /// assemble with a placeholder in a slot it hosts, so the placeholder's
    /// methods are never called.
    #[doc(hidden)]
    const ABSENT: bool = false;

    /// Handle one request.
    fn generate(
        &self,
        cx: Cx,
        req: Self::Req,
    ) -> impl Future<Output = Result<Items<Self::Item>>> + Send;
}

/// A forward edge node: transforms the request on its way in. It sees nothing
/// of the response.
pub trait Forward: Send + Sync + 'static {
    /// The request type it receives.
    type In: Send + 'static;
    /// The request type it passes on.
    type Out: Send + 'static;

    /// Whether this is the `Absent` placeholder: a segment refuses to
    /// assemble with a placeholder in a slot it hosts, so the placeholder's
    /// methods are never called.
    #[doc(hidden)]
    const ABSENT: bool = false;

    /// Transform one request.
    fn forward(&self, cx: &Cx, req: Self::In) -> impl Future<Output = Result<Self::Out>> + Send;
}

/// A backward edge node: transforms the response stream on its way out. It
/// sees nothing of the request.
pub trait Backward: Send + Sync + 'static {
    /// The item type it receives.
    type In: Send + 'static;
    /// The item type it passes on.
    type Out: Send + 'static;

    /// Whether this is the `Absent` placeholder: a segment refuses to
    /// assemble with a placeholder in a slot it hosts, so the placeholder's
    /// methods are never called.
    #[doc(hidden)]
    const ABSENT: bool = false;

    /// Whether the node can take items without passing one on (a filter, a
    /// batcher). Such a node loops over its input inside one poll; under the
    /// request scope nothing else below the request's edge returns to it,
    /// so its input then yields every 1,024 ready items, and a cancel or a
    /// client's detach reaches the edge. A node that passes each item on
    /// leaves it `false` and pays nothing.
    const FILTERS: bool = false;

    /// Transform one response stream. The returned stream must own what it
    /// uses: it outlives this call.
    fn backward(&self, cx: &Cx, input: Items<Self::In>) -> Items<Self::Out>;
}

/// A node with a forward half and a backward half in one hop. The forward
/// half changes the request and keeps a value; the backward half of the same
/// attempt receives that value with the response stream.
///
/// It is an operator that calls the rest of the hop exactly once, folded
/// into the hop: it costs what a forward and a backward node cost, not an
/// operator's hop of its own. Each call runs `forward` once and then,
/// unless the call ends first, `backward` once, with the value that call's
/// `forward` kept. A retry is a new call with a new value.
///
/// An `Err` from `forward` is the call's error. A failure in `backward`
/// is an error item in the stream, so a check that only needs the request
/// belongs in `forward`.
pub trait Around: Send + Sync + 'static {
    /// The request it receives from the outer end's side.
    type UpReq: Send + 'static;
    /// The item type it passes on toward the outer end.
    type UpItem: Send + 'static;
    /// The request it passes on toward the inner end.
    type DownReq: Send + 'static;
    /// The item type it receives from the inner end's side.
    type DownItem: Send + 'static;
    /// What the forward half keeps for the backward half of the same
    /// attempt. It moves a few times on each request, so a large value is
    /// better behind a box the node makes.
    type State: Send + 'static;

    /// Whether the node can take items without passing one on (a filter, a
    /// batcher). Such a node loops over its input inside one poll; under the
    /// request scope nothing else below the request's edge returns to it,
    /// so its input then yields every 1,024 ready items, and a cancel or a
    /// client's detach reaches the edge. A node that passes each item on
    /// leaves it `false` and pays nothing.
    const FILTERS: bool = false;

    /// Whether this is the `Absent` placeholder: a segment refuses to
    /// assemble with a placeholder in a slot it hosts, so the placeholder's
    /// methods are never called.
    #[doc(hidden)]
    const ABSENT: bool = false;

    /// Transform one request, and keep a value for this attempt's backward
    /// half.
    fn forward(
        &self,
        cx: &Cx,
        req: Self::UpReq,
    ) -> impl Future<Output = Result<(Self::DownReq, Self::State)>> + Send;

    /// Transform this attempt's response stream. `state` is the value this
    /// attempt's `forward` kept. The returned stream must own what it uses:
    /// it outlives this call.
    fn backward(
        &self,
        cx: &Cx,
        state: Self::State,
        input: Items<Self::DownItem>,
    ) -> Items<Self::UpItem>;
}

/// A unary service: one request in, one response out. It is not in the
/// topology. A node calls it through a [`UnaryClient`](crate::UnaryClient),
/// whose call costs about what a direct call costs when the service is in
/// the caller's segment.
///
/// A sync service writes an `async fn` with no `.await`: its future is
/// ready at the first poll. A future of up to 256 bytes, aligned to 8 or
/// less, is built inside the call's own future, so a local call makes no
/// allocation of its own; a larger future is boxed.
pub trait Unary: Send + Sync + 'static {
    /// The request type.
    type Req: Send + 'static;
    /// The response type.
    type Resp: Send + 'static;

    /// Whether this is the `Absent` placeholder: a segment refuses to
    /// assemble with a placeholder in a slot it hosts, so the placeholder's
    /// methods are never called.
    #[doc(hidden)]
    const ABSENT: bool = false;

    /// Answer one request.
    fn call(&self, cx: &Cx, req: Self::Req) -> impl Future<Output = Result<Self::Resp>> + Send;
}

/// The downstream part of a graph as an operator sees it.
///
/// A call made with the operator's own context runs under the request's
/// scope: the hop below adds no scope of its own, its nodes share the
/// request's, and a call that finds its request over ends quietly, with an
/// empty stream, while the request's edge reports why. A call is an attempt,
/// with its own cancellation scope below `cx`, when the operator sets
/// [`Operator::CALL_SCOPE`], when it calls with a context it derived (a
/// child scope or another deadline), or when the hop below is remote; once
/// its `cx` is cancelled or past its deadline, it fails at once. Either way,
/// dropping the returned stream, or the pending future, ends that call, and
/// a `Next` may be cloned, kept, and called again (retry, migration). See
/// docs/src/concepts/requests.md.
pub trait Next<Req, Item>: Clone + Send + Sync + 'static {
    /// Start one call.
    fn generate(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Item>>> + Send;

    /// Start one call under the caller's own scope: no scope, guard, race,
    /// or boundary of its own. A hop overrides it; any other `Next` calls
    /// `generate`.
    #[doc(hidden)]
    fn pass(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Item>>> + Send {
        self.generate(cx, req)
    }
}

/// A node that sees both the request and the response: it receives the
/// upstream request, calls the downstream through `next` zero or more times,
/// and returns the upstream response stream.
pub trait Operator: Send + Sync + 'static {
    /// The request it receives from upstream.
    type UpReq: Send + 'static;
    /// The item type it returns upstream.
    type UpItem: Send + 'static;
    /// The request it sends downstream.
    type DownReq: Send + 'static;
    /// The item type it receives from downstream.
    type DownItem: Send + 'static;

    /// Whether this is the `Absent` placeholder: a segment refuses to
    /// assemble with a placeholder in a slot it hosts, so the placeholder's
    /// methods are never called.
    #[doc(hidden)]
    const ABSENT: bool = false;

    /// Whether each call of `next` is an attempt, with a scope of its own:
    /// a cancel of that call alone, which its nodes see at once; checks of
    /// its cancel and deadline every 64 items; its own `Cancelled`, rather
    /// than a quiet end, when its request is cancelled; and a clean end
    /// after a cancel reported as `Cancelled`. Without it, a call of `next`
    /// with the operator's own context runs under the request's scope, which
    /// costs about 49 Ir less per item and 1,600 Ir and three allocations
    /// less per request (docs/src/operations/feature-costs.md).
    ///
    /// Set it if the operator hedges or abandons calls and expects the
    /// dropped call's nodes to see a cancel, turns its `next`'s end into
    /// output and must tell a cancel from a clean end, filters or batches
    /// an always-ready local source, or calls `next` from a task it spawned.
    /// A call with a context the operator derived is an attempt whatever
    /// this says.
    const CALL_SCOPE: bool = false;

    /// Handle one upstream request.
    fn generate<N>(
        &self,
        cx: Cx,
        req: Self::UpReq,
        next: N,
    ) -> impl Future<Output = Result<Items<Self::UpItem>>> + Send
    where
        N: Next<Self::DownReq, Self::DownItem>;
}

/// How a generated segment binary constructs a node implementation from that
/// node's section of the segment configuration, and the metrics it reports.
///
/// A node without metrics writes `type Metrics = ();` and takes `(): ()`:
/// associated type defaults are not stable Rust, and a second trait for
/// nodes with metrics would be a second way to build a node.
pub trait Build: Sized + Send + Sync + 'static {
    /// The configuration section.
    type Config: DeserializeOwned + Send;
    /// The node's own metrics, a type that derives [`Metrics`](crate::Metrics),
    /// or `()`.
    type Metrics: crate::Metrics;

    /// Construct the implementation from its configuration and its metrics,
    /// already registered with the segment process's registry. Return
    /// quickly: the segment registers with its deployment once every node
    /// is built, so that the deployment knows the process is live while the
    /// nodes get ready.
    fn build(
        config: Self::Config,
        metrics: Self::Metrics,
    ) -> impl Future<Output = Result<Self>> + Send;

    /// Resolve once the implementation can take work: a property is loaded, a
    /// connection is open. The segment announces itself as loaded, and takes
    /// requests, only after every node it hosts has. Ready at once by
    /// default.
    fn ready(&self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}
