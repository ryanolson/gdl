use std::future::Future;

use serde::de::DeserializeOwned;

use crate::cx::Cx;
use crate::error::Result;
use crate::items::Items;

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

    /// The node this slot stands in for, when it is the `Absent`
    /// placeholder: a segment refuses to assemble with a placeholder in a
    /// slot it hosts, so the placeholder's methods are never called.
    #[doc(hidden)]
    const ABSENT: Option<&'static str> = None;

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

    /// The node this slot stands in for, when it is the `Absent`
    /// placeholder: a segment refuses to assemble with a placeholder in a
    /// slot it hosts, so the placeholder's methods are never called.
    #[doc(hidden)]
    const ABSENT: Option<&'static str> = None;

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

    /// The node this slot stands in for, when it is the `Absent`
    /// placeholder: a segment refuses to assemble with a placeholder in a
    /// slot it hosts, so the placeholder's methods are never called.
    #[doc(hidden)]
    const ABSENT: Option<&'static str> = None;

    /// Transform one response stream. The returned stream must own what it
    /// uses: it outlives this call.
    fn backward(&self, cx: &Cx, input: Items<Self::In>) -> Items<Self::Out>;
}

/// The downstream part of a graph as an operator sees it.
///
/// Each call is one attempt with its own cancellation scope below `cx`.
/// Dropping the returned stream, or the pending future, cancels that attempt
/// only. A `Next` may be cloned, kept, and called again (retry, migration);
/// once `cx` is cancelled or past its deadline, calls fail at once.
pub trait Next<Req, Item>: Clone + Send + Sync + 'static {
    /// Start one attempt.
    fn generate(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Item>>> + Send;
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

    /// The node this slot stands in for, when it is the `Absent`
    /// placeholder: a segment refuses to assemble with a placeholder in a
    /// slot it hosts, so the placeholder's methods are never called.
    #[doc(hidden)]
    const ABSENT: Option<&'static str> = None;

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
/// node's section of the segment configuration.
pub trait Build: Sized + Send + Sync + 'static {
    /// The configuration section.
    type Config: DeserializeOwned + Send;

    /// Construct the implementation.
    fn build(config: Self::Config) -> impl Future<Output = Result<Self>> + Send;
}
