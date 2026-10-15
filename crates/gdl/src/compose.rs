//! Local composition: the typed building blocks a graph is assembled from
//! when its stages share a segment. Everything here is a plain nested call;
//! there is no anchor, no map, and no lock on this path.

use std::future::Future;
use std::sync::Arc;

use crate::cx::Cx;
use crate::error::Result;
use crate::items::{Items, bound, fuse};
use crate::role::{Backward, Forward, Next, Operator, Sink};

/// A hop: a forward chain, an inner end, and a backward chain. As the `next`
/// of its outer end it is where attempts begin.
#[derive(Clone)]
pub struct Hop<F, I, B> {
    fwd: F,
    inner: I,
    bwd: B,
}

impl<F, I, B> Hop<F, I, B> {
    /// Assemble a hop.
    pub fn new(fwd: F, inner: I, bwd: B) -> Self {
        Self { fwd, inner, bwd }
    }
}

impl<Req, F, I, B> Next<Req, B::Out> for Hop<F, I, B>
where
    Req: Send + 'static,
    F: ForwardChain<Req>,
    I: Inner<F::Out>,
    B: BackwardChain<I::Item>,
{
    async fn generate(&self, cx: Cx, req: Req) -> Result<Items<B::Out>> {
        cx.check()?;
        let attempt = cx.child();
        // Dropping this future before it returns drops the guard and cancels
        // the attempt; on success the guard moves into the response stream,
        // which cancels the attempt when the stream ends or is dropped.
        let guard = attempt.cancel_on_drop();
        let run = async {
            let req = self.fwd.run(&attempt, req).await?;
            // A forward stage can finish in the same instant the attempt is
            // interrupted; the inner end must not start for a dead attempt.
            attempt.check()?;
            if B::EMPTY {
                // The inner end's output is the attempt's output; the empty
                // chain passes it through unchanged.
                let items = self.inner.call(attempt.clone(), req).await?;
                return Ok::<_, crate::Error>(self.bwd.apply(&attempt, items));
            }
            // The inner end runs in its own scope below the attempt, so a
            // terminal item from it stops it even while a backward node
            // still holds that item.
            let scope = attempt.child();
            let items = self.inner.call(scope.clone(), req).await?;
            Ok(self
                .bwd
                .apply(&attempt, fuse(items, scope.cancel_on_drop())))
        };
        let items = tokio::select! {
            biased;
            reason = attempt.done() => return Err(reason),
            result = run => result?,
        };
        // Success is reported only for a live attempt; dropping `items` on
        // the error path ends whatever the inner end started.
        attempt.check()?;
        Ok(bound(&attempt, guard, items))
    }
}

/// The inner end of a hop: a sink, or an operator together with its own hop.
pub trait Inner<Req>: Clone + Send + Sync + 'static {
    /// The item type the inner end produces.
    type Item: Send + 'static;

    /// Call the inner end within the hop's attempt.
    fn call(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Self::Item>>> + Send;
}

/// A sink as the inner end of a hop.
pub struct SinkStage<S>(Arc<S>);

impl<S> SinkStage<S> {
    /// Wrap a sink.
    pub fn new(sink: Arc<S>) -> Self {
        Self(sink)
    }
}

impl<S> Clone for SinkStage<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<S: Sink> Inner<S::Req> for SinkStage<S> {
    type Item = S::Item;

    async fn call(&self, cx: Cx, req: S::Req) -> Result<Items<S::Item>> {
        self.0.generate(cx, req).await
    }
}

/// An operator as the inner end of a hop, with the hop it calls as `next`.
pub struct OperatorStage<O, H> {
    op: Arc<O>,
    next: H,
}

impl<O, H> OperatorStage<O, H> {
    /// Wrap an operator and its downstream hop.
    pub fn new(op: Arc<O>, next: H) -> Self {
        Self { op, next }
    }
}

impl<O, H: Clone> Clone for OperatorStage<O, H> {
    fn clone(&self) -> Self {
        Self {
            op: Arc::clone(&self.op),
            next: self.next.clone(),
        }
    }
}

impl<O, H> Inner<O::UpReq> for OperatorStage<O, H>
where
    O: Operator,
    H: Next<O::DownReq, O::DownItem>,
{
    type Item = O::UpItem;

    async fn call(&self, cx: Cx, req: O::UpReq) -> Result<Items<O::UpItem>> {
        self.op.generate(cx, req, self.next.clone()).await
    }
}

/// Zero or more forward nodes, as a cons list: `()` is empty, `(Arc<F>, Rest)`
/// runs `F` and then `Rest`.
pub trait ForwardChain<In>: Clone + Send + Sync + 'static {
    /// The request type after the whole chain.
    type Out: Send + 'static;

    /// Run the chain on one request.
    fn run(&self, cx: &Cx, req: In) -> impl Future<Output = Result<Self::Out>> + Send;
}

impl<In: Send + 'static> ForwardChain<In> for () {
    type Out = In;

    async fn run(&self, _cx: &Cx, req: In) -> Result<In> {
        Ok(req)
    }
}

impl<F, R> ForwardChain<F::In> for (Arc<F>, R)
where
    F: Forward,
    R: ForwardChain<F::Out>,
{
    type Out = R::Out;

    async fn run(&self, cx: &Cx, req: F::In) -> Result<R::Out> {
        let req = self.0.forward(cx, req).await?;
        self.1.run(cx, req).await
    }
}

/// Zero or more backward nodes, as a cons list in the order the response
/// passes through them: `(Arc<B>, Rest)` applies `B` first.
///
/// Every node output is fused exactly once: each node but the last runs in
/// its own scope below the attempt and its output is fused against that
/// scope; the last node's output is fused by the hop's attempt boundary.
pub trait BackwardChain<In>: Clone + Send + Sync + 'static {
    /// The item type after the whole chain.
    type Out: Send + 'static;

    /// Whether the chain has no nodes.
    const EMPTY: bool;

    /// Wrap one response stream, already fused, in the chain. `attempt` is
    /// the hop's attempt context.
    fn apply(&self, attempt: &Cx, items: Items<In>) -> Items<Self::Out>;
}

impl<In: Send + 'static> BackwardChain<In> for () {
    type Out = In;

    const EMPTY: bool = true;

    fn apply(&self, _attempt: &Cx, items: Items<In>) -> Items<In> {
        items
    }
}

impl<B, R> BackwardChain<B::In> for (Arc<B>, R)
where
    B: Backward,
    R: BackwardChain<B::Out>,
{
    type Out = R::Out;

    const EMPTY: bool = false;

    fn apply(&self, attempt: &Cx, items: Items<B::In>) -> Items<R::Out> {
        if R::EMPTY {
            return self.1.apply(attempt, self.0.backward(attempt, items));
        }
        let scope = attempt.child();
        let out = self.0.backward(&scope, items);
        self.1.apply(attempt, fuse(out, scope.cancel_on_drop()))
    }
}

/// The entry point of a graph: the source and the hop below it.
#[derive(Clone)]
pub struct Source<H>(H);

impl<H> Source<H> {
    /// Wrap the source's hop.
    pub fn new(hop: H) -> Self {
        Self(hop)
    }

    /// Send one request. The response stream is bound to a child of `cx`:
    /// cancelling `cx` or dropping the stream ends the request.
    pub async fn call<Req, Item>(&self, cx: Cx, req: Req) -> Result<Items<Item>>
    where
        H: Next<Req, Item>,
    {
        self.0.generate(cx, req).await
    }
}
