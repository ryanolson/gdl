//! Local composition: the typed building blocks a graph is assembled from
//! when its stages share a segment. Everything here is a plain nested call;
//! there is no anchor and no map on this path.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use crate::Cx;
use crate::Items;
use crate::Result;
use crate::items::{Producer, bound, fuse};
use crate::{Backward, Forward, Next, Operator, Sink};

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
    // A block, not an `async fn`: the block uses `req` where it was
    // captured, where an `async fn` would move it into a local first.
    #[allow(clippy::manual_async_fn)]
    fn generate(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<B::Out>>> + Send {
        async move {
            // Every attempt spends cooperative budget: an operator retrying
            // attempts that fail at once would otherwise loop without ever
            // yielding, starving the task that would cancel it.
            tokio::task::coop::consume_budget().await;
            cx.check()?;
            let attempt = cx.child();
            // Dropping this future before it returns drops the guard and cancels
            // the attempt; on success the guard moves into the response stream,
            // which cancels the attempt when the stream ends or is dropped.
            let guard = attempt.cancel_on_drop();
            // Built where it is pinned, so it is not moved there. The forward
            // chain hands the request on to the inner end, not back here: each
            // return would move the whole request once more.
            let at = &attempt;
            let mut run = pin!(self.fwd.run(at, req, move |req| async move {
                // A forward stage can finish in the same instant the attempt is
                // interrupted; the inner end must not start for a dead attempt.
                let attempt = at;
                attempt.check()?;
                // The inner end always runs in its own scope below the attempt:
                // a terminal item from it stops it even while a backward node
                // still holds that item, and a node that cancels its own context
                // gets the same outcome whether or not backward nodes follow.
                let scope = attempt.child();
                let producer = Producer::of(&scope);
                let items = self.inner.call(scope, req).await?;
                let items = producer.fuse(items);
                // As before the inner end: no backward stage starts for an
                // attempt interrupted while the inner end was finishing.
                attempt.check()?;
                Ok(self.bwd.apply(attempt, items))
            }));
            let mut done = pin!(attempt.done());
            // The first pass polls `run` before the cancel, so an attempt that
            // finishes at once never registers a waiter (two more locks of the
            // attempt's token); the checks inside `run` and below still see a
            // cancel that came first. Later passes poll the cancel first, and no
            // pass polls `run` twice. As in `tokio::select!`, a pass with the
            // task's budget spent yields at once: the cancel would yield for it
            // and `run`, polled alone, could finish a cancelled attempt.
            let mut first = true;
            let items = poll_fn(|task| {
                if !tokio::task::coop::has_budget_remaining() {
                    task.waker().wake_by_ref();
                    return Poll::Pending;
                }
                if !first && let Poll::Ready(reason) = done.as_mut().poll(task) {
                    return Poll::Ready(Err(reason));
                }
                if let Poll::Ready(result) = run.as_mut().poll(task) {
                    return Poll::Ready(result);
                }
                if std::mem::take(&mut first)
                    && let Poll::Ready(reason) = done.as_mut().poll(task)
                {
                    return Poll::Ready(Err(reason));
                }
                Poll::Pending
            })
            .await?;
            // Success is reported only for a live attempt; dropping `items` on
            // the error path ends whatever the inner end started.
            attempt.check()?;
            Ok(bound(&attempt, guard, items))
        }
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

    fn call(&self, cx: Cx, req: S::Req) -> impl Future<Output = Result<Items<S::Item>>> + Send {
        self.0.generate(cx, req)
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

    fn call(&self, cx: Cx, req: O::UpReq) -> impl Future<Output = Result<Items<O::UpItem>>> + Send {
        // The operator's future holds the future of the hop it calls, and
        // that one the hops below it. Held inline, the hop above would carry
        // all of them, and a future is moved whole: one allocation here costs
        // less than the moves it saves (docs/src/operations/feature-costs.md).
        // Cloned first: a clone that writes into the future as it is built
        // keeps the compiler from building it in place.
        let next = self.next.clone();
        boxed(|| self.op.generate(cx, req, next))
    }
}

/// Zero or more forward nodes, as a cons list: `()` is empty, `(Arc<F>, Rest)`
/// runs `F` and then `Rest`.
pub trait ForwardChain<In>: Clone + Send + Sync + 'static {
    /// The request type after the whole chain.
    type Out: Send + 'static;

    /// Run the chain on one request, then `then` on the request it makes.
    ///
    /// The request goes on to `then` rather than back to the caller: each
    /// future that returned it would move the whole request once more.
    fn run<T, K, Fut>(&self, cx: &Cx, req: In, then: K) -> impl Future<Output = Result<T>> + Send
    where
        K: FnOnce(Self::Out) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send;
}

impl<In: Send + 'static> ForwardChain<In> for () {
    type Out = In;

    fn run<T, K, Fut>(&self, _cx: &Cx, req: In, then: K) -> impl Future<Output = Result<T>> + Send
    where
        K: FnOnce(In) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        then(req)
    }
}

impl<F, R> ForwardChain<F::In> for (Arc<F>, R)
where
    F: Forward,
    R: ForwardChain<F::Out>,
{
    type Out = R::Out;

    // As in `Hop::generate`: a block that uses `req` where it was captured,
    // and a match where `?` would move the request again.
    #[allow(clippy::manual_async_fn)]
    fn run<T, K, Fut>(&self, cx: &Cx, req: F::In, then: K) -> impl Future<Output = Result<T>> + Send
    where
        K: FnOnce(R::Out) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        async move {
            // Each forward stage may have side effects; none starts for an
            // attempt that an earlier stage, or anything else, has
            // interrupted.
            cx.check()?;
            let req = match self.0.forward(cx, req).await {
                Ok(req) => req,
                Err(error) => return Err(error),
            };
            self.1.run(cx, req, then).await
        }
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
        // A backward node's constructor may have side effects; an interrupted
        // attempt skips every remaining node and carries the reason through.
        if let Err(reason) = attempt.check() {
            return self.1.apply(attempt, crate::items::failed(reason));
        }
        if R::EMPTY {
            return self.1.apply(attempt, self.0.backward(attempt, items));
        }
        let scope = attempt.child();
        let out = self.0.backward(&scope, items);
        self.1.apply(attempt, fuse(out, &scope))
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
    ///
    /// The future is the hop's own: a future around it would be moved whole,
    /// with the hop's inside it, each time it is awaited.
    pub fn call<Req, Item>(
        &self,
        cx: Cx,
        req: Req,
    ) -> impl Future<Output = Result<Items<Item>>> + Send
    where
        H: Next<Req, Item>,
    {
        self.0.generate(cx, req)
    }
}

impl<H, Req, Item> Next<Req, Item> for Source<H>
where
    H: Next<Req, Item>,
    Req: Send + 'static,
    Item: Send + 'static,
{
    fn generate(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Item>>> + Send {
        self.call(cx, req)
    }
}

/// A typed join from an entry arm into a shared pipeline.
///
/// The pipeline remains concrete here; type erasure is confined to the service client.
pub struct Join<N, Item> {
    next: N,
    item: std::marker::PhantomData<fn() -> Item>,
}
impl<N, Item> Join<N, Item> {
    /// Join an existing pipeline, usually held in an `Arc` shared by the entries.
    pub fn new(next: N) -> Self {
        Self {
            next,
            item: std::marker::PhantomData,
        }
    }
}
impl<N: Clone, Item> Clone for Join<N, Item> {
    fn clone(&self) -> Self {
        Self::new(self.next.clone())
    }
}
impl<Req: Send + 'static, Item: Send + 'static, N: Next<Req, Item>> Inner<Req> for Join<N, Item> {
    type Item = Item;
    fn call(&self, cx: Cx, request: Req) -> impl Future<Output = Result<Items<Item>>> + Send {
        // On the heap for the reason `OperatorStage::call` gives.
        boxed(|| self.next.generate(cx, request))
    }
}

/// A future on the heap, built there. `Box::pin(make())` builds the future
/// on the stack and then copies it whole into its box. Here the allocation
/// comes first, and `build` writes the future into it.
#[inline(always)]
fn boxed<F: Future>(make: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    let slot = Box::new_uninit();
    Box::into_pin(Box::write(slot, build(make)))
}

/// Out of line on purpose: the compiler then passes the allocation to it as
/// the place to return into. Inlined, a default release build (16 codegen
/// units) still built the future on the stack and copied it
/// (`tests/codegen.rs`, `an_operators_future_is_built_on_the_heap`).
#[inline(never)]
fn build<F>(make: impl FnOnce() -> F) -> F {
    make()
}
