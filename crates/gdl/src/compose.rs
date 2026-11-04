//! Local composition: the typed building blocks a graph is assembled from
//! when its stages share a segment. Everything here is a plain nested call;
//! there is no anchor and no map on this path.

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Poll;

use futures::Stream;

use crate::Cx;
use crate::Items;
use crate::Result;
use crate::cx::Scope;
use crate::items::{Fuse, Producer, bound, fuse};
use crate::{Around, Backward, Forward, Next, Operator, Sink};

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
    B: BackwardChain<I::Item, F::Stack>,
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
            // The inner end's scope, made before the race so that the race
            // watches it: every call the inner end makes without a scope of
            // its own runs under it, so its cancel is the request's. A child
            // sees its parent's cancel, so the race still sees the attempt's.
            let scope = attempt.child();
            let below = scope.cancel_token();
            // Built where it is pinned, so it is not moved there. The forward
            // chain hands the request on to the inner end, not back here: each
            // return would move the whole request once more.
            let at = &attempt;
            // What the chain's around nodes kept travels with the request, to
            // the backward chain of this attempt alone.
            let run = pin!(self.fwd.run(at, req, (), move |req, kept| async move {
                // A forward stage can finish in the same instant the attempt is
                // interrupted; the inner end must not start for a dead attempt.
                let attempt = at;
                attempt.check()?;
                // The inner end always runs in its own scope below the attempt:
                // a terminal item from it stops it even while a backward node
                // still holds that item, and a node that cancels its own context
                // gets the same outcome whether or not backward nodes follow.
                let producer = Producer::of(&scope);
                let items = self.inner.call(scope, req).await?;
                let items = producer.fused(items);
                // As before the inner end: no backward stage starts for an
                // attempt interrupted while the inner end was finishing.
                attempt.check()?;
                Ok(self.bwd.answer(attempt, items, kept))
            }));
            // The checks inside `run` give a cancel that came first its
            // precedence; the race only waits.
            let items = race_below(at, &below, run).await?;
            // Success is reported only for a live attempt; dropping `items` on
            // the error path ends whatever the inner end started.
            attempt.check()?;
            Ok(bound(&attempt, guard, items))
        }
    }

    // The same work under the caller's scope: no attempt, guard, race, or
    // boundary. The checks stay, so nothing starts for a dead request, but a
    // call that finds its request over ends quietly: the request's edge
    // reports why, once (docs/src/concepts/requests.md).
    #[allow(clippy::manual_async_fn)]
    fn pass(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<B::Out>>> + Send {
        async move {
            tokio::task::coop::consume_budget().await;
            if cx.check().is_err() {
                return Ok(crate::items::quiet());
            }
            let at = &cx;
            let run = self.fwd.run(at, req, (), move |req, kept| async move {
                if !F::EMPTY {
                    at.check()?;
                }
                let items = self.inner.call(at.clone(), req).await?;
                Ok(self.bwd.pass(at, items, kept))
            });
            match run.await {
                Err(_) if cx.check().is_err() => Ok(crate::items::quiet()),
                run => run,
            }
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
        let next = Called::of::<O>(self.next.clone(), &cx);
        boxed(|| self.op.generate(cx, req, next))
    }
}

/// The `next` an operator receives: its hop, and the context the operator
/// was called with when it did not ask for a scope per call. A call with
/// that context unchanged passes through, under the request scope; a call
/// with a child scope or another deadline gets the hop's own attempt, as
/// every call does when the operator sets `CALL_SCOPE`.
#[doc(hidden)]
pub struct Called<H> {
    next: H,
    own: Option<(Scope, Option<crate::Deadline>)>,
}

impl<H> Called<H> {
    /// `next` for one call of operator `O` under `cx`.
    pub fn of<O: Operator>(next: H, cx: &Cx) -> Self {
        let own = (!O::CALL_SCOPE).then(|| (cx.cancel_token(), cx.deadline()));
        // Every call scoped, as before the request scope: the suite run so
        // fails exactly where the two forms differ (docs/src/development/testing.md).
        #[cfg(gdl_scoped_calls)]
        let own = own.and(None);
        Self { next, own }
    }
}

impl<H: Clone> Clone for Called<H> {
    fn clone(&self) -> Self {
        Self {
            next: self.next.clone(),
            own: self.own.clone(),
        }
    }
}

impl<Req, Item, H> Next<Req, Item> for Called<H>
where
    Req: Send + 'static,
    Item: Send + 'static,
    H: Next<Req, Item>,
{
    fn generate(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Item>>> + Send {
        use futures::future::Either;
        match &self.own {
            Some((scope, deadline)) if cx.scope_is(scope) && cx.deadline() == *deadline => {
                Either::Left(self.next.pass(cx, req))
            }
            // Boxed: the pass-through arm is the common one, and an unboxed
            // scoped arm sized every operator's boxed future for it, past the
            // allocator's small-size cache (docs/src/operations/feature-costs.md).
            // Built in its box, as an operator's future is.
            _ => Either::Right(boxed(|| self.next.generate(cx, req))),
        }
    }
}

/// Zero or more forward nodes, as a cons list: `()` is empty, `(Arc<F>, Rest)`
/// runs `F` and then `Rest`, and `(ForwardHalf<P>, Rest)` runs the forward
/// half of an [`Around`] node `P` and then `Rest`.
///
/// `Below` is the stack of values that the around nodes before this part of
/// the chain kept. Each forward half pushes its value on it, so the last one
/// is on top, where the first backward half takes it.
pub trait ForwardChain<In, Below = ()>: Clone + Send + Sync + 'static {
    /// The request type after the whole chain.
    type Out: Send + 'static;
    /// Whether the chain has no nodes.
    #[doc(hidden)]
    const EMPTY: bool = false;
    /// `Below` with the value of each around node in the chain pushed on.
    type Stack: Send + 'static;

    /// Run the chain on one request, then `then` on the request it makes
    /// and the values its around nodes kept.
    ///
    /// The request goes on to `then` rather than back to the caller: each
    /// future that returned it would move the whole request once more.
    fn run<T, K, Fut>(
        &self,
        cx: &Cx,
        req: In,
        below: Below,
        then: K,
    ) -> impl Future<Output = Result<T>> + Send
    where
        K: FnOnce(Self::Out, Self::Stack) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send;
}

impl<In: Send + 'static, Below: Send + 'static> ForwardChain<In, Below> for () {
    type Out = In;
    const EMPTY: bool = true;
    type Stack = Below;

    fn run<T, K, Fut>(
        &self,
        _cx: &Cx,
        req: In,
        below: Below,
        then: K,
    ) -> impl Future<Output = Result<T>> + Send
    where
        K: FnOnce(In, Below) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        then(req, below)
    }
}

impl<F, R, Below> ForwardChain<F::In, Below> for (Arc<F>, R)
where
    F: Forward,
    R: ForwardChain<F::Out, Below>,
    Below: Send + 'static,
{
    type Out = R::Out;
    type Stack = R::Stack;

    // As in `Hop::generate`: a block that uses `req` where it was captured,
    // and a match where `?` would move the request again.
    #[allow(clippy::manual_async_fn)]
    fn run<T, K, Fut>(
        &self,
        cx: &Cx,
        req: F::In,
        below: Below,
        then: K,
    ) -> impl Future<Output = Result<T>> + Send
    where
        K: FnOnce(R::Out, R::Stack) -> Fut + Send,
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
            self.1.run(cx, req, below, then).await
        }
    }
}

/// The forward half of an [`Around`] node, for a hop's forward chain. Its
/// backward half, [`BackwardHalf`], goes in the same hop's backward chain;
/// [`around`] makes both.
pub struct ForwardHalf<P>(Arc<P>);

/// The backward half of an [`Around`] node, for a hop's backward chain.
pub struct BackwardHalf<P>(Arc<P>);

impl<P> Clone for ForwardHalf<P> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<P> Clone for BackwardHalf<P> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// Both halves of one [`Around`] node: the first for a hop's forward chain,
/// the second for the same hop's backward chain.
///
/// The halves pair by nesting, as brackets do: the backward chain meets the
/// halves in the reverse of the order the forward chain does. A half with no
/// partner, or two pairs of different node types that cross, do not
/// compile. Two nodes of the same type that cross are not caught.
pub fn around<P: Around>(node: Arc<P>) -> (ForwardHalf<P>, BackwardHalf<P>) {
    (ForwardHalf(Arc::clone(&node)), BackwardHalf(node))
}

/// The value one [`Around`] node kept for its backward half, on a hop's
/// stack. Tagged with the node's type, so that two nodes whose values have
/// one type still cannot take each other's.
pub struct Kept<P: Around>(P::State);

impl<P, R, Below> ForwardChain<P::UpReq, Below> for (ForwardHalf<P>, R)
where
    P: Around,
    R: ForwardChain<P::DownReq, (Kept<P>, Below)>,
    Below: Send + 'static,
{
    type Out = R::Out;
    type Stack = R::Stack;

    // As for a forward node.
    #[allow(clippy::manual_async_fn)]
    fn run<T, K, Fut>(
        &self,
        cx: &Cx,
        req: P::UpReq,
        below: Below,
        then: K,
    ) -> impl Future<Output = Result<T>> + Send
    where
        K: FnOnce(R::Out, R::Stack) -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        async move {
            cx.check()?;
            let (req, state) = match self.0.0.forward(cx, req).await {
                Ok(done) => done,
                Err(error) => return Err(error),
            };
            self.1.run(cx, req, (Kept(state), below), then).await
        }
    }
}

/// Zero or more backward nodes, as a cons list in the order the response
/// passes through them: `(Arc<B>, Rest)` applies `B` first, and
/// `(BackwardHalf<P>, Rest)` applies the backward half of an [`Around`] node
/// `P` first, with the value on top of `Stack`.
///
/// In an attempt, every node output is fused exactly once: each node but
/// the last runs in its own scope below the attempt and its output is fused
/// against that scope; the last node's output is fused by the hop's
/// attempt boundary. Under the request scope, no output is fused
/// ([`BackwardChain::pass`]).
///
/// The empty chain takes only an empty stack, so a hop whose around halves
/// do not pair is not a `Next`.
pub trait BackwardChain<In, Stack = ()>: Clone + Send + Sync + 'static {
    /// The item type after the whole chain.
    type Out: Send + 'static;

    /// Whether the chain has no nodes.
    const EMPTY: bool;

    /// Wrap one response stream, already fused, in the chain. `attempt` is
    /// the hop's attempt context; `stack` holds what the forward chain's
    /// around nodes kept.
    fn apply(&self, attempt: &Cx, items: Items<In>, stack: Stack) -> Items<Self::Out>;

    /// The stream the hop's attempt boundary takes: the chain's, or, with no
    /// nodes, the inner end's node boundary itself, unboxed. Every chain in
    /// the crate implements it; it is not part of the interface.
    #[doc(hidden)]
    type Answer: Stream<Item = Result<Self::Out>> + Unpin + Send + 'static;

    /// The chain over the inner end's node boundary, for the hop's attempt
    /// boundary.
    #[doc(hidden)]
    fn answer(&self, attempt: &Cx, fused: Fuse<In>, stack: Stack) -> Self::Answer;

    /// The chain under its caller's scope: every node gets `cx`, no node's
    /// stream is fused, and a node that filters gets its input behind a
    /// yield. A request that is over builds no further node.
    #[doc(hidden)]
    fn pass(&self, cx: &Cx, items: Items<In>, stack: Stack) -> Items<Self::Out>;
}

impl<In: Send + 'static> BackwardChain<In, ()> for () {
    type Out = In;

    const EMPTY: bool = true;

    fn apply(&self, _attempt: &Cx, items: Items<In>, (): ()) -> Items<In> {
        items
    }

    // Nothing comes between the node boundary and the attempt boundary, so
    // the attempt boundary holds it as it is: one box fewer per request, and
    // one dynamic poll fewer per item (docs/src/development/runtime-design.md).
    type Answer = Fuse<In>;

    fn answer(&self, _attempt: &Cx, fused: Fuse<In>, (): ()) -> Fuse<In> {
        fused
    }

    fn pass(&self, _cx: &Cx, items: Items<In>, (): ()) -> Items<In> {
        items
    }
}

impl<B, R, S> BackwardChain<B::In, S> for (Arc<B>, R)
where
    B: Backward,
    R: BackwardChain<B::Out, S>,
    S: Send + 'static,
{
    type Out = R::Out;

    const EMPTY: bool = false;

    fn apply(&self, attempt: &Cx, items: Items<B::In>, stack: S) -> Items<R::Out> {
        // A backward node's constructor may have side effects; an interrupted
        // attempt skips every remaining node and carries the reason through.
        if let Err(reason) = attempt.check() {
            return self.1.apply(attempt, crate::items::failed(reason), stack);
        }
        if R::EMPTY {
            return self
                .1
                .apply(attempt, self.0.backward(attempt, items), stack);
        }
        let scope = attempt.child();
        let out = self.0.backward(&scope, items);
        self.1.apply(attempt, fuse(out, &scope), stack)
    }

    type Answer = Items<R::Out>;

    fn answer(&self, attempt: &Cx, fused: Fuse<B::In>, stack: S) -> Items<R::Out> {
        self.apply(attempt, Box::pin(fused), stack)
    }

    fn pass(&self, cx: &Cx, items: Items<B::In>, stack: S) -> Items<R::Out> {
        // As in `apply`: a constructor may have side effects, and none runs
        // for a request that is over; the rest of the chain ends quietly.
        if cx.check().is_err() {
            return self.1.pass(cx, crate::items::quiet(), stack);
        }
        let items = if B::FILTERS {
            crate::items::yielding(items)
        } else {
            items
        };
        self.1.pass(cx, self.0.backward(cx, items), stack)
    }
}

impl<P, R, Rest> BackwardChain<P::DownItem, (Kept<P>, Rest)> for (BackwardHalf<P>, R)
where
    P: Around,
    R: BackwardChain<P::UpItem, Rest>,
    Rest: Send + 'static,
{
    type Out = R::Out;

    const EMPTY: bool = false;

    // The contexts are a backward node's: the last node of the chain gets
    // the attempt itself, so its stop reaches the inner end, and the others
    // a scope of their own.
    fn apply(
        &self,
        attempt: &Cx,
        items: Items<P::DownItem>,
        (Kept(state), rest): (Kept<P>, Rest),
    ) -> Items<R::Out> {
        // Skipped, as a backward node is: the value is dropped here, and the
        // node never sees it.
        if let Err(reason) = attempt.check() {
            drop(state);
            return self.1.apply(attempt, crate::items::failed(reason), rest);
        }
        if R::EMPTY {
            let out = self.0.0.backward(attempt, state, items);
            return self.1.apply(attempt, out, rest);
        }
        let scope = attempt.child();
        let out = self.0.0.backward(&scope, state, items);
        self.1.apply(attempt, fuse(out, &scope), rest)
    }

    type Answer = Items<R::Out>;

    fn answer(
        &self,
        attempt: &Cx,
        fused: Fuse<P::DownItem>,
        stack: (Kept<P>, Rest),
    ) -> Items<R::Out> {
        self.apply(attempt, Box::pin(fused), stack)
    }

    fn pass(
        &self,
        cx: &Cx,
        items: Items<P::DownItem>,
        (Kept(state), rest): (Kept<P>, Rest),
    ) -> Items<R::Out> {
        if cx.check().is_err() {
            drop(state);
            return self.1.pass(cx, crate::items::quiet(), rest);
        }
        let items = if P::FILTERS {
            crate::items::yielding(items)
        } else {
            items
        };
        self.1.pass(cx, self.0.0.backward(cx, state, items), rest)
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

    // A source nested as an operator's `next` is its hop: a hand-built
    // graph then gets what the generated one does.
    fn pass(&self, cx: Cx, req: Req) -> impl Future<Output = Result<Items<Item>>> + Send {
        self.0.pass(cx, req)
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
#[doc(hidden)]
#[inline(always)]
pub fn boxed<F: Future>(make: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
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

/// Run an attempt's work against the attempt's deadline and the cancel of
/// `below`: the scope the attempt hands the stage under it, the request
/// scope that every call below shares unless it has a scope of its own. A
/// child sees its parent's cancel, so the race sees the attempt's too.
///
/// The first pass polls `run` before the cancel, so an attempt that
/// finishes at once registers no waiter. Later passes poll the cancel
/// first, and no pass polls `run` twice. As in `tokio::select!`, a pass
/// with the task's budget spent yields at once: the cancel would yield for
/// it, and `run`, polled alone, could finish a cancelled attempt.
///
/// The race alone gives a cancel no precedence over the first poll of
/// `run`: a cancel made just before it still lets `run` take one step. So
/// `run` must check `attempt` before it acts, as a hop's forward chain and
/// inner end do. `run` is pinned by the caller, where it was built: moved
/// in here, a hop's future would copy its request once more.
#[doc(hidden)]
pub async fn race_below<T, F>(attempt: &Cx, below: &Scope, mut run: Pin<&mut F>) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let mut done = pin!(attempt.done_below(below));
    let mut first = true;
    poll_fn(|task| {
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
    .await
}
