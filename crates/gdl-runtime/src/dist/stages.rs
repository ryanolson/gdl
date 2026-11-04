//! Typed stages of a distributed hop, and the walks over them.
//!
//! A walk applies stages while they belong to the current run, and leaves at
//! the first stage that does not, encoding the value or stream with the codec
//! of the edge it leaves on. A streamed request leaves as its head's bytes and
//! a stream of encoded items. Entering a hop at a position decodes with the
//! codec of the edge entering that position. So inside a run every call is
//! static, and the only type erasure is the bytes at a crossing.

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;

use super::{EncodedItem, Head};
use crate::wire::{Codec, RequestCodec, SizeHint};
use crate::zstd::Compression;
use gdl::__runtime::{Fuse, fuse, fused, until_error};
use gdl::Cx;
use gdl::{Around, Backward, Forward, Next, Operator, Sink};
use gdl::{Error, ErrorKind, Result};
use gdl::{Items, items};

/// Where a forward walk ended.
#[doc(hidden)]
pub enum FwdExit<T> {
    /// The value reached the inner end in this segment.
    Here(T),
    /// The walk left the run before position `at`, carrying the request's
    /// head and, for a streamed request, its encoded items.
    Leave {
        at: usize,
        head: Head,
        input: Option<Items<EncodedItem>>,
    },
}

/// Where a backward walk ended.
#[doc(hidden)]
pub enum BwdExit<T> {
    /// The stream reached the outer end in this run.
    Here(Items<T>),
    /// The walk left the run before position `at`, carrying encoded items.
    Leave {
        at: usize,
        bytes: Items<EncodedItem>,
    },
}

/// A hop's forward nodes in this segment's view, as a cons list ending in
/// [`FwdEnd`]. Position 0 is the outer end; the first node is position 1.
///
/// `Below` is the stack of slots that the around nodes before this part of
/// the chain fill: one slot for each, whichever segment runs it, so that
/// one build serves every placement. A slot is filled only where its
/// forward half ran.
pub trait DForward<Below = ()>: sealed::Sealed + Send + Sync + 'static {
    /// The request type the chain receives from the outer end.
    type In: Send + 'static;
    /// The request type that reaches the inner end.
    type Out: Send + 'static;
    /// `Below` with a slot pushed for each around node of the chain.
    #[doc(hidden)]
    type Stack: Send + 'static;

    /// The stack of this part of the chain with every slot it adds empty:
    /// for the positions a walk does not run.
    #[doc(hidden)]
    fn empty(below: Below) -> Self::Stack;

    /// Collect the positions in this part of the hop, from `pos`, whose
    /// slot is `Absent`.
    #[doc(hidden)]
    fn absent(&self, pos: usize, out: &mut Vec<usize>);

    /// Collect, for each forward edge from this part of the hop on, whether
    /// its codec carries a streamed request.
    #[doc(hidden)]
    fn streamed(&self, out: &mut Vec<bool>);

    /// Collect, for each forward edge from this part of the hop on, the
    /// head type, its name, and its encoding.
    #[doc(hidden)]
    fn heads(&self, out: &mut Vec<HeadInfo>);

    /// Compress the stream items of each forward edge from this part of
    /// the hop on, from `pos`, as `items[pos]` says: the edge that enters
    /// position `pos`.
    #[doc(hidden)]
    fn compress(&mut self, pos: usize, items: &[Option<Compression>]);

    #[doc(hidden)]
    fn walk(
        &self,
        pos: usize,
        end: usize,
        cx: &Cx,
        value: Self::In,
        below: Below,
    ) -> impl Future<Output = Result<(FwdExit<Self::Out>, Self::Stack)>> + Send;

    #[doc(hidden)]
    #[allow(
        clippy::too_many_arguments,
        reason = "a walk's position, its request, and its stack, each passed down the chain"
    )]
    fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        cx: &Cx,
        head: &[u8],
        input: Option<Items<Bytes>>,
        below: Below,
    ) -> impl Future<Output = Result<(FwdExit<Self::Out>, Self::Stack)>> + Send;
}

/// The value one around node kept, or `None` where its forward half did not
/// run. Tagged with the node's type, as the local chain's entries are.
pub struct Slot<P: Around>(Option<P::State>);

/// The head a forward edge carries, including when its node is absent.
#[doc(hidden)]
pub struct HeadInfo {
    pub(crate) id: std::any::TypeId,
    pub(crate) name: &'static str,
    pub(crate) encoding: gdl_model::Encoding,
}

impl HeadInfo {
    fn new<C: RequestCodec>(codec: Option<C>) -> Self {
        let (id, name) = C::head();
        Self {
            id,
            name,
            encoding: codec.map(|codec| codec.head_encoding()).unwrap_or_default(),
        }
    }
}

/// One forward node of a distributed hop. `C` is the codec of the edge
/// entering it: a [`Codec`], or a [`StreamCodec`](crate::StreamCodec) for a
/// streamed request.
pub struct FwdStep<F, R, C> {
    node: Arc<F>,
    codec: Option<C>,
    rest: R,
}

impl<F, R, C> FwdStep<F, R, C>
where
    F: Forward,
    C: RequestCodec<Value = F::In>,
{
    /// A forward node, the codec of the edge entering it, and the rest.
    pub fn new(node: Arc<F>, codec: Option<C>, rest: R) -> Self {
        Self { node, codec, rest }
    }
}

/// The end of a forward chain: the inner end's input edge, by its codec.
pub struct FwdEnd<C> {
    codec: Option<C>,
}

impl<C: RequestCodec> FwdEnd<C> {
    /// The codec of the edge entering the inner end.
    pub fn new(codec: Option<C>) -> Self {
        Self { codec }
    }
}

impl<F, R, C, Below> DForward<Below> for FwdStep<F, R, C>
where
    F: Forward,
    R: DForward<Below, In = F::Out>,
    C: RequestCodec<Value = F::In>,
    Below: Send + 'static,
{
    type In = F::In;
    type Out = R::Out;
    type Stack = R::Stack;

    fn empty(below: Below) -> R::Stack {
        R::empty(below)
    }

    fn absent(&self, pos: usize, out: &mut Vec<usize>) {
        if F::ABSENT {
            out.push(pos);
        }
        self.rest.absent(pos + 1, out);
    }

    fn streamed(&self, out: &mut Vec<bool>) {
        out.push(C::STREAMED);
        self.rest.streamed(out);
    }

    fn heads(&self, out: &mut Vec<HeadInfo>) {
        out.push(HeadInfo::new(self.codec));
        self.rest.heads(out);
    }

    fn compress(&mut self, pos: usize, items: &[Option<Compression>]) {
        if let Some(codec) = &mut self.codec {
            codec.compress_items(items.get(pos).copied().flatten());
        }
        self.rest.compress(pos + 1, items);
    }

    async fn walk(
        &self,
        pos: usize,
        end: usize,
        cx: &Cx,
        value: F::In,
        below: Below,
    ) -> Result<(FwdExit<R::Out>, R::Stack)> {
        if pos >= end {
            return Ok((leave(self.codec, pos, value)?, R::empty(below)));
        }
        // As in local composition: no forward stage starts for an
        // interrupted request.
        cx.check()?;
        let out = self.node.forward(cx, value).await?;
        self.rest.walk(pos + 1, end, cx, out, below).await
    }

    async fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        cx: &Cx,
        head: &[u8],
        input: Option<Items<Bytes>>,
        below: Below,
    ) -> Result<(FwdExit<R::Out>, R::Stack)> {
        if pos == target {
            let value = join(self.codec, head, input)?;
            return self.walk(pos, end, cx, value, below).await;
        }
        self.rest
            .enter(pos + 1, target, end, cx, head, input, below)
            .await
    }
}

/// The forward half of an around node in a distributed hop. `C` is the
/// codec of the edge entering it.
pub struct FwdAround<P, R, C> {
    node: Arc<P>,
    codec: Option<C>,
    rest: R,
}

impl<P, R, C> FwdAround<P, R, C>
where
    P: Around,
    C: RequestCodec<Value = P::UpReq>,
{
    /// An around node, the codec of the edge entering it, and the rest.
    pub fn new(node: Arc<P>, codec: Option<C>, rest: R) -> Self {
        Self { node, codec, rest }
    }
}

impl<P, R, C, Below> DForward<Below> for FwdAround<P, R, C>
where
    P: Around,
    R: DForward<(Slot<P>, Below), In = P::DownReq>,
    C: RequestCodec<Value = P::UpReq>,
    Below: Send + 'static,
{
    type In = P::UpReq;
    type Out = R::Out;
    type Stack = R::Stack;

    fn empty(below: Below) -> R::Stack {
        R::empty((Slot(None), below))
    }

    fn absent(&self, pos: usize, out: &mut Vec<usize>) {
        if P::ABSENT {
            out.push(pos);
        }
        self.rest.absent(pos + 1, out);
    }

    fn streamed(&self, out: &mut Vec<bool>) {
        out.push(C::STREAMED);
        self.rest.streamed(out);
    }

    fn heads(&self, out: &mut Vec<HeadInfo>) {
        out.push(HeadInfo::new(self.codec));
        self.rest.heads(out);
    }

    fn compress(&mut self, pos: usize, items: &[Option<Compression>]) {
        if let Some(codec) = &mut self.codec {
            codec.compress_items(items.get(pos).copied().flatten());
        }
        self.rest.compress(pos + 1, items);
    }

    async fn walk(
        &self,
        pos: usize,
        end: usize,
        cx: &Cx,
        value: P::UpReq,
        below: Below,
    ) -> Result<(FwdExit<R::Out>, R::Stack)> {
        if pos >= end {
            return Ok((leave(self.codec, pos, value)?, Self::empty(below)));
        }
        cx.check()?;
        let (out, state) = self.node.forward(cx, value).await?;
        let below = (Slot(Some(state)), below);
        self.rest.walk(pos + 1, end, cx, out, below).await
    }

    async fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        cx: &Cx,
        head: &[u8],
        input: Option<Items<Bytes>>,
        below: Below,
    ) -> Result<(FwdExit<R::Out>, R::Stack)> {
        if pos == target {
            let value = join(self.codec, head, input)?;
            return self.walk(pos, end, cx, value, below).await;
        }
        // A position before the one this run starts at: its forward half
        // ran elsewhere, if anywhere, and its slot stays empty.
        let below = (Slot(None), below);
        self.rest
            .enter(pos + 1, target, end, cx, head, input, below)
            .await
    }
}

impl<C: RequestCodec, Below: Send + 'static> DForward<Below> for FwdEnd<C> {
    type In = C::Value;
    type Out = C::Value;
    type Stack = Below;

    fn empty(below: Below) -> Below {
        below
    }

    fn absent(&self, _pos: usize, _out: &mut Vec<usize>) {}

    fn streamed(&self, out: &mut Vec<bool>) {
        out.push(C::STREAMED);
    }

    fn heads(&self, out: &mut Vec<HeadInfo>) {
        out.push(HeadInfo::new(self.codec));
    }

    fn compress(&mut self, pos: usize, items: &[Option<Compression>]) {
        if let Some(codec) = &mut self.codec {
            codec.compress_items(items.get(pos).copied().flatten());
        }
    }

    async fn walk(
        &self,
        pos: usize,
        end: usize,
        _cx: &Cx,
        value: C::Value,
        below: Below,
    ) -> Result<(FwdExit<C::Value>, Below)> {
        if pos >= end {
            return Ok((leave(self.codec, pos, value)?, below));
        }
        Ok((FwdExit::Here(value), below))
    }

    async fn enter(
        &self,
        pos: usize,
        target: usize,
        _end: usize,
        _cx: &Cx,
        head: &[u8],
        input: Option<Items<Bytes>>,
        below: Below,
    ) -> Result<(FwdExit<C::Value>, Below)> {
        if pos != target {
            return Err(position_error(target));
        }
        Ok((FwdExit::Here(join(self.codec, head, input)?), below))
    }
}

/// A hop's backward nodes in this segment's view, as a cons list ending in
/// [`BwdEnd`]. Position 0 is the inner end; the first node is position 1.
///
/// `Stack` holds the slots of the around nodes whose backward halves are in
/// this part of the chain, the next one on top. The end takes only an
/// empty stack.
pub trait DBackward<Stack = ()>: sealed::Sealed + Send + Sync + 'static {
    /// The item type the chain receives from the inner end.
    type In: Send + 'static;
    /// The item type that reaches the outer end.
    type Out: Send + 'static;

    /// Collect the positions in this part of the hop, from `pos`, whose
    /// slot is `Absent`.
    #[doc(hidden)]
    fn absent(&self, pos: usize, out: &mut Vec<usize>);

    /// Compress the items of each backward edge from this part of the hop
    /// on, from `pos`, as `items[pos]` says: the edge that enters position
    /// `pos`.
    #[doc(hidden)]
    fn compress(&mut self, pos: usize, items: &[Option<Compression>]);

    #[doc(hidden)]
    fn walk(
        &self,
        pos: usize,
        end: usize,
        attempt: &Cx,
        items: Items<Self::In>,
        stack: Stack,
    ) -> BwdExit<Self::Out>;

    #[doc(hidden)]
    fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        attempt: &Cx,
        bytes: Items<Bytes>,
        stack: Stack,
    ) -> Result<BwdExit<Self::Out>>;

    /// The stream a local plan's attempt boundary takes: the whole chain's,
    /// or, with no backward node, the inner end's node boundary itself,
    /// unboxed. `None` if the chain would leave the segment, which a local
    /// plan never does.
    #[doc(hidden)]
    type Answer: futures::Stream<Item = Result<Self::Out>> + Unpin + Send + 'static;

    /// The whole chain, from its first position, over the inner end's node
    /// boundary, with the slots of its around nodes.
    #[doc(hidden)]
    fn answer(&self, attempt: &Cx, fused: Fuse<Self::In>, stack: Stack) -> Option<Self::Answer>;
}

/// One backward node of a distributed hop.
pub struct BwdStep<B: Backward, R> {
    node: Arc<B>,
    codec: Option<Codec<B::In>>,
    rest: R,
}

impl<B: Backward, R> BwdStep<B, R> {
    /// A backward node, the codec of the edge entering it, and the rest.
    pub fn new(node: Arc<B>, codec: Option<Codec<B::In>>, rest: R) -> Self {
        Self { node, codec, rest }
    }
}

/// The end of a backward chain: the outer end's input edge.
pub struct BwdEnd<T> {
    codec: Option<Codec<T>>,
}

impl<T> BwdEnd<T> {
    /// The codec of the edge entering the outer end.
    pub fn new(codec: Option<Codec<T>>) -> Self {
        Self { codec }
    }
}

impl<B, R, S> DBackward<S> for BwdStep<B, R>
where
    B: Backward,
    R: DBackward<S, In = B::Out>,
    S: Send + 'static,
{
    type In = B::In;
    type Out = R::Out;

    fn absent(&self, pos: usize, out: &mut Vec<usize>) {
        if B::ABSENT {
            out.push(pos);
        }
        self.rest.absent(pos + 1, out);
    }

    fn compress(&mut self, pos: usize, items: &[Option<Compression>]) {
        if let Some(codec) = &mut self.codec {
            codec.compress(items.get(pos).copied().flatten());
        }
        self.rest.compress(pos + 1, items);
    }

    fn walk(
        &self,
        pos: usize,
        end: usize,
        attempt: &Cx,
        items: Items<B::In>,
        stack: S,
    ) -> BwdExit<R::Out> {
        if pos >= end {
            return BwdExit::Leave {
                at: pos,
                bytes: encode_stream(self.codec, items),
            };
        }
        // As in local composition: an interrupted request starts no further
        // backward node, and the reason travels on in its place.
        if let Err(reason) = attempt.check() {
            let failed = gdl::__runtime::failed(reason);
            return self.rest.walk(pos + 1, end, attempt, failed, stack);
        }
        // Each node runs in its own scope, ended by the fuse after it, as in
        // local composition.
        let scope = attempt.child();
        let out = self.node.backward(&scope, items);
        self.rest
            .walk(pos + 1, end, attempt, fuse(out, &scope), stack)
    }

    fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        attempt: &Cx,
        bytes: Items<Bytes>,
        stack: S,
    ) -> Result<BwdExit<R::Out>> {
        if pos == target {
            let items = decode_stream(self.codec, bytes);
            return Ok(self.walk(pos, end, attempt, items, stack));
        }
        self.rest.enter(pos + 1, target, end, attempt, bytes, stack)
    }

    type Answer = Items<R::Out>;

    fn answer(&self, attempt: &Cx, fused: Fuse<B::In>, stack: S) -> Option<Items<R::Out>> {
        match self.walk(1, usize::MAX, attempt, Box::pin(fused), stack) {
            BwdExit::Here(items) => Some(items),
            BwdExit::Leave { .. } => None,
        }
    }
}

/// The backward half of an around node in a distributed hop.
pub struct BwdAround<P: Around, R> {
    node: Arc<P>,
    codec: Option<Codec<P::DownItem>>,
    rest: R,
}

impl<P: Around, R> BwdAround<P, R> {
    /// An around node, the codec of the edge entering its backward half, and
    /// the rest.
    pub fn new(node: Arc<P>, codec: Option<Codec<P::DownItem>>, rest: R) -> Self {
        Self { node, codec, rest }
    }
}

impl<P, R, Rest> DBackward<(Slot<P>, Rest)> for BwdAround<P, R>
where
    P: Around,
    R: DBackward<Rest, In = P::UpItem>,
    Rest: Send + 'static,
{
    type In = P::DownItem;
    type Out = R::Out;

    fn absent(&self, pos: usize, out: &mut Vec<usize>) {
        if P::ABSENT {
            out.push(pos);
        }
        self.rest.absent(pos + 1, out);
    }

    fn compress(&mut self, pos: usize, items: &[Option<Compression>]) {
        if let Some(codec) = &mut self.codec {
            codec.compress(items.get(pos).copied().flatten());
        }
        self.rest.compress(pos + 1, items);
    }

    fn walk(
        &self,
        pos: usize,
        end: usize,
        attempt: &Cx,
        items: Items<P::DownItem>,
        (Slot(state), rest): (Slot<P>, Rest),
    ) -> BwdExit<R::Out> {
        if pos >= end {
            return BwdExit::Leave {
                at: pos,
                bytes: encode_stream(self.codec, items),
            };
        }
        // Skipped, as a backward node is: the value is dropped here, and the
        // node never sees it.
        if let Err(reason) = attempt.check() {
            drop(state);
            let failed = gdl::__runtime::failed(reason);
            return self.rest.walk(pos + 1, end, attempt, failed, rest);
        }
        // The layout places both halves where one task runs them, so the
        // forward half filled this slot in this task.
        let Some(state) = state else {
            let failed = gdl::__runtime::failed(Error::new(
                ErrorKind::Placement,
                "an around node's backward half ran where its forward half did not",
            ));
            return self.rest.walk(pos + 1, end, attempt, failed, rest);
        };
        let scope = attempt.child();
        let out = self.node.backward(&scope, state, items);
        self.rest
            .walk(pos + 1, end, attempt, fuse(out, &scope), rest)
    }

    fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        attempt: &Cx,
        bytes: Items<Bytes>,
        (slot, rest): (Slot<P>, Rest),
    ) -> Result<BwdExit<R::Out>> {
        if pos == target {
            let items = decode_stream(self.codec, bytes);
            return Ok(self.walk(pos, end, attempt, items, (slot, rest)));
        }
        drop(slot);
        self.rest.enter(pos + 1, target, end, attempt, bytes, rest)
    }

    // An around node's backward half takes `Items`, as a backward node
    // does: the inner end's node boundary is boxed into it.
    type Answer = Items<R::Out>;

    fn answer(
        &self,
        attempt: &Cx,
        fused: Fuse<P::DownItem>,
        stack: (Slot<P>, Rest),
    ) -> Option<Items<R::Out>> {
        match self.walk(1, usize::MAX, attempt, Box::pin(fused), stack) {
            BwdExit::Here(items) => Some(items),
            BwdExit::Leave { .. } => None,
        }
    }
}

impl<T: Send + 'static> DBackward<()> for BwdEnd<T> {
    type In = T;
    type Out = T;

    fn absent(&self, _pos: usize, _out: &mut Vec<usize>) {}

    fn compress(&mut self, pos: usize, items: &[Option<Compression>]) {
        if let Some(codec) = &mut self.codec {
            codec.compress(items.get(pos).copied().flatten());
        }
    }

    fn walk(&self, pos: usize, end: usize, _attempt: &Cx, items: Items<T>, (): ()) -> BwdExit<T> {
        if pos >= end {
            return BwdExit::Leave {
                at: pos,
                bytes: encode_stream(self.codec, items),
            };
        }
        BwdExit::Here(items)
    }

    fn enter(
        &self,
        pos: usize,
        target: usize,
        _end: usize,
        _attempt: &Cx,
        bytes: Items<Bytes>,
        (): (),
    ) -> Result<BwdExit<T>> {
        if pos != target {
            return Err(position_error(target));
        }
        Ok(BwdExit::Here(decode_stream(self.codec, bytes)))
    }

    // Nothing comes between the node boundary and the attempt boundary, so
    // the attempt boundary holds it as it is, as in local composition: one
    // box fewer per request, and one dynamic poll fewer per item.
    type Answer = Fuse<T>;

    fn answer(&self, _attempt: &Cx, fused: Fuse<T>, (): ()) -> Option<Fuse<T>> {
        Some(fused)
    }
}

/// The inner end of a distributed hop: a sink, or an operator with its own
/// downstream hop.
pub trait DInner: sealed::Sealed + Send + Sync + 'static {
    /// The request type the inner end receives.
    type Req: Send + 'static;
    /// The item type the inner end produces.
    type Item: Send + 'static;

    /// Whether the inner end's slot is `Absent`.
    #[doc(hidden)]
    fn absent(&self) -> bool;

    /// Call the inner end under `scope`, its own scope, and return its
    /// stream behind a node boundary, ended with the scope. The boundary is
    /// not boxed: a local plan's attempt boundary may hold it as it is.
    #[doc(hidden)]
    fn call(
        &self,
        scope: &Cx,
        req: Self::Req,
    ) -> impl Future<Output = Result<Fuse<Self::Item>>> + Send;

    #[doc(hidden)]
    fn register(&self, entries: &mut super::runtime::Entries);
}

/// A sink as the inner end of a distributed hop.
pub struct DSink<S> {
    sink: Arc<S>,
}

impl<S> DSink<S> {
    /// Wrap a sink (or an `Absent` sink slot).
    pub fn new(sink: Arc<S>) -> Self {
        Self { sink }
    }
}

impl<S: Sink> DInner for DSink<S> {
    type Req = S::Req;
    type Item = S::Item;

    fn absent(&self) -> bool {
        S::ABSENT
    }

    async fn call(&self, scope: &Cx, req: S::Req) -> Result<Fuse<S::Item>> {
        let items = self.sink.generate(scope.clone(), req).await?;
        // The request has reached its sink: the segment's request path
        // ends here, unless a hand-off ended it first.
        if let Some(visit) = scope.visit() {
            visit.producing();
        }
        Ok(fused(items, scope))
    }

    fn register(&self, _entries: &mut super::runtime::Entries) {}
}

/// An operator as the inner end of a distributed hop, with its downstream hop.
pub struct DOperator<O, H> {
    op: Arc<O>,
    next: H,
}

impl<O, H> DOperator<O, H> {
    /// Wrap an operator (or an `Absent` operator slot) and its downstream hop.
    pub fn new(op: Arc<O>, next: H) -> Self {
        Self { op, next }
    }
}

impl<O, H> DInner for DOperator<O, H>
where
    O: Operator,
    H: Next<O::DownReq, O::DownItem> + super::hop::Register,
{
    type Req = O::UpReq;
    type Item = O::UpItem;

    /// The operator only: its downstream hop checks its own slots.
    fn absent(&self) -> bool {
        O::ABSENT
    }

    /// An operator's return does not end the request path: it may return
    /// its stream before it calls `next`, and the hop it calls ends it.
    async fn call(&self, scope: &Cx, req: O::UpReq) -> Result<Fuse<O::UpItem>> {
        let items = self
            .op
            .generate(scope.clone(), req, self.next.for_request(scope))
            .await?;
        Ok(fused(items, scope))
    }

    fn register(&self, entries: &mut super::runtime::Entries) {
        self.next.register(entries);
    }
}

fn leave<C: RequestCodec, T>(codec: Option<C>, at: usize, value: C::Value) -> Result<FwdExit<T>> {
    let split = codec.ok_or_else(not_wire)?.split(value)?;
    Ok(FwdExit::Leave {
        at,
        head: split.head,
        input: split.input,
    })
}

fn join<C: RequestCodec>(
    codec: Option<C>,
    head: &[u8],
    input: Option<Items<Bytes>>,
) -> Result<C::Value> {
    codec.ok_or_else(not_wire)?.join(head, input)
}

fn encode_stream<T: Send + 'static>(
    codec: Option<Codec<T>>,
    input: Items<T>,
) -> Items<EncodedItem> {
    match codec {
        Some(codec) => {
            let mut hint = SizeHint::default();
            let mut scratch = Vec::new();
            items(input.map(move |item| codec.encode_item(&item?, &mut hint, &mut scratch)))
        }
        None => items(futures::stream::once(async { Err(not_wire()) })),
    }
}

fn decode_stream<T: Send + 'static>(codec: Option<Codec<T>>, input: Items<Bytes>) -> Items<T> {
    match codec {
        // A crossing is a boundary like any other: nothing follows an error.
        Some(codec) => {
            // The stream's own buffer for compressed items: one allocation
            // for its first, none for the rest.
            let mut scratch = Vec::new();
            until_error(items(
                input.map(move |bytes| codec.decode_item(&bytes?, &mut scratch)),
            ))
        }
        None => items(futures::stream::once(async { Err(not_wire()) })),
    }
}

fn not_wire() -> Error {
    // Placement validation rejects a crossing over an edge without a codec,
    // so reaching this means the layout and the bindings disagree.
    Error::new(ErrorKind::Placement, "an edge without a codec was crossed")
}

fn position_error(target: usize) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("no stage at position {target} of this hop"),
    )
}

/// Only this module's stage types implement the stage traits: their methods
/// are the runtime's internals, not an extension point.
mod sealed {
    pub trait Sealed {}

    impl<F, R, C> Sealed for super::FwdStep<F, R, C> {}
    impl<C> Sealed for super::FwdEnd<C> {}
    impl<B: gdl::Backward, R> Sealed for super::BwdStep<B, R> {}
    impl<P, R, C> Sealed for super::FwdAround<P, R, C> {}
    impl<P: gdl::Around, R> Sealed for super::BwdAround<P, R> {}
    impl<T> Sealed for super::BwdEnd<T> {}
    impl<S> Sealed for super::DSink<S> {}
    impl<O, H> Sealed for super::DOperator<O, H> {}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// Sets its flag when dropped.
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn decoding_ends_at_the_first_error_and_releases_its_input() {
        let codec = crate::wire_codec!(u32).expect("u32 is a wire type");
        let good = codec.encode(&7).expect("encode");
        let released = Arc::new(AtomicBool::new(false));
        let guard = Dropped(Arc::clone(&released));
        let input = items(
            // 0xc1 is the one byte msgpack never uses.
            futures::stream::iter([Ok(Bytes::from_static(&[0xc1])), Ok(Bytes::from(good))]).map(
                move |item| {
                    let _guard = &guard;
                    item
                },
            ),
        );
        let mut decoded = decode_stream(Some(codec), input);
        let first = decoded.next().await;
        assert!(matches!(first, Some(Err(_))), "{first:?}");
        assert!(
            released.load(Ordering::SeqCst),
            "the input outlived its error"
        );
        assert!(decoded.next().await.is_none(), "an item followed the error");
    }
}
