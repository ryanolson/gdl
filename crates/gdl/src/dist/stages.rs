//! Typed stages of a distributed hop, and the walks over them.
//!
//! A walk applies stages while they belong to the current run, and leaves at
//! the first stage that does not, encoding the value or stream with the codec
//! of the edge it leaves on. Entering a hop at a position decodes with the
//! codec of the edge entering that position. So inside a run every call is
//! static, and the only type erasure is the bytes at a crossing.

use std::future::Future;
use std::sync::Arc;

use futures::StreamExt;

use crate::cx::Cx;
use crate::error::{Error, ErrorKind, Result};
use crate::items::{Items, fuse, items};
use crate::role::{Backward, Forward, Next, Operator, Sink};
use crate::wire::Codec;

/// Where a forward walk ended.
#[doc(hidden)]
pub enum FwdExit<T> {
    /// The value reached the inner end in this segment.
    Here(T),
    /// The walk left the run before position `at`, carrying `bytes`.
    Leave { at: usize, bytes: Vec<u8> },
}

/// Where a backward walk ended.
#[doc(hidden)]
pub enum BwdExit<T> {
    /// The stream reached the outer end in this run.
    Here(Items<T>),
    /// The walk left the run before position `at`, carrying encoded items.
    Leave { at: usize, bytes: Items<Vec<u8>> },
}

/// A hop's forward nodes in this segment's view, as a cons list ending in
/// [`FwdEnd`]. Position 0 is the outer end; the first node is position 1.
pub trait DForward: sealed::Sealed + Send + Sync + 'static {
    /// The request type the chain receives from the outer end.
    type In: Send + 'static;
    /// The request type that reaches the inner end.
    type Out: Send + 'static;

    #[doc(hidden)]
    fn walk(
        &self,
        pos: usize,
        end: usize,
        cx: &Cx,
        value: Self::In,
    ) -> impl Future<Output = Result<FwdExit<Self::Out>>> + Send;

    #[doc(hidden)]
    fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        cx: &Cx,
        bytes: &[u8],
    ) -> impl Future<Output = Result<FwdExit<Self::Out>>> + Send;
}

/// One forward node of a distributed hop.
pub struct FwdStep<F: Forward, R> {
    node: Arc<F>,
    codec: Option<Codec<F::In>>,
    rest: R,
}

impl<F: Forward, R> FwdStep<F, R> {
    /// A forward node, the codec of the edge entering it, and the rest.
    pub fn new(node: Arc<F>, codec: Option<Codec<F::In>>, rest: R) -> Self {
        Self { node, codec, rest }
    }
}

/// The end of a forward chain: the inner end's input edge.
pub struct FwdEnd<T> {
    codec: Option<Codec<T>>,
}

impl<T> FwdEnd<T> {
    /// The codec of the edge entering the inner end.
    pub fn new(codec: Option<Codec<T>>) -> Self {
        Self { codec }
    }
}

impl<F, R> DForward for FwdStep<F, R>
where
    F: Forward,
    R: DForward<In = F::Out>,
{
    type In = F::In;
    type Out = R::Out;

    async fn walk(&self, pos: usize, end: usize, cx: &Cx, value: F::In) -> Result<FwdExit<R::Out>> {
        if pos >= end {
            return Ok(FwdExit::Leave {
                at: pos,
                bytes: encode(self.codec, &value)?,
            });
        }
        // As in local composition: no forward stage starts for an
        // interrupted request.
        cx.check()?;
        let out = self.node.forward(cx, value).await?;
        self.rest.walk(pos + 1, end, cx, out).await
    }

    async fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        cx: &Cx,
        bytes: &[u8],
    ) -> Result<FwdExit<R::Out>> {
        if pos == target {
            let value = decode(self.codec, bytes)?;
            return self.walk(pos, end, cx, value).await;
        }
        self.rest.enter(pos + 1, target, end, cx, bytes).await
    }
}

impl<T: Send + 'static> DForward for FwdEnd<T> {
    type In = T;
    type Out = T;

    async fn walk(&self, pos: usize, end: usize, _cx: &Cx, value: T) -> Result<FwdExit<T>> {
        if pos >= end {
            return Ok(FwdExit::Leave {
                at: pos,
                bytes: encode(self.codec, &value)?,
            });
        }
        Ok(FwdExit::Here(value))
    }

    async fn enter(
        &self,
        pos: usize,
        target: usize,
        _end: usize,
        _cx: &Cx,
        bytes: &[u8],
    ) -> Result<FwdExit<T>> {
        if pos != target {
            return Err(position_error(target));
        }
        Ok(FwdExit::Here(decode(self.codec, bytes)?))
    }
}

/// A hop's backward nodes in this segment's view, as a cons list ending in
/// [`BwdEnd`]. Position 0 is the inner end; the first node is position 1.
pub trait DBackward: sealed::Sealed + Send + Sync + 'static {
    /// The item type the chain receives from the inner end.
    type In: Send + 'static;
    /// The item type that reaches the outer end.
    type Out: Send + 'static;

    #[doc(hidden)]
    fn walk(
        &self,
        pos: usize,
        end: usize,
        attempt: &Cx,
        items: Items<Self::In>,
    ) -> BwdExit<Self::Out>;

    #[doc(hidden)]
    fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        attempt: &Cx,
        bytes: Items<Vec<u8>>,
    ) -> Result<BwdExit<Self::Out>>;
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

impl<B, R> DBackward for BwdStep<B, R>
where
    B: Backward,
    R: DBackward<In = B::Out>,
{
    type In = B::In;
    type Out = R::Out;

    fn walk(&self, pos: usize, end: usize, attempt: &Cx, items: Items<B::In>) -> BwdExit<R::Out> {
        if pos >= end {
            return BwdExit::Leave {
                at: pos,
                bytes: encode_stream(self.codec, items),
            };
        }
        // As in local composition: an interrupted request starts no further
        // backward node, and the reason travels on in its place.
        if let Err(reason) = attempt.check() {
            return self
                .rest
                .walk(pos + 1, end, attempt, crate::items::failed(reason));
        }
        // Each node runs in its own scope, ended by the fuse after it, as in
        // local composition.
        let scope = attempt.child();
        let out = self.node.backward(&scope, items);
        self.rest.walk(pos + 1, end, attempt, fuse(out, &scope))
    }

    fn enter(
        &self,
        pos: usize,
        target: usize,
        end: usize,
        attempt: &Cx,
        bytes: Items<Vec<u8>>,
    ) -> Result<BwdExit<R::Out>> {
        if pos == target {
            let items = decode_stream(self.codec, bytes);
            return Ok(self.walk(pos, end, attempt, items));
        }
        self.rest.enter(pos + 1, target, end, attempt, bytes)
    }
}

impl<T: Send + 'static> DBackward for BwdEnd<T> {
    type In = T;
    type Out = T;

    fn walk(&self, pos: usize, end: usize, _attempt: &Cx, items: Items<T>) -> BwdExit<T> {
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
        bytes: Items<Vec<u8>>,
    ) -> Result<BwdExit<T>> {
        if pos != target {
            return Err(position_error(target));
        }
        Ok(BwdExit::Here(decode_stream(self.codec, bytes)))
    }
}

/// The inner end of a distributed hop: a sink, or an operator with its own
/// downstream hop.
pub trait DInner: sealed::Sealed + Send + Sync + 'static {
    /// The request type the inner end receives.
    type Req: Send + 'static;
    /// The item type the inner end produces.
    type Item: Send + 'static;

    #[doc(hidden)]
    fn call(
        &self,
        cx: Cx,
        req: Self::Req,
    ) -> impl Future<Output = Result<Items<Self::Item>>> + Send;

    #[doc(hidden)]
    fn register(&self, entries: &mut super::runtime::Entries);
}

/// A sink as the inner end of a distributed hop.
pub struct DSink<S>(Arc<S>);

impl<S> DSink<S> {
    /// Wrap a sink (or an `Absent` sink slot).
    pub fn new(sink: Arc<S>) -> Self {
        Self(sink)
    }
}

impl<S: Sink> DInner for DSink<S> {
    type Req = S::Req;
    type Item = S::Item;

    async fn call(&self, cx: Cx, req: S::Req) -> Result<Items<S::Item>> {
        self.0.generate(cx, req).await
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

    async fn call(&self, cx: Cx, req: O::UpReq) -> Result<Items<O::UpItem>> {
        self.op.generate(cx, req, self.next.clone()).await
    }

    fn register(&self, entries: &mut super::runtime::Entries) {
        self.next.register(entries);
    }
}

fn encode<T>(codec: Option<Codec<T>>, value: &T) -> Result<Vec<u8>> {
    codec.ok_or_else(not_wire)?.encode(value)
}

fn decode<T>(codec: Option<Codec<T>>, bytes: &[u8]) -> Result<T> {
    codec.ok_or_else(not_wire)?.decode(bytes)
}

fn encode_stream<T: Send + 'static>(codec: Option<Codec<T>>, input: Items<T>) -> Items<Vec<u8>> {
    match codec {
        Some(codec) => items(input.map(move |item| codec.encode(&item?))),
        None => items(futures::stream::once(async { Err(not_wire()) })),
    }
}

fn decode_stream<T: Send + 'static>(codec: Option<Codec<T>>, input: Items<Vec<u8>>) -> Items<T> {
    match codec {
        Some(codec) => items(input.map(move |bytes| codec.decode(&bytes?))),
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

    impl<F: crate::role::Forward, R> Sealed for super::FwdStep<F, R> {}
    impl<T> Sealed for super::FwdEnd<T> {}
    impl<B: crate::role::Backward, R> Sealed for super::BwdStep<B, R> {}
    impl<T> Sealed for super::BwdEnd<T> {}
    impl<S> Sealed for super::DSink<S> {}
    impl<O, H> Sealed for super::DOperator<O, H> {}
}
