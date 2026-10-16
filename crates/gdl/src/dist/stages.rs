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

use futures::StreamExt;

use crate::cx::Cx;
use crate::error::{Error, ErrorKind, Result};
use crate::items::{Items, fuse, items, until_error};
use crate::role::{Backward, Forward, Next, Operator, Sink};
use crate::wire::{Codec, RequestCodec};

/// Where a forward walk ended.
#[doc(hidden)]
pub enum FwdExit<T> {
    /// The value reached the inner end in this segment.
    Here(T),
    /// The walk left the run before position `at`, carrying the request's
    /// head and, for a streamed request, its encoded items.
    Leave {
        at: usize,
        head: Vec<u8>,
        input: Option<Items<Vec<u8>>>,
    },
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

    /// Collect the positions in this part of the hop, from `pos`, whose
    /// slot is `Absent`.
    #[doc(hidden)]
    fn absent(&self, pos: usize, out: &mut Vec<usize>);

    /// Collect, for each forward edge from this part of the hop on, whether
    /// its codec carries a streamed request.
    #[doc(hidden)]
    fn streamed(&self, out: &mut Vec<bool>);

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
        head: &[u8],
        input: Option<Items<Vec<u8>>>,
    ) -> impl Future<Output = Result<FwdExit<Self::Out>>> + Send;
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

impl<F, R, C> DForward for FwdStep<F, R, C>
where
    F: Forward,
    R: DForward<In = F::Out>,
    C: RequestCodec<Value = F::In>,
{
    type In = F::In;
    type Out = R::Out;

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

    async fn walk(&self, pos: usize, end: usize, cx: &Cx, value: F::In) -> Result<FwdExit<R::Out>> {
        if pos >= end {
            return leave(self.codec, pos, value);
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
        head: &[u8],
        input: Option<Items<Vec<u8>>>,
    ) -> Result<FwdExit<R::Out>> {
        if pos == target {
            let value = join(self.codec, head, input)?;
            return self.walk(pos, end, cx, value).await;
        }
        self.rest.enter(pos + 1, target, end, cx, head, input).await
    }
}

impl<C: RequestCodec> DForward for FwdEnd<C> {
    type In = C::Value;
    type Out = C::Value;

    fn absent(&self, _pos: usize, _out: &mut Vec<usize>) {}

    fn streamed(&self, out: &mut Vec<bool>) {
        out.push(C::STREAMED);
    }

    async fn walk(
        &self,
        pos: usize,
        end: usize,
        _cx: &Cx,
        value: C::Value,
    ) -> Result<FwdExit<C::Value>> {
        if pos >= end {
            return leave(self.codec, pos, value);
        }
        Ok(FwdExit::Here(value))
    }

    async fn enter(
        &self,
        pos: usize,
        target: usize,
        _end: usize,
        _cx: &Cx,
        head: &[u8],
        input: Option<Items<Vec<u8>>>,
    ) -> Result<FwdExit<C::Value>> {
        if pos != target {
            return Err(position_error(target));
        }
        Ok(FwdExit::Here(join(self.codec, head, input)?))
    }
}

/// A hop's backward nodes in this segment's view, as a cons list ending in
/// [`BwdEnd`]. Position 0 is the inner end; the first node is position 1.
pub trait DBackward: sealed::Sealed + Send + Sync + 'static {
    /// The item type the chain receives from the inner end.
    type In: Send + 'static;
    /// The item type that reaches the outer end.
    type Out: Send + 'static;

    /// Collect the positions in this part of the hop, from `pos`, whose
    /// slot is `Absent`.
    #[doc(hidden)]
    fn absent(&self, pos: usize, out: &mut Vec<usize>);

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

    fn absent(&self, pos: usize, out: &mut Vec<usize>) {
        if B::ABSENT {
            out.push(pos);
        }
        self.rest.absent(pos + 1, out);
    }

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

    fn absent(&self, _pos: usize, _out: &mut Vec<usize>) {}

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

    /// Whether the inner end's slot is `Absent`.
    #[doc(hidden)]
    fn absent(&self) -> bool;

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

    fn absent(&self) -> bool {
        S::ABSENT
    }

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

    /// The operator only: its downstream hop checks its own slots.
    fn absent(&self) -> bool {
        O::ABSENT
    }

    async fn call(&self, cx: Cx, req: O::UpReq) -> Result<Items<O::UpItem>> {
        self.op.generate(cx, req, self.next.clone()).await
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
    input: Option<Items<Vec<u8>>>,
) -> Result<C::Value> {
    codec.ok_or_else(not_wire)?.join(head, input)
}

fn encode_stream<T: Send + 'static>(codec: Option<Codec<T>>, input: Items<T>) -> Items<Vec<u8>> {
    match codec {
        Some(codec) => items(input.map(move |item| codec.encode(&item?))),
        None => items(futures::stream::once(async { Err(not_wire()) })),
    }
}

fn decode_stream<T: Send + 'static>(codec: Option<Codec<T>>, input: Items<Vec<u8>>) -> Items<T> {
    match codec {
        // A crossing is a boundary like any other: nothing follows an error.
        Some(codec) => until_error(items(input.map(move |bytes| codec.decode(&bytes?)))),
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
    impl<B: crate::role::Backward, R> Sealed for super::BwdStep<B, R> {}
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
            futures::stream::iter([Ok(vec![0xc1]), Ok(good)]).map(move |item| {
                let _guard = &guard;
                item
            }),
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
