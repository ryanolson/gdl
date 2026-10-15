//! A hop of a distributed graph: one typed structure that runs as a nested
//! call when its stages share a segment, and across segments otherwise.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Future, Stream, StreamExt};

use super::envelope::{Forward, Open, VERSION, WireCx, WireItem};
use super::fabric::{Canceller, Frame, Inbound, Outbound, ReplyTo};
use super::runtime::{Entries, Runtime};
use super::stages::{BwdExit, DBackward, DForward, DInner, FwdExit};
use crate::cx::{Cx, Deadline};
use crate::error::{Error, ErrorKind, Result};
use crate::items::{Items, bound, fuse, items};
use crate::placement::HopLayout;
use crate::role::Next;

/// Registers the entries a hop, and every hop nested under it, hosts in this
/// segment.
pub trait Register {
    #[doc(hidden)]
    fn register(&self, entries: &mut Entries);
}

/// A hop of a distributed graph. As an outer end's `next` it starts
/// attempts; as a registered hop it serves the forward hand-offs and backward
/// runs this segment hosts.
pub struct DHop<F, I, B>(Arc<HopCore<F, I, B>>);

impl<F, I, B> Clone for DHop<F, I, B> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

struct HopCore<F, I, B> {
    rt: Arc<Runtime>,
    index: u16,
    fwd: F,
    inner: I,
    bwd: B,
    plan: HopPlan,
}

/// The parts of the layout one hop needs at run time, by path position.
struct HopPlan {
    local: bool,
    /// Segment of each forward-path position: outer, forward nodes, inner.
    fwd_segments: Vec<String>,
    /// Segment of each backward-path position: inner, backward nodes, outer.
    bwd_segments: Vec<String>,
    /// Intermediate backward runs to open, in opening order, by start
    /// position and segment.
    provisioning: Vec<(usize, String)>,
    /// First backward position the outer end applies itself.
    outer_start: usize,
    /// Backward position where the inner end's own run stops.
    inner_end: usize,
}

impl HopPlan {
    fn new(hop: &HopLayout) -> Self {
        let expand = |runs: &[crate::placement::Run]| {
            runs.iter()
                .flat_map(|run| run.stages.iter().map(|_| run.segment.clone()))
                .collect::<Vec<_>>()
        };
        let runs = &hop.backward_runs;
        let last = runs.len() - 1;
        Self {
            local: hop.local,
            fwd_segments: expand(&hop.forward_runs),
            bwd_segments: expand(runs),
            provisioning: hop
                .provisioning
                .iter()
                .map(|&run| (runs[run].start, runs[run].segment.clone()))
                .collect(),
            // When the response never crosses a segment (the forward path
            // left and came back), the inner end sends its own output to the
            // outer end's anchor, and the outer end applies every backward
            // node itself.
            outer_start: if last > 0 { runs[last].start } else { 1 },
            inner_end: if last > 0 { runs[1].start } else { 1 },
        }
    }

    /// The first forward position after `pos` that is not in `pos`'s segment.
    fn fwd_end(&self, pos: usize) -> usize {
        run_end(&self.fwd_segments, pos)
    }

    /// Whether a forward run starting at `pos` reaches the inner end.
    fn reaches_inner(&self, pos: usize) -> bool {
        self.fwd_end(pos) == self.fwd_segments.len()
    }

    /// The first backward position after `pos` that is not in `pos`'s segment.
    fn bwd_end(&self, pos: usize) -> usize {
        run_end(&self.bwd_segments, pos)
    }
}

fn run_end(segments: &[String], pos: usize) -> usize {
    (pos + 1..segments.len())
        .find(|&q| segments[q] != segments[pos])
        .unwrap_or(segments.len())
}

impl<F, I, B> DHop<F, I, B> {
    pub(crate) fn new(rt: Arc<Runtime>, index: u16, fwd: F, inner: I, bwd: B) -> Result<Self> {
        let hop = rt.layout.hops().get(usize::from(index)).ok_or_else(|| {
            Error::new(
                ErrorKind::Placement,
                format!("the layout has no hop {index}"),
            )
        })?;
        let plan = HopPlan::new(hop);
        Ok(Self(Arc::new(HopCore {
            rt,
            index,
            fwd,
            inner,
            bwd,
            plan,
        })))
    }
}

impl<F, I, B> Next<F::In, B::Out> for DHop<F, I, B>
where
    F: DForward,
    I: DInner<Req = F::Out>,
    B: DBackward<In = I::Item>,
{
    async fn generate(&self, cx: Cx, req: F::In) -> Result<Items<B::Out>> {
        // As in local composition: every attempt spends cooperative budget.
        tokio::task::coop::consume_budget().await;
        cx.check()?;
        let attempt = cx.child();
        // Dropping this future before it returns cancels the attempt; after
        // it returns, the response stream owns the guard.
        let guard = attempt.cancel_on_drop();
        let core = &self.0;
        if core.plan.local {
            let items = tokio::select! {
                biased;
                reason = attempt.done() => return Err(reason),
                result = core.run_local(&attempt, req) => result?,
            };
            attempt.check()?;
            return Ok(bound(&attempt, guard, items));
        }
        // Remote work that no cancel can reach ends only by its deadline, so
        // no attempt crosses segments without one.
        let attempt = match attempt.deadline() {
            Some(_) => attempt,
            None => attempt.with_deadline(Deadline::after(core.rt.config.default_deadline)),
        };
        let slot = core.rt.admission.attempt(attempt.id().as_u128())?;
        let items = tokio::select! {
            biased;
            reason = attempt.done() => return Err(reason),
            result = core.run_remote(&attempt, req) => result?,
        };
        attempt.check()?;
        // The attempt slot is held for as long as the response stream lives.
        let items = items_holding(items, slot);
        Ok(bound(&attempt, guard, items))
    }
}

impl<F, I, B> Register for DHop<F, I, B>
where
    F: DForward,
    I: DInner<Req = F::Out>,
    B: DBackward<In = I::Item>,
{
    fn register(&self, entries: &mut Entries) {
        let core = &self.0;
        let here = core.rt.segment.as_str();
        // Forward hand-offs can arrive at any hosted forward position after
        // the outer end; prepared runs can start at any hosted backward
        // position strictly between the inner and the outer end.
        for pos in 1..core.plan.fwd_segments.len() {
            if core.plan.fwd_segments[pos] == here {
                let hop = Arc::clone(core);
                entries.add_forward(
                    core.index,
                    pos as u16,
                    Arc::new(move |envelope| {
                        let hop = Arc::clone(&hop);
                        Box::pin(async move { hop.accept_forward(envelope).await })
                    }),
                );
            }
        }
        for pos in 1..core.plan.bwd_segments.len().saturating_sub(1) {
            if core.plan.bwd_segments[pos] == here {
                let hop = Arc::clone(core);
                entries.add_open(
                    core.index,
                    pos as u16,
                    Arc::new(move |open| {
                        let hop = Arc::clone(&hop);
                        Box::pin(async move { hop.accept_open(open).await })
                    }),
                );
            }
        }
        core.inner.register(entries);
    }
}

impl<F, I, B> HopCore<F, I, B>
where
    F: DForward,
    I: DInner<Req = F::Out>,
    B: DBackward<In = I::Item>,
{
    /// Every stage is in this segment: a nested call, as in local
    /// composition.
    async fn run_local(&self, attempt: &Cx, req: F::In) -> Result<Items<B::Out>> {
        let value = match self.fwd.walk(1, usize::MAX, attempt, req).await? {
            FwdExit::Here(value) => value,
            FwdExit::Leave { .. } => return Err(internal("a local hop left its segment")),
        };
        attempt.check()?;
        let scope = attempt.child();
        let items = self.inner.call(scope.clone(), value).await?;
        let items = fuse(items, &scope);
        attempt.check()?;
        match self.bwd.walk(1, usize::MAX, attempt, items) {
            BwdExit::Here(items) => Ok(items),
            BwdExit::Leave { .. } => Err(internal("a local hop left its segment")),
        }
    }

    /// The outer end's side of a hop that leaves its segment.
    async fn run_remote(self: &Arc<Self>, attempt: &Cx, req: F::In) -> Result<Items<B::Out>> {
        let rt = &self.rt;
        let (mut inbound, mut reply) = rt.fabric.mint();
        // The caller may hold the response stream without reading it; the
        // anchor must still be released at the deadline, not when the stream
        // is next polled or finally dropped.
        if let Some(deadline) = attempt.deadline() {
            let watcher = inbound.canceller();
            let ended = attempt.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = tokio::time::sleep_until(deadline.instant()) => watcher.cancel(),
                    () = ended.cancelled() => {}
                }
            });
        }
        // Open the intermediate runs nearest-to-the-outer-end first, so each
        // run's reply anchor already exists when it attaches. A failure
        // returns here; dropping `inbound` then cancels every run already
        // opened, through the stream each one attached.
        for (pos, segment) in &self.plan.provisioning {
            reply = if *segment == rt.segment {
                // A child scope: the run's end must not end the attempt.
                self.open_run(attempt.child(), *pos, reply, None).await?
            } else {
                let open = Open {
                    version: VERSION,
                    identity: rt.identity,
                    hop: self.index,
                    pos: *pos as u16,
                    cx: WireCx::of(attempt),
                    reply,
                };
                rt.send_open(segment, open).await?
            };
        }
        match self.fwd.walk(1, self.plan.fwd_end(0), attempt, req).await? {
            FwdExit::Here(value) => {
                attempt.check()?;
                let egress = rt.egress.acquire().await?;
                let hop = Arc::clone(self);
                // A child scope: the inner end's task cancels its scope when
                // it ends, which must not end the attempt reading its output.
                let cx = attempt.child();
                tokio::spawn(async move { hop.run_inner(cx, value, reply, None, egress).await });
            }
            FwdExit::Leave { at, bytes } => {
                let envelope = Forward {
                    version: VERSION,
                    identity: rt.identity,
                    hop: self.index,
                    pos: at as u16,
                    cx: WireCx::of(attempt),
                    reply,
                    payload: bytes,
                };
                rt.send_forward(&self.plan.fwd_segments[at], envelope)
                    .await?;
            }
        }
        // As in local composition, no backward node of the outer end runs
        // before the inner end has its stream: wait for the open signal, or
        // return the failure that came instead.
        let mut stop_sent = false;
        loop {
            tokio::select! {
                biased;
                reason = attempt.done() => return Err(reason),
                () = attempt.stopping(), if !stop_sent => {
                    stop_sent = true;
                    inbound.request_stop();
                }
                frame = inbound.next() => match frame {
                    Some(Frame::Open) => break,
                    Some(Frame::Fail(error) | Frame::Broken(error)) => return Err(error),
                    Some(Frame::End) | None => {
                        return Err(truncated("the producer ended before opening"));
                    }
                    Some(Frame::Item(_)) => {
                        return Err(internal("an item arrived before the open signal"));
                    }
                },
            }
        }
        let bytes = inbound_items(inbound, Some(attempt.clone()));
        match self
            .bwd
            .enter(1, self.plan.outer_start, usize::MAX, attempt, bytes)?
        {
            BwdExit::Here(items) => Ok(items),
            BwdExit::Leave { .. } => Err(internal("the outer end's run left its segment")),
        }
    }

    /// A forward hand-off arrived. Acknowledge once it is admitted, then run
    /// it; from here until it hands the request on, this stage owns the
    /// reply and must deliver any failure to it.
    async fn accept_forward(self: &Arc<Self>, envelope: Forward) -> Result<()> {
        let rt = &self.rt;
        #[cfg(feature = "fault-injection")]
        if let Some(delay) = rt.config.faults.delay_forward_arrival {
            tokio::time::sleep(delay).await;
        }
        rt.check_identity(envelope.version, envelope.identity)?;
        let share = rt.admission.admit(envelope.cx.root()).await?;
        let cx = envelope.cx.arrive(rt.config.clock_skew, || rt.wall_now())?;
        // A run that ends at the inner end here will produce a stream; it
        // takes its egress permit before acknowledging, so a full instance
        // refuses instead of stalling.
        let egress = if self.plan.reaches_inner(usize::from(envelope.pos)) {
            Some(rt.egress.acquire().await?)
        } else {
            None
        };
        rt.served
            .forwards
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let hop = Arc::clone(self);
        tokio::spawn(async move {
            // However this task ends, the request's work here ends with it.
            let _end = cx.cancel_on_drop();
            let pos = usize::from(envelope.pos);
            let end = hop.plan.fwd_end(pos);
            let walked = tokio::select! {
                biased;
                reason = cx.done() => Err(reason),
                walked = hop.fwd.enter(1, pos, end, &cx, &envelope.payload) => walked,
            };
            match walked {
                Ok(FwdExit::Here(value)) => match egress {
                    Some(egress) => {
                        hop.run_inner(cx, value, envelope.reply, Some(share), egress)
                            .await;
                    }
                    None => {
                        let error = internal("a run reached an inner end it did not expect");
                        hop.fail(&envelope.reply, error).await;
                    }
                },
                Ok(FwdExit::Leave { at, bytes }) => {
                    let onward = Forward {
                        version: VERSION,
                        identity: hop.rt.identity,
                        hop: hop.index,
                        pos: at as u16,
                        cx: WireCx::of(&cx),
                        reply: envelope.reply.clone(),
                        payload: bytes,
                    };
                    let sent = hop
                        .rt
                        .send_forward(&hop.plan.fwd_segments[at], onward)
                        .await;
                    // The hand-off is complete: the next stage owns the reply.
                    drop(share);
                    if let Err(error) = sent {
                        hop.fail(&envelope.reply, error).await;
                    }
                }
                Err(error) => hop.fail(&envelope.reply, error).await,
            }
        });
        #[cfg(feature = "fault-injection")]
        if let Some(delay) = self.rt.config.faults.delay_forward_ack {
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    /// A request to open a backward run arrived.
    async fn accept_open(self: &Arc<Self>, open: Open) -> Result<ReplyTo> {
        let rt = &self.rt;
        rt.check_identity(open.version, open.identity)?;
        let share = rt.admission.admit(open.cx.root()).await?;
        let cx = open.cx.arrive(rt.config.clock_skew, || rt.wall_now())?;
        let input = self
            .open_run(cx, usize::from(open.pos), open.reply, Some(share))
            .await?;
        #[cfg(feature = "fault-injection")]
        if let Some(delay) = rt.config.faults.delay_open_reply {
            tokio::time::sleep(delay).await;
        }
        Ok(input)
    }

    /// Prepare the backward run that starts at `pos`: attach to its reply
    /// first (so a cancel from downstream reaches it at once), mint its input
    /// anchor, and wait for its producer's `Open` before running anything.
    async fn open_run(
        self: &Arc<Self>,
        cx: Cx,
        pos: usize,
        reply: ReplyTo,
        share: Option<super::admission::Share>,
    ) -> Result<ReplyTo> {
        let rt = &self.rt;
        let egress = rt.egress.acquire().await?;
        let out = tokio::time::timeout(rt.config.setup_timeout, rt.fabric.attach(&reply))
            .await
            .map_err(|_| setup_timeout("attach"))??;
        rt.served
            .runs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (inbound, input) = rt.fabric.mint();
        let hop = Arc::clone(self);
        let end = self.plan.bwd_end(pos);
        tokio::spawn(async move {
            let _end = cx.cancel_on_drop();
            hop.run_prepared(cx, pos, end, inbound, out).await;
            drop(share);
            drop(egress);
        });
        Ok(input)
    }

    async fn run_prepared(
        &self,
        cx: Cx,
        pos: usize,
        end: usize,
        mut inbound: Inbound,
        out: Outbound,
    ) {
        let cancel = out.cancel_token();
        let stop = out.stop_token();
        let mut stopped = false;
        let first = loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = stop.cancelled(), if !stopped => {
                    stopped = true;
                    inbound.request_stop();
                }
                reason = cx.done() => return finish_failed(out, reason).await,
                frame = inbound.next() => break frame,
            }
        };
        match first {
            // The open signal travels on: the next run, or the outer end,
            // activates only once the inner end's stream exists.
            Some(Frame::Open) => {
                if out.send(WireItem::Open).await.is_err() {
                    return;
                }
            }
            Some(Frame::Fail(error) | Frame::Broken(error)) => {
                return finish_failed(out, error).await;
            }
            Some(Frame::End) | None => {
                return finish_failed(out, truncated("the run's producer ended before opening"))
                    .await;
            }
            Some(Frame::Item(_)) => {
                return finish_failed(out, internal("an item arrived before the open signal"))
                    .await;
            }
        }
        let upstream = inbound.canceller();
        let bytes = inbound_items(inbound, None);
        match self.bwd.enter(1, pos, end, &cx, bytes) {
            Ok(BwdExit::Leave { bytes, .. }) => pump(bytes, out, &cx, Some(upstream)).await,
            Ok(BwdExit::Here(_)) => {
                finish_failed(out, internal("a prepared run reached the outer end")).await;
            }
            Err(error) => finish_failed(out, error).await,
        }
    }

    /// Run the inner end here and send its output, through this segment's
    /// part of the backward path, to `reply`.
    async fn run_inner(
        &self,
        cx: Cx,
        value: I::Req,
        reply: ReplyTo,
        share: Option<super::admission::Share>,
        egress: super::admission::EgressPermit,
    ) {
        // However this task ends, the request's work here ends with it,
        // including anything the inner end started under its context.
        let _end = cx.cancel_on_drop();
        let _share = share;
        #[cfg(feature = "fault-injection")]
        if self.rt.config.faults.skip_inner_attach {
            cx.done().await;
            return;
        }
        let attach =
            tokio::time::timeout(self.rt.config.setup_timeout, self.rt.fabric.attach(&reply));
        // Losing the attach means another stage already owns the reply (or it
        // is gone); this stage exits and its work ends with it.
        let Ok(Ok(out)) = attach.await else {
            return;
        };
        self.produce_inner(&cx, value, out).await;
        drop(egress);
    }

    async fn produce_inner(&self, cx: &Cx, value: I::Req, out: Outbound) {
        let cancel = out.cancel_token();
        let stop = out.stop_token();
        let scope = cx.child();
        // Armed before the node runs: an early exit (failure, interruption)
        // cancels whatever the node started under its context.
        let armed = scope.cancel_on_drop();
        let call = self.inner.call(scope.clone(), value);
        tokio::pin!(call);
        let mut stopped = false;
        let produced = loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                // A stop while the node is still setting up its stream is
                // passed on, and the node is left to finish gracefully.
                () = stop.cancelled(), if !stopped => {
                    stopped = true;
                    cx.request_stop();
                }
                reason = cx.done() => return finish_failed(out, reason).await,
                produced = &mut call => break produced,
            }
        };
        let items = match produced {
            Ok(items) => {
                armed.disarm();
                fuse(items, &scope)
            }
            Err(error) => return finish_failed(out, error).await,
        };
        // The inner end's stream exists: downstream runs may activate.
        if out.send(WireItem::Open).await.is_err() {
            return;
        }
        match self.bwd.walk(1, self.plan.inner_end, cx, items) {
            BwdExit::Leave { bytes, .. } => pump(bytes, out, cx, None).await,
            BwdExit::Here(_) => {
                finish_failed(out, internal("the inner end's run reached the outer end")).await;
            }
        }
    }

    /// Deliver a failure to a reply this stage owns.
    async fn fail(&self, reply: &ReplyTo, error: Error) {
        let attach =
            tokio::time::timeout(self.rt.config.setup_timeout, self.rt.fabric.attach(reply));
        if let Ok(Ok(out)) = attach.await {
            finish_failed(out, error).await;
        }
    }
}

/// Send items to a consumer until the stream ends, the consumer cancels, or
/// the request's deadline passes. A stop or cancel from the consumer is
/// passed upstream: to `cx`, which the stages feeding this pump run under,
/// and to `upstream`, the anchor this run reads from.
async fn pump(mut bytes: Items<Vec<u8>>, out: Outbound, cx: &Cx, upstream: Option<Canceller>) {
    let upstream = Upstream {
        cx,
        anchor: upstream,
    };
    let cancel = out.cancel_token();
    let stop = out.stop_token();
    let mut stopped = false;
    loop {
        let item = tokio::select! {
            biased;
            () = cancel.cancelled() => return upstream.cancel(),
            () = stop.cancelled(), if !stopped => {
                stopped = true;
                upstream.request_stop();
                continue;
            }
            reason = cx.done() => return finish_failed(out, reason).await,
            item = bytes.next() => item,
        };
        match item {
            Some(Ok(item)) => {
                match deliver(&out, WireItem::Item(item), &stop, &mut stopped, &upstream).await {
                    Delivery::Sent => {}
                    Delivery::Gone => return upstream.cancel(),
                    Delivery::Interrupted(reason) => return finish_failed(out, reason).await,
                }
            }
            Some(Err(error)) => return finish_failed(out, error).await,
            None => return out.finalize(),
        }
    }
}

/// What a pump stops or cancels when its consumer does.
struct Upstream<'a> {
    cx: &'a Cx,
    anchor: Option<Canceller>,
}

impl Upstream<'_> {
    fn cancel(&self) {
        self.cx.cancel();
        if let Some(anchor) = &self.anchor {
            anchor.cancel();
        }
    }

    fn request_stop(&self) {
        self.cx.request_stop();
        if let Some(anchor) = &self.anchor {
            anchor.request_stop();
        }
    }
}

enum Delivery {
    Sent,
    /// The consumer cancelled or went away.
    Gone,
    /// The request's deadline passed, or it was cancelled, while the send
    /// waited for a consumer that is not reading.
    Interrupted(Error),
}

/// Send one item. A send with room completes in one poll; only a send that
/// has to wait for the consumer also watches stop, cancel, and the deadline,
/// so a stalled consumer cannot hold this stage past its deadline and the
/// common path builds no timer.
async fn deliver(
    out: &Outbound,
    item: WireItem,
    stop: &tokio_util::sync::CancellationToken,
    stopped: &mut bool,
    upstream: &Upstream<'_>,
) -> Delivery {
    let send = out.send(item);
    tokio::pin!(send);
    if let Poll::Ready(sent) = futures::poll!(&mut send) {
        return if sent.is_ok() {
            Delivery::Sent
        } else {
            Delivery::Gone
        };
    }
    loop {
        tokio::select! {
            biased;
            reason = upstream.cx.done() => return Delivery::Interrupted(reason),
            () = stop.cancelled(), if !*stopped => {
                *stopped = true;
                upstream.request_stop();
            }
            sent = &mut send => {
                return if sent.is_ok() { Delivery::Sent } else { Delivery::Gone };
            }
        }
    }
}

/// How long a stage tries to deliver its terminal failure. A consumer that is
/// not reading would otherwise hold the stage forever; past this, the stage
/// drops its sender and the consumer sees the stream break instead.
const FAIL_DELIVERY: std::time::Duration = std::time::Duration::from_millis(100);

async fn finish_failed(out: Outbound, error: Error) {
    if let Ok(Ok(())) = tokio::time::timeout(FAIL_DELIVERY, out.send(WireItem::Fail(error))).await {
        out.finalize();
    }
}

/// An inbound anchor as a stream of encoded items. With `stop_from`, a stop
/// requested on that context is passed to the anchor's producer.
fn inbound_items(inbound: Inbound, stop_from: Option<Cx>) -> Items<Vec<u8>> {
    let stopping: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = stop_from.map(|cx| {
        Box::pin(async move { cx.stopping().await }) as Pin<Box<dyn Future<Output = ()> + Send>>
    });
    items(InboundItems { inbound, stopping })
}

struct InboundItems {
    inbound: Inbound,
    stopping: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Stream for InboundItems {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(stopping) = self.stopping.as_mut()
            && stopping.as_mut().poll(cx).is_ready()
        {
            self.stopping = None;
            self.inbound.request_stop();
        }
        loop {
            return match Pin::new(&mut self.inbound).poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Some(Frame::Open)) => continue,
                Poll::Ready(Some(Frame::Item(bytes))) => Poll::Ready(Some(Ok(bytes))),
                Poll::Ready(Some(Frame::Fail(error) | Frame::Broken(error))) => {
                    Poll::Ready(Some(Err(error)))
                }
                Poll::Ready(Some(Frame::End) | None) => Poll::Ready(None),
            };
        }
    }
}

fn items_holding<T: Send + 'static>(inner: Items<T>, held: impl Send + 'static) -> Items<T> {
    items(inner.map(move |item| {
        let _held = &held;
        item
    }))
}

fn internal(why: &str) -> Error {
    Error::new(ErrorKind::Placement, format!("internal: {why}"))
}

fn truncated(why: &str) -> Error {
    Error::new(ErrorKind::Truncated, why)
}

fn setup_timeout(step: &str) -> Error {
    Error::new(
        ErrorKind::Transport,
        format!("{step} did not finish in time"),
    )
}
