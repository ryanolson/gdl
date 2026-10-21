//! A hop of a distributed graph: one typed structure that runs as a nested
//! call when its stages share a segment, and across segments otherwise.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Future, Stream, StreamExt};

use super::admission::{EgressPermit, Share};
use super::envelope::{Accepted, Forward, Open, VERSION, WireCx, WireItem};
use super::fabric::{Canceller, Frame, Inbound, Outbound, ReplyTo, Retirer};
use super::runtime::{Entries, Runtime};
use super::stages::{BwdExit, DBackward, DForward, DInner, FwdExit};
use crate::cx::{Cx, Deadline};
use crate::error::{Error, ErrorKind, Result};
use crate::items::{CHECK_INTERVAL, Items, bound_measured, items};
use crate::meter::{Meter, Outcome};
use crate::role::Next;
use gdl_model::HopLayout;

/// Registers the entries a hop, and every hop nested under it, hosts in this
/// segment.
pub trait Register {
    #[doc(hidden)]
    fn register(&self, entries: &mut Entries);
}

impl Register for () {
    fn register(&self, _: &mut Entries) {}
}
impl<H: Register, T: Register> Register for (H, T) {
    fn register(&self, entries: &mut Entries) {
        self.0.register(entries);
        self.1.register(entries);
    }
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
    /// The source's figures, on the hop whose outer end is the graph's
    /// source in this segment. Another hop's outer end is an operator,
    /// metered where it is called as an inner end: its calls to `next`
    /// are attempts, not requests.
    source: Option<&'static Meter>,
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
    /// Whether each forward edge carries a streamed request, outer end
    /// first.
    fwd_streamed: Vec<bool>,
}

impl HopPlan {
    fn new(hop: &HopLayout) -> Self {
        let expand = |runs: &[gdl_model::Run]| {
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
            fwd_streamed: hop.request_streams.clone(),
        }
    }

    /// Whether the edge entering forward position `pos` carries a streamed
    /// request: a run that starts there reads its input from an anchor.
    fn streamed_into(&self, pos: usize) -> bool {
        self.fwd_streamed[pos - 1]
    }

    /// Whether a forward run starting at `pos` hands a streamed request on
    /// to another segment, and so produces the next run's input.
    fn relays(&self, pos: usize) -> bool {
        !self.reaches_inner(pos) && self.fwd_streamed[self.fwd_end(pos) - 1]
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

impl<F: DForward, I: DInner, B: DBackward> DHop<F, I, B> {
    pub(crate) fn new(
        rt: Arc<Runtime>,
        index: u16,
        mut fwd: F,
        mut inner: I,
        mut bwd: B,
    ) -> Result<Self> {
        let hop = rt.layout.hops().get(usize::from(index)).ok_or_else(|| {
            Error::new(
                ErrorKind::Placement,
                format!("the layout has no hop {index}"),
            )
        })?;
        // A placeholder answers every call with `NotHosted`: in a slot this
        // segment hosts, it would fail each request instead of the start.
        // Slots are checked by position, so the error names the node the
        // topology puts there, whatever placeholder fills it.
        let path = |runs: &[gdl_model::Run]| -> Vec<(String, String)> {
            runs.iter()
                .flat_map(|run| {
                    run.stages
                        .iter()
                        .map(|stage| (run.segment.clone(), stage.clone()))
                })
                .collect()
        };
        let forward = path(&hop.forward_runs);
        let backward = path(&hop.backward_runs);
        let mut forward_absent = Vec::new();
        fwd.absent(1, &mut forward_absent);
        if inner.absent() {
            forward_absent.push(forward.len() - 1);
        }
        let mut backward_absent = Vec::new();
        bwd.absent(1, &mut backward_absent);
        let unbound = forward_absent
            .iter()
            .filter_map(|&pos| forward.get(pos))
            .chain(backward_absent.iter().filter_map(|&pos| backward.get(pos)))
            .find(|(segment, _)| *segment == rt.segment);
        if let Some((_, name)) = unbound {
            return Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "segment `{}` hosts node `{name}`, but its binding is absent",
                    rt.segment
                ),
            ));
        }
        // The bindings' codecs and the manifest must agree on which forward
        // edges carry a stream: a streamed request split by a value codec,
        // or the reverse, would fail on every request that crosses there.
        let mut streamed = Vec::new();
        fwd.streamed(&mut streamed);
        if streamed != hop.request_streams {
            return Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "hop {index}: the bindings stream requests on edges {streamed:?}, the manifest on {:?}",
                    hop.request_streams
                ),
            ));
        }
        // Each node this segment hosts gets its figures by its position.
        let meter_at = |path: &[(String, String)], pos: usize| {
            let (segment, node) = path.get(pos)?;
            (*segment == rt.segment).then_some(())?;
            rt.meters.as_ref()?.get(node)
        };
        fwd.meter(1, &|pos| meter_at(&forward, pos));
        inner.meter(meter_at(&forward, forward.len() - 1));
        bwd.meter(1, &|pos| meter_at(&backward, pos));
        // The source is the one outer end that is no hop's inner end.
        let outer = &forward[0].1;
        let source = rt
            .layout
            .hops()
            .iter()
            .all(|hop| hop.forward_runs.last().and_then(|run| run.stages.last()) != Some(outer))
            .then(|| meter_at(&forward, 0))
            .flatten();
        let plan = HopPlan::new(hop);
        Ok(Self(Arc::new(HopCore {
            rt,
            index,
            fwd,
            inner,
            bwd,
            plan,
            source,
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
        // The source's request, when this hop's outer end is the source,
        // noted at each exit of this one function. A wrapping function
        // would put this future inside another, and moving it whole cost
        // a request 8,000 instructions (`agent-docs/perf/m21-metrics.md`).
        let mut source = Outcome::start(self.0.source, &cx);
        // As in local composition: every attempt spends cooperative budget.
        tokio::task::coop::consume_budget().await;
        source.note(cx.check())?;
        let attempt = cx.child();
        // Dropping this future before it returns cancels the attempt; after
        // it returns, the response stream owns the guard.
        let guard = attempt.cancel_on_drop();
        let core = &self.0;
        if core.plan.local {
            let items = tokio::select! {
                biased;
                reason = attempt.done() => return source.note(Err(reason)),
                result = core.run_local(&attempt, req) => source.note(result)?,
            };
            source.note(attempt.check())?;
            return Ok(bound_measured(&attempt, guard, items, source.reading()));
        }
        // Remote work that no cancel can reach ends only by its deadline, so
        // no attempt crosses segments without one.
        let attempt = match attempt.deadline() {
            Some(_) => attempt,
            None => attempt.with_deadline(Deadline::after(core.rt.config.default_deadline)),
        };
        source.deadline(&attempt);
        let slot = source.note(core.rt.admission.attempt(attempt.id().as_u128()))?;
        let items = tokio::select! {
            biased;
            reason = attempt.done() => return source.note(Err(reason)),
            result = core.run_remote(&attempt, req) => source.note(result)?,
        };
        source.note(attempt.check())?;
        // The attempt slot is held for as long as the response stream lives.
        let items = items_holding(items, slot);
        Ok(bound_measured(&attempt, guard, items, source.reading()))
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
        let items = self.inner.call(&scope, value).await?;
        attempt.check()?;
        match self.bwd.walk(1, usize::MAX, attempt, items) {
            BwdExit::Here(items) => Ok(items),
            BwdExit::Leave { .. } => Err(internal("a local hop left its segment")),
        }
    }

    /// The outer end's side of a hop that leaves its segment.
    async fn run_remote(self: &Arc<Self>, attempt: &Cx, req: F::In) -> Result<Items<B::Out>> {
        let rt = &self.rt;
        let (inbound, mut reply) = rt.fabric.mint();
        let mut held_reply = rt.fabric.hold_reply(&reply);
        // From here on, a stop, a cancel, and the deadline must reach the
        // producer without waiting for anyone to read: during provisioning
        // and dispatch nothing reads yet, and afterwards the caller may hold
        // the response stream unread. A watcher forwards stop, and at the
        // deadline or cancel drops the anchor, releasing its buffer and
        // cancelling the producer. It ends with the attempt, which the
        // response stream cancels when it ends.
        let (mut inbound, retire) = inbound.share();
        let watched = attempt.clone();
        tokio::spawn(async move {
            let mut stopped = false;
            let reason = loop {
                tokio::select! {
                    biased;
                    reason = watched.done() => break reason,
                    () = watched.stopping(), if !stopped => {
                        stopped = true;
                        retire.request_stop();
                    }
                }
            };
            retire.retire(reason);
            // The rest of the attempt here (the outer end's backward nodes,
            // anything they started) ends with it, read or not. A deadline
            // still reads as a deadline: boundaries check it before cancel.
            watched.cancel();
        });
        // Open the intermediate runs nearest-to-the-outer-end first, so each
        // run's reply anchor already exists when it attaches. A failure
        // returns here; dropping `inbound` then cancels every run already
        // opened, through the stream each one attached.
        for (pos, segment) in &self.plan.provisioning {
            reply = if *segment == rt.segment {
                // A child scope: the run's end must not end the attempt.
                // Bounded as a remote opening is, by the setup timeout.
                let opening = self.open_run(attempt.child(), *pos, reply, None);
                setup(attempt, rt.config.setup_timeout, "open", opening).await?
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
            held_reply = {
                drop(held_reply);
                rt.fabric.hold_reply(&reply)
            };
        }
        let exit = self.fwd.walk(1, self.plan.fwd_end(0), attempt, req).await?;
        // As in local composition, nothing further starts for an interrupted
        // request. The last forward node may have ended the attempt, and a
        // hand-off sent now would start remote work under a context that
        // does not carry the cancel.
        attempt.check()?;
        match exit {
            FwdExit::Here(value) => {
                let egress = setup(
                    attempt,
                    rt.config.setup_timeout,
                    "egress",
                    rt.egress.acquire(),
                )
                .await?;
                let hop = Arc::clone(self);
                // A child scope: the inner end's task cancels its scope when
                // it ends, which must not end the attempt reading its output.
                let cx = attempt.child();
                tokio::spawn(async move { hop.run_inner(cx, value, reply, None, egress).await });
            }
            FwdExit::Leave { at, head, input } => {
                // An input pump is a producer. It takes its permit before the
                // hand-off, so a refused permit never leaves a receiver
                // waiting for input that will not come.
                let egress = match input {
                    Some(_) => Some(
                        setup(
                            attempt,
                            rt.config.setup_timeout,
                            "egress",
                            rt.egress.acquire(),
                        )
                        .await?,
                    ),
                    None => None,
                };
                let envelope = Forward {
                    version: VERSION,
                    identity: rt.identity,
                    hop: self.index,
                    pos: at as u16,
                    cx: WireCx::of(attempt),
                    reply,
                    payload: head,
                };
                let accepted = rt
                    .send_forward(&self.plan.fwd_segments[at], envelope)
                    .await?;
                match (input, accepted, egress) {
                    (None, None, _) => {}
                    // Started before waiting for the open signal: a sink may
                    // read its input before it returns its stream, and the
                    // signal follows that return.
                    (Some(input), Some(to), Some(egress)) => {
                        let rt = Arc::clone(rt);
                        // A child scope: a receiver that drops its input
                        // cancels the pump's scope, which must not end the
                        // attempt (the response decides its outcome). A stop
                        // from the receiver reaches the attempt itself, as
                        // a stop one stage asks for reaches every stage of a
                        // local attempt.
                        let attempt = attempt.clone();
                        let cx = attempt.child();
                        tokio::spawn(async move {
                            send_input(&rt, &cx, to, input, None, Some(&attempt)).await;
                            drop(egress);
                        });
                    }
                    _ => return Err(disagree()),
                }
            }
        }
        drop(held_reply);
        // As in local composition, no backward node of the outer end runs
        // before the inner end has its stream: wait for the open signal, or
        // return the failure that came instead.
        let first = tokio::select! {
            biased;
            reason = attempt.done() => return Err(reason),
            frame = inbound.next() => frame,
        };
        match first {
            Some(Frame::Open) => {}
            Some(Frame::Fail(error) | Frame::Broken(error)) => return Err(error),
            Some(Frame::End) | None => {
                return Err(truncated("the producer ended before opening"));
            }
            Some(Frame::Item(_)) => {
                return Err(internal("an item arrived before the open signal"));
            }
        }
        let bytes = inbound_items(inbound);
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
    /// reply and must deliver any failure to it. For a streamed request the
    /// acknowledgement names the input anchor this stage minted.
    async fn accept_forward(self: &Arc<Self>, envelope: Forward) -> Result<Accepted> {
        let rt = &self.rt;
        #[cfg(feature = "fault-injection")]
        if let Some(delay) = rt.config.faults.delay_forward_arrival {
            tokio::time::sleep(delay).await;
        }
        rt.check_identity(envelope.version, envelope.identity)?;
        // The context first: an expired request is refused before it takes
        // anything, and every wait below ends at its deadline.
        let root = envelope.cx.root();
        let cx = envelope.cx.arrive(rt.config.clock_skew, || rt.wall_now())?;
        let share = setup(
            &cx,
            rt.config.setup_timeout,
            "admission",
            rt.admission.admit(root),
        )
        .await?;
        let pos = usize::from(envelope.pos);
        // A run that will produce a stream (the inner end's output, or a
        // relay's onward input) takes its egress permit before
        // acknowledging, so a full instance refuses instead of stalling.
        let egress = if self.plan.reaches_inner(pos) || self.plan.relays(pos) {
            Some(setup(&cx, rt.config.setup_timeout, "egress", rt.egress.acquire()).await?)
        } else {
            None
        };
        // Minted last, after everything that can refuse: a refusal leaves
        // no anchor behind.
        let (input, guard, accepted) = if self.plan.streamed_into(pos) {
            let (inbound, reply) = rt.fabric.mint();
            let (shared, retirer) = inbound.share();
            rt.served
                .inputs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            (
                Some(inbound_items(shared)),
                Some(InputGuard(retirer)),
                Some(reply),
            )
        } else {
            (None, None, None)
        };
        rt.served
            .forwards
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Forward { payload, reply, .. } = envelope;
        let held_reply = rt.fabric.hold_reply(&reply);
        let hop = Arc::clone(self);
        tokio::spawn(async move {
            // However this task ends, the request's work here ends with it.
            let _held_reply = held_reply;
            let _end = cx.cancel_on_drop();
            let upstream = guard.as_ref().map(InputGuard::canceller);
            if let Some(input) = upstream.clone() {
                // A stop reaches this run's context from the stream it
                // produces, or from a node that asks for one, and travels on
                // up the request path to the stage that sends this run its
                // input. It does not end the input: the sender decides that.
                // A task of its own, and only for a streamed run: a branch
                // beside the run would register a waiter on every item.
                let watched = cx.clone();
                tokio::spawn(async move {
                    // `stopping` resolves on a cancel too, and a cancel is
                    // the run going away, not asking to stop: passed on as
                    // a stop, it would stop the outer end's attempt.
                    tokio::select! {
                        biased;
                        () = watched.cancelled() => {}
                        () = watched.stopping() => {
                            if !watched.is_cancelled() {
                                input.request_stop();
                            }
                        }
                    }
                });
            }
            // The walk of this segment's forward stages, then the inner end
            // here or a hand-off onward. Inline rather than an `async fn`,
            // whose future would store its arguments and its locals both,
            // in every receiving task.
            let end = hop.plan.fwd_end(pos);
            let walked = tokio::select! {
                biased;
                reason = cx.done() => Err(reason),
                walked = hop.fwd.enter(1, pos, end, &cx, &payload, input) => walked,
            };
            match walked {
                Ok(FwdExit::Here(value)) => match egress {
                    Some(egress) => {
                        hop.run_inner(cx, value, reply, Some(share), egress).await;
                    }
                    None => {
                        let error = internal("a run reached an inner end it did not expect");
                        hop.fail(&cx, &reply, error).await;
                    }
                },
                Ok(FwdExit::Leave { at, head, input }) => {
                    let onward = Forward {
                        version: VERSION,
                        identity: hop.rt.identity,
                        hop: hop.index,
                        pos: at as u16,
                        cx: WireCx::of(&cx),
                        reply: reply.clone(),
                        payload: head,
                    };
                    let sent =
                        bounded(&cx, hop.rt.send_forward(&hop.plan.fwd_segments[at], onward)).await;
                    match (sent, input, egress) {
                        // The next stage owns the reply now.
                        (Ok(None), None, _) => drop(share),
                        // A relay. The next stage owns the reply; this one keeps
                        // its admission share and its permit until it has sent
                        // the items on.
                        (Ok(Some(next)), Some(input), Some(egress)) => {
                            Box::pin(send_input(&hop.rt, &cx, next, input, upstream, None)).await;
                            drop((share, egress));
                        }
                        (Ok(_), _, _) => hop.fail(&cx, &reply, disagree()).await,
                        // This stage still owns the reply until the failure is
                        // delivered, and counts against admission until then.
                        (Err(error), _, _) => {
                            hop.fail(&cx, &reply, error).await;
                            drop(share);
                        }
                    }
                }
                Err(error) => hop.fail(&cx, &reply, error).await,
            }
            // Dropping the guard retires the input anchor.
            drop(guard);
        });
        #[cfg(feature = "fault-injection")]
        if let Some(delay) = self.rt.config.faults.delay_forward_ack {
            tokio::time::sleep(delay).await;
        }
        Ok(accepted)
    }

    /// A request to open a backward run arrived.
    async fn accept_open(self: &Arc<Self>, open: Open) -> Result<ReplyTo> {
        let rt = &self.rt;
        rt.check_identity(open.version, open.identity)?;
        let root = open.cx.root();
        let cx = open.cx.arrive(rt.config.clock_skew, || rt.wall_now())?;
        let share = setup(
            &cx,
            rt.config.setup_timeout,
            "admission",
            rt.admission.admit(root),
        )
        .await?;
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
        share: Option<Share>,
    ) -> Result<ReplyTo> {
        let rt = &self.rt;
        let egress = setup(&cx, rt.config.setup_timeout, "egress", rt.egress.acquire()).await?;
        let out = setup(
            &cx,
            rt.config.setup_timeout,
            "attach",
            rt.fabric.attach(&reply),
        )
        .await?;
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
                let upstream = Upstream {
                    cx: &cx,
                    anchor: Some(inbound.canceller()),
                    stops: None,
                };
                let (stop_open, done_open) = (stop.cancelled(), cx.done());
                tokio::pin!(stop_open, done_open);
                let watch = Watch {
                    stop: stop_open.as_mut(),
                    stopped: &mut stopped,
                    done: done_open.as_mut(),
                };
                match deliver(&out, WireItem::Open, watch, &upstream).await {
                    Delivery::Sent => {}
                    Delivery::Gone => return upstream.cancel(),
                    Delivery::Interrupted(reason) => return finish_failed(out, reason).await,
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
        let bytes = inbound_items(inbound);
        match self.bwd.enter(1, pos, end, &cx, bytes) {
            Ok(BwdExit::Leave { bytes, .. }) => pump(bytes, out, &cx, Some(upstream), None).await,
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
        share: Option<Share>,
        egress: EgressPermit,
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
        let attach = setup(
            &cx,
            self.rt.config.setup_timeout,
            "attach",
            self.rt.fabric.attach(&reply),
        );
        // Losing the attach means another stage already owns the reply (or it
        // is gone); this stage exits and its work ends with it.
        let Ok(out) = attach.await else {
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
        let call = self.inner.call(&scope, value);
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
                items
            }
            Err(error) => return finish_failed(out, error).await,
        };
        // The inner end's stream exists: downstream runs may activate.
        let upstream = Upstream {
            cx,
            anchor: None,
            stops: None,
        };
        let (stop_open, done_open) = (stop.cancelled(), cx.done());
        tokio::pin!(stop_open, done_open);
        let watch = Watch {
            stop: stop_open.as_mut(),
            stopped: &mut stopped,
            done: done_open.as_mut(),
        };
        match deliver(&out, WireItem::Open, watch, &upstream).await {
            Delivery::Sent => {}
            Delivery::Gone => return upstream.cancel(),
            Delivery::Interrupted(reason) => return finish_failed(out, reason).await,
        }
        match self.bwd.walk(1, self.plan.inner_end, cx, items) {
            BwdExit::Leave { bytes, .. } => pump(bytes, out, cx, None, None).await,
            BwdExit::Here(_) => {
                finish_failed(out, internal("the inner end's run reached the outer end")).await;
            }
        }
    }

    /// Deliver `error` to `reply`, unless the request is over: then its
    /// consumer has the reason already.
    async fn fail(&self, cx: &Cx, reply: &ReplyTo, error: Error) {
        #[cfg(feature = "fault-injection")]
        if let Some(gate) = &self.rt.config.faults.failure_gate {
            gate.arrived.notify_one();
            gate.release.notified().await;
        }
        let attach = setup(
            cx,
            self.rt.config.setup_timeout,
            "attach",
            self.rt.fabric.attach(reply),
        );
        if let Ok(out) = attach.await {
            finish_failed(out, error).await;
        }
    }
}

/// Send a streamed request's items into the input anchor its receiver
/// minted, until they end. A receiver that is already gone ends the pump
/// without a word, and cancels what feeds it: the response decides the
/// request's outcome.
///
/// `stops` is a context that a stop from the receiver reaches as well as
/// `cx`: the outer end's attempt, whose forward stages feed this pump under
/// the attempt itself, while a cancel must end only the pump.
async fn send_input(
    rt: &Runtime,
    cx: &Cx,
    to: ReplyTo,
    input: Items<Vec<u8>>,
    upstream: Option<Canceller>,
    stops: Option<&Cx>,
) {
    let attach = setup(cx, rt.config.setup_timeout, "attach", rt.fabric.attach(&to));
    let Ok(out) = attach.await else {
        if let Some(upstream) = upstream {
            upstream.cancel();
        }
        return;
    };
    pump(input, out, cx, upstream, stops).await;
}

/// Retires a run's input anchor when the run ends, however it ends. Nothing
/// else bounds that anchor: a ticket pauses its unattached timeout, a
/// sender whose acknowledgement was lost never attaches, and a node may hold
/// its input unread.
struct InputGuard(Retirer);

impl InputGuard {
    fn canceller(&self) -> Canceller {
        self.0.canceller()
    }
}

impl Drop for InputGuard {
    fn drop(&mut self) {
        self.0.retire(Error::cancelled());
    }
}

/// Send items to a consumer until the stream ends, the consumer cancels, or
/// the request's deadline passes. A stop or cancel from the consumer is
/// passed upstream: to `cx`, which the stages feeding this pump run under,
/// and to `upstream`, the anchor this run reads from.
///
/// Not an `async fn` itself: a second frame around `pump_watching` would
/// enlarge the future of every stream that crosses a segment.
fn pump<'a>(
    bytes: Items<Vec<u8>>,
    out: Outbound,
    cx: &'a Cx,
    upstream: Option<Canceller>,
    stops: Option<&'a Cx>,
) -> impl Future<Output = ()> + Send + 'a {
    // Built once per stream: a fresh future registers again when polled,
    // the deadline with the runtime's shared timer and the tokens in their
    // waiter lists, which would be a lock per item that every stream of the
    // runtime contends on.
    let consumer = Tokens {
        cancel: out.cancel_token(),
        stop: out.stop_token(),
    };
    pump_watching(bytes, out, cx, upstream, stops, consumer)
}

/// Where a pump learns that its consumer cancelled or asked to stop. A
/// seam: a test can fire them in any order.
trait Consumer {
    fn cancelled(&self) -> impl Future<Output = ()> + Send + '_;
    fn stopped(&self) -> impl Future<Output = ()> + Send + '_;
}

/// The consumer's signals as velo gives them.
struct Tokens {
    cancel: tokio_util::sync::CancellationToken,
    stop: tokio_util::sync::CancellationToken,
}

impl Consumer for Tokens {
    fn cancelled(&self) -> impl Future<Output = ()> + Send + '_ {
        self.cancel.cancelled()
    }

    fn stopped(&self) -> impl Future<Output = ()> + Send + '_ {
        self.stop.cancelled()
    }
}

/// [`pump`], given where its consumer's signals come from.
async fn pump_watching(
    mut bytes: Items<Vec<u8>>,
    out: Outbound,
    cx: &Cx,
    upstream: Option<Canceller>,
    stops: Option<&Cx>,
    consumer: impl Consumer,
) {
    let upstream = Upstream {
        cx,
        anchor: upstream,
        stops,
    };
    let cancel = out.cancel_token();
    let cancelled = consumer.cancelled();
    let stop = consumer.stopped();
    let done = cx.done();
    tokio::pin!(cancelled, stop, done);
    let mut stopped = false;
    let mut ready_run: u8 = 0;
    loop {
        // An item already there is taken without polling the watchers. They
        // are polled every `CHECK_INTERVAL` items and whenever the input has
        // to wait, as at an attempt boundary: a ready producer can overrun a
        // cancel, a stop, or the deadline by at most that many items, and a
        // send that has to wait watches all of them (`deliver`).
        let ready = if ready_run < CHECK_INTERVAL {
            match futures::poll!(bytes.next()) {
                Poll::Ready(item) => Some(item),
                Poll::Pending => None,
            }
        } else {
            None
        };
        let item = match ready {
            Some(item) => {
                ready_run += 1;
                item
            }
            None => {
                ready_run = 0;
                tokio::select! {
                    biased;
                    () = &mut cancelled => return upstream.cancel(),
                    () = &mut stop, if !stopped => {
                        if left(&cancel) {
                            return upstream.cancel();
                        }
                        stopped = true;
                        upstream.request_stop();
                        continue;
                    }
                    reason = &mut done => return finish_failed(out, reason).await,
                    item = bytes.next() => item,
                }
            }
        };
        match item {
            Some(Ok(item)) => {
                let watch = Watch {
                    stop: stop.as_mut(),
                    stopped: &mut stopped,
                    done: done.as_mut(),
                };
                match deliver(&out, WireItem::Item(item), watch, &upstream).await {
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

/// Whether a consumer's stop token fired because the consumer cancelled.
/// velo fires the stop token on a cancel as well, and a cancel can land
/// between a select's poll of the cancel and its poll of the stop. A cancel
/// is the consumer leaving, not asking to stop: passed on as a stop, it
/// would stop what feeds the pump, the outer end's attempt included. The
/// cancel token is set before its child stop token fires, so a stop that
/// came with a cancel always finds it.
fn left(cancel: &tokio_util::sync::CancellationToken) -> bool {
    cancel.is_cancelled()
}

/// What a pump stops or cancels when its consumer does.
struct Upstream<'a> {
    cx: &'a Cx,
    anchor: Option<Canceller>,
    /// A context that a stop reaches as well, and a cancel does not.
    stops: Option<&'a Cx>,
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
        if let Some(stops) = self.stops {
            stops.request_stop();
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
async fn deliver<S, D>(
    out: &Outbound,
    item: WireItem,
    watch: Watch<'_, S, D>,
    upstream: &Upstream<'_>,
) -> Delivery
where
    S: Future<Output = ()>,
    D: Future<Output = Error>,
{
    let Watch {
        mut stop,
        stopped,
        mut done,
    } = watch;
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
            reason = done.as_mut() => return Delivery::Interrupted(reason),
            () = stop.as_mut(), if !*stopped => {
                if left(&out.cancel_token()) {
                    return Delivery::Gone;
                }
                *stopped = true;
                upstream.request_stop();
            }
            sent = &mut send => {
                return if sent.is_ok() { Delivery::Sent } else { Delivery::Gone };
            }
        }
    }
}

/// The stream-long futures a blocked send watches: the consumer's stop and
/// the request's end. The caller builds them once and lends them per send.
struct Watch<'a, S, D> {
    stop: Pin<&'a mut S>,
    stopped: &'a mut bool,
    done: Pin<&'a mut D>,
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

/// An inbound anchor as a stream of encoded items. A stop does not pass
/// through here: the watcher or the pump that holds the anchor forwards it.
fn inbound_items<S>(inbound: S) -> Items<Vec<u8>>
where
    S: Stream<Item = Frame> + Unpin + Send + 'static,
{
    items(InboundItems(inbound))
}

struct InboundItems<S>(S);

impl<S: Stream<Item = Frame> + Unpin> Stream for InboundItems<S> {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return match Pin::new(&mut self.0).poll_next(cx) {
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

fn disagree() -> Error {
    internal("a hand-off and its acknowledgement disagree on whether the request is streamed")
}

fn internal(why: &str) -> Error {
    Error::new(ErrorKind::Placement, format!("internal: {why}"))
}

fn truncated(why: &str) -> Error {
    Error::new(ErrorKind::Truncated, why)
}

/// A setup step, bounded by the request (its deadline and cancel) as well
/// as the setup timeout: setup work in another segment that outlives its
/// request holds that segment's admission share and task for nothing.
async fn setup<T>(
    cx: &Cx,
    limit: std::time::Duration,
    step: &str,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        biased;
        reason = cx.done() => Err(reason),
        done = tokio::time::timeout(limit, work) => done.map_err(|_| setup_timeout(step))?,
    }
}

/// A step that has its own bound, bounded by the request as well.
async fn bounded<T>(cx: &Cx, work: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::select! {
        biased;
        reason = cx.done() => Err(reason),
        done = work => done,
    }
}

fn setup_timeout(step: &str) -> Error {
    Error::new(
        ErrorKind::Transport,
        format!("{step} did not finish in time"),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::dist::fabric::{Fabric, bind_node};

    /// A consumer whose cancel the pump has not seen yet, and whose stop has
    /// fired.
    struct Racy;

    impl Consumer for Racy {
        fn cancelled(&self) -> impl Future<Output = ()> + Send + '_ {
            std::future::pending()
        }

        fn stopped(&self) -> impl Future<Output = ()> + Send + '_ {
            std::future::ready(())
        }
    }

    /// A pump whose consumer cancels between its cancel poll and its stop
    /// poll: velo fires the stop token with the cancel, so the pump sees a
    /// stop and a cancel that its first poll missed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pump_takes_a_stop_that_comes_with_a_cancel_for_the_cancel() {
        let node = bind_node(([127, 0, 0, 1], 0).into()).await.expect("node");
        let fabric = Fabric::new(
            node,
            Some(Duration::from_secs(5)),
            None,
            Duration::from_secs(30),
        );
        let (_inbound, reply) = fabric.mint();
        let out = fabric.attach(&reply).await.expect("attach");
        // The consumer has cancelled, which fired the stop token too, but
        // the pump's cancel future has not seen it yet.
        let cancel = out.cancel_token();
        cancel.cancel();
        let attempt = Cx::new();
        let scope = attempt.child();
        let input = items(futures::stream::pending::<Result<Vec<u8>>>());
        // Bounded: a pump that took the cancel for a stop goes on waiting
        // for its input.
        let pumped = pump_watching(input, out, &scope, None, Some(&attempt), Racy);
        let ended = tokio::time::timeout(Duration::from_secs(5), pumped).await;
        assert!(
            ended.is_ok(),
            "the pump did not end at the consumer's cancel"
        );
        assert!(scope.is_cancelled(), "the pump's scope must end");
        assert!(
            !attempt.is_stopping(),
            "a cancel from the consumer stopped what feeds the pump"
        );
    }

    /// The control: a stop alone reaches the attempt.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pump_passes_a_stop_alone_to_the_attempt() {
        let node = bind_node(([127, 0, 0, 1], 0).into()).await.expect("node");
        let fabric = Fabric::new(
            node,
            Some(Duration::from_secs(5)),
            None,
            Duration::from_secs(30),
        );
        let (_inbound, reply) = fabric.mint();
        let out = fabric.attach(&reply).await.expect("attach");
        let attempt = Cx::new();
        let scope = attempt.child();
        // A stop does not end the input: the pump goes on waiting for it.
        let input = items(futures::stream::pending::<Result<Vec<u8>>>());
        let pumped = pump_watching(input, out, &scope, None, Some(&attempt), Racy);
        let ended = tokio::time::timeout(Duration::from_millis(200), pumped).await;
        assert!(ended.is_err(), "a stop ended the pump");
        assert!(attempt.is_stopping(), "the stop did not reach the attempt");
        assert!(!attempt.is_cancelled());
    }
}
