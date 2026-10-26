//! A hop of a distributed graph: one typed structure that runs as a nested
//! call when its stages share a segment, and across segments otherwise.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Future, Stream, StreamExt};

use super::admission::{EgressPermit, Share};
use super::envelope::{Accepted, Forward, Open, VERSION, WireCx};
use super::fabric::{Canceller, Frame, Inbound, Outbound, Outgoing, ReplyTo, Retirer};
use super::offers::{Refusal, Refused};
use super::runtime::{Entries, Handed, Held, Router, Runtime};
use super::stages::{BwdExit, DBackward, DForward, DInner, FwdExit};
use gdl::__runtime as meter;
use gdl::__runtime::{CHECK_INTERVAL, bound_measured};
use gdl::__runtime::{END_TO_END, Ending, Figures, Meters, Owner, REQUEST, RESPONSE, Visit};
use gdl::Next;
use gdl::{Cx, Deadline};
use gdl::{Error, ErrorKind, Result};
use gdl::{Items, items};
use gdl_model::HopLayout;
use tokio::time::Instant;

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
    /// The figures of the entries this hop has in this segment, when the
    /// segment records them.
    figures: HopFigures,
    /// Whether the hop's hand-offs to this segment are offers from another
    /// class, which a draining cell's entry declines.
    offer: bool,
}

/// The figures of a hop's entries in one segment: where a visit to the
/// segment begins.
#[derive(Default)]
struct HopFigures {
    /// The root's, on the hop whose outer end is the graph's source.
    /// Another hop's outer end is an operator: its calls to `next` are
    /// attempts within the visit that called it.
    source: Option<&'static Figures>,
    /// A `gdl.fwd` arriving at each forward position, where a run starts
    /// here.
    forward: Vec<Option<&'static Figures>>,
    /// The inner end run the outer end spawns, when the forward path stays
    /// in its segment and the response crosses.
    inner: Option<&'static Figures>,
    /// A backward run that starts here, by its first position.
    backward: Vec<Option<&'static Figures>>,
}

impl HopFigures {
    fn new(
        meters: &Meters,
        here: &str,
        forward: &[(String, String)],
        backward: &[(String, String)],
        plan: &HopPlan,
        source: bool,
    ) -> Self {
        fn hosted<'a>(path: &'a [(String, String)], pos: usize, here: &str) -> Option<&'a str> {
            path.get(pos)
                .filter(|(segment, _)| segment == here)
                .map(|(_, node)| node.as_str())
        }
        let outer = hosted(forward, 0, here);
        HopFigures {
            source: outer
                .filter(|_| source)
                .map(|node| meters.entry(node, REQUEST | RESPONSE | END_TO_END)),
            // A hand-off arrives where a run starts: the walk that sends it
            // leaves at the first position outside its own segment.
            forward: (0..forward.len())
                .map(|pos| {
                    let node = hosted(forward, pos, here)?;
                    (pos > 0 && forward[pos - 1].0 != here).then(|| {
                        let parts = if plan.reaches_inner(pos) {
                            REQUEST | RESPONSE
                        } else {
                            REQUEST
                        };
                        meters.entry(node, parts)
                    })
                })
                .collect(),
            inner: (outer.is_some() && !plan.local && plan.reaches_inner(0))
                .then(|| hosted(forward, forward.len() - 1, here))
                .flatten()
                .map(|node| meters.entry(node, RESPONSE)),
            backward: (0..backward.len())
                .map(|pos| {
                    let opened = plan
                        .provisioning
                        .iter()
                        .any(|(start, segment)| *start == pos && segment == here);
                    let node = hosted(backward, pos, here).filter(|_| opened)?;
                    Some(meters.entry(node, RESPONSE))
                })
                .collect(),
        }
    }

    fn at(figures: &[Option<&'static Figures>], pos: usize) -> Option<&'static Figures> {
        figures.get(pos).copied().flatten()
    }
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
    /// How the hand-off into each forward position chooses its instance,
    /// resolved once; read only where a run in this segment hands on.
    routers: Vec<Router>,
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
            routers: Vec::new(),
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
    pub(crate) fn new(rt: Arc<Runtime>, index: u16, fwd: F, inner: I, bwd: B) -> Result<Self> {
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
        let mut plan = HopPlan::new(hop);
        // A selector that reads the head as a type must be given that
        // type: each crossing this segment sends is checked against its
        // edge's codec, which every slot has, `Absent` or not.
        let mut heads = Vec::new();
        fwd.heads(&mut heads);
        // A selector reads the head in its edge's encoding: the head that
        // crosses at `pos` is the one stage `pos - 1` sends.
        plan.routers = (0..forward.len())
            .map(|pos| {
                let encoding = pos
                    .checked_sub(1)
                    .map(|at| heads[at].encoding)
                    .unwrap_or_default();
                rt.router(usize::from(index), pos, encoding)
            })
            .collect();
        for pos in 1..forward.len() {
            let sends = forward[pos - 1].0 == rt.segment && forward[pos].0 != rt.segment;
            let Some((name, selector)) = plan.routers[pos].selector() else {
                continue;
            };
            let head = &heads[pos - 1];
            let Some((wants, wanted)) = selector.edge() else {
                continue;
            };
            if sends && wants != head.id {
                let carried = head.name;
                return Err(Error::new(
                    ErrorKind::Placement,
                    format!(
                        "segment `{}`: selector `{name}` reads the head as `{wanted}`, but the request that crosses into `{}` (hop {index}) carries `{carried}`",
                        rt.segment, forward[pos].1
                    ),
                ));
            }
            // A projection reads fields by name, which a positional
            // encoding does not carry: every read would fail.
            if sends && selector.by_name() && head.encoding == gdl_model::Encoding::MsgpackArray {
                return Err(Error::new(
                    ErrorKind::Placement,
                    format!(
                        "segment `{}`: selector `{name}` reads the head's fields by name, but the request that crosses into `{}` (hop {index}) is encoded as `msgpack_array`, which carries no names",
                        rt.segment, forward[pos].1
                    ),
                ));
            }
        }
        // A cell policy that reads the head as a type must be given that
        // type too, and one that reads its fields by name an encoding that
        // names them.
        #[cfg(feature = "hub")]
        for pos in 1..forward.len() {
            if let Router::Cells(crossing) = &plan.routers[pos] {
                let head = &heads[pos - 1];
                crossing.check_head(&rt.segment, head.id, head.name)?;
            }
        }
        // The source is the one outer end that is no hop's inner end.
        let outer = &forward[0].1;
        let source =
            rt.layout.hops().iter().all(|hop| {
                hop.forward_runs.last().and_then(|run| run.stages.last()) != Some(outer)
            });
        let figures = match &rt.meters {
            Some(meters) => {
                HopFigures::new(meters, &rt.segment, &forward, &backward, &plan, source)
            }
            None => HopFigures::default(),
        };
        let offer = rt.offered(usize::from(index));
        Ok(Self(Arc::new(HopCore {
            offer,
            rt,
            index,
            fwd,
            inner,
            bwd,
            plan,
            figures,
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
        let core = &self.0;
        // The root's visit, when this hop's outer end is the source, noted
        // at each exit of this one function. A wrapping function would put
        // this future inside another, and moving it whole cost a request
        // 8,000 instructions (see docs/src/operations/feature-costs.md).
        let called = core.figures.source.map(|figures| (figures, Instant::now()));
        // As in local composition: every attempt spends cooperative budget.
        tokio::task::coop::consume_budget().await;
        let attempt = cx.child();
        // Remote work that no cancel can reach ends only by its deadline, so
        // no attempt crosses segments without one.
        let attempt = match attempt.deadline() {
            None if !core.plan.local => {
                attempt.with_deadline(Deadline::after(core.rt.config.default_deadline))
            }
            _ => attempt,
        };
        let root =
            called.map(|(figures, at)| Visit::request(figures, at, attempt.deadline(), true));
        let attempt = match &root {
            Some(root) => attempt.with_visit(Some(Arc::clone(root.visit()))),
            None => attempt,
        };
        meter::note(root.as_ref(), attempt.check())?;
        // Dropping this future before it returns cancels the attempt; after
        // it returns, the response stream owns the guard.
        let guard = attempt.cancel_on_drop();
        if core.plan.local {
            let items = tokio::select! {
                biased;
                reason = attempt.done() => return meter::note(root.as_ref(), Err(reason)),
                result = core.run_local(&attempt, req) => meter::note(root.as_ref(), result)?,
            };
            meter::note(root.as_ref(), attempt.check())?;
            return Ok(bound_measured(&attempt, guard, items, root));
        }
        let slot = meter::note(
            root.as_ref(),
            core.rt.admission.attempt(attempt.id().as_u128()),
        )?;
        let (items, lease) = tokio::select! {
            biased;
            reason = attempt.done() => return meter::note(root.as_ref(), Err(reason)),
            result = core.run_remote(&attempt, req) => meter::note(root.as_ref(), result)?,
        };
        meter::note(root.as_ref(), attempt.check())?;
        // The attempt slot, and the hand-off's leases, are held for as long
        // as the response stream lives: one holder, so a lease adds no
        // wrapper.
        let items = items_holding(items, (slot, lease));
        Ok(bound_measured(&attempt, guard, items, root))
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
                        Box::pin(async move {
                            let accepted = hop.accept_forward(envelope).await;
                            if let Err(refused) = &accepted
                                && refused.error.kind() == ErrorKind::Overloaded
                            {
                                hop.rt
                                    .served
                                    .declined
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            accepted
                        })
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

    /// The outer end's side of a hop that leaves its segment, with what
    /// the hand-off holds (the lease of the selector that chose where it
    /// went, if one did, and a chooser's lease on the cell it offered the
    /// request to): the caller holds it as long as the response stream.
    async fn run_remote(
        self: &Arc<Self>,
        attempt: &Cx,
        req: F::In,
    ) -> Result<(Items<B::Out>, Held)> {
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
        // The source's visit, when this hop's outer end is the source: a
        // cancel or the deadline ends it here, though the caller may hold the
        // response unread. Only the source's own hop may: an operator's
        // attempt carries its visit too, and one attempt's end is not the
        // visit's. Whichever ends the visit first records it, once.
        let root = self.figures.source.and(attempt.visit().cloned());
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
            if let Some(root) = root {
                root.end(Ending::Failed(&reason));
            }
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
        let mut lease = Held::default();
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
                // The inner end's stream is one this segment sends on, with
                // a visit of its own; its sink's start ends the request
                // path of the visit the attempt belongs to.
                let owner = self.figures.inner.map(|figures| {
                    Visit::response(figures, attempt.deadline(), attempt.visit().cloned())
                });
                // A child scope: the inner end's task cancels its scope when
                // it ends, which must not end the attempt reading its output.
                let cx = attempt
                    .child()
                    .with_visit(owner.as_ref().map(|owner| Arc::clone(owner.visit())));
                tokio::spawn(async move {
                    hop.run_inner(cx, value, reply, None, egress, owner).await;
                });
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
                let Handed {
                    accepted,
                    sent,
                    held: chosen,
                } = rt
                    .send_forward(
                        &self.plan.fwd_segments[at],
                        &self.plan.routers[at],
                        attempt,
                        envelope,
                        attempt.visit().is_some(),
                    )
                    .await?;
                lease = chosen;
                // Handed on: the visit's request path ends where the
                // hand-off left, and its response begins.
                if let (Some(visit), Some(sent)) = (attempt.visit(), sent) {
                    visit.handed_on(true, sent);
                }
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
            // Raised in the segment that sent it, and counted there.
            Some(Frame::Fail(error)) => return Err(error.relayed()),
            Some(Frame::Broken(error)) => return Err(error),
            Some(Frame::End) | None => {
                return Err(truncated("the producer ended before opening"));
            }
            Some(Frame::Item(_)) => {
                return Err(internal("an item arrived before the open signal"));
            }
        }
        let bytes = response_items(inbound, attempt.visit().cloned());
        match self
            .bwd
            .enter(1, self.plan.outer_start, usize::MAX, attempt, bytes)?
        {
            BwdExit::Here(items) => Ok((items, lease)),
            BwdExit::Leave { .. } => Err(internal("the outer end's run left its segment")),
        }
    }

    /// A forward hand-off arrived. Acknowledge once it is admitted, then run
    /// it; from here until it hands the request on, this stage owns the
    /// reply and must deliver any failure to it. For a streamed request the
    /// acknowledgement names the input anchor this stage minted.
    ///
    /// Every refusal returns before the request's task is spawned, with the
    /// envelope ([`HopCore::admit`]), and after it only the acknowledgement
    /// does: a sender takes an `Overloaded` or `Placement` answer as its
    /// offer declined, and offers the request to another instance, or a
    /// chooser to another cell.
    async fn accept_forward(
        self: &Arc<Self>,
        mut envelope: Forward,
    ) -> std::result::Result<Accepted, Refused> {
        let rt = &self.rt;
        let pos = usize::from(envelope.pos);
        // The request's visit to this segment begins at its arrival, but
        // only a request this segment admits is one.
        let arrived =
            HopFigures::at(&self.figures.forward, pos).map(|figures| (figures, Instant::now()));
        #[cfg(feature = "fault-injection")]
        rt.served
            .offered
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        #[cfg(feature = "fault-injection")]
        if let Some(delay) = rt.config.faults.delay_forward_arrival {
            tokio::time::sleep(delay).await;
        }
        #[cfg(feature = "fault-injection")]
        if let Some(gate) = &rt.config.faults.forward_gate {
            gate.arrived.notify_one();
            gate.release.notified().await;
        }
        let (cx, share, egress) = match self.admit(&mut envelope).await {
            Ok(admitted) => admitted,
            Err(Refusal { error, decline }) => {
                return Err(Refused {
                    error,
                    decline,
                    envelope: Box::new(envelope),
                });
            }
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
                Some(input_items(shared)),
                Some(InputGuard(retirer)),
                Some(reply),
            )
        } else {
            (None, None, None)
        };
        rt.served
            .forwards
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let owner = arrived.map(|(figures, at)| Visit::request(figures, at, cx.deadline(), false));
        let cx = cx.with_visit(owner.as_ref().map(|owner| Arc::clone(owner.visit())));
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
                        hop.run_inner(cx, value, reply, Some(share), egress, owner)
                            .await;
                    }
                    None => {
                        let error = internal("a run reached an inner end it did not expect");
                        meter::ended(owner.as_ref(), Ending::Failed(&error));
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
                    let segment = &hop.plan.fwd_segments[at];
                    let sending = hop.rt.send_forward(
                        segment,
                        &hop.plan.routers[at],
                        &cx,
                        onward,
                        owner.is_some(),
                    );
                    let (sent, sent_at, lease) = match bounded(&cx, sending).await {
                        Ok(handed) => (Ok(handed.accepted), handed.sent, handed.held),
                        Err(error) => (Err(error), None, Held::default()),
                    };
                    // Handed on, the visit ends here: the response flows
                    // past this segment.
                    let handed = || {
                        if let (Some(owner), Some(sent_at)) = (&owner, sent_at) {
                            owner.visit().handed_on(false, sent_at);
                        }
                    };
                    match (sent, input, egress) {
                        // The next stage owns the reply now, and nothing of
                        // the response passes here: the lease ends with the
                        // acknowledgement.
                        (Ok(None), None, _) => {
                            handed();
                            drop((share, lease));
                        }
                        // A relay. The next stage owns the reply; this one keeps
                        // its admission share and its permit until it has sent
                        // the items on.
                        (Ok(Some(next)), Some(input), Some(egress)) => {
                            handed();
                            Box::pin(send_input(&hop.rt, &cx, next, input, upstream, None)).await;
                            drop((share, egress, lease));
                        }
                        (Ok(_), _, _) => {
                            let error = disagree();
                            meter::ended(owner.as_ref(), Ending::Failed(&error));
                            hop.fail(&cx, &reply, error).await;
                        }
                        // This stage still owns the reply until the failure is
                        // delivered, and counts against admission until then.
                        (Err(error), _, _) => {
                            meter::ended(owner.as_ref(), Ending::Failed(&error));
                            hop.fail(&cx, &reply, error).await;
                            drop(share);
                        }
                    }
                }
                Err(error) => {
                    meter::ended(owner.as_ref(), Ending::Failed(&error));
                    hop.fail(&cx, &reply, error).await;
                }
            }
            // Dropping the guard retires the input anchor.
            drop(guard);
        });
        #[cfg(feature = "fault-injection")]
        if let Some(delay) = self.rt.config.faults.delay_forward_ack {
            tokio::time::sleep(delay).await;
        }
        #[cfg(feature = "fault-injection")]
        if let Some(gate) = &self.rt.config.faults.forward_ack_gate {
            gate.arrived.notify_one();
            gate.release.notified().await;
        }
        Ok(accepted)
    }

    /// Everything that may refuse a forward hand-off, in order: the
    /// deployment, the request's deadline, admission, a retired instance or
    /// a draining cell's entry, and egress. Takes `envelope`'s context,
    /// and leaves the rest of it, so that a refused envelope goes back
    /// whole but for that.
    async fn admit(
        &self,
        envelope: &mut Forward,
    ) -> std::result::Result<(Cx, Share, Option<EgressPermit>), Refusal> {
        let rt = &self.rt;
        let pos = usize::from(envelope.pos);
        rt.check_identity(envelope.version, envelope.identity)?;
        // The context first: an expired request is refused before it takes
        // anything, and every wait below ends at its deadline.
        let wire = std::mem::take(&mut envelope.cx);
        let root = wire.root();
        let cx = wire.arrive(rt.config.clock_skew, || rt.wall_now())?;
        let share = match setup(
            &cx,
            rt.config.setup_timeout,
            "admission",
            rt.admission.admit(root),
        )
        .await
        {
            Ok(share) => share,
            // An offer that admission refuses still says that a cell that
            // declines offers is gone, not full.
            Err(error) => {
                if self.offer {
                    rt.check_offer()?;
                }
                return Err(error.into());
            }
        };
        // A retiring instance that has closed refuses, releasing the share
        // it just took; so does a draining cell's entry, for an offer.
        rt.check_open()?;
        if self.offer {
            rt.check_offer()?;
        }
        // A run that will produce a stream (the inner end's output, or a
        // relay's onward input) takes its egress permit before
        // acknowledging, so a full instance refuses instead of stalling.
        let egress = if self.plan.reaches_inner(pos) || self.plan.relays(pos) {
            Some(setup(&cx, rt.config.setup_timeout, "egress", rt.egress.acquire()).await?)
        } else {
            None
        };
        Ok((cx, share, egress))
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
        rt.check_open()?;
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
        // The run's stream is one this segment sends on, with a visit of
        // its own; one the outer end opens itself must not report to the
        // attempt's.
        let owner = HopFigures::at(&self.figures.backward, pos)
            .map(|figures| Visit::response(figures, cx.deadline(), None));
        let cx = cx.with_visit(owner.as_ref().map(|owner| Arc::clone(owner.visit())));
        let hop = Arc::clone(self);
        let end = self.plan.bwd_end(pos);
        tokio::spawn(async move {
            let _end = cx.cancel_on_drop();
            hop.run_prepared(cx, pos, end, inbound, out, owner).await;
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
        owner: Option<Owner>,
    ) {
        let held = owner.as_ref();
        let cancel = out.cancel_token();
        let stop = out.stop_token();
        let mut stopped = false;
        let first = loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return meter::ended(held, Ending::Gone),
                () = stop.cancelled(), if !stopped => {
                    stopped = true;
                    inbound.request_stop();
                }
                reason = cx.done() => return failed(out, reason, held).await,
                frame = inbound.next() => break frame,
            }
        };
        match first {
            // The open signal travels on: the next run, or the outer end,
            // activates only once the inner end's stream exists.
            Some(Frame::Open) => {
                if let Some(owner) = held {
                    owner.visit().producing();
                }
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
                match deliver(&out, Outgoing::Open, watch, &upstream).await {
                    Delivery::Sent => {}
                    Delivery::Gone => {
                        meter::ended(held, Ending::Gone);
                        return upstream.cancel();
                    }
                    Delivery::Interrupted(reason) => return failed(out, reason, held).await,
                }
            }
            // Raised in the segment that sent it, and counted there.
            Some(Frame::Fail(error)) => return failed(out, error.relayed(), held).await,
            Some(Frame::Broken(error)) => return failed(out, error, held).await,
            Some(Frame::End) | None => {
                let error = truncated("the run's producer ended before opening");
                return failed(out, error, held).await;
            }
            Some(Frame::Item(_)) => {
                let error = internal("an item arrived before the open signal");
                return failed(out, error, held).await;
            }
        }
        let upstream = inbound.canceller();
        let bytes = response_items(inbound, held.map(|owner| Arc::clone(owner.visit())));
        match self.bwd.enter(1, pos, end, &cx, bytes) {
            Ok(BwdExit::Leave { bytes, .. }) => {
                pump(bytes, out, &cx, Some(upstream), None, owner).await;
            }
            Ok(BwdExit::Here(_)) => {
                let error = internal("a prepared run reached the outer end");
                failed(out, error, held).await;
            }
            Err(error) => failed(out, error, held).await,
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
        owner: Option<Owner>,
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
        let out = match attach.await {
            Ok(out) => out,
            Err(error) => return meter::ended(owner.as_ref(), Ending::Failed(&error)),
        };
        self.produce_inner(&cx, value, out, owner).await;
        drop(egress);
    }

    async fn produce_inner(&self, cx: &Cx, value: I::Req, out: Outbound, owner: Option<Owner>) {
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
                () = cancel.cancelled() => return meter::ended(owner.as_ref(), Ending::Gone),
                // A stop while the node is still setting up its stream is
                // passed on, and the node is left to finish gracefully.
                () = stop.cancelled(), if !stopped => {
                    stopped = true;
                    cx.request_stop();
                }
                reason = cx.done() => return failed(out, reason, owner.as_ref()).await,
                produced = &mut call => break produced,
            }
        };
        let items = match produced {
            Ok(items) => {
                armed.disarm();
                items
            }
            Err(error) => return failed(out, error, owner.as_ref()).await,
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
        match deliver(&out, Outgoing::Open, watch, &upstream).await {
            Delivery::Sent => {}
            Delivery::Gone => {
                meter::ended(owner.as_ref(), Ending::Gone);
                return upstream.cancel();
            }
            Delivery::Interrupted(reason) => {
                return failed(out, reason, owner.as_ref()).await;
            }
        }
        match self.bwd.walk(1, self.plan.inner_end, cx, items) {
            BwdExit::Leave { bytes, .. } => pump(bytes, out, cx, None, None, owner).await,
            BwdExit::Here(_) => {
                let error = internal("the inner end's run reached the outer end");
                failed(out, error, owner.as_ref()).await;
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
    pump(input, out, cx, upstream, stops, None).await;
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
/// the request's deadline passes. After a clean end it returns only once
/// velo has handed the output on (or the request ends), so that what its
/// caller holds for the stream, the admission share and the egress permit,
/// covers the output too. A stop or cancel from the consumer is
/// passed upstream: to `cx`, which the stages feeding this pump run under,
/// and to `upstream`, the anchor this run reads from. `response` is the
/// visit whose response this is, when the segment records it; an input
/// pump has none.
///
/// Not an `async fn` itself: a second frame around `pump_watching` would
/// enlarge the future of every stream that crosses a segment.
fn pump<'a>(
    bytes: Items<Vec<u8>>,
    out: Outbound,
    cx: &'a Cx,
    upstream: Option<Canceller>,
    stops: Option<&'a Cx>,
    response: Option<Owner>,
) -> impl Future<Output = ()> + Send + 'a {
    // Built once per stream: a fresh future registers again when polled,
    // the deadline with the runtime's shared timer and the tokens in their
    // waiter lists, which would be a lock per item that every stream of the
    // runtime contends on.
    let consumer = Tokens {
        cancel: out.cancel_token(),
        stop: out.stop_token(),
    };
    pump_watching(bytes, out, cx, upstream, stops, consumer, response)
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
    response: Option<Owner>,
) {
    let response = response.as_ref();
    // One flag per item, no counter: whether the response's first item is
    // still to be sent.
    let mut first_pending = response.is_some();
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
                    () = &mut cancelled => {
                        meter::ended(response, Ending::Gone);
                        return upstream.cancel();
                    }
                    () = &mut stop, if !stopped => {
                        if left(&cancel) {
                            meter::ended(response, Ending::Gone);
                            return upstream.cancel();
                        }
                        stopped = true;
                        upstream.request_stop();
                        continue;
                    }
                    reason = &mut done => return failed(out, reason, response).await,
                    item = bytes.next() => item,
                }
            }
        };
        match item {
            // An empty item would cross as the open signal, and the
            // consumer would skip it. No encoding writes zero bytes for a
            // value, but a hand-written `Serialize` could.
            Some(Ok(item)) if item.is_empty() => {
                return failed(out, empty_encode(), response).await;
            }
            Some(Ok(item)) => {
                let watch = Watch {
                    stop: stop.as_mut(),
                    stopped: &mut stopped,
                    done: done.as_mut(),
                };
                match deliver(&out, Outgoing::Item(item), watch, &upstream).await {
                    Delivery::Sent => {
                        if first_pending {
                            first_pending = false;
                            if let Some(response) = response {
                                response.visit().first_out();
                            }
                        }
                    }
                    Delivery::Gone => {
                        meter::ended(response, Ending::Gone);
                        return upstream.cancel();
                    }
                    Delivery::Interrupted(reason) => return failed(out, reason, response).await,
                }
            }
            Some(Err(error)) => return failed(out, error, response).await,
            None => {
                meter::ended(response, Ending::Finished);
                // The caller holds the stream's share and permit until this
                // returns: until velo has handed the output on, which a
                // consumer that does not read can hold off until the
                // request's end. `done` is the future built above, so the
                // wait registers no new timer.
                let handed = out.finish();
                tokio::select! {
                    biased;
                    () = handed => {}
                    _ = &mut done => {}
                }
                return;
            }
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
    item: Outgoing,
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
    // One bound for the frame and its hand-off: a consumer that is not
    // reading holds neither longer than this.
    let _ = tokio::time::timeout(FAIL_DELIVERY, async {
        if out.fail(&error).await.is_ok() {
            out.finish().await;
        }
    })
    .await;
}

/// End the visit `owner` holds with `error`, then deliver the error.
async fn failed(out: Outbound, error: Error, owner: Option<&Owner>) {
    meter::ended(owner, Ending::Failed(&error));
    finish_failed(out, error).await;
}

/// A streamed request's input anchor as a stream of encoded items. A stop
/// does not pass through here: the pump that holds the anchor forwards it.
fn input_items<S>(inbound: S) -> Items<Bytes>
where
    S: Stream<Item = Frame> + Unpin + Send + 'static,
{
    items(InboundItems {
        inbound,
        visit: None,
        relays: false,
    })
}

/// A response anchor as a stream of encoded items: its first item is the
/// first to reach `visit` from another segment, and a failure on it was
/// raised in the segment that sent it, so it is relayed here.
fn response_items<S>(inbound: S, visit: Option<Arc<Visit>>) -> Items<Bytes>
where
    S: Stream<Item = Frame> + Unpin + Send + 'static,
{
    items(InboundItems {
        inbound,
        visit,
        relays: true,
    })
}

struct InboundItems<S> {
    inbound: S,
    /// The visit to stamp with the first item, until it arrives.
    visit: Option<Arc<Visit>>,
    relays: bool,
}

impl<S: Stream<Item = Frame> + Unpin> Stream for InboundItems<S> {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            return match Pin::new(&mut this.inbound).poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Some(Frame::Open)) => continue,
                Poll::Ready(Some(Frame::Item(bytes))) => {
                    // A load and a branch per item; the stamp, once.
                    if let Some(visit) = &this.visit {
                        visit.first_in();
                        this.visit = None;
                    }
                    Poll::Ready(Some(Ok(bytes)))
                }
                Poll::Ready(Some(Frame::Fail(error))) if this.relays => {
                    Poll::Ready(Some(Err(error.relayed())))
                }
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

fn empty_encode() -> Error {
    Error::new(
        ErrorKind::Codec,
        "an item encoded to no bytes, which cannot cross",
    )
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

    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Buf;

    use super::*;
    use crate::dist::fabric::{Fabric, bind_node, frame_of};
    use crate::dist::stages::{BwdEnd, FwdEnd};
    use crate::wire::{Codec, StreamCodec};

    /// An item as velo's byte path delivers it: a view past the frame's
    /// header into the buffer the frame arrived in, which is exactly as
    /// long as the frame.
    fn delivered(payload: &[u8]) -> Bytes {
        // `StreamFrame::Item` and a msgpack `bin8` header, as velo writes
        // an item of under 256 bytes.
        let header = [
            0x81,
            0xa4,
            b'I',
            b't',
            b'e',
            b'm',
            0xc4,
            payload.len() as u8,
        ];
        let mut frame = Vec::with_capacity(header.len() + payload.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(payload);
        let mut item = Bytes::from(frame);
        item.advance(header.len());
        item
    }

    /// Where the last item a test codec decoded began, one per test.
    static RESPONSE_READ: AtomicUsize = AtomicUsize::new(0);
    static INPUT_READ: AtomicUsize = AtomicUsize::new(0);

    fn read_at(at: &AtomicUsize, bytes: &[u8]) -> Result<u32> {
        at.store(bytes.as_ptr() as usize, Ordering::SeqCst);
        crate::wire_codec!(u32).expect("wire").decode(bytes)
    }

    fn encode(value: &u32) -> Result<Vec<u8>> {
        crate::wire_codec!(u32).expect("wire").encode(value)
    }

    /// The edge decode reads an item where velo delivered it, on a
    /// response and on a streamed request's input: no copy, and no move to
    /// the start of its buffer, which turning the view into a `Vec` makes.
    #[tokio::test]
    async fn the_edge_decode_reads_an_item_in_the_buffer_velo_delivered() {
        let payload = encode(&7).expect("encode");

        let item = delivered(&payload);
        let at = item.as_ptr() as usize;
        let frames = futures::stream::iter([frame_of(Some(Ok(velo::StreamFrame::Item(item))))]);
        let codec = Codec::from_fns(encode, |bytes| read_at(&RESPONSE_READ, bytes));
        let Ok(BwdExit::Here(mut decoded)) = BwdEnd::new(Some(codec)).enter(
            1,
            1,
            usize::MAX,
            &Cx::new(),
            response_items(frames, None),
        ) else {
            panic!("the response did not enter its outer end");
        };
        assert_eq!(decoded.next().await.expect("an item").expect("decoded"), 7);
        assert_eq!(
            RESPONSE_READ.load(Ordering::SeqCst),
            at,
            "a response item moved"
        );

        let item = delivered(&payload);
        let at = item.as_ptr() as usize;
        let frames = futures::stream::iter([frame_of(Some(Ok(velo::StreamFrame::Item(item))))]);
        let head = crate::wire_codec!(u32).expect("wire");
        let codec = StreamCodec::pair(
            Some(head),
            Some(Codec::from_fns(encode, |bytes| read_at(&INPUT_READ, bytes))),
        );
        let entered = FwdEnd::new(codec)
            .enter(
                1,
                1,
                usize::MAX,
                &Cx::new(),
                &payload,
                Some(input_items(frames)),
            )
            .await;
        let Ok(FwdExit::Here(request)) = entered else {
            panic!("the request did not enter its inner end");
        };
        let (_, mut input) = request.into_parts();
        assert_eq!(input.next().await.expect("an item").expect("decoded"), 7);
        assert_eq!(INPUT_READ.load(Ordering::SeqCst), at, "an input item moved");
    }

    /// An item that encodes to no bytes would cross as the open signal and
    /// vanish; the pump fails the stream with a codec error instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_empty_encode_fails_the_stream_as_a_codec_error() {
        let node = bind_node(([127, 0, 0, 1], 0).into()).await.expect("node");
        let fabric = Fabric::new(node, None, None, Duration::from_secs(30));
        let (mut inbound, reply) = fabric.mint();
        let out = fabric.attach(&reply).await.expect("attach");
        let cx = Cx::new();
        let input = items(futures::stream::iter([Ok(Vec::new()), Ok(vec![7])]));
        let wait = Duration::from_secs(5);
        tokio::time::timeout(wait, pump(input, out, &cx, None, None, None))
            .await
            .expect("the pump ended");
        match tokio::time::timeout(wait, inbound.next()).await {
            Ok(Some(Frame::Fail(error))) => assert_eq!(error.kind(), ErrorKind::Codec, "{error}"),
            other => panic!("the consumer did not see a codec failure: {other:?}"),
        }
    }

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
        let pumped = pump_watching(input, out, &scope, None, Some(&attempt), Racy, None);
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
        let pumped = pump_watching(input, out, &scope, None, Some(&attempt), Racy, None);
        let ended = tokio::time::timeout(Duration::from_millis(200), pumped).await;
        assert!(ended.is_err(), "a stop ended the pump");
        assert!(attempt.is_stopping(), "the stop did not reach the attempt");
        assert!(!attempt.is_cancelled());
    }
}
