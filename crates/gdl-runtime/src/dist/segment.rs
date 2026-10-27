//! Standing up one segment instance.

use std::sync::{Arc, Weak};

use velo::Velo;

use super::envelope::{Forward, Open, decode, encode};
use super::fabric::Fabric;
use super::hop::{DHop, Register};
use super::offers::{Decline, Refusal, refusal};
use super::roster::Roster;
use super::runtime::{Entries, Runtime, RuntimeConfig};
use super::stages::{DBackward, DForward, DInner};
use gdl::__runtime::Meters;
use gdl::Cx;
use gdl::Items;
use gdl::Next;
use gdl::{Error, ErrorKind, Result};
use gdl_model::Manifest;
use gdl_model::{Placement, layout};

/// One segment instance of a deployed graph, before its hops are assembled.
///
/// Validates the placement against the manifest, connects to the roster's
/// peers, and hands out the runtime that the hops of this segment share.
pub struct Deployment {
    rt: Arc<Runtime>,
}

impl Deployment {
    /// Prepare `segment` of `manifest` under `placement`, on the velo node
    /// `velo`.
    pub fn new(
        velo: Velo,
        manifest: &Manifest,
        placement: &Placement,
        segment: &str,
        roster: &Roster,
        config: RuntimeConfig,
    ) -> Result<Self> {
        config.validate()?;
        let layout = layout(manifest, placement)?;
        if !placement.segments().any(|(name, _)| name == segment) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("the placement has no segment `{segment}`"),
            ));
        }
        // The runtime's figures, registered before the hops are assembled;
        // each hop adds the entries it has here.
        let meters = config.metrics.as_ref().map(Meters::register).transpose()?;
        let fabric = Fabric::new(
            velo,
            config.anchor_timeout,
            config.heartbeat_interval,
            config.reservation_renew_interval,
        );
        let rt = Runtime::new(fabric, layout, segment.to_string(), roster, config, meters)?;
        Ok(Self { rt: Arc::new(rt) })
    }

    /// Fail unless this deployment was made for the contract whose manifest
    /// hashes to `contract_hash`. A generated `assemble` checks this first:
    /// another contract's graph of the same shape would otherwise run here
    /// while its envelopes claim this deployment's contract.
    pub fn check_contract(&self, contract_hash: u64) -> Result<()> {
        let expected = self.rt.layout.contract_hash();
        if contract_hash == expected {
            return Ok(());
        }
        Err(Error::new(
            ErrorKind::Placement,
            format!(
                "this deployment was made for contract {expected:016x}, not for the graph's contract {contract_hash:016x}"
            ),
        ))
    }

    /// Assemble hop `index` from its stages. Slots of nodes this segment does
    /// not host are `Absent`.
    pub fn hop<F, I, B>(&self, index: u16, fwd: F, inner: I, bwd: B) -> Result<DHop<F, I, B>>
    where
        F: DForward,
        I: DInner,
        B: DBackward,
    {
        DHop::new(Arc::clone(&self.rt), index, fwd, inner, bwd)
    }

    /// Register the entries the graph rooted at `root` serves here, and start
    /// serving them.
    pub fn start<H: Register>(self, root: H) -> Result<Segment<H>> {
        let mut entries = Entries::default();
        root.register(&mut entries);
        let entries = Arc::new(entries);
        self.rt.set_entries(&entries);

        // Velo owns the handlers; they hold the entries weakly, so dropping
        // the segment ends them instead of keeping the graph alive forever.
        let weak = Arc::downgrade(&entries);
        let names = self.rt.names();
        self.rt.fabric.register(&names.forward, {
            let weak = Weak::clone(&weak);
            move |bytes| {
                let weak = Weak::clone(&weak);
                Box::pin(async move { forward_reply(forward(&weak, &bytes).await) })
            }
        })?;
        self.rt.fabric.register(&names.open, move |bytes| {
            let weak = Weak::clone(&weak);
            Box::pin(async move { encode_reply(open(&weak, &bytes).await) })
        })?;
        Ok(Segment {
            rt: self.rt,
            _entries: entries,
            root,
        })
    }
}

async fn forward(
    entries: &Weak<Entries>,
    bytes: &[u8],
) -> std::result::Result<super::envelope::Accepted, Refusal> {
    let envelope: Forward = decode(bytes)?;
    let entry = Arc::clone(upgrade(entries)?.forward(envelope.hop, envelope.pos)?);
    entry(envelope).await.map_err(|refused| Refusal {
        error: refused.error,
        decline: refused.decline,
    })
}

async fn open(entries: &Weak<Entries>, bytes: &[u8]) -> Result<super::fabric::ReplyTo> {
    let open: Open = decode(bytes)?;
    let entry = Arc::clone(upgrade(entries)?.open(open.hop, open.pos)?);
    entry(open).await
}

fn upgrade(entries: &Weak<Entries>) -> Result<Arc<Entries>> {
    entries
        .upgrade()
        .ok_or_else(|| Error::new(ErrorKind::Transport, "the segment has shut down"))
}

/// A forward entry's answer: the acceptance, or the refusal with its
/// reason after it ([`refusal`]).
fn forward_reply(reply: std::result::Result<super::envelope::Accepted, Refusal>) -> Vec<u8> {
    match reply {
        Ok(accepted) => encode_reply(Ok(accepted)),
        // As for any reply: an empty one makes the caller report a codec
        // error.
        Err(Refusal { error, decline }) => refusal(&error, decline).unwrap_or_default(),
    }
}

fn encode_reply<T: serde::Serialize>(reply: Result<T>) -> Vec<u8> {
    // The reply type is always encodable; if even the error cannot be
    // encoded, an empty reply makes the caller report a codec error.
    encode(&reply).unwrap_or_default()
}

/// Work an instance has taken on since it started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Served {
    /// Forward hand-offs it accepted.
    pub forwards: usize,
    /// Backward runs it prepared.
    pub runs: usize,
    /// Request streams it received: one input anchor each.
    pub inputs: usize,
}

/// What an instance's admission holds, as its load report reads it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentLoad {
    /// The root requests it admits now.
    pub admitted: usize,
    /// How many it admits at once: `[runtime] admission_limit`.
    pub limit: usize,
    /// The forward hand-offs it has declined since it started, with no
    /// room (admission or egress refused) or retired.
    pub declined: usize,
}

/// A running segment instance.
pub struct Segment<H> {
    rt: Arc<Runtime>,
    _entries: Arc<Entries>,
    root: H,
}

impl<H> Segment<H> {
    /// Bind one named pipeline while keeping this segment's handlers alive.
    pub fn client<Req, Item, N>(
        self: &Arc<Self>,
        select: fn(&H) -> N,
    ) -> impl Next<Req, Item> + use<H, Req, Item, N>
    where
        H: Send + Sync + 'static,
        Req: Send + 'static,
        Item: Send + 'static,
        N: Next<Req, Item>,
    {
        EntryClient {
            segment: Arc::clone(self),
            next: select(&self.root),
        }
    }

    /// Send one request into the graph. Valid on the segment that hosts the
    /// source.
    pub async fn call<Req, Item>(&self, cx: Cx, req: Req) -> Result<Items<Item>>
    where
        H: Next<Req, Item>,
    {
        self.root.generate(cx, req).await
    }

    /// The runtime the segment's hops share, for the runtime's own tests.
    #[cfg(test)]
    pub(crate) fn runtime(&self) -> &Arc<Runtime> {
        &self.rt
    }

    /// Root requests this instance currently admits. Returns to zero when
    /// every request it served has finished.
    pub fn admitted(&self) -> usize {
        self.rt.admission.admitted()
    }

    /// What its admission holds now, read from counters the admission and
    /// the forward entries keep, without a lock a request takes.
    pub fn load(&self) -> SegmentLoad {
        let (admitted, limit) = self.rt.admission.load();
        SegmentLoad {
            admitted,
            limit,
            declined: self
                .rt
                .served
                .declined
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Refuse every forward hand-off and backward-run opening that arrives
    /// from now on: a retiring instance closes once nothing should route
    /// to it any more, then waits for what it admitted.
    pub fn close(&self) {
        self.rt.close();
    }

    /// Whether the instance has closed its entries.
    pub fn is_closed(&self) -> bool {
        self.rt.is_closed()
    }

    /// Refuse every offer that arrives from now on, the forward hand-offs
    /// from another class, and take the rest: an entry instance of a
    /// draining cell stops taking new work, and still takes the hand-offs
    /// inside its cell that carry the requests it took. The refusal says
    /// that the cell is gone, and nothing undoes it.
    pub fn decline_offers(&self) {
        self.rt.decline_offers();
    }

    /// Refuse every offer that arrives from now on by this segment's own
    /// policy, saying `decline` to the chooser that sent it, and take the
    /// rest, until [`Segment::take_offers`]. [`Decline::Gone`] keeps each
    /// chooser that hears it from offering this cell more work until the
    /// cell's follow changes (a new generation, state, or offered state);
    /// [`Decline::Full`] damps the cell for a short window. Either is
    /// about the whole cell, since a chooser chooses cells: a chooser that
    /// hears gone from one entry instance offers no entry instance of the
    /// cell more work. So decline as gone on every entry instance, for a
    /// reason that holds for the cell. [`Decline::Instance`] is about this
    /// instance alone: a chooser that hears it offers the request to the
    /// cell's other entry instances, each once, before another cell, and
    /// damps nothing. A call replaces the reason an earlier one gave. Costs nothing per request beyond the one load an
    /// offer already makes.
    pub fn decline_offers_as(&self, decline: Decline) {
        self.rt.decline_offers_as(decline);
    }

    /// Take offers again after [`Segment::decline_offers_as`]. A drain's
    /// refusal ([`Segment::decline_offers`]) stays.
    pub fn take_offers(&self) {
        self.rt.take_offers();
    }

    /// Whether the instance declines offers, for a drain or by its own
    /// policy.
    pub fn declines_offers(&self) -> bool {
        self.rt.declines_offers()
    }

    /// Root requests with outstanding remote attempts started here.
    pub fn attempting(&self) -> usize {
        self.rt.admission.attempting()
    }

    /// Forward hand-offs, backward runs, and request streams this instance
    /// has taken on.
    pub fn served(&self) -> Served {
        use std::sync::atomic::Ordering::Relaxed;
        Served {
            forwards: self.rt.served.forwards.load(Relaxed),
            runs: self.rt.served.runs.load(Relaxed),
            inputs: self.rt.served.inputs.load(Relaxed),
        }
    }

    /// Forward hand-offs that reached this instance, those it refused
    /// included. Available only with the `fault-injection` feature.
    #[cfg(feature = "fault-injection")]
    pub fn offered(&self) -> usize {
        self.rt
            .served
            .offered
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Egress permits this instance holds: one per stream it is producing.
    pub fn egress_in_use(&self) -> usize {
        self.rt.egress.in_use()
    }

    /// Anchors this instance's velo node currently holds.
    pub fn active_anchors(&self) -> usize {
        self.rt.fabric.active_anchors()
    }

    /// This instance's velo identity, for building a roster.
    pub fn peer_info(&self) -> velo::PeerInfo {
        self.rt.fabric.peer_info()
    }
}

struct EntryClient<H, N> {
    segment: Arc<Segment<H>>,
    next: N,
}
impl<H, N: Clone> Clone for EntryClient<H, N> {
    fn clone(&self) -> Self {
        Self {
            segment: Arc::clone(&self.segment),
            next: self.next.clone(),
        }
    }
}
impl<H, N, Req, Item> Next<Req, Item> for EntryClient<H, N>
where
    H: Send + Sync + 'static,
    N: Next<Req, Item>,
    Req: Send + 'static,
    Item: Send + 'static,
{
    async fn generate(&self, cx: Cx, req: Req) -> Result<Items<Item>> {
        self.next.generate(cx, req).await
    }
}
