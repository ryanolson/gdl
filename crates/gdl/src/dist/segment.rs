//! Standing up one segment instance.

use std::sync::{Arc, Weak};

use velo::Velo;

use super::envelope::{Forward, Open, decode, encode};
use super::fabric::Fabric;
use super::hop::{DHop, Register};
use super::runtime::{Entries, Roster, Runtime, RuntimeConfig};
use super::stages::{DBackward, DForward, DInner};
use crate::cx::Cx;
use crate::error::{Error, ErrorKind, Result};
use crate::items::Items;
use crate::role::Next;
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
        velo: Arc<Velo>,
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
        let fabric = Fabric::new(
            velo,
            config.anchor_timeout,
            config.heartbeat_interval,
            config.reservation_renew_interval,
        );
        let rt = Runtime::new(fabric, layout, segment.to_string(), roster, config)?;
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
                Box::pin(async move { encode_reply(forward(&weak, &bytes).await) })
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

async fn forward(entries: &Weak<Entries>, bytes: &[u8]) -> Result<super::envelope::Accepted> {
    let envelope: Forward = decode(bytes)?;
    let entry = Arc::clone(upgrade(entries)?.forward(envelope.hop, envelope.pos)?);
    entry(envelope).await
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

    /// Root requests this instance currently admits. Returns to zero when
    /// every request it served has finished.
    pub fn admitted(&self) -> usize {
        self.rt.admission.admitted()
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

impl<H, Req, Item> Next<Req, Item> for Arc<Segment<H>>
where
    H: Next<Req, Item>,
    Req: Send + 'static,
    Item: Send + 'static,
{
    async fn generate(&self, cx: Cx, req: Req) -> Result<Items<Item>> {
        self.call(cx, req).await
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
