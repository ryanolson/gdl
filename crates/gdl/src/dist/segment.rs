//! Standing up one segment instance.

use std::sync::{Arc, Weak};

use velo::Velo;

use super::envelope::{Forward, Open, decode, encode};
use super::fabric::Fabric;
use super::hop::{DHop, Register};
use super::runtime::{Entries, Roster, Runtime, RuntimeConfig};
use crate::cx::Cx;
use crate::error::{Error, ErrorKind, Result};
use crate::items::Items;
use crate::manifest::Manifest;
use crate::placement::{Placement, layout};
use crate::role::Next;

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
        // A zero limit refuses everything, and the attempt ledger would
        // keep an entry for every refused root.
        for (name, limit) in [
            ("admission_limit", config.admission_limit),
            ("attempt_limit", config.attempt_limit),
            ("egress_limit", config.egress_limit),
        ] {
            if limit == 0 {
                return Err(Error::new(
                    ErrorKind::Placement,
                    format!("runtime setting `{name}` must be at least 1"),
                ));
            }
        }
        let layout = layout(manifest, placement)?;
        if !placement.segments().any(|(name, _)| name == segment) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("the placement has no segment `{segment}`"),
            ));
        }
        let fabric = Fabric::new(velo, config.anchor_timeout, config.heartbeat_interval);
        let rt = Runtime::new(fabric, layout, segment.to_string(), roster, config)?;
        Ok(Self { rt: Arc::new(rt) })
    }

    /// Assemble hop `index` from its stages. Slots of nodes this segment does
    /// not host are `Absent`.
    pub fn hop<F, I, B>(&self, index: u16, fwd: F, inner: I, bwd: B) -> Result<DHop<F, I, B>> {
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

async fn forward(entries: &Weak<Entries>, bytes: &[u8]) -> Result<()> {
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

/// A running segment instance.
pub struct Segment<H> {
    rt: Arc<Runtime>,
    _entries: Arc<Entries>,
    root: H,
}

impl<H> Segment<H> {
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

    /// Forward hand-offs and backward runs this instance has taken on.
    pub fn served(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.rt.served.forwards.load(Relaxed),
            self.rt.served.runs.load(Relaxed),
        )
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
