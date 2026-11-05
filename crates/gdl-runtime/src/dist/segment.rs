//! Standing up one segment instance.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use velo::Velo;

use super::calls::{self, CALL, Hosted, Serve};
use super::envelope::{Forward, Open, decode, encode};
use super::fabric::Fabric;
use super::held::Held;
use super::hop::{DHop, Register};
use super::offers::{Refusal, refusal};
use super::roster::Roster;
use super::runtime::{Entries, Runtime, RuntimeConfig};
use super::stages::{DBackward, DForward, DInner};
use crate::wire::Codec;
use gdl::__runtime::Meters;
use gdl::Cx;
use gdl::Decline;
use gdl::Items;
use gdl::Next;
use gdl::Unary;
use gdl::{Error, ErrorKind, Result};
use gdl_model::Manifest;
use gdl_model::{Placement, layout};

/// One segment instance of a deployed graph, before its hops are assembled.
///
/// Validates the placement against the manifest, connects to the roster's
/// peers, and hands out the runtime that the hops of this segment share.
pub struct Deployment {
    rt: Arc<Runtime>,
    /// The unary services this instance hosts, by name: the segment owns
    /// them until it is dropped, so that no client decides their lifetime.
    hosted: Mutex<Vec<(String, Arc<dyn Any + Send + Sync>)>>,
    /// Each unary node's index among the unary nodes in name order, by its
    /// name: what a call to it says it calls.
    indices: BTreeMap<String, u16>,
    /// The unary nodes the graph marks `idempotent`: a call to one that may
    /// have run is tried again.
    idempotent: BTreeSet<String>,
    /// The unary services this instance hosts for callers in other
    /// segments, by their indices.
    served: Mutex<BTreeMap<u16, Arc<dyn Serve>>>,
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
        rt.check_selector_needs(manifest)?;
        let rt = Arc::new(rt);
        if let Some(registry) = &rt.config.metrics {
            Held::register(registry, Arc::downgrade(&rt))?;
        }
        // By name, not by declaration: the contract hash ignores the order
        // the graph declares its nodes in, so two ends that agree on the hash
        // agree on these.
        let mut names: Vec<&str> = manifest
            .nodes
            .iter()
            .filter(|node| node.role == gdl_model::Role::Unary)
            .map(|node| node.name.as_str())
            .collect();
        names.sort_unstable();
        let indices = names
            .into_iter()
            .enumerate()
            .filter_map(|(index, name)| Some((name.to_string(), u16::try_from(index).ok()?)))
            .collect();
        let idempotent = manifest
            .nodes
            .iter()
            .filter(|node| node.unary.as_ref().is_some_and(|unary| unary.idempotent))
            .map(|node| node.name.clone())
            .collect();
        Ok(Self {
            rt,
            indices,
            idempotent,
            served: Mutex::new(BTreeMap::new()),
            hosted: Mutex::new(Vec::new()),
        })
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

    /// Hold the handles of the deployment's runtime, its instance's
    /// properties and the pools its nodes read, to the property
    /// declarations of `manifest`, each kind with its type in `types`, the
    /// contract's `property_types()`, as
    /// [`declare_properties`](crate::declare_properties) does. A generated
    /// `assemble` calls it, so a deployment made by hand is held to them
    /// too.
    pub fn declare_properties(
        &self,
        manifest: &Manifest,
        types: &[gdl::PropertyType],
    ) -> Result<()> {
        let layout = &self.rt.layout;
        let hosts = |node: &str| layout.segment_of(node) == Some(self.rt.segment.as_str());
        crate::declared::declare(
            manifest,
            &hosts,
            types,
            &[],
            &self.rt.config.properties,
            &self.rt.config.followed,
        )
    }

    /// Host `service` as unary node `name` for as long as the segment runs,
    /// and for the callers in other segments whose calls the layout resolves
    /// to this segment, its request and response decoded and encoded by
    /// `req` and `resp`. A generated `assemble` gives it every unary node's
    /// slot. A slot left `Absent` is refused when the segment hosts the
    /// node, as a pipeline node's is, and is neither held nor served
    /// otherwise; one no other segment calls here is held, not served.
    pub fn unary<S: Unary>(
        &self,
        name: &str,
        service: Arc<S>,
        req: Option<Codec<S::Req>>,
        resp: Option<Codec<S::Resp>>,
    ) -> Result<()> {
        let segment = self.rt.segment.as_str();
        let hosts = self.rt.layout.service_segments(name).contains(&segment);
        if S::ABSENT {
            return match hosts {
                true => Err(Error::new(
                    ErrorKind::Placement,
                    format!("segment `{segment}` hosts node `{name}`, but its binding is absent"),
                )),
                false => Ok(()),
            };
        }
        self.hosted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((
                name.to_string(),
                Arc::clone(&service) as Arc<dyn Any + Send + Sync>,
            ));
        if !self.rt.layout.called_in(name, &self.rt.segment) {
            return Ok(());
        }
        let (Some(req), Some(resp), Some(&index)) = (req, resp, self.indices.get(name)) else {
            return Err(Error::new(
                ErrorKind::Codec,
                format!(
                    "unary node `{name}` is called from another segment, which encodes its request and its response, and one is not a wire type"
                ),
            ));
        };
        let (request, reply) =
            calls::call_codec(&self.rt.layout, name, self.rt.config.decompressed_max);
        let hosted = Hosted {
            name: name.to_string(),
            service,
            req,
            resp,
            request,
            reply,
        };
        self.served
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(index, Arc::new(hosted));
        Ok(())
    }

    /// Assemble hop `index` from its stages. Slots of nodes this segment does
    /// not host are `Absent`.
    pub fn hop<F, I, B>(&self, index: u16, fwd: F, inner: I, bwd: B) -> Result<DHop<F, I, B>>
    where
        F: DForward,
        I: DInner,
        B: DBackward<F::Stack>,
    {
        DHop::new(Arc::clone(&self.rt), index, fwd, inner, bwd)
    }

    /// Register the entries the graph rooted at `root` serves here, and start
    /// serving them.
    pub fn start<H: Register>(self, root: H) -> Result<Segment<H>> {
        if let Some(want) = self.rt.unread_want() {
            let reads = want.edge().map_or("its bytes", |(_, name)| name);
            return Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "segment `{}`: the want of property kind `{}` reads the head as `{reads}`, but no request that crosses into the segment from another class carries it",
                    self.rt.segment,
                    want.kind()
                ),
            ));
        }
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
                Box::pin(async move { forward_reply(forward(&weak, bytes).await) })
            }
        })?;
        self.rt.fabric.register(&names.open, move |bytes| {
            let weak = Weak::clone(&weak);
            Box::pin(async move { encode_reply(open(&weak, bytes).await) })
        })?;
        // Only a segment that hosts a service for callers in other segments
        // answers calls. The segment holds the services; the handler, which
        // velo keeps as long as the node runs, holds them and the runtime
        // weakly, as the others hold the entries.
        let served = Arc::new(std::mem::take(
            &mut *self.served.lock().unwrap_or_else(PoisonError::into_inner),
        ));
        if !served.is_empty() {
            let (rt, hosted) = (Arc::downgrade(&self.rt), Arc::downgrade(&served));
            self.rt.fabric.register(CALL, move |bytes| {
                let (rt, hosted) = (Weak::clone(&rt), Weak::clone(&hosted));
                Box::pin(async move { calls::handle(&rt, &hosted, bytes).await })
            })?;
        }
        // Last: a call made through a link from here on is sent.
        self.rt
            .config
            .calls
            .bind(&self.rt, &self.indices, &self.idempotent)?;
        Ok(Segment {
            rt: self.rt,
            _entries: entries,
            _hosted: std::mem::take(
                &mut *self.hosted.lock().unwrap_or_else(PoisonError::into_inner),
            ),
            _served: served,
            root,
        })
    }
}

/// The bytes a hand-off or an opening arrived in are a view of velo's read
/// buffer, which a held view keeps whole: each is read out of them, and
/// they are let go before its entry runs.
async fn forward(
    entries: &Weak<Entries>,
    bytes: bytes::Bytes,
) -> std::result::Result<super::envelope::Accepted, Refusal> {
    let entries = upgrade(entries)?;
    let envelope = Forward::read(&bytes, |hop, pos| entries.compressed(hop, pos))?;
    drop(bytes);
    let entry = Arc::clone(entries.forward(envelope.hop, envelope.pos)?);
    entry(envelope).await.map_err(|refused| Refusal {
        error: refused.error,
        decline: refused.decline,
    })
}

async fn open(entries: &Weak<Entries>, bytes: bytes::Bytes) -> Result<super::fabric::ReplyTo> {
    let open: Open = decode(&bytes)?;
    drop(bytes);
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
    /// The unary services it hosts, owned until it is dropped.
    _hosted: Vec<(String, Arc<dyn Any + Send + Sync>)>,
    /// The unary services it hosts for callers in other segments.
    _served: Arc<BTreeMap<u16, Arc<dyn Serve>>>,
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

    /// Root requests this instance currently admits, and calls from other
    /// segments whose permits it holds. Returns to zero when every request
    /// and call it served has finished, and the reply to each call has been
    /// sent or has passed its call's bound and `call_reply_grace`. Drain
    /// waits for it.
    pub fn admitted(&self) -> usize {
        self.rt.admission.admitted() + self.rt.call_limit.held()
    }

    /// The requests this instance holds that may still send work to
    /// another segment: a hand-off it has not yet handed on or run to its
    /// inner end's stream, and an operator's `next` still held. Counted
    /// only in a class that a hop enters, whose pools drain segment by
    /// segment; 0 elsewhere.
    pub fn senders(&self) -> usize {
        self.rt.senders()
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

    /// When the instance first closed its entries, if it has. Available
    /// only with the `fault-injection` feature.
    #[cfg(feature = "fault-injection")]
    pub fn closed_at(&self) -> Option<tokio::time::Instant> {
        self.rt.closed_at()
    }

    /// When this instance promised never to route to `peer` again, if it
    /// has: taken before the promise is sent, so a peer that waits for
    /// the promise sees it no earlier. Available only with the
    /// `fault-injection` feature.
    #[cfg(feature = "fault-injection")]
    pub fn promised_to_avoid(&self, peer: velo::InstanceId) -> Option<tokio::time::Instant> {
        self.rt.routes().promised(peer)
    }

    /// Refuse every offer that arrives from now on, the forward hand-offs
    /// from another class, and take the rest: an entry instance of a
    /// draining pool stops taking new work, and still takes the hand-offs
    /// inside its pool that carry the requests it took. The refusal says
    /// that the pool is gone, and nothing undoes it.
    pub fn decline_offers(&self) {
        self.rt.decline_offers();
    }

    /// Refuse every offer that arrives from now on by this segment's own
    /// policy, saying `decline` to the chooser that sent it, and take the
    /// rest, until [`Segment::take_offers`]. [`Decline::Gone`] keeps each
    /// chooser that hears it from offering this pool more work until the
    /// pool's follow changes (a new generation, state, or offered state);
    /// [`Decline::Full`] damps the pool for a short window. Either is
    /// about the whole pool, since a chooser chooses pools: a chooser that
    /// hears gone from one entry instance offers no entry instance of the
    /// pool more work. So decline as gone on every entry instance, for a
    /// reason that holds for the pool. [`Decline::Instance`] is about this
    /// instance alone: a chooser that hears it offers the request to the
    /// pool's other entry instances, each once, before another pool, and
    /// damps nothing. [`Decline::NotPresent`] says that the instance does
    /// not have what the request wants: a chooser that hears it offers the
    /// request to another pool, and damps nothing. A call replaces the
    /// reason an earlier one gave. Costs nothing per request beyond the one
    /// load an offer already makes.
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
