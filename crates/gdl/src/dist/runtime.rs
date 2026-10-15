//! What every distributed hop of one segment instance shares.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use velo::{InstanceId, PeerInfo};

use super::admission::{Admission, Egress};
use super::envelope::{Forward, Identity, Open};
use super::fabric::{Fabric, ReplyTo};
use crate::error::{Error, ErrorKind, Result};
use crate::placement::Layout;

/// Runtime settings for one segment instance.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Root requests one instance serves at once.
    pub admission_limit: usize,
    /// How long an entry waits for an admission permit before refusing.
    pub admission_wait: Duration,
    /// Outstanding remote attempts one root request may hold.
    pub attempt_limit: usize,
    /// The longest any setup step (a forward hand-off, a run opening, an
    /// attach) may take, on top of the request's own deadline.
    pub setup_timeout: Duration,
    /// How long a minted anchor waits for its producer.
    pub anchor_timeout: Duration,
    /// The assumed maximum clock difference between segments.
    pub clock_skew: Duration,
    /// Streams one instance may produce at once, counting each stream until
    /// `egress_grace` after it ends.
    pub egress_limit: usize,
    /// How long a finished stream keeps its egress permit: velo may still
    /// hold its unsent output that long.
    pub egress_grace: Duration,
    /// How often producers send heartbeats; a dead producer is detected
    /// after about three missed intervals. `None` keeps velo's default.
    pub heartbeat_interval: Option<Duration>,
    /// Test-only delays and drops.
    #[cfg(feature = "fault-injection")]
    pub faults: Faults,
}

/// Steps the fault tests delay or skip. Available only with the
/// `fault-injection` feature.
#[cfg(feature = "fault-injection")]
#[derive(Debug, Clone, Default)]
pub struct Faults {
    /// Hold an arriving forward hand-off this long before checking it, as a
    /// slow network would.
    pub delay_forward_arrival: Option<Duration>,
    /// Hold the acknowledgement of an accepted forward hand-off this long, as
    /// a lost reply would.
    pub delay_forward_ack: Option<Duration>,
    /// Hold the reply to an opened backward run this long, as a lost reply
    /// would.
    pub delay_open_reply: Option<Duration>,
    /// The inner end never attaches to its reply, as a producer that never
    /// opens its ticket would.
    pub skip_inner_attach: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            admission_limit: 1024,
            admission_wait: Duration::from_millis(100),
            attempt_limit: 16,
            setup_timeout: Duration::from_secs(10),
            anchor_timeout: Duration::from_secs(30),
            clock_skew: Duration::from_millis(50),
            egress_limit: 1024,
            egress_grace: Duration::from_secs(5),
            heartbeat_interval: None,
            #[cfg(feature = "fault-injection")]
            faults: Faults::default(),
        }
    }
}

/// The instances of every segment, as velo peers.
#[derive(Debug, Clone, Default)]
pub struct Roster {
    segments: BTreeMap<String, Vec<PeerInfo>>,
}

impl Roster {
    /// An empty roster.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one instance of `segment`.
    pub fn add(&mut self, segment: impl Into<String>, peer: PeerInfo) {
        self.segments.entry(segment.into()).or_default().push(peer);
    }

    fn peers(&self) -> impl Iterator<Item = &PeerInfo> {
        self.segments.values().flatten()
    }
}

/// Picks an instance of a segment, round robin.
struct Picker {
    instances: BTreeMap<String, (Vec<InstanceId>, AtomicUsize)>,
}

impl Picker {
    fn new(roster: &Roster) -> Self {
        Self {
            instances: roster
                .segments
                .iter()
                .map(|(segment, peers)| {
                    let ids = peers.iter().map(PeerInfo::instance_id).collect();
                    (segment.clone(), (ids, AtomicUsize::new(0)))
                })
                .collect(),
        }
    }

    fn pick(&self, segment: &str) -> Result<InstanceId> {
        let (ids, next) = self.instances.get(segment).ok_or_else(|| {
            Error::new(
                ErrorKind::Placement,
                format!("no instance of segment `{segment}` in the roster"),
            )
        })?;
        if ids.is_empty() {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("segment `{segment}` has no instances"),
            ));
        }
        Ok(ids[next.fetch_add(1, Ordering::Relaxed) % ids.len()])
    }
}

/// A forward hand-off arriving at this instance.
pub(crate) type ForwardEntry = Arc<dyn Fn(Forward) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// A request to open a backward run on this instance.
pub(crate) type OpenEntry = Arc<dyn Fn(Open) -> BoxFuture<'static, Result<ReplyTo>> + Send + Sync>;

/// Entries registered by the hops this instance hosts, by hop and position.
/// Opaque: only the runtime reads or fills it.
#[doc(hidden)]
#[derive(Default)]
pub struct Entries {
    forward: BTreeMap<(u16, u16), ForwardEntry>,
    open: BTreeMap<(u16, u16), OpenEntry>,
}

impl Entries {
    pub(crate) fn add_forward(&mut self, hop: u16, pos: u16, entry: ForwardEntry) {
        self.forward.insert((hop, pos), entry);
    }

    pub(crate) fn add_open(&mut self, hop: u16, pos: u16, entry: OpenEntry) {
        self.open.insert((hop, pos), entry);
    }

    pub(crate) fn forward(&self, hop: u16, pos: u16) -> Result<&ForwardEntry> {
        self.forward
            .get(&(hop, pos))
            .ok_or_else(|| no_entry("forward", hop, pos))
    }

    pub(crate) fn open(&self, hop: u16, pos: u16) -> Result<&OpenEntry> {
        self.open
            .get(&(hop, pos))
            .ok_or_else(|| no_entry("open", hop, pos))
    }
}

fn no_entry(kind: &str, hop: u16, pos: u16) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("this instance hosts no {kind} entry for hop {hop} position {pos}"),
    )
}

/// The shared state of one segment instance.
pub(crate) struct Runtime {
    pub(crate) fabric: Fabric,
    pub(crate) layout: Layout,
    pub(crate) segment: String,
    pub(crate) config: RuntimeConfig,
    pub(crate) admission: Admission,
    pub(crate) egress: Egress,
    pub(crate) identity: Identity,
    picker: Picker,
    names: Names,
    /// Forward hand-offs and backward runs this instance has taken on.
    pub(crate) served: Served,
    /// Filled once at start; read by local short-circuits.
    entries: std::sync::OnceLock<std::sync::Weak<Entries>>,
}

/// Velo handler names for one deployment.
pub(crate) struct Names {
    pub(crate) forward: String,
    pub(crate) open: String,
}

impl Runtime {
    pub(crate) fn new(
        fabric: Fabric,
        layout: Layout,
        segment: String,
        roster: &Roster,
        config: RuntimeConfig,
    ) -> Result<Self> {
        for peer in roster.peers() {
            if peer.instance_id() != fabric.instance() {
                fabric.register_peer(peer.clone())?;
            }
        }
        let identity = Identity {
            contract: layout.contract_hash(),
            placement: layout.placement_hash(),
        };
        let tag = format!("{:016x}-{:016x}", identity.contract, identity.placement);
        Ok(Self {
            admission: Admission::new(
                config.admission_limit,
                config.admission_wait,
                config.attempt_limit,
            ),
            egress: Egress::new(
                config.egress_limit,
                config.admission_wait,
                config.egress_grace,
            ),
            picker: Picker::new(roster),
            names: Names {
                forward: format!("gdl-fwd-{tag}"),
                open: format!("gdl-open-{tag}"),
            },
            fabric,
            layout,
            segment,
            config,
            identity,
            served: Served::default(),
            entries: std::sync::OnceLock::new(),
        })
    }

    pub(crate) fn names(&self) -> &Names {
        &self.names
    }

    pub(crate) fn set_entries(&self, entries: &Arc<Entries>) {
        let _ = self.entries.set(Arc::downgrade(entries));
    }

    fn entries(&self) -> Result<Arc<Entries>> {
        self.entries
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Error::new(ErrorKind::Transport, "the segment has shut down"))
    }

    /// Hand a request to the next segment and wait for it to accept.
    ///
    /// When the chosen instance is this one, the entry runs by a direct
    /// call: velo is not relied on to deliver a message to its own sender.
    pub(crate) async fn send_forward(&self, segment: &str, envelope: Forward) -> Result<()> {
        let instance = self.picker.pick(segment)?;
        if instance == self.fabric.instance() {
            let entries = self.entries()?;
            let entry = Arc::clone(entries.forward(envelope.hop, envelope.pos)?);
            return entry(envelope).await;
        }
        let reply = self
            .fabric
            .call(
                instance,
                &self.names.forward,
                super::envelope::encode(&envelope)?,
                self.config.setup_timeout,
            )
            .await?;
        super::envelope::decode::<Result<(), Error>>(&reply)?
    }

    /// Ask an instance of `segment` to open a backward run.
    pub(crate) async fn send_open(&self, segment: &str, open: Open) -> Result<ReplyTo> {
        let instance = self.picker.pick(segment)?;
        if instance == self.fabric.instance() {
            let entries = self.entries()?;
            let entry = Arc::clone(entries.open(open.hop, open.pos)?);
            return entry(open).await;
        }
        let reply = self
            .fabric
            .call(
                instance,
                &self.names.open,
                super::envelope::encode(&open)?,
                self.config.setup_timeout,
            )
            .await?;
        super::envelope::decode::<Result<ReplyTo, Error>>(&reply)?
    }

    /// Whether an arriving message belongs to this deployment.
    pub(crate) fn check_identity(&self, version: u8, identity: Identity) -> Result<()> {
        if version != super::envelope::VERSION {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("envelope version {version} is not supported"),
            ));
        }
        if identity != self.identity {
            return Err(Error::new(
                ErrorKind::Placement,
                "the message belongs to a different contract or placement",
            ));
        }
        Ok(())
    }
}

/// Work an instance has taken on, for tests and diagnostics.
#[derive(Default)]
pub(crate) struct Served {
    pub(crate) forwards: AtomicUsize,
    pub(crate) runs: AtomicUsize,
}
