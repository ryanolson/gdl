//! What every distributed hop of one segment instance shares.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Deserialize;
use velo::{InstanceId, PeerInfo};

use super::admission::{Admission, Egress};
use super::envelope::{Accepted, Forward, Identity, Open};
use super::fabric::{Fabric, ReplyTo};
use crate::error::{Error, ErrorKind, Result};
use crate::placement::Layout;

/// Runtime settings for one segment instance.
///
/// Deserializes from a table whose keys are the field names, with durations
/// in milliseconds and a `_ms` suffix (`admission_wait_ms = 100`). Missing
/// keys keep their defaults; unknown keys are an error, so a misspelled
/// setting is not silently ignored.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Settings")]
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
    /// The deadline given to a request that crosses segments without one.
    /// Work in another segment that no cancel can reach (a forward stage
    /// still running when its caller gives up) ends only by its deadline, so
    /// no remote work may run without one.
    pub default_deadline: Duration,
    /// Streams one instance may be producing at once.
    pub egress_limit: usize,
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
    /// Shift this instance's wall clock by this many nanoseconds (positive:
    /// ahead), as an unsynchronized machine would.
    pub clock_offset_nanos: i64,
    /// Hold each stage's delivery of a failure at this gate before it
    /// attaches, as a slow consumer would; the test observes the hold and
    /// ends it.
    pub failure_gate: Option<Arc<Gate>>,
}

/// A point a runtime step stops at until a test lets it go. Available only
/// with the `fault-injection` feature.
#[cfg(feature = "fault-injection")]
#[derive(Debug, Default)]
pub struct Gate {
    /// Signalled once for each arrival at the gate.
    pub arrived: tokio::sync::Notify,
    /// Signal once to let one arrival through.
    pub release: tokio::sync::Notify,
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
            default_deadline: Duration::from_secs(600),
            egress_limit: 1024,
            heartbeat_interval: None,
            #[cfg(feature = "fault-injection")]
            faults: Faults::default(),
        }
    }
}

/// The wire form of [`RuntimeConfig`].
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Settings {
    admission_limit: usize,
    admission_wait_ms: u64,
    attempt_limit: usize,
    setup_timeout_ms: u64,
    anchor_timeout_ms: u64,
    clock_skew_ms: u64,
    default_deadline_ms: u64,
    egress_limit: usize,
    heartbeat_interval_ms: Option<u64>,
}

impl Default for Settings {
    fn default() -> Self {
        let config = RuntimeConfig::default();
        let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        Self {
            admission_limit: config.admission_limit,
            admission_wait_ms: ms(config.admission_wait),
            attempt_limit: config.attempt_limit,
            setup_timeout_ms: ms(config.setup_timeout),
            anchor_timeout_ms: ms(config.anchor_timeout),
            clock_skew_ms: ms(config.clock_skew),
            default_deadline_ms: ms(config.default_deadline),
            egress_limit: config.egress_limit,
            heartbeat_interval_ms: config.heartbeat_interval.map(ms),
        }
    }
}

impl TryFrom<Settings> for RuntimeConfig {
    type Error = Error;

    fn try_from(s: Settings) -> Result<Self> {
        let config = Self {
            admission_limit: s.admission_limit,
            admission_wait: Duration::from_millis(s.admission_wait_ms),
            attempt_limit: s.attempt_limit,
            setup_timeout: Duration::from_millis(s.setup_timeout_ms),
            anchor_timeout: Duration::from_millis(s.anchor_timeout_ms),
            clock_skew: Duration::from_millis(s.clock_skew_ms),
            default_deadline: Duration::from_millis(s.default_deadline_ms),
            egress_limit: s.egress_limit,
            heartbeat_interval: s.heartbeat_interval_ms.map(Duration::from_millis),
            #[cfg(feature = "fault-injection")]
            faults: Faults::default(),
        };
        config.validate()?;
        Ok(config)
    }
}

impl RuntimeConfig {
    /// Refuse settings the runtime cannot run with: a zero limit refuses
    /// everything (and a zero attempt limit would keep a ledger entry for
    /// every refused root), and velo builds its heartbeat timer with the
    /// interval, which tokio refuses with a panic when it is zero.
    pub(crate) fn validate(&self) -> Result<()> {
        for (name, limit) in [
            ("admission_limit", self.admission_limit),
            ("attempt_limit", self.attempt_limit),
            ("egress_limit", self.egress_limit),
        ] {
            if limit == 0 {
                return Err(Error::new(
                    ErrorKind::Placement,
                    format!("runtime setting `{name}` must be at least 1"),
                ));
            }
        }
        // Both are semaphore sizes; tokio panics on a larger one.
        for (name, limit) in [
            ("admission_limit", self.admission_limit),
            ("egress_limit", self.egress_limit),
        ] {
            if limit > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(Error::new(
                    ErrorKind::Placement,
                    format!(
                        "runtime setting `{name}` must be at most {}",
                        tokio::sync::Semaphore::MAX_PERMITS
                    ),
                ));
            }
        }
        // velo carries the interval in whole milliseconds, so anything
        // shorter becomes zero on the way.
        if self
            .heartbeat_interval
            .is_some_and(|interval| interval < Duration::from_millis(1))
        {
            return Err(Error::new(
                ErrorKind::Placement,
                "runtime setting `heartbeat_interval_ms` must be at least 1",
            ));
        }
        Ok(())
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

    /// The instances of `segment`.
    pub fn instances(&self, segment: &str) -> &[PeerInfo] {
        self.segments.get(segment).map_or(&[], Vec::as_slice)
    }

    pub(crate) fn peers(&self) -> impl Iterator<Item = &PeerInfo> {
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
pub(crate) type ForwardEntry =
    Arc<dyn Fn(Forward) -> BoxFuture<'static, Result<Accepted>> + Send + Sync>;

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
    /// Forward hand-offs, backward runs, and request streams this instance
    /// has taken on.
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
            egress: Egress::new(config.egress_limit, config.admission_wait),
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

    /// Hand a request to the next segment and wait for it to accept. A
    /// stage that accepts a streamed request answers with where to send its
    /// items.
    ///
    /// When the chosen instance is this one, the entry runs by a direct
    /// call: velo is not relied on to deliver a message to its own sender.
    pub(crate) async fn send_forward(&self, segment: &str, envelope: Forward) -> Result<Accepted> {
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
        super::envelope::decode::<Result<Accepted, Error>>(&reply)?
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

    /// This instance's wall clock, in Unix nanoseconds.
    pub(crate) fn wall_now(&self) -> u64 {
        let now = crate::cx::wall_now_nanos();
        #[cfg(feature = "fault-injection")]
        let now = now.saturating_add_signed(self.config.faults.clock_offset_nanos);
        now
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
    pub(crate) inputs: AtomicUsize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_config_reads_milliseconds_and_keeps_defaults() {
        let config: RuntimeConfig = toml::from_str(
            "admission_limit = 8\ndefault_deadline_ms = 2500\nheartbeat_interval_ms = 50\n",
        )
        .expect("parse");
        assert_eq!(config.admission_limit, 8);
        assert_eq!(config.default_deadline, Duration::from_millis(2500));
        assert_eq!(config.heartbeat_interval, Some(Duration::from_millis(50)));
        let defaults = RuntimeConfig::default();
        assert_eq!(config.egress_limit, defaults.egress_limit);
        assert_eq!(config.setup_timeout, defaults.setup_timeout);
        let empty: RuntimeConfig = toml::from_str("").expect("empty");
        assert_eq!(empty.admission_wait, defaults.admission_wait);
        assert_eq!(empty.heartbeat_interval, None);
    }

    #[test]
    fn runtime_config_rejects_a_setting_velo_cannot_run_with() {
        // velo builds its heartbeat timer with this period, and tokio panics
        // on a zero one when a stream attaches.
        for text in [
            "heartbeat_interval_ms = 0\n",
            "admission_limit = 0\n",
            "attempt_limit = 0\n",
            "egress_limit = 0\n",
        ] {
            let Err(error) = toml::from_str::<RuntimeConfig>(text) else {
                panic!("accepted {text:?}");
            };
            let key = text.split(' ').next().unwrap_or_default();
            assert!(error.to_string().contains(key), "{error}");
        }
    }

    #[test]
    fn runtime_config_refuses_more_permits_than_a_semaphore_holds() {
        let most = tokio::sync::Semaphore::MAX_PERMITS;
        for key in ["admission_limit", "egress_limit"] {
            let fits: RuntimeConfig =
                toml::from_str(&format!("{key} = {most}\n")).expect("the most a semaphore holds");
            assert!(fits.validate().is_ok());
            let Err(error) = toml::from_str::<RuntimeConfig>(&format!("{key} = {}\n", most + 1))
            else {
                panic!("{key} above the semaphore's limit was accepted");
            };
            assert!(error.to_string().contains(key), "{error}");
        }
    }

    #[test]
    fn runtime_config_rejects_an_unknown_key() {
        let Err(error) = toml::from_str::<RuntimeConfig>("admision_limit = 8\n") else {
            panic!("a misspelled key parsed");
        };
        assert!(error.to_string().contains("admision_limit"), "{error}");
    }
}
