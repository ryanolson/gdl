//! What every distributed hop of one segment instance shares.

use std::collections::BTreeMap;
use std::hash::RandomState;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Deserialize;

use super::admission::{Admission, Egress};
use super::envelope::{Accepted, Forward, Identity, Open};
use super::fabric::{Fabric, ReplyTo};
#[cfg(feature = "hub")]
use super::offers::Answer;
use super::offers::{Decline, Offer, Refusal, Refused};
use super::roster::{Live, Picker, Policy, Roster};
use gdl::__runtime::Meters;
use gdl::__runtime::{Erased, Lease};
use gdl::Cx;
use gdl::{CellPolicies, FollowedCells, HomeFirstAt, Selectors};
use gdl::{Error, ErrorKind, Result};
use gdl_model::{Layout, RequestRouter};

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
    /// How many other cells a chooser offers a request to after the cell it
    /// chose first declines it, or the hand-off to it provably never left
    /// this instance: only for a hand-off into another class, from an
    /// instance in a cell. 0 offers each request to one cell.
    pub cell_failovers: usize,
    /// How long a chooser passes over a cell after its first decline. The
    /// window doubles with each further decline after it ended, up to
    /// `cell_damping_max`. At least 1 ms.
    pub cell_damping_first: Duration,
    /// The longest a chooser passes over a cell that keeps declining: it
    /// is tried again at least this often. From `cell_damping_first` to
    /// one hour.
    pub cell_damping_max: Duration,
    /// How many of its periods a cell's load summary stays fresh after it
    /// reached the chooser's hub. A policy that chooses by load, as `home_first`
    /// does, passes over a cell whose summary is stale. From 1 to 100.
    pub cell_fresh_periods: u32,
    /// The thresholds of the chooser's `home_first`: its saturation
    /// threshold, from 1 to 100 percent of a cell's admission limit, and
    /// its spill bound, at most the saturation threshold.
    pub home_first: HomeFirstAt,
    /// How many more times a crossing's router chooses an instance after
    /// the one it chose declined a hand-off (no room, or retired), or the
    /// hand-off to it provably never left this instance: for every
    /// hand-off a policy or a selector routes. The same instance may be
    /// chosen again. 0 makes each refusal final.
    pub instance_failovers: usize,
    /// Override how long an ordinary anchor waits for its producer; `None` uses Velo defaults.
    pub anchor_timeout: Option<Duration>,
    /// Cadence for batching held reply reservations. Defaults to 30 seconds.
    pub reservation_renew_interval: Duration,
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
    /// How long a stopped instance may drain: keep serving until the
    /// sources that send to it have drained, then until it holds no
    /// request. `None` is `default_deadline`, since no request admitted
    /// before the stop outlives it unless it set a longer deadline itself.
    pub drain: Option<Duration>,
    /// Where the runtime records what each request does in this segment:
    /// its request path, the response streams it sends on, and, at the
    /// source, the request end to end; `None` records nothing. Not a
    /// setting: a segment binary gives its process's registry when its
    /// system listener can serve it.
    pub metrics: Option<prometheus::Registry>,
    /// The selectors a crossing's router can name, by name. A name the
    /// placement uses that is not here is gdl's own selector of that name,
    /// with its defaults. Not a setting: a segment binary registers them
    /// through its `Builder`.
    pub selectors: Selectors,
    /// The cell policies a placement's `[cells.<class>] policy` can name,
    /// by name. A name that is not here is gdl's own policy of that name.
    /// Not a setting: a segment binary registers them through its
    /// `Builder`. A chooser launched through a `ProcessConfig` checks the
    /// names against its `runtime`'s.
    pub cell_policies: CellPolicies,
    /// The handle through which the segment's nodes read the cells its
    /// chooser follows. Not a setting: a segment binary's `Builder` gives
    /// it to the nodes it builds, and the launch binds it to the chooser's
    /// tables. A segment that sends into no other class, or runs without a
    /// hub, leaves it unbound.
    pub followed: FollowedCells,
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
    /// Hold each arriving forward hand-off at this gate before checking it;
    /// the test observes the arrival and lets it go.
    pub forward_gate: Option<Arc<Gate>>,
    /// Hold the acknowledgement of an accepted forward hand-off this long, as
    /// a lost reply would.
    pub delay_forward_ack: Option<Duration>,
    /// Hold the acknowledgement of each accepted forward hand-off at this
    /// gate; the test observes the hold and ends it.
    pub forward_ack_gate: Option<Arc<Gate>>,
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

impl RuntimeConfig {
    /// How long a stopped instance may drain.
    pub fn drain_bound(&self) -> Duration {
        self.drain.unwrap_or(self.default_deadline)
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            admission_limit: 1024,
            admission_wait: Duration::from_millis(100),
            attempt_limit: 16,
            setup_timeout: Duration::from_secs(10),
            cell_failovers: 2,
            cell_damping_first: Self::DEFAULT_CELL_DAMPING_FIRST,
            cell_damping_max: Self::DEFAULT_CELL_DAMPING_MAX,
            cell_fresh_periods: gdl::Load::FRESH_PERIODS,
            home_first: HomeFirstAt::DEFAULT,
            instance_failovers: 2,
            anchor_timeout: None,
            reservation_renew_interval: Duration::from_secs(30),
            clock_skew: Duration::from_millis(50),
            default_deadline: gdl_model::DEFAULT_DEADLINE,
            egress_limit: 1024,
            heartbeat_interval: None,
            drain: None,
            metrics: None,
            selectors: Selectors::new(),
            cell_policies: CellPolicies::new(),
            followed: FollowedCells::new(),
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
    cell_failovers: usize,
    cell_damping_first_ms: u64,
    cell_damping_max_ms: u64,
    cell_fresh_periods: u32,
    home_first_saturated_percent: u64,
    home_first_spill_percent: u64,
    instance_failovers: usize,
    anchor_timeout_ms: Option<u64>,
    reservation_renew_interval_ms: u64,
    clock_skew_ms: u64,
    default_deadline_ms: u64,
    egress_limit: usize,
    heartbeat_interval_ms: Option<u64>,
    drain_ms: Option<u64>,
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
            cell_failovers: config.cell_failovers,
            cell_damping_first_ms: ms(config.cell_damping_first),
            cell_damping_max_ms: ms(config.cell_damping_max),
            cell_fresh_periods: config.cell_fresh_periods,
            home_first_saturated_percent: config.home_first.saturated_percent,
            home_first_spill_percent: config.home_first.spill_percent,
            instance_failovers: config.instance_failovers,
            anchor_timeout_ms: config.anchor_timeout.map(ms),
            reservation_renew_interval_ms: ms(config.reservation_renew_interval),
            clock_skew_ms: ms(config.clock_skew),
            default_deadline_ms: ms(config.default_deadline),
            egress_limit: config.egress_limit,
            heartbeat_interval_ms: config.heartbeat_interval.map(ms),
            drain_ms: config.drain.map(ms),
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
            cell_failovers: s.cell_failovers,
            cell_damping_first: Duration::from_millis(s.cell_damping_first_ms),
            cell_damping_max: Duration::from_millis(s.cell_damping_max_ms),
            cell_fresh_periods: s.cell_fresh_periods,
            home_first: HomeFirstAt {
                saturated_percent: s.home_first_saturated_percent,
                spill_percent: s.home_first_spill_percent,
            },
            instance_failovers: s.instance_failovers,
            anchor_timeout: s.anchor_timeout_ms.map(Duration::from_millis),
            reservation_renew_interval: Duration::from_millis(s.reservation_renew_interval_ms),
            clock_skew: Duration::from_millis(s.clock_skew_ms),
            default_deadline: Duration::from_millis(s.default_deadline_ms),
            egress_limit: s.egress_limit,
            heartbeat_interval: s.heartbeat_interval_ms.map(Duration::from_millis),
            drain: s.drain_ms.map(Duration::from_millis),
            metrics: None,
            selectors: Selectors::new(),
            cell_policies: CellPolicies::new(),
            followed: FollowedCells::new(),
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
        if self.reservation_renew_interval.is_zero() {
            return Err(Error::new(
                ErrorKind::Placement,
                "runtime setting `reservation_renew_interval_ms` must be positive",
            ));
        }
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
        self.validate_cells()?;
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

impl RuntimeConfig {
    /// The damping window after a cell's first decline, unless
    /// `cell_damping_first_ms` is set: 1 s.
    pub(crate) const DEFAULT_CELL_DAMPING_FIRST: Duration = Duration::from_secs(1);

    /// The longest damping window, unless `cell_damping_max_ms` is set:
    /// 30 s.
    pub(crate) const DEFAULT_CELL_DAMPING_MAX: Duration = Duration::from_secs(30);

    /// The longest damping window: a cell is tried again at least once an
    /// hour, and a window's end stays far inside what the clock holds.
    const MAX_DAMPING: Duration = Duration::from_secs(3600);

    /// The most periods a summary may stay fresh: a summary 100 periods
    /// old says nothing of the cell's load now.
    const MAX_FRESH_PERIODS: u32 = 100;

    /// Refuse the chooser's cell settings that break what it relies on:
    /// windows that cannot grow or end, a summary that is never fresh or
    /// is fresh too long, and thresholds outside a cell's admission limit,
    /// or a spill bound past the saturation threshold, which no cell
    /// elsewhere could reach unsaturated.
    fn validate_cells(&self) -> Result<()> {
        let refuse = |key: &str, why: String| {
            Err(Error::new(
                ErrorKind::Placement,
                format!("runtime setting `{key}` {why}"),
            ))
        };
        let ms = |d: Duration| d.as_millis();
        if self.cell_damping_first < Duration::from_millis(1) {
            return refuse("cell_damping_first_ms", "must be at least 1".into());
        }
        if self.cell_damping_max < self.cell_damping_first {
            return refuse(
                "cell_damping_max_ms",
                format!(
                    "is {}; it must be at least `cell_damping_first_ms`, {}",
                    ms(self.cell_damping_max),
                    ms(self.cell_damping_first)
                ),
            );
        }
        if self.cell_damping_max > Self::MAX_DAMPING {
            return refuse(
                "cell_damping_max_ms",
                format!("must be at most {}", ms(Self::MAX_DAMPING)),
            );
        }
        if !(1..=Self::MAX_FRESH_PERIODS).contains(&self.cell_fresh_periods) {
            return refuse(
                "cell_fresh_periods",
                format!("must be from 1 to {}", Self::MAX_FRESH_PERIODS),
            );
        }
        let HomeFirstAt {
            saturated_percent,
            spill_percent,
        } = self.home_first;
        if !(1..=100).contains(&saturated_percent) {
            return refuse(
                "home_first_saturated_percent",
                "must be from 1 to 100".into(),
            );
        }
        if spill_percent > saturated_percent {
            return refuse(
                "home_first_spill_percent",
                format!(
                    "is {spill_percent}; it must be at most `home_first_saturated_percent`, {saturated_percent}"
                ),
            );
        }
        Ok(())
    }
}

/// A forward hand-off arriving at this instance.
pub(crate) type ForwardEntry =
    Arc<dyn Fn(Forward) -> BoxFuture<'static, Result<Accepted, Refused>> + Send + Sync>;

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
    /// Whom to route to, shared with the roster the instance launched
    /// with, which follows membership.
    routes: Arc<Live>,
    /// The selectors this instance's crossings name, resolved once: one
    /// per name, shared by every crossing that names it.
    selectors: BTreeMap<String, Arc<dyn Erased>>,
    /// A chooser's hand-offs into other classes, by hop and position,
    /// with the cells each chooses from; empty for any other instance.
    #[cfg(feature = "hub")]
    crossings: BTreeMap<(usize, usize), super::cells::Target>,
    random: RandomState,
    /// Set once a retiring instance stops taking hand-offs and openings.
    closed: AtomicBool,
    /// Why the instance declines offers, the hand-offs from another
    /// class, if it does: [`DRAINED`] once an entry instance of a
    /// draining cell stops taking them, which nothing undoes, and the
    /// segment's own refusal ([`Runtime::decline_offers_as`]) in the bits
    /// below it. One byte, so that an offer reads both in one load.
    declining: AtomicU8,
    /// Whether each hop's hand-offs to this segment are offers, worked out
    /// once.
    offers: Vec<bool>,
    names: Names,
    /// Forward hand-offs, backward runs, and request streams this instance
    /// has taken on.
    pub(crate) served: Served,
    /// Filled once at start; read by local short-circuits.
    entries: std::sync::OnceLock<std::sync::Weak<Entries>>,
    /// The figures of each entry the segment hosts, when the instance
    /// records them.
    pub(crate) meters: Option<Meters>,
}

/// [`Runtime::declining`]: a drain declines offers, as gone.
const DRAINED: u8 = 0b100;
/// The segment's own refusal, as gone.
const GONE: u8 = 0b010;
/// The segment's own refusal, as full.
const FULL: u8 = 0b001;

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
        meters: Option<Meters>,
    ) -> Result<Self> {
        let mut selectors = BTreeMap::new();
        for (hop, at, name) in sent_selectors(&layout, &segment) {
            if selectors.contains_key(name) {
                continue;
            }
            let Some(selector) = config.selectors.resolve(name) else {
                return Err(Error::new(
                    ErrorKind::Placement,
                    format!(
                        "segment `{segment}` routes hop {hop}'s crossing at position {at} through selector `{name}`, which nothing registers and gdl does not have"
                    ),
                ));
            };
            selectors.insert(name.to_string(), selector);
        }
        #[cfg(feature = "hub")]
        let crossings = match roster.cells() {
            Some(cells) => super::cells::targets(
                &layout,
                &segment,
                cells,
                &config.cell_policies,
                config.home_first,
            )?,
            None => BTreeMap::new(),
        };
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
        let offers = super::offers::offered_hops(layout.boundaries(), layout.hops().len(), |hop| {
            layout.hops()[hop]
                .backward_runs
                .first()
                .is_some_and(|inner| inner.segment == segment)
        });
        Ok(Self {
            admission: Admission::new(
                config.admission_limit,
                config.admission_wait,
                config.attempt_limit,
            ),
            egress: Egress::new(config.egress_limit, config.admission_wait),
            routes: Arc::clone(roster.live()),
            selectors,
            #[cfg(feature = "hub")]
            crossings,
            random: RandomState::new(),
            closed: AtomicBool::new(false),
            declining: AtomicU8::new(0),
            offers,
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
            meters,
        })
    }

    /// Refuse every hand-off and opening admitted from now on. Stored
    /// before the drain reads the ledger, and read by an entry after its
    /// share is in the ledger, both `SeqCst`: an entry is either in the
    /// ledger the drain waits for, or refused.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Refuse an entry the instance has admitted once it has closed. One
    /// atomic load per entry.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn check_open(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::new(
                ErrorKind::Overloaded,
                "the instance has retired and takes no new work",
            ));
        }
        Ok(())
    }

    /// Refuse every offer admitted from now on, for good, saying that the
    /// cell is gone: a forward hand-off on a hop that crosses into this
    /// segment's class, which only another class's instances send. The
    /// hand-offs inside the class, which carry the requests already taken,
    /// are still taken. Ordered with the ledger as [`Runtime::close`] is.
    pub(crate) fn decline_offers(&self) {
        self.declining.fetch_or(DRAINED, Ordering::SeqCst);
    }

    /// Refuse every offer admitted from now on, by the segment's own
    /// policy, for `decline`, until [`Runtime::take_offers`]; a drain's
    /// refusal outlasts both.
    pub(crate) fn decline_offers_as(&self, decline: Decline) {
        let policy = match decline {
            Decline::Full => FULL,
            Decline::Gone => GONE,
        };
        self.set_policy(policy);
    }

    /// Take offers again after [`Runtime::decline_offers_as`], unless a
    /// drain declines them.
    pub(crate) fn take_offers(&self) {
        self.set_policy(0);
    }

    /// Replace the segment's own refusal with `policy`, keeping a drain's.
    fn set_policy(&self, policy: u8) {
        let _ = self
            .declining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |declining| {
                Some(declining & DRAINED | policy)
            });
    }

    /// Whether the instance declines offers, for a drain or by its own
    /// policy.
    pub(crate) fn declines_offers(&self) -> bool {
        self.declining.load(Ordering::SeqCst) != 0
    }

    /// Refuse an offer the instance has admitted once it declines offers,
    /// with the reason a chooser reads: gone for a drain, since a draining
    /// cell takes no new work from another cell again, and the segment's
    /// own reason otherwise. One atomic load per offer, and none for any
    /// other hand-off.
    pub(crate) fn check_offer(&self) -> std::result::Result<(), Refusal> {
        let declining = self.declining.load(Ordering::SeqCst);
        if declining == 0 {
            return Ok(());
        }
        let (why, decline) = if declining & DRAINED != 0 {
            (
                "the instance's cell is draining and takes no new work from another cell",
                Decline::Gone,
            )
        } else if declining & GONE != 0 {
            (
                "the instance's segment takes no work from another cell by its own policy",
                Decline::Gone,
            )
        } else {
            (
                "the instance's segment has no room for work from another cell by its own policy",
                Decline::Full,
            )
        };
        Err(Refusal {
            error: Error::new(ErrorKind::Overloaded, why),
            decline,
        })
    }

    /// Whether the forward hand-offs of hop `hop` that reach this segment
    /// are offers from another class ([`super::offers::offered_hops`]).
    pub(crate) fn offered(&self, hop: usize) -> bool {
        self.offers.get(hop).copied().unwrap_or(false)
    }

    pub(crate) fn names(&self) -> &Names {
        &self.names
    }

    /// Whom to route to: the table every crossing reads.
    pub(crate) fn routes(&self) -> &Live {
        &self.routes
    }

    /// What the built-in routers draw from.
    pub(crate) fn random(&self) -> &RandomState {
        &self.random
    }

    /// Wait until the roster is released: every instance this one can
    /// route to is serving. One atomic read once it is: the timeout is
    /// built only for a held roster, since building one touches the
    /// runtime's shared handle.
    async fn released(&self) -> Result<()> {
        if self.routes.is_released() {
            return Ok(());
        }
        tokio::time::timeout(self.config.setup_timeout, self.routes.released())
            .await
            .map_err(|_| {
                Error::new(
                    ErrorKind::Transport,
                    "the instances this one routes to were not all serving within the setup timeout",
                )
            })
    }

    pub(crate) fn set_entries(&self, entries: &Arc<Entries>) {
        let _ = self.entries.set(Arc::downgrade(entries));
    }

    pub(crate) fn entries(&self) -> Result<Arc<Entries>> {
        self.entries
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Error::new(ErrorKind::Transport, "the segment has shut down"))
    }

    /// The router of the hand-off at forward position `pos` of hop `hop`,
    /// for a crossing this instance sends: a policy, or a selector that
    /// `Runtime::new` resolved, reading the head in `encoding`, its edge's;
    /// for a chooser's hand-off into another class, a cell of that class
    /// first, then an instance within it by the same.
    pub(crate) fn router(&self, hop: usize, pos: usize, encoding: gdl_model::Encoding) -> Router {
        let within = self.within(hop, pos, encoding);
        #[cfg(feature = "hub")]
        if let Some(target) = self.crossings.get(&(hop, pos)) {
            return Router::Cells(Arc::new(super::cells::Crossing::new(
                target.clone(),
                within,
                encoding,
            )));
        }
        Router::Roster(within)
    }

    /// The placement's router of the hand-off at forward position `pos`
    /// of hop `hop`, which chooses an instance.
    fn within(&self, hop: usize, pos: usize, encoding: gdl_model::Encoding) -> Within {
        match self.layout.request_router(hop, pos) {
            RequestRouter::RoundRobin => Within::Policy(Policy::RoundRobin),
            RequestRouter::Random => Within::Policy(Policy::Random),
            RequestRouter::Selector(name) => match self.selectors.get(name) {
                Some(selector) => Within::Selector {
                    name: Arc::from(name.as_str()),
                    selector: Arc::clone(selector),
                    encoding,
                },
                // Resolved for every crossing this instance sends; another
                // segment's crossing is never sent from here.
                None => Within::Policy(Policy::RoundRobin),
            },
        }
    }

    /// Hand a request to the next segment and wait for it to accept. A
    /// stage that accepts a streamed request answers with where to send its
    /// items. `router` chooses the instance: a policy picks at once; a
    /// selector is given `cx` and the head, and what it chose comes back
    /// with its lease, for the caller to hold while it holds the request.
    ///
    /// The hand-off is an offer ([`super::offers`]): when the instance
    /// declines it, or it never left, the router chooses again, up to
    /// `instance_failovers` more times ([`Runtime::offer_again`]). A
    /// chooser's hand-off into another class first chooses a cell, then
    /// an instance of it the same way, and offers the request to another
    /// cell when that one declines it ([`Runtime::fail_over`]); which
    /// hand-offs those are was settled when the runtime was built.
    ///
    /// With `timed`, it also returns when the hand-off left: after the
    /// wait for the roster, the routing, and the encoding, which are the
    /// sender's work, and before anything the receiver does.
    ///
    /// Not an `async fn`: one moves its arguments into locals of its body,
    /// so its future holds the envelope twice, as the argument it captured
    /// and as the local. The block below uses the captured envelope itself.
    #[allow(
        clippy::manual_async_fn,
        reason = "an `async fn` would hold the envelope twice"
    )]
    pub(crate) fn send_forward<'a>(
        &'a self,
        segment: &'a str,
        router: &'a Router,
        cx: &'a Cx,
        mut envelope: Forward,
        timed: bool,
    ) -> impl Future<Output = Result<Handed>> + 'a {
        async move {
            self.released().await?;
            #[cfg_attr(
                not(feature = "hub"),
                allow(
                    clippy::infallible_destructuring_match,
                    reason = "a hand-off into another class is under `hub`"
                )
            )]
            let within = match router {
                Router::Roster(within) => within,
                // A chooser's hand-off into another class: a cell of the class,
                // then an instance of it, always remote, since a chooser never
                // chooses itself (the cell's entry is of another class). Inline,
                // as the first offer to an instance is below: an `async` call
                // keeps its own state beside this future's, rather than in the
                // room the other path's state takes when this one is not running.
                #[cfg(feature = "hub")]
                Router::Cells(crossing) => {
                    let (instance, cell, mut held) = self
                        .choose_cell(crossing, cx, &mut envelope, &[])
                        .await?
                        .map_err(|no| crossing.no_cell(no))?;
                    let (offering, sent) = self.deliver(instance, &envelope, timed)?;
                    return match super::cells::answer(offering.await, crossing, cell, &mut held) {
                        Answer::Accepted(accepted) => Ok(Handed {
                            accepted,
                            sent,
                            held,
                        }),
                        Answer::Failed(error) => Err(error),
                        // Boxed, and only here, as for an instance.
                        Answer::Missed(missed) => {
                            drop(held);
                            let failover =
                                self.fail_over(crossing, cx, &mut envelope, timed, cell, missed);
                            Box::pin(failover).await
                        }
                    };
                }
            };
            let (instance, held) = self.choose(segment, within, cx, &mut envelope, &[]).await?;
            // Inline rather than a call that takes the envelope: an `async`
            // call holds what it takes in its own future, so every hand-off's
            // future would hold the envelope twice.
            let offer = if instance == self.fabric.instance() {
                let entering = self.enter(envelope)?;
                let sent = timed.then(tokio::time::Instant::now);
                Offer::entered(entering.await, sent)?
            } else {
                let (offering, sent) = self.deliver(instance, &envelope, timed)?;
                Offer::delivered(offering.await, sent, envelope)?
            };
            match offer {
                Offer::Accepted(accepted, sent) => Ok(Handed {
                    accepted,
                    sent,
                    held,
                }),
                // Boxed, and only here: inline, the failover's state would
                // grow this future, and so every hand-off's task.
                Offer::Missed(missed, envelope) => {
                    drop(held);
                    let failover =
                        self.offer_again(segment, within, cx, envelope, timed, instance, missed);
                    Box::pin(failover).await
                }
            }
        }
    }

    /// Ask an instance of `segment` to open a backward run.
    pub(crate) async fn send_open(&self, segment: &str, open: Open) -> Result<ReplyTo> {
        self.released().await?;
        let instance = self
            .routes
            .pick(segment, Picker::Open, Policy::RoundRobin, &self.random)?;
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
        let now = gdl::__runtime::wall_now_nanos();
        #[cfg(feature = "fault-injection")]
        let now = now.saturating_add_signed(self.config.faults.clock_offset_nanos);
        now
    }

    /// Whether an arriving message belongs to this deployment.
    pub(crate) fn check_identity(&self, version: u8, identity: Identity) -> Result<()> {
        super::envelope::check_version(version)?;
        if identity != self.identity {
            return Err(Error::new(
                ErrorKind::Placement,
                "the message belongs to a different contract or placement",
            ));
        }
        Ok(())
    }
}

/// How a crossing chooses where a hand-off goes.
#[derive(Clone)]
pub(crate) enum Router {
    /// An instance of the next segment, from the roster's routing table.
    Roster(Within),
    /// A chooser's hand-off into another class: a cell by the class's
    /// cell policy, then an instance of it by the placement's router.
    #[cfg(feature = "hub")]
    Cells(Arc<super::cells::Crossing>),
}

impl Router {
    /// How the instance is chosen: in the roster, or within the chosen
    /// cell.
    fn within(&self) -> &Within {
        match self {
            Self::Roster(within) => within,
            #[cfg(feature = "hub")]
            Self::Cells(crossing) => crossing.within(),
        }
    }

    /// The selector that chooses the instance, by name, if one does.
    pub(crate) fn selector(&self) -> Option<(&Arc<str>, &Arc<dyn Erased>)> {
        match self.within() {
            Within::Policy(_) => None,
            Within::Selector { name, selector, .. } => Some((name, selector)),
        }
    }
}

/// How an instance is chosen: the placement's router of a crossing.
#[derive(Clone)]
pub(crate) enum Within {
    /// `round_robin` or `random`: `pick`, and nothing else.
    Policy(Policy),
    /// A selector, by the name the placement gives it.
    Selector {
        name: Arc<str>,
        selector: Arc<dyn Erased>,
        encoding: gdl_model::Encoding,
    },
}

/// A hand-off the next segment accepted.
pub(crate) struct Handed {
    /// What it answered: for a streamed request, where to send the items.
    pub(crate) accepted: Accepted,
    /// When it left, if the caller asked: after the wait for the roster,
    /// the routing, and the encoding.
    pub(crate) sent: Option<tokio::time::Instant>,
    /// What the sender holds while it holds the request.
    pub(crate) held: Held,
}

/// What a hand-off holds while its sender holds the request: the lease of
/// the selector that chose where it went, if one did, and, for a
/// chooser's offer to a cell, the lease on the cell's follow, by which the
/// chooser knows when it no longer holds an offer to it, with the cell
/// policy's lease on its choice.
///
/// Under the `hub` feature the cell's lease makes it, and so every
/// hand-off's [`Handed`], 16 bytes larger: 32 bytes against the
/// selector's lease's 16, computed from the fields (a boxed lease is two
/// words, an `Arc` one, the policy's thin lease one, each `Option`
/// niched), not measured. A hand-off that is not
/// a chooser's offer to a cell carries `None` there: making it and
/// dropping it take no atomic operation, only the empty check on drop.
#[derive(Default)]
#[allow(dead_code, reason = "held for their drop")]
pub(crate) struct Held {
    selector: Option<Lease>,
    #[cfg(feature = "hub")]
    cell: Option<super::cells::CellLease>,
}

impl Held {
    /// A selector's lease, if it gave one.
    pub(crate) fn selector(selector: Option<Lease>) -> Self {
        #[cfg(feature = "hub")]
        let held = Self {
            selector,
            cell: None,
        };
        #[cfg(not(feature = "hub"))]
        let held = Self { selector };
        held
    }

    /// A chooser's offer to a cell: the selector's lease, if it gave one,
    /// and the lease on the cell's follow.
    #[cfg(feature = "hub")]
    pub(crate) fn cell(selector: Option<Lease>, cell: super::cells::CellLease) -> Self {
        Self {
            selector,
            cell: Some(cell),
        }
    }

    /// The lease on the follow of the cell this offer went to, if it went
    /// to a cell.
    #[cfg(feature = "hub")]
    pub(crate) fn cell_lease(&self) -> Option<&super::cells::CellLease> {
        self.cell.as_ref()
    }

    /// The cell this offer went to accepted it: tell the cell policy's
    /// lease, if it gave one.
    #[cfg(feature = "hub")]
    #[inline]
    pub(crate) fn accepted(&mut self) {
        if let Some(cell) = &mut self.cell {
            cell.accepted();
        }
    }
}

/// The selectors `segment` sends through: each crossing whose first node
/// is in another segment, whose sending run is in `segment`, and whose
/// router names a selector, as (hop, position, name).
fn sent_selectors<'a>(
    layout: &'a Layout,
    segment: &'a str,
) -> impl Iterator<Item = (usize, usize, &'a str)> + 'a {
    layout
        .hops()
        .iter()
        .enumerate()
        .flat_map(move |(hop, plan)| {
            plan.forward_runs.windows(2).filter_map(move |pair| {
                if pair[0].segment != segment {
                    return None;
                }
                match layout.request_router(hop, pair[1].start) {
                    RequestRouter::Selector(name) => Some((hop, pair[1].start, name.as_str())),
                    _ => None,
                }
            })
        })
}

/// Work an instance has taken on, for tests and diagnostics.
#[derive(Default)]
pub(crate) struct Served {
    pub(crate) forwards: AtomicUsize,
    pub(crate) runs: AtomicUsize,
    pub(crate) inputs: AtomicUsize,
    /// Forward hand-offs declined with no room (admission or egress
    /// refused, or the instance retired): what a load report counts as
    /// refusals. Only a refusal touches it.
    pub(crate) declined: AtomicUsize,
    /// Forward hand-offs that arrived, refused ones too: what a test
    /// counts to see where a request was offered.
    #[cfg(feature = "fault-injection")]
    pub(crate) offered: AtomicUsize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_cadence_belongs_to_runtime_config() {
        let defaults: RuntimeConfig = toml::from_str("").expect("defaults");
        assert_eq!(defaults.reservation_renew_interval, Duration::from_secs(30));
        let configured: RuntimeConfig =
            toml::from_str("reservation_renew_interval_ms = 25").expect("cadence");
        assert_eq!(
            configured.reservation_renew_interval,
            Duration::from_millis(25)
        );
        assert!(toml::from_str::<RuntimeConfig>("reservation_renew_interval_ms = 0").is_err());
    }

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
    fn cell_failovers_are_two_by_default_and_may_be_none() {
        let defaults: RuntimeConfig = toml::from_str("").expect("defaults");
        assert_eq!(defaults.cell_failovers, 2);
        let none: RuntimeConfig = toml::from_str("cell_failovers = 0\n").expect("none");
        assert_eq!(none.cell_failovers, 0);
    }

    #[test]
    fn instance_failovers_are_two_by_default_and_may_be_none() {
        let defaults: RuntimeConfig = toml::from_str("").expect("defaults");
        assert_eq!(defaults.instance_failovers, 2);
        let none: RuntimeConfig = toml::from_str("instance_failovers = 0\n").expect("none");
        assert_eq!(none.instance_failovers, 0);
        assert_eq!(none.cell_failovers, 2, "the two bounds are apart");
    }

    /// A chooser's damping windows, the periods a summary stays fresh, and
    /// `home_first`'s thresholds are settings, each with the value it had
    /// when it was fixed in the binary unless it is set.
    #[test]
    fn a_choosers_cell_settings_keep_their_defaults_unless_set() {
        for defaults in [
            RuntimeConfig::default(),
            toml::from_str::<RuntimeConfig>("").expect("defaults"),
        ] {
            assert_eq!(defaults.cell_damping_first, Duration::from_secs(1));
            assert_eq!(defaults.cell_damping_max, Duration::from_secs(30));
            assert_eq!(defaults.cell_fresh_periods, 3);
            assert_eq!(defaults.home_first.saturated_percent, 100);
            assert_eq!(defaults.home_first.spill_percent, 80);
        }
        let set: RuntimeConfig = toml::from_str(
            "cell_damping_first_ms = 2000\ncell_damping_max_ms = 5000\ncell_fresh_periods = 5\n\
             home_first_saturated_percent = 90\nhome_first_spill_percent = 0\n",
        )
        .expect("set");
        assert_eq!(set.cell_damping_first, Duration::from_secs(2));
        assert_eq!(set.cell_damping_max, Duration::from_secs(5));
        assert_eq!(set.cell_fresh_periods, 5);
        assert_eq!(set.home_first.saturated_percent, 90);
        assert_eq!(set.home_first.spill_percent, 0);
    }

    /// A chooser setting that breaks what the chooser relies on is refused,
    /// naming its key.
    #[test]
    fn a_choosers_cell_setting_out_of_its_bounds_is_refused_by_key() {
        for (bad, key) in [
            ("cell_damping_first_ms = 0\n", "cell_damping_first_ms"),
            (
                "cell_damping_first_ms = 3000\ncell_damping_max_ms = 2000\n",
                "cell_damping_max_ms",
            ),
            // Against the first window's default, 1 s.
            ("cell_damping_max_ms = 999\n", "cell_damping_max_ms"),
            ("cell_damping_max_ms = 3600001\n", "cell_damping_max_ms"),
            ("cell_fresh_periods = 0\n", "cell_fresh_periods"),
            ("cell_fresh_periods = 101\n", "cell_fresh_periods"),
            (
                "home_first_saturated_percent = 0\n",
                "home_first_saturated_percent",
            ),
            (
                "home_first_saturated_percent = 101\n",
                "home_first_saturated_percent",
            ),
            // Against the saturation threshold's default, 100.
            (
                "home_first_spill_percent = 101\n",
                "home_first_spill_percent",
            ),
            (
                "home_first_saturated_percent = 50\nhome_first_spill_percent = 60\n",
                "home_first_spill_percent",
            ),
        ] {
            let Err(error) = toml::from_str::<RuntimeConfig>(bad) else {
                panic!("{bad} was accepted");
            };
            assert!(
                error.to_string().contains(&format!("`{key}`")),
                "{bad}: {error}"
            );
        }
        // The bounds themselves are taken.
        let edge: RuntimeConfig = toml::from_str(
            "cell_damping_first_ms = 3600000\ncell_damping_max_ms = 3600000\ncell_fresh_periods = 100\n\
             home_first_saturated_percent = 1\nhome_first_spill_percent = 1\n",
        )
        .expect("the bounds");
        assert_eq!(edge.cell_damping_max, Duration::from_secs(3600));
    }

    #[test]
    fn runtime_config_rejects_an_unknown_key() {
        let Err(error) = toml::from_str::<RuntimeConfig>("admision_limit = 8\n") else {
            panic!("a misspelled key parsed");
        };
        assert!(error.to_string().contains("admision_limit"), "{error}");
    }
}
