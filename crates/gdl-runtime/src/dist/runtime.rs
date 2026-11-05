//! What every distributed hop of one segment instance shares.

use std::collections::BTreeMap;
use std::hash::RandomState;
use std::sync::Arc;

use crate::zstd::{Compression, HeadCompression};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Deserialize;

use super::admission::{Admission, Egress};
use super::calls::{CallLimit, Callee, Calls};
use super::envelope::{Accepted, Forward, Identity, Open};
use super::fabric::{Fabric, ReplyTo};
#[cfg(feature = "hub")]
use super::offers::Answer;
use super::offers::{Offer, Refusal, Refused};
use super::roster::{Live, Picker, Policy, Roster, Unchosen};
use gdl::__runtime::Meters;
use gdl::__runtime::Want;
use gdl::__runtime::{Erased, Lease};
use gdl::Cx;
use gdl::Decline;
use gdl::{Error, ErrorKind, Result};
use gdl::{
    FollowedPools, HomeFirstAt, HubRecords, InstanceKv, InstanceProperties, Lifecycle, OfferGate,
    PoolPolicies, Selectors,
};
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
    /// How many other pools a chooser offers a request to after the pool it
    /// chose first declines it, or the hand-off to it provably never left
    /// this instance: only for a hand-off into another class, from an
    /// instance in a pool. 0 offers each request to one pool.
    pub pool_failovers: usize,
    /// How long a chooser passes over a pool after its first decline. The
    /// window doubles with each further decline after it ended, up to
    /// `pool_damping_max`. At least 1 ms.
    pub pool_damping_first: Duration,
    /// The longest a chooser passes over a pool that keeps declining: it
    /// is tried again at least this often. From `pool_damping_first` to
    /// one hour.
    pub pool_damping_max: Duration,
    /// The longest a chooser waits before it offers a request again to the
    /// pool its pool policy re-queued it for (`PoolPolicy::requeue`), when
    /// no offer it made to that pool ends first: room that another chooser
    /// frees, or a property that becomes ready, is seen no later. From
    /// 1 ms to one hour.
    pub pool_requeue_backoff: Duration,
    /// How many of its periods a pool's load summary stays fresh after it
    /// reached the chooser's hub. A policy that chooses by load, as `home_first`
    /// does, passes over a pool whose summary is stale. From 1 to 100.
    pub pool_fresh_periods: u32,
    /// The thresholds of the chooser's `home_first`: its saturation
    /// threshold, from 1 to 100 percent of a pool's admission limit, and
    /// its spill bound, at most the saturation threshold.
    pub home_first: HomeFirstAt,
    /// How many more times a crossing's router chooses an instance after
    /// the one it chose declined a hand-off (no room, or retired), or the
    /// hand-off to it provably never left this instance: for every
    /// hand-off a policy or a selector routes. The same instance may be
    /// chosen again. 0 makes each refusal final. It also bounds the more
    /// tries of a call to a unary service in another segment.
    pub instance_failovers: usize,
    /// The most bytes one compressed value that arrives on an edge with a
    /// `[codec]` entry may decompress into. A frame that states more, or
    /// none, is refused before anything is allocated.
    pub decompressed_max: usize,
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
    /// The bound of a call to a unary service in another segment whose
    /// request has no deadline: the call fails with `DeadlineExceeded` once
    /// it passes, and the service runs under a context with that deadline.
    /// A request's own deadline bounds its calls otherwise.
    pub call_timeout: Duration,
    /// The largest reply, in bytes, that a unary service in this segment
    /// sends to a caller in another: a larger one fails the call with
    /// `Codec`. At most 16 MiB less 64 KiB: velo's 16 MiB frame ends a
    /// reply, and also carries the reply's tag and velo's header.
    pub call_reply_max: usize,
    /// Calls from other segments one instance serves at once, each with a
    /// permit of its own, apart from the requests' admission: a call does
    /// not join its request's share. A call that finds none free is
    /// refused before the service runs, and the caller tries another
    /// instance. `None` is `admission_limit`.
    pub call_limit: Option<usize>,
    /// How long past its call's bound a reply may hold its call permit
    /// while velo has not sent it: a caller that stops reading cannot hold
    /// `call_limit` for longer.
    pub call_reply_grace: Duration,
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
    /// How long a leave hook may run from the stop. `None`, or anything
    /// past `drain`, ends it with the drain's bound.
    pub leave: Option<Duration>,
    /// How long after its retirement an instance waits for its peers
    /// before it tells its leave hooks that nothing confirmed that it is
    /// out of routing. At most half of `drain` counts from the stop.
    /// `None` is the setup timeout and twice the pool's `summary_ms`.
    pub leave_after: Option<Duration>,
    /// How long after its stop an instance its pool's hub asked to retire
    /// may stay, once drained, if nothing ends its hold first. `None`
    /// means the drain's bound and 30 s, which outlasts an
    /// operator's `drainBound`: a hold that ends by itself exits a
    /// container, which its pod then starts again.
    pub hold: Option<Duration>,
    /// Where the runtime records what each request does in this segment:
    /// its request path, the response streams it sends on, and, at the
    /// source, the request end to end; `None` records nothing. Not a
    /// setting: a segment binary gives its process's registry when its
    /// system listener can serve it.
    pub metrics: Option<prometheus::Registry>,
    /// The selectors a crossing's or a call's router can name, by name. A name the
    /// placement uses that is not here is gdl's own selector of that name,
    /// with its defaults. Not a setting: a segment binary registers them
    /// through its `Builder`.
    pub selectors: Selectors,
    /// The pool policies a placement's `[pools.<class>] policy` can name,
    /// by name. A name that is not here is gdl's own policy of that name.
    /// Not a setting: a segment binary registers them through its
    /// `Builder`. A chooser launched through a `ProcessConfig` checks the
    /// names against its `runtime`'s.
    pub pool_policies: PoolPolicies,
    /// The handle through which the segment's nodes read the pools its
    /// chooser follows. Not a setting: a segment binary's `Builder` gives
    /// it to the nodes it builds, and the launch binds it to the chooser's
    /// tables. A segment that sends into no other class, or runs without a
    /// hub, leaves it unbound.
    pub followed: FollowedPools,
    /// The record service through which the segment's nodes take tables of
    /// their pool's hub ([`gdl::PoolRecords::current`]). Not a setting: a
    /// `ProcessConfig` with a `[hub]` and a `[pool]` makes one that tables
    /// can be taken from, a segment binary's `Builder` gives it to the
    /// nodes it builds, and the launch binds it to the hub once the
    /// instance has registered there. Without both, it is not bound. Give
    /// each instance its own.
    pub records: HubRecords,
    /// The handle through which the segment's nodes say which properties the
    /// instance has. Not a setting: a segment binary's `Builder` gives it
    /// to the nodes it builds, and an entry instance in a pool reports what
    /// they set with its load reports.
    pub properties: InstanceProperties,
    /// The handle through which the segment's nodes say what the
    /// instance's KV cache holds. Not a setting: a segment binary's
    /// `Builder` gives it to the nodes it builds, and an entry instance in
    /// a pool reports what they set with its load reports.
    pub kv: InstanceKv,
    /// The gate by which the segment's nodes decide each offer from
    /// another class before admission, and read their own pool's latest
    /// summary. Not a setting: a segment binary's `Builder` gives it to the
    /// nodes it builds.
    pub gate: OfferGate,
    /// The handle through which the segment's nodes register their leave
    /// hooks. Not a setting: a segment binary's `Builder` gives it to the
    /// nodes it builds, and the launch runs what they registered.
    pub lifecycle: Lifecycle,
    /// The links through which the segment's nodes call unary services in
    /// other segments, and the services it hosts for callers in others.
    /// Not a setting: a segment binary's `Builder` makes the links its
    /// nodes take, and the deployment binds them when it starts. Give each
    /// instance its own: a clone shares the links, and a deployment binds
    /// only links to the services its own segment calls.
    pub calls: Calls,
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
    /// Hold the stop that a streamed run passes up its input until the run
    /// has retired that input, as a host too busy to run the stop watcher
    /// before the run ends would.
    pub stop_after_input_retired: bool,
    /// Never announce that the instance detached from its draining pool,
    /// as a build from before the announcement does.
    pub withhold_detached: bool,
    /// The announcement that the instance detached never gets an answer,
    /// as from a hub that stops answering.
    pub stall_detached: bool,
    /// Keep a clone of the bytes of each reply to a call that holds a call
    /// permit, as velo keeps a reply it cannot write to a peer that does
    /// not read: the permit is then released only at the call's bound plus
    /// `call_reply_grace`.
    pub retain_call_replies: Option<Arc<std::sync::Mutex<Vec<bytes::Bytes>>>>,
    /// Report this many calls this instance makes, answered, as failed
    /// after their send, as a transport that lost the reply would: the
    /// service ran.
    pub lose_call_replies: Option<Arc<AtomicUsize>>,
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

    /// How many calls from other segments one instance serves at once:
    /// `call_limit`, or the admission limit.
    pub fn call_permits(&self) -> usize {
        self.call_limit.unwrap_or(self.admission_limit)
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            admission_limit: 1024,
            admission_wait: Duration::from_millis(100),
            attempt_limit: 16,
            setup_timeout: Duration::from_secs(10),
            pool_failovers: 2,
            pool_damping_first: Self::DEFAULT_POOL_DAMPING_FIRST,
            pool_damping_max: Self::DEFAULT_POOL_DAMPING_MAX,
            pool_requeue_backoff: Self::DEFAULT_POOL_REQUEUE_BACKOFF,
            pool_fresh_periods: gdl::Load::FRESH_PERIODS,
            home_first: HomeFirstAt::DEFAULT,
            instance_failovers: 2,
            decompressed_max: Self::DEFAULT_DECOMPRESSED_MAX,
            anchor_timeout: None,
            reservation_renew_interval: Duration::from_secs(30),
            clock_skew: Duration::from_millis(50),
            default_deadline: gdl_model::DEFAULT_DEADLINE,
            call_timeout: Self::DEFAULT_CALL_TIMEOUT,
            call_reply_max: Self::DEFAULT_CALL_REPLY_MAX,
            call_limit: None,
            call_reply_grace: Self::DEFAULT_CALL_REPLY_GRACE,
            egress_limit: 1024,
            heartbeat_interval: None,
            drain: None,
            leave: None,
            leave_after: None,
            hold: None,
            metrics: None,
            selectors: Selectors::new(),
            pool_policies: PoolPolicies::new(),
            followed: FollowedPools::new(),
            records: HubRecords::new(),
            properties: InstanceProperties::new(),
            kv: InstanceKv::new(),
            gate: OfferGate::new(),
            lifecycle: Lifecycle::new(),
            calls: Calls::default(),
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
    pool_failovers: usize,
    pool_damping_first_ms: u64,
    pool_damping_max_ms: u64,
    pool_requeue_backoff_ms: u64,
    pool_fresh_periods: u32,
    home_first_saturated_percent: u64,
    home_first_spill_percent: u64,
    instance_failovers: usize,
    decompressed_max: usize,
    anchor_timeout_ms: Option<u64>,
    reservation_renew_interval_ms: u64,
    clock_skew_ms: u64,
    default_deadline_ms: u64,
    call_timeout_ms: u64,
    call_reply_max: usize,
    call_limit: Option<usize>,
    call_reply_grace_ms: u64,
    egress_limit: usize,
    heartbeat_interval_ms: Option<u64>,
    drain_ms: Option<u64>,
    leave_ms: Option<u64>,
    leave_after_ms: Option<u64>,
    hold_ms: Option<u64>,
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
            pool_failovers: config.pool_failovers,
            pool_damping_first_ms: ms(config.pool_damping_first),
            pool_damping_max_ms: ms(config.pool_damping_max),
            pool_requeue_backoff_ms: ms(config.pool_requeue_backoff),
            pool_fresh_periods: config.pool_fresh_periods,
            home_first_saturated_percent: config.home_first.saturated_percent,
            home_first_spill_percent: config.home_first.spill_percent,
            instance_failovers: config.instance_failovers,
            decompressed_max: config.decompressed_max,
            anchor_timeout_ms: config.anchor_timeout.map(ms),
            reservation_renew_interval_ms: ms(config.reservation_renew_interval),
            clock_skew_ms: ms(config.clock_skew),
            default_deadline_ms: ms(config.default_deadline),
            call_timeout_ms: ms(config.call_timeout),
            call_reply_max: config.call_reply_max,
            call_limit: config.call_limit,
            call_reply_grace_ms: ms(config.call_reply_grace),
            egress_limit: config.egress_limit,
            heartbeat_interval_ms: config.heartbeat_interval.map(ms),
            drain_ms: config.drain.map(ms),
            leave_ms: config.leave.map(ms),
            leave_after_ms: config.leave_after.map(ms),
            hold_ms: config.hold.map(ms),
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
            pool_failovers: s.pool_failovers,
            pool_damping_first: Duration::from_millis(s.pool_damping_first_ms),
            pool_damping_max: Duration::from_millis(s.pool_damping_max_ms),
            pool_requeue_backoff: Duration::from_millis(s.pool_requeue_backoff_ms),
            pool_fresh_periods: s.pool_fresh_periods,
            home_first: HomeFirstAt {
                saturated_percent: s.home_first_saturated_percent,
                spill_percent: s.home_first_spill_percent,
            },
            instance_failovers: s.instance_failovers,
            decompressed_max: s.decompressed_max,
            anchor_timeout: s.anchor_timeout_ms.map(Duration::from_millis),
            reservation_renew_interval: Duration::from_millis(s.reservation_renew_interval_ms),
            clock_skew: Duration::from_millis(s.clock_skew_ms),
            default_deadline: Duration::from_millis(s.default_deadline_ms),
            call_timeout: Duration::from_millis(s.call_timeout_ms),
            call_reply_max: s.call_reply_max,
            call_limit: s.call_limit,
            call_reply_grace: Duration::from_millis(s.call_reply_grace_ms),
            egress_limit: s.egress_limit,
            heartbeat_interval: s.heartbeat_interval_ms.map(Duration::from_millis),
            drain: s.drain_ms.map(Duration::from_millis),
            leave: s.leave_ms.map(Duration::from_millis),
            leave_after: s.leave_after_ms.map(Duration::from_millis),
            hold: s.hold_ms.map(Duration::from_millis),
            metrics: None,
            selectors: Selectors::new(),
            pool_policies: PoolPolicies::new(),
            followed: FollowedPools::new(),
            records: HubRecords::new(),
            properties: InstanceProperties::new(),
            kv: InstanceKv::new(),
            gate: OfferGate::new(),
            lifecycle: Lifecycle::new(),
            calls: Calls::default(),
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
        if self.call_limit == Some(0) {
            return Err(Error::new(
                ErrorKind::Placement,
                "runtime setting `call_limit` must be at least 1",
            ));
        }
        // Each is a semaphore's size; tokio panics on a larger one.
        for (name, limit) in [
            ("admission_limit", self.admission_limit),
            ("egress_limit", self.egress_limit),
            ("call_limit", self.call_permits()),
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
        if self.call_timeout.is_zero() {
            return Err(Error::new(
                ErrorKind::Placement,
                "runtime setting `call_timeout_ms` must be positive",
            ));
        }
        let (least, most) = (Self::CALL_REPLY_MIN, Self::FRAME_MAX - Self::FRAME_ROOM);
        if !(least..=most).contains(&self.call_reply_max) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "runtime setting `call_reply_max` must be from {least} to {most} bytes: a reply past it is replaced by an error of up to {} bytes, and velo's frame of {} bytes also carries velo's own header",
                    super::calls::FALLBACK_MAX,
                    Self::FRAME_MAX
                ),
            ));
        }
        self.validate_pools()?;
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
    /// The most bytes a compressed value may decompress into, unless
    /// `decompressed_max` is set: 256 MiB.
    pub const DEFAULT_DECOMPRESSED_MAX: usize = 256 << 20;

    /// A call's bound when its request has no deadline, unless
    /// `call_timeout_ms` is set: 30 s.
    pub(crate) const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);

    /// The largest reply a unary service sends to another segment, unless
    /// `call_reply_max` is set: 8 MiB.
    pub(crate) const DEFAULT_CALL_REPLY_MAX: usize = 8 << 20;

    /// How long past its call's bound a reply holds its permit at most,
    /// unless `call_reply_grace_ms` is set: 1 s.
    pub(crate) const DEFAULT_CALL_REPLY_GRACE: Duration = Duration::from_secs(1);

    /// velo's TCP frame: a unary reply cannot be larger.
    pub(crate) const FRAME_MAX: usize = 16 << 20;

    /// What a reply's frame holds besides the response: its tag, one byte,
    /// and velo's header, which names the call and is far smaller.
    pub(crate) const FRAME_ROOM: usize = 64 << 10;

    /// The least `call_reply_max` the settings take: room for the error
    /// that replaces a reply past the limit, whose size has a fixed bound,
    /// with room to spare, so that the replacement always fits.
    pub const CALL_REPLY_MIN: usize = 512;
}

impl RuntimeConfig {
    /// The damping window after a pool's first decline, unless
    /// `pool_damping_first_ms` is set: 1 s.
    pub(crate) const DEFAULT_POOL_DAMPING_FIRST: Duration = Duration::from_secs(1);

    /// The longest damping window, unless `pool_damping_max_ms` is set:
    /// 30 s.
    pub(crate) const DEFAULT_POOL_DAMPING_MAX: Duration = Duration::from_secs(30);

    /// The longest wait before a re-queued request is offered again,
    /// unless `pool_requeue_backoff_ms` is set: 50 ms.
    pub(crate) const DEFAULT_POOL_REQUEUE_BACKOFF: Duration = Duration::from_millis(50);

    /// The longest damping window: a pool is tried again at least once an
    /// hour, and a window's end stays far inside what the clock holds.
    const MAX_DAMPING: Duration = Duration::from_secs(3600);

    /// The most periods a summary may stay fresh: a summary 100 periods
    /// old says nothing of the pool's load now.
    const MAX_FRESH_PERIODS: u32 = 100;

    /// Refuse the chooser's pool settings that break what it relies on:
    /// windows that cannot grow or end, a summary that is never fresh or
    /// is fresh too long, and thresholds outside a pool's admission limit,
    /// or a spill bound past the saturation threshold, which no pool
    /// elsewhere could reach unsaturated.
    fn validate_pools(&self) -> Result<()> {
        let refuse = |key: &str, why: String| {
            Err(Error::new(
                ErrorKind::Placement,
                format!("runtime setting `{key}` {why}"),
            ))
        };
        let ms = |d: Duration| d.as_millis();
        if self.pool_damping_first < Duration::from_millis(1) {
            return refuse("pool_damping_first_ms", "must be at least 1".into());
        }
        if self.pool_damping_max < self.pool_damping_first {
            return refuse(
                "pool_damping_max_ms",
                format!(
                    "is {}; it must be at least `pool_damping_first_ms`, {}",
                    ms(self.pool_damping_max),
                    ms(self.pool_damping_first)
                ),
            );
        }
        if self.pool_damping_max > Self::MAX_DAMPING {
            return refuse(
                "pool_damping_max_ms",
                format!("must be at most {}", ms(Self::MAX_DAMPING)),
            );
        }
        if self.pool_requeue_backoff < Duration::from_millis(1) {
            return refuse("pool_requeue_backoff_ms", "must be at least 1".into());
        }
        if self.pool_requeue_backoff > Self::MAX_DAMPING {
            return refuse(
                "pool_requeue_backoff_ms",
                format!("must be at most {}", ms(Self::MAX_DAMPING)),
            );
        }
        if !(1..=Self::MAX_FRESH_PERIODS).contains(&self.pool_fresh_periods) {
            return refuse(
                "pool_fresh_periods",
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
    /// The forward entries whose edge compresses its head, by hop and
    /// position: what a frame that arrives there may decompress into.
    compressed: BTreeMap<(u16, u16), Compression>,
}

impl Entries {
    pub(crate) fn add_forward(&mut self, hop: u16, pos: u16, entry: ForwardEntry) {
        self.forward.insert((hop, pos), entry);
    }

    pub(crate) fn add_open(&mut self, hop: u16, pos: u16, entry: OpenEntry) {
        self.open.insert((hop, pos), entry);
    }

    /// Read a frame that arrives at forward entry `hop`, `pos`, as
    /// `compression` says.
    pub(crate) fn add_compressed(&mut self, hop: u16, pos: u16, compression: Compression) {
        self.compressed.insert((hop, pos), compression);
    }

    /// How a head that arrives at forward entry `hop`, `pos` is
    /// compressed: `None` where its edge does not compress.
    pub(crate) fn compressed(&self, hop: u16, pos: u16) -> Option<Compression> {
        self.compressed.get(&(hop, pos)).copied()
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
    /// How each forward edge this segment can send on compresses its
    /// head, by hop and the position the edge enters: empty unless a
    /// `[codec]` entry names one.
    heads: BTreeMap<(u16, u16), HeadCompression>,
    pub(crate) segment: String,
    pub(crate) config: RuntimeConfig,
    pub(crate) admission: Admission,
    /// The calls from other segments this instance serves at once.
    pub(crate) call_limit: Arc<CallLimit>,
    pub(crate) egress: Egress,
    pub(crate) identity: Identity,
    /// Whom to route to, shared with the roster the instance launched
    /// with, which follows membership.
    routes: Arc<Live>,
    /// The selectors this instance's crossings name, resolved once: one
    /// per name, shared by every crossing that names it.
    selectors: BTreeMap<String, Arc<dyn Erased>>,
    /// A chooser's hand-offs into other classes, by hop and position,
    /// with the pools each chooses from; empty for any other instance.
    #[cfg(feature = "hub")]
    crossings: BTreeMap<(usize, usize), super::pools::Target>,
    random: RandomState,
    /// Set once a retiring instance stops taking hand-offs and openings.
    closed: AtomicBool,
    /// When it first closed, for the tests that order the close after
    /// the senders' promises.
    #[cfg(feature = "fault-injection")]
    closed_at: std::sync::OnceLock<tokio::time::Instant>,
    /// Why the instance declines offers, the hand-offs from another
    /// class, if it does: [`DRAINED`] once an entry instance of a
    /// draining pool stops taking them, which nothing undoes, and the
    /// segment's own refusal ([`Runtime::decline_offers_as`]) in the bits
    /// below it. One byte, so that an offer reads both in one load.
    declining: AtomicU8,
    /// Whether each hop's hand-offs to this segment are offers, worked out
    /// once.
    offers: Vec<bool>,
    /// What the segment's nodes said the offers want, each with whether a
    /// hop's offers read it, so that the start can refuse one that none
    /// reads ([`Runtime::unread_want`]).
    wants: Box<[(Arc<dyn Want>, AtomicBool)]>,
    /// Whether the instance counts the requests it holds that may still
    /// send work onward ([`Admission::senders`]): only in a class that a
    /// hop enters, whose pools drain segment by segment. Elsewhere no drain
    /// reads the count, and a request pays nothing for it.
    pub(crate) counts_senders: bool,
    /// Whether a request the instance holds counts as one that may still
    /// send until its response stream ends, and a call it accepted until
    /// the call ends: when it counts senders and its nodes call a unary
    /// service in another segment, which a returned stream can still do.
    pub(crate) sends_until_answered: bool,
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
const DRAINED: u8 = 0b1000;
/// The segment's own refusal, as gone.
const GONE: u8 = 0b0100;
/// The segment's own refusal, as this instance's alone.
const INSTANCE: u8 = 0b0010;
/// The segment's own refusal, as full.
const FULL: u8 = 0b0001;
/// The segment's own refusal, as not present.
const NOT_PRESENT: u8 = 0b1_0000;
/// The segment's own refusal, as over its budget.
const BUDGET: u8 = 0b10_0000;

/// Velo handler names for one deployment.
pub(crate) struct Names {
    pub(crate) forward: String,
    pub(crate) open: String,
}

/// Refuse a segment that sends or receives on an edge that a `[codec]`
/// entry compresses, when it was not `built` with the `zstd` feature: the
/// generator turns the feature on for each such segment, so this is a
/// build that did not come from it.
pub(crate) fn compresses(layout: &Layout, segment: &str, built: bool) -> Result<()> {
    if built || !layout.compressing_segments().contains(segment) {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::Placement,
        format!(
            "segment `{segment}` sends or receives on an edge that a `[codec]` entry compresses, but was built without gdl-runtime's `zstd` feature"
        ),
    ))
}

/// How each forward edge compresses its head, by hop and the position it
/// enters, reading frames of up to `limit` bytes. The sender keeps the
/// plain head after compressing it only where routing reads it again: a
/// selector, `affinity` among them, or the pool policy of a crossing that
/// `pooled` says chooses pools ([`HeadCompression`]).
pub(crate) fn compressed_heads(
    layout: &Layout,
    limit: usize,
    pooled: impl Fn(usize, usize) -> bool,
) -> BTreeMap<(u16, u16), HeadCompression> {
    let mut heads = BTreeMap::new();
    for (hop, plan) in layout.hops().iter().enumerate() {
        let len: usize = plan.forward_runs.iter().map(|run| run.stages.len()).sum();
        for pos in 1..len {
            let Some(edge) = layout
                .compression(hop, gdl_model::Leg::Forward, pos)
                .filter(|edge| edge.head)
            else {
                continue;
            };
            let reads = !matches!(
                layout.request_router(hop, pos),
                RequestRouter::RoundRobin | RequestRouter::Random
            ) || pooled(hop, pos);
            if let (Ok(hop), Ok(pos)) = (u16::try_from(hop), u16::try_from(pos)) {
                let head = HeadCompression {
                    compression: Compression::new(edge.floor, limit),
                    keeps_plain: reads,
                };
                heads.insert((hop, pos), head);
            }
        }
    }
    heads
}

impl Runtime {
    /// How the head that a hand-off sends into position `pos` of hop `hop`
    /// is compressed: `None` where the edge does not compress, and at once
    /// for a layout with no `[codec]` entry.
    pub(crate) fn head_compression(&self, hop: u16, pos: u16) -> Option<HeadCompression> {
        if self.heads.is_empty() {
            return None;
        }
        self.heads.get(&(hop, pos)).copied()
    }

    pub(crate) fn new(
        fabric: Fabric,
        layout: Layout,
        segment: String,
        roster: &Roster,
        config: RuntimeConfig,
        meters: Option<Meters>,
    ) -> Result<Self> {
        let mut selectors = BTreeMap::new();
        for (hop, at, name) in layout.sent_selectors(&segment) {
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
        for (service, name) in layout.call_selectors(&segment) {
            if selectors.contains_key(name) {
                continue;
            }
            let Some(selector) = config.selectors.resolve(name) else {
                return Err(Error::new(
                    ErrorKind::Placement,
                    format!(
                        "segment `{segment}` calls unary node `{service}` through selector `{name}`, which nothing registers and gdl does not have"
                    ),
                ));
            };
            selectors.insert(name.to_string(), selector);
        }
        #[cfg(feature = "hub")]
        let crossings = match roster.pools() {
            Some(pools) => super::pools::targets(
                &layout,
                &segment,
                pools,
                &config.pool_policies,
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
        let wants = config
            .properties
            .wanted()
            .into_iter()
            .map(|want| (want, AtomicBool::new(false)))
            .collect();
        let counts_senders = layout
            .class_of(&segment)
            .is_some_and(|class| !layout.entry_segments(class).is_empty());
        compresses(&layout, &segment, cfg!(feature = "zstd"))?;
        #[cfg(feature = "hub")]
        let pooled = |hop: usize, pos: usize| crossings.contains_key(&(hop, pos));
        #[cfg(not(feature = "hub"))]
        let pooled = |_: usize, _: usize| false;
        let heads = compressed_heads(&layout, config.decompressed_max, pooled);
        let sends_until_answered = counts_senders && layout.calls_from(&segment).next().is_some();
        Ok(Self {
            heads,
            counts_senders,
            sends_until_answered,
            admission: Admission::new(
                config.admission_limit,
                config.admission_wait,
                config.attempt_limit,
            ),
            egress: Egress::new(config.egress_limit, config.admission_wait),
            call_limit: CallLimit::new(config.call_permits(), config.call_reply_grace),
            routes: Arc::clone(roster.live()),
            selectors,
            #[cfg(feature = "hub")]
            crossings,
            random: RandomState::new(),
            closed: AtomicBool::new(false),
            #[cfg(feature = "fault-injection")]
            closed_at: std::sync::OnceLock::new(),
            declining: AtomicU8::new(0),
            offers,
            wants,
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
        #[cfg(feature = "fault-injection")]
        self.closed_at.get_or_init(tokio::time::Instant::now);
        self.closed.store(true, Ordering::SeqCst);
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) fn closed_at(&self) -> Option<tokio::time::Instant> {
        self.closed_at.get().copied()
    }

    /// The requests this instance holds that may still send work onward:
    /// a forward hand-off not yet handed on or run to its inner end's
    /// stream, and an operator's `next` still held. Always 0 outside a
    /// class that a hop enters, which does not count them.
    pub(crate) fn senders(&self) -> usize {
        self.admission.senders()
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
    /// pool is gone: a forward hand-off on a hop that crosses into this
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
            Decline::NotPresent => NOT_PRESENT,
            Decline::Gone => GONE,
            Decline::Instance => INSTANCE,
            Decline::Budget => BUDGET,
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
    /// pool takes no new work from another pool again, and the segment's
    /// own reason otherwise. One atomic load per offer, and none for any
    /// other hand-off.
    pub(crate) fn check_offer(&self) -> std::result::Result<(), Refusal> {
        let declining = self.declining.load(Ordering::SeqCst);
        if declining == 0 {
            return Ok(());
        }
        let (why, decline) = if declining & DRAINED != 0 {
            (
                "the instance's pool is draining and takes no new work from another pool",
                Decline::Gone,
            )
        } else if declining & GONE != 0 {
            (
                "the instance's segment takes no work from another pool by its own policy",
                Decline::Gone,
            )
        } else if declining & INSTANCE != 0 {
            (
                "the instance cannot serve work from another pool by its own policy; another instance of its pool may",
                Decline::Instance,
            )
        } else if declining & NOT_PRESENT != 0 {
            (
                "the instance's segment does not have what work from another pool wants, by its own policy",
                Decline::NotPresent,
            )
        } else if declining & BUDGET != 0 {
            (
                "the instance's segment is over its budget for work from another pool, by its own policy",
                Decline::Budget,
            )
        } else {
            (
                "the instance's segment has no room for work from another pool by its own policy",
                Decline::Full,
            )
        };
        Err(Refusal {
            error: Error::new(ErrorKind::Overloaded, why),
            decline,
        })
    }

    /// What the segment's nodes said the offers want, each with whether a
    /// hop's offers read it: a hop sets it as it is built.
    pub(crate) fn wants(&self) -> &[(Arc<dyn Want>, AtomicBool)] {
        &self.wants
    }

    /// A want that no hop's offers read, once every hop is built, when
    /// the segment takes offers at all: it would check nothing. A segment
    /// that takes none, as outside a pool, reads none, and that is no
    /// mistake.
    pub(crate) fn unread_want(&self) -> Option<&dyn Want> {
        if !self.offers.iter().any(|offered| *offered) {
            return None;
        }
        self.wants
            .iter()
            .find(|(_, read)| !read.load(Ordering::Relaxed))
            .map(|(want, _)| &**want)
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

    /// Refuse the segment when a selector it sends through needs a property
    /// kind that the class of a crossing it routes does not supply
    /// ([`gdl::Selector::NEEDS`]), as `cargo gdl check` holds a bound one: a
    /// selector registered in code is checked only here. Only the crossings
    /// this segment sends are its own.
    pub(crate) fn check_selector_needs(&self, manifest: &gdl_model::Manifest) -> Result<()> {
        let consumers: Vec<gdl_model::PropertyConsumer> = self
            .selectors
            .iter()
            .flat_map(|(name, selector)| {
                gdl_model::selector_consumers(&self.layout, name, selector.needs())
            })
            .filter(|consumer| consumer.segment.as_deref() == Some(self.segment.as_str()))
            .collect();
        if consumers.is_empty() {
            return Ok(());
        }
        let unmet = gdl_model::property_supply(manifest, &self.layout, &consumers);
        if unmet.is_empty() {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::Placement, unmet.join("\n")))
        }
    }

    /// The router of the hand-off at forward position `pos` of hop `hop`,
    /// for a crossing this instance sends: a policy, or a selector that
    /// `Runtime::new` resolved, reading the head in `encoding`, its edge's;
    /// for a chooser's hand-off into another class, a pool of that class
    /// first, then an instance within it by the same.
    pub(crate) fn router(&self, hop: usize, pos: usize, encoding: gdl_model::Encoding) -> Router {
        let within = self.within(hop, pos, encoding);
        #[cfg(feature = "hub")]
        if let Some(target) = self.crossings.get(&(hop, pos)) {
            return Router::Pools(Arc::new(super::pools::Crossing::new(
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
    /// chooser's hand-off into another class first chooses a pool, then
    /// an instance of it the same way, and offers the request to another
    /// pool when that one declines it ([`Runtime::fail_over`]); which
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
                // A chooser's hand-off into another class: a pool of the class,
                // then an instance of it, always remote, since a chooser never
                // chooses itself (the pool's entry is of another class). Inline,
                // as the first offer to an instance is below: an `async` call
                // keeps its own state beside this future's, rather than in the
                // room the other path's state takes when this one is not running.
                #[cfg(feature = "hub")]
                Router::Pools(crossing) => {
                    let (instance, pool, mut held) = self
                        .choose_pool(crossing, cx, &mut envelope, super::pools::Left::NONE)
                        .await?
                        .map_err(|no| crossing.no_pool(no))?;
                    // Read before the offer, so that a re-queue after it
                    // sees every lease end since (`Leases::room`).
                    let ends = held.pool_lease().map_or(0, super::pools::PoolLease::ends);
                    let counted = super::pools::timed(timed, &held);
                    let (offering, sent) = self.deliver(instance, &mut envelope, counted)?;
                    let offered = offering.await;
                    return match super::pools::answer(offered, crossing, pool, &mut held, sent) {
                        Answer::Accepted(accepted) => Ok(Handed {
                            accepted,
                            sent,
                            held,
                        }),
                        Answer::Failed(error) => Err(error),
                        // Boxed, and only here, as for an instance.
                        Answer::Missed(missed) => {
                            let seen = super::pools::Seen::of(held.pool_lease(), ends);
                            let choice = held.missed();
                            let declined = super::pools::Declined {
                                pool,
                                instance,
                                choice,
                                missed,
                                seen,
                            };
                            let failover =
                                self.fail_over(crossing, cx, &mut envelope, timed, declined);
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
                let (offering, sent) = self.deliver(instance, &mut envelope, timed)?;
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
                super::envelope::encode(&open)?.into(),
                self.config.setup_timeout,
            )
            .await?;
        super::envelope::decode::<Result<ReplyTo, Error>>(&reply)?
    }

    /// Call `callee` with `payload`, the encoded request behind its
    /// envelope's room. The call's bound is the request's deadline, or now
    /// plus `call_timeout`: the envelope carries it, and the call fails with
    /// `DeadlineExceeded` once it passes. The reply is the handler's,
    /// uncopied. A call that an instance refused, or that velo refused to
    /// send, is tried on the instance the policy picks next, within the
    /// bound ([`Runtime::call_again`]); the first try is made here, inline,
    /// so that a call answered at once costs no more than it did. A call
    /// through a selector goes to [`Runtime::call_selected`].
    pub(crate) async fn call(
        &self,
        callee: &Callee,
        cx: &Cx,
        payload: super::envelope::CallHead,
    ) -> Result<bytes::Bytes> {
        let bound = cx
            .deadline()
            .unwrap_or_else(|| gdl::Deadline::after(self.config.call_timeout));
        if left(bound).is_zero() {
            return Err(Error::deadline_exceeded());
        }
        tokio::time::timeout(left(bound), self.released())
            .await
            .map_err(|_| Error::deadline_exceeded())??;
        let policy = match &callee.within {
            Within::Policy(policy) => *policy,
            Within::Selector {
                selector, encoding, ..
            } => {
                return self
                    .call_selected(callee, &**selector, *encoding, cx, bound, payload)
                    .await;
            }
        };
        let instance = self.pick_call(callee, policy)?;
        let (envelope, left) = self.call_envelope(
            callee.index,
            super::envelope::WireCx::bounded(cx, bound),
            payload,
            bound,
        )?;
        // A clone of the envelope is a count: its bytes are shared, and
        // kept for another try.
        let called = self
            .fabric
            .call_bounded(instance, super::calls::CALL, envelope.clone(), left)
            .await;
        #[cfg(feature = "fault-injection")]
        let called = self.lose(called);
        match CallMiss::of(called, callee) {
            Ok(reply) => Ok(reply),
            Err(missed) => Box::pin(self.call_again(callee, policy, bound, envelope, missed)).await,
        }
    }

    /// After a try of `envelope` missed, try it on the instance `policy`
    /// picks next, as long as `missed` allows another try, the bound has
    /// not passed, and `instance_failovers` more tries are left. A refusal,
    /// or a send velo refused, allows one for any service; a call that may
    /// have run allows one only for an `idempotent` service. The call fails
    /// with its last refusal, unless a later try may have run, and a send
    /// that never left never replaces a refusal, as for offers.
    async fn call_again(
        &self,
        callee: &Callee,
        policy: Policy,
        bound: gdl::Deadline,
        envelope: bytes::Bytes,
        missed: CallMiss,
    ) -> Result<bytes::Bytes> {
        let mut missed = missed;
        for _ in 0..self.config.instance_failovers {
            if !missed.again(callee) {
                break;
            }
            let left = left(bound);
            if left.is_zero() {
                return Err(Error::deadline_exceeded());
            }
            // Nothing left to route to: the call fails with its last miss.
            let Ok(instance) = self.pick_call(callee, policy) else {
                break;
            };
            let called = self
                .fabric
                .call_bounded(instance, super::calls::CALL, envelope.clone(), left)
                .await;
            #[cfg(feature = "fault-injection")]
            let called = self.lose(called);
            match CallMiss::of(called, callee) {
                Ok(reply) => return Ok(reply),
                Err(then) => missed = missed.then(then),
            }
        }
        Err(missed.into_error())
    }

    /// [`Runtime::call`] through a selector: it chooses the instance within
    /// the call's `bound`, and what it chose to tell the service goes in the
    /// call's context. Its lease is held until the reply comes back, so that
    /// a selector that counts what it sent sees the call while it runs. A
    /// try that missed is tried again by the rules of
    /// [`Runtime::call_again`]: the selector chooses again, told which
    /// instances refused the call, and the declined choice's lease ends
    /// first.
    ///
    /// Boxed, and not `async`: a call's future holds only the box, so the
    /// selector's state does not grow every call's, a policy's too.
    fn call_selected<'a>(
        &'a self,
        callee: &'a Callee,
        selector: &'a dyn Erased,
        encoding: gdl_model::Encoding,
        cx: &'a Cx,
        bound: gdl::Deadline,
        payload: super::envelope::CallHead,
    ) -> futures::future::BoxFuture<'a, Result<bytes::Bytes>> {
        Box::pin(async move {
            let mut payload = payload;
            let mut declined = Vec::new();
            let mut missed: Option<CallMiss> = None;
            for _ in 0..=self.config.instance_failovers {
                let left = left(bound);
                if left.is_zero() {
                    return Err(Error::deadline_exceeded());
                }
                let chosen = self.routes.choose(
                    &callee.segment,
                    selector,
                    cx,
                    payload.plain(),
                    encoding,
                    &declined,
                );
                let (instance, picked) = match tokio::time::timeout(left, chosen).await {
                    Err(_) => return Err(Error::deadline_exceeded()),
                    Ok(Ok(chosen)) => chosen,
                    // Nothing left to route to: the call fails with its last
                    // miss, or, before any, with `Routing`.
                    Ok(Err(Unchosen::Unroutable(error))) => {
                        return Err(match missed {
                            Some(missed) => missed.into_error(),
                            None => unroutable(callee, &error),
                        });
                    }
                    Ok(Err(Unchosen::Failed(error))) => return Err(error),
                };
                let mut wire_cx = super::envelope::WireCx::bounded(cx, bound);
                wire_cx.add_meta(picked.meta);
                let lease = picked.lease;
                let (envelope, left) = self.call_envelope(callee.index, wire_cx, payload, bound)?;
                let called = self
                    .fabric
                    .call_bounded(instance, super::calls::CALL, envelope.clone(), left)
                    .await;
                #[cfg(feature = "fault-injection")]
                let called = self.lose(called);
                let then = match CallMiss::of(called, callee) {
                    Ok(reply) => return Ok(reply),
                    Err(then) => then,
                };
                drop(lease);
                if let CallMiss::Refused(_) = then {
                    declined.push(super::roster::selector_id(instance));
                }
                let now = match missed.take() {
                    Some(missed) => missed.then(then),
                    None => then,
                };
                if !now.again(callee) {
                    return Err(now.into_error());
                }
                missed = Some(now);
                // The next try's envelope carries the next choice's meta,
                // so the request is copied out of this one: on a miss
                // only.
                payload = super::envelope::CallHead::sent_in(&envelope)?;
            }
            Err(missed.map_or_else(
                || Error::routing("no try of the call was made"),
                CallMiss::into_error,
            ))
        })
    }

    /// The envelope of one call, `cx` its context, and the time left to its
    /// `bound`: refused once the bound has passed.
    fn call_envelope(
        &self,
        index: u16,
        cx: super::envelope::WireCx,
        payload: super::envelope::CallHead,
        bound: gdl::Deadline,
    ) -> Result<(bytes::Bytes, Duration)> {
        let envelope = super::envelope::Call {
            identity: self.identity,
            service: index,
            cx,
            payload,
        }
        .wire()?;
        let left = left(bound);
        if left.is_zero() {
            return Err(Error::deadline_exceeded());
        }
        Ok((envelope, left))
    }

    /// `called`, or, while `lose_call_replies` counts, an answer lost after
    /// its send.
    #[cfg(feature = "fault-injection")]
    fn lose(&self, called: super::fabric::Called) -> super::fabric::Called {
        use super::fabric::Called;
        let lost = self
            .config
            .faults
            .lose_call_replies
            .as_ref()
            .is_some_and(|left| {
                left.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            });
        match called {
            Called::Answered(_) if lost => Called::Unknown(Error::new(
                ErrorKind::Transport,
                "the reply was lost after the send",
            )),
            called => called,
        }
    }

    /// An instance of `callee`'s segment, as `policy` picks: `Routing` when
    /// the routing table has none.
    fn pick_call(&self, callee: &Callee, policy: Policy) -> Result<velo::InstanceId> {
        self.routes
            .pick(&callee.segment, Picker::Call, policy, &self.random)
            .map_err(|error| unroutable(callee, &error))
    }

    /// How this segment picks an instance for its calls to unary node
    /// `service`: `round_robin`, `random`, or a selector that `Runtime::new`
    /// resolved, which reads the call's request in `encoding`, its codec's.
    pub(crate) fn call_router(&self, service: &str, encoding: gdl_model::Encoding) -> Within {
        match self.layout.call_router(service) {
            RequestRouter::RoundRobin => Within::Policy(Policy::RoundRobin),
            RequestRouter::Random => Within::Policy(Policy::Random),
            RequestRouter::Selector(name) => match self.selectors.get(name) {
                Some(selector) => Within::Selector {
                    name: Arc::from(name.as_str()),
                    selector: Arc::clone(selector),
                    encoding,
                },
                // Resolved for every service this segment calls in another
                // segment; a call to its own copy is never routed.
                None => Within::Policy(Policy::RoundRobin),
            },
        }
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

/// The error of a call to `callee` with no instance to route to, as the
/// routing table said, `error`.
fn unroutable(callee: &Callee, error: &Error) -> Error {
    Error::routing(format!(
        "no instance of segment `{}` to call unary node `{}` on: {}",
        &callee.segment,
        &callee.service,
        error.message()
    ))
}

/// The time left until `bound`.
fn left(bound: gdl::Deadline) -> Duration {
    bound
        .instant()
        .saturating_duration_since(tokio::time::Instant::now())
}

/// Why one try of a call did not end it, as [`Runtime::call_again`] reads
/// it.
enum CallMiss {
    /// The instance answered that the service never ran.
    Refused(Error),
    /// velo refused the send before the transport took it.
    Unsent(Error),
    /// The send failed after the transport took it: the service may have
    /// run, an `OutcomeUnknown` error.
    Unknown(Error),
    /// The call's bound passed: never tried again.
    Expired(Error),
}

impl CallMiss {
    /// What one try came to: the reply, unless it was refused, or why not.
    /// A completed error is the reply's, and reaches the caller as it is.
    fn of(
        called: super::fabric::Called,
        callee: &Callee,
    ) -> std::result::Result<bytes::Bytes, Self> {
        use super::fabric::Called;
        match called {
            Called::Answered(reply) => match super::calls::refusal(&reply) {
                Some(error) => Err(Self::Refused(error)),
                None => Ok(reply),
            },
            Called::Unsent(error) => Err(Self::Unsent(error)),
            Called::Unknown(error) => Err(Self::Unknown(Error::new(
                ErrorKind::OutcomeUnknown,
                format!(
                    "the call to unary node `{}` may have run: {}",
                    &callee.service,
                    error.message()
                ),
            ))),
            Called::Expired(error) => Err(Self::Expired(error)),
        }
    }

    /// Whether the call may be tried again after this miss.
    fn again(&self, callee: &Callee) -> bool {
        match self {
            Self::Refused(_) | Self::Unsent(_) => true,
            Self::Unknown(_) => callee.idempotent,
            Self::Expired(_) => false,
        }
    }

    /// The miss a call that missed `self` and then `then` reports: the
    /// last, unless it never left and an instance had refused.
    fn then(self, then: CallMiss) -> CallMiss {
        match (self, then) {
            (refused @ Self::Refused(_), Self::Unsent(_)) => refused,
            (_, then) => then,
        }
    }

    fn into_error(self) -> Error {
        match self {
            Self::Refused(error)
            | Self::Unsent(error)
            | Self::Unknown(error)
            | Self::Expired(error) => error,
        }
    }
}

/// How a crossing chooses where a hand-off goes.
#[derive(Clone)]
pub(crate) enum Router {
    /// An instance of the next segment, from the roster's routing table.
    Roster(Within),
    /// A chooser's hand-off into another class: a pool by the class's
    /// pool policy, then an instance of it by the placement's router.
    #[cfg(feature = "hub")]
    Pools(Arc<super::pools::Crossing>),
}

impl Router {
    /// How the instance is chosen: in the roster, or within the chosen
    /// pool.
    fn within(&self) -> &Within {
        match self {
            Self::Roster(within) => within,
            #[cfg(feature = "hub")]
            Self::Pools(crossing) => crossing.within(),
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
/// chooser's offer to a pool, the lease on the pool's follow, by which the
/// chooser knows when it no longer holds an offer to it, with the pool
/// policy's lease on its choice.
///
/// Under the `hub` feature the pool's lease makes it, and so every
/// hand-off's [`Handed`], 16 bytes larger: 32 bytes against the
/// selector's lease's 16, computed from the fields (a boxed lease is two
/// words, an `Arc` one, the policy's thin lease one, each `Option`
/// niched), not measured. A hand-off that is not
/// a chooser's offer to a pool carries `None` there: making it and
/// dropping it take no atomic operation, only the empty check on drop.
#[derive(Default)]
#[allow(dead_code, reason = "held for their drop")]
pub(crate) struct Held {
    selector: Option<Lease>,
    #[cfg(feature = "hub")]
    pool: Option<super::pools::PoolLease>,
}

impl Held {
    /// A selector's lease, if it gave one.
    pub(crate) fn selector(selector: Option<Lease>) -> Self {
        #[cfg(feature = "hub")]
        let held = Self {
            selector,
            pool: None,
        };
        #[cfg(not(feature = "hub"))]
        let held = Self { selector };
        held
    }

    /// A chooser's offer to a pool: the selector's lease, if it gave one,
    /// and the lease on the pool's follow.
    #[cfg(feature = "hub")]
    pub(crate) fn pool(selector: Option<Lease>, pool: super::pools::PoolLease) -> Self {
        Self {
            selector,
            pool: Some(pool),
        }
    }

    /// The lease on the follow of the pool this offer went to, if it went
    /// to a pool.
    #[cfg(feature = "hub")]
    pub(crate) fn pool_lease(&self) -> Option<&super::pools::PoolLease> {
        self.pool.as_ref()
    }

    /// The pool this offer went to did not take it: end its leases, the
    /// lease on the pool's follow without waking a request that waits for
    /// room in the pool, since the miss freed none. Gives back the pool
    /// policy's lease on the choice, if it gave one.
    #[cfg(feature = "hub")]
    pub(crate) fn missed(self) -> Option<gdl::__runtime::ChoiceHeld> {
        self.pool.and_then(super::pools::PoolLease::declined)
    }

    /// The pool this offer went to accepted it: tell the pool policy's
    /// lease, if it gave one.
    #[cfg(feature = "hub")]
    #[inline]
    pub(crate) fn accepted(&mut self) {
        if let Some(pool) = &mut self.pool {
            pool.accepted();
        }
    }
}

/// Work an instance has taken on, for tests and diagnostics.
#[derive(Default)]
pub(crate) struct Served {
    pub(crate) forwards: AtomicUsize,
    pub(crate) runs: AtomicUsize,
    pub(crate) inputs: AtomicUsize,
    /// Forward hand-offs declined with no room (admission or egress
    /// refused, or the instance retired), or declined for the pool by its
    /// own policy: what a load report counts as refusals. An instance's
    /// own decline is not counted. Only a refusal touches it.
    pub(crate) declined: AtomicUsize,
    /// Offers declined, by reason, in the order of [`DECLINES`]: every
    /// decline, whether or not a load report counts it, so that a decline
    /// over a pool's budget shows apart from a full pool's.
    pub(crate) declines: [AtomicU64; DECLINES.len()],
    /// Forward hand-offs that arrived, refused ones too: what a test
    /// counts to see where a request was offered.
    #[cfg(feature = "fault-injection")]
    pub(crate) offered: AtomicUsize,
}

/// Every reason an offer is declined for, with its `reason` label, in the
/// order of [`Served::declines`].
pub(crate) const DECLINES: [(Decline, &str); 5] = [
    (Decline::Full, "full"),
    (Decline::Gone, "gone"),
    (Decline::Instance, "instance"),
    (Decline::NotPresent, "not_present"),
    (Decline::Budget, "budget"),
];

impl Served {
    /// Count an offer declined for `decline`.
    pub(crate) fn decline(&self, decline: Decline) {
        let at = DECLINES
            .iter()
            .position(|(reason, _)| *reason == decline)
            .unwrap_or(0);
        self.declines[at].fetch_add(1, Ordering::Relaxed);
    }

    /// The offers declined for `decline`.
    pub(crate) fn declined_as(&self, decline: Decline) -> u64 {
        DECLINES
            .iter()
            .position(|(reason, _)| *reason == decline)
            .map_or(0, |at| self.declines[at].load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `S => D` in two segments, with an edge codec on `S -> D` routed by
    /// `router`, and `G` in a third.
    fn routed(router: RequestRouter) -> Layout {
        coded_with(true, Some(router))
    }

    /// The sender keeps a compressed head's plain bytes only where routing
    /// reads the head again: a selector, `affinity` among them, or a pool
    /// policy. `round_robin` and `random` read nothing.
    #[test]
    fn the_plain_head_is_kept_only_where_routing_reads_it() {
        let keeps = |layout: &Layout, pooled: bool| {
            let heads = compressed_heads(layout, 1 << 20, |_, _| pooled);
            heads[&(0, 1)].keeps_plain
        };
        assert!(!keeps(&routed(RequestRouter::RoundRobin), false));
        assert!(!keeps(&routed(RequestRouter::Random), false));
        assert!(keeps(
            &routed(RequestRouter::Selector("affinity".into())),
            false
        ));
        assert!(keeps(
            &routed(RequestRouter::Selector("by_head".into())),
            false
        ));
        assert!(
            keeps(&routed(RequestRouter::RoundRobin), true),
            "a pool policy"
        );
    }

    #[cfg(feature = "zstd")]
    async fn local_offer_bounds(check_over: bool) {
        use crate::dist::envelope::{VERSION, WireCx};
        use crate::dist::fabric::bind_node;
        use crate::wire::Codec;

        const LIMIT: usize = 16 * 1024;
        let node = bind_node(([127, 0, 0, 1], 0).into()).await.expect("node");
        let rt = Runtime::new(
            Fabric::new(node, None, None, Duration::from_secs(30)),
            coded(true),
            "front".into(),
            &Roster::new(),
            RuntimeConfig {
                decompressed_max: LIMIT,
                ..RuntimeConfig::default()
            },
            None,
        )
        .expect("runtime");
        let called = Arc::new(AtomicUsize::new(0));
        let mut entries = Entries::default();
        entries.add_forward(0, 1, {
            let called = Arc::clone(&called);
            Arc::new(move |forward| {
                Codec::<String>::new(Default::default())
                    .decode(&forward.payload)
                    .expect("the local entry receives plain MessagePack");
                called.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(None) })
            })
        });
        let entries = Arc::new(entries);
        rt.set_entries(&entries);
        let offered = |text_len| {
            let mut forward = Forward {
                version: VERSION,
                identity: rt.identity,
                hop: 0,
                pos: 1,
                cx: WireCx::of(&Cx::new()),
                reply: ReplyTo::test_value(7),
                payload: Codec::<String>::new(Default::default())
                    .encode_head(&"x".repeat(text_len))
                    .expect("head"),
            };
            forward
                .wire(rt.head_compression(0, 1))
                .expect("remote offer");
            assert!(forward.payload.is_empty(), "the plain head was released");
            forward
        };
        let accepted = rt.enter(offered(LIMIT - 3)).expect("frame at the limit");
        tokio::time::timeout(Duration::from_secs(1), accepted)
            .await
            .expect("bounded local entry")
            .unwrap_or_else(|refused| panic!("{}", refused.error));
        assert_eq!(
            called.load(Ordering::SeqCst),
            1,
            "the control reaches the entry"
        );
        if !check_over {
            return;
        }
        let error = match rt.enter(offered(LIMIT * 2)) {
            Err(error) => error,
            Ok(_) => panic!(
                "a 32771-byte frame bypassed decompressed_max={LIMIT}; the local entry ran {} times",
                called.load(Ordering::SeqCst)
            ),
        };
        assert_eq!(error.kind(), ErrorKind::Codec);
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn a_local_offer_accepts_a_frame_at_the_decompressed_limit() {
        local_offer_bounds(false).await;
    }

    /// A local re-offer must enforce the same frame limit as a remote arrival.
    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn a_local_offer_enforces_the_decompressed_limit() {
        local_offer_bounds(true).await;
    }

    /// `S => D` in two segments, with an edge codec on `D -> S` when
    /// `coded`, and a third segment, `idle`, that holds `G`, a backward node
    /// on no compressed edge.
    fn coded(coded: bool) -> Layout {
        coded_with(coded, None)
    }

    fn coded_with(coded: bool, router: Option<RequestRouter>) -> Layout {
        use gdl_model::{
            CodecEntry, EdgeCodec, HopInfo, Manifest, NodeInfo, Placement, Role, TypeInfo,
        };
        let node = |name: &str, role| NodeInfo {
            name: name.into(),
            role,
            binding: (role != Role::Source).then(|| name.to_lowercase()),
            calls: Vec::new(),
            unary: None,
        };
        let ty = |name: &str| TypeInfo::new(name, true, Default::default());
        let manifest = Manifest {
            name: "coded".into(),
            schema: 1,
            nodes: vec![
                node("S", Role::Source),
                node("D", Role::Sink),
                node("G", Role::Backward),
            ],
            hops: vec![HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec!["G".into()],
                forward_types: vec![ty("Ask")],
                backward_types: vec![ty("Token"), ty("Text")],
            }],
            properties: Vec::new(),
        };
        let mut placement = Placement::new()
            .segment("front", ["S"])
            .segment("engine", ["D"])
            .segment("idle", ["G"]);
        if coded {
            placement = placement.codec("S -> D", CodecEntry::every(EdgeCodec::MsgpackZstd));
        }
        if let Some(router) = router {
            placement = placement.route("D", router);
        }
        gdl_model::layout(&manifest, &placement).expect("a layout")
    }

    /// A segment at either end of a compressed edge needs the `zstd`
    /// feature; one at neither end, and every segment of a layout without
    /// an entry, does not.
    #[test]
    fn a_segment_on_a_compressed_edge_needs_the_zstd_feature() {
        let layout = coded(true);
        for segment in ["front", "engine"] {
            let refused = compresses(&layout, segment, false).expect_err(segment);
            assert_eq!(refused.kind(), ErrorKind::Placement);
            assert!(refused.to_string().contains("`zstd` feature"), "{refused}");
            compresses(&layout, segment, true).expect(segment);
        }
        compresses(&layout, "idle", false).expect("on no compressed edge");
        compresses(&coded(false), "front", false).expect("no entry");
    }

    #[test]
    fn decompressed_max_is_256_mib_by_default_and_may_be_set() {
        let defaults: RuntimeConfig = toml::from_str("").expect("defaults");
        assert_eq!(defaults.decompressed_max, 256 << 20);
        let set: RuntimeConfig = toml::from_str("decompressed_max = 1024\n").expect("set");
        assert_eq!(set.decompressed_max, 1024);
    }

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
    fn pool_failovers_are_two_by_default_and_may_be_none() {
        let defaults: RuntimeConfig = toml::from_str("").expect("defaults");
        assert_eq!(defaults.pool_failovers, 2);
        let none: RuntimeConfig = toml::from_str("pool_failovers = 0\n").expect("none");
        assert_eq!(none.pool_failovers, 0);
    }

    #[test]
    fn instance_failovers_are_two_by_default_and_may_be_none() {
        let defaults: RuntimeConfig = toml::from_str("").expect("defaults");
        assert_eq!(defaults.instance_failovers, 2);
        let none: RuntimeConfig = toml::from_str("instance_failovers = 0\n").expect("none");
        assert_eq!(none.instance_failovers, 0);
        assert_eq!(none.pool_failovers, 2, "the two bounds are apart");
    }

    /// A chooser's damping windows, the periods a summary stays fresh, and
    /// `home_first`'s thresholds are settings, each with the value it had
    /// when it was fixed in the binary unless it is set.
    #[test]
    fn a_choosers_pool_settings_keep_their_defaults_unless_set() {
        for defaults in [
            RuntimeConfig::default(),
            toml::from_str::<RuntimeConfig>("").expect("defaults"),
        ] {
            assert_eq!(defaults.pool_damping_first, Duration::from_secs(1));
            assert_eq!(defaults.pool_damping_max, Duration::from_secs(30));
            assert_eq!(defaults.pool_requeue_backoff, Duration::from_millis(50));
            assert_eq!(defaults.pool_fresh_periods, 3);
            assert_eq!(defaults.home_first.saturated_percent, 100);
            assert_eq!(defaults.home_first.spill_percent, 80);
        }
        let set: RuntimeConfig = toml::from_str(
            "pool_damping_first_ms = 2000\npool_damping_max_ms = 5000\npool_fresh_periods = 5\n\
             home_first_saturated_percent = 90\nhome_first_spill_percent = 0\n\
             pool_requeue_backoff_ms = 20\n",
        )
        .expect("set");
        assert_eq!(set.pool_damping_first, Duration::from_secs(2));
        assert_eq!(set.pool_damping_max, Duration::from_secs(5));
        assert_eq!(set.pool_requeue_backoff, Duration::from_millis(20));
        assert_eq!(set.pool_fresh_periods, 5);
        assert_eq!(set.home_first.saturated_percent, 90);
        assert_eq!(set.home_first.spill_percent, 0);
    }

    /// A chooser setting that breaks what the chooser relies on is refused,
    /// naming its key.
    #[test]
    fn a_choosers_pool_setting_out_of_its_bounds_is_refused_by_key() {
        for (bad, key) in [
            ("pool_damping_first_ms = 0\n", "pool_damping_first_ms"),
            (
                "pool_damping_first_ms = 3000\npool_damping_max_ms = 2000\n",
                "pool_damping_max_ms",
            ),
            // Against the first window's default, 1 s.
            ("pool_damping_max_ms = 999\n", "pool_damping_max_ms"),
            ("pool_damping_max_ms = 3600001\n", "pool_damping_max_ms"),
            ("pool_requeue_backoff_ms = 0\n", "pool_requeue_backoff_ms"),
            (
                "pool_requeue_backoff_ms = 3600001\n",
                "pool_requeue_backoff_ms",
            ),
            ("pool_fresh_periods = 0\n", "pool_fresh_periods"),
            ("pool_fresh_periods = 101\n", "pool_fresh_periods"),
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
            "pool_damping_first_ms = 3600000\npool_damping_max_ms = 3600000\npool_fresh_periods = 100\n\
             home_first_saturated_percent = 1\nhome_first_spill_percent = 1\n\
             pool_requeue_backoff_ms = 3600000\n",
        )
        .expect("the bounds");
        assert_eq!(edge.pool_damping_max, Duration::from_secs(3600));
        assert_eq!(edge.pool_requeue_backoff, Duration::from_secs(3600));
        let least: RuntimeConfig =
            toml::from_str("pool_requeue_backoff_ms = 1\n").expect("the least backoff");
        assert_eq!(least.pool_requeue_backoff, Duration::from_millis(1));
    }

    #[test]
    fn runtime_config_rejects_an_unknown_key() {
        let Err(error) = toml::from_str::<RuntimeConfig>("admision_limit = 8\n") else {
            panic!("a misspelled key parsed");
        };
        assert!(error.to_string().contains("admision_limit"), "{error}");
    }
}
