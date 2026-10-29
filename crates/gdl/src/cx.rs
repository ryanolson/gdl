use std::any::{Any, TypeId};
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::sync::atomic::AtomicBool;

use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::error::{Error, Result};
use crate::meter::Visit;

/// The per-request context every node receives.
///
/// Cloning is cheap. [`Cx::child`] makes the context for one attempt: it
/// shares the identity, metadata, deadline, and extensions, and gets its
/// own stop and cancel scope below the parent's.
#[derive(Clone)]
pub struct Cx {
    id: RequestId,
    meta: Arc<BTreeMap<String, String>>,
    deadline: Option<Deadline>,
    scope: Scope,
    ext: Extensions,
    /// The request's visit to this segment, when the runtime records one:
    /// the hops a node calls under this context end its request path and
    /// stamp its response. Like the extensions, it stays in the segment.
    visit: Option<Arc<Visit>>,
}

impl Cx {
    /// A fresh root context with a new request id, no metadata, and no deadline.
    pub fn new() -> Self {
        Self {
            id: RequestId::mint(),
            meta: Arc::default(),
            deadline: None,
            scope: Scope::root(),
            ext: Extensions::default(),
            visit: None,
        }
    }

    /// Set the deadline.
    pub fn with_deadline(mut self, deadline: Deadline) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Set a deadline `timeout` from now.
    pub fn with_timeout(self, timeout: Duration) -> Self {
        self.with_deadline(Deadline::after(timeout))
    }

    /// The request id. It identifies the request in logs and traces only; the
    /// runtime never uses it to route a response.
    pub fn id(&self) -> RequestId {
        self.id
    }

    /// Metadata that travels with the request across segments.
    pub fn meta(&self) -> &BTreeMap<String, String> {
        &self.meta
    }

    /// Add or replace one metadata entry. Only this context and the contexts
    /// derived from it afterwards see the change.
    pub fn insert_meta(&mut self, key: impl Into<String>, value: impl Into<String>) {
        Arc::make_mut(&mut self.meta).insert(key.into(), value.into());
    }

    /// The deadline, if the request has one.
    pub fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }

    /// A value stored under its type. Extensions stay in the segment that set
    /// them; a node in another segment never sees them.
    pub fn ext<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.ext.get::<T>()
    }

    /// Store a value under its type, replacing any previous value of that type.
    pub fn insert_ext<T: Any + Send + Sync>(&mut self, value: T) {
        self.ext.insert(value);
    }

    /// The context for one attempt below this one. Cancelling or stopping this
    /// context reaches the child; cancelling the child does not reach this one.
    pub fn child(&self) -> Self {
        Self {
            id: self.id,
            meta: Arc::clone(&self.meta),
            deadline: self.deadline,
            scope: self.scope.child(),
            ext: self.ext.clone(),
            visit: self.visit.clone(),
        }
    }

    /// Cancel this context and every context below it.
    pub fn cancel(&self) {
        self.scope.cancel.cancel();
    }

    /// Ask this context and every context below it to stop gracefully:
    /// finish buffered output, then end.
    pub fn request_stop(&self) {
        self.scope.stop.stop();
    }

    /// Whether this context was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.scope.cancel.is_cancelled()
    }

    /// Resolves when this context is cancelled.
    pub async fn cancelled(&self) {
        self.scope.cancel.cancelled().await;
    }

    /// Whether this context should stop producing: a stop was requested or it
    /// was cancelled. A cancel is a stop that also discards.
    pub fn is_stopping(&self) -> bool {
        self.scope.stop.is_stopped() || self.scope.cancel.is_cancelled()
    }

    /// Resolves when a stop is requested or the context is cancelled.
    pub async fn stopping(&self) {
        tokio::select! {
            () = self.scope.stop.stopped() => {}
            () = self.scope.cancel.cancelled() => {}
        }
    }

    /// `Ok` while the request may continue; the reason otherwise.
    ///
    /// A passed deadline is reported ahead of a cancel: when the deadline
    /// ends a stream, dropping that stream cancels the attempt too, and the
    /// deadline is the cause worth reporting.
    pub fn check(&self) -> Result<()> {
        if self.deadline.is_some_and(|d| d.is_expired()) {
            return Err(Error::deadline_exceeded());
        }
        if self.is_cancelled() {
            return Err(Error::cancelled());
        }
        Ok(())
    }

    /// Resolves with the reason the request must end: deadline or cancel,
    /// preferring the deadline when both have happened (see [`Cx::check`]).
    pub async fn done(&self) -> Error {
        match self.deadline {
            Some(deadline) => tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline.instant()) => Error::deadline_exceeded(),
                // The timer fires on a whole-millisecond tick; a deadline
                // that has passed is reported even if its timer has not.
                () = self.scope.cancel.cancelled() => if deadline.is_expired() {
                    Error::deadline_exceeded()
                } else {
                    Error::cancelled()
                },
            },
            None => {
                self.scope.cancel.cancelled().await;
                Error::cancelled()
            }
        }
    }

    /// The cancel token of this context's scope.
    #[doc(hidden)]
    pub fn cancel_token(&self) -> CancellationToken {
        self.scope.cancel.clone()
    }

    /// A context for a request that arrived from another segment: the same
    /// id, metadata, and deadline, with a fresh root scope that the runtime
    /// links to the stream that carries the response back.
    #[doc(hidden)]
    pub fn arrived(
        id: RequestId,
        meta: BTreeMap<String, String>,
        deadline: Option<Deadline>,
    ) -> Self {
        Self {
            id,
            meta: Arc::new(meta),
            deadline,
            scope: Scope::root(),
            ext: Extensions::default(),
            visit: None,
        }
    }

    /// This context with `visit` as the visit the work under it belongs
    /// to; `None` for work that must not touch its parent's.
    #[doc(hidden)]
    pub fn with_visit(mut self, visit: Option<Arc<Visit>>) -> Self {
        self.visit = visit;
        self
    }

    /// The visit the work under this context belongs to, if recorded.
    #[doc(hidden)]
    pub fn visit(&self) -> Option<&Arc<Visit>> {
        self.visit.as_ref()
    }

    /// A guard that cancels this context when dropped. The runtime holds one
    /// inside every attempt's response stream, so dropping the stream cancels
    /// the attempt.
    #[doc(hidden)]
    pub fn cancel_on_drop(&self) -> DropGuard {
        self.scope.cancel.clone().drop_guard()
    }
}

impl Default for Cx {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Cx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cx")
            .field("id", &self.id)
            .field("meta", &self.meta)
            .field("deadline", &self.deadline)
            .field("stopping", &self.is_stopping())
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Stop and cancel are separate trees. Stop must not imply cancel (a stopped
/// producer still delivers its buffered output), and a cancel must reach
/// children whose stop has its own parent, so each signal gets its own tree
/// and readers check both.
#[derive(Clone)]
struct Scope {
    stop: Stop,
    cancel: CancellationToken,
}

impl Scope {
    fn root() -> Self {
        Self {
            stop: Stop::root(),
            cancel: CancellationToken::new(),
        }
    }

    fn child(&self) -> Self {
        Self {
            stop: self.stop.child(),
            cancel: self.cancel.child_token(),
        }
    }
}

/// One scope of the stop tree. A child points at its parent and a parent
/// knows nothing of its children, so making, cloning, and dropping a scope
/// takes no lock, where a cancel token takes one for each. A context checks
/// its own flag and each of its parents'. A stop is rare, so every waiter of
/// one request waits on one `Notify` at the root, and a stop wakes them all
/// to check again.
#[derive(Clone)]
struct Stop(Arc<StopNode>);

struct StopNode {
    stopped: AtomicBool,
    up: Up,
}

enum Up {
    Root(Notify),
    Parent(Arc<StopNode>),
    /// Only while the node is dropped, which nothing reads.
    Gone,
}

impl Drop for StopNode {
    fn drop(&mut self) {
        // A context replaced by its child, over and over, holds a long chain
        // of parents that only it keeps. Dropped by recursion, the chain
        // would overflow the stack; unlink it one node at a time instead.
        let mut up = std::mem::replace(&mut self.up, Up::Gone);
        while let Up::Parent(parent) = up {
            let Ok(mut parent) = Arc::try_unwrap(parent) else {
                return;
            };
            up = std::mem::replace(&mut parent.up, Up::Gone);
        }
    }
}

impl Stop {
    fn root() -> Self {
        Self(Arc::new(StopNode {
            stopped: AtomicBool::new(false),
            up: Up::Root(Notify::new()),
        }))
    }

    fn child(&self) -> Self {
        Self(Arc::new(StopNode {
            stopped: AtomicBool::new(false),
            up: Up::Parent(Arc::clone(&self.0)),
        }))
    }

    fn stop(&self) {
        // Set before the wake: a waiter that misses the wake sees the flag.
        if !self.0.stopped.swap(true, Ordering::SeqCst) {
            self.notify().notify_waiters();
        }
    }

    fn is_stopped(&self) -> bool {
        let mut node = &*self.0;
        loop {
            if node.stopped.load(Ordering::SeqCst) {
                return true;
            }
            match &node.up {
                Up::Parent(parent) => node = parent,
                Up::Root(_) | Up::Gone => return false,
            }
        }
    }

    async fn stopped(&self) {
        let notify = self.notify();
        loop {
            // Made before the check: `notify_waiters` wakes a `Notified`
            // from the moment it exists, so a stop between the check and the
            // await still wakes it.
            let notified = notify.notified();
            if self.is_stopped() {
                return;
            }
            notified.await;
        }
    }

    fn notify(&self) -> &Notify {
        let mut node = &*self.0;
        loop {
            match &node.up {
                Up::Root(notify) => return notify,
                Up::Parent(parent) => node = parent,
                Up::Gone => unreachable!("a node being dropped is never read"),
            }
        }
    }
}

/// A request identifier for logs and traces: an RFC 9562 UUID, version 7.
///
/// Its first 48 bits are the Unix time in milliseconds when it was minted,
/// so ids sort by time, as integers and as text. The 12 bits after the
/// version are a counter, so the ids one process mints sort in the order
/// they were minted, even within one millisecond (the RFC's method 1). The
/// last 62 bits are random, so ids from different processes rarely collide
/// (see [`RequestId::mint`] for a fork).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(u128);

impl RequestId {
    /// A new identifier: unique within the process, later than every id the
    /// process minted before it, and, with high probability, unique across
    /// processes. A child forked without exec can repeat its parent's ids.
    /// It is not a secret.
    ///
    /// Minting takes no lock and allocates nothing: one read of the wall
    /// clock, one or two atomic operations on the process's stamp, and a
    /// step of a per-thread random generator.
    pub fn mint() -> Self {
        static STAMPS: Stamps = Stamps::new();
        let stamp = STAMPS.next(unix_millis());
        let millis = (stamp >> COUNTER_BITS) & ((1 << 48) - 1);
        let counter = stamp & ((1 << COUNTER_BITS) - 1);
        let high = millis << 16 | 0x7 << 12 | counter;
        let low = 0b10 << 62 | random() >> 2;
        Self(u128::from(high) << 64 | u128::from(low))
    }

    /// Rebuild an identifier from its integer form.
    pub fn from_u128(raw: u128) -> Self {
        Self(raw)
    }

    /// The integer form.
    pub fn as_u128(self) -> u128 {
        self.0
    }
}

/// The hyphenated UUID form, `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, in
/// lowercase.
impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.0;
        write!(
            f,
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            id >> 96,
            (id >> 80) & 0xffff,
            (id >> 64) & 0xffff,
            (id >> 48) & 0xffff,
            id & 0xffff_ffff_ffff,
        )
    }
}

/// The same form as [`Display`](fmt::Display), so logs show one form.
impl fmt::Debug for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RequestId")
            .field(&format_args!("{self}"))
            .finish()
    }
}

/// Bits of a stamp below the milliseconds: the counter in the UUID's
/// `rand_a` field.
const COUNTER_BITS: u32 = 12;

/// The time and counter of the last id a process minted, as
/// `millis << COUNTER_BITS | counter`. Each mint takes a stamp above every
/// earlier one: the clock's when it has moved on, else the last plus one.
/// A counter that fills its 12 bits carries into the milliseconds, and a
/// clock that steps back is ignored until it passes the last stamp, so the
/// order never breaks; the stamp runs ahead of the clock meanwhile, which
/// the RFC allows.
///
/// The common case is one `fetch_add` on the shared stamp, as the counter
/// it replaced was. Only when the clock has moved past the stamp, about
/// once a millisecond, does a mint also raise it. A read before the add
/// would cost a second transfer of the contended line, and a
/// compare-and-swap loop would retry under contention.
///
/// The stamp is alone on its cache line. The linker can place the pointer
/// that the clock read loads next to it, and each mint's add then takes
/// that line from the threads about to read the clock: eight threads
/// minting at once pay 46 ns per id instead of 27 (`benches/mint.rs`).
#[repr(align(128))]
struct Stamps(AtomicU64);

impl Stamps {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// The next stamp, at `now_millis` or later. Relaxed is enough: the
    /// stamps form one modification order, so a mint that happens before
    /// another gets the lower stamp.
    ///
    /// A taken value is always below the stamp. When the clock has moved
    /// past it, the mint that raises the stamp from at most `floor` to
    /// `floor + 1` takes `floor`, which nobody can have taken; a mint that
    /// finds it already raised takes the next value as usual. The value the
    /// first add returned is then skipped, which costs nothing.
    fn next(&self, now_millis: u64) -> u64 {
        let floor = now_millis << COUNTER_BITS;
        let taken = self.0.fetch_add(1, Ordering::Relaxed);
        if taken >= floor {
            return taken;
        }
        if self.0.fetch_max(floor + 1, Ordering::Relaxed) <= floor {
            return floor;
        }
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

/// Milliseconds since the Unix epoch on the kernel's coarse wall clock.
/// It is read without the hardware counter, in about 6 ns against 23 to
/// 30 ns for the precise clock on a Cortex-X925, and lags the precise
/// clock by at most one kernel tick (1 to 10 ms), which an id that orders
/// requests in logs can spare. Elsewhere, the precise clock.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn unix_millis() -> u64 {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::RealtimeCoarse);
    let secs = u64::try_from(now.tv_sec).unwrap_or(0);
    let millis = u64::try_from(now.tv_nsec).unwrap_or(0) / 1_000_000;
    secs.saturating_mul(1_000).saturating_add(millis)
}

/// Milliseconds since the Unix epoch on the wall clock.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// 64 random bits from a per-thread SplitMix64 generator, seeded from the
/// thread's random hash keys on first use. A thread-local pool needs no
/// lock, and a `const` one no allocation or destructor. The bits need not
/// be secret, only distinct across processes; the admission ledger also
/// shards roots by them.
fn random() -> u64 {
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }
    STATE.with(|state| {
        let mut seed = state.get();
        if seed == 0 {
            seed = RandomState::new().hash_one(0_u8);
        }
        let seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        state.set(seed);
        let mut z = seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    })
}

/// When a request must end, on both the local monotonic clock and the wall
/// clock. The wall-clock form is what crosses segments; sending remaining
/// time instead would let every delayed hop restart the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline {
    at: Instant,
    unix_nanos: u64,
}

impl Deadline {
    /// A deadline `timeout` from now.
    pub fn after(timeout: Duration) -> Self {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .saturating_add(timeout);
        Self {
            at: Instant::now() + timeout,
            unix_nanos: u64::try_from(wall.as_nanos()).unwrap_or(u64::MAX),
        }
    }

    /// The deadline on the local monotonic clock.
    pub fn instant(self) -> Instant {
        self.at
    }

    /// The deadline as wall-clock nanoseconds since the Unix epoch.
    pub fn unix_nanos(self) -> u64 {
        self.unix_nanos
    }

    /// Whether the deadline has passed on the local monotonic clock.
    pub fn is_expired(self) -> bool {
        Instant::now() >= self.at
    }

    /// Convert a deadline received from another segment, or `None` if it has
    /// already passed.
    ///
    /// The wall-clock deadline crosses segments unchanged; each receiver maps
    /// it onto its own monotonic clock as `now + (deadline - wall_now - skew)`.
    /// With segment clocks within `skew` of each other, no receiver runs past
    /// the source's deadline; it may stop up to `2 * skew` early. A deadline
    /// within `skew` of now counts as passed, so a late delivery cannot start
    /// work the source has already given up on.
    ///
    /// The monotonic clock is read before the wall clock (`wall_now` reads
    /// it): a delay between the two reads then shortens the budget instead
    /// of extending it.
    #[doc(hidden)]
    pub fn from_wall(
        unix_nanos: u64,
        skew: Duration,
        wall_now: impl FnOnce() -> u64,
    ) -> Option<Self> {
        let mono_now = Instant::now();
        Self::from_wall_at(unix_nanos, skew, wall_now(), mono_now)
    }

    fn from_wall_at(
        unix_nanos: u64,
        skew: Duration,
        wall_now: u64,
        mono_now: Instant,
    ) -> Option<Self> {
        let skew = u64::try_from(skew.as_nanos()).unwrap_or(u64::MAX);
        let remaining = unix_nanos.saturating_sub(wall_now).saturating_sub(skew);
        (remaining > 0).then(|| Self {
            at: mono_now + Duration::from_nanos(remaining),
            unix_nanos,
        })
    }
}

/// Nanoseconds since the Unix epoch on this machine's wall clock.
pub fn wall_now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_nanos()).ok())
        .unwrap_or(0)
}

/// Values stored by type, local to one segment. Copy on write, so a child
/// context can add a value without its parent seeing it.
#[derive(Clone, Default)]
struct Extensions(Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>);

impl Extensions {
    fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        Arc::make_mut(&mut self.0).insert(TypeId::of::<T>(), Arc::new(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_and_cancel_are_separate_signals() {
        let cx = Cx::new();
        cx.request_stop();
        assert!(cx.is_stopping());
        assert!(!cx.is_cancelled());
        assert!(cx.check().is_ok());

        let cx = Cx::new();
        cx.cancel();
        assert!(cx.is_stopping(), "a cancel is also a stop");
        assert!(cx.is_cancelled());
    }

    #[test]
    fn signals_flow_from_parent_to_child_only() {
        let parent = Cx::new();
        let child = parent.child();
        child.cancel();
        assert!(!parent.is_cancelled());

        let sibling = parent.child();
        parent.request_stop();
        assert!(sibling.is_stopping());
        parent.cancel();
        assert!(sibling.is_cancelled());
    }

    #[test]
    fn a_stop_reaches_down_and_never_up_or_across() {
        let root = Cx::new();
        let parent = root.child();
        let child = parent.child();
        let sibling = root.child();
        child.request_stop();
        assert!(child.is_stopping());
        assert!(!parent.is_stopping() && !root.is_stopping());

        root.request_stop();
        assert!(parent.is_stopping() && sibling.is_stopping());
        let later = sibling.child().child();
        assert!(later.is_stopping(), "a context made after the stop");
        assert!(!later.is_cancelled());
    }

    #[tokio::test]
    async fn a_stop_wakes_the_waiters_below_it_only() {
        let root = Cx::new();
        let parent = root.child();
        let below = parent.child().child();
        let sibling = root.child();
        let waiting = tokio::spawn({
            let below = below.clone();
            async move { below.stopping().await }
        });
        let mut aside = std::pin::pin!(sibling.stopping());
        // Both wait before any stop.
        tokio::task::yield_now().await;
        assert!(futures::poll!(aside.as_mut()).is_pending());
        assert!(!waiting.is_finished());

        parent.request_stop();
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("woken by the stop above it")
            .expect("the waiter");
        assert!(futures::poll!(aside.as_mut()).is_pending());
        root.request_stop();
        tokio::time::timeout(Duration::from_secs(5), aside)
            .await
            .expect("woken by the root's stop");
    }

    /// A waiter makes its wake before it checks the flags, and a stop sets
    /// its flag before it wakes: whichever runs first, no stop is lost. A
    /// barrier starts the stop and the wait together, so they overlap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stop_racing_a_new_waiter_is_never_lost() {
        for _ in 0..2_000 {
            let parent = Cx::new().child();
            let below = parent.child().child();
            let start = Arc::new(std::sync::Barrier::new(2));
            let stopper = std::thread::spawn({
                let start = Arc::clone(&start);
                move || {
                    start.wait();
                    parent.request_stop();
                }
            });
            let waiter = tokio::spawn(async move {
                start.wait();
                below.stopping().await;
            });
            tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("the stop was lost")
                .expect("the waiter");
            stopper.join().expect("the stopper");
        }
    }

    #[test]
    fn children_share_identity_but_not_later_metadata() {
        let mut parent = Cx::new();
        parent.insert_meta("k", "1");
        parent.insert_ext(5_u32);
        let mut child = parent.child();
        assert_eq!(child.id(), parent.id());
        assert_eq!(child.meta().get("k").map(String::as_str), Some("1"));
        assert_eq!(child.ext::<u32>(), Some(&5));

        child.insert_meta("k", "2");
        child.insert_ext(6_u32);
        assert_eq!(parent.meta().get("k").map(String::as_str), Some("1"));
        assert_eq!(parent.ext::<u32>(), Some(&5));
    }

    #[test]
    fn request_ids_are_distinct() {
        let ids: std::collections::HashSet<_> = (0..1000).map(|_| RequestId::mint()).collect();
        assert_eq!(ids.len(), 1000);
    }

    /// The precise wall clock, independent of the clock ids are minted by.
    fn unix_millis() -> u64 {
        wall_now_nanos() / 1_000_000
    }

    /// How far the mint's coarse clock can lag the precise one: one kernel
    /// tick, plus a millisecond for rounding each down.
    fn coarse_lag_millis() -> u64 {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let tick = {
            let res = rustix::time::clock_getres(rustix::time::ClockId::RealtimeCoarse);
            u64::try_from(res.tv_sec).expect("seconds") * 1_000
                + u64::try_from(res.tv_nsec)
                    .expect("nanos")
                    .div_ceil(1_000_000)
        };
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let tick = 0;
        tick + 1
    }

    /// The text form parses as an RFC 9562 UUID, version 7, whose time is
    /// the time it was minted. The parser is the `uuid` crate's, not ours.
    #[test]
    fn a_request_id_is_a_uuid_v7_stamped_with_its_mint_time() {
        let before = unix_millis();
        let id = RequestId::mint();
        let after = unix_millis();

        let text = id.to_string();
        let uuid = uuid::Uuid::parse_str(&text).expect("a UUID");
        assert_eq!(uuid.as_u128(), id.as_u128());
        assert_eq!(uuid.hyphenated().to_string(), text, "the canonical form");
        assert_eq!(uuid.get_version(), Some(uuid::Version::SortRand));
        assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);

        let (secs, nanos) = uuid.get_timestamp().expect("a v7 time").to_unix();
        let stamped = secs * 1_000 + u64::from(nanos) / 1_000_000;
        let lag = coarse_lag_millis();
        assert!(
            stamped + lag >= before,
            "{stamped} is more than {lag} ms before the mint at {before}"
        );
        // Another test minting many ids at once may carry the shared counter
        // a few milliseconds ahead of the clock; a second is far more.
        assert!(stamped <= after + 1_000, "{stamped} is after {after}");
    }

    /// Ids minted one after another sort in that order, as integers and as
    /// text. 10,000 ids are more than one millisecond's counter can hold,
    /// so the counter carries into the time at least once here.
    #[test]
    fn ids_minted_in_order_sort_in_that_order() {
        let mut last = RequestId::mint();
        for _ in 0..10_000 {
            let next = RequestId::mint();
            assert!(next > last, "{next} after {last}");
            assert!(next.to_string() > last.to_string());
            last = next;
        }
    }

    /// The order holds across threads: an id minted after another thread's
    /// mint is seen sorts after it.
    #[test]
    fn ids_minted_in_order_on_other_threads_sort_in_that_order() {
        let first = RequestId::mint();
        let second = std::thread::spawn(RequestId::mint).join().expect("minted");
        let third = RequestId::mint();
        assert!(first < second && second < third, "{first} {second} {third}");
    }

    #[test]
    fn the_counter_carries_into_the_next_millisecond() {
        let stamps = Stamps::new();
        let ms = 1_000;
        let first = stamps.next(ms);
        assert_eq!(first, ms << COUNTER_BITS);
        let mut last = first;
        for _ in 0..(1 << COUNTER_BITS) {
            let next = stamps.next(ms);
            assert_eq!(next, last + 1);
            last = next;
        }
        assert_eq!(last >> COUNTER_BITS, ms + 1, "carried into the next ms");
        // When the clock catches up, the stamp follows it.
        assert_eq!(stamps.next(ms + 5), (ms + 5) << COUNTER_BITS);
    }

    /// Threads that take stamps at once, from a clock that keeps moving,
    /// each get stamps that rise, no two alike, and none below the clock.
    /// This reaches the path where a thread moves the stamp up to the clock
    /// while others take stamps.
    #[test]
    fn stamps_taken_at_once_are_distinct_and_rise_on_each_thread() {
        const THREADS: usize = 8;
        const EACH: u64 = 20_000;
        let stamps = Stamps::new();
        let clock = AtomicU64::new(1_000);
        let taken: Vec<Vec<u64>> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..THREADS)
                .map(|_| {
                    scope.spawn(|| {
                        (0..EACH)
                            .map(|i| {
                                if i % 7 == 0 {
                                    clock.fetch_add(1, Ordering::Relaxed);
                                }
                                let now = clock.load(Ordering::Relaxed);
                                let stamp = stamps.next(now);
                                assert!(stamp >= now << COUNTER_BITS, "below the clock");
                                stamp
                            })
                            .collect()
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|w| w.join().expect("took stamps"))
                .collect()
        });
        for stamps in &taken {
            assert!(stamps.windows(2).all(|w| w[0] < w[1]), "fell on a thread");
        }
        let all: std::collections::HashSet<u64> = taken.iter().flatten().copied().collect();
        assert_eq!(all.len(), THREADS * usize::try_from(EACH).expect("fits"));
    }

    #[test]
    fn a_clock_that_steps_back_does_not_reorder_ids() {
        let stamps = Stamps::new();
        let late = stamps.next(2_000);
        let early = stamps.next(1_000);
        assert_eq!(early, late + 1);
    }

    #[test]
    fn a_received_deadline_is_shortened_by_the_skew_and_never_extended() {
        let skew = Duration::from_millis(50);
        let mono = Instant::now();
        let wall = 1_000_000_000_000_u64;
        let ms = |n: u64| n * 1_000_000;

        // 200 ms left by the source's clock: run for 150 ms here.
        let d = Deadline::from_wall_at(wall + ms(200), skew, wall, mono).expect("live");
        assert_eq!(d.instant(), mono + Duration::from_millis(150));
        // The wall-clock form is forwarded unchanged.
        assert_eq!(d.unix_nanos(), wall + ms(200));

        // Within the skew of now, or already past: rejected on arrival.
        assert!(Deadline::from_wall_at(wall + ms(50), skew, wall, mono).is_none());
        assert!(Deadline::from_wall_at(wall - ms(1), skew, wall, mono).is_none());

        // A receiver whose clock runs 40 ms behind the source's sees 40 ms more
        // remaining; the skew allowance still ends it before the source's
        // deadline.
        let behind =
            Deadline::from_wall_at(wall + ms(200), skew, wall - ms(40), mono).expect("live");
        assert!(behind.instant() <= mono + Duration::from_millis(200));
        // A receiver 40 ms ahead stops early, never late.
        let ahead =
            Deadline::from_wall_at(wall + ms(200), skew, wall + ms(40), mono).expect("live");
        assert_eq!(ahead.instant(), mono + Duration::from_millis(110));
    }

    #[test]
    fn a_delay_between_the_clock_reads_shortens_the_budget() {
        let skew = Duration::from_millis(10);
        let wall_before = wall_now_nanos();
        let mono_before = Instant::now();
        let deadline_wall = wall_before + 200_000_000;
        // The task stalls for 50 ms right after reading the wall clock. Were
        // the monotonic clock read after the wall clock, the stall would be
        // added to the budget.
        let d = Deadline::from_wall(deadline_wall, skew, || {
            let wall = wall_now_nanos();
            std::thread::sleep(Duration::from_millis(50));
            wall
        })
        .expect("live");
        let latest = mono_before + Duration::from_millis(200) - skew;
        assert!(d.instant() <= latest, "the stall extended the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn done_reports_a_passed_deadline_whose_timer_has_not_fired() {
        let cx = Cx::new().with_timeout(Duration::from_micros(1_500));
        tokio::time::advance(Duration::from_micros(1_600)).await;
        cx.cancel();
        assert_eq!(cx.done().await.kind(), crate::ErrorKind::DeadlineExceeded);
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_is_reported_ahead_of_a_cancel() {
        let cx = Cx::new().with_timeout(Duration::from_millis(10));
        tokio::time::advance(Duration::from_millis(20)).await;
        cx.cancel();
        assert_eq!(
            cx.check().map_err(|e| e.kind()),
            Err(crate::ErrorKind::DeadlineExceeded)
        );
        assert_eq!(cx.done().await.kind(), crate::ErrorKind::DeadlineExceeded);
    }
}
