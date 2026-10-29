//! Pool policies: how a chooser picks, once per attempt, the pool of a
//! class it sends to. A placement names one per entered class
//! (`[pools.<class>] policy`): gdl's `round_robin` (the default) or
//! `home_first`, or one a crate registers by name ([`PoolPolicies`]).
//!
//! A policy may read the request's head as a [`Selector`](crate::Selector)
//! does, as the edge's type, as [`Encoded`](crate::Encoded) bytes, or as a
//! [`Projected`](crate::Projected) part of it. Its inputs are plain values
//! ([`Pool`], [`PoolRequest`], [`Damped`]) that a policy crate can build
//! itself, so that it tests its policy without a hub or a runtime.

use std::any::TypeId;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

use gdl_model::Encoding;
use tokio::time::Instant;

use crate::cx::Cx;
use crate::error::{Error, ErrorKind, Result};
use crate::followed::{NO_PROPERTIES, PropertyIndexes, PropertyKinds};
use crate::select::{Head, sealed::Sealed};

mod damped;
mod home_first;
mod round_robin;

pub use damped::{Damped, Window};
pub use gdl_pools::{LoadSummary, PoolCondition, PoolState, PropertyState};
pub use home_first::{HomeFirst, HomeFirstAt};
pub use round_robin::RoundRobin;

/// Chooses the pool of a class that one attempt of a hand-off goes to. A
/// placement names it in `[pools.<class>] policy`, and a deployment
/// registers it under that name ([`PoolPolicies`]).
///
/// `choose` runs in the sending segment once per attempt: for the first
/// offer, and again for each pool that declines it, up to `[runtime]
/// pool_failovers` more times. It is synchronous and runs on every
/// hand-off into the class, so it should take no lock and allocate
/// nothing when it chooses a pool.
pub trait PoolPolicy: Send + Sync + 'static {
    /// What the policy reads of the request's head: the head's own type,
    /// [`Encoded`](crate::Encoded), or a [`Projected`](crate::Projected)
    /// part of it. The runtime checks it against the crossing's edge when
    /// the segment starts.
    type Head: Head;

    /// The pool in `pools` to send this attempt to, or [`NoPool`] when no
    /// pool may take it, which fails the request with `Routing` and the
    /// reason the policy gave, if any. `pools` are the class's pools the
    /// chooser follows, in a fixed order: by class and pool. A choice with
    /// a lease learns whether its pool accepted the offer
    /// ([`PoolChoice::with_lease`]).
    fn choose(
        &self,
        request: PoolRequest<'_, Self::Head>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool>;

    /// Whether `pool`, which this policy chose, may still be sent to at
    /// `now`: a choice that waited on a selector is checked again with
    /// it, since the pool's state or summary may have changed meanwhile.
    /// By default, while the pool [takes work](Pool::takes_work).
    fn eligible(&self, pool: &Pool, region: &str, now: Instant) -> bool {
        let _ = (region, now);
        pool.takes_work()
    }

    /// Register in `kinds` each property kind the policy reads from
    /// [`PoolRequest::properties`], so that the chooser decodes it
    /// ([`PropertyKinds::register`]). The runtime calls it once, before the
    /// chooser reads the class's pools; a refusal fails the segment's
    /// start. By default, none.
    fn properties(&self, kinds: &PropertyKinds) -> Result<()> {
        let _ = kinds;
        Ok(())
    }
}

/// What a pool policy chose for one attempt: the index of a pool in the
/// pools it was given, and, if it asks, a lease that learns whether that
/// pool accepted the offer.
pub struct PoolChoice {
    at: usize,
    lease: Option<ChoiceHeld>,
}

/// A choice's lease behind one thin pointer, so that a choice is two words
/// and a choice without a lease costs the pick what an index did: a fat
/// pointer cost each pick 2 to 4 ns more
/// (`docs/src/operations/feature-costs.md`). A lease is two allocations.
#[doc(hidden)]
pub struct ChoiceHeld(Box<Box<dyn ChoiceLease>>);

impl ChoiceHeld {
    /// The chosen pool accepted the offer.
    #[inline]
    pub fn accepted(&mut self) {
        self.0.accepted();
    }
}

impl PoolChoice {
    /// Send this attempt to the pool at `at` in the pools the policy was
    /// given.
    #[inline]
    pub fn new(at: usize) -> Self {
        Self { at, lease: None }
    }

    /// Keep `lease` with the choice. The runtime calls its
    /// [`accepted`](ChoiceLease::accepted) when the chosen pool accepts
    /// the offer, then holds it while the sending segment holds the
    /// request, as a selector's lease is held. It drops `lease` without
    /// telling it when the pool declines the offer, when the offer is
    /// never sent, when the outcome is unknown (no answer came, so the
    /// pool may have run the request but did not say it took it), and
    /// when the runtime chooses again because the pool stopped taking
    /// work. So a request that no pool accepts tells none of its leases.
    /// The lease is boxed, twice: a choice without one allocates nothing.
    pub fn with_lease(mut self, lease: impl ChoiceLease) -> Self {
        self.lease = Some(ChoiceHeld(Box::new(Box::new(lease))));
        self
    }

    /// The index of the chosen pool.
    #[inline]
    pub fn at(&self) -> usize {
        self.at
    }

    /// The index and the lease, for the runtime.
    #[doc(hidden)]
    #[inline]
    pub fn into_parts(self) -> (usize, Option<ChoiceHeld>) {
        (self.at, self.lease)
    }
}

impl fmt::Debug for PoolChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolChoice")
            .field("at", &self.at)
            .field("lease", &self.lease.is_some())
            .finish()
    }
}

/// A pool policy chose no pool for this attempt, with a short reason or
/// none. The first attempt's `NoPool` fails the request with `Routing`,
/// and its reason follows the class and the segment in the error, so a
/// policy can name what no pool serves: "no pool serves shard `a`".
/// A failover attempt's `NoPool` ends the failover, and the request fails
/// with the last pool's refusal, which says more than the policy can.
///
/// `NoPool` is two words, as a reason is a [`Cow`]: a policy's return is
/// no larger than an `Option<PoolChoice>` would be, and a choice costs
/// nothing more. A fixed reason allocates nothing; a reason built per
/// request allocates only when no pool is chosen.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NoPool {
    reason: Option<Cow<'static, str>>,
}

impl NoPool {
    /// No pool, with no reason: the error says that no ready pool of the
    /// class has an instance of the segment.
    #[inline]
    pub fn new() -> Self {
        Self { reason: None }
    }

    /// No pool, because of `reason`: a short phrase, such as "no pool
    /// serves shard `a`", that the request's `Routing` error carries.
    pub fn because(reason: impl Into<Cow<'static, str>>) -> Self {
        Self {
            reason: Some(reason.into()),
        }
    }

    /// The reason the policy gave, if it gave one.
    #[inline]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// What a pool policy keeps with its choice to learn its outcome
/// ([`PoolChoice::with_lease`]): told once if the chosen pool accepts the
/// offer, and dropped when the runtime lets it go.
pub trait ChoiceLease: Send + 'static {
    /// The pool this lease's choice went to accepted the offer: it admitted
    /// the request and will answer it. Called at most once, on the
    /// sending segment's task, before the request's response starts.
    fn accepted(&mut self);
}

/// One followed pool of the class a hand-off enters, as a policy sees it:
/// its name, region, state, load summary, and how many entry instances it
/// has to route to. The runtime makes one for each pool when its table
/// changes, never per request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pool {
    class: String,
    name: String,
    id: u64,
    region: String,
    state: PoolState,
    load: Option<Load>,
    entries: usize,
    /// The follow the chooser's table holds the pool in, and how many
    /// times the relay changed it: a pool that declined an offer as gone
    /// is passed over until either changes ([`Damped::is_gone`]).
    generation: u64,
    changes: u64,
}

impl Pool {
    /// The pool `name` of `class`, in `region`, in `state`, with no entry
    /// instance and no load summary.
    pub fn new(
        class: impl Into<String>,
        name: impl Into<String>,
        region: impl Into<String>,
        state: PoolState,
    ) -> Self {
        let (class, name) = (class.into(), name.into());
        Self {
            id: pool_id(&class, &name),
            class,
            name,
            region: region.into(),
            state,
            load: None,
            entries: 0,
            generation: 0,
            changes: 0,
        }
    }

    /// The pool as the follow `generation` of the chooser's hub shows it,
    /// after the relay changed its state or offered state `changes` times.
    #[doc(hidden)]
    #[inline]
    pub fn with_follow(mut self, generation: u64, changes: u64) -> Self {
        self.generation = generation;
        self.changes = changes;
        self
    }

    /// The pool with `entries` entry instances to route to.
    #[inline]
    pub fn with_entries(mut self, entries: usize) -> Self {
        self.entries = entries;
        self
    }

    /// The pool with its load summary.
    #[inline]
    pub fn with_load(mut self, load: Load) -> Self {
        self.load = Some(load);
        self
    }

    /// The pool's class.
    #[inline]
    pub fn class(&self) -> &str {
        &self.class
    }

    /// The pool's name within its class.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The pool's name as a number: the same in every table of the pool,
    /// and what [`PoolRequest::excluded`] and [`Damped`] hold. It is a hash
    /// that only one build of gdl is sure to make alike, so key anything
    /// kept or shared across instances by class and name, not by id.
    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The region the pool runs in.
    #[inline]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// The pool's state, as its hub last sent it.
    #[inline]
    pub fn state(&self) -> PoolState {
        self.state
    }

    /// The pool's load summary, if one has arrived.
    #[inline]
    pub fn load(&self) -> Option<&Load> {
        self.load.as_ref()
    }

    /// How many entry instances the chooser may route to: none while the
    /// pool is draining, unavailable, not offered, or unread.
    #[inline]
    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Whether the pool takes work: `ready` or `degraded`, with an entry
    /// instance to route to.
    #[inline]
    pub fn takes_work(&self) -> bool {
        self.entries > 0 && matches!(self.state, PoolState::Ready | PoolState::Degraded)
    }
}

/// A pool's load summary as the chooser holds it: when it arrived, on the
/// chooser's clock, and until when it is fresh, worked out once as it
/// arrives so that a choice compares one instant rather than doing the
/// arithmetic per pool and pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Load {
    summary: LoadSummary,
    arrived: Instant,
    fresh_until: Option<Instant>,
}

impl Load {
    /// A summary older than this many of its pool's summary periods is
    /// stale, on the chooser's clock, unless the chooser's
    /// `pool_fresh_periods` says otherwise
    /// ([`with_fresh_periods`](Load::with_fresh_periods)).
    pub const FRESH_PERIODS: u32 = 3;

    /// `summary`, which arrived at `arrived`, fresh for
    /// [`FRESH_PERIODS`](Load::FRESH_PERIODS) of its
    /// periods.
    pub fn new(summary: LoadSummary, arrived: Instant) -> Self {
        Self {
            summary,
            arrived,
            fresh_until: None,
        }
        .with_fresh_periods(Self::FRESH_PERIODS)
    }

    /// This summary, fresh for `periods` of its periods after it arrived.
    pub fn with_fresh_periods(self, periods: u32) -> Self {
        let fresh =
            std::time::Duration::from_millis(self.summary.period_ms).saturating_mul(periods);
        Self {
            fresh_until: (self.summary.period_ms > 0)
                .then(|| self.arrived.checked_add(fresh))
                .flatten(),
            ..self
        }
    }

    /// The summary, as the pool's hub sent it.
    #[inline]
    pub fn summary(&self) -> &LoadSummary {
        &self.summary
    }

    /// When it arrived, on the chooser's clock. The chooser's runtime
    /// dates a summary back by the age its hub sends with it, so this is
    /// when the summary reached the chooser's hub.
    #[inline]
    pub fn arrived(&self) -> Instant {
        self.arrived
    }

    /// The last instant the summary is fresh at: the chooser's fresh
    /// periods of its periods after it arrived
    /// ([`with_fresh_periods`](Load::with_fresh_periods)). `None` when it is never fresh: it names no period, or one
    /// so long that the instant overflows the clock.
    #[inline]
    pub fn fresh_until(&self) -> Option<Instant> {
        self.fresh_until
    }

    /// Whether the summary is fresh at `now`.
    #[inline]
    pub fn is_fresh(&self, now: Instant) -> bool {
        self.fresh_until.is_some_and(|until| now <= until)
    }
}

/// The number that names the pool `pool` of `class`: a hash of the two,
/// so that a request can keep the pools it excluded without cloning a
/// name. Two pools whose names hash alike would be excluded together, for
/// one request: with a 64-bit hash and a fleet's few pools, that does not
/// happen. `DefaultHasher::new` has fixed keys, so every process of one
/// build names a pool alike; its algorithm can change between Rust
/// releases.
#[doc(hidden)]
pub fn pool_id(class: &str, pool: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    (class, pool).hash(&mut hasher);
    hasher.finish()
}

/// What a pool policy sees of one attempt: the request's context and
/// head, the chooser's region, the pools the request excluded, the pools
/// the chooser damps, the chooser's clock, and the attempt's turn.
pub struct PoolRequest<'a, H> {
    cx: &'a Cx,
    head: &'a H,
    bytes: &'a [u8],
    region: &'a str,
    excluded: &'a [u64],
    damped: &'a Damped,
    now: Instant,
    turn: u64,
    random: Option<&'a RandomState>,
    properties: &'a PropertyIndexes,
}

impl<H> Clone for PoolRequest<'_, H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H> Copy for PoolRequest<'_, H> {}

/// No damped pool, for a request that names none.
static UNDAMPED: Damped = Damped::EMPTY;

impl<'a, H> PoolRequest<'a, H> {
    /// The first attempt of a request with context `cx` and head `head`,
    /// from a chooser in `region` at `now`: no pool excluded or damped,
    /// and turn 0.
    pub fn new(cx: &'a Cx, head: &'a H, region: &'a str, now: Instant) -> Self {
        Self {
            cx,
            head,
            bytes: &[],
            region,
            excluded: &[],
            damped: &UNDAMPED,
            now,
            turn: 0,
            random: None,
            properties: &NO_PROPERTIES,
        }
    }

    /// The request with `excluded`, the [`id`](Pool::id)s of the pools
    /// that declined it.
    #[inline]
    pub fn with_excluded(mut self, excluded: &'a [u64]) -> Self {
        self.excluded = excluded;
        self
    }

    /// The request with the chooser's damped pools.
    #[inline]
    pub fn with_damped(mut self, damped: &'a Damped) -> Self {
        self.damped = damped;
        self
    }

    /// The request at turn `turn`.
    #[inline]
    pub fn with_turn(mut self, turn: u64) -> Self {
        self.turn = turn;
        self
    }

    /// The request with its head's encoded bytes.
    #[inline]
    pub fn with_bytes(mut self, bytes: &'a [u8]) -> Self {
        self.bytes = bytes;
        self
    }

    /// The request with `random` to hash its turn by, as each chooser's
    /// crossing has its own.
    #[doc(hidden)]
    #[inline]
    pub fn with_random(mut self, random: &'a RandomState) -> Self {
        self.random = Some(random);
        self
    }

    /// The request with the class's property indexes: the runtime gives
    /// the indexes of the table the policy chooses from, and a policy's
    /// test builds them with [`PropertyIndexes::new`].
    #[inline]
    pub fn with_properties(mut self, properties: &'a PropertyIndexes) -> Self {
        self.properties = properties;
        self
    }

    /// The class's property indexes, from the same table as the pools the
    /// policy chooses from: for each kind the policy registered
    /// ([`PoolPolicy::properties`]), every key a pool of the class lists,
    /// with the pools that list it ([`PropertyIndexes`]).
    /// [`PropertyIndexes::NONE`] for a request built without them. A
    /// policy that sends a request only to the pools that serve what it
    /// asks for keeps them with `choose_where`.
    #[inline]
    pub fn properties(&self) -> &'a PropertyIndexes {
        self.properties
    }

    /// The same attempt, with `head` read from its bytes.
    fn with_head<'b, G>(&self, head: &'b G) -> PoolRequest<'b, G>
    where
        'a: 'b,
    {
        PoolRequest {
            cx: self.cx,
            head,
            bytes: self.bytes,
            region: self.region,
            excluded: self.excluded,
            damped: self.damped,
            now: self.now,
            turn: self.turn,
            random: self.random,
            properties: self.properties,
        }
    }

    /// The request's context: its id, metadata, and deadline.
    #[inline]
    pub fn cx(&self) -> &'a Cx {
        self.cx
    }

    /// The request's head, decoded.
    #[inline]
    pub fn head(&self) -> &'a H {
        self.head
    }

    /// The head as it crosses, encoded by the edge's codec.
    #[inline]
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// The region of the chooser's own pool.
    #[inline]
    pub fn region(&self) -> &'a str {
        self.region
    }

    /// The [`id`](Pool::id)s of the pools that declined this request:
    /// never chosen again for it.
    #[inline]
    pub fn excluded(&self) -> &'a [u64] {
        self.excluded
    }

    /// Whether `pool` may not be chosen for this request: it declined
    /// it, or it declined an offer as gone and its follow has not changed
    /// since ([`Damped::is_gone`]).
    #[inline]
    pub fn is_excluded(&self, pool: &Pool) -> bool {
        self.excluded.contains(&pool.id) || self.damped.is_gone(pool)
    }

    /// The pools the chooser damps, as one moment's view.
    #[inline]
    pub fn damped(&self) -> &'a Damped {
        self.damped
    }

    /// The chooser's clock, on which each pool's summary arrived.
    #[inline]
    pub fn now(&self) -> Instant {
        self.now
    }

    /// The attempt's turn: the count of the attempts the chooser's
    /// hand-off made before it, over every request.
    #[inline]
    pub fn turn(&self) -> u64 {
        self.turn
    }

    /// A random number for this attempt, the turn hashed: computed when
    /// asked, so that a policy that takes turns pays for no draw.
    #[inline]
    pub fn draw(&self) -> u64 {
        match self.random {
            Some(random) => random.hash_one(self.turn),
            None => mix(self.turn, 0),
        }
    }
}

/// A pool policy as the runtime holds it, whatever its head.
#[doc(hidden)]
pub trait ErasedPolicy: Send + Sync + 'static {
    /// Read the head from `request`'s bytes in `encoding`, and choose.
    fn choose(
        &self,
        request: PoolRequest<'_, ()>,
        encoding: Encoding,
        pools: &[Pool],
    ) -> Result<Result<PoolChoice, NoPool>>;

    fn eligible(&self, pool: &Pool, region: &str, now: Instant) -> bool;

    /// The head type it reads, if it reads one.
    fn edge(&self) -> Option<(TypeId, &'static str)>;

    /// Whether it reads the head's fields by name, which a positional
    /// encoding cannot give.
    fn by_name(&self) -> bool;

    /// Register the property kinds it reads.
    fn properties(&self, kinds: &PropertyKinds) -> Result<()>;
}

/// A typed pool policy behind [`ErasedPolicy`].
#[doc(hidden)]
pub struct TypedPolicy<P>(pub Arc<P>);

impl<P: PoolPolicy> ErasedPolicy for TypedPolicy<P> {
    fn choose(
        &self,
        request: PoolRequest<'_, ()>,
        encoding: Encoding,
        pools: &[Pool],
    ) -> Result<Result<PoolChoice, NoPool>> {
        let head = <P::Head as Sealed>::decode(request.bytes, encoding)?;
        Ok(self.0.choose(request.with_head(&head), pools))
    }

    fn eligible(&self, pool: &Pool, region: &str, now: Instant) -> bool {
        self.0.eligible(pool, region, now)
    }

    fn edge(&self) -> Option<(TypeId, &'static str)> {
        <P::Head as Sealed>::edge()
    }

    fn by_name(&self) -> bool {
        <P::Head as Sealed>::by_name()
    }

    fn properties(&self, kinds: &PropertyKinds) -> Result<()> {
        self.0.properties(kinds)
    }
}

/// The policies gdl provides, which a placement names without registering.
const OWN: [&str; 2] = ["home_first", "round_robin"];

/// Pool policies by name, for a deployment to choose pools with. A name
/// the placement uses that is not here resolves to gdl's own policy of
/// that name, if gdl has one.
#[derive(Clone, Default)]
pub struct PoolPolicies {
    named: BTreeMap<String, Arc<dyn ErasedPolicy>>,
}

impl PoolPolicies {
    /// No policies.
    pub fn new() -> Self {
        Self::default()
    }

    /// Choose the pools of the classes whose `[pools.<class>] policy` is
    /// `name` by `policy`. Refuses a name a placement cannot use, the name
    /// of one of gdl's own policies, and a name already added.
    pub fn add<P: PoolPolicy>(&mut self, name: &str, policy: P) -> Result<()> {
        self.add_shared(name, Arc::new(policy))
    }

    #[doc(hidden)]
    pub fn add_shared<P: PoolPolicy>(&mut self, name: &str, policy: Arc<P>) -> Result<()> {
        gdl_model::check_pool_policy_name(name)
            .map_err(|why| Error::new(ErrorKind::Placement, why))?;
        <P::Head as Sealed>::names().map_err(|why| {
            Error::new(ErrorKind::Placement, format!("pool policy `{name}` {why}"))
        })?;
        if OWN.contains(&name) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("pool policy `{name}` is gdl's own, and cannot be registered"),
            ));
        }
        if self.named.contains_key(name) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("pool policy `{name}` is registered twice"),
            ));
        }
        self.named
            .insert(name.to_string(), Arc::new(TypedPolicy(policy)));
        Ok(())
    }

    /// Register in `kinds` the property kinds each registered policy
    /// reads ([`PoolPolicy::properties`]), for the runtime's chooser.
    #[doc(hidden)]
    pub fn register_properties(&self, kinds: &PropertyKinds) -> Result<()> {
        self.named.iter().try_for_each(|(name, policy)| {
            policy
                .properties(kinds)
                .map_err(|e| Error::new(ErrorKind::Placement, format!("pool policy `{name}`: {e}")))
        })
    }

    /// Whether `name` is registered here.
    pub fn contains(&self, name: &str) -> bool {
        self.named.contains_key(name)
    }

    /// The policy a class that names `name` chooses its pools by: the one
    /// registered, or else gdl's own of that name, with `home_first` at the
    /// chooser's thresholds ([`HomeFirstAt`]).
    #[doc(hidden)]
    pub fn resolve(&self, name: &str, home_first: HomeFirstAt) -> Option<Arc<dyn ErasedPolicy>> {
        if let Some(policy) = self.named.get(name) {
            return Some(Arc::clone(policy));
        }
        match name {
            "home_first" => Some(Arc::new(TypedPolicy(Arc::new(home_first)))),
            "round_robin" => Some(Arc::new(TypedPolicy(Arc::new(RoundRobin)))),
            _ => None,
        }
    }

    /// The names a placement can use: the registered ones and gdl's own,
    /// for a refusal to list.
    #[doc(hidden)]
    pub fn names(&self) -> impl Iterator<Item = &str> {
        OWN.into_iter().chain(self.named.keys().map(String::as_str))
    }
}

impl fmt::Debug for PoolPolicies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.named.keys()).finish()
    }
}

/// The damping cut of a choice among `pools`, of which `eligible` says
/// which may be chosen, at `now`: the least `cut` such that no more than
/// half of the eligible pools, rounded down, have more than `cut`
/// milliseconds of their damping windows left in `damped`. A pool with
/// more left than the cut is passed over, and one with no more is chosen
/// as if it were not damped; 0 when no more than half have any left, so
/// that each of those is passed over. A counting pass, then, only when
/// more than half are damped, a binary search of counting passes, at most
/// about 15 over a window of 30 s, each a search of the damped pools per
/// pool, with no allocation. Taken only while a window lasts.
fn cut(pools: &[Pool], eligible: impl Fn(&Pool) -> bool, damped: &Damped, now: Instant) -> u64 {
    let (mut count, mut held, mut longest) = (0u64, 0u64, 0u64);
    for pool in pools {
        if !eligible(pool) {
            continue;
        }
        count += 1;
        let left = damped.left(pool.id, now);
        if left > 0 {
            held += 1;
            longest = longest.max(left);
        }
    }
    let allowed = count / 2;
    if held <= allowed {
        return 0;
    }
    let over = |cut: u64| -> u64 {
        let damped = pools
            .iter()
            .filter(|pool| eligible(pool) && damped.left(pool.id, now) > cut);
        u64::try_from(damped.count()).unwrap_or(u64::MAX)
    };
    let (mut low, mut high) = (0, longest);
    while low < high {
        let mid = low + (high - low) / 2;
        if over(mid) <= allowed {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    high
}

/// A random number for the `nth` pool of a choice from its `draw`: the
/// splitmix64 finalizer, so that one draw gives each pool its own.
fn mix(draw: u64, nth: u64) -> u64 {
    let mut z = draw ^ nth.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    /// A summary is fresh for three of its periods after it arrived,
    /// unless the chooser is given another count.
    #[test]
    fn a_summary_is_fresh_for_the_periods_it_is_given() {
        let arrived = Instant::now();
        let period = std::time::Duration::from_secs(2);
        let summary = LoadSummary {
            limit: 1,
            period_ms: 2000,
            ..LoadSummary::default()
        };
        let load = Load::new(summary, arrived);
        assert_eq!(load.fresh_until(), Some(arrived + period * 3));
        let longer = load.with_fresh_periods(5);
        assert_eq!(longer.fresh_until(), Some(arrived + period * 5));
        assert!(longer.is_fresh(arrived + period * 5));
        assert!(!load.is_fresh(arrived + period * 4));
        assert_eq!(longer.arrived(), arrived);
    }

    use super::*;
    use crate::select::Projected;

    #[derive(Serialize, Deserialize)]
    struct Routed {
        request: Vec<String>,
        hint: Hint,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Hint {
        target: String,
    }

    #[derive(Deserialize, gdl::Projection)]
    #[gdl(of = Routed)]
    struct Hinted {
        hint: Hint,
    }

    /// The index of the pool the hint names.
    struct ByHint;

    impl PoolPolicy for ByHint {
        type Head = Projected<Hinted>;

        fn choose(
            &self,
            request: PoolRequest<'_, Projected<Hinted>>,
            pools: &[Pool],
        ) -> std::result::Result<PoolChoice, NoPool> {
            let target = &request.head().hint.target;
            pools
                .iter()
                .position(|pool| pool.name() == target)
                .map(PoolChoice::new)
                .ok_or_else(NoPool::new)
        }
    }

    /// A choice's lease reaches the runtime through `into_parts`, is told
    /// there, and is dropped with it; a choice without one has none.
    #[test]
    fn a_choice_carries_its_lease_to_the_runtime() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Told(Arc<AtomicUsize>);

        impl ChoiceLease for Told {
            fn accepted(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl Drop for Told {
            fn drop(&mut self) {
                self.0.fetch_add(10, Ordering::SeqCst);
            }
        }

        let told = Arc::new(AtomicUsize::new(0));
        let choice = PoolChoice::new(3).with_lease(Told(Arc::clone(&told)));
        assert_eq!(format!("{choice:?}"), "PoolChoice { at: 3, lease: true }");
        let (at, lease) = choice.into_parts();
        assert_eq!(at, 3);
        let mut lease = lease.expect("a lease");
        lease.accepted();
        assert_eq!(told.load(Ordering::SeqCst), 1);
        drop(lease);
        assert_eq!(told.load(Ordering::SeqCst), 11);
        assert!(PoolChoice::new(0).into_parts().1.is_none());
        assert_eq!(std::mem::size_of::<PoolChoice>(), 16);
    }

    /// A policy's return with a reason is no larger than one without
    /// could be, so a choice pays nothing for it; a fixed reason is
    /// borrowed, and a reason built for the request is kept.
    #[test]
    fn no_pool_carries_its_reason_at_no_cost_to_a_choice() {
        use std::mem::size_of;

        assert_eq!(
            size_of::<std::result::Result<PoolChoice, NoPool>>(),
            size_of::<Option<PoolChoice>>()
        );
        assert_eq!(NoPool::new().reason(), None);
        assert_eq!(NoPool::default(), NoPool::new());
        let fixed = NoPool::because("draining");
        assert!(matches!(fixed.reason, Some(Cow::Borrowed("draining"))));
        let shard = "a";
        let built = NoPool::because(format!("no pool serves shard `{shard}`"));
        assert_eq!(built.reason(), Some("no pool serves shard `a`"));
    }

    /// A projection reads its fields from the whole request in either
    /// encoding that names fields, and names the edge type it projects,
    /// not its own; a request without its fields fails with `Codec`.
    #[test]
    fn a_projection_reads_its_fields_of_the_whole_request() {
        let policy = TypedPolicy(Arc::new(ByHint));
        let pools = [
            Pool::new("c", "a", "r", PoolState::Ready),
            Pool::new("c", "b", "r", PoolState::Ready),
        ];
        let routed = Routed {
            request: vec!["x".repeat(100); 10],
            hint: Hint { target: "b".into() },
        };
        let cx = Cx::new();
        for encoding in [Encoding::Msgpack, Encoding::Json] {
            let bytes = crate::codec::encoder::<Routed>(encoding)(&routed).expect("encoded");
            let request = PoolRequest::new(&cx, &(), "r", Instant::now()).with_bytes(&bytes);
            assert_eq!(
                policy
                    .choose(request, encoding, &pools)
                    .expect("read")
                    .map(|choice| choice.at()),
                Ok(1),
                "{encoding:?}"
            );
            let other = crate::codec::encoder::<Hint>(encoding)(&routed.hint).expect("encoded");
            let request = PoolRequest::new(&cx, &(), "r", Instant::now()).with_bytes(&other);
            let error = policy
                .choose(request, encoding, &pools)
                .expect_err("no hint");
            assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
        }
        assert_eq!(
            policy.edge().map(|(id, _)| id),
            Some(TypeId::of::<Routed>())
        );
        assert!(policy.by_name());
        let own = TypedPolicy(Arc::new(HomeFirst));
        assert!(own.edge().is_none() && !own.by_name());
    }
}
