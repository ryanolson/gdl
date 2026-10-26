//! Cell policies: how a chooser picks, once per attempt, the cell of a
//! class it sends to. A placement names one per entered class
//! (`[cells.<class>] policy`): gdl's `round_robin` (the default) or
//! `home_first`, or one a crate registers by name ([`CellPolicies`]).
//!
//! A policy may read the request's head as a [`Selector`](crate::Selector)
//! does, as the edge's type, as [`Encoded`](crate::Encoded) bytes, or as a
//! [`Projected`](crate::Projected) part of it. Its inputs are plain values
//! ([`Cell`], [`CellRequest`], [`Damped`]) that a policy crate can build
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
use crate::select::{Head, sealed::Sealed};

mod damped;
mod home_first;
mod round_robin;

pub use damped::{Damped, Window};
pub use gdl_cells::{CellState, LoadSummary};
pub use home_first::HomeFirst;
pub use round_robin::RoundRobin;

/// Chooses the cell of a class that one attempt of a hand-off goes to. A
/// placement names it in `[cells.<class>] policy`, and a deployment
/// registers it under that name ([`CellPolicies`]).
///
/// `choose` runs in the sending segment once per attempt: for the first
/// offer, and again for each cell that declines it, up to `[runtime]
/// cell_failovers` more times. It is synchronous and runs on every
/// hand-off into the class, so it should take no lock and allocate
/// nothing when it chooses a cell.
pub trait CellPolicy: Send + Sync + 'static {
    /// What the policy reads of the request's head: the head's own type,
    /// [`Encoded`](crate::Encoded), or a [`Projected`](crate::Projected)
    /// part of it. The runtime checks it against the crossing's edge when
    /// the segment starts.
    type Head: Head;

    /// The cell in `cells` to send this attempt to, or [`NoCell`] when no
    /// cell may take it, which fails the request with `Routing` and the
    /// reason the policy gave, if any. `cells` are the class's cells the
    /// chooser follows, in a fixed order: by class and cell. A choice with
    /// a lease learns whether its cell accepted the offer
    /// ([`CellChoice::with_lease`]).
    fn choose(
        &self,
        request: CellRequest<'_, Self::Head>,
        cells: &[Cell],
    ) -> Result<CellChoice, NoCell>;

    /// Whether `cell`, which this policy chose, may still be sent to at
    /// `now`: a choice that waited on a selector is checked again with
    /// it, since the cell's state or summary may have changed meanwhile.
    /// By default, while the cell [takes work](Cell::takes_work).
    fn eligible(&self, cell: &Cell, region: &str, now: Instant) -> bool {
        let _ = (region, now);
        cell.takes_work()
    }
}

/// What a cell policy chose for one attempt: the index of a cell in the
/// cells it was given, and, if it asks, a lease that learns whether that
/// cell accepted the offer.
pub struct CellChoice {
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
    /// The chosen cell accepted the offer.
    #[inline]
    pub fn accepted(&mut self) {
        self.0.accepted();
    }
}

impl CellChoice {
    /// Send this attempt to the cell at `at` in the cells the policy was
    /// given.
    #[inline]
    pub fn new(at: usize) -> Self {
        Self { at, lease: None }
    }

    /// Keep `lease` with the choice. The runtime calls its
    /// [`accepted`](ChoiceLease::accepted) when the chosen cell accepts
    /// the offer, then holds it while the sending segment holds the
    /// request, as a selector's lease is held. It drops `lease` without
    /// telling it when the cell declines the offer, when the offer is
    /// never sent, when the outcome is unknown (no answer came, so the
    /// cell may have run the request but did not say it took it), and
    /// when the runtime chooses again because the cell stopped taking
    /// work. So a request that no cell accepts tells none of its leases.
    /// The lease is boxed, twice: a choice without one allocates nothing.
    pub fn with_lease(mut self, lease: impl ChoiceLease) -> Self {
        self.lease = Some(ChoiceHeld(Box::new(Box::new(lease))));
        self
    }

    /// The index of the chosen cell.
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

impl fmt::Debug for CellChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CellChoice")
            .field("at", &self.at)
            .field("lease", &self.lease.is_some())
            .finish()
    }
}

/// A cell policy chose no cell for this attempt, with a short reason or
/// none. The first attempt's `NoCell` fails the request with `Routing`,
/// and its reason follows the class and the segment in the error, so a
/// policy can name what no cell serves: "no set serves model `mistral`".
/// A failover attempt's `NoCell` ends the failover, and the request fails
/// with the last cell's refusal, which says more than the policy can.
///
/// `NoCell` is two words, as a reason is a [`Cow`]: a policy's return is
/// no larger than an `Option<CellChoice>` would be, and a choice costs
/// nothing more. A fixed reason allocates nothing; a reason built per
/// request allocates only when no cell is chosen.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NoCell {
    reason: Option<Cow<'static, str>>,
}

impl NoCell {
    /// No cell, with no reason: the error says that no ready cell of the
    /// class has an instance of the segment.
    #[inline]
    pub fn new() -> Self {
        Self { reason: None }
    }

    /// No cell, because of `reason`: a short phrase, such as "no set
    /// serves model `mistral`", that the request's `Routing` error carries.
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

/// What a cell policy keeps with its choice to learn its outcome
/// ([`CellChoice::with_lease`]): told once if the chosen cell accepts the
/// offer, and dropped when the runtime lets it go.
pub trait ChoiceLease: Send + 'static {
    /// The cell this lease's choice went to accepted the offer: it admitted
    /// the request and will answer it. Called at most once, on the
    /// sending segment's task, before the request's response starts.
    fn accepted(&mut self);
}

/// One followed cell of the class a hand-off enters, as a policy sees it:
/// its name, region, state, load summary, and how many entry instances it
/// has to route to. The runtime makes one for each cell when its table
/// changes, never per request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    class: String,
    name: String,
    id: u64,
    region: String,
    state: CellState,
    load: Option<Load>,
    entries: usize,
}

impl Cell {
    /// The cell `name` of `class`, in `region`, in `state`, with no entry
    /// instance and no load summary.
    pub fn new(
        class: impl Into<String>,
        name: impl Into<String>,
        region: impl Into<String>,
        state: CellState,
    ) -> Self {
        let (class, name) = (class.into(), name.into());
        Self {
            id: cell_id(&class, &name),
            class,
            name,
            region: region.into(),
            state,
            load: None,
            entries: 0,
        }
    }

    /// The cell with `entries` entry instances to route to.
    #[inline]
    pub fn with_entries(mut self, entries: usize) -> Self {
        self.entries = entries;
        self
    }

    /// The cell with its load summary.
    #[inline]
    pub fn with_load(mut self, load: Load) -> Self {
        self.load = Some(load);
        self
    }

    /// The cell's class.
    #[inline]
    pub fn class(&self) -> &str {
        &self.class
    }

    /// The cell's name within its class.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The cell's name as a number: the same in every table of the cell,
    /// and what [`CellRequest::excluded`] and [`Damped`] hold. It is a hash
    /// that only one build of gdl is sure to make alike, so key anything
    /// kept or shared across instances by class and name, not by id.
    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The region the cell runs in.
    #[inline]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// The cell's state, as its hub last sent it.
    #[inline]
    pub fn state(&self) -> CellState {
        self.state
    }

    /// The cell's load summary, if one has arrived.
    #[inline]
    pub fn load(&self) -> Option<&Load> {
        self.load.as_ref()
    }

    /// How many entry instances the chooser may route to: none while the
    /// cell is draining, unavailable, not offered, or unread.
    #[inline]
    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Whether the cell takes work: `ready` or `degraded`, with an entry
    /// instance to route to.
    #[inline]
    pub fn takes_work(&self) -> bool {
        self.entries > 0 && matches!(self.state, CellState::Ready | CellState::Degraded)
    }
}

/// A cell's load summary as the chooser holds it: when it arrived, on the
/// chooser's clock, and until when it is fresh, worked out once as it
/// arrives so that a choice compares one instant rather than doing the
/// arithmetic per cell and pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Load {
    summary: LoadSummary,
    arrived: Instant,
    fresh_until: Option<Instant>,
}

impl Load {
    /// A summary older than this many of its cell's summary periods is
    /// stale, on the chooser's clock.
    pub const FRESH_PERIODS: u32 = 3;

    /// `summary`, which arrived at `arrived`.
    pub fn new(summary: LoadSummary, arrived: Instant) -> Self {
        let fresh =
            std::time::Duration::from_millis(summary.period_ms).saturating_mul(Self::FRESH_PERIODS);
        Self {
            summary,
            arrived,
            fresh_until: (summary.period_ms > 0)
                .then(|| arrived.checked_add(fresh))
                .flatten(),
        }
    }

    /// The summary, as the cell's hub sent it.
    #[inline]
    pub fn summary(&self) -> &LoadSummary {
        &self.summary
    }

    /// When it arrived, on the chooser's clock.
    #[inline]
    pub fn arrived(&self) -> Instant {
        self.arrived
    }

    /// The last instant the summary is fresh at:
    /// [`FRESH_PERIODS`](Load::FRESH_PERIODS) of its periods after it
    /// arrived. `None` when it is never fresh: it names no period, or one
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

/// The number that names the cell `cell` of `class`: a hash of the two,
/// so that a request can keep the cells it excluded without cloning a
/// name. Two cells whose names hash alike would be excluded together, for
/// one request: with a 64-bit hash and a fleet's few cells, that does not
/// happen. `DefaultHasher::new` has fixed keys, so every process of one
/// build names a cell alike; its algorithm can change between Rust
/// releases.
#[doc(hidden)]
pub fn cell_id(class: &str, cell: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    (class, cell).hash(&mut hasher);
    hasher.finish()
}

/// What a cell policy sees of one attempt: the request's context and
/// head, the chooser's region, the cells the request excluded, the cells
/// the chooser damps, the chooser's clock, and the attempt's turn.
pub struct CellRequest<'a, H> {
    cx: &'a Cx,
    head: &'a H,
    bytes: &'a [u8],
    region: &'a str,
    excluded: &'a [u64],
    damped: &'a Damped,
    now: Instant,
    turn: u64,
    random: Option<&'a RandomState>,
}

impl<H> Clone for CellRequest<'_, H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H> Copy for CellRequest<'_, H> {}

/// No damped cell, for a request that names none.
static UNDAMPED: Damped = Damped::EMPTY;

impl<'a, H> CellRequest<'a, H> {
    /// The first attempt of a request with context `cx` and head `head`,
    /// from a chooser in `region` at `now`: no cell excluded or damped,
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
        }
    }

    /// The request with `excluded`, the [`id`](Cell::id)s of the cells
    /// that declined it.
    #[inline]
    pub fn with_excluded(mut self, excluded: &'a [u64]) -> Self {
        self.excluded = excluded;
        self
    }

    /// The request with the chooser's damped cells.
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

    /// The same attempt, with `head` read from its bytes.
    fn with_head<'b, G>(&self, head: &'b G) -> CellRequest<'b, G>
    where
        'a: 'b,
    {
        CellRequest {
            cx: self.cx,
            head,
            bytes: self.bytes,
            region: self.region,
            excluded: self.excluded,
            damped: self.damped,
            now: self.now,
            turn: self.turn,
            random: self.random,
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

    /// The region of the chooser's own cell.
    #[inline]
    pub fn region(&self) -> &'a str {
        self.region
    }

    /// The [`id`](Cell::id)s of the cells that declined this request:
    /// never chosen again for it.
    #[inline]
    pub fn excluded(&self) -> &'a [u64] {
        self.excluded
    }

    /// Whether `cell` declined this request.
    #[inline]
    pub fn is_excluded(&self, cell: &Cell) -> bool {
        self.excluded.contains(&cell.id)
    }

    /// The cells the chooser damps, as one moment's view.
    #[inline]
    pub fn damped(&self) -> &'a Damped {
        self.damped
    }

    /// The chooser's clock, on which each cell's summary arrived.
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

/// A cell policy as the runtime holds it, whatever its head.
#[doc(hidden)]
pub trait ErasedPolicy: Send + Sync + 'static {
    /// Read the head from `request`'s bytes in `encoding`, and choose.
    fn choose(
        &self,
        request: CellRequest<'_, ()>,
        encoding: Encoding,
        cells: &[Cell],
    ) -> Result<Result<CellChoice, NoCell>>;

    fn eligible(&self, cell: &Cell, region: &str, now: Instant) -> bool;

    /// The head type it reads, if it reads one.
    fn edge(&self) -> Option<(TypeId, &'static str)>;

    /// Whether it reads the head's fields by name, which a positional
    /// encoding cannot give.
    fn by_name(&self) -> bool;
}

/// A typed cell policy behind [`ErasedPolicy`].
#[doc(hidden)]
pub struct TypedPolicy<P>(pub Arc<P>);

impl<P: CellPolicy> ErasedPolicy for TypedPolicy<P> {
    fn choose(
        &self,
        request: CellRequest<'_, ()>,
        encoding: Encoding,
        cells: &[Cell],
    ) -> Result<Result<CellChoice, NoCell>> {
        let head = <P::Head as Sealed>::decode(request.bytes, encoding)?;
        Ok(self.0.choose(request.with_head(&head), cells))
    }

    fn eligible(&self, cell: &Cell, region: &str, now: Instant) -> bool {
        self.0.eligible(cell, region, now)
    }

    fn edge(&self) -> Option<(TypeId, &'static str)> {
        <P::Head as Sealed>::edge()
    }

    fn by_name(&self) -> bool {
        <P::Head as Sealed>::by_name()
    }
}

/// The policies gdl provides, which a placement names without registering.
const OWN: [&str; 2] = ["home_first", "round_robin"];

/// Cell policies by name, for a deployment to choose cells with. A name
/// the placement uses that is not here resolves to gdl's own policy of
/// that name, if gdl has one.
#[derive(Clone, Default)]
pub struct CellPolicies {
    named: BTreeMap<String, Arc<dyn ErasedPolicy>>,
}

impl CellPolicies {
    /// No policies.
    pub fn new() -> Self {
        Self::default()
    }

    /// Choose the cells of the classes whose `[cells.<class>] policy` is
    /// `name` by `policy`. Refuses a name a placement cannot use, the name
    /// of one of gdl's own policies, and a name already added.
    pub fn add<P: CellPolicy>(&mut self, name: &str, policy: P) -> Result<()> {
        self.add_shared(name, Arc::new(policy))
    }

    #[doc(hidden)]
    pub fn add_shared<P: CellPolicy>(&mut self, name: &str, policy: Arc<P>) -> Result<()> {
        gdl_model::check_cell_policy_name(name)
            .map_err(|why| Error::new(ErrorKind::Placement, why))?;
        <P::Head as Sealed>::names().map_err(|why| {
            Error::new(ErrorKind::Placement, format!("cell policy `{name}` {why}"))
        })?;
        if OWN.contains(&name) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("cell policy `{name}` is gdl's own, and cannot be registered"),
            ));
        }
        if self.named.contains_key(name) {
            return Err(Error::new(
                ErrorKind::Placement,
                format!("cell policy `{name}` is registered twice"),
            ));
        }
        self.named
            .insert(name.to_string(), Arc::new(TypedPolicy(policy)));
        Ok(())
    }

    /// Whether `name` is registered here.
    pub fn contains(&self, name: &str) -> bool {
        self.named.contains_key(name)
    }

    /// The policy a class that names `name` chooses its cells by: the one
    /// registered, or else gdl's own of that name.
    #[doc(hidden)]
    pub fn resolve(&self, name: &str) -> Option<Arc<dyn ErasedPolicy>> {
        if let Some(policy) = self.named.get(name) {
            return Some(Arc::clone(policy));
        }
        match name {
            "home_first" => Some(Arc::new(TypedPolicy(Arc::new(HomeFirst)))),
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

impl fmt::Debug for CellPolicies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.named.keys()).finish()
    }
}

/// The damping cut of a choice among `cells`, of which `eligible` says
/// which may be chosen, at `now`: the least `cut` such that no more than
/// half of the eligible cells, rounded down, have more than `cut`
/// milliseconds of their damping windows left in `damped`. A cell with
/// more left than the cut is passed over, and one with no more is chosen
/// as if it were not damped; 0 when no more than half have any left, so
/// that each of those is passed over. A counting pass, then, only when
/// more than half are damped, a binary search of counting passes, at most
/// about 15 over a window of 30 s, each a search of the damped cells per
/// cell, with no allocation. Taken only while a window lasts.
fn cut(cells: &[Cell], eligible: impl Fn(&Cell) -> bool, damped: &Damped, now: Instant) -> u64 {
    let (mut count, mut held, mut longest) = (0u64, 0u64, 0u64);
    for cell in cells {
        if !eligible(cell) {
            continue;
        }
        count += 1;
        let left = damped.left(cell.id, now);
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
        let damped = cells
            .iter()
            .filter(|cell| eligible(cell) && damped.left(cell.id, now) > cut);
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

/// A random number for the `nth` cell of a choice from its `draw`: the
/// splitmix64 finalizer, so that one draw gives each cell its own.
fn mix(draw: u64, nth: u64) -> u64 {
    let mut z = draw ^ nth.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::select::Projected;

    #[derive(Serialize, Deserialize)]
    struct Routed {
        request: Vec<String>,
        hint: Hint,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Hint {
        model: String,
    }

    #[derive(Deserialize, gdl::Projection)]
    #[gdl(of = Routed)]
    struct Hinted {
        hint: Hint,
    }

    /// The index of the cell named as the hint's model.
    struct ByHint;

    impl CellPolicy for ByHint {
        type Head = Projected<Hinted>;

        fn choose(
            &self,
            request: CellRequest<'_, Projected<Hinted>>,
            cells: &[Cell],
        ) -> std::result::Result<CellChoice, NoCell> {
            let model = &request.head().hint.model;
            cells
                .iter()
                .position(|cell| cell.name() == model)
                .map(CellChoice::new)
                .ok_or_else(NoCell::new)
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
        let choice = CellChoice::new(3).with_lease(Told(Arc::clone(&told)));
        assert_eq!(format!("{choice:?}"), "CellChoice { at: 3, lease: true }");
        let (at, lease) = choice.into_parts();
        assert_eq!(at, 3);
        let mut lease = lease.expect("a lease");
        lease.accepted();
        assert_eq!(told.load(Ordering::SeqCst), 1);
        drop(lease);
        assert_eq!(told.load(Ordering::SeqCst), 11);
        assert!(CellChoice::new(0).into_parts().1.is_none());
        assert_eq!(std::mem::size_of::<CellChoice>(), 16);
    }

    /// A policy's return with a reason is no larger than one without
    /// could be, so a choice pays nothing for it; a fixed reason is
    /// borrowed, and a reason built for the request is kept.
    #[test]
    fn no_cell_carries_its_reason_at_no_cost_to_a_choice() {
        use std::mem::size_of;

        assert_eq!(
            size_of::<std::result::Result<CellChoice, NoCell>>(),
            size_of::<Option<CellChoice>>()
        );
        assert_eq!(NoCell::new().reason(), None);
        assert_eq!(NoCell::default(), NoCell::new());
        let fixed = NoCell::because("draining");
        assert!(matches!(fixed.reason, Some(Cow::Borrowed("draining"))));
        let model = "mistral";
        let built = NoCell::because(format!("no set serves model `{model}`"));
        assert_eq!(built.reason(), Some("no set serves model `mistral`"));
    }

    /// A projection reads its fields from the whole request in either
    /// encoding that names fields, and names the edge type it projects,
    /// not its own; a request without its fields fails with `Codec`.
    #[test]
    fn a_projection_reads_its_fields_of_the_whole_request() {
        let policy = TypedPolicy(Arc::new(ByHint));
        let cells = [
            Cell::new("c", "a", "r", CellState::Ready),
            Cell::new("c", "b", "r", CellState::Ready),
        ];
        let routed = Routed {
            request: vec!["x".repeat(100); 10],
            hint: Hint { model: "b".into() },
        };
        let cx = Cx::new();
        for encoding in [Encoding::Msgpack, Encoding::Json] {
            let bytes = crate::codec::encoder::<Routed>(encoding)(&routed).expect("encoded");
            let request = CellRequest::new(&cx, &(), "r", Instant::now()).with_bytes(&bytes);
            assert_eq!(
                policy
                    .choose(request, encoding, &cells)
                    .expect("read")
                    .map(|choice| choice.at()),
                Ok(1),
                "{encoding:?}"
            );
            let other = crate::codec::encoder::<Hint>(encoding)(&routed.hint).expect("encoded");
            let request = CellRequest::new(&cx, &(), "r", Instant::now()).with_bytes(&other);
            let error = policy
                .choose(request, encoding, &cells)
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
