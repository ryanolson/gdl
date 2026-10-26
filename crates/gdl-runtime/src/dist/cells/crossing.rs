//! A chooser's hand-off into another class: for each attempt, a cell of
//! the class by its cell policy, then an instance of that cell's entry
//! segment by the hand-off's own router. The chosen cell is a local of
//! the hand-off, never on the wire and never kept per request.

use std::collections::BTreeMap;
use std::hash::RandomState;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use futures::future::BoxFuture;
use gdl::__runtime::{ChoiceHeld, Erased, ErasedPolicy, Picked};
use gdl::{CellPolicies, CellRequest, Cx, Error, ErrorKind, Instances, NoCell, Result};
use gdl_model::{Encoding, Layout};
use velo::InstanceId;

use super::damping::Damping;
use super::lease::CellLease;
use super::policy;
use super::{CellTable, Cells, EntryTable};
use crate::dist::roster::{CHOICES, Policy, turn};
use crate::dist::runtime::Within;

/// One hand-off into another class, as a chooser sends it.
pub(crate) struct Crossing {
    target: Target,
    within: Within,
    /// The encoding of the head that crosses, which a cell policy that
    /// reads it reads it in.
    encoding: Encoding,
    /// The attempts' turns for the cell policy, and the draws hashed from
    /// them for a policy that draws at random, apart from the turns of the
    /// instance within the cell: with one sequence and one hasher for
    /// both, the two choices of an attempt would be correlated.
    draws: AtomicUsize,
    random: RandomState,
}

impl Crossing {
    /// The hand-off into `target` of a head in `encoding`, choosing the
    /// instance within the chosen cell as `within` chooses.
    pub(crate) fn new(target: Target, within: Within, encoding: Encoding) -> Self {
        Self {
            target,
            within,
            encoding,
            draws: AtomicUsize::new(0),
            random: RandomState::new(),
        }
    }

    /// How the instance within the chosen cell is chosen.
    pub(crate) fn within(&self) -> &Within {
        &self.within
    }

    /// Choose a cell for one attempt of the request with context `cx`
    /// and head `head`, then an instance of it by `policy`, with the
    /// chosen cell's [`id`](CellTable::id) and a lease on its follow,
    /// which holds the cell policy's lease on the choice, if it gave one, or
    /// [`NoCell`], with the policy's reason if it gave one, when no cell
    /// but those in `excluded` may take it: one load
    /// of the table, a read of the clock, two counter increments, the cell
    /// policy's read of the head and its passes over the followed cells,
    /// and the lease, with no lock and no allocation of the runtime's. A
    /// cell whose follow became not offered since the table was loaded
    /// refuses the lease, and the table, which then no longer offers it,
    /// is loaded again. Fails when the cell policy cannot read the head.
    pub(crate) fn pick(
        &self,
        cx: &Cx,
        head: &[u8],
        policy: Policy,
        random: &RandomState,
        excluded: &[u64],
    ) -> Result<Result<(InstanceId, u64, CellLease), NoCell>> {
        for _ in 0..CHOICES {
            let table = self.target.table.load();
            let (cell, choice) = match self.cell(&table, cx, head, excluded)? {
                Ok(chosen) => chosen,
                Err(no) => return Ok(Err(no)),
            };
            let Some(lease) = CellLease::take(&cell.leases) else {
                continue;
            };
            let sequence = cell.turn.fetch_add(1, Ordering::Relaxed);
            return Ok(Ok((
                cell.eligible[turn(policy, sequence, cell.eligible.len(), random)],
                cell.id,
                lease.with_choice(choice),
            )));
        }
        Ok(Err(NoCell::new()))
    }

    /// Choose a cell for one attempt, then ask `selector` to choose among
    /// that cell's eligible entry instances alone; [`NoCell`] when no cell
    /// but those in `excluded` may take it. A selector may wait, so the choice
    /// is checked when it returns: if the instance is no
    /// longer eligible in the cell's current table, or the cell itself no
    /// longer is by the cell policy (it stopped taking work, or, under
    /// `home_first`, its summary went or went stale), or
    /// its follow refuses the lease because it is no longer offered, the
    /// choice and its lease are dropped and both the cell and the instance
    /// are chosen again. A request that ended while its selector chose
    /// is not handed on. Boxed, as
    /// [`Live::choose`](crate::dist::roster::Live::choose) is.
    pub(crate) fn choose<'a>(
        &'a self,
        selector: &'a dyn Erased,
        cx: &'a Cx,
        head: &'a [u8],
        encoding: gdl_model::Encoding,
        excluded: &'a [u64],
    ) -> BoxFuture<'a, Result<Result<Chosen, NoCell>>> {
        Box::pin(async move {
            for _ in 0..CHOICES {
                let table = self.target.table.load_full();
                let (cell, choice) = match self.cell(&table, cx, head, excluded)? {
                    Ok(chosen) => chosen,
                    Err(no) => return Ok(Err(no)),
                };
                let cell = Arc::clone(cell);
                let picked = selector
                    .select(
                        cx,
                        head,
                        encoding,
                        // The cells failover chooses another cell, so no
                        // instance of this one has declined the request.
                        &[],
                        Instances::new(&self.target.segment, &cell.offered),
                    )
                    .await?;
                // A request that ended while its selector chose gets no
                // instance; its lease goes with `picked`. The caller's
                // select sees the end only at its next await, and the
                // send can leave before that.
                cx.check()?;
                let chosen = cell.eligible[picked.index];
                let target = &self.target;
                let now = tokio::time::Instant::now();
                let policy =
                    |current: &gdl::Cell| target.policy.eligible(current, &target.region, now);
                let current = target.table.load();
                let lease = still_eligible(&current, &cell, chosen, policy)
                    .and_then(|now| CellLease::take(&now.leases));
                if let Some(lease) = lease {
                    return Ok(Ok((chosen, cell.id, picked, lease.with_choice(choice))));
                }
            }
            Err(Error::routing(format!(
                "the cells of class `{}` changed under the selector of segment `{}` {CHOICES} times",
                self.target.class, self.target.segment
            )))
        })
    }

    /// The request's `Routing` error when no cell of the class may take
    /// it (`no`): a request never waits for a cell to appear. A policy's
    /// reason follows its name, the class, and the segment; it replaces
    /// the claim that no cell is ready, since a policy may refuse while
    /// every cell is. The reason is copied into the message, which is
    /// built here in any case: a reason costs the policy's own allocation,
    /// if it made one, and its bytes in the message.
    pub(crate) fn no_cell(&self, no: NoCell) -> Error {
        let target = &self.target;
        match no.reason() {
            None => Error::routing(format!(
                "no ready cell of class `{}` has an instance of segment `{}`",
                target.class, target.segment
            )),
            Some(reason) => Error::routing(format!(
                "cell policy `{}` of class `{}` chose no cell for segment `{}`: {reason}",
                target.name, target.class, target.segment
            )),
        }
    }

    /// The class this hand-off enters.
    pub(crate) fn class(&self) -> &str {
        &self.target.class
    }

    /// The cell `cell` declined an offer: the chooser damps it.
    pub(crate) fn declined(&self, cell: u64) {
        self.target
            .damping
            .decline(cell, tokio::time::Instant::now());
    }

    /// The cell `cell` accepted an offer: its damping, as the chooser has
    /// it now, ends, whatever it was when the offer was chosen: another
    /// request's decline may have damped it since, or another acceptance
    /// ended it. A cell that is not damped writes nothing, and costs one
    /// load and a search of the damped cells, with no allocation.
    pub(crate) fn accepted(&self, cell: u64) {
        self.target.damping.accept(cell);
    }

    /// The cell the policy chooses from `table` for one attempt of the
    /// request with context `cx` and head `head`, of those not in
    /// `excluded`, by the summaries' ages now, with the policy's lease on
    /// the choice, if it gave one, or the policy's [`NoCell`]: one relaxed
    /// increment of the attempts' count, which a policy that draws at
    /// random hashes when it asks. Fails when the policy cannot read the
    /// head.
    fn cell<'t>(
        &self,
        table: &'t EntryTable,
        cx: &Cx,
        head: &[u8],
        excluded: &[u64],
    ) -> Result<Result<Celled<'t>, NoCell>> {
        let target = &self.target;
        let turn = self.draws.fetch_add(1, Ordering::Relaxed) as u64;
        let damped = target.damping.view();
        let request = CellRequest::new(cx, &(), &target.region, tokio::time::Instant::now())
            .with_bytes(head)
            .with_excluded(excluded)
            .with_damped(&damped)
            .with_turn(turn)
            .with_random(&self.random);
        let (at, choice) = match target.policy.choose(request, self.encoding, &table.views)? {
            Ok(choice) => choice.into_parts(),
            Err(no) => return Ok(Err(no)),
        };
        // A policy names a cell by its place in the views it was given,
        // which are the table's, index for index; a place past them is
        // no cell.
        Ok(table
            .cells
            .get(at)
            .map(|cell| (cell, choice))
            .ok_or_else(NoCell::new))
    }

    /// Refuse a cell policy that reads the head as a type the crossing's
    /// edge does not carry (`carried`, with the id `carries`), or by its
    /// fields' names when the edge's encoding is positional: checked when
    /// the segment starts, not per request.
    pub(crate) fn check_head(
        &self,
        segment: &str,
        carries: std::any::TypeId,
        carried: &str,
    ) -> Result<()> {
        let target = &self.target;
        let refused = |why: String| {
            Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "segment `{segment}`: cell policy `{}` of class `{}` {why}",
                    target.name, target.class
                ),
            ))
        };
        if let Some((wants, wanted)) = target.policy.edge()
            && wants != carries
        {
            return refused(format!(
                "reads the head as `{wanted}`, but the request that crosses into `{}` carries `{carried}`",
                target.segment
            ));
        }
        if target.policy.by_name() && self.encoding == Encoding::MsgpackArray {
            return refused(format!(
                "reads the head's fields by name, but the request that crosses into `{}` is encoded as `msgpack_array`, which carries no names",
                target.segment
            ));
        }
        Ok(())
    }
}

/// The cell's current table in `table`, which may be newer, if `chosen`,
/// an instance of `cell`, is still eligible in it: the cell's current
/// table, of whatever generation, is `cell` itself or still offers it, and
/// `policy` still takes it, as it sees it. The lease is taken from it,
/// since it is the follow the offer is made under.
fn still_eligible<'t>(
    table: &'t EntryTable,
    cell: &Arc<CellTable>,
    chosen: InstanceId,
    policy: impl Fn(&gdl::Cell) -> bool,
) -> Option<&'t Arc<CellTable>> {
    table
        .cells
        .iter()
        .zip(&table.views)
        .find(|(now, _)| now.class == cell.class && now.cell == cell.cell)
        .filter(|(now, view)| {
            (Arc::ptr_eq(now, cell) || now.eligible.contains(&chosen)) && policy(view)
        })
        .map(|(now, _)| now)
}

/// A selector's choice within a cell: the instance, the cell's
/// [`id`](CellTable::id), what the selector picked, and the lease on the
/// cell's follow.
pub(crate) type Chosen = (InstanceId, u64, Picked, CellLease);

/// A cell policy's choice: the chosen cell's table, and the policy's lease
/// on the choice, if it gave one.
type Celled<'t> = (&'t Arc<CellTable>, Option<ChoiceHeld>);

/// What a hand-off into another class chooses from: the class, its entry
/// segment, the chooser's table of that segment's cells, the chooser's
/// region, the class's cell policy and its name, and the cells the
/// chooser damps.
#[derive(Clone)]
pub(crate) struct Target {
    class: String,
    segment: String,
    table: Arc<ArcSwap<EntryTable>>,
    region: Arc<str>,
    policy: Arc<dyn ErasedPolicy>,
    name: Arc<str>,
    /// The cells the chooser damps, shared by all its hand-offs.
    damping: Arc<Damping>,
}

/// The hand-offs of `segment` that cross into another class, by hop and
/// forward position, each with what it chooses from: those whose hop's
/// outer end is in `segment` and whose entry segment `cells` keeps a
/// table for. Found once, when the runtime is built, so that a request
/// pays one branch on what was found. A cell policy that `policies` does
/// not register and gdl does not have is refused.
pub(crate) fn targets(
    layout: &Layout,
    segment: &str,
    cells: &Cells,
    policies: &CellPolicies,
) -> Result<BTreeMap<(usize, usize), Target>> {
    let mut targets = BTreeMap::new();
    for boundary in layout.boundaries() {
        let runs = &layout.hops()[boundary.hop].forward_runs;
        // The boundary rule: the hop leaves its outer end's segment only
        // for its inner end, so the hand-off into the last run is the one
        // that crosses.
        let (Some(outer), Some(inner)) = (runs.first(), runs.last()) else {
            continue;
        };
        if runs.len() < 2 || outer.segment != segment {
            continue;
        }
        let Some(table) = cells.slot(&inner.segment) else {
            continue;
        };
        let (name, policy) = policy::of(layout, &boundary.into, policies)?;
        targets.insert(
            (boundary.hop, inner.start),
            Target {
                class: boundary.into.clone(),
                segment: inner.segment.clone(),
                table,
                region: cells.region(),
                policy,
                name,
                damping: cells.damping(),
            },
        );
    }
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    use gdl_cells::CellState;

    use super::*;
    use crate::dist::roster::Picker;

    fn cell(name: &str, state: CellState, eligible: &[InstanceId]) -> Arc<CellTable> {
        cell_turning(name, state, eligible, Arc::default())
    }

    /// A table of `name` that takes its turns from `turn`, as each table
    /// of one follow does.
    fn cell_turning(
        name: &str,
        state: CellState,
        eligible: &[InstanceId],
        turn: Arc<AtomicUsize>,
    ) -> Arc<CellTable> {
        Arc::new(CellTable {
            class: "model_x".into(),
            cell: name.into(),
            id: super::super::cell_id("model_x", name),
            generation: 1,
            region: "home".into(),
            state,
            load: super::super::tests::idle(),
            eligible: eligible.into(),
            offered: eligible
                .iter()
                .copied()
                .map(crate::dist::roster::selector_id)
                .collect(),
            retired: Box::new([]),
            unread: false,
            turn,
            offering: true,
            revision: 1,
            leases: Arc::default(),
        })
    }

    fn crossing(cells: Vec<Arc<CellTable>>, within: Within) -> Crossing {
        let table = Arc::new(ArcSwap::from_pointee(EntryTable::new(
            cells.into_boxed_slice(),
        )));
        Crossing::new(
            Target {
                class: "model_x".into(),
                segment: "f".into(),
                table,
                region: Arc::from("home"),
                policy: Arc::new(gdl::__runtime::TypedPolicy(Arc::new(gdl::HomeFirst))),
                name: Arc::from("home_first"),
                damping: Arc::default(),
            },
            within,
            Default::default(),
        )
    }

    #[derive(serde::Deserialize)]
    struct Routed {
        #[allow(dead_code, reason = "a field to read, never read")]
        hint: String,
    }

    #[derive(serde::Deserialize, gdl::Projection)]
    #[gdl(of = Routed)]
    struct Hinted {
        #[allow(dead_code, reason = "a field to read, never read")]
        hint: String,
    }

    /// Reads a projection of `Routed`'s fields.
    struct ByName;

    impl gdl::CellPolicy for ByName {
        type Head = gdl::Projected<Hinted>;

        fn choose(
            &self,
            _: CellRequest<'_, gdl::Projected<Hinted>>,
            _: &[gdl::Cell],
        ) -> std::result::Result<gdl::CellChoice, NoCell> {
            Err(NoCell::new())
        }
    }

    /// The edge check at the segment's start: a policy that reads a type
    /// must be given that type, and one that reads fields by name an
    /// encoding that names them; a policy that reads no type takes any
    /// edge.
    #[test]
    fn a_policy_head_is_checked_against_the_edge() {
        let by_name = |encoding| {
            let mut crossing = crossing(Vec::new(), Within::Policy(Policy::RoundRobin));
            crossing.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(ByName)));
            crossing.target.name = Arc::from("by_name");
            crossing.encoding = encoding;
            crossing
        };
        let routed = std::any::TypeId::of::<Routed>();
        for encoding in [Encoding::Msgpack, Encoding::Json] {
            by_name(encoding)
                .check_head("a", routed, "Routed")
                .expect("an edge of the type, in an encoding that names fields");
        }
        let error = by_name(Encoding::MsgpackArray)
            .check_head("a", routed, "Routed")
            .expect_err("a positional encoding");
        let message = error.to_string();
        assert_eq!(error.kind(), ErrorKind::Placement);
        assert!(
            message.contains("by_name") && message.contains("msgpack_array"),
            "{message}"
        );
        let error = by_name(Encoding::Msgpack)
            .check_head("a", std::any::TypeId::of::<u32>(), "u32")
            .expect_err("another type");
        let message = error.to_string();
        assert!(
            message.contains("Routed") && message.contains("u32"),
            "{message}"
        );
        let own = crossing(Vec::new(), Within::Policy(Policy::RoundRobin));
        own.check_head("a", std::any::TypeId::of::<u32>(), "u32")
            .expect("home_first reads no head");
    }

    /// The longest a test waits for something it expects.
    const WAIT: Duration = Duration::from_secs(5);

    fn ids(n: usize) -> Vec<InstanceId> {
        (0..n).map(|_| InstanceId::new_v4()).collect()
    }

    #[test]
    fn round_robin_takes_turns_within_a_cell_across_its_tables() {
        let cx = Cx::new();
        let ids = ids(2);
        let first = cell("m", CellState::Ready, &ids);
        let crossing = crossing(vec![Arc::clone(&first)], Within::Policy(Policy::RoundRobin));
        let random = RandomState::new();
        let pick = || {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                .expect("a pick")
                .expect("one")
                .0
        };
        assert_eq!([pick(), pick(), pick()], [ids[0], ids[1], ids[0]]);
        // The follower makes a new table for an event, carrying the turn.
        let again = cell_turning("m", CellState::Ready, &ids, Arc::clone(&first.turn));
        crossing
            .target
            .table
            .store(Arc::new(EntryTable::new(Box::new([again]))));
        assert_eq!(pick(), ids[1], "a new table reset the turn");
    }

    #[test]
    fn no_cell_to_choose_is_a_routing_error_naming_the_class() {
        let cx = Cx::new();
        let crossing = crossing(
            vec![cell("m", CellState::Unavailable, &[])],
            Within::Policy(Policy::RoundRobin),
        );
        let no = crossing
            .pick(&cx, &[], Policy::RoundRobin, &RandomState::new(), &[])
            .expect("a pick")
            .expect_err("no cell");
        assert_eq!(no, gdl::NoCell::new());
        let error = crossing.no_cell(no);
        assert_eq!(error.kind(), gdl::ErrorKind::Routing);
        assert_eq!(
            error.to_string(),
            "routing error: no ready cell of class `model_x` has an instance of segment `f`"
        );
    }

    /// Refuses every request, naming why.
    struct Refusing;

    impl gdl::CellPolicy for Refusing {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            _: CellRequest<'_, gdl::Encoded>,
            _: &[gdl::Cell],
        ) -> std::result::Result<gdl::CellChoice, gdl::NoCell> {
            Err(gdl::NoCell::because("no set serves model `mistral`"))
        }
    }

    /// A policy's reason for choosing no cell reaches the request's
    /// `Routing` error, by a pick and by a selector's choice alike, after
    /// the policy, the class, and the segment, and with no claim that no
    /// cell is ready: every cell here is.
    #[tokio::test]
    async fn a_policy_reason_for_no_cell_reaches_the_error() {
        let ids = ids(1);
        let mut crossing = crossing(
            vec![cell("m", CellState::Ready, &ids)],
            Within::Policy(Policy::RoundRobin),
        );
        crossing.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(Refusing)));
        crossing.target.name = Arc::from("refusing");
        let cx = Cx::new();
        let picked = crossing
            .pick(&cx, &[], Policy::RoundRobin, &RandomState::new(), &[])
            .expect("a pick")
            .expect_err("no cell");
        let selector = gdl::__runtime::Typed(Arc::new(gdl::LeastLoaded::new()));
        let Err(chosen) = crossing
            .choose(&selector, &cx, &[], Default::default(), &[])
            .await
            .expect("a choice")
        else {
            panic!("a cell");
        };
        for no in [picked, chosen] {
            assert_eq!(no.reason(), Some("no set serves model `mistral`"));
            let error = crossing.no_cell(no);
            assert_eq!(error.kind(), gdl::ErrorKind::Routing);
            assert_eq!(
                error.to_string(),
                "routing error: cell policy `refusing` of class `model_x` chose no cell for segment `f`: \
                 no set serves model `mistral`"
            );
        }
    }

    /// Refuses every request with no reason, a fixed one, or one built
    /// for the request.
    enum Reasoning {
        None,
        Fixed,
        Built(&'static str),
    }

    impl gdl::CellPolicy for Reasoning {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            _: CellRequest<'_, gdl::Encoded>,
            _: &[gdl::Cell],
        ) -> std::result::Result<gdl::CellChoice, NoCell> {
            Err(match self {
                Self::None => NoCell::new(),
                Self::Fixed => NoCell::because("no set serves the model"),
                Self::Built(model) => NoCell::because(format!("no set serves model `{model}`")),
            })
        }
    }

    /// What choosing no cell costs, in time, for the record in the book: a
    /// pick that the policy refuses and the request's error made from it,
    /// with no reason, a fixed one, and one built for the request. Ignored:
    /// it times, and asserts nothing. Run it in release:
    /// `cargo test --release -p gdl-runtime --lib cost_of_no_cell -- --ignored --nocapture`.
    #[test]
    #[ignore = "a timing, not a check"]
    fn cost_of_no_cell_and_its_reason() {
        const PICKS: u32 = 1_000_000;
        let ids = ids(1);
        let cx = Cx::new();
        let random = RandomState::new();
        for (name, policy) in [
            ("no reason", Reasoning::None),
            ("a fixed reason", Reasoning::Fixed),
            ("a built reason", Reasoning::Built("mistral")),
        ] {
            let mut crossing = crossing(
                vec![cell("m", CellState::Ready, &ids)],
                Within::Policy(Policy::RoundRobin),
            );
            crossing.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(policy)));
            let mut best = Duration::MAX;
            for _ in 0..5 {
                let start = Instant::now();
                for _ in 0..PICKS {
                    let picked = crossing
                        .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                        .expect("a pick");
                    let Err(no) = picked else { panic!("a cell") };
                    std::hint::black_box(crossing.no_cell(no));
                }
                best = best.min(start.elapsed());
            }
            println!(
                "no cell, {name}: {:.1} ns",
                best.as_nanos() as f64 / f64::from(PICKS)
            );
        }
    }

    /// A cell the request excluded is passed over, by the policy and by a
    /// selector alike, and with every cell excluded there is none.
    #[tokio::test]
    async fn an_excluded_cell_is_passed_over() {
        let ids = ids(2);
        let crossing = crossing(
            vec![
                cell("m1", CellState::Ready, &ids[..1]),
                cell("m2", CellState::Degraded, &ids[1..]),
            ],
            Within::Policy(Policy::RoundRobin),
        );
        let random = RandomState::new();
        let cx = Cx::new();
        let m1 = super::super::cell_id("model_x", "m1");
        let m2 = super::super::cell_id("model_x", "m2");
        let pick = |excluded: &[u64]| {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, excluded)
                .expect("a pick")
                .map(|(id, cell, _)| (id, cell))
        };
        assert_eq!(pick(&[]), Ok((ids[0], m1)));
        assert_eq!(pick(&[m1]), Ok((ids[1], m2)));
        assert_eq!(pick(&[m1, m2]), Err(gdl::NoCell::new()));
        let selector = gdl::__runtime::Typed(Arc::new(gdl::LeastLoaded::new()));
        let cx = Cx::new();
        let chosen = crossing
            .choose(&selector, &cx, &[], Default::default(), &[m1])
            .await
            .expect("a choice")
            .map(|(id, cell, _, _)| (id, cell));
        assert_eq!(chosen, Ok((ids[1], m2)));
        let none = crossing
            .choose(&selector, &cx, &[], Default::default(), &[m1, m2])
            .await
            .expect("no error");
        assert!(none.is_err());
    }

    /// The first call waits at a gate, then takes `target`; later calls
    /// take the first instance. Each choice holds a lease.
    struct Waiting {
        target: InstanceId,
        calls: AtomicUsize,
        arrived: tokio::sync::Notify,
        release: tokio::sync::Notify,
        leases: Arc<AtomicUsize>,
    }

    struct Lease(Arc<AtomicUsize>);

    impl Drop for Lease {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl gdl::Selector for Waiting {
        type Head = gdl::Encoded;

        async fn select<'a>(
            &'a self,
            _: gdl::Request<'a, gdl::Encoded>,
            instances: Instances<'a>,
        ) -> Result<gdl::Choice<'a>> {
            let chosen = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.arrived.notify_one();
                self.release.notified().await;
                instances
                    .iter()
                    .find(|i| i.id().as_u128() == self.target.as_u128())
                    .expect("the target is offered")
            } else {
                instances.get(0).expect("one")
            };
            self.leases.fetch_add(1, Ordering::SeqCst);
            Ok(gdl::Choice::new(chosen).with_lease(Lease(Arc::clone(&self.leases))))
        }
    }

    #[tokio::test]
    async fn a_choice_that_left_its_cell_while_the_selector_waited_is_made_again() {
        let ids = ids(2);
        let waiting = Arc::new(Waiting {
            target: ids[0],
            calls: AtomicUsize::new(0),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            leases: Arc::new(AtomicUsize::new(0)),
        });
        let crossing = Arc::new(crossing(
            vec![cell("m", CellState::Ready, &ids)],
            Within::Policy(Policy::RoundRobin),
        ));
        let choosing = tokio::spawn({
            let crossing = Arc::clone(&crossing);
            let waiting = Arc::clone(&waiting);
            async move {
                let selector = gdl::__runtime::Typed(waiting);
                let cx = Cx::new();
                crossing
                    .choose(&selector, &cx, &[], Default::default(), &[])
                    .await
                    .map(|chosen| chosen.map(|(id, _, picked, _)| (id, picked.lease)))
            }
        });
        tokio::time::timeout(WAIT, waiting.arrived.notified())
            .await
            .expect("the selector was asked in time");
        // While the selector waits, its choice leaves the cell's table.
        crossing
            .target
            .table
            .store(Arc::new(EntryTable::new(Box::new([cell(
                "m",
                CellState::Ready,
                &ids[1..],
            )]))));
        waiting.release.notify_one();
        let (chosen, lease) = tokio::time::timeout(WAIT, choosing)
            .await
            .expect("chose in time")
            .expect("joined")
            .expect("no error")
            .expect("a choice");
        assert_eq!(chosen, ids[1], "sent to an instance its cell no longer has");
        assert_eq!(waiting.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            waiting.leases.load(Ordering::SeqCst),
            1,
            "the stale choice's lease was kept"
        );
        drop(lease);
        assert_eq!(waiting.leases.load(Ordering::SeqCst), 0);
    }

    /// A selector that waits, over a crossing into the one cell `m`, whose
    /// first choice is held until `change` has run: what the choice comes
    /// to once it is let go.
    async fn chosen_after(
        change: impl AsyncFnOnce(&Crossing, &[InstanceId]),
    ) -> Option<(InstanceId, u64)> {
        let ids = ids(2);
        let waiting = Arc::new(Waiting {
            target: ids[0],
            calls: AtomicUsize::new(0),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            leases: Arc::new(AtomicUsize::new(0)),
        });
        let crossing = Arc::new(crossing(
            vec![cell("m", CellState::Ready, &ids)],
            Within::Policy(Policy::RoundRobin),
        ));
        let choosing = tokio::spawn({
            let crossing = Arc::clone(&crossing);
            let waiting = Arc::clone(&waiting);
            async move {
                let selector = gdl::__runtime::Typed(waiting);
                let cx = Cx::new();
                crossing
                    .choose(&selector, &cx, &[], Default::default(), &[])
                    .await
                    .map(|chosen| chosen.ok().map(|(id, cell, _, _)| (id, cell)))
            }
        });
        tokio::time::timeout(WAIT, waiting.arrived.notified())
            .await
            .expect("the selector was asked in time");
        change(&crossing, &ids).await;
        waiting.release.notify_one();
        tokio::time::timeout(WAIT, choosing)
            .await
            .expect("chose in time")
            .expect("joined")
            .expect("no error")
    }

    /// A cell whose summary is gone while the selector waited is not sent
    /// to, though it still has the chosen instance: with no other cell,
    /// there is no choice, and the request fails with `Routing`.
    #[tokio::test]
    async fn a_cell_whose_summary_went_while_the_selector_waited_is_not_sent_to() {
        let chosen = chosen_after(async |crossing: &Crossing, ids: &[InstanceId]| {
            let mut unknown = Arc::try_unwrap(cell("m", CellState::Ready, ids)).expect("one owner");
            unknown.load = None;
            crossing
                .target
                .table
                .store(Arc::new(EntryTable::new(Box::new([Arc::new(unknown)]))));
        })
        .await;
        assert_eq!(chosen, None, "sent to a cell with no summary");
    }

    /// A cell whose summary went stale while the selector waited, on the
    /// chooser's clock, is not sent to either, though its table is the one
    /// it was chosen from.
    #[tokio::test(start_paused = true)]
    async fn a_cell_whose_summary_went_stale_while_the_selector_waited_is_not_sent_to() {
        let chosen = chosen_after(async |_: &Crossing, _: &[InstanceId]| {
            // The summary's period is 60 s; it is stale after three.
            tokio::time::advance(Duration::from_secs(181)).await;
        })
        .await;
        assert_eq!(chosen, None, "sent to a cell whose summary is stale");
    }

    /// Cancels the request it chooses for, then chooses, with a lease.
    struct Cancelling(Arc<AtomicUsize>);

    impl gdl::Selector for Cancelling {
        type Head = gdl::Encoded;

        async fn select<'a>(
            &'a self,
            request: gdl::Request<'a, gdl::Encoded>,
            instances: Instances<'a>,
        ) -> Result<gdl::Choice<'a>> {
            request.cx().cancel();
            self.0.fetch_add(1, Ordering::SeqCst);
            let chosen = instances.get(0).expect("one");
            Ok(gdl::Choice::new(chosen).with_lease(Lease(Arc::clone(&self.0))))
        }
    }

    #[tokio::test]
    async fn a_request_cancelled_while_its_selector_chose_gets_no_instance() {
        let ids = ids(1);
        let leases = Arc::new(AtomicUsize::new(0));
        let crossing = crossing(
            vec![cell("m", CellState::Ready, &ids)],
            Within::Policy(Policy::RoundRobin),
        );
        let selector = gdl::__runtime::Typed(Arc::new(Cancelling(Arc::clone(&leases))));
        let cx = Cx::new();
        let error = crossing
            .choose(&selector, &cx, &[], Default::default(), &[])
            .await
            .map(|chosen| chosen.map(|(id, ..)| id))
            .expect_err("a cancelled request was given an instance");
        assert_eq!(error.kind(), gdl::ErrorKind::Cancelled, "{error}");
        assert_eq!(leases.load(Ordering::SeqCst), 0, "the lease was kept");
        assert_eq!(
            crossing.target.table.load().cells[0].leases.held(),
            0,
            "the lease on the cell's follow was kept"
        );
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Prompt {
        text: String,
    }

    /// Reads the head as a `Prompt`, then takes the first instance.
    struct Reading;

    impl gdl::Selector for Reading {
        type Head = Prompt;

        async fn select<'a>(
            &'a self,
            request: gdl::Request<'a, Prompt>,
            instances: Instances<'a>,
        ) -> Result<gdl::Choice<'a>> {
            assert_eq!(request.head().text, "hello");
            Ok(gdl::Choice::new(instances.get(0).expect("one")))
        }
    }

    #[tokio::test]
    async fn a_selector_reads_the_head_in_its_edges_encoding() {
        let ids = ids(1);
        let crossing = crossing(
            vec![cell("m", CellState::Ready, &ids)],
            Within::Policy(Policy::RoundRobin),
        );
        let selector = gdl::__runtime::Typed(Arc::new(Reading));
        let head = serde_json::to_vec(&Prompt {
            text: "hello".into(),
        })
        .expect("encoded");
        let (chosen, ..) = crossing
            .choose(&selector, &Cx::new(), &head, gdl_model::Encoding::Json, &[])
            .await
            .expect("the selector read a JSON head")
            .expect("a cell");
        assert_eq!(chosen, ids[0]);
    }

    #[tokio::test]
    async fn a_choice_still_in_its_cells_newer_table_is_kept() {
        let ids = ids(2);
        let first = cell("m", CellState::Ready, &ids);
        let crossing = crossing(vec![first], Within::Policy(Policy::RoundRobin));
        let table = crossing.target.table.load_full();
        let chosen = &table.cells[0];
        // An event made the cell's table again, with the same instances.
        let newer = EntryTable::new(Box::new([cell("m", CellState::Ready, &ids)]));
        assert!(still_eligible(&newer, chosen, ids[0], |_| true).is_some());
        let fewer = EntryTable::new(Box::new([cell("m", CellState::Ready, &ids[1..])]));
        assert!(still_eligible(&fewer, chosen, ids[0], |_| true).is_none());
        let gone = EntryTable::new(Box::new([]));
        assert!(still_eligible(&gone, chosen, ids[1], |_| true).is_none());
    }

    /// What a chooser's boundary pick costs over a pick in its own roster,
    /// over the same instances, what of it is the read of the clock that
    /// judges each summary's age and each cell's damping, and what is the
    /// lease on the cell's follow, taken and let go, and a pick with the
    /// acceptance that follows it: reported, not gated. Run it with
    /// `--ignored --nocapture`.
    #[test]
    #[ignore = "a measurement, not a check: run it with --ignored --nocapture"]
    fn the_cost_of_a_boundary_pick() {
        let cx = Cx::new();
        const PICKS: u32 = 2_000_000;
        let ids = ids(4);
        let roster = crate::dist::Roster::new();
        let live = roster.live();
        for id in &ids {
            live.join("f", *id);
        }
        let crossing = crossing(
            vec![
                cell("m1", CellState::Ready, &ids[..2]),
                cell("m2", CellState::Ready, &ids[2..]),
            ],
            Within::Policy(Policy::RoundRobin),
        );
        let random = RandomState::new();
        let time = |pick: &dyn Fn() -> InstanceId| -> Duration {
            let start = Instant::now();
            for _ in 0..PICKS {
                std::hint::black_box(pick());
            }
            start.elapsed() / PICKS
        };
        for round in 0..3 {
            let roster = time(&|| {
                live.pick("f", Picker::Request, Policy::RoundRobin, &random)
                    .expect("one")
            });
            let boundary = time(&|| {
                crossing
                    .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                    .expect("a pick")
                    .expect("one")
                    .0
            });
            let clock = time(&|| {
                std::hint::black_box(tokio::time::Instant::now());
                ids[0]
            });
            let leases = Arc::clone(&crossing.target.table.load().cells[0].leases);
            let lease = time(&|| {
                drop(std::hint::black_box(CellLease::take(&leases)));
                ids[0]
            });
            let accepted = time(&|| {
                let (id, cell, _) = crossing
                    .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                    .expect("a pick")
                    .expect("one");
                crossing.accepted(cell);
                id
            });
            println!(
                "round {round}: roster pick {roster:?}, boundary pick {boundary:?}, of which the clock {clock:?} and the lease {lease:?}; pick and acceptance {accepted:?}"
            );
        }
    }
}
