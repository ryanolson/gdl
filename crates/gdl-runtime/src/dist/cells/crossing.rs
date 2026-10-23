//! A chooser's hand-off into another class: for each attempt, a cell of
//! the class by its cell policy, then an instance of that cell's entry
//! segment by the hand-off's own router. The chosen cell is a local of
//! the hand-off, never on the wire and never kept per request.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use futures::future::BoxFuture;
use gdl::__runtime::{Erased, Picked};
use gdl::{Cx, Error, Instances, Result};
use gdl_model::Layout;
use velo::InstanceId;

use super::policy::{self, CellChooser};
use super::{CellTable, Cells, EntryTable};
use crate::dist::roster::{CHOICES, Policy, turn};
use crate::dist::runtime::Router;

/// One hand-off into another class, as a chooser sends it.
pub(crate) struct Crossing {
    target: Target,
    within: Within,
    /// The attempts' draws for the cell policy, apart from the turns of
    /// the instance within the cell: with one sequence and one hasher for
    /// both, the two choices of an attempt would be correlated.
    draws: AtomicUsize,
    random: RandomState,
}

impl Crossing {
    /// The hand-off into `target`, choosing the instance within the
    /// chosen cell as `router` chooses.
    pub(crate) fn new(target: Target, router: Router) -> Self {
        let within = match router {
            Router::Policy(policy) => Within::Policy(policy),
            Router::Selector { name, selector } => Within::Selector { name, selector },
            Router::Cells(crossing) => crossing.within.clone(),
        };
        Self {
            target,
            within,
            draws: AtomicUsize::new(0),
            random: RandomState::new(),
        }
    }

    /// How the instance within the chosen cell is chosen.
    pub(crate) fn within(&self) -> &Within {
        &self.within
    }

    /// Choose a cell for one attempt, then an instance of it by `policy`:
    /// one load of the table, two counter increments, and the policy's
    /// pass over the followed cells, with no lock and no allocation.
    pub(crate) fn pick(&self, policy: Policy, random: &RandomState) -> Result<InstanceId> {
        let table = self.target.table.load();
        let cell = self.cell(&table)?;
        let sequence = cell.turn.fetch_add(1, Ordering::Relaxed);
        Ok(cell.eligible[turn(policy, sequence, cell.eligible.len(), random)])
    }

    /// Choose a cell for one attempt, then ask `selector` to choose among
    /// that cell's eligible entry instances alone. A selector may wait, so
    /// the choice is checked when it returns (ruling 154): if the cell's
    /// table changed meanwhile and the instance is no longer eligible in
    /// the cell's current table, the choice and its lease are dropped and
    /// both the cell and the instance are chosen again. Boxed, as
    /// [`Live::choose`](crate::dist::roster::Live::choose) is.
    pub(crate) fn choose<'a>(
        &'a self,
        selector: &'a dyn Erased,
        cx: &'a Cx,
        head: &'a [u8],
    ) -> BoxFuture<'a, Result<(InstanceId, Picked)>> {
        Box::pin(async move {
            for _ in 0..CHOICES {
                let table = self.target.table.load_full();
                let cell = Arc::clone(self.cell(&table)?);
                let picked = selector
                    .select(
                        cx,
                        head,
                        Instances::new(&self.target.segment, &cell.offered),
                    )
                    .await?;
                let chosen = cell.eligible[picked.index];
                if still_eligible(&self.target.table.load(), &cell, chosen) {
                    return Ok((chosen, picked));
                }
            }
            Err(Error::routing(format!(
                "the cells of class `{}` changed under the selector of segment `{}` {CHOICES} times",
                self.target.class, self.target.segment
            )))
        })
    }

    /// The cell the policy chooses from `table` for one attempt, or the
    /// request's `Routing` error when none may take it: a request never
    /// waits for a cell to appear.
    fn cell<'t>(&self, table: &'t EntryTable) -> Result<&'t Arc<CellTable>> {
        let draw = self
            .random
            .hash_one(self.draws.fetch_add(1, Ordering::Relaxed));
        let target = &self.target;
        target
            .policy
            .choose(&table.cells, &target.region, draw)
            .map(|at| &table.cells[at])
            .ok_or_else(|| {
                Error::routing(format!(
                    "no ready cell of class `{}` has an instance of segment `{}`",
                    target.class, target.segment
                ))
            })
    }
}

/// Whether `chosen`, an instance of `cell`, is still eligible in `table`,
/// which may be newer: `cell` itself is still current, or the cell's
/// current table, of whatever generation, still offers it.
fn still_eligible(table: &EntryTable, cell: &Arc<CellTable>, chosen: InstanceId) -> bool {
    table
        .cells
        .iter()
        .find(|now| now.class == cell.class && now.cell == cell.cell)
        .is_some_and(|now| Arc::ptr_eq(now, cell) || now.eligible.contains(&chosen))
}

/// How the instance within the chosen cell is chosen: the hand-off's own
/// router.
#[derive(Clone)]
pub(crate) enum Within {
    /// `round_robin` or `random`, with a turn per entry segment and cell.
    Policy(Policy),
    /// A selector, by the name the placement gives it.
    Selector {
        name: Arc<str>,
        selector: Arc<dyn Erased>,
    },
}

/// What a hand-off into another class chooses from: the class, its entry
/// segment, the chooser's table of that segment's cells, the chooser's
/// region, and the class's cell policy.
#[derive(Clone)]
pub(crate) struct Target {
    class: String,
    segment: String,
    table: Arc<ArcSwap<EntryTable>>,
    region: Arc<str>,
    policy: &'static dyn CellChooser,
}

/// The hand-offs of `segment` that cross into another class, by hop and
/// forward position, each with what it chooses from: those whose hop's
/// outer end is in `segment` and whose entry segment `cells` keeps a
/// table for. Found once, when the runtime is built, so that a request
/// pays one branch on what was found. A cell policy the runtime does not
/// provide is refused.
pub(crate) fn targets(
    layout: &Layout,
    segment: &str,
    cells: &Cells,
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
        targets.insert(
            (boundary.hop, inner.start),
            Target {
                class: boundary.into.clone(),
                segment: inner.segment.clone(),
                table,
                region: cells.region(),
                policy: policy::of(layout, &boundary.into)?,
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
            generation: 1,
            region: "home".into(),
            state,
            eligible: eligible.into(),
            offered: eligible
                .iter()
                .map(|id| gdl::InstanceId::from_u128(id.as_u128()))
                .collect(),
            retired: Box::new([]),
            turn,
        })
    }

    fn crossing(cells: Vec<Arc<CellTable>>, router: Router) -> Crossing {
        let table = Arc::new(ArcSwap::from_pointee(EntryTable {
            cells: cells.into_boxed_slice(),
        }));
        Crossing::new(
            Target {
                class: "model_x".into(),
                segment: "f".into(),
                table,
                region: Arc::from("home"),
                policy: &policy::HomeFirst,
            },
            router,
        )
    }

    /// The longest a test waits for something it expects.
    const WAIT: Duration = Duration::from_secs(5);

    fn ids(n: usize) -> Vec<InstanceId> {
        (0..n).map(|_| InstanceId::new_v4()).collect()
    }

    #[test]
    fn round_robin_takes_turns_within_a_cell_across_its_tables() {
        let ids = ids(2);
        let first = cell("m", CellState::Ready, &ids);
        let crossing = crossing(vec![Arc::clone(&first)], Router::Policy(Policy::RoundRobin));
        let random = RandomState::new();
        let pick = || crossing.pick(Policy::RoundRobin, &random).expect("one");
        assert_eq!([pick(), pick(), pick()], [ids[0], ids[1], ids[0]]);
        // The follower makes a new table for an event, carrying the turn.
        let again = cell_turning("m", CellState::Ready, &ids, Arc::clone(&first.turn));
        crossing.target.table.store(Arc::new(EntryTable {
            cells: Box::new([again]),
        }));
        assert_eq!(pick(), ids[1], "a new table reset the turn");
    }

    #[test]
    fn no_cell_to_choose_is_a_routing_error_naming_the_class() {
        let crossing = crossing(
            vec![cell("m", CellState::Unavailable, &[])],
            Router::Policy(Policy::RoundRobin),
        );
        let error = crossing
            .pick(Policy::RoundRobin, &RandomState::new())
            .expect_err("nothing to choose");
        assert_eq!(error.kind(), gdl::ErrorKind::Routing);
        assert!(error.to_string().contains("`model_x`"), "{error}");
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
            Router::Policy(Policy::RoundRobin),
        ));
        let choosing = tokio::spawn({
            let crossing = Arc::clone(&crossing);
            let waiting = Arc::clone(&waiting);
            async move {
                let selector = gdl::__runtime::Typed(waiting);
                let cx = Cx::new();
                crossing
                    .choose(&selector, &cx, &[])
                    .await
                    .map(|(id, picked)| (id, picked.lease))
            }
        });
        tokio::time::timeout(WAIT, waiting.arrived.notified())
            .await
            .expect("the selector was asked in time");
        // While the selector waits, its choice leaves the cell's table.
        crossing.target.table.store(Arc::new(EntryTable {
            cells: Box::new([cell("m", CellState::Ready, &ids[1..])]),
        }));
        waiting.release.notify_one();
        let (chosen, lease) = tokio::time::timeout(WAIT, choosing)
            .await
            .expect("chose in time")
            .expect("joined")
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

    #[tokio::test]
    async fn a_choice_still_in_its_cells_newer_table_is_kept() {
        let ids = ids(2);
        let first = cell("m", CellState::Ready, &ids);
        let crossing = crossing(vec![first], Router::Policy(Policy::RoundRobin));
        let table = crossing.target.table.load_full();
        let chosen = &table.cells[0];
        // An event made the cell's table again, with the same instances.
        let newer = EntryTable {
            cells: Box::new([cell("m", CellState::Ready, &ids)]),
        };
        assert!(still_eligible(&newer, chosen, ids[0]));
        let fewer = EntryTable {
            cells: Box::new([cell("m", CellState::Ready, &ids[1..])]),
        };
        assert!(!still_eligible(&fewer, chosen, ids[0]));
        let gone = EntryTable {
            cells: Box::new([]),
        };
        assert!(!still_eligible(&gone, chosen, ids[1]));
    }

    /// What a chooser's boundary pick costs over a pick in its own roster,
    /// over the same instances: reported, not gated. Run it with
    /// `--ignored --nocapture`.
    #[test]
    #[ignore = "a measurement, not a check: run it with --ignored --nocapture"]
    fn the_cost_of_a_boundary_pick() {
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
            Router::Policy(Policy::RoundRobin),
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
            let boundary = time(&|| crossing.pick(Policy::RoundRobin, &random).expect("one"));
            println!("round {round}: roster pick {roster:?}, boundary pick {boundary:?}");
        }
    }
}
