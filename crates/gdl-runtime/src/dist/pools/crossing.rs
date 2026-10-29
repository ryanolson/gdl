//! A chooser's hand-off into another class: for each attempt, a pool of
//! the class by its pool policy, then an instance of that pool's entry
//! segment by the hand-off's own router. The chosen pool is a local of
//! the hand-off, never on the wire and never kept per request.

use std::collections::BTreeMap;
use std::hash::RandomState;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use futures::future::BoxFuture;
use gdl::__runtime::{ChoiceHeld, Erased, ErasedPolicy, Picked};
use gdl::{
    Cx, Damped, Error, ErrorKind, HomeFirstAt, Instances, NoPool, PoolPolicies, PoolRequest, Result,
};
use gdl_model::{Encoding, Layout};
use velo::InstanceId;

use super::damping::Damping;
use super::lease::PoolLease;
use super::policy;
use super::{EntryTable, PoolTable, Pools};
use crate::dist::offers::Decline;
use crate::dist::roster::{CHOICES, Policy, turn};
use crate::dist::runtime::Within;

/// One hand-off into another class, as a chooser sends it.
pub(crate) struct Crossing {
    target: Target,
    within: Within,
    /// The encoding of the head that crosses, which a pool policy that
    /// reads it reads it in.
    encoding: Encoding,
    /// The attempts' turns for the pool policy, and the draws hashed from
    /// them for a policy that draws at random, apart from the turns of the
    /// instance within the pool: with one sequence and one hasher for
    /// both, the two choices of an attempt would be correlated.
    draws: AtomicUsize,
    random: RandomState,
}

impl Crossing {
    /// The hand-off into `target` of a head in `encoding`, choosing the
    /// instance within the chosen pool as `within` chooses.
    pub(crate) fn new(target: Target, within: Within, encoding: Encoding) -> Self {
        Self {
            target,
            within,
            encoding,
            draws: AtomicUsize::new(0),
            random: RandomState::new(),
        }
    }

    /// How the instance within the chosen pool is chosen.
    pub(crate) fn within(&self) -> &Within {
        &self.within
    }

    /// Choose a pool for one attempt of the request with context `cx`
    /// and head `head`, then an instance of it by `policy`, with the
    /// chosen pool's [`id`](PoolTable::id) and a lease on its follow,
    /// which holds the pool policy's lease on the choice, if it gave one, or
    /// [`NoPool`], with the policy's reason if it gave one, when no pool
    /// but those in `excluded` may take it: one load
    /// of the table, a read of the clock, two counter increments, the pool
    /// policy's read of the head and its passes over the followed pools,
    /// and the lease, with no lock and no allocation of the runtime's. A
    /// pool whose follow became not offered since the table was loaded
    /// refuses the lease, and the table, which then no longer offers it,
    /// is loaded again. Fails when the pool policy cannot read the head.
    pub(crate) fn pick(
        &self,
        cx: &Cx,
        head: &[u8],
        policy: Policy,
        random: &RandomState,
        excluded: &[u64],
    ) -> Result<Result<(InstanceId, u64, PoolLease), NoPool>> {
        for _ in 0..CHOICES {
            let table = self.target.table.load();
            let (pool, choice) = match self.pool(&table, cx, head, excluded)? {
                Ok(chosen) => chosen,
                Err(no) => return Ok(Err(no)),
            };
            let Some(lease) = PoolLease::take(&pool.leases) else {
                continue;
            };
            let sequence = pool.turn.fetch_add(1, Ordering::Relaxed);
            return Ok(Ok((
                pool.eligible[turn(policy, sequence, pool.eligible.len(), random)],
                pool.id,
                lease.with_choice(choice),
            )));
        }
        Ok(Err(NoPool::new()))
    }

    /// Choose a pool for one attempt, then ask `selector` to choose among
    /// that pool's eligible entry instances alone; [`NoPool`] when no pool
    /// but those in `excluded` may take it. A selector may wait, so the choice
    /// is checked when it returns: if the instance is no
    /// longer eligible in the pool's current table, or the pool itself no
    /// longer is by the pool policy (it stopped taking work, or, under
    /// `home_first`, its summary went or went stale), or
    /// its follow refuses the lease because it is no longer offered, the
    /// choice and its lease are dropped and both the pool and the instance
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
    ) -> BoxFuture<'a, Result<Result<Chosen, NoPool>>> {
        Box::pin(async move {
            for _ in 0..CHOICES {
                let table = self.target.table.load_full();
                let (pool, choice) = match self.pool(&table, cx, head, excluded)? {
                    Ok(chosen) => chosen,
                    Err(no) => return Ok(Err(no)),
                };
                let pool = Arc::clone(pool);
                let picked = selector
                    .select(
                        cx,
                        head,
                        encoding,
                        // The pools failover chooses another pool, so no
                        // instance of this one has declined the request.
                        &[],
                        Instances::new(&self.target.segment, &pool.offered),
                    )
                    .await?;
                // A request that ended while its selector chose gets no
                // instance; its lease goes with `picked`. The caller's
                // select sees the end only at its next await, and the
                // send can leave before that.
                cx.check()?;
                let chosen = pool.eligible[picked.index];
                let target = &self.target;
                let now = tokio::time::Instant::now();
                let policy =
                    |current: &gdl::Pool| target.policy.eligible(current, &target.region, now);
                let current = target.table.load();
                let lease = still_eligible(&current, &pool, chosen, policy)
                    .and_then(|now| PoolLease::take(&now.leases));
                if let Some(lease) = lease {
                    return Ok(Ok((chosen, pool.id, picked, lease.with_choice(choice))));
                }
            }
            Err(Error::routing(format!(
                "the pools of class `{}` changed under the selector of segment `{}` {CHOICES} times",
                self.target.class, self.target.segment
            )))
        })
    }

    /// The request's `Routing` error when no pool of the class may take
    /// it (`no`): a request never waits for a pool to appear. A policy's
    /// reason follows its name, the class, and the segment; it replaces
    /// the claim that no pool is ready, since a policy may refuse while
    /// every pool is. The reason is copied into the message, which is
    /// built here in any case: a reason costs the policy's own allocation,
    /// if it made one, and its bytes in the message.
    pub(crate) fn no_pool(&self, no: NoPool) -> Error {
        let target = &self.target;
        match no.reason() {
            None => Error::routing(format!(
                "no ready pool of class `{}` has an instance of segment `{}`",
                target.class, target.segment
            )),
            Some(reason) => Error::routing(format!(
                "pool policy `{}` of class `{}` chose no pool for segment `{}`: {reason}",
                target.name, target.class, target.segment
            )),
        }
    }

    /// The class this hand-off enters.
    pub(crate) fn class(&self) -> &str {
        &self.target.class
    }

    /// The pool `pool` declined an offer, made under `lease`, for
    /// `decline`: the chooser damps it, or, when it said it is gone,
    /// passes it over until its follow changes. An instance's own decline
    /// changes neither. Gone holds the follow as
    /// the pool's current table shows it, and only while that table is of
    /// the follow the offer was made under: a decline that lands after a
    /// newer generation replaced it says nothing of the newer one, which
    /// is damped instead, as for a full pool. A full decline leaves a pool
    /// that is gone in its current follow gone. Read only on a decline.
    pub(crate) fn declined(&self, pool: u64, decline: Decline, lease: Option<&PoolLease>) {
        // An instance's own decline says nothing of its pool's room or
        // state: damping the pool would turn away requests its other
        // instances serve.
        if decline == Decline::Instance {
            return;
        }
        let damping = &self.target.damping;
        let now = tokio::time::Instant::now();
        let table = self.target.table.load();
        let current = table
            .pools
            .iter()
            .zip(&table.views)
            .find(|(shown, _)| shown.id == pool);
        match current {
            Some((shown, view))
                if decline == Decline::Gone && lease.is_some_and(|lease| lease.is_of(shown)) =>
            {
                damping.gone(view);
            }
            Some((_, view)) => damping.decline_shown(view, now),
            None => damping.decline(pool, now),
        }
    }

    /// The pool policy's [`NoPool`] for a request whose every pool in
    /// `offered` the chooser holds gone, when the policy chooses no other
    /// pool for it: what the requests after it meet, since no choice
    /// takes a gone pool. `None` when a pool in `offered` is not gone or
    /// the policy chooses a pool. The marks and the choice read one load
    /// of the table and one of the damping, so that they are of one
    /// moment. Read only once a request can be offered no more pools.
    /// Fails when the pool policy cannot read the head.
    pub(crate) fn gone_only(
        &self,
        cx: &Cx,
        head: &[u8],
        offered: &[u64],
    ) -> Result<Option<NoPool>> {
        let table = self.target.table.load();
        let damped = self.target.damping.view();
        let gone = offered.iter().all(|id| {
            table
                .pools
                .iter()
                .zip(&table.views)
                .find(|(shown, _)| shown.id == *id)
                .is_some_and(|(_, view)| damped.is_gone(view))
        });
        if !gone {
            return Ok(None);
        }
        Ok(self.pool_by(&table, &damped, cx, head, offered)?.err())
    }

    /// The pool `pool` accepted an offer: its damping, as the chooser has
    /// it now, ends, whatever it was when the offer was chosen: another
    /// request's decline may have damped it since, or another acceptance
    /// ended it. A pool that is not damped writes nothing, and costs one
    /// load and a search of the damped pools, with no allocation.
    pub(crate) fn accepted(&self, pool: u64) {
        self.target.damping.accept(pool);
    }

    /// The walk over the other entry instances of the pool `pool`, after
    /// `declined`, one of them, declined a request as its own: from the
    /// instance after it, in the order of the pool's current table, to
    /// the one before it. Every instance of that table when `declined` is
    /// no longer in it. `None` when the chooser no longer follows the
    /// pool. Made only once an instance declined: one load of the table,
    /// a search of its pools and of the pool's instances, and a clone of
    /// an `Arc`.
    pub(crate) fn walk(&self, pool: u64, declined: InstanceId) -> Option<Walk> {
        let table = self.target.table.load();
        let shown = table.pools.iter().find(|shown| shown.id == pool)?;
        let count = shown.eligible.len();
        let (at, left) = match shown.eligible.iter().position(|id| *id == declined) {
            Some(at) => (at, count - 1),
            None => (count.saturating_sub(1), count),
        };
        Some(Walk {
            pool: Arc::clone(shown),
            at,
            left,
        })
    }

    /// The next instance of `walk` that the pool's current table still
    /// offers, and its pool policy still takes, with the pool's
    /// [`id`](PoolTable::id) and a lease on its follow that holds
    /// `choice`, the pool policy's lease on the choice of the pool, if it
    /// gave one. `None` once the walk has offered every instance: the
    /// request then leaves the pool. The pool policy is not asked again:
    /// the decline was the instance's, and the pool is still its choice.
    pub(crate) fn pick_in(
        &self,
        walk: &mut Walk,
        choice: &mut Option<ChoiceHeld>,
    ) -> Option<(InstanceId, u64, PoolLease)> {
        while let Some(chosen) = walk.next() {
            if let Some(lease) = self.lease_in(walk, chosen) {
                return Some((chosen, walk.pool.id, lease.with_choice(choice.take())));
            }
        }
        None
    }

    /// [`Crossing::pick_in`], asking `selector` for each instance, with
    /// that instance alone to choose from, so that it holds its lease and
    /// gives its meta for the instance the request is offered to. A
    /// selector may wait, so the instance is checked again when it
    /// returns. A request that ended while its selector chose is not
    /// handed on.
    pub(crate) async fn choose_in(
        &self,
        walk: &mut Walk,
        selector: &dyn Erased,
        cx: &Cx,
        head: &[u8],
        encoding: gdl_model::Encoding,
        choice: &mut Option<ChoiceHeld>,
    ) -> Result<Option<Chosen>> {
        while let Some(chosen) = walk.next() {
            if self.lease_in(walk, chosen).is_none() {
                continue;
            }
            let alone = &walk.pool.offered[walk.at..=walk.at];
            let picked = selector
                .select(
                    cx,
                    head,
                    encoding,
                    &[],
                    Instances::new(&self.target.segment, alone),
                )
                .await?;
            cx.check()?;
            if let Some(lease) = self.lease_in(walk, chosen) {
                return Ok(Some((
                    chosen,
                    walk.pool.id,
                    picked,
                    lease.with_choice(choice.take()),
                )));
            }
        }
        Ok(None)
    }

    /// A lease on the follow of `walk`'s pool, if its current table still
    /// offers `chosen` and the pool policy still takes the pool.
    fn lease_in(&self, walk: &Walk, chosen: InstanceId) -> Option<PoolLease> {
        let target = &self.target;
        let now = tokio::time::Instant::now();
        let policy = |current: &gdl::Pool| target.policy.eligible(current, &target.region, now);
        let current = target.table.load();
        still_eligible(&current, &walk.pool, chosen, policy)
            .and_then(|now| PoolLease::take(&now.leases))
    }

    /// The pool the policy chooses from `table` for one attempt of the
    /// request with context `cx` and head `head`, of those not in
    /// `excluded`, by the summaries' ages now, with the policy's lease on
    /// the choice, if it gave one, or the policy's [`NoPool`]: one relaxed
    /// increment of the attempts' count, which a policy that draws at
    /// random hashes when it asks. Fails when the policy cannot read the
    /// head.
    fn pool<'t>(
        &self,
        table: &'t EntryTable,
        cx: &Cx,
        head: &[u8],
        excluded: &[u64],
    ) -> Result<Result<Pooled<'t>, NoPool>> {
        self.pool_by(table, &self.target.damping.view(), cx, head, excluded)
    }

    /// [`Crossing::pool`], with the chooser's damping as `damped` holds it.
    fn pool_by<'t>(
        &self,
        table: &'t EntryTable,
        damped: &Damped,
        cx: &Cx,
        head: &[u8],
        excluded: &[u64],
    ) -> Result<Result<Pooled<'t>, NoPool>> {
        let target = &self.target;
        let turn = self.draws.fetch_add(1, Ordering::Relaxed) as u64;
        let request = PoolRequest::new(cx, &(), &target.region, tokio::time::Instant::now())
            .with_bytes(head)
            .with_excluded(excluded)
            .with_damped(damped)
            .with_turn(turn)
            .with_random(&self.random)
            .with_models(&table.models);
        let (at, choice) = match target.policy.choose(request, self.encoding, &table.views)? {
            Ok(choice) => choice.into_parts(),
            Err(no) => return Ok(Err(no)),
        };
        // A policy names a pool by its place in the views it was given,
        // which are the table's, index for index; a place past them is
        // no pool.
        Ok(table
            .pools
            .get(at)
            .map(|pool| (pool, choice))
            .ok_or_else(NoPool::new))
    }

    /// Refuse a pool policy that reads the head as a type the crossing's
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
                    "segment `{segment}`: pool policy `{}` of class `{}` {why}",
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

/// The pool's current table in `table`, which may be newer, if `chosen`,
/// an instance of `pool`, is still eligible in it: the pool's current
/// table, of whatever generation, is `pool` itself or still offers it, and
/// `policy` still takes it, as it sees it. The lease is taken from it,
/// since it is the follow the offer is made under.
fn still_eligible<'t>(
    table: &'t EntryTable,
    pool: &Arc<PoolTable>,
    chosen: InstanceId,
    policy: impl Fn(&gdl::Pool) -> bool,
) -> Option<&'t Arc<PoolTable>> {
    table
        .pools
        .iter()
        .zip(&table.views)
        .find(|(now, _)| now.class == pool.class && now.pool == pool.pool)
        .filter(|(now, view)| {
            (Arc::ptr_eq(now, pool) || now.eligible.contains(&chosen)) && policy(view)
        })
        .map(|(now, _)| now)
}

/// A selector's choice within a pool: the instance, the pool's
/// [`id`](PoolTable::id), what the selector picked, and the lease on the
/// pool's follow.
pub(crate) type Chosen = (InstanceId, u64, Picked, PoolLease);

/// The entry instances of one pool that a request is offered to in turn
/// after one of them declined it as its own ([`Decline::Instance`]): the
/// pool's table when the decline landed, the place in its instances of
/// the last one offered, and how many are left. A counter over the
/// table's order rather than a set of the instances tried: it is three
/// words for a pool of any size, it offers each instance of the table at
/// most once, and it allocates nothing. An instance that joined the pool
/// after the decline is not offered the request.
pub(crate) struct Walk {
    pool: Arc<PoolTable>,
    at: usize,
    left: usize,
}

impl Walk {
    /// The next instance to offer, or `None` once every one was.
    fn next(&mut self) -> Option<InstanceId> {
        self.left = self.left.checked_sub(1)?;
        self.at = (self.at + 1) % self.pool.eligible.len();
        Some(self.pool.eligible[self.at])
    }
}

/// A pool policy's choice: the chosen pool's table, and the policy's lease
/// on the choice, if it gave one.
type Pooled<'t> = (&'t Arc<PoolTable>, Option<ChoiceHeld>);

/// What a hand-off into another class chooses from: the class, its entry
/// segment, the chooser's table of that segment's pools, the chooser's
/// region, the class's pool policy and its name, and the pools the
/// chooser damps.
#[derive(Clone)]
pub(crate) struct Target {
    class: String,
    segment: String,
    table: Arc<ArcSwap<EntryTable>>,
    region: Arc<str>,
    policy: Arc<dyn ErasedPolicy>,
    name: Arc<str>,
    /// The pools the chooser damps, shared by all its hand-offs.
    damping: Arc<Damping>,
}

/// The hand-offs of `segment` that cross into another class, by hop and
/// forward position, each with what it chooses from: those whose hop's
/// outer end is in `segment` and whose entry segment `pools` keeps a
/// table for. Found once, when the runtime is built, so that a request
/// pays one branch on what was found. A pool policy that `policies` does
/// not register and gdl does not have is refused. A class that names
/// `home_first` chooses by `home_first`, the chooser's configured one.
pub(crate) fn targets(
    layout: &Layout,
    segment: &str,
    pools: &Pools,
    policies: &PoolPolicies,
    home_first: HomeFirstAt,
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
        let Some(table) = pools.slot(&inner.segment) else {
            continue;
        };
        let (name, policy) = policy::of(layout, &boundary.into, policies, home_first)?;
        targets.insert(
            (boundary.hop, inner.start),
            Target {
                class: boundary.into.clone(),
                segment: inner.segment.clone(),
                table,
                region: pools.region(),
                policy,
                name,
                damping: pools.damping(),
            },
        );
    }
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    use gdl_pools::PoolState;

    use super::*;
    use crate::dist::roster::Picker;

    fn pool(name: &str, state: PoolState, eligible: &[InstanceId]) -> Arc<PoolTable> {
        pool_turning(name, state, eligible, Arc::default())
    }

    /// A table of `name` that takes its turns from `turn`, as each table
    /// of one follow does.
    fn pool_turning(
        name: &str,
        state: PoolState,
        eligible: &[InstanceId],
        turn: Arc<AtomicUsize>,
    ) -> Arc<PoolTable> {
        Arc::new(PoolTable {
            class: "model_x".into(),
            pool: name.into(),
            id: super::super::pool_id("model_x", name),
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
            changes: 0,
            leases: Arc::default(),
        })
    }

    fn crossing(pools: Vec<Arc<PoolTable>>, within: Within) -> Crossing {
        let table = Arc::new(ArcSwap::from_pointee(EntryTable::new(
            pools.into_boxed_slice(),
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

    impl gdl::PoolPolicy for ByName {
        type Head = gdl::Projected<Hinted>;

        fn choose(
            &self,
            _: PoolRequest<'_, gdl::Projected<Hinted>>,
            _: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            Err(NoPool::new())
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
    fn round_robin_takes_turns_within_a_pool_across_its_tables() {
        let cx = Cx::new();
        let ids = ids(2);
        let first = pool("m", PoolState::Ready, &ids);
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
        let again = pool_turning("m", PoolState::Ready, &ids, Arc::clone(&first.turn));
        crossing
            .target
            .table
            .store(Arc::new(EntryTable::new(Box::new([again]))));
        assert_eq!(pick(), ids[1], "a new table reset the turn");
    }

    #[test]
    fn no_pool_to_choose_is_a_routing_error_naming_the_class() {
        let cx = Cx::new();
        let crossing = crossing(
            vec![pool("m", PoolState::Unavailable, &[])],
            Within::Policy(Policy::RoundRobin),
        );
        let no = crossing
            .pick(&cx, &[], Policy::RoundRobin, &RandomState::new(), &[])
            .expect("a pick")
            .expect_err("no pool");
        assert_eq!(no, gdl::NoPool::new());
        let error = crossing.no_pool(no);
        assert_eq!(error.kind(), gdl::ErrorKind::Routing);
        assert_eq!(
            error.to_string(),
            "routing error: no ready pool of class `model_x` has an instance of segment `f`"
        );
    }

    /// Refuses every request, naming why.
    struct Refusing;

    impl gdl::PoolPolicy for Refusing {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            _: PoolRequest<'_, gdl::Encoded>,
            _: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, gdl::NoPool> {
            Err(gdl::NoPool::because("no set serves model `mistral`"))
        }
    }

    /// A policy's reason for choosing no pool reaches the request's
    /// `Routing` error, by a pick and by a selector's choice alike, after
    /// the policy, the class, and the segment, and with no claim that no
    /// pool is ready: every pool here is.
    #[tokio::test]
    async fn a_policy_reason_for_no_pool_reaches_the_error() {
        let ids = ids(1);
        let mut crossing = crossing(
            vec![pool("m", PoolState::Ready, &ids)],
            Within::Policy(Policy::RoundRobin),
        );
        crossing.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(Refusing)));
        crossing.target.name = Arc::from("refusing");
        let cx = Cx::new();
        let picked = crossing
            .pick(&cx, &[], Policy::RoundRobin, &RandomState::new(), &[])
            .expect("a pick")
            .expect_err("no pool");
        let selector = gdl::__runtime::Typed(Arc::new(gdl::LeastLoaded::new()));
        let Err(chosen) = crossing
            .choose(&selector, &cx, &[], Default::default(), &[])
            .await
            .expect("a choice")
        else {
            panic!("a pool");
        };
        for no in [picked, chosen] {
            assert_eq!(no.reason(), Some("no set serves model `mistral`"));
            let error = crossing.no_pool(no);
            assert_eq!(error.kind(), gdl::ErrorKind::Routing);
            assert_eq!(
                error.to_string(),
                "routing error: pool policy `refusing` of class `model_x` chose no pool for segment `f`: \
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

    impl gdl::PoolPolicy for Reasoning {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            _: PoolRequest<'_, gdl::Encoded>,
            _: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            Err(match self {
                Self::None => NoPool::new(),
                Self::Fixed => NoPool::because("no set serves the model"),
                Self::Built(model) => NoPool::because(format!("no set serves model `{model}`")),
            })
        }
    }

    /// What choosing no pool costs, in time, for the record in the book: a
    /// pick that the policy refuses and the request's error made from it,
    /// with no reason, a fixed one, and one built for the request. Ignored:
    /// it times, and asserts nothing. Run it in release:
    /// `cargo test --release -p gdl-runtime --lib cost_of_no_pool -- --ignored --nocapture`.
    #[test]
    #[ignore = "a timing, not a check"]
    fn cost_of_no_pool_and_its_reason() {
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
                vec![pool("m", PoolState::Ready, &ids)],
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
                    let Err(no) = picked else { panic!("a pool") };
                    std::hint::black_box(crossing.no_pool(no));
                }
                best = best.min(start.elapsed());
            }
            println!(
                "no pool, {name}: {:.1} ns",
                best.as_nanos() as f64 / f64::from(PICKS)
            );
        }
    }

    /// A pool the request excluded is passed over, by the policy and by a
    /// selector alike, and with every pool excluded there is none.
    #[tokio::test]
    async fn an_excluded_pool_is_passed_over() {
        let ids = ids(2);
        let crossing = crossing(
            vec![
                pool("m1", PoolState::Ready, &ids[..1]),
                pool("m2", PoolState::Degraded, &ids[1..]),
            ],
            Within::Policy(Policy::RoundRobin),
        );
        let random = RandomState::new();
        let cx = Cx::new();
        let m1 = super::super::pool_id("model_x", "m1");
        let m2 = super::super::pool_id("model_x", "m2");
        let pick = |excluded: &[u64]| {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, excluded)
                .expect("a pick")
                .map(|(id, pool, _)| (id, pool))
        };
        assert_eq!(pick(&[]), Ok((ids[0], m1)));
        assert_eq!(pick(&[m1]), Ok((ids[1], m2)));
        assert_eq!(pick(&[m1, m2]), Err(gdl::NoPool::new()));
        let selector = gdl::__runtime::Typed(Arc::new(gdl::LeastLoaded::new()));
        let cx = Cx::new();
        let chosen = crossing
            .choose(&selector, &cx, &[], Default::default(), &[m1])
            .await
            .expect("a choice")
            .map(|(id, pool, _, _)| (id, pool));
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
    async fn a_choice_that_left_its_pool_while_the_selector_waited_is_made_again() {
        let ids = ids(2);
        let waiting = Arc::new(Waiting {
            target: ids[0],
            calls: AtomicUsize::new(0),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            leases: Arc::new(AtomicUsize::new(0)),
        });
        let crossing = Arc::new(crossing(
            vec![pool("m", PoolState::Ready, &ids)],
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
        // While the selector waits, its choice leaves the pool's table.
        crossing
            .target
            .table
            .store(Arc::new(EntryTable::new(Box::new([pool(
                "m",
                PoolState::Ready,
                &ids[1..],
            )]))));
        waiting.release.notify_one();
        let (chosen, lease) = tokio::time::timeout(WAIT, choosing)
            .await
            .expect("chose in time")
            .expect("joined")
            .expect("no error")
            .expect("a choice");
        assert_eq!(chosen, ids[1], "sent to an instance its pool no longer has");
        assert_eq!(waiting.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            waiting.leases.load(Ordering::SeqCst),
            1,
            "the stale choice's lease was kept"
        );
        drop(lease);
        assert_eq!(waiting.leases.load(Ordering::SeqCst), 0);
    }

    /// A selector that waits, over a crossing into the one pool `m`, whose
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
            vec![pool("m", PoolState::Ready, &ids)],
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
                    .map(|chosen| chosen.ok().map(|(id, pool, _, _)| (id, pool)))
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

    /// A pool whose summary is gone while the selector waited is not sent
    /// to, though it still has the chosen instance: with no other pool,
    /// there is no choice, and the request fails with `Routing`.
    #[tokio::test]
    async fn a_pool_whose_summary_went_while_the_selector_waited_is_not_sent_to() {
        let chosen = chosen_after(async |crossing: &Crossing, ids: &[InstanceId]| {
            let mut unknown = Arc::try_unwrap(pool("m", PoolState::Ready, ids)).expect("one owner");
            unknown.load = None;
            crossing
                .target
                .table
                .store(Arc::new(EntryTable::new(Box::new([Arc::new(unknown)]))));
        })
        .await;
        assert_eq!(chosen, None, "sent to a pool with no summary");
    }

    /// A pool whose summary went stale while the selector waited, on the
    /// chooser's clock, is not sent to either, though its table is the one
    /// it was chosen from.
    #[tokio::test(start_paused = true)]
    async fn a_pool_whose_summary_went_stale_while_the_selector_waited_is_not_sent_to() {
        let chosen = chosen_after(async |_: &Crossing, _: &[InstanceId]| {
            // The summary's period is 60 s; it is stale after three.
            tokio::time::advance(Duration::from_secs(181)).await;
        })
        .await;
        assert_eq!(chosen, None, "sent to a pool whose summary is stale");
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
            vec![pool("m", PoolState::Ready, &ids)],
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
            crossing.target.table.load().pools[0].leases.held(),
            0,
            "the lease on the pool's follow was kept"
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
            vec![pool("m", PoolState::Ready, &ids)],
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
            .expect("a pool");
        assert_eq!(chosen, ids[0]);
    }

    /// Chooses the first pool the model index lists `qwen-32b` in, as
    /// offered: what a policy that routes by model does.
    struct ByModel;

    impl gdl::PoolPolicy for ByModel {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            request: PoolRequest<'_, gdl::Encoded>,
            pools: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            let sets = request
                .models()
                .sets("qwen-32b", None)
                .ok_or_else(|| NoPool::because("no pool lists qwen-32b"))?;
            pools
                .iter()
                .position(|pool| {
                    sets.iter()
                        .any(|set| set.is_offered() && set.pool() == pool.name())
                })
                .map(gdl::PoolChoice::new)
                .ok_or_else(|| NoPool::because("no offered pool serves qwen-32b"))
        }
    }

    /// A pool policy reads the class's model index from the table it
    /// chooses from, as a node does.
    #[tokio::test]
    async fn a_policy_reads_the_model_index_of_its_table() {
        let ids = ids(2);
        let mut crossing = crossing(
            vec![
                pool("m1", PoolState::Ready, &ids[..1]),
                pool("m2", PoolState::Ready, &ids[1..]),
            ],
            Within::Policy(Policy::RoundRobin),
        );
        crossing.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(ByModel)));
        let set = |pool: &str| {
            gdl::ModelSet::new(
                pool,
                "home",
                gdl::ModelState::Ready,
                gdl::PoolCondition::Live,
            )
            .with_offered(true)
        };
        let index = gdl::ModelIndex::new(true, [(gdl::ModelKey::new("qwen-32b"), set("m2"))]);
        let table = crossing.target.table.load_full();
        crossing.target.table.store(Arc::new(
            EntryTable::new(table.pools.clone()).with_models(Arc::new(index)),
        ));
        let selector = gdl::__runtime::Typed(Arc::new(Reading));
        let head = serde_json::to_vec(&Prompt {
            text: "hello".into(),
        })
        .expect("encoded");
        let (chosen, ..) = crossing
            .choose(&selector, &Cx::new(), &head, gdl_model::Encoding::Json, &[])
            .await
            .expect("chosen")
            .expect("a pool");
        assert_eq!(chosen, ids[1], "the pool the index lists");
    }

    #[tokio::test]
    async fn a_choice_still_in_its_pools_newer_table_is_kept() {
        let ids = ids(2);
        let first = pool("m", PoolState::Ready, &ids);
        let crossing = crossing(vec![first], Within::Policy(Policy::RoundRobin));
        let table = crossing.target.table.load_full();
        let chosen = &table.pools[0];
        // An event made the pool's table again, with the same instances.
        let newer = EntryTable::new(Box::new([pool("m", PoolState::Ready, &ids)]));
        assert!(still_eligible(&newer, chosen, ids[0], |_| true).is_some());
        let fewer = EntryTable::new(Box::new([pool("m", PoolState::Ready, &ids[1..])]));
        assert!(still_eligible(&fewer, chosen, ids[0], |_| true).is_none());
        let gone = EntryTable::new(Box::new([]));
        assert!(still_eligible(&gone, chosen, ids[1], |_| true).is_none());
    }

    /// What a chooser's boundary pick costs over a pick in its own roster,
    /// over the same instances, what of it is the read of the clock that
    /// judges each summary's age and each pool's damping, and what is the
    /// lease on the pool's follow, taken and let go, and a pick with the
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
                pool("m1", PoolState::Ready, &ids[..2]),
                pool("m2", PoolState::Ready, &ids[2..]),
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
            let leases = Arc::clone(&crossing.target.table.load().pools[0].leases);
            let lease = time(&|| {
                drop(std::hint::black_box(PoolLease::take(&leases)));
                ids[0]
            });
            let accepted = time(&|| {
                let (id, pool, _) = crossing
                    .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                    .expect("a pick")
                    .expect("one");
                crossing.accepted(pool);
                id
            });
            println!(
                "round {round}: roster pick {roster:?}, boundary pick {boundary:?}, of which the clock {clock:?} and the lease {lease:?}; pick and acceptance {accepted:?}"
            );
        }
    }
}
