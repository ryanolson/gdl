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
    Cx, Damped, Error, ErrorKind, HomeFirstAt, NoPool, OfferView, PoolPolicies, PoolRequest, Result,
};
use gdl_model::{Encoding, Layout};
use velo::InstanceId;

use super::damping::Damping;
use super::lease::{PoolLease, Seen, Wake};
use super::meter::PoolFigures;
use super::policy;
use super::{EntryTable, PoolTable, Pools};
use crate::dist::roster::{CHOICES, Policy, selector_id, turn};
use crate::dist::runtime::Within;
use gdl::Decline;

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
    /// but those `left` holds may take it: one load
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
        left: Left<'_>,
        view: &mut Option<OfferView>,
    ) -> Result<Result<(InstanceId, u64, PoolLease), NoPool>> {
        for _ in 0..CHOICES {
            let table = self.target.table.load();
            let (pool, choice, wants_view, chosen_view) = match self.pool(&table, cx, head, left)? {
                Ok(chosen) => chosen,
                Err(no) => return Ok(Err(no)),
            };
            let Some(lease) = PoolLease::take(&pool.leases) else {
                continue;
            };
            let sequence = pool.turn.fetch_add(1, Ordering::Relaxed);
            *view = chosen_view;
            return Ok(Ok((
                pool.eligible[turn(policy, sequence, pool.eligible.len(), random)],
                pool.id,
                lease.with_choice(choice).with_wants_view(wants_view),
            )));
        }
        Ok(Err(NoPool::new()))
    }

    /// Choose a pool for one attempt, then ask `selector` to choose among
    /// that pool's eligible entry instances alone, with the view to put on
    /// the offer in `view`; [`NoPool`] when no pool
    /// but those `left` holds may take it. A selector may wait, so the choice
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
        left: Left<'a>,
        view: &'a mut Option<OfferView>,
    ) -> BoxFuture<'a, Result<Result<Chosen, NoPool>>> {
        Box::pin(async move {
            for _ in 0..CHOICES {
                let table = self.target.table.load_full();
                // The view is the row's the policy chose from, taken
                // before the selector's wait, under which the table can
                // change: the offer says what the choice was made on.
                let (pool, choice, wants_view, chosen_view) =
                    match self.pool(&table, cx, head, left)? {
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
                        pool.instances(&self.target.segment),
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
                    *view = chosen_view;
                    return Ok(Ok((
                        chosen,
                        pool.id,
                        picked,
                        lease.with_choice(choice).with_wants_view(wants_view),
                    )));
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
    /// passes it over until its follow changes. An instance's own decline,
    /// and a decline as not present, change neither. Gone holds the follow as
    /// the pool's current table shows it, and only while that table is of
    /// the follow the offer was made under: a decline that lands after a
    /// newer generation replaced it says nothing of the newer one, which
    /// is damped instead, as for a full pool. A full decline leaves a pool
    /// that is gone in its current follow gone. Read only on a decline.
    pub(crate) fn declined(&self, pool: u64, decline: Decline, lease: Option<&PoolLease>) {
        // An instance's own decline says nothing of its pool's room or
        // state: damping the pool would turn away requests its other
        // instances serve. Not present says only that the pool lacks this
        // request's property: damping it would turn away the requests for
        // the properties it has.
        if matches!(decline, Decline::Instance | Decline::NotPresent) {
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

    /// The pool policy's [`NoPool`] for a request whose every pool it
    /// `left` the chooser holds gone, when the policy chooses no other
    /// pool for it: what the requests after it meet, since no choice
    /// takes a gone pool. `None` when a pool it left is not gone or
    /// the policy chooses a pool. The marks and the choice read one load
    /// of the table and one of the damping, so that they are of one
    /// moment. Read only once a request can be offered no more pools.
    /// Fails when the pool policy cannot read the head.
    pub(crate) fn gone_only(&self, cx: &Cx, head: &[u8], left: Left<'_>) -> Result<Option<NoPool>> {
        let table = self.target.table.load();
        let damped = self.target.damping.view();
        let gone = left.excluded.iter().all(|id| {
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
        Ok(self.pool_by(&table, &damped, cx, head, left)?.err())
    }

    /// Whether the pool policy re-queues the request with context `cx` and
    /// head `head` for `pool`, which declined it as full, of the pools it
    /// `left` ([`gdl::PoolPolicy::requeue`]). `false` when the chooser no
    /// longer follows the pool. Asked only on a full decline: one load of
    /// the table, a search of its pools, and the policy's read of the
    /// head. Fails when the pool policy cannot read the head.
    pub(crate) fn requeue(&self, cx: &Cx, head: &[u8], left: Left<'_>, pool: u64) -> Result<bool> {
        let target = &self.target;
        let table = target.table.load();
        let Some(view) = table.views.iter().find(|view| view.id() == pool) else {
            return Ok(false);
        };
        let damped = target.damping.view();
        let request = PoolRequest::new(cx, &(), &target.region, tokio::time::Instant::now())
            .with_bytes(head)
            .with_excluded(left.excluded)
            .with_declines(left.declines)
            .with_damped(&damped);
        let request = table.fill(request);
        target.policy.requeue(request, self.encoding, view)
    }

    /// Wait for room in `pool`, which a request is re-queued for and saw
    /// as `seen` before its last offer: until an offer this chooser made
    /// to the pool ends, unless one ended since, for at most `backoff`, and
    /// never past `deadline` ([`Seen::room`]). Returns at once when the
    /// chooser no longer follows the pool, or follows it under another
    /// follow than the offer's. Holds no lease on the pool's follow, so
    /// that a drain does not wait for it. The wake that ended the wait, if
    /// one did, is the caller's to use or pass on ([`Wake`]).
    pub(crate) async fn room(
        &self,
        pool: u64,
        seen: &Seen,
        backoff: std::time::Duration,
        deadline: Option<tokio::time::Instant>,
    ) -> Option<Wake> {
        let leases = {
            let table = self.target.table.load();
            match table.pools.iter().find(|shown| shown.id == pool) {
                Some(shown) => Arc::clone(&shown.leases),
                None => return None,
            }
        };
        Some(seen.room(&leases, backoff, deadline).await)
    }

    /// The counts of `pool`'s offers, if the chooser records them and its
    /// current table shows the pool: one load of the table and a search
    /// of its pools, with no lock and no allocation.
    pub(crate) fn figures(&self, pool: u64) -> Option<&'static PoolFigures> {
        let table = self.target.table.load();
        let shown = table.pools.iter().find(|shown| shown.id == pool)?;
        shown.leases.figures()
    }

    /// An instance of `pool`, which a request is re-queued for, by
    /// `policy`, with a lease on the pool's follow that holds `choice`, the
    /// pool policy's lease on the choice of the pool: from the pool's
    /// current table, while the pool policy still takes the pool
    /// ([`gdl::PoolPolicy::eligible`]) and its follow is offered. `None`
    /// once it is not, and the request leaves the pool. The pool policy is
    /// not asked to choose: it chose the pool. When the offer carries a
    /// view (`carried` is `Some`), it becomes the row's, from the same table,
    /// so that a re-offer says the summary it is made on. One load of the
    /// table, a search of its pools, the clock, and the lease, with no
    /// allocation.
    pub(crate) fn pick_again(
        &self,
        pool: u64,
        policy: Policy,
        random: &RandomState,
        choice: &mut Option<ChoiceHeld>,
        carried: Option<&mut Option<OfferView>>,
    ) -> Option<(InstanceId, PoolLease)> {
        let target = &self.target;
        let table = target.table.load();
        let (shown, view) = table
            .pools
            .iter()
            .zip(&table.views)
            .find(|(shown, _)| shown.id == pool)?;
        let now = tokio::time::Instant::now();
        if shown.eligible.is_empty() || !target.policy.eligible(view, &target.region, now) {
            return None;
        }
        let lease = PoolLease::take(&shown.leases)?;
        let sequence = shown.turn.fetch_add(1, Ordering::Relaxed);
        let chosen = shown.eligible[turn(policy, sequence, shown.eligible.len(), random)];
        let wants_view = carried.is_some();
        if let Some(carried) = carried {
            *carried = shown.offer_view(table.fill_max_ppm);
        }
        Some((
            chosen,
            lease.with_choice(choice.take()).with_wants_view(wants_view),
        ))
    }

    /// [`Crossing::pick_again`], asking `selector` to choose among the
    /// pool's eligible entry instances. A selector may wait, so the choice
    /// is checked when it returns, as [`Crossing::choose`] checks its own.
    /// A request that ended while its selector chose is not handed on.
    /// When the pool's instances changed under every one of its choices,
    /// the pool still takes work, so the request stays with it
    /// ([`Again::Stale`]): it is not a reason to leave a pool the policy
    /// re-queued the request for.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn choose_again(
        &self,
        pool: u64,
        selector: &dyn Erased,
        cx: &Cx,
        head: &[u8],
        encoding: Encoding,
        choice: &mut Option<ChoiceHeld>,
        mut carried: Option<&mut Option<OfferView>>,
    ) -> Result<Again<(InstanceId, Picked, PoolLease)>> {
        let target = &self.target;
        let policy = |current: &gdl::Pool| {
            target
                .policy
                .eligible(current, &target.region, tokio::time::Instant::now())
        };
        for _ in 0..CHOICES {
            let table = target.table.load_full();
            let Some((shown, view)) = table
                .pools
                .iter()
                .zip(&table.views)
                .find(|(shown, _)| shown.id == pool)
            else {
                return Ok(Again::Unavailable);
            };
            if shown.offered.is_empty() || !policy(view) {
                return Ok(Again::Unavailable);
            }
            // The view of the row this round chooses from, taken before the
            // selector's wait, as a first choice's is.
            let shown_view = carried
                .is_some()
                .then(|| shown.offer_view(table.fill_max_ppm))
                .flatten();
            let wants_view = carried.is_some();
            let shown = Arc::clone(shown);
            let picked = selector
                .select(cx, head, encoding, &[], shown.instances(&target.segment))
                .await?;
            cx.check()?;
            let chosen = shown.eligible[picked.index];
            let current = target.table.load();
            let lease = still_eligible(&current, &shown, chosen, policy)
                .and_then(|now| PoolLease::take(&now.leases));
            if let Some(lease) = lease {
                if let Some(carried) = carried.as_deref_mut() {
                    *carried = shown_view;
                }
                return Ok(Again::Chosen((
                    chosen,
                    picked,
                    lease.with_choice(choice.take()).with_wants_view(wants_view),
                )));
            }
        }
        Ok(Again::Stale)
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
    /// `declined`, one of them, declined a request as its own: each of
    /// them once, from the pool's current table, chosen by the crossing's
    /// router. Every instance of that table when `declined` is no longer
    /// in it. `None` when the chooser no longer follows the pool. Made
    /// only once an instance declined: one load of the table, a search of
    /// its pools and of the pool's instances, and a clone of an `Arc`.
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
            first: declined,
            declined: Vec::new(),
            wants_view: false,
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
                return Some((
                    chosen,
                    walk.pool.id,
                    lease
                        .with_choice(choice.take())
                        .with_wants_view(walk.wants_view),
                ));
            }
        }
        None
    }

    /// [`Crossing::pick_in`], asking `selector` to choose among every
    /// instance of the walk's table, told which instances declined the
    /// request: the one whose decline began the walk, and each the walk
    /// offered it to since, or found gone from the pool's current table.
    /// So a selector ranks the whole pool on each choice, as within a
    /// class, and `affinity` with a session's cutoff offers the request
    /// inside the cutoff before outside it ([`gdl::Affinity`]). Each
    /// choice uses one of the walk's offers, whichever instance it names,
    /// so the walk ends after as many offers as the pool has other
    /// instances. The list of declines is the walk's one allocation, made
    /// at its first choice. A selector may wait, so the instance is
    /// checked again when it returns. A request that ended while its
    /// selector chose is not handed on.
    pub(crate) async fn choose_in(
        &self,
        walk: &mut Walk,
        selector: &dyn Erased,
        cx: &Cx,
        head: &[u8],
        encoding: gdl_model::Encoding,
        choice: &mut Option<ChoiceHeld>,
    ) -> Result<Option<Chosen>> {
        if walk.declined.is_empty() {
            walk.declined.reserve_exact(walk.left + 1);
            walk.declined.push(selector_id(walk.first));
        }
        let segment = &self.target.segment;
        while let Some(left) = walk.left.checked_sub(1) {
            walk.left = left;
            let mut picked = selector
                .select(
                    cx,
                    head,
                    encoding,
                    &walk.declined,
                    walk.pool.instances(segment),
                )
                .await?;
            let mut at = picked.index;
            if walk.declined.contains(&walk.pool.offered[at]) {
                // A selector that does not read the declines named one
                // again: the walk offers the next instance in the table's
                // order that it has not, asking about it alone, so that
                // each instance is still offered the request once.
                drop(picked);
                let Some(next) = walk.unoffered() else {
                    break;
                };
                at = next;
                picked = selector
                    .select(
                        cx,
                        head,
                        encoding,
                        &walk.declined,
                        walk.pool.instance(segment, at),
                    )
                    .await?;
            }
            cx.check()?;
            let chosen = walk.pool.eligible[at];
            walk.declined.push(walk.pool.offered[at]);
            if let Some(lease) = self.lease_in(walk, chosen) {
                return Ok(Some((
                    chosen,
                    walk.pool.id,
                    picked,
                    lease
                        .with_choice(choice.take())
                        .with_wants_view(walk.wants_view),
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
    /// request with context `cx` and head `head`, of those not `left`, by
    /// the summaries' ages now, with the policy's lease on
    /// the choice, if it gave one, or the policy's [`NoPool`]: one relaxed
    /// increment of the attempts' count, which a policy that draws at
    /// random hashes when it asks. Fails when the policy cannot read the
    /// head.
    fn pool<'t>(
        &self,
        table: &'t EntryTable,
        cx: &Cx,
        head: &[u8],
        left: Left<'_>,
    ) -> Result<Result<Pooled<'t>, NoPool>> {
        self.pool_by(table, &self.target.damping.view(), cx, head, left)
    }

    /// [`Crossing::pool`], with the chooser's damping as `damped` holds it.
    fn pool_by<'t>(
        &self,
        table: &'t EntryTable,
        damped: &Damped,
        cx: &Cx,
        head: &[u8],
        left: Left<'_>,
    ) -> Result<Result<Pooled<'t>, NoPool>> {
        let target = &self.target;
        let turn = self.draws.fetch_add(1, Ordering::Relaxed) as u64;
        let request = PoolRequest::new(cx, &(), &target.region, tokio::time::Instant::now())
            .with_bytes(head)
            .with_excluded(left.excluded)
            .with_declines(left.declines)
            .with_damped(damped)
            .with_turn(turn)
            .with_random(&self.random);
        let request = table.fill(request);
        let choice = match target.policy.choose(request, self.encoding, &table.views)? {
            Ok(choice) => choice,
            Err(no) => return Ok(Err(no)),
        };
        let wants_view = choice.wants_view();
        let (at, choice) = choice.into_parts();
        // A policy names a pool by its place in the views it was given,
        // which are the table's, index for index; a place past them is
        // no pool.
        Ok(table
            .pools
            .get(at)
            .map(|pool| {
                (
                    pool,
                    choice,
                    wants_view,
                    wants_view
                        .then(|| pool.offer_view(table.fill_max_ppm))
                        .flatten(),
                )
            })
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

/// What the next choice of a re-queued request within its pool came to.
pub(crate) enum Again<T> {
    /// An instance of the pool, and what the offer to it holds.
    Chosen(T),
    /// The pool still takes work, but its entry instances changed under
    /// the selector at each of its choices: the request waits another
    /// round for the pool, and chooses again.
    Stale,
    /// The pool no longer takes work as the pool policy sees it, its
    /// follow is not offered, or the chooser no longer follows it: the
    /// request leaves it.
    Unavailable,
}

/// A selector's choice within a pool: the instance, the pool's
/// [`id`](PoolTable::id), what the selector picked, and the lease on the
/// pool's follow.
pub(crate) type Chosen = (InstanceId, u64, Picked, PoolLease);

/// The entry instances of one pool that a request is offered to in turn
/// after one of them declined it as its own ([`Decline::Instance`]): the
/// pool's table when the decline landed, the place in its instances of
/// the last one offered, and how many offers are left. Under a pool
/// policy, a counter over the table's order rather than a set of the
/// instances tried: it offers each instance of the table at most once,
/// and allocates nothing. Under a selector, the instances that declined,
/// for the selector to rank the pool by ([`Crossing::choose_in`]). An
/// instance that joined the pool after the decline is not offered the
/// request.
pub(crate) struct Walk {
    pool: Arc<PoolTable>,
    at: usize,
    left: usize,
    /// The instance whose decline began the walk.
    first: InstanceId,
    /// Under a selector, `first` and each instance offered the request
    /// since, as the selector reads them: empty under a pool policy.
    declined: Vec<gdl::InstanceId>,
    /// Whether the choice of the pool asked for the view: each instance's
    /// lease keeps it, for a re-queue after its decline.
    pub(crate) wants_view: bool,
}

impl Walk {
    /// The next instance to offer, or `None` once every one was.
    fn next(&mut self) -> Option<InstanceId> {
        self.left = self.left.checked_sub(1)?;
        self.at = (self.at + 1) % self.pool.eligible.len();
        Some(self.pool.eligible[self.at])
    }

    /// Under a selector, the place of the next instance in the table's
    /// order that the walk has not offered the request to, which becomes
    /// the place of the last one offered; `None` once it offered every
    /// one.
    fn unoffered(&mut self) -> Option<usize> {
        let count = self.pool.offered.len();
        let next = (1..=count)
            .map(|step| (self.at + step) % count)
            .find(|&at| !self.declined.contains(&self.pool.offered[at]))?;
        self.at = next;
        Some(next)
    }
}

/// The pools a request left before an attempt, as its pool policy reads
/// them: every pool it was offered to, and why each that declined it did
/// ([`gdl::PoolRequest::declines`]). Two borrowed slices of the failover's
/// own lists, so that the first attempt, which left none, copies nothing.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Left<'a> {
    excluded: &'a [u64],
    declines: &'a [(u64, Decline)],
}

impl<'a> Left<'a> {
    /// No pool left: the first attempt.
    pub(crate) const NONE: Left<'static> = Left {
        excluded: &[],
        declines: &[],
    };

    /// The pools in `excluded` left, of which those in `declines` declined
    /// the request, for the reasons given.
    pub(crate) fn new(excluded: &'a [u64], declines: &'a [(u64, Decline)]) -> Self {
        Self { excluded, declines }
    }

    /// The pools in `excluded` left, with no reason known.
    #[cfg(test)]
    pub(crate) fn excluding(excluded: &'a [u64]) -> Self {
        Self::new(excluded, &[])
    }
}

/// A pool policy's choice: the chosen pool's table, the policy's lease on
/// the choice, if it gave one, whether it asked for the view, and the view
/// to put on the offer, if it asked for one and the table has one.
type Pooled<'t> = (
    &'t Arc<PoolTable>,
    Option<ChoiceHeld>,
    bool,
    Option<OfferView>,
);

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

    use gdl::Instances;
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
            epoch: 0,
            region: "home".into(),
            state,
            load: super::super::tests::idle(),
            eligible: eligible.into(),
            offered: eligible
                .iter()
                .copied()
                .map(crate::dist::roster::selector_id)
                .collect(),
            stamps: gdl::JoinStamps::default(),
            retired: Box::new([]),
            unread: false,
            turn,
            offering: true,
            revision: 1,
            changes: 0,
            leases: Arc::default(),
            properties_version: 0,
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

    struct Replacing {
        table: Arc<ArcSwap<EntryTable>>,
        calls: AtomicUsize,
    }

    impl gdl::Selector for Replacing {
        type Head = gdl::Encoded;

        async fn select<'a>(
            &'a self,
            _: gdl::Request<'a, gdl::Encoded>,
            instances: Instances<'a>,
        ) -> Result<gdl::Choice<'a>> {
            let chosen = instances.get(0).expect("the pool always has an entry");
            if self.calls.fetch_add(1, Ordering::SeqCst) < CHOICES {
                // Each selection spans an entry replacement, with room in the same pool.
                let current = self.table.load();
                let mut next =
                    Arc::try_unwrap(pool("m", PoolState::Ready, &[InstanceId::new_v4()]))
                        .expect("one table owner");
                next.leases = Arc::clone(&current.pools[0].leases);
                next.turn = Arc::clone(&current.pools[0].turn);
                self.table
                    .store(Arc::new(EntryTable::new(Box::new([Arc::new(next)]))));
            }
            Ok(gdl::Choice::new(chosen))
        }
    }

    /// A pool `m` of the instances `eligible`, in this order, with the
    /// join stamps `joined`, of its hub of `epoch`.
    fn stamped(eligible: &[InstanceId], joined: &[u64], epoch: u64) -> Arc<PoolTable> {
        let mut table = pool("m", PoolState::Ready, eligible);
        let held = Arc::get_mut(&mut table).expect("one owner");
        held.stamps = gdl::JoinStamps::new(&held.offered, joined.into(), epoch);
        held.epoch = epoch;
        table
    }

    /// After an entry instance declines a request as its own, the walk
    /// over the pool's other instances asks the selector with every one of
    /// them and the instances that declined, so that `affinity` with a
    /// session's cutoff offers the request to the other instance inside
    /// the cutoff before one that joined after it, though the later one
    /// comes first in the table's order. Only then is it offered outside,
    /// and the walk ends once each instance was offered it.
    #[tokio::test]
    async fn an_instances_decline_goes_to_the_next_instance_inside_the_cutoff() {
        const EPOCH: u64 = 7;
        let mut ids = ids(3);
        ids.sort_by_key(|id| id.as_u128());
        // In table order: the first, joined at 10; one that joined after
        // the cutoff, at 30; and one at 20, inside it.
        let (first, later, inside) = (ids[0], ids[1], ids[2]);
        let crossing = crossing(
            vec![stamped(&ids, &[10, 30, 20], EPOCH)],
            Within::Policy(Policy::RoundRobin),
        );
        let affinity = gdl::Affinity::new(gdl::AffinityConfig {
            key: Some("session".into()),
            through: true,
        });
        let selector = gdl::__runtime::Typed(Arc::new(affinity));
        let id = super::super::pool_id("model_x", "m");
        // A session whose first choice is the first instance.
        let mut found = None;
        for key in 0..256 {
            let mut cx = Cx::new().with_timeout(Duration::from_secs(5));
            cx.insert_meta("session", format!("s-{key}"));
            cx.insert_ext(gdl::JoinedThrough {
                epoch: EPOCH,
                joined: 20,
            });
            let chosen = crossing
                .choose(
                    &selector,
                    &cx,
                    &[],
                    Default::default(),
                    Left::default(),
                    &mut None,
                )
                .await
                .expect("a choice")
                .expect("a pool");
            if chosen.0 == first {
                found = Some(cx);
                break;
            }
        }
        let cx = found.expect("a session on the first instance");
        let mut walk = crossing.walk(id, first).expect("followed");
        let mut offered = Vec::new();
        while let Some((to, ..)) = crossing
            .choose_in(
                &mut walk,
                &selector,
                &cx,
                &[],
                Default::default(),
                &mut None,
            )
            .await
            .expect("a choice")
        {
            offered.push(to);
        }
        assert_eq!(offered, [inside, later]);
    }

    /// A singleton retry still describes a known member of the same pool.
    /// A selector can require that evidence before it uses a cutoff.
    #[tokio::test]
    async fn a_walk_keeps_join_order_when_a_selector_repeats_an_instance() {
        struct Cutoff;

        impl gdl::Selector for Cutoff {
            type Head = gdl::Encoded;

            async fn select<'a>(
                &'a self,
                request: gdl::Request<'a, gdl::Encoded>,
                instances: gdl::Instances<'a>,
            ) -> Result<gdl::Choice<'a>> {
                let cut = request.cx().ext::<gdl::JoinedThrough>().expect("a cutoff");
                if instances.epoch() != cut.epoch
                    || instances.iter().any(|instance| instance.joined().is_none())
                {
                    return Err(Error::routing(
                        "the offered pool member lost its known join order",
                    ));
                }
                let chosen = instances
                    .iter()
                    .find(|instance| instance.joined().is_some_and(|joined| joined <= cut.joined))
                    .ok_or_else(|| Error::routing("no instance inside the cutoff"))?;
                Ok(gdl::Choice::new(chosen))
            }
        }

        let ids = ids(3);
        let crossing = crossing(
            vec![stamped(&ids, &[10, 20, 30], 7)],
            Within::Policy(Policy::RoundRobin),
        );
        let mut cx = Cx::new();
        cx.insert_ext(gdl::JoinedThrough {
            epoch: 7,
            joined: 20,
        });
        let selector = gdl::__runtime::Typed(Arc::new(Cutoff));
        let pool = super::super::pool_id("model_x", "m");
        let first = crossing
            .choose(
                &selector,
                &cx,
                &[],
                Default::default(),
                Left::NONE,
                &mut None,
            )
            .await
            .expect("the full table has known join order")
            .expect("a usable cohort");
        assert_eq!(first.0, ids[0]);
        drop(first);
        let mut walk = crossing.walk(pool, ids[0]).expect("the pool is followed");
        let selected = crossing
            .choose_in(
                &mut walk,
                &selector,
                &cx,
                &[],
                Default::default(),
                &mut None,
            )
            .await
            .expect("the singleton keeps its pool's epoch and join stamp")
            .expect("the second member is inside the cutoff and usable");
        assert_eq!(selected.0, ids[1]);
    }

    #[tokio::test]
    async fn requeue_does_not_leave_a_pool_after_stale_selector_choices() {
        let crossing = crossing(
            vec![pool("m", PoolState::Ready, &ids(1))],
            Within::Policy(Policy::RoundRobin),
        );
        let id = super::super::pool_id("model_x", "m");
        let selector = gdl::__runtime::Typed(Arc::new(Replacing {
            table: Arc::clone(&crossing.target.table),
            calls: AtomicUsize::new(0),
        }));
        let cx = Cx::new().with_timeout(Duration::from_secs(5));
        let result = crossing
            .choose_again(id, &selector, &cx, &[], Default::default(), &mut None, None)
            .await;
        assert!(selector.0.calls.load(Ordering::SeqCst) >= CHOICES);
        assert!(cx.check().is_ok(), "the deadline has not passed");
        let control = crossing
            .choose_again(id, &selector, &cx, &[], Default::default(), &mut None, None)
            .await
            .expect("the selector succeeds once the entries stop changing");
        assert!(
            matches!(control, Again::Chosen(_)),
            "the same pinned pool still takes work"
        );
        assert!(
            matches!(result, Ok(Again::Stale)),
            "a full pinned pool still takes work, but choose_again reported it unavailable for failover"
        );
    }

    /// The control: a follow replaced before the request's wait registers
    /// is seen at once, as another follow than its offer's.
    #[tokio::test(start_paused = true)]
    async fn a_follow_replaced_before_wait_registration_is_seen() {
        assert!(replaced_follow(false).await);
    }

    /// A follow replaced while a request waits on it wakes the request,
    /// which chooses again under the new follow, where a lease ended: no
    /// lease can end on the old one any more.
    #[tokio::test(start_paused = true)]
    async fn a_follow_replaced_after_wait_registration_wakes_the_request() {
        assert!(
            replaced_follow(true).await,
            "the pinned pool has room in its new follow, but the request expires waiting on its previous follow"
        );
    }

    /// A request whose offer under one follow of its pool was declined
    /// waits, registering before the follow is replaced if
    /// `register_before_change`; the follow is replaced, and a lease ends
    /// under the new one. Whether the wait returned with no time passed.
    async fn replaced_follow(register_before_change: bool) -> bool {
        use std::task::{Context, Poll};

        let previous = pool("m", PoolState::Ready, &ids(1));
        let crossing = crossing(
            vec![Arc::clone(&previous)],
            Within::Policy(Policy::RoundRobin),
        );
        let lease = PoolLease::take(&previous.leases).expect("offered");
        let seen = Seen::of(Some(&lease), lease.ends());
        lease.declined();
        let request = Cx::new().with_timeout(Duration::from_millis(20));
        let mut wait = Box::pin(crossing.room(
            previous.id,
            &seen,
            crate::dist::runtime::RuntimeConfig::DEFAULT_POOL_REQUEUE_BACKOFF,
            request.deadline().map(gdl::Deadline::instant),
        ));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        if register_before_change {
            assert!(wait.as_mut().poll(&mut cx).is_pending());
        }

        let mut current = pool("m", PoolState::Ready, &ids(1));
        Arc::get_mut(&mut current).expect("one owner").generation = 2;
        previous.leases.unoffer();
        crossing
            .target
            .table
            .store(Arc::new(EntryTable::new(Box::new([Arc::clone(&current)]))));
        assert!(
            previous.leases.idle(),
            "no more leases can end on the previous follow"
        );
        let (_, held) = crossing
            .pick_again(
                previous.id,
                Policy::RoundRobin,
                &RandomState::new(),
                &mut None,
                None,
            )
            .expect("the same pool still takes work under its new follow");
        drop(held);
        assert_eq!(current.leases.ends(), 1);
        let ready = match wait.as_mut().poll(&mut cx) {
            Poll::Ready(woken) => {
                request.check().expect("the request can offer again");
                drop(woken);
                true
            }
            Poll::Pending => {
                tokio::time::advance(Duration::from_millis(20)).await;
                drop(wait.as_mut().await);
                assert_eq!(
                    request.check().expect_err("expired").kind(),
                    ErrorKind::DeadlineExceeded
                );
                false
            }
        };
        drop(wait);
        ready
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
                .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
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
            .pick(
                &cx,
                &[],
                Policy::RoundRobin,
                &RandomState::new(),
                Left::NONE,
                &mut None,
            )
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
            Err(gdl::NoPool::because("no pool serves shard `a`"))
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
            .pick(
                &cx,
                &[],
                Policy::RoundRobin,
                &RandomState::new(),
                Left::NONE,
                &mut None,
            )
            .expect("a pick")
            .expect_err("no pool");
        let selector = gdl::__runtime::Typed(Arc::new(gdl::LeastLoaded::new()));
        let Err(chosen) = crossing
            .choose(
                &selector,
                &cx,
                &[],
                Default::default(),
                Left::NONE,
                &mut None,
            )
            .await
            .expect("a choice")
        else {
            panic!("a pool");
        };
        for no in [picked, chosen] {
            assert_eq!(no.reason(), Some("no pool serves shard `a`"));
            let error = crossing.no_pool(no);
            assert_eq!(error.kind(), gdl::ErrorKind::Routing);
            assert_eq!(
                error.to_string(),
                "routing error: pool policy `refusing` of class `model_x` chose no pool for segment `f`: \
                 no pool serves shard `a`"
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
                Self::Fixed => NoPool::because("no pool serves the shard"),
                Self::Built(shard) => NoPool::because(format!("no pool serves shard `{shard}`")),
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
            ("a built reason", Reasoning::Built("a")),
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
                        .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
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
                .pick(
                    &cx,
                    &[],
                    Policy::RoundRobin,
                    &random,
                    Left::excluding(excluded),
                    &mut None,
                )
                .expect("a pick")
                .map(|(id, pool, _)| (id, pool))
        };
        assert_eq!(pick(&[]), Ok((ids[0], m1)));
        assert_eq!(pick(&[m1]), Ok((ids[1], m2)));
        assert_eq!(pick(&[m1, m2]), Err(gdl::NoPool::new()));
        let selector = gdl::__runtime::Typed(Arc::new(gdl::LeastLoaded::new()));
        let cx = Cx::new();
        let chosen = crossing
            .choose(
                &selector,
                &cx,
                &[],
                Default::default(),
                Left::excluding(&[m1]),
                &mut None,
            )
            .await
            .expect("a choice")
            .map(|(id, pool, _, _)| (id, pool));
        assert_eq!(chosen, Ok((ids[1], m2)));
        let none = crossing
            .choose(
                &selector,
                &cx,
                &[],
                Default::default(),
                Left::excluding(&[m1, m2]),
                &mut None,
            )
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
                    .choose(
                        &selector,
                        &cx,
                        &[],
                        Default::default(),
                        Left::NONE,
                        &mut None,
                    )
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

    /// A policy that chooses the first pool that takes work, and asks for
    /// the view on its offer.
    struct Viewed;

    impl gdl::PoolPolicy for Viewed {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            _: PoolRequest<'_, gdl::Encoded>,
            pools: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            pools
                .iter()
                .position(gdl::Pool::takes_work)
                .map(|at| gdl::PoolChoice::new(at).with_view())
                .ok_or_else(NoPool::new)
        }
    }

    /// `pool`'s table as a follow of the hub of `epoch` shows it, holding
    /// the summary numbered `sequence`, with the leases of `leases`.
    fn numbered(
        pool: Arc<PoolTable>,
        epoch: u64,
        sequence: u64,
        leases: &Arc<super::super::lease::Leases>,
    ) -> Arc<PoolTable> {
        let mut table = Arc::try_unwrap(pool).expect("one owner");
        table.epoch = epoch;
        table.leases = Arc::clone(leases);
        table.load = Some(gdl::Load::new(
            gdl::LoadSummary {
                limit: 16,
                instances: 1,
                period_ms: 60_000,
                sequence,
                ..Default::default()
            },
            tokio::time::Instant::now(),
        ));
        Arc::new(table)
    }

    /// The offer carries the view of the row its policy chose from, though
    /// a newer summary, or a later hub of the pool, replaced that row while
    /// the selector waited and the chosen instance is still eligible: the
    /// view says what the choice was made on, not what the table holds when
    /// the offer leaves.
    #[tokio::test]
    async fn the_offer_carries_the_view_its_policy_chose_from() {
        for (epoch, sequence) in [(1, 8), (2, 1)] {
            let ids = ids(2);
            let waiting = Arc::new(Waiting {
                target: ids[0],
                calls: AtomicUsize::new(0),
                arrived: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                leases: Arc::new(AtomicUsize::new(0)),
            });
            let leases = Arc::default();
            let mut viewed = crossing(
                vec![numbered(pool("m", PoolState::Ready, &ids), 1, 7, &leases)],
                Within::Policy(Policy::RoundRobin),
            );
            viewed.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(Viewed)));
            viewed.target.name = Arc::from("viewed");
            let viewed = Arc::new(viewed);
            let choosing = tokio::spawn({
                let crossing = Arc::clone(&viewed);
                let waiting = Arc::clone(&waiting);
                async move {
                    let selector = gdl::__runtime::Typed(waiting);
                    let cx = Cx::new();
                    let mut view = None;
                    let chosen = crossing
                        .choose(
                            &selector,
                            &cx,
                            &[],
                            Default::default(),
                            Left::NONE,
                            &mut view,
                        )
                        .await
                        .expect("no error")
                        .ok()
                        .map(|(id, ..)| id);
                    (chosen, view)
                }
            });
            tokio::time::timeout(WAIT, waiting.arrived.notified())
                .await
                .expect("the selector was asked in time");
            viewed
                .target
                .table
                .store(Arc::new(EntryTable::new(Box::new([numbered(
                    pool("m", PoolState::Ready, &ids),
                    epoch,
                    sequence,
                    &leases,
                )]))));
            waiting.release.notify_one();
            let (chosen, view) = tokio::time::timeout(WAIT, choosing)
                .await
                .expect("chose in time")
                .expect("joined");
            assert_eq!(chosen, Some(ids[0]), "still eligible, so not chosen again");
            assert_eq!(
                view,
                Some(gdl::OfferView {
                    epoch: 1,
                    sequence: 7,
                    fill_max_ppm: None,
                }),
                "replaced by ({epoch}, {sequence})"
            );
        }
    }

    /// The fleet's fullest fill an offer carries is the table's own, made
    /// with it from the rows a policy chooses from: the largest fill of the
    /// rows that report capacity above 0 and committed KV. A table with no
    /// such row carries none.
    #[test]
    fn an_offer_carries_its_tables_fullest_fill() {
        let ids = ids(4);
        let cx = Cx::new();
        let random = RandomState::new();
        let leases = Arc::default();
        let with_kv = |name: &str, eligible: &[InstanceId], kv: gdl::KvLoad| {
            let mut table = Arc::try_unwrap(numbered(
                pool(name, PoolState::Ready, eligible),
                1,
                5,
                &leases,
            ))
            .expect("one owner");
            table.load = table.load.map(|load| {
                gdl::Load::new(
                    gdl::LoadSummary {
                        kv,
                        ..*load.summary()
                    },
                    load.arrived(),
                )
            });
            Arc::new(table)
        };
        let kv = |capacity, committed| {
            gdl::KvLoad::NONE
                .with_capacity(capacity)
                .with_committed(committed)
        };
        let rows = vec![
            with_kv("m1", &ids[..1], kv(1_000, 500)),
            with_kv("m2", &ids[1..2], kv(1_000, 900)),
            // No capacity: not counted, though it would be the fullest.
            with_kv("m3", &ids[2..3], kv(0, 5_000)),
            // No committed KV: not counted.
            with_kv("m4", &ids[3..], gdl::KvLoad::NONE.with_capacity(10)),
        ];
        let mut viewed = crossing(rows, Within::Policy(Policy::RoundRobin));
        viewed.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(Viewed)));
        let mut view = None;
        viewed
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut view)
            .expect("no error")
            .expect("a pick");
        assert_eq!(view.and_then(|view| view.fill_max_ppm), Some(900_000));

        let none = vec![with_kv(
            "m4",
            &ids[3..],
            gdl::KvLoad::NONE.with_capacity(10),
        )];
        let mut unknown = crossing(none, Within::Policy(Policy::RoundRobin));
        unknown.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(Viewed)));
        unknown
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut view)
            .expect("no error")
            .expect("a pick");
        let view = view.expect("a numbered row's view");
        assert_eq!(view.fill_max_ppm, None);
        assert_eq!((view.epoch, view.sequence), (1, 5));
    }

    /// A choice whose policy asks for no view puts none on the offer, and
    /// one that asks for it over a row with no numbered summary puts none.
    #[test]
    fn an_offer_carries_a_view_only_when_asked_and_known() {
        let ids = ids(1);
        let cx = Cx::new();
        let random = RandomState::new();
        let leases = Arc::default();
        let numbered_row = numbered(pool("m", PoolState::Ready, &ids), 3, 4, &leases);
        let plain = crossing(
            vec![Arc::clone(&numbered_row)],
            Within::Policy(Policy::RoundRobin),
        );
        let mut view = Some(gdl::OfferView {
            epoch: 9,
            sequence: 9,
            ..Default::default()
        });
        plain
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut view)
            .expect("no error")
            .expect("a pick");
        assert_eq!(view, None, "home_first asks for no view");
        let mut unnumbered = crossing(
            vec![pool("m", PoolState::Ready, &ids)],
            Within::Policy(Policy::RoundRobin),
        );
        unnumbered.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(Viewed)));
        unnumbered
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut view)
            .expect("no error")
            .expect("a pick");
        assert_eq!(view, None, "a summary of an earlier hub has no number");
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
                    .choose(
                        &selector,
                        &cx,
                        &[],
                        Default::default(),
                        Left::NONE,
                        &mut None,
                    )
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
            .choose(
                &selector,
                &cx,
                &[],
                Default::default(),
                Left::NONE,
                &mut None,
            )
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
            .choose(
                &selector,
                &Cx::new(),
                &head,
                gdl_model::Encoding::Json,
                Left::NONE,
                &mut None,
            )
            .await
            .expect("the selector read a JSON head")
            .expect("a pool");
        assert_eq!(chosen, ids[0]);
    }

    /// The test property type.
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
    struct Shard {
        name: String,
    }

    const SHARD: gdl::PropertyKind<Shard> = gdl::PropertyKind::new("shard");

    /// Chooses the first pool the property index lists shard `a` in, as
    /// offered: what a policy that routes by a property does.
    struct ByShard;

    impl gdl::PoolPolicy for ByShard {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            request: PoolRequest<'_, gdl::Encoded>,
            pools: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            let listed = request
                .properties()
                .get(&SHARD)
                .map(|index| index.get(&Shard { name: "a".into() }))
                .filter(|listed| !listed.is_empty())
                .ok_or_else(|| NoPool::because("no pool lists shard `a`"))?;
            pools
                .iter()
                .position(|pool| {
                    listed
                        .iter()
                        .any(|(_, listing)| listing.is_offered() && listing.pool() == pool.name())
                })
                .map(gdl::PoolChoice::new)
                .ok_or_else(|| NoPool::because("no offered pool serves shard `a`"))
        }
    }

    /// A pool policy reads the class's property indexes from the table it
    /// chooses from, as a node does.
    #[tokio::test]
    async fn a_policy_reads_the_property_indexes_of_its_table() {
        let ids = ids(2);
        let mut crossing = crossing(
            vec![
                pool("m1", PoolState::Ready, &ids[..1]),
                pool("m2", PoolState::Ready, &ids[1..]),
            ],
            Within::Policy(Policy::RoundRobin),
        );
        crossing.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(ByShard)));
        let listing = |pool: &str| {
            gdl::PropertyPool::new(
                pool,
                "home",
                gdl::PropertyState::Ready,
                gdl::PoolCondition::Live,
            )
            .with_offered(true)
        };
        let index = gdl::PropertyIndexes::new(true).with(
            &SHARD,
            gdl::PropertyIndex::new([(Shard { name: "a".into() }, listing("m2"))]),
        );
        let table = crossing.target.table.load_full();
        crossing.target.table.store(Arc::new(
            EntryTable::new(table.pools.clone()).with_properties(Arc::new(index)),
        ));
        let selector = gdl::__runtime::Typed(Arc::new(Reading));
        let head = serde_json::to_vec(&Prompt {
            text: "hello".into(),
        })
        .expect("encoded");
        let (chosen, ..) = crossing
            .choose(
                &selector,
                &Cx::new(),
                &head,
                gdl_model::Encoding::Json,
                Left::NONE,
                &mut None,
            )
            .await
            .expect("chosen")
            .expect("a pool");
        assert_eq!(chosen, ids[1], "the pool the index lists");
    }

    /// A live pool, as its hub of `epoch` says.
    fn live(epoch: u64) -> gdl::PoolEvidence {
        gdl::PoolEvidence::new(epoch, gdl::PoolCondition::Live)
    }

    /// `pool`, with its property-list version at `tag`: a mark that tells
    /// one test table from another, not a drain's order.
    fn tagged(name: &str, state: PoolState, eligible: &[InstanceId], tag: u64) -> Arc<PoolTable> {
        let mut table = Arc::try_unwrap(pool(name, state, eligible)).expect("one table owner");
        table.properties_version = tag;
        Arc::new(table)
    }

    /// Keeps a request with its pin, `m1`, until the chooser's hub lists
    /// `m1` as draining, then moves it to `m2` if `m2` takes work, as the
    /// endpoints' set router moves a pin. Before it reads, it stores the
    /// next of `newer`, if any: a table made while it chooses. It records
    /// the tag of the rows it chose from and whether `m1` drained, for
    /// each choice and each re-queue it is asked.
    /// The tag of a read's rows, whether `m1` withdrew, and `m1`'s and
    /// `m3`'s epochs.
    type Evidence = (u64, bool, Option<u64>, Option<u64>);

    struct Pinned {
        table: Arc<ArcSwap<EntryTable>>,
        newer: std::sync::Mutex<std::collections::VecDeque<EntryTable>>,
        seen: std::sync::Mutex<Vec<(u64, bool)>>,
        /// What each read saw: `m3` is listed, with no row.
        evidence: std::sync::Mutex<Vec<Evidence>>,
    }

    impl Pinned {
        fn new(
            table: &Arc<ArcSwap<EntryTable>>,
            newer: impl IntoIterator<Item = EntryTable>,
        ) -> Self {
            Self {
                table: Arc::clone(table),
                newer: std::sync::Mutex::new(newer.into_iter().collect()),
                seen: std::sync::Mutex::default(),
                evidence: std::sync::Mutex::default(),
            }
        }

        /// Store the next newer table, then record what `request` reads
        /// with the rows `tag` names.
        fn read(&self, request: &PoolRequest<'_, gdl::Encoded>, tag: u64) -> bool {
            if let Some(newer) = self.newer.lock().expect("unpoisoned").pop_front() {
                self.table.store(Arc::new(newer));
            }
            let draining = request.is_draining("m1");
            self.seen.lock().expect("unpoisoned").push((tag, draining));
            self.evidence.lock().expect("unpoisoned").push((
                tag,
                request.is_withdrawn("m1"),
                request.epoch("m1"),
                request.epoch("m3"),
            ));
            draining
        }

        fn evidence(&self) -> Vec<Evidence> {
            self.evidence.lock().expect("unpoisoned").clone()
        }

        fn seen(&self) -> Vec<(u64, bool)> {
            self.seen.lock().expect("unpoisoned").clone()
        }
    }

    impl gdl::PoolPolicy for Pinned {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            request: PoolRequest<'_, gdl::Encoded>,
            pools: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            let at = |name: &str| pools.iter().position(|pool| pool.name() == name);
            if !self.read(&request, pools[0].properties_version()) {
                return at("m1").map(gdl::PoolChoice::new).ok_or_else(NoPool::new);
            }
            at("m2")
                .filter(|&m2| pools[m2].takes_work())
                .map(gdl::PoolChoice::new)
                .ok_or_else(|| NoPool::because("the pin drains, and no other set takes work"))
        }

        fn requeue(&self, request: PoolRequest<'_, gdl::Encoded>, pool: &gdl::Pool) -> bool {
            !self.read(&request, pool.properties_version())
        }
    }

    /// A pinning policy behind `table`'s crossing, which stores `newer`
    /// one table per read.
    fn pinned(tables: impl IntoIterator<Item = EntryTable>) -> (Crossing, Arc<Pinned>) {
        let mut tables = tables.into_iter();
        let first = tables.next().expect("a first table");
        let mut crossing = crossing(Vec::new(), Within::Policy(Policy::RoundRobin));
        crossing.target.table.store(Arc::new(first));
        let policy = Arc::new(Pinned::new(&crossing.target.table, tables));
        crossing.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::clone(&policy)));
        crossing.target.name = Arc::from("pinned");
        (crossing, policy)
    }

    /// A drain that lands while a policy chooses is not seen by that
    /// choice, whose rows are older, and is seen by the next, with the
    /// rows of its own table. Read from a newer table than the rows, the
    /// drain would move the pin to `m2`, which neither table allows: the
    /// first keeps the pin, and in the second `m2` takes no work.
    #[test]
    fn a_drain_after_the_table_was_loaded_waits_for_the_next_choice() {
        let ids = ids(2);
        let before = EntryTable::new(Box::new([
            tagged("m1", PoolState::Ready, &ids[..1], 1),
            tagged("m2", PoolState::Ready, &ids[1..], 1),
        ]));
        let drained = EntryTable::new(Box::new([
            tagged("m1", PoolState::Draining, &ids[..1], 2),
            tagged("m2", PoolState::Unavailable, &ids[1..], 2),
        ]))
        .with_evidence(Arc::from([("m1".into(), live(0).with_draining(true))]));
        let (crossing, policy) = pinned([before, drained]);
        let cx = Cx::new();
        let random = RandomState::new();
        let pick = || crossing.pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None);
        let (_, first, _) = pick().expect("a pick").expect("a pool");
        assert_eq!(
            first,
            super::super::pool_id("model_x", "m1"),
            "the pin holds"
        );
        assert_eq!(
            pick().expect("a pick").err(),
            Some(NoPool::because(
                "the pin drains, and no other set takes work"
            )),
            "the next choice sees the drain, with its own table's rows"
        );
        assert_eq!(policy.seen(), [(1, false), (2, true)]);
    }

    /// Whether a pool drains and the rows a policy reads are always one
    /// table's, through each table the chooser stores, for a choice and
    /// for a re-queue alike. Each table here drains `m1` when its tag is
    /// odd, and a new one is stored while each read is under way.
    #[test]
    fn draining_and_the_rows_are_read_from_one_table() {
        const TABLES: u64 = 8;
        let ids = ids(2);
        let table = |tag: u64| {
            let made = EntryTable::new(Box::new([
                tagged("m1", PoolState::Ready, &ids[..1], tag),
                tagged("m2", PoolState::Ready, &ids[1..], tag),
            ]));
            if tag % 2 == 1 {
                made.with_evidence(Arc::from([("m1".into(), live(0).with_draining(true))]))
            } else {
                made
            }
        };
        let (crossing, policy) = pinned((0..TABLES).map(table));
        let cx = Cx::new();
        let random = RandomState::new();
        let m1 = super::super::pool_id("model_x", "m1");
        for tag in 0..TABLES {
            if tag % 3 == 2 {
                let requeued = crossing.requeue(&cx, &[], Left::NONE, m1).expect("asked");
                assert_eq!(requeued, tag % 2 == 0, "tag {tag}");
            } else {
                let (_, chosen, _) = crossing
                    .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                    .expect("a pick")
                    .expect("a pool");
                let pin = if tag % 2 == 0 { "m1" } else { "m2" };
                assert_eq!(chosen, super::super::pool_id("model_x", pin), "tag {tag}");
            }
        }
        let seen = policy.seen();
        let one_table: Vec<(u64, bool)> = (0..TABLES).map(|tag| (tag, tag % 2 == 1)).collect();
        assert_eq!(seen, one_table);
    }

    /// Whether a pool withdrew and each pool's epoch are read from the
    /// same table as the rows, through each table the chooser stores, for
    /// a choice and for a re-queue alike: both paths that build a request.
    /// Each table here withdraws `m1` when its tag is odd, and numbers
    /// `m1` and `m3`, which is listed with no row, by its tag; a new one is
    /// stored while each read is under way.
    #[test]
    fn withdrawals_epochs_and_the_rows_are_read_from_one_table() {
        const TABLES: u64 = 8;
        let ids = ids(2);
        let table = |tag: u64| {
            EntryTable::new(Box::new([
                tagged("m1", PoolState::Ready, &ids[..1], tag),
                tagged("m2", PoolState::Ready, &ids[1..], tag),
            ]))
            .with_evidence(Arc::from([
                ("m1".into(), live(tag).with_withdrawn(tag % 2 == 1)),
                ("m2".into(), live(1)),
                ("m3".into(), live(100 + tag)),
            ]))
        };
        let (crossing, policy) = pinned((0..TABLES).map(table));
        let cx = Cx::new();
        let random = RandomState::new();
        let m1 = super::super::pool_id("model_x", "m1");
        for tag in 0..TABLES {
            if tag % 3 == 2 {
                crossing.requeue(&cx, &[], Left::NONE, m1).expect("asked");
            } else {
                crossing
                    .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                    .expect("a pick")
                    .expect("a pool");
            }
        }
        let one_table: Vec<_> = (0..TABLES)
            .map(|tag| (tag, tag % 2 == 1, Some(tag), Some(100 + tag)))
            .collect();
        assert_eq!(policy.evidence(), one_table);
    }

    /// Records the epochs its leases are told the offer was accepted at.
    struct Accepting(Arc<std::sync::Mutex<Vec<u64>>>);

    impl gdl::ChoiceLease for Accepting {
        fn accepted(&mut self) {
            unreachable!("the runtime tells the epoch");
        }

        fn accepted_at(&mut self, epoch: u64) {
            self.0.lock().expect("unpoisoned").push(epoch);
        }
    }

    /// Chooses the first pool, with a lease that records its acceptance.
    struct FirstWithLease(Arc<std::sync::Mutex<Vec<u64>>>);

    impl gdl::PoolPolicy for FirstWithLease {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            _: PoolRequest<'_, gdl::Encoded>,
            _: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            Ok(gdl::PoolChoice::new(0).with_lease(Accepting(Arc::clone(&self.0))))
        }
    }

    /// Chooses the first instance, and, while it chooses, stores `next`
    /// once, if given: the pool followed again, its hub restarted, with
    /// the same instance.
    struct Restarting {
        table: Arc<ArcSwap<EntryTable>>,
        next: std::sync::Mutex<Option<EntryTable>>,
    }

    impl gdl::Selector for Restarting {
        type Head = gdl::Encoded;

        async fn select<'a>(
            &'a self,
            _: gdl::Request<'a, gdl::Encoded>,
            instances: Instances<'a>,
        ) -> Result<gdl::Choice<'a>> {
            if let Some(next) = self.next.lock().expect("unpoisoned").take() {
                self.table.store(Arc::new(next));
            }
            Ok(gdl::Choice::new(
                instances.get(0).expect("the pool has an entry"),
            ))
        }
    }

    /// `m`'s row, in follow `generation` of its hub of `epoch`, with
    /// `eligible`.
    fn row_of(generation: u64, epoch: u64, eligible: &[InstanceId]) -> Arc<PoolTable> {
        let mut row = Arc::try_unwrap(pool("m", PoolState::Ready, eligible)).expect("one owner");
        row.generation = generation;
        row.epoch = epoch;
        row.leases = Arc::new(super::super::lease::Leases::default().of_epoch(epoch));
        Arc::new(row)
    }

    /// A choice the selector makes while the pool's hub restarts, of an
    /// instance still eligible under the later hub's follow, is offered
    /// under that follow: the policy's lease is told the later hub's epoch,
    /// not the epoch of the row it chose from. The control keeps its row's.
    #[tokio::test]
    async fn an_acceptance_says_the_epoch_of_the_follow_it_was_offered_under() {
        let instance = ids(1);
        for (restarts, expected) in [(false, 5), (true, 7)] {
            let mut crossing = crossing(
                vec![row_of(1, 5, &instance)],
                Within::Policy(Policy::RoundRobin),
            );
            let accepted = Arc::new(std::sync::Mutex::new(Vec::new()));
            crossing.target.policy = Arc::new(gdl::__runtime::TypedPolicy(Arc::new(
                FirstWithLease(Arc::clone(&accepted)),
            )));
            let next = restarts.then(|| EntryTable::new(Box::new([row_of(2, 7, &instance)])));
            let selector = gdl::__runtime::Typed(Arc::new(Restarting {
                table: Arc::clone(&crossing.target.table),
                next: std::sync::Mutex::new(next),
            }));
            let cx = Cx::new();
            let (chosen, _, _, mut lease) = crossing
                .choose(
                    &selector,
                    &cx,
                    &[],
                    Default::default(),
                    Left::NONE,
                    &mut None,
                )
                .await
                .expect("chosen")
                .expect("a pool");
            assert_eq!(chosen, instance[0]);
            lease.accepted();
            assert_eq!(
                *accepted.lock().expect("unpoisoned"),
                [expected],
                "restarts: {restarts}"
            );
        }
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
                    .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
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
                    .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
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
