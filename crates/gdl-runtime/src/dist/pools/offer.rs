//! A chooser's hand-off into another class is an offer, as every routed
//! hand-off is ([`crate::dist::offers`]): the pool chosen for it may
//! decline it, and then another pool is chosen inside the same call. An
//! entry instance that declines as its own leaves it in its pool, whose
//! other entry instances are offered it first. Only
//! a proven refusal moves the request on. Anything else after the send
//! may have run the request there, so it is the request's error, and is
//! not tried again.
//!
//! The first offer is made where any hand-off is
//! (`Runtime::send_forward`), and costs what a hand-off to an instance
//! does: the pools already offered the request, and the failover's own
//! state, are made only once a pool declined it ([`Runtime::fail_over`]).
//! They are locals of that call: never on the wire, never kept per
//! request, and gone when it returns. What outlives the request is the
//! chooser's damping of a pool that declined ([`super::damping`]), written
//! only when a pool declines, or when a damped pool accepts.
//!
//! A pool policy may re-queue a request for the pool that declined it as
//! full ([`gdl::PoolPolicy::requeue`]): the chooser then waits for room
//! in that pool and offers the request to it again, until it takes it, the
//! request's deadline passes, or the pool declines it otherwise or stops
//! taking work, which sends it on to another pool as before
//! ([`Runtime::requeue`]). So a full decline damps its pool only once the
//! policy did not re-queue the request.

use gdl::__runtime::ChoiceHeld;
use gdl::{Cx, ErrorKind, NoPool, Result};
use tokio::time::Instant;
use velo::InstanceId;

use super::crossing::{Again, Left, Walk};
use super::meter::{Outcome, PoolFigures};
use super::{Crossing, PoolLease, Seen};
use crate::dist::envelope::{Forward, WireCx};
use crate::dist::fabric::Offered;
use crate::dist::offers::{Answer, Missed, delivered};
use crate::dist::runtime::{Handed, Held, Runtime, Within};
use gdl::Decline;

/// What became of an offer, as `crossing` sent it to the pool `pool`
/// with `held`. A pool that declined it as gone is passed over until its
/// follow changes, and a damped pool that accepted it is no longer. A
/// full decline damps nothing here: the failover damps the pool once its
/// pool policy did not re-queue the request for it ([`Runtime::fail_over`]).
/// An acceptance is told to the pool policy's
/// lease in `held`, if it gave one. A hand-off that never left says
/// nothing of the pool, and changes nothing. An unknown outcome names the
/// class, since it is not offered to another pool. Every answer counts
/// under the pool and how it ended, when the chooser records its offers:
/// one relaxed add to the counts the lease on the pool's follow reaches,
/// and, with `sent`, when the offer left ([`timed`]), its round trip.
pub(crate) fn answer(
    offered: Offered,
    crossing: &Crossing,
    pool: u64,
    held: &mut Held,
    sent: Option<Instant>,
) -> Answer {
    let answer = delivered(offered, Some(crossing.class()));
    if let Some(figures) = held.pool_lease().and_then(PoolLease::figures) {
        figures.offered(Outcome::of(&answer), sent.map(|sent| sent.elapsed()));
    }
    match &answer {
        Answer::Accepted(_) => {
            crossing.accepted(pool);
            held.accepted();
        }
        Answer::Missed(Missed::Declined(_, Decline::Full | Decline::Budget)) => {}
        Answer::Missed(Missed::Declined(_, decline)) => {
            crossing.declined(pool, *decline, held.pool_lease());
        }
        Answer::Missed(Missed::Unsent(_)) | Answer::Failed(_) => {}
    }
    answer
}

/// Whether an offer that `held` holds is timed: when the hand-off's own
/// visit times it (`timed`), or when the chooser counts the pool's offers,
/// which then measures its round trip from the same instant. One instant,
/// which the hand-off already keeps across its wait, serves both.
#[inline]
pub(crate) fn timed(timed: bool, held: &Held) -> bool {
    timed || held.pool_lease().and_then(PoolLease::figures).is_some()
}

impl Runtime {
    /// Choose, for `envelope`, a pool of the class `crossing` enters that
    /// the request has not `left`, by the class's pool policy, then an instance
    /// of it by the hop's router: the instance, the pool's
    /// [`id`](super::PoolTable::id), and what the offer holds while the
    /// request is held, a selector's lease and the lease on the pool's
    /// follow, with the selector's meta added to `envelope`, and the view
    /// of the pool the choice was made on, if its policy asked for one, or
    /// none, in place of an earlier choice's. [`NoPool`]
    /// when no such pool may take it.
    pub(crate) async fn choose_pool(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        left: Left<'_>,
    ) -> Result<Result<(InstanceId, u64, Held), NoPool>> {
        match crossing.within() {
            Within::Policy(policy) => {
                let mut view = None;
                let picked = crossing.pick(
                    cx,
                    &envelope.payload,
                    *policy,
                    self.random(),
                    left,
                    &mut view,
                )?;
                Ok(picked.map(|(instance, pool, lease)| {
                    envelope.set_view(view);
                    (instance, pool, Held::pool(None, lease))
                }))
            }
            Within::Selector {
                selector, encoding, ..
            } => {
                let mut view = None;
                let chosen = crossing
                    .choose(
                        &**selector,
                        cx,
                        &envelope.payload,
                        *encoding,
                        left,
                        &mut view,
                    )
                    .await?;
                Ok(chosen.map(|(instance, pool, picked, lease)| {
                    envelope.cx.add_meta(picked.meta);
                    envelope.set_view(view);
                    (instance, pool, Held::pool(picked.lease, lease))
                }))
            }
        }
    }

    /// After the first offer of `envelope` missed (`declined`), offer it
    /// again, as the first was chosen, until a pool takes it, the request
    /// fails, or `pool_failovers` more pools were offered it. An entry
    /// instance that declined it as its own ([`Decline::Instance`]) leaves
    /// the request in its pool: each other entry instance of the pool is
    /// offered it once ([`Crossing::walk`]), before the chooser offers it
    /// to another pool, and those offers use none of `pool_failovers`.
    /// The pool policy's lease on the choice of that pool passes to each
    /// of those offers, so that it learns of an acceptance by any of them.
    /// Any other miss leaves the pool at once. The pool policy reads why
    /// each pool it left declined the request, if it did
    /// ([`gdl::PoolRequest::declines`]). A pool that declined it as full
    /// is asked about first: the pool policy may re-queue the request for
    /// it ([`Runtime::requeue`]), which uses none of `pool_failovers` and
    /// counts no failover; otherwise the pool is damped, and the request
    /// leaves it. Each pool is asked once. A pool that declined it over
    /// its budget is not asked about, and not damped. When no pool may
    /// take it any more, or no failover is left, and a pool declined it
    /// over its budget, it is offered once more, with no view
    /// ([`Runtime::force`]). Otherwise, or when that offer misses too, the
    /// request fails with the last refusal, or, when no pool refused it, with the last hand-off that
    /// never left. When the chooser holds every pool it left gone and its
    /// pool policy chooses no other, it fails with `Routing`, as the
    /// requests after it do, which find those pools gone before any
    /// offer ([`Crossing::gone_only`]). Each choice starts from the request's own context,
    /// `cx`, which `envelope`'s was made from, so a declined selector's
    /// meta is not sent to the next instance; a declined choice's leases,
    /// its selector's and its pool's, end before the next choice. It
    /// borrows the envelope from `send_forward`'s future.
    ///
    /// A chooser never chooses itself: the entry segment is in another
    /// class, so the hand-off always crosses velo.
    pub(crate) async fn fail_over(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        timed: bool,
        declined: Declined,
    ) -> Result<Handed> {
        let Declined {
            mut pool,
            mut instance,
            mut choice,
            mut missed,
            mut seen,
        } = declined;
        let mut last = reason(&missed);
        let mut offered = Vec::new();
        // Beside `offered`, not in it: a pool whose hand-off never left
        // said nothing, and the policy reads only the pools that declined.
        let mut declines = Vec::new();
        // The pools the policy was asked to re-queue the request for.
        let mut asked = Vec::new();
        let mut walk: Option<Walk> = None;
        // The pool a re-queue just left for an instance's own decline,
        // whose other entry instances are offered the request before it
        // leaves the pool: it was left and counted when it declined first.
        let mut requeued: Option<u64> = None;
        // Whether the request was placed past every pool's budget: once
        // at most.
        let mut forced = false;
        loop {
            if let Within::Selector { .. } = crossing.within() {
                envelope.cx = WireCx::of(cx);
            }
            let mut next = None;
            if last == Some(Decline::Instance) {
                if walk.is_none() {
                    walk = crossing.walk(pool, instance).map(|mut walk| {
                        walk.wants_view = seen.wants_view();
                        walk
                    });
                }
                if let Some(walk) = &mut walk {
                    next = self
                        .choose_in(crossing, cx, envelope, walk, &mut choice)
                        .await?;
                }
            }
            let (to, at, mut held) = match next {
                Some(chosen) => chosen,
                None => {
                    walk = None;
                    if requeued.take() == Some(pool) {
                        set_reason(&mut declines, pool, last);
                    } else {
                        // A policy that chooses a pool it left again is
                        // not asked about it again, whatever the pool's
                        // reasons between, and the pool counts again, so
                        // that `pool_failovers` bounds it.
                        offered.push(pool);
                        // The pool's latest reason is the one the policy
                        // reads: a forced offer's full decline replaces
                        // the budget's before it.
                        if declines.iter().any(|&(left, _)| left == pool) {
                            set_reason(&mut declines, pool, last);
                        } else if let Some(decline) = last {
                            declines.push((pool, decline));
                        }
                        if last == Some(Decline::Full) {
                            let first = !asked.contains(&pool);
                            if first {
                                asked.push(pool);
                            }
                            let left = Left::new(&offered, &declines);
                            if first && crossing.requeue(cx, &envelope.payload, left, pool)? {
                                let requeueing = Requeueing::new(crossing.figures(pool), cx);
                                let requeued_for = self.requeue(
                                    crossing,
                                    cx,
                                    envelope,
                                    timed,
                                    pool,
                                    &mut choice,
                                    &mut seen,
                                );
                                let ended = requeued_for.await;
                                requeueing.ended(&ended);
                                match ended? {
                                    Requeued::Handed(handed) => return Ok(handed),
                                    Requeued::Missed(to, then) => {
                                        last = reason(&then);
                                        instance = to;
                                        missed = missed.then(then);
                                        set_reason(&mut declines, pool, last);
                                        // The pool's other entry instances
                                        // are offered it, as after any
                                        // instance's own decline.
                                        if last == Some(Decline::Instance) {
                                            requeued = Some(pool);
                                            continue;
                                        }
                                    }
                                    Requeued::Unavailable => {}
                                }
                            } else {
                                crossing.declined(pool, Decline::Full, None);
                            }
                        }
                    }
                    // The request leaves the pool, and the pool policy's
                    // lease on it ends before the next choice.
                    drop(choice.take());
                    let chosen = if offered.len() > self.config.pool_failovers {
                        None
                    } else {
                        let left = Left::new(&offered, &declines);
                        self.choose_pool(crossing, cx, envelope, left).await?.ok()
                    };
                    let chosen = match chosen {
                        Some(chosen) => Some(chosen),
                        None if forced => None,
                        None => {
                            forced = true;
                            self.force(crossing, cx, envelope, &offered, &declines)
                                .await?
                        }
                    };
                    let Some(chosen) = chosen else {
                        break;
                    };
                    chosen
                }
            };
            // A forced placement, and every offer after it, carries no
            // view, so that no pool's budget declines it again.
            if forced {
                envelope.set_view(None);
            }
            let ends = held.pool_lease().map_or(0, PoolLease::ends);
            let (offering, sent) = self.deliver(to, envelope, self::timed(timed, &held))?;
            let answer = answer(offering.await, crossing, at, &mut held, sent);
            // Counted with the answer, so that an offer whose answer is
            // never read counts nothing.
            if let Some(figures) = held.pool_lease().and_then(PoolLease::figures) {
                figures.failover();
            }
            match answer {
                Answer::Accepted(accepted) => {
                    return Ok(Handed {
                        accepted,
                        sent,
                        held,
                    });
                }
                Answer::Failed(error) => return Err(error),
                Answer::Missed(then) => {
                    last = reason(&then);
                    seen = Seen::of(held.pool_lease(), ends);
                    if forced {
                        seen = seen.without_view();
                    }
                    choice = held.missed();
                    (pool, instance) = (at, to);
                    missed = missed.then(then);
                }
            }
        }
        // A refusal by a pool that may take work again says more about
        // this request than the policy's reason for no pool. When every
        // pool it left is gone, the requests after it meet the policy's
        // `Routing`, and so does this one.
        let left = Left::new(&offered, &declines);
        if let Some(no) = crossing.gone_only(cx, &envelope.payload, left)? {
            return Err(crossing.no_pool(no));
        }
        Err(missed.into_error())
    }

    /// Past every pool's budget: the instance, the pool, and what the offer
    /// holds, for one placement of `envelope` with no view, chosen as the
    /// first offer was, from the current table, with the budget's declines
    /// cleared and every other kept, so that the policy's first choice
    /// among those pools takes the request. `None` when no pool the request
    /// left declined it over its budget, or the policy chooses none. Counted
    /// under the pool. Made at most once a request, on its way to failing,
    /// so the two lists it builds cost nothing else.
    async fn force(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        offered: &[u64],
        declines: &[(u64, Decline)],
    ) -> Result<Option<(InstanceId, u64, Held)>> {
        let over = |pool: u64| {
            declines
                .iter()
                .any(|&(at, why)| at == pool && why == Decline::Budget)
        };
        if !offered.iter().any(|&pool| over(pool)) {
            return Ok(None);
        }
        let excluded: Vec<u64> = offered
            .iter()
            .copied()
            .filter(|&pool| !over(pool))
            .collect();
        let kept: Vec<(u64, Decline)> = declines
            .iter()
            .copied()
            .filter(|&(_, why)| why != Decline::Budget)
            .collect();
        let left = Left::new(&excluded, &kept);
        let Ok(chosen) = self.choose_pool(crossing, cx, envelope, left).await? else {
            return Ok(None);
        };
        if let Some(figures) = chosen.2.pool_lease().and_then(PoolLease::figures) {
            figures.budget_forced();
        }
        Ok(Some(chosen))
    }

    /// The next instance of `walk`, chosen as the first offer's was, with
    /// the pool's id and what the offer holds, `choice` among it; `None`
    /// once the walk has offered every instance of its pool.
    async fn choose_in(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        walk: &mut Walk,
        choice: &mut Option<ChoiceHeld>,
    ) -> Result<Option<(InstanceId, u64, Held)>> {
        match crossing.within() {
            Within::Policy(_) => Ok(crossing
                .pick_in(walk, choice)
                .map(|(instance, pool, lease)| (instance, pool, Held::pool(None, lease)))),
            Within::Selector {
                selector, encoding, ..
            } => {
                let chosen = crossing
                    .choose_in(walk, &**selector, cx, &envelope.payload, *encoding, choice)
                    .await?;
                Ok(chosen.map(|(instance, pool, picked, lease)| {
                    envelope.cx.add_meta(picked.meta);
                    (instance, pool, Held::pool(picked.lease, lease))
                }))
            }
        }
    }
}

impl Runtime {
    /// Offer `envelope` again to `pool`, which declined it as full and for
    /// which the pool policy re-queued it: wait for room in the pool
    /// ([`Crossing::room`]), choose an instance of it as the first offer
    /// was chosen, from its current table, and offer the request to it, until
    /// the pool takes it, or declines it other than as full. Each wait
    /// holds no lease on the pool's follow, and allocates nothing beyond
    /// its timer; the pool policy's lease on the choice of the pool,
    /// `choice`, passes to each offer. No failover is counted, and a full
    /// decline damps nothing. The request's deadline, which every request
    /// that crosses segments has, ends the wait: it fails the request with
    /// `DeadlineExceeded`, and so does a cancel. A pool that no longer takes
    /// work, or whose follow is no longer offered, ends the re-queue
    /// ([`Requeued::Unavailable`]), as does a decline for another reason
    /// ([`Requeued::Missed`]). `seen` is what the request saw of the pool
    /// before its last offer, kept up to date before each offer here, so
    /// that each wait returns at once when an offer to the pool ended
    /// since ([`Seen::room`]).
    #[allow(clippy::too_many_arguments)]
    async fn requeue(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        timed: bool,
        pool: u64,
        choice: &mut Option<ChoiceHeld>,
        seen: &mut Seen,
    ) -> Result<Requeued> {
        let deadline = cx.deadline().map(gdl::Deadline::instant);
        loop {
            // Held until the request offers again: any way out of this
            // round before the offer passes the wake to the next waiter.
            let woken = crossing
                .room(pool, seen, self.config.pool_requeue_backoff, deadline)
                .await;
            cx.check()?;
            // A re-offer whose choice asked for the view takes its row's,
            // even when the first offer could carry none.
            let wants_view = seen.wants_view();
            let mut view = None;
            let chosen = match crossing.within() {
                Within::Policy(policy) => {
                    match crossing.pick_again(
                        pool,
                        *policy,
                        self.random(),
                        choice,
                        wants_view.then_some(&mut view),
                    ) {
                        Some((to, lease)) => Again::Chosen((to, Held::pool(None, lease))),
                        None => Again::Unavailable,
                    }
                }
                Within::Selector {
                    selector, encoding, ..
                } => {
                    envelope.cx = WireCx::of(cx);
                    let chosen = crossing
                        .choose_again(
                            pool,
                            &**selector,
                            cx,
                            &envelope.payload,
                            *encoding,
                            choice,
                            wants_view.then_some(&mut view),
                        )
                        .await?;
                    match chosen {
                        Again::Chosen((to, picked, lease)) => {
                            envelope.cx.add_meta(picked.meta);
                            Again::Chosen((to, Held::pool(picked.lease, lease)))
                        }
                        Again::Stale => Again::Stale,
                        Again::Unavailable => Again::Unavailable,
                    }
                }
            };
            let (to, mut held) = match chosen {
                Again::Chosen(chosen) => chosen,
                // The pool still takes work: the request waits a round
                // for it, bounded by its deadline, and chooses again.
                Again::Stale => continue,
                Again::Unavailable => return Ok(Requeued::Unavailable),
            };
            if wants_view {
                envelope.set_view(view);
            }
            let ends = held.pool_lease().map_or(0, PoolLease::ends);
            let (offering, sent) = self.deliver(to, envelope, self::timed(timed, &held))?;
            if let Some(woken) = woken {
                woken.used();
            }
            match answer(offering.await, crossing, pool, &mut held, sent) {
                Answer::Accepted(accepted) => {
                    return Ok(Requeued::Handed(Handed {
                        accepted,
                        sent,
                        held,
                    }));
                }
                Answer::Failed(error) => return Err(error),
                Answer::Missed(then) => {
                    *seen = Seen::of(held.pool_lease(), ends);
                    *choice = held.missed();
                    // A pool still full keeps the request; any other miss,
                    // over its budget too, ends the wait.
                    if !matches!(then, Missed::Declined(_, Decline::Full)) {
                        return Ok(Requeued::Missed(to, then));
                    }
                }
            }
        }
    }
}

/// A request re-queued for a pool, counted under the pool when the chooser
/// records its offers: once as it is re-queued, and once more if it ends at
/// its deadline while re-queued. That is when the re-queue fails with
/// `DeadlineExceeded`, or when its future is dropped once the deadline has
/// passed: a hop bounds its hand-off by the deadline, and that bound answers
/// first, wherever the re-queue is. A drop before the deadline, as a
/// cancel's, counts nothing more.
struct Requeueing {
    figures: Option<&'static PoolFigures>,
    deadline: Option<Instant>,
}

impl Requeueing {
    /// Count the re-queue of the request with `cx` into `figures`.
    fn new(figures: Option<&'static PoolFigures>, cx: &Cx) -> Self {
        if let Some(figures) = figures {
            figures.requeued();
        }
        Self {
            figures,
            deadline: cx.deadline().map(gdl::Deadline::instant),
        }
    }

    /// The re-queue ended with `ended`.
    fn ended(mut self, ended: &Result<Requeued>) {
        if let (Some(figures), Err(error)) = (self.figures.take(), ended)
            && error.kind() == ErrorKind::DeadlineExceeded
        {
            figures.requeue_deadline();
        }
    }
}

impl Drop for Requeueing {
    fn drop(&mut self) {
        if let Some(figures) = self.figures
            && self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            figures.requeue_deadline();
        }
    }
}

/// How a re-queue of a request for one pool ended, short of the request's
/// own error.
enum Requeued {
    /// The pool took the request.
    Handed(Handed),
    /// The pool's entry instance `.0` declined the request other than as
    /// full, or the hand-off to it never left (`.1`): the request is
    /// failed over as after any such miss.
    Missed(InstanceId, Missed),
    /// The pool no longer takes work, or the chooser no longer follows it:
    /// the request leaves it.
    Unavailable,
}

/// What a chooser's first offer into another class came to, when it
/// missed: the pool and the instance it went to, the pool policy's lease
/// on the choice of that pool, if it gave one, the miss, and what the
/// request saw of the pool before the offer, for a re-queue's wait.
pub(crate) struct Declined {
    pub(crate) pool: u64,
    pub(crate) instance: InstanceId,
    pub(crate) choice: Option<ChoiceHeld>,
    pub(crate) missed: Missed,
    pub(crate) seen: Seen,
}

/// Record `last` as why `pool`, which the request left, declined it, in
/// place of the reason it gave before: what the pool policy reads of it
/// next. A hand-off that never left (`None`) keeps the reason before.
fn set_reason(declines: &mut [(u64, Decline)], pool: u64, last: Option<Decline>) {
    if let Some(why) = last
        && let Some(left) = declines.iter_mut().find(|(at, _)| *at == pool)
    {
        left.1 = why;
    }
}

/// Why `missed`'s entry declined, or `None` for a hand-off that never
/// left: [`Decline::Instance`] leaves the request in its pool.
fn reason(missed: &Missed) -> Option<Decline> {
    match missed {
        Missed::Declined(_, decline) => Some(*decline),
        Missed::Unsent(_) => None,
    }
}

#[cfg(test)]
mod instances;
#[cfg(test)]
mod requeue;

#[cfg(test)]
mod tests {
    use gdl::{Error, ErrorKind};
    use std::cell::Cell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gdl::__runtime::Typed;
    use gdl_pools::PoolState;

    use super::super::{EntryTable, PoolTable, Pools, pool_id, targets};
    use super::*;
    use crate::dist::counting::{Counted, counted, counting};
    use crate::dist::envelope::{Accepted, VERSION, WireCx};
    use crate::dist::fabric::{Fabric, ReplyTo, bind_node};
    use crate::dist::roster::{Policy, Roster};
    use crate::dist::runtime::{Router, RuntimeConfig};
    use gdl::Decline;

    /// Changes the request's `session` meta, which the request sets too.
    struct Replacing;

    impl gdl::Selector for Replacing {
        type Head = gdl::Encoded;

        async fn select<'a>(
            &'a self,
            _: gdl::Request<'a, gdl::Encoded>,
            instances: gdl::Instances<'a>,
        ) -> Result<gdl::Choice<'a>> {
            let chosen = instances.get(0).expect("never empty");
            Ok(gdl::Choice::new(chosen).with_meta("session", "chosen"))
        }
    }

    /// `S => D`, with `S` in `a` of `global` and `D` in `d` of `model_x`.
    pub(super) fn layout() -> gdl_model::Layout {
        layout_under(gdl_model::PoolPolicy::default())
    }

    /// [`layout`], choosing a pool of `model_x` by `policy`.
    pub(super) fn layout_under(policy: gdl_model::PoolPolicy) -> gdl_model::Layout {
        let manifest = gdl_model::Manifest {
            name: "pools".into(),
            schema: 1,
            nodes: vec![
                gdl_model::NodeInfo {
                    name: "S".into(),
                    role: gdl_model::Role::Source,
                    binding: None,
                    calls: Vec::new(),
                    unary: None,
                },
                gdl_model::NodeInfo {
                    name: "D".into(),
                    role: gdl_model::Role::Sink,
                    binding: Some("d".into()),
                    calls: Vec::new(),
                    unary: None,
                },
            ],
            hops: vec![gdl_model::HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![gdl_model::TypeInfo::new("T", true, Default::default())],
                backward_types: vec![gdl_model::TypeInfo::new("T", true, Default::default())],
            }],
            properties: Vec::new(),
        };
        let placement = gdl_model::Placement::new()
            .segment("a", ["S"])
            .segment("d", ["D"])
            .class("global", ["a"])
            .class("model_x", ["d"])
            .pool_policy("model_x", policy);
        gdl_model::layout(&manifest, &placement).expect("valid test fixture")
    }

    /// The allocations each of `routers` makes to send one hand-off that
    /// is accepted, its head encoded before the count begins, from the instance of `a` that `rt` is to the one
    /// entry instance of `d`: the fewest over a few sends, after a first
    /// that meets the peer.
    async fn allocations(rt: &Runtime, routers: &[Router]) -> Vec<u64> {
        let mut fewest = vec![u64::MAX; routers.len()];
        let codec = crate::wire::Codec::<Vec<u8>>::new(Default::default());
        for round in 0..31 {
            for (at, router) in routers.iter().enumerate() {
                let mut cx = Cx::new();
                cx.insert_meta("session", "s1");
                // Encoded as an edge encodes a head, with room after it.
                let payload = codec.encode_head(&vec![1, 2, 3]).expect("a head");
                let envelope = Forward {
                    version: VERSION,
                    identity: rt.identity,
                    hop: 0,
                    pos: 1,
                    cx: WireCx::of(&cx),
                    reply: ReplyTo::test_value(7),
                    payload,
                };
                let send = Counted::new(rt.send_forward("d", router, &cx, envelope, false));
                let (sent, made) = tokio::time::timeout(Duration::from_secs(5), send)
                    .await
                    .expect("answered in time");
                sent.expect("accepted");
                if round > 0 {
                    fewest[at] = fewest[at].min(made);
                }
            }
        }
        fewest
    }

    /// A chooser's first offer, accepted, allocates what the same hand-off
    /// routed by the roster does, with a policy and with a selector that
    /// replaces the request's own meta: nothing for a failover is made
    /// before a pool declines, and counting the offer's outcome and its
    /// round trip allocates nothing, and nor does a view on the offer. The
    /// hand-off is not timed for a visit, and its round trip is still
    /// measured.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_accepted_first_offer_allocates_what_a_roster_hand_off_does() {
        let layout = layout();
        let entry = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let chooser = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let mut roster = Roster::new();
        roster.add("d", entry.peer_info());
        let fabric = Fabric::new(chooser, None, None, Duration::from_secs(30));
        let rt = Runtime::new(
            fabric,
            layout.clone(),
            "a".into(),
            &roster,
            RuntimeConfig::default(),
            None,
        )
        .expect("a runtime");
        let accepted = crate::dist::envelope::encode(&Ok::<Accepted, Error>(None)).expect("encode");
        Fabric::new(entry.clone(), None, None, Duration::from_secs(30))
            .register(&rt.names().forward, move |_| {
                let accepted = accepted.clone();
                Box::pin(async move { accepted })
            })
            .expect("a handler");

        let registry = prometheus::Registry::new();
        let meters = super::super::PoolMeters::register(&registry).expect("registered");
        let pools = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
            gdl::PropertyKinds::new(),
        );
        let id = entry.instance_id();
        pools.store(
            "d",
            EntryTable::new(Box::new([Arc::new(PoolTable {
                class: "model_x".into(),
                pool: "m1".into(),
                id: pool_id("model_x", "m1"),
                generation: 1,
                epoch: 0,
                region: "home".into(),
                state: PoolState::Ready,
                // Numbered, so that a choice that asks for the view gets one.
                load: super::super::tests::idle().map(|idle| {
                    gdl::Load::new(
                        gdl::LoadSummary {
                            sequence: 3,
                            ..*idle.summary()
                        },
                        idle.arrived(),
                    )
                }),
                eligible: Box::new([id]),
                offered: Box::new([crate::dist::roster::selector_id(id)]),
                stamps: gdl::JoinStamps::default(),
                retired: Box::new([]),
                unread: false,
                turn: Arc::new(AtomicUsize::new(0)),
                offering: true,
                revision: 1,
                changes: 0,
                leases: Arc::new(super::super::lease::Leases::counted(Some(
                    meters.figures("model_x", "m1", "home"),
                ))),
                properties_version: 0,
            })])),
        );
        let viewing = layout_under(gdl_model::PoolPolicy {
            policy: "viewed".into(),
            provides: Vec::new(),
        });
        let mut policies = gdl::PoolPolicies::new();
        policies.add("viewed", Viewed).expect("registered");
        let crossing_under = |layout: &gdl_model::Layout, within: Within| {
            let mut targets = targets(layout, "a", &pools, &policies, gdl::HomeFirstAt::DEFAULT)
                .expect("targets");
            let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
            Router::Pools(Arc::new(Crossing::new(target, within, Default::default())))
        };
        let crossing = |within: Within| crossing_under(&layout, within);
        let policy = Within::Policy(Policy::RoundRobin);
        let selector = Within::Selector {
            name: Arc::from("replacing"),
            selector: Arc::new(Typed(Arc::new(Replacing))),
            encoding: Default::default(),
        };
        let routers = [
            Router::Roster(policy.clone()),
            crossing(policy.clone()),
            Router::Roster(selector.clone()),
            crossing(selector.clone()),
            crossing_under(&viewing, policy),
            crossing_under(&viewing, selector),
        ];
        let writes = super::super::damping::WRITES.with(Cell::get);
        let made = allocations(&rt, &routers).await;
        eprintln!("allocations per accepted first offer: {made:?}");
        assert_eq!(made[1], made[0], "with a policy: {made:?}");
        assert_eq!(made[3], made[2], "with a selector: {made:?}");
        assert_eq!(made[4], made[1], "a view, with a policy: {made:?}");
        assert_eq!(made[5], made[3], "a view, with a selector: {made:?}");
        assert_eq!(
            super::super::damping::WRITES.with(Cell::get),
            writes,
            "an accepted offer to an undamped pool wrote damping state"
        );
        // Each crossing sent 31 hand-offs, every one counted.
        assert_eq!(
            super::super::meter::tests::offers(&registry, "m1", "accepted"),
            Some(124.0)
        );
        let round_trips = super::super::meter::tests::round_trips(&registry, "m1", "accepted")
            .expect("a histogram");
        assert_eq!(round_trips.get_sample_count(), 124);
    }

    /// Chooses the first pool that takes work, and asks for the view.
    struct Viewed;

    impl gdl::PoolPolicy for Viewed {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            _: gdl::PoolRequest<'_, gdl::Encoded>,
            pools: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, gdl::NoPool> {
            pools
                .iter()
                .position(gdl::Pool::takes_work)
                .map(|at| gdl::PoolChoice::new(at).with_view())
                .ok_or_else(gdl::NoPool::new)
        }
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Prompt {
        text: String,
    }

    /// Reads the head as a `Prompt`, counts the reads, and takes the
    /// first instance.
    struct Reading(AtomicUsize);

    impl gdl::Selector for Reading {
        type Head = Prompt;

        async fn select<'a>(
            &'a self,
            request: gdl::Request<'a, Prompt>,
            instances: gdl::Instances<'a>,
        ) -> Result<gdl::Choice<'a>> {
            assert_eq!(request.head().text, "hello");
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(gdl::Choice::new(instances.get(0).expect("never empty")))
        }
    }

    /// A failover chooses the next pool's instance with the selector as
    /// the first offer did, reading the head in the edge's encoding: the
    /// first offer, to either pool, is declined, and the other pool's
    /// entry takes the request.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failover_reads_the_head_in_the_edges_encoding() {
        let layout = layout();
        let chooser = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let mut entries = Vec::new();
        let mut roster = Roster::new();
        for _ in 0..2 {
            let entry = bind_node(([127, 0, 0, 1], 0).into())
                .await
                .expect("a velo node");
            roster.add("d", entry.peer_info());
            entries.push(entry);
        }
        let fabric = Fabric::new(chooser, None, None, Duration::from_secs(30));
        let rt = Runtime::new(
            fabric,
            layout.clone(),
            "a".into(),
            &roster,
            RuntimeConfig::default(),
            None,
        )
        .expect("a runtime");
        let offers = Arc::new(AtomicUsize::new(0));
        let declined = crate::dist::envelope::encode(&Err::<Accepted, Error>(Error::new(
            ErrorKind::Overloaded,
            "full",
        )))
        .expect("encode");
        let accepted = crate::dist::envelope::encode(&Ok::<Accepted, Error>(None)).expect("encode");
        for entry in &entries {
            let offers = Arc::clone(&offers);
            let (declined, accepted) = (declined.clone(), accepted.clone());
            Fabric::new(entry.clone(), None, None, Duration::from_secs(30))
                .register(&rt.names().forward, move |_| {
                    let first = offers.fetch_add(1, Ordering::SeqCst) == 0;
                    let reply = if first {
                        declined.clone()
                    } else {
                        accepted.clone()
                    };
                    Box::pin(async move { reply })
                })
                .expect("a handler");
        }
        let pools = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
            gdl::PropertyKinds::new(),
        );
        let tables = entries
            .iter()
            .zip(["m1", "m2"])
            .map(|(entry, name)| {
                let id = entry.instance_id();
                Arc::new(PoolTable {
                    class: "model_x".into(),
                    pool: name.into(),
                    id: pool_id("model_x", name),
                    generation: 1,
                    epoch: 0,
                    region: "home".into(),
                    state: PoolState::Ready,
                    load: super::super::tests::idle(),
                    eligible: Box::new([id]),
                    offered: Box::new([crate::dist::roster::selector_id(id)]),
                    stamps: gdl::JoinStamps::default(),
                    retired: Box::new([]),
                    unread: false,
                    turn: Arc::new(AtomicUsize::new(0)),
                    offering: true,
                    revision: 1,
                    changes: 0,
                    leases: Arc::default(),
                    properties_version: 0,
                })
            })
            .collect();
        pools.store("d", EntryTable::new(tables));
        let mut targets = targets(
            &layout,
            "a",
            &pools,
            &gdl::PoolPolicies::new(),
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        let reading = Arc::new(Reading(AtomicUsize::new(0)));
        let router = Router::Pools(Arc::new(Crossing::new(
            target,
            Within::Selector {
                name: Arc::from("reading"),
                selector: Arc::new(Typed(Arc::clone(&reading))),
                encoding: gdl_model::Encoding::Json,
            },
            gdl_model::Encoding::Json,
        )));
        let cx = Cx::new();
        let envelope = Forward {
            version: VERSION,
            identity: rt.identity,
            hop: 0,
            pos: 1,
            cx: WireCx::of(&cx),
            reply: ReplyTo::test_value(7),
            payload: serde_json::to_vec(&Prompt {
                text: "hello".into(),
            })
            .expect("encoded")
            .into(),
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            rt.send_forward("d", &router, &cx, envelope, false),
        )
        .await
        .expect("answered in time")
        .expect("the second pool took it");
        assert_eq!(offers.load(Ordering::SeqCst), 2, "one decline, one take");
        assert_eq!(reading.0.load(Ordering::SeqCst), 2, "a choice per pool");
    }

    /// Every offer counts under its pool and how it ended, and each one
    /// after the request's first counts as a failover: `m0` has no entry
    /// velo can reach, so its offers never leave; `m1`'s entry declines
    /// as full, `m2`'s as gone, and `m3`'s accepts. Each request ends at
    /// `m3`, so the outcomes add up to the offers made, and the failovers
    /// to the offers less the requests.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_offer_counts_under_its_pool_and_outcome() {
        use super::super::lease::Leases;
        use super::super::meter::tests::{offers, scraped};
        use super::super::meter::{FAILOVERS, PoolMeters};
        const REQUESTS: usize = 20;
        let layout = layout();
        let chooser = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let mut roster = Roster::new();
        let mut entries = Vec::new();
        for _ in 0..3 {
            let entry = bind_node(([127, 0, 0, 1], 0).into())
                .await
                .expect("a velo node");
            roster.add("d", entry.peer_info());
            entries.push(entry);
        }
        let fabric = Fabric::new(chooser, None, None, Duration::from_secs(30));
        let rt = Runtime::new(
            fabric,
            layout.clone(),
            "a".into(),
            &roster,
            RuntimeConfig {
                pool_failovers: 3,
                ..RuntimeConfig::default()
            },
            None,
        )
        .expect("a runtime");
        let full = Error::new(ErrorKind::Overloaded, "full");
        let replies = [
            crate::dist::offers::refusal(&full, Decline::Full).expect("encode"),
            crate::dist::offers::refusal(&full, Decline::Gone).expect("encode"),
            crate::dist::envelope::encode(&Ok::<Accepted, Error>(None)).expect("encode"),
        ];
        let answered: Vec<Arc<AtomicUsize>> = entries.iter().map(|_| Arc::default()).collect();
        for ((entry, reply), answered) in entries.iter().zip(replies).zip(&answered) {
            let answered = Arc::clone(answered);
            Fabric::new(entry.clone(), None, None, Duration::from_secs(30))
                .register(&rt.names().forward, move |_| {
                    answered.fetch_add(1, Ordering::SeqCst);
                    let reply = reply.clone();
                    Box::pin(async move { reply })
                })
                .expect("a handler");
        }
        let registry = prometheus::Registry::new();
        let meters = PoolMeters::register(&registry).expect("registered");
        let pools = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
            gdl::PropertyKinds::new(),
        );
        // `m0`'s entry is no peer of the chooser's velo node.
        let ids = std::iter::once(velo::InstanceId::new_v4())
            .chain(entries.iter().map(|entry| entry.instance_id()));
        let tables = ids
            .zip(["m0", "m1", "m2", "m3"])
            .map(|(id, name)| {
                Arc::new(PoolTable {
                    class: "model_x".into(),
                    pool: name.into(),
                    id: pool_id("model_x", name),
                    generation: 1,
                    epoch: 0,
                    region: "home".into(),
                    state: PoolState::Ready,
                    load: super::super::tests::idle(),
                    eligible: Box::new([id]),
                    offered: Box::new([crate::dist::roster::selector_id(id)]),
                    stamps: gdl::JoinStamps::default(),
                    retired: Box::new([]),
                    unread: false,
                    turn: Arc::new(AtomicUsize::new(0)),
                    offering: true,
                    revision: 1,
                    changes: 0,
                    leases: Arc::new(Leases::counted(Some(
                        meters.figures("model_x", name, "home"),
                    ))),
                    properties_version: 0,
                })
            })
            .collect();
        pools.store("d", EntryTable::new(tables));
        let mut targets = targets(
            &layout,
            "a",
            &pools,
            &gdl::PoolPolicies::new(),
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        let router = Router::Pools(Arc::new(Crossing::new(
            target,
            Within::Policy(Policy::RoundRobin),
            Default::default(),
        )));
        for _ in 0..REQUESTS {
            let cx = Cx::new();
            let envelope = Forward {
                version: VERSION,
                identity: rt.identity,
                hop: 0,
                pos: 1,
                cx: WireCx::of(&cx),
                reply: ReplyTo::test_value(7),
                payload: vec![1, 2, 3].into(),
            };
            tokio::time::timeout(
                Duration::from_secs(5),
                rt.send_forward("d", &router, &cx, envelope, false),
            )
            .await
            .expect("answered in time")
            .expect("m3 took it");
        }
        let count = |pool: &str, outcome: &str| {
            offers(&registry, pool, outcome)
                .unwrap_or_else(|| panic!("no series for {pool} {outcome}")) as usize
        };
        let answered: Vec<usize> = answered.iter().map(|n| n.load(Ordering::SeqCst)).collect();
        assert_eq!(count("m1", "declined_full"), answered[0]);
        assert_eq!(count("m2", "declined_gone"), answered[1]);
        assert_eq!((count("m3", "accepted"), answered[2]), (REQUESTS, REQUESTS));
        assert!(count("m0", "unsent") > 0 && answered[0] > 0 && answered[1] > 0);
        let outcomes = [
            "accepted",
            "declined_full",
            "declined_gone",
            "declined_instance",
            "declined_not_present",
            "unsent",
            "outcome_unknown",
            "failed",
        ];
        let (mut offered, mut failovers) = (0, 0);
        for pool in ["m0", "m1", "m2", "m3"] {
            offered += outcomes.iter().map(|o| count(pool, o)).sum::<usize>();
            let labels = [("class", "model_x"), ("pool", pool)];
            failovers += scraped(&registry, FAILOVERS, &labels).expect("a series") as usize;
        }
        assert_eq!(
            offered,
            count("m0", "unsent") + answered.iter().sum::<usize>()
        );
        assert_eq!(failovers, offered - REQUESTS, "every offer but each first");
    }

    /// A request dropped while its failover waits for the answer counts
    /// nothing for that offer, neither an outcome nor a failover: `m0`'s
    /// first offer never leaves, and `m1`'s entry holds the failover until
    /// the request is gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_failover_counts_no_offer_without_an_answer() {
        use super::super::lease::Leases;
        use super::super::meter::tests::{offers, scraped};
        use super::super::meter::{FAILOVERS, PoolMeters};

        let layout = layout();
        let chooser = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a chooser");
        let entry = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("an entry");
        let mut roster = Roster::new();
        roster.add("d", entry.peer_info());
        let rt = Runtime::new(
            Fabric::new(chooser, None, None, Duration::from_secs(30)),
            layout.clone(),
            "a".into(),
            &roster,
            RuntimeConfig::default(),
            None,
        )
        .expect("a runtime");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let accepted = crate::dist::envelope::encode(&Ok::<Accepted, Error>(None)).expect("encode");
        Fabric::new(entry.clone(), None, None, Duration::from_secs(30))
            .register(&rt.names().forward, {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move |_| {
                    entered.notify_one();
                    let release = Arc::clone(&release);
                    let accepted = accepted.clone();
                    Box::pin(async move {
                        tokio::time::timeout(Duration::from_secs(5), release.notified())
                            .await
                            .expect("the entry is released");
                        accepted
                    })
                }
            })
            .expect("a handler");
        let registry = prometheus::Registry::new();
        let meters = PoolMeters::register(&registry).expect("registered");
        let pools = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
            gdl::PropertyKinds::new(),
        );
        let tables = [
            ("m0", velo::InstanceId::new_v4()),
            ("m1", entry.instance_id()),
        ]
        .into_iter()
        .map(|(name, id)| {
            Arc::new(PoolTable {
                class: "model_x".into(),
                pool: name.into(),
                id: pool_id("model_x", name),
                generation: 1,
                epoch: 0,
                region: "home".into(),
                state: PoolState::Ready,
                load: super::super::tests::idle(),
                eligible: Box::new([id]),
                offered: Box::new([crate::dist::roster::selector_id(id)]),
                stamps: gdl::JoinStamps::default(),
                retired: Box::new([]),
                unread: false,
                turn: Arc::new(AtomicUsize::new(0)),
                offering: true,
                revision: 1,
                changes: 0,
                leases: Arc::new(Leases::counted(Some(
                    meters.figures("model_x", name, "home"),
                ))),
                properties_version: 0,
            })
        })
        .collect();
        pools.store("d", EntryTable::new(tables));
        let mut targets = targets(
            &layout,
            "a",
            &pools,
            &gdl::PoolPolicies::new(),
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let router = Router::Pools(Arc::new(Crossing::new(
            targets.remove(&(0, 1)).expect("the class cut"),
            Within::Policy(Policy::RoundRobin),
            Default::default(),
        )));
        let cx = Cx::new();
        let envelope = Forward {
            version: VERSION,
            identity: rt.identity,
            hop: 0,
            pos: 1,
            cx: WireCx::of(&cx),
            reply: ReplyTo::test_value(7),
            payload: vec![1, 2, 3].into(),
        };
        let mut offer = Box::pin(rt.send_forward("d", &router, &cx, envelope, false));
        tokio::select! {
            _ = &mut offer => panic!("the entry has not answered"),
            arrived = tokio::time::timeout(Duration::from_secs(5), entered.notified()) => {
                arrived.expect("the failover reached the entry");
            }
        }
        assert_eq!(offers(&registry, "m0", "unsent"), Some(1.0));
        drop(offer);
        release.notify_one();
        for outcome in [
            "accepted",
            "declined_full",
            "declined_gone",
            "declined_instance",
            "declined_not_present",
            "unsent",
            "outcome_unknown",
            "failed",
        ] {
            assert_eq!(offers(&registry, "m1", outcome), Some(0.0));
        }
        assert_eq!(
            scraped(
                &registry,
                FAILOVERS,
                &[("class", "model_x"), ("pool", "m1")],
            ),
            Some(0.0),
            "a dropped unanswered offer must not leave a failover count"
        );
    }

    /// Sends a prompt to the pool whose name is its text, and says why
    /// when there is none or that pool declined.
    struct ByText;

    impl gdl::PoolPolicy for ByText {
        type Head = Prompt;

        fn choose(
            &self,
            request: gdl::PoolRequest<'_, Prompt>,
            pools: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            let text = &request.head().text;
            let at = pools
                .iter()
                .position(|pool| pool.name() == text)
                .ok_or_else(|| NoPool::because(format!("no pool is named `{text}`")))?;
            if request.is_excluded(&pools[at]) {
                return Err(NoPool::because(format!("pool `{text}` declined")));
            }
            Ok(gdl::PoolChoice::new(at))
        }
    }

    /// A registered pool policy reads the head, in the edge's encoding,
    /// and chooses the pool: two requests that differ only in the field
    /// it reads are offered to different pools, each to the one it names,
    /// over velo. A head it cannot read fails the request with `Codec`,
    /// and offers it to no pool. A head that names no pool fails with
    /// `Routing` and the policy's reason. A pool that declines leaves the
    /// request its refusal, not the reason the policy gives for choosing
    /// no other pool.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_registered_policy_chooses_the_pool_by_the_head() {
        let layout = layout_under(gdl_model::PoolPolicy {
            policy: "by_text".into(),
            provides: Vec::new(),
        });
        let chooser = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let mut entries = Vec::new();
        let mut roster = Roster::new();
        for _ in 0..2 {
            let entry = bind_node(([127, 0, 0, 1], 0).into())
                .await
                .expect("a velo node");
            roster.add("d", entry.peer_info());
            entries.push(entry);
        }
        let mut policies = gdl::PoolPolicies::new();
        policies.add("by_text", ByText).expect("registered");
        let fabric = Fabric::new(chooser, None, None, Duration::from_secs(30));
        let rt = Runtime::new(
            fabric,
            layout.clone(),
            "a".into(),
            &roster,
            RuntimeConfig {
                pool_policies: policies.clone(),
                ..RuntimeConfig::default()
            },
            None,
        )
        .expect("a runtime");
        let accepted = crate::dist::envelope::encode(&Ok::<Accepted, Error>(None)).expect("encode");
        let declined = crate::dist::envelope::encode(&Err::<Accepted, Error>(Error::new(
            ErrorKind::Overloaded,
            "full",
        )))
        .expect("encode");
        let full = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let offers: Vec<Arc<AtomicUsize>> = entries.iter().map(|_| Arc::default()).collect();
        for (entry, offers) in entries.iter().zip(&offers) {
            let (offers, accepted, declined) =
                (Arc::clone(offers), accepted.clone(), declined.clone());
            let full = Arc::clone(&full);
            Fabric::new(entry.clone(), None, None, Duration::from_secs(30))
                .register(&rt.names().forward, move |_| {
                    offers.fetch_add(1, Ordering::SeqCst);
                    let reply = if full.load(Ordering::SeqCst) {
                        declined.clone()
                    } else {
                        accepted.clone()
                    };
                    Box::pin(async move { reply })
                })
                .expect("a handler");
        }
        let pools = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
            gdl::PropertyKinds::new(),
        );
        let tables = entries
            .iter()
            .zip(["m1", "m2"])
            .map(|(entry, name)| {
                let id = entry.instance_id();
                Arc::new(PoolTable {
                    class: "model_x".into(),
                    pool: name.into(),
                    id: pool_id("model_x", name),
                    generation: 1,
                    epoch: 0,
                    region: "home".into(),
                    state: PoolState::Ready,
                    load: super::super::tests::idle(),
                    eligible: Box::new([id]),
                    offered: Box::new([crate::dist::roster::selector_id(id)]),
                    stamps: gdl::JoinStamps::default(),
                    retired: Box::new([]),
                    unread: false,
                    turn: Arc::new(AtomicUsize::new(0)),
                    offering: true,
                    revision: 1,
                    changes: 0,
                    leases: Arc::default(),
                    properties_version: 0,
                })
            })
            .collect();
        pools.store("d", EntryTable::new(tables));
        let mut targets =
            targets(&layout, "a", &pools, &policies, gdl::HomeFirstAt::DEFAULT).expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        let router = Router::Pools(Arc::new(Crossing::new(
            target,
            Within::Policy(Policy::RoundRobin),
            gdl_model::Encoding::Json,
        )));
        let send = |payload: Vec<u8>| {
            let cx = Cx::new();
            let envelope = Forward {
                version: VERSION,
                identity: rt.identity,
                hop: 0,
                pos: 1,
                cx: WireCx::of(&cx),
                reply: ReplyTo::test_value(7),
                payload: payload.into(),
            };
            let rt = &rt;
            let router = &router;
            async move {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    rt.send_forward("d", router, &cx, envelope, false),
                )
                .await
                .expect("answered in time")
                .map(|_| ())
            }
        };
        let prompt =
            |text: &str| serde_json::to_vec(&Prompt { text: text.into() }).expect("encoded");
        let offered = || {
            offers
                .iter()
                .map(|n| n.load(Ordering::SeqCst))
                .collect::<Vec<_>>()
        };
        send(prompt("m2")).await.expect("m2 took it");
        assert_eq!(offered(), [0, 1]);
        send(prompt("m1")).await.expect("m1 took it");
        assert_eq!(offered(), [1, 1]);
        send(prompt("m2")).await.expect("m2 took it");
        assert_eq!(offered(), [1, 2]);
        let error = send(prompt("m3")).await.expect_err("no pool is m3");
        assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
        assert!(
            error.to_string().contains("no pool is named `m3`"),
            "{error}"
        );
        let error = send(b"not json".to_vec())
            .await
            .expect_err("no head to read");
        assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
        assert_eq!(offered(), [1, 2], "a request no pool may take was offered");
        full.store(true, Ordering::SeqCst);
        let error = send(prompt("m1")).await.expect_err("m1 declined");
        assert_eq!(offered(), [2, 2], "m1 was offered it once");
        assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
        assert!(
            error.to_string().contains("full") && !error.to_string().contains("declined"),
            "{error}"
        );
    }

    /// The crossing into `model_x` of [`layout`], over `pools`, each with
    /// one entry instance and an idle summary.
    fn crossing_over(names: &[&str]) -> Crossing {
        followed_over(names).0
    }

    /// [`crossing_over`], with the tables it chooses from.
    fn followed_over(names: &[&str]) -> (Crossing, Pools) {
        followed_under(&layout(), names)
    }

    /// [`followed_over`], for the crossing of `layout`.
    fn followed_under(layout: &gdl_model::Layout, names: &[&str]) -> (Crossing, Pools) {
        followed_with(layout, names, &gdl::PoolPolicies::new())
    }

    /// [`followed_under`], with `policies` registered.
    fn followed_with(
        layout: &gdl_model::Layout,
        names: &[&str],
        policies: &gdl::PoolPolicies,
    ) -> (Crossing, Pools) {
        let pools = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
            gdl::PropertyKinds::new(),
        );
        let tables = names
            .iter()
            .map(|name| {
                let id = velo::InstanceId::new_v4();
                Arc::new(PoolTable {
                    class: "model_x".into(),
                    pool: (*name).into(),
                    id: pool_id("model_x", name),
                    generation: 1,
                    epoch: 0,
                    region: "home".into(),
                    state: PoolState::Ready,
                    load: super::super::tests::idle(),
                    eligible: Box::new([id]),
                    offered: Box::new([crate::dist::roster::selector_id(id)]),
                    stamps: gdl::JoinStamps::default(),
                    retired: Box::new([]),
                    unread: false,
                    turn: Arc::new(AtomicUsize::new(0)),
                    offering: true,
                    revision: 1,
                    changes: 0,
                    leases: Arc::default(),
                    properties_version: 0,
                })
            })
            .collect();
        pools.store("d", EntryTable::new(tables));
        let mut targets =
            targets(layout, "a", &pools, policies, gdl::HomeFirstAt::DEFAULT).expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        (
            Crossing::new(
                target,
                Within::Policy(Policy::RoundRobin),
                Default::default(),
            ),
            pools,
        )
    }

    /// An acceptance ends the damping the pool has when it accepts, not
    /// the one it had when its offer was chosen: an offer chosen before a
    /// decline damped the pool ends that damping, and of two offers chosen
    /// while it was damped, only the first acceptance writes.
    #[test]
    fn an_acceptance_ends_the_damping_the_pool_has_when_it_accepts() {
        let cx = Cx::new();
        let writes = || super::super::damping::WRITES.with(Cell::get);
        let random = std::hash::RandomState::new();
        let (lone, pools) = followed_over(&["m1"]);
        let pick = || {
            lone.pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                .expect("a pick")
                .expect("a lone pool is always chosen")
                .1
        };
        let damped = || pools.followed("d")[0].damped.is_some();
        let (first, second) = (pick(), pick());
        lone.declined(second, Decline::Full, None);
        assert!(damped());
        lone.accepted(first);
        assert!(
            !damped(),
            "an offer chosen before the decline left it damped"
        );

        lone.declined(pick(), Decline::Full, None);
        let (first, second) = (pick(), pick());
        let wrote = writes();
        lone.accepted(first);
        lone.accepted(second);
        assert!(!damped());
        assert_eq!(writes() - wrote, 1, "an undamped pool's acceptance wrote");
    }

    /// `pools`' table of `name` replaced by the one `change` makes of it,
    /// as the follower makes a new table for each change the relay sends;
    /// the other pools' tables stay as they are.
    fn changed(pools: &Pools, name: &str, change: impl FnOnce(&mut PoolTable)) {
        let table = pools.table("d").expect("a table");
        let mut change = Some(change);
        let tables = table
            .pools
            .iter()
            .map(|pool| {
                if pool.pool != name {
                    return Arc::clone(pool);
                }
                let mut next = PoolTable {
                    class: pool.class.clone(),
                    pool: pool.pool.clone(),
                    id: pool.id,
                    generation: pool.generation,
                    epoch: 0,
                    region: pool.region.clone(),
                    state: pool.state,
                    load: pool.load,
                    eligible: pool.eligible.clone(),
                    offered: pool.offered.clone(),
                    stamps: gdl::JoinStamps::default(),
                    retired: pool.retired.clone(),
                    unread: pool.unread,
                    turn: Arc::clone(&pool.turn),
                    offering: pool.offering,
                    revision: pool.revision,
                    changes: pool.changes,
                    leases: Arc::clone(&pool.leases),
                    properties_version: 0,
                };
                change.take().expect("one pool of the name")(&mut next);
                Arc::new(next)
            })
            .collect();
        pools.store("d", EntryTable::new(tables));
    }

    /// A pool that declines an offer as gone, as a draining pool's entry
    /// does, is passed over until the relay changes its follow, however
    /// long that takes; a new load summary is no change. A full pool's
    /// decline, the control, damps the pool, which is chosen again once
    /// its window has passed. An acceptance ends a mark as gone. A gone
    /// decline of an offer made under an earlier generation damps the newer
    /// one instead. A lone gone pool is not chosen: the cap on damping
    /// does not hold it in the choice.
    #[tokio::test(start_paused = true)]
    async fn a_pool_that_declined_as_gone_is_passed_over_until_its_follow_changes() {
        const FIRST_WINDOW: Duration = crate::RuntimeConfig::DEFAULT_POOL_DAMPING_FIRST;
        const MAX_WINDOW: Duration = crate::RuntimeConfig::DEFAULT_POOL_DAMPING_MAX;

        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let (crossing, pools) = followed_over(&["m1", "m2", "m3"]);
        let m1 = pool_id("model_x", "m1");
        let pick = || {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                .expect("a pick")
                .expect("a pool")
        };
        // An offer to `m1`, with its lease.
        let offer = || loop {
            let (_, pool, lease) = pick();
            if pool == m1 {
                return lease;
            }
        };
        let chosen = || (0..12).any(|_| pick().1 == m1);
        let shown = || {
            pools
                .followed("d")
                .into_iter()
                .find(|pool| pool.pool == "m1")
                .expect("followed")
        };

        let lease = offer();
        crossing.declined(m1, Decline::Full, Some(&lease));
        drop(lease);
        assert!(!chosen(), "chosen while its window lasts");
        tokio::time::advance(FIRST_WINDOW).await;
        assert!(
            chosen(),
            "a full pool was not chosen once its window passed"
        );

        let lease = offer();
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        let m = shown();
        assert!(m.gone && m.damped.is_none(), "{m:?}");
        tokio::time::advance(MAX_WINDOW * 2).await;
        assert!(!chosen(), "chosen again while its follow is unchanged");
        changed(&pools, "m1", |pool| pool.load = super::super::tests::idle());
        assert!(!chosen(), "a new load summary ended it");
        changed(&pools, "m1", |pool| pool.changes += 1);
        assert!(chosen(), "not chosen once the relay changed its state");
        assert!(!shown().gone);

        let lease = offer();
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        assert!(!chosen());
        crossing.accepted(m1);
        assert!(chosen(), "not chosen once it accepted an offer");

        let lease = offer();
        changed(&pools, "m1", |pool| {
            pool.generation += 1;
            pool.leases = Arc::default();
        });
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        let m = shown();
        assert!(!m.gone && m.damped.is_some(), "{m:?}");
        crossing.accepted(m1);

        let (lone, _pools) = followed_over(&["m1"]);
        let (_, pool, lease) = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
            .expect("a pick")
            .expect("a lone pool");
        lone.declined(pool, Decline::Gone, Some(&lease));
        drop(lease);
        let no = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
            .expect("a pick")
            .expect_err("a lone gone pool was chosen");
        assert_eq!(no, NoPool::new());
    }

    /// Replies to offers already in flight can arrive in either order.
    /// A full reply gives no evidence that a gone pool takes work again.
    #[tokio::test(start_paused = true)]
    async fn a_late_full_reply_preserves_a_gone_pool() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let (crossing, pools) = followed_over(&["m1"]);
        let pick = || {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                .expect("a pick")
        };
        let (_, pool, first) = pick().expect("the first offer");
        let (_, same, second) = pick().expect("the second offer");
        assert_eq!(pool, same);
        crossing.declined(pool, Decline::Gone, Some(&first));
        drop(first);
        assert!(pools.followed("d")[0].gone);
        assert!(pick().is_err(), "the gone pool was chosen");

        crossing.declined(pool, Decline::Full, Some(&second));
        drop(second);
        tokio::time::advance(crate::RuntimeConfig::DEFAULT_POOL_DAMPING_MAX * 2).await;
        let shown = &pools.followed("d")[0];
        assert!(
            shown.gone && pick().is_err(),
            "a full reply revived a gone pool without a follow change: {shown:?}"
        );
    }

    /// A node that reads the chooser's tables holds a pool gone exactly
    /// while the chooser's choice passes it over: from its gone decline,
    /// across a new load summary, until the relay changes its follow or it
    /// accepts an offer. A full decline, the control, damps the pool and
    /// is not gone.
    #[tokio::test(start_paused = true)]
    async fn a_node_reads_a_pool_gone_while_the_choice_passes_it_over() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let (crossing, pools) = followed_over(&["m1", "m2"]);
        let node = gdl::FollowedPools::new();
        assert!(node.bind(Arc::new(pools.clone())));
        let m1 = pool_id("model_x", "m1");
        let pick = || {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                .expect("a pick")
                .expect("a pool")
        };
        let offer = || loop {
            let (_, pool, lease) = pick();
            if pool == m1 {
                return lease;
            }
        };
        let chosen = || (0..12).any(|_| pick().1 == m1);
        let read = || {
            node.read("model_x", |pools| {
                (pools.is_gone("m1"), pools.is_gone("m2"))
            })
            .expect("the chooser sends to model_x")
        };
        assert_eq!(read(), (false, false));

        let lease = offer();
        crossing.declined(m1, Decline::Full, Some(&lease));
        drop(lease);
        assert_eq!(read(), (false, false), "a full pool read gone");
        crossing.accepted(m1);

        let lease = offer();
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        assert!(!chosen());
        assert_eq!(read(), (true, false), "the chooser passes m1 over");
        changed(&pools, "m1", |pool| pool.load = super::super::tests::idle());
        assert!(!chosen());
        assert_eq!(read(), (true, false), "a new load summary ended it");
        changed(&pools, "m1", |pool| pool.changes += 1);
        assert!(chosen());
        assert_eq!(read(), (false, false), "the follow changed");

        let lease = offer();
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        assert_eq!(read(), (true, false));
        crossing.accepted(m1);
        assert_eq!(read(), (false, false), "m1 accepted an offer");
    }

    /// A pick whose pool accepts allocates nothing and, while the pool is
    /// not damped, writes no damping state: an acceptance by an undamped
    /// pool costs a branch. A decline writes, and so does the acceptance
    /// that ends a pool's damping, once.
    #[test]
    fn an_accepted_pick_allocates_nothing_and_writes_no_damping() {
        let cx = Cx::new();
        let writes = || super::super::damping::WRITES.with(Cell::get);
        let random = std::hash::RandomState::new();
        let crossing = crossing_over(&["m1", "m2", "m3"]);
        let (before, wrote) = (counted(), writes());
        counting(true);
        for _ in 0..1000 {
            let (_, chosen, _) = crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                .expect("a pick")
                .expect("a pool");
            crossing.accepted(chosen);
        }
        counting(false);
        assert_eq!(counted() - before, 0, "a pick allocated");
        assert_eq!(writes() - wrote, 0, "an accepted pick wrote damping state");

        let lone = crossing_over(&["m1"]);
        let (_, chosen, _) = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
            .expect("a pick")
            .expect("a pool");
        lone.declined(chosen, Decline::Full, None);
        assert_eq!(writes() - wrote, 1, "a decline wrote nothing");
        // A lone pool is never damped out of the choice.
        let (_, chosen, _) = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
            .expect("a pick")
            .expect("a pool");
        lone.accepted(chosen);
        assert_eq!(writes() - wrote, 2, "the acceptance kept the damping");
        let (_, chosen, _) = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
            .expect("a pick")
            .expect("a pool");
        lone.accepted(chosen);
        assert_eq!(writes() - wrote, 2, "an undamped pool's acceptance wrote");
    }

    /// What a pool choice and an accepted first offer cost, in time, for
    /// the record in the book: a pick under each policy the runtime
    /// provides, over four idle pools, the count of an offer's outcome,
    /// and one accepted offer over loopback, its outcome counted, against
    /// the same hand-off routed by the roster. Ignored: it times,
    /// and asserts nothing. Run it in release:
    /// `cargo test --release -p gdl-runtime --lib cost_of -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "a timing, not a check"]
    async fn cost_of_a_pool_choice_and_an_offer() {
        const PICKS: u32 = 2_000_000;
        let random = std::hash::RandomState::new();
        let cx = Cx::new();
        for name in ["round_robin", "home_first"] {
            let layout = layout_under(gdl_model::PoolPolicy {
                policy: name.into(),
                provides: Vec::new(),
            });
            let (crossing, _pools) = followed_under(&layout, &["m1", "m2", "m3", "m4"]);
            let mut best = Duration::MAX;
            for _ in 0..5 {
                let start = std::time::Instant::now();
                for _ in 0..PICKS {
                    let _ = std::hint::black_box(
                        crossing
                            .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                            .expect("a pick"),
                    );
                }
                best = best.min(start.elapsed());
            }
            println!(
                "pick {name}: {:.1} ns",
                best.as_nanos() as f64 / f64::from(PICKS)
            );
        }
        // What an offer adds to count its outcome under its pool.
        let meters = super::super::PoolMeters::unregistered();
        let figures = meters.figures("model_x", "m1", "home");
        let mut best = Duration::MAX;
        for _ in 0..5 {
            let start = std::time::Instant::now();
            for _ in 0..PICKS {
                std::hint::black_box(figures).offered(super::super::meter::Outcome::Accepted, None);
            }
            best = best.min(start.elapsed());
        }
        println!(
            "count an outcome: {:.1} ns",
            best.as_nanos() as f64 / f64::from(PICKS)
        );

        const OFFERS: u32 = 20_000;
        let layout = layout();
        let entry = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let chooser = bind_node(([127, 0, 0, 1], 0).into())
            .await
            .expect("a velo node");
        let mut roster = Roster::new();
        roster.add("d", entry.peer_info());
        let fabric = Fabric::new(chooser, None, None, Duration::from_secs(30));
        let rt = Runtime::new(
            fabric,
            layout.clone(),
            "a".into(),
            &roster,
            RuntimeConfig::default(),
            None,
        )
        .expect("a runtime");
        let accepted = crate::dist::envelope::encode(&Ok::<Accepted, Error>(None)).expect("encode");
        Fabric::new(entry.clone(), None, None, Duration::from_secs(30))
            .register(&rt.names().forward, move |_| {
                let accepted = accepted.clone();
                Box::pin(async move { accepted })
            })
            .expect("a handler");
        let pools = Pools::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
            gdl::PropertyKinds::new(),
        );
        let id = entry.instance_id();
        pools.store(
            "d",
            EntryTable::new(Box::new([Arc::new(PoolTable {
                class: "model_x".into(),
                pool: "m1".into(),
                id: pool_id("model_x", "m1"),
                generation: 1,
                epoch: 0,
                region: "home".into(),
                state: PoolState::Ready,
                load: super::super::tests::idle(),
                eligible: Box::new([id]),
                offered: Box::new([crate::dist::roster::selector_id(id)]),
                stamps: gdl::JoinStamps::default(),
                retired: Box::new([]),
                unread: false,
                turn: Arc::new(AtomicUsize::new(0)),
                offering: true,
                revision: 1,
                changes: 0,
                // Counted, as a chooser whose listener is on counts.
                leases: Arc::new(super::super::lease::Leases::counted(Some(figures))),
                properties_version: 0,
            })])),
        );
        let mut targets = targets(
            &layout,
            "a",
            &pools,
            &gdl::PoolPolicies::new(),
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        let policy = Within::Policy(Policy::RoundRobin);
        let routers = [
            ("roster", Router::Roster(policy.clone())),
            (
                "pools",
                Router::Pools(Arc::new(Crossing::new(target, policy, Default::default()))),
            ),
        ];
        for (name, router) in &routers {
            let mut best = Duration::MAX;
            for _ in 0..3 {
                let start = std::time::Instant::now();
                for _ in 0..OFFERS {
                    let cx = Cx::new();
                    let envelope = Forward {
                        version: VERSION,
                        identity: rt.identity,
                        hop: 0,
                        pos: 1,
                        cx: WireCx::of(&cx),
                        reply: ReplyTo::test_value(7),
                        payload: vec![1, 2, 3].into(),
                    };
                    rt.send_forward("d", router, &cx, envelope, false)
                        .await
                        .expect("accepted");
                }
                best = best.min(start.elapsed());
            }
            println!(
                "offer {name}: {:.2} us",
                best.as_nanos() as f64 / f64::from(OFFERS) / 1000.0
            );
        }
    }

    /// Chooses as `home_first` or `round_robin` among every pool its
    /// request does not list as draining or withdrawn, and that its hub
    /// numbered past epoch 0, and builds a reason when none is left.
    struct KeptBut(bool);

    impl gdl::PoolPolicy for KeptBut {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            request: gdl::PoolRequest<'_, gdl::Encoded>,
            pools: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            let keep = |pool: &gdl::Pool| {
                let name = pool.name();
                !request.is_draining(name)
                    && !request.is_withdrawn(name)
                    && request.epoch(name).is_some_and(|epoch| epoch > 0)
            };
            let at = if self.0 {
                gdl::HomeFirst.choose_where(request, pools, keep)
            } else {
                gdl::RoundRobin.choose_where(request, pools, keep)
            };
            at.map(gdl::PoolChoice::new)
                .ok_or_else(|| NoPool::because(format!("none of {} pools is kept", pools.len())))
        }
    }

    /// A pick allocates nothing under either policy the runtime provides,
    /// nor under a registered one that runs either over the pools it
    /// keeps, here those not draining or withdrawn, by their epochs, also while a pool's damping window
    /// lasts, which takes the pass that sets which damped pools are passed
    /// over, with a pool excluded, and with a pool gone, which is never
    /// chosen. The registered ones build a reason when they choose no
    /// pool, which allocates then and only then.
    #[test]
    fn a_pick_allocates_nothing_under_either_policy() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let mut policies = gdl::PoolPolicies::new();
        policies
            .add("kept_home_first", KeptBut(true))
            .expect("registered");
        policies
            .add("kept_round_robin", KeptBut(false))
            .expect("registered");
        for name in [
            "home_first",
            "round_robin",
            "kept_home_first",
            "kept_round_robin",
        ] {
            let layout = layout_under(gdl_model::PoolPolicy {
                policy: name.into(),
                provides: Vec::new(),
            });
            let (crossing, pools) =
                followed_with(&layout, &["m1", "m2", "m3", "m4", "m5"], &policies);
            // `m0`'s follow was dropped while it drains, and `m6` withdrew.
            let followed = pools.table("d").expect("a table").pools.clone();
            let evidence = ["m0", "m1", "m2", "m3", "m4", "m5", "m6"]
                .map(|name| {
                    let evidence = gdl::PoolEvidence::new(1, gdl::PoolCondition::Live)
                        .with_draining(matches!(name, "m0" | "m2"))
                        .with_withdrawn(name == "m6");
                    (Box::from(name), evidence)
                })
                .into();
            pools.store("d", EntryTable::new(followed).with_evidence(evidence));
            let excluded = [pool_id("model_x", "m4")];
            crossing.declined(pool_id("model_x", "m1"), Decline::Full, None);
            crossing.declined(pool_id("model_x", "m2"), Decline::Full, None);
            pools
                .damping()
                .gone(&pools.table("d").expect("a table").views[4]);
            let damped = pools.followed("d");
            assert!(damped[0].damped.is_some() && damped[1].damped.is_some());
            assert!(damped[4].gone);
            let before = counted();
            // Made before counting, so that the record of the choices
            // allocates nothing while it counts.
            let mut chosen = Vec::with_capacity(1000);
            counting(true);
            for _ in 0..1000 {
                let (_, pool, _) = crossing
                    .pick(
                        &cx,
                        &[],
                        Policy::RoundRobin,
                        &random,
                        Left::excluding(&excluded),
                        &mut None,
                    )
                    .expect("a pick")
                    .expect("a pool");
                chosen.push(pool);
            }
            counting(false);
            let chosen: std::collections::BTreeSet<u64> = chosen.into_iter().collect();
            assert_eq!(counted() - before, 0, "a {name} pick allocated");
            // The undamped pool is chosen, and the excluded one never.
            assert!(chosen.contains(&pool_id("model_x", "m3")), "{name}");
            assert!(!chosen.contains(&excluded[0]), "{name}");
            assert!(!chosen.contains(&pool_id("model_x", "m5")), "{name}");
            if name.starts_with("kept") {
                assert!(!chosen.contains(&pool_id("model_x", "m2")), "{name}");
            }
            let every = ["m1", "m2", "m3", "m4"].map(|pool| pool_id("model_x", pool));
            let before = counted();
            counting(true);
            let no = crossing
                .pick(
                    &cx,
                    &[],
                    Policy::RoundRobin,
                    &random,
                    Left::excluding(&every),
                    &mut None,
                )
                .expect("a pick")
                .expect_err("no pool");
            counting(false);
            let built = counted() - before;
            if name.starts_with("kept") {
                assert_eq!(no.reason(), Some("none of 5 pools is kept"));
                assert!(built > 0, "a {name} reason was built");
            } else {
                assert_eq!((no, built), (NoPool::new(), 0), "{name}");
            }
        }
    }

    /// Gives every choice a lease that does nothing.
    struct Leasing;

    /// Not zero-sized, so that it takes an allocation of its own.
    struct Idle(#[allow(dead_code, reason = "held for its size")] u64);

    impl gdl::ChoiceLease for Idle {
        fn accepted(&mut self) {}
    }

    impl gdl::PoolPolicy for Leasing {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            request: gdl::PoolRequest<'_, gdl::Encoded>,
            pools: &[gdl::Pool],
        ) -> std::result::Result<gdl::PoolChoice, NoPool> {
            let at = gdl::RoundRobin
                .choose_where(request, pools, |_| true)
                .ok_or_else(NoPool::new)?;
            Ok(gdl::PoolChoice::new(at).with_lease(Idle(0)))
        }
    }

    /// A choice with a lease allocates once per pick, the block that holds
    /// the lease, and the pick nothing more.
    #[test]
    fn a_choice_with_a_lease_allocates_once() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let mut policies = gdl::PoolPolicies::new();
        policies.add("leasing", Leasing).expect("registered");
        let layout = layout_under(gdl_model::PoolPolicy {
            policy: "leasing".into(),
            provides: Vec::new(),
        });
        let (crossing, _pools) = followed_with(&layout, &["m1", "m2"], &policies);
        let before = counted();
        counting(true);
        for _ in 0..1000 {
            let (_, pool, lease) = crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, Left::NONE, &mut None)
                .expect("a pick")
                .expect("a pool");
            crossing.accepted(pool);
            drop(lease);
        }
        counting(false);
        assert_eq!(counted() - before, 1000);
    }
}
