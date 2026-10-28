//! A chooser's hand-off into another class is an offer, as every routed
//! hand-off is ([`crate::dist::offers`]): the cell chosen for it may
//! decline it, and then another cell is chosen inside the same call. An
//! entry instance that declines as its own leaves it in its cell, whose
//! other entry instances are offered it first. Only
//! a proven refusal moves the request on. Anything else after the send
//! may have run the request there, so it is the request's error, and is
//! not tried again.
//!
//! The first offer is made where any hand-off is
//! (`Runtime::send_forward`), and costs what a hand-off to an instance
//! does: the cells already offered the request, and the failover's own
//! state, are made only once a cell declined it ([`Runtime::fail_over`]).
//! They are locals of that call: never on the wire, never kept per
//! request, and gone when it returns. What outlives the request is the
//! chooser's damping of a cell that declined ([`super::damping`]), written
//! only when a cell declines, or when a damped cell accepts.

use gdl::__runtime::ChoiceHeld;
use gdl::{Cx, NoCell, Result};
use velo::InstanceId;

use super::crossing::Walk;
use super::meter::Outcome;
use super::{CellLease, Crossing};
use crate::dist::envelope::{Forward, WireCx};
use crate::dist::fabric::Offered;
use crate::dist::offers::{Answer, Decline, Missed, delivered};
use crate::dist::runtime::{Handed, Held, Runtime, Within};

/// What became of an offer, as `crossing` sent it to the cell `cell`
/// with `held`. A cell that declined it is damped, or passed over until
/// its follow changes when it said it is gone, and a damped cell that
/// accepted it is no longer; an acceptance is told to the cell policy's
/// lease in `held`, if it gave one. A hand-off that never left says
/// nothing of the cell, and changes nothing. An unknown outcome names the
/// class, since it is not offered to another cell. Every answer counts
/// under the cell and how it ended, when the chooser records its offers:
/// one relaxed add to the counts the lease on the cell's follow reaches.
pub(crate) fn answer(offered: Offered, crossing: &Crossing, cell: u64, held: &mut Held) -> Answer {
    let answer = delivered(offered, Some(crossing.class()));
    if let Some(figures) = held.cell_lease().and_then(CellLease::figures) {
        figures.offered(Outcome::of(&answer));
    }
    match &answer {
        Answer::Accepted(_) => {
            crossing.accepted(cell);
            held.accepted();
        }
        Answer::Missed(Missed::Declined(_, decline)) => {
            crossing.declined(cell, *decline, held.cell_lease());
        }
        Answer::Missed(Missed::Unsent(_)) | Answer::Failed(_) => {}
    }
    answer
}

impl Runtime {
    /// Choose, for `envelope`, a cell of the class `crossing` enters that
    /// is not in `offered`, by the class's cell policy, then an instance
    /// of it by the hop's router: the instance, the cell's
    /// [`id`](super::CellTable::id), and what the offer holds while the
    /// request is held, a selector's lease and the lease on the cell's
    /// follow, with the selector's meta added to `envelope`. [`NoCell`]
    /// when no such cell may take it.
    pub(crate) async fn choose_cell(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        offered: &[u64],
    ) -> Result<Result<(InstanceId, u64, Held), NoCell>> {
        match crossing.within() {
            Within::Policy(policy) => Ok(crossing
                .pick(cx, &envelope.payload, *policy, self.random(), offered)?
                .map(|(instance, cell, lease)| (instance, cell, Held::cell(None, lease)))),
            Within::Selector {
                selector, encoding, ..
            } => {
                let chosen = crossing
                    .choose(&**selector, cx, &envelope.payload, *encoding, offered)
                    .await?;
                Ok(chosen.map(|(instance, cell, picked, lease)| {
                    envelope.cx.add_meta(picked.meta);
                    (instance, cell, Held::cell(picked.lease, lease))
                }))
            }
        }
    }

    /// After the first offer of `envelope` missed (`declined`), offer it
    /// again, as the first was chosen, until a cell takes it, the request
    /// fails, or `cell_failovers` more cells were offered it. An entry
    /// instance that declined it as its own ([`Decline::Instance`]) leaves
    /// the request in its cell: each other entry instance of the cell is
    /// offered it once ([`Crossing::walk`]), before the chooser offers it
    /// to another cell, and those offers use none of `cell_failovers`.
    /// The cell policy's lease on the choice of that cell passes to each
    /// of those offers, so that it learns of an acceptance by any of them.
    /// Any other miss leaves the cell at once. When no cell may take it
    /// any more, or no failover is left, the request fails with the last
    /// refusal, or, when no cell refused it, with the last hand-off that
    /// never left. When the chooser holds every cell it left gone and its
    /// cell policy chooses no other, it fails with `Routing`, as the
    /// requests after it do, which find those cells gone before any
    /// offer ([`Crossing::gone_only`]). Each choice starts from the request's own context,
    /// `cx`, which `envelope`'s was made from, so a declined selector's
    /// meta is not sent to the next instance; a declined choice's leases,
    /// its selector's and its cell's, end before the next choice. It
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
            mut cell,
            mut instance,
            mut choice,
            mut missed,
        } = declined;
        let mut last = reason(&missed);
        let mut offered = Vec::new();
        let mut walk: Option<Walk> = None;
        loop {
            if let Within::Selector { .. } = crossing.within() {
                envelope.cx = WireCx::of(cx);
            }
            let mut next = None;
            if last == Some(Decline::Instance) {
                if walk.is_none() {
                    walk = crossing.walk(cell, instance);
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
                    // The request leaves the cell, and the cell policy's
                    // lease on it ends before the next choice.
                    walk = None;
                    drop(choice.take());
                    offered.push(cell);
                    if offered.len() > self.config.cell_failovers {
                        break;
                    }
                    let Ok(chosen) = self.choose_cell(crossing, cx, envelope, &offered).await?
                    else {
                        break;
                    };
                    chosen
                }
            };
            let (offering, sent) = self.deliver(to, envelope, timed)?;
            let answer = answer(offering.await, crossing, at, &mut held);
            // Counted with the answer, so that an offer whose answer is
            // never read counts nothing.
            if let Some(figures) = held.cell_lease().and_then(CellLease::figures) {
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
                    choice = held.take_choice();
                    drop(held);
                    (cell, instance) = (at, to);
                    missed = missed.then(then);
                }
            }
        }
        // A refusal by a cell that may take work again says more about
        // this request than the policy's reason for no cell. When every
        // cell it left is gone, the requests after it meet the policy's
        // `Routing`, and so does this one.
        if let Some(no) = crossing.gone_only(cx, &envelope.payload, &offered)? {
            return Err(crossing.no_cell(no));
        }
        Err(missed.into_error())
    }

    /// The next instance of `walk`, chosen as the first offer's was, with
    /// the cell's id and what the offer holds, `choice` among it; `None`
    /// once the walk has offered every instance of its cell.
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
                .map(|(instance, cell, lease)| (instance, cell, Held::cell(None, lease)))),
            Within::Selector {
                selector, encoding, ..
            } => {
                let chosen = crossing
                    .choose_in(walk, &**selector, cx, &envelope.payload, *encoding, choice)
                    .await?;
                Ok(chosen.map(|(instance, cell, picked, lease)| {
                    envelope.cx.add_meta(picked.meta);
                    (instance, cell, Held::cell(picked.lease, lease))
                }))
            }
        }
    }
}

/// What a chooser's first offer into another class came to, when it
/// missed: the cell and the instance it went to, the cell policy's lease
/// on the choice of that cell, if it gave one, and the miss.
pub(crate) struct Declined {
    pub(crate) cell: u64,
    pub(crate) instance: InstanceId,
    pub(crate) choice: Option<ChoiceHeld>,
    pub(crate) missed: Missed,
}

/// Why `missed`'s entry declined, or `None` for a hand-off that never
/// left: [`Decline::Instance`] leaves the request in its cell.
fn reason(missed: &Missed) -> Option<Decline> {
    match missed {
        Missed::Declined(_, decline) => Some(*decline),
        Missed::Unsent(_) => None,
    }
}

#[cfg(test)]
mod instances;

#[cfg(test)]
mod tests {
    use gdl::{Error, ErrorKind};
    use std::cell::Cell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gdl::__runtime::Typed;
    use gdl_cells::CellState;

    use super::super::{CellTable, Cells, EntryTable, cell_id, targets};
    use super::*;
    use crate::dist::counting::{Counted, counted, counting};
    use crate::dist::envelope::{Accepted, VERSION, WireCx};
    use crate::dist::fabric::{Fabric, ReplyTo, bind_node};
    use crate::dist::offers::Decline;
    use crate::dist::roster::{Policy, Roster};
    use crate::dist::runtime::{Router, RuntimeConfig};

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
        layout_under(gdl_model::CellPolicy::default())
    }

    /// [`layout`], choosing a cell of `model_x` by `policy`.
    pub(super) fn layout_under(policy: gdl_model::CellPolicy) -> gdl_model::Layout {
        let manifest = gdl_model::Manifest {
            name: "cells".into(),
            schema: 1,
            nodes: vec![
                gdl_model::NodeInfo {
                    name: "S".into(),
                    role: gdl_model::Role::Source,
                    binding: None,
                },
                gdl_model::NodeInfo {
                    name: "D".into(),
                    role: gdl_model::Role::Sink,
                    binding: Some("d".into()),
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
        };
        let placement = gdl_model::Placement::new()
            .segment("a", ["S"])
            .segment("d", ["D"])
            .class("global", ["a"])
            .class("model_x", ["d"])
            .cell_policy("model_x", policy);
        gdl_model::layout(&manifest, &placement).expect("valid test fixture")
    }

    /// The allocations each of `routers` makes to send one hand-off that
    /// is accepted, from the instance of `a` that `rt` is to the one
    /// entry instance of `d`: the fewest over a few sends, after a first
    /// that meets the peer.
    async fn allocations(rt: &Runtime, routers: &[Router]) -> Vec<u64> {
        let mut fewest = vec![u64::MAX; routers.len()];
        for round in 0..31 {
            for (at, router) in routers.iter().enumerate() {
                let mut cx = Cx::new();
                cx.insert_meta("session", "s1");
                let envelope = Forward {
                    version: VERSION,
                    identity: rt.identity,
                    hop: 0,
                    pos: 1,
                    cx: WireCx::of(&cx),
                    reply: ReplyTo::test_value(7),
                    payload: vec![1, 2, 3].into(),
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
    /// before a cell declines, and counting the offer's outcome allocates
    /// nothing.
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
        let meters = super::super::CellMeters::register(&registry).expect("registered");
        let cells = Cells::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
        );
        let id = entry.instance_id();
        cells.store(
            "d",
            EntryTable::new(Box::new([Arc::new(CellTable {
                class: "model_x".into(),
                cell: "m1".into(),
                id: cell_id("model_x", "m1"),
                generation: 1,
                region: "home".into(),
                state: CellState::Ready,
                load: super::super::tests::idle(),
                eligible: Box::new([id]),
                offered: Box::new([crate::dist::roster::selector_id(id)]),
                retired: Box::new([]),
                unread: false,
                turn: Arc::new(AtomicUsize::new(0)),
                offering: true,
                revision: 1,
                changes: 0,
                leases: Arc::new(super::super::lease::Leases::counted(Some(
                    meters.figures("model_x", "m1"),
                ))),
            })])),
        );
        let crossing = |within: Within| {
            let mut targets = targets(
                &layout,
                "a",
                &cells,
                &gdl::CellPolicies::new(),
                gdl::HomeFirstAt::DEFAULT,
            )
            .expect("targets");
            let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
            Router::Cells(Arc::new(Crossing::new(target, within, Default::default())))
        };
        let policy = Within::Policy(Policy::RoundRobin);
        let selector = Within::Selector {
            name: Arc::from("replacing"),
            selector: Arc::new(Typed(Arc::new(Replacing))),
            encoding: Default::default(),
        };
        let routers = [
            Router::Roster(policy.clone()),
            crossing(policy),
            Router::Roster(selector.clone()),
            crossing(selector),
        ];
        let writes = super::super::damping::WRITES.with(Cell::get);
        let made = allocations(&rt, &routers).await;
        assert_eq!(made[1], made[0], "with a policy: {made:?}");
        assert_eq!(made[3], made[2], "with a selector: {made:?}");
        assert_eq!(
            super::super::damping::WRITES.with(Cell::get),
            writes,
            "an accepted offer to an undamped cell wrote damping state"
        );
        // Each crossing sent 31 hand-offs, every one counted.
        assert_eq!(
            super::super::meter::tests::offers(&registry, "m1", "accepted"),
            Some(62.0)
        );
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

    /// A failover chooses the next cell's instance with the selector as
    /// the first offer did, reading the head in the edge's encoding: the
    /// first offer, to either cell, is declined, and the other cell's
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
        let cells = Cells::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
        );
        let tables = entries
            .iter()
            .zip(["m1", "m2"])
            .map(|(entry, name)| {
                let id = entry.instance_id();
                Arc::new(CellTable {
                    class: "model_x".into(),
                    cell: name.into(),
                    id: cell_id("model_x", name),
                    generation: 1,
                    region: "home".into(),
                    state: CellState::Ready,
                    load: super::super::tests::idle(),
                    eligible: Box::new([id]),
                    offered: Box::new([crate::dist::roster::selector_id(id)]),
                    retired: Box::new([]),
                    unread: false,
                    turn: Arc::new(AtomicUsize::new(0)),
                    offering: true,
                    revision: 1,
                    changes: 0,
                    leases: Arc::default(),
                })
            })
            .collect();
        cells.store("d", EntryTable::new(tables));
        let mut targets = targets(
            &layout,
            "a",
            &cells,
            &gdl::CellPolicies::new(),
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        let reading = Arc::new(Reading(AtomicUsize::new(0)));
        let router = Router::Cells(Arc::new(Crossing::new(
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
        .expect("the second cell took it");
        assert_eq!(offers.load(Ordering::SeqCst), 2, "one decline, one take");
        assert_eq!(reading.0.load(Ordering::SeqCst), 2, "a choice per cell");
    }

    /// Every offer counts under its cell and how it ended, and each one
    /// after the request's first counts as a failover: `m0` has no entry
    /// velo can reach, so its offers never leave; `m1`'s entry declines
    /// as full, `m2`'s as gone, and `m3`'s accepts. Each request ends at
    /// `m3`, so the outcomes add up to the offers made, and the failovers
    /// to the offers less the requests.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_offer_counts_under_its_cell_and_outcome() {
        use super::super::lease::Leases;
        use super::super::meter::tests::{offers, scraped};
        use super::super::meter::{CellMeters, FAILOVERS};
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
                cell_failovers: 3,
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
        let meters = CellMeters::register(&registry).expect("registered");
        let cells = Cells::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
        );
        // `m0`'s entry is no peer of the chooser's velo node.
        let ids = std::iter::once(velo::InstanceId::new_v4())
            .chain(entries.iter().map(|entry| entry.instance_id()));
        let tables = ids
            .zip(["m0", "m1", "m2", "m3"])
            .map(|(id, name)| {
                Arc::new(CellTable {
                    class: "model_x".into(),
                    cell: name.into(),
                    id: cell_id("model_x", name),
                    generation: 1,
                    region: "home".into(),
                    state: CellState::Ready,
                    load: super::super::tests::idle(),
                    eligible: Box::new([id]),
                    offered: Box::new([crate::dist::roster::selector_id(id)]),
                    retired: Box::new([]),
                    unread: false,
                    turn: Arc::new(AtomicUsize::new(0)),
                    offering: true,
                    revision: 1,
                    changes: 0,
                    leases: Arc::new(Leases::counted(Some(meters.figures("model_x", name)))),
                })
            })
            .collect();
        cells.store("d", EntryTable::new(tables));
        let mut targets = targets(
            &layout,
            "a",
            &cells,
            &gdl::CellPolicies::new(),
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        let router = Router::Cells(Arc::new(Crossing::new(
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
        let count = |cell: &str, outcome: &str| {
            offers(&registry, cell, outcome)
                .unwrap_or_else(|| panic!("no series for {cell} {outcome}")) as usize
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
            "unsent",
            "outcome_unknown",
            "failed",
        ];
        let (mut offered, mut failovers) = (0, 0);
        for cell in ["m0", "m1", "m2", "m3"] {
            offered += outcomes.iter().map(|o| count(cell, o)).sum::<usize>();
            let labels = [("class", "model_x"), ("cell", cell)];
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
        use super::super::meter::{CellMeters, FAILOVERS};

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
        let meters = CellMeters::register(&registry).expect("registered");
        let cells = Cells::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
        );
        let tables = [
            ("m0", velo::InstanceId::new_v4()),
            ("m1", entry.instance_id()),
        ]
        .into_iter()
        .map(|(name, id)| {
            Arc::new(CellTable {
                class: "model_x".into(),
                cell: name.into(),
                id: cell_id("model_x", name),
                generation: 1,
                region: "home".into(),
                state: CellState::Ready,
                load: super::super::tests::idle(),
                eligible: Box::new([id]),
                offered: Box::new([crate::dist::roster::selector_id(id)]),
                retired: Box::new([]),
                unread: false,
                turn: Arc::new(AtomicUsize::new(0)),
                offering: true,
                revision: 1,
                changes: 0,
                leases: Arc::new(Leases::counted(Some(meters.figures("model_x", name)))),
            })
        })
        .collect();
        cells.store("d", EntryTable::new(tables));
        let mut targets = targets(
            &layout,
            "a",
            &cells,
            &gdl::CellPolicies::new(),
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let router = Router::Cells(Arc::new(Crossing::new(
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
                &[("class", "model_x"), ("cell", "m1")],
            ),
            Some(0.0),
            "a dropped unanswered offer must not leave a failover count"
        );
    }

    /// Sends a prompt to the cell whose name is its text, and says why
    /// when there is none or that cell declined.
    struct ByText;

    impl gdl::CellPolicy for ByText {
        type Head = Prompt;

        fn choose(
            &self,
            request: gdl::CellRequest<'_, Prompt>,
            cells: &[gdl::Cell],
        ) -> std::result::Result<gdl::CellChoice, NoCell> {
            let text = &request.head().text;
            let at = cells
                .iter()
                .position(|cell| cell.name() == text)
                .ok_or_else(|| NoCell::because(format!("no cell is named `{text}`")))?;
            if request.is_excluded(&cells[at]) {
                return Err(NoCell::because(format!("cell `{text}` declined")));
            }
            Ok(gdl::CellChoice::new(at))
        }
    }

    /// A registered cell policy reads the head, in the edge's encoding,
    /// and chooses the cell: two requests that differ only in the field
    /// it reads are offered to different cells, each to the one it names,
    /// over velo. A head it cannot read fails the request with `Codec`,
    /// and offers it to no cell. A head that names no cell fails with
    /// `Routing` and the policy's reason. A cell that declines leaves the
    /// request its refusal, not the reason the policy gives for choosing
    /// no other cell.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_registered_policy_chooses_the_cell_by_the_head() {
        let layout = layout_under(gdl_model::CellPolicy {
            policy: "by_text".into(),
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
        let mut policies = gdl::CellPolicies::new();
        policies.add("by_text", ByText).expect("registered");
        let fabric = Fabric::new(chooser, None, None, Duration::from_secs(30));
        let rt = Runtime::new(
            fabric,
            layout.clone(),
            "a".into(),
            &roster,
            RuntimeConfig {
                cell_policies: policies.clone(),
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
        let cells = Cells::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
        );
        let tables = entries
            .iter()
            .zip(["m1", "m2"])
            .map(|(entry, name)| {
                let id = entry.instance_id();
                Arc::new(CellTable {
                    class: "model_x".into(),
                    cell: name.into(),
                    id: cell_id("model_x", name),
                    generation: 1,
                    region: "home".into(),
                    state: CellState::Ready,
                    load: super::super::tests::idle(),
                    eligible: Box::new([id]),
                    offered: Box::new([crate::dist::roster::selector_id(id)]),
                    retired: Box::new([]),
                    unread: false,
                    turn: Arc::new(AtomicUsize::new(0)),
                    offering: true,
                    revision: 1,
                    changes: 0,
                    leases: Arc::default(),
                })
            })
            .collect();
        cells.store("d", EntryTable::new(tables));
        let mut targets =
            targets(&layout, "a", &cells, &policies, gdl::HomeFirstAt::DEFAULT).expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        let router = Router::Cells(Arc::new(Crossing::new(
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
        let error = send(prompt("m3")).await.expect_err("no cell is m3");
        assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
        assert!(
            error.to_string().contains("no cell is named `m3`"),
            "{error}"
        );
        let error = send(b"not json".to_vec())
            .await
            .expect_err("no head to read");
        assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
        assert_eq!(offered(), [1, 2], "a request no cell may take was offered");
        full.store(true, Ordering::SeqCst);
        let error = send(prompt("m1")).await.expect_err("m1 declined");
        assert_eq!(offered(), [2, 2], "m1 was offered it once");
        assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
        assert!(
            error.to_string().contains("full") && !error.to_string().contains("declined"),
            "{error}"
        );
    }

    /// The crossing into `model_x` of [`layout`], over `cells`, each with
    /// one entry instance and an idle summary.
    fn crossing_over(names: &[&str]) -> Crossing {
        followed_over(names).0
    }

    /// [`crossing_over`], with the tables it chooses from.
    fn followed_over(names: &[&str]) -> (Crossing, Cells) {
        followed_under(&layout(), names)
    }

    /// [`followed_over`], for the crossing of `layout`.
    fn followed_under(layout: &gdl_model::Layout, names: &[&str]) -> (Crossing, Cells) {
        followed_with(layout, names, &gdl::CellPolicies::new())
    }

    /// [`followed_under`], with `policies` registered.
    fn followed_with(
        layout: &gdl_model::Layout,
        names: &[&str],
        policies: &gdl::CellPolicies,
    ) -> (Crossing, Cells) {
        let cells = Cells::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
        );
        let tables = names
            .iter()
            .map(|name| {
                let id = velo::InstanceId::new_v4();
                Arc::new(CellTable {
                    class: "model_x".into(),
                    cell: (*name).into(),
                    id: cell_id("model_x", name),
                    generation: 1,
                    region: "home".into(),
                    state: CellState::Ready,
                    load: super::super::tests::idle(),
                    eligible: Box::new([id]),
                    offered: Box::new([crate::dist::roster::selector_id(id)]),
                    retired: Box::new([]),
                    unread: false,
                    turn: Arc::new(AtomicUsize::new(0)),
                    offering: true,
                    revision: 1,
                    changes: 0,
                    leases: Arc::default(),
                })
            })
            .collect();
        cells.store("d", EntryTable::new(tables));
        let mut targets =
            targets(layout, "a", &cells, policies, gdl::HomeFirstAt::DEFAULT).expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        (
            Crossing::new(
                target,
                Within::Policy(Policy::RoundRobin),
                Default::default(),
            ),
            cells,
        )
    }

    /// An acceptance ends the damping the cell has when it accepts, not
    /// the one it had when its offer was chosen: an offer chosen before a
    /// decline damped the cell ends that damping, and of two offers chosen
    /// while it was damped, only the first acceptance writes.
    #[test]
    fn an_acceptance_ends_the_damping_the_cell_has_when_it_accepts() {
        let cx = Cx::new();
        let writes = || super::super::damping::WRITES.with(Cell::get);
        let random = std::hash::RandomState::new();
        let (lone, cells) = followed_over(&["m1"]);
        let pick = || {
            lone.pick(&cx, &[], Policy::RoundRobin, &random, &[])
                .expect("a pick")
                .expect("a lone cell is always chosen")
                .1
        };
        let damped = || cells.followed("d")[0].damped.is_some();
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
        assert_eq!(writes() - wrote, 1, "an undamped cell's acceptance wrote");
    }

    /// `cells`' table of `name` replaced by the one `change` makes of it,
    /// as the follower makes a new table for each change the relay sends;
    /// the other cells' tables stay as they are.
    fn changed(cells: &Cells, name: &str, change: impl FnOnce(&mut CellTable)) {
        let table = cells.table("d").expect("a table");
        let mut change = Some(change);
        let tables = table
            .cells
            .iter()
            .map(|cell| {
                if cell.cell != name {
                    return Arc::clone(cell);
                }
                let mut next = CellTable {
                    class: cell.class.clone(),
                    cell: cell.cell.clone(),
                    id: cell.id,
                    generation: cell.generation,
                    region: cell.region.clone(),
                    state: cell.state,
                    load: cell.load,
                    eligible: cell.eligible.clone(),
                    offered: cell.offered.clone(),
                    retired: cell.retired.clone(),
                    unread: cell.unread,
                    turn: Arc::clone(&cell.turn),
                    offering: cell.offering,
                    revision: cell.revision,
                    changes: cell.changes,
                    leases: Arc::clone(&cell.leases),
                };
                change.take().expect("one cell of the name")(&mut next);
                Arc::new(next)
            })
            .collect();
        cells.store("d", EntryTable::new(tables));
    }

    /// A cell that declines an offer as gone, as a draining cell's entry
    /// does, is passed over until the relay changes its follow, however
    /// long that takes; a new load summary is no change. A full cell's
    /// decline, the control, damps the cell, which is chosen again once
    /// its window has passed. An acceptance ends a mark as gone. A gone
    /// decline of an offer made under an earlier generation damps the newer
    /// one instead. A lone gone cell is not chosen: the cap on damping
    /// does not hold it in the choice.
    #[tokio::test(start_paused = true)]
    async fn a_cell_that_declined_as_gone_is_passed_over_until_its_follow_changes() {
        const FIRST_WINDOW: Duration = crate::RuntimeConfig::DEFAULT_CELL_DAMPING_FIRST;
        const MAX_WINDOW: Duration = crate::RuntimeConfig::DEFAULT_CELL_DAMPING_MAX;

        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let (crossing, cells) = followed_over(&["m1", "m2", "m3"]);
        let m1 = cell_id("model_x", "m1");
        let pick = || {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                .expect("a pick")
                .expect("a cell")
        };
        // An offer to `m1`, with its lease.
        let offer = || loop {
            let (_, cell, lease) = pick();
            if cell == m1 {
                return lease;
            }
        };
        let chosen = || (0..12).any(|_| pick().1 == m1);
        let shown = || {
            cells
                .followed("d")
                .into_iter()
                .find(|cell| cell.cell == "m1")
                .expect("followed")
        };

        let lease = offer();
        crossing.declined(m1, Decline::Full, Some(&lease));
        drop(lease);
        assert!(!chosen(), "chosen while its window lasts");
        tokio::time::advance(FIRST_WINDOW).await;
        assert!(
            chosen(),
            "a full cell was not chosen once its window passed"
        );

        let lease = offer();
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        let m = shown();
        assert!(m.gone && m.damped.is_none(), "{m:?}");
        tokio::time::advance(MAX_WINDOW * 2).await;
        assert!(!chosen(), "chosen again while its follow is unchanged");
        changed(&cells, "m1", |cell| cell.load = super::super::tests::idle());
        assert!(!chosen(), "a new load summary ended it");
        changed(&cells, "m1", |cell| cell.changes += 1);
        assert!(chosen(), "not chosen once the relay changed its state");
        assert!(!shown().gone);

        let lease = offer();
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        assert!(!chosen());
        crossing.accepted(m1);
        assert!(chosen(), "not chosen once it accepted an offer");

        let lease = offer();
        changed(&cells, "m1", |cell| {
            cell.generation += 1;
            cell.leases = Arc::default();
        });
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        let m = shown();
        assert!(!m.gone && m.damped.is_some(), "{m:?}");
        crossing.accepted(m1);

        let (lone, _cells) = followed_over(&["m1"]);
        let (_, cell, lease) = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, &[])
            .expect("a pick")
            .expect("a lone cell");
        lone.declined(cell, Decline::Gone, Some(&lease));
        drop(lease);
        let no = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, &[])
            .expect("a pick")
            .expect_err("a lone gone cell was chosen");
        assert_eq!(no, NoCell::new());
    }

    /// Replies to offers already in flight can arrive in either order.
    /// A full reply gives no evidence that a gone cell takes work again.
    #[tokio::test(start_paused = true)]
    async fn a_late_full_reply_preserves_a_gone_cell() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let (crossing, cells) = followed_over(&["m1"]);
        let pick = || {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                .expect("a pick")
        };
        let (_, cell, first) = pick().expect("the first offer");
        let (_, same, second) = pick().expect("the second offer");
        assert_eq!(cell, same);
        crossing.declined(cell, Decline::Gone, Some(&first));
        drop(first);
        assert!(cells.followed("d")[0].gone);
        assert!(pick().is_err(), "the gone cell was chosen");

        crossing.declined(cell, Decline::Full, Some(&second));
        drop(second);
        tokio::time::advance(crate::RuntimeConfig::DEFAULT_CELL_DAMPING_MAX * 2).await;
        let shown = &cells.followed("d")[0];
        assert!(
            shown.gone && pick().is_err(),
            "a full reply revived a gone cell without a follow change: {shown:?}"
        );
    }

    /// A node that reads the chooser's tables holds a cell gone exactly
    /// while the chooser's choice passes it over: from its gone decline,
    /// across a new load summary, until the relay changes its follow or it
    /// accepts an offer. A full decline, the control, damps the cell and
    /// is not gone.
    #[tokio::test(start_paused = true)]
    async fn a_node_reads_a_cell_gone_while_the_choice_passes_it_over() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let (crossing, cells) = followed_over(&["m1", "m2"]);
        let node = gdl::FollowedCells::new();
        assert!(node.bind(Arc::new(cells.clone())));
        let m1 = cell_id("model_x", "m1");
        let pick = || {
            crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                .expect("a pick")
                .expect("a cell")
        };
        let offer = || loop {
            let (_, cell, lease) = pick();
            if cell == m1 {
                return lease;
            }
        };
        let chosen = || (0..12).any(|_| pick().1 == m1);
        let read = || {
            node.read("model_x", |cells| {
                (cells.is_gone("m1"), cells.is_gone("m2"))
            })
            .expect("the chooser sends to model_x")
        };
        assert_eq!(read(), (false, false));

        let lease = offer();
        crossing.declined(m1, Decline::Full, Some(&lease));
        drop(lease);
        assert_eq!(read(), (false, false), "a full cell read gone");
        crossing.accepted(m1);

        let lease = offer();
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        assert!(!chosen());
        assert_eq!(read(), (true, false), "the chooser passes m1 over");
        changed(&cells, "m1", |cell| cell.load = super::super::tests::idle());
        assert!(!chosen());
        assert_eq!(read(), (true, false), "a new load summary ended it");
        changed(&cells, "m1", |cell| cell.changes += 1);
        assert!(chosen());
        assert_eq!(read(), (false, false), "the follow changed");

        let lease = offer();
        crossing.declined(m1, Decline::Gone, Some(&lease));
        drop(lease);
        assert_eq!(read(), (true, false));
        crossing.accepted(m1);
        assert_eq!(read(), (false, false), "m1 accepted an offer");
    }

    /// A pick whose cell accepts allocates nothing and, while the cell is
    /// not damped, writes no damping state: an acceptance by an undamped
    /// cell costs a branch. A decline writes, and so does the acceptance
    /// that ends a cell's damping, once.
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
                .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                .expect("a pick")
                .expect("a cell");
            crossing.accepted(chosen);
        }
        counting(false);
        assert_eq!(counted() - before, 0, "a pick allocated");
        assert_eq!(writes() - wrote, 0, "an accepted pick wrote damping state");

        let lone = crossing_over(&["m1"]);
        let (_, chosen, _) = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, &[])
            .expect("a pick")
            .expect("a cell");
        lone.declined(chosen, Decline::Full, None);
        assert_eq!(writes() - wrote, 1, "a decline wrote nothing");
        // A lone cell is never damped out of the choice.
        let (_, chosen, _) = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, &[])
            .expect("a pick")
            .expect("a cell");
        lone.accepted(chosen);
        assert_eq!(writes() - wrote, 2, "the acceptance kept the damping");
        let (_, chosen, _) = lone
            .pick(&cx, &[], Policy::RoundRobin, &random, &[])
            .expect("a pick")
            .expect("a cell");
        lone.accepted(chosen);
        assert_eq!(writes() - wrote, 2, "an undamped cell's acceptance wrote");
    }

    /// What a cell choice and an accepted first offer cost, in time, for
    /// the record in the book: a pick under each policy the runtime
    /// provides, over four idle cells, the count of an offer's outcome,
    /// and one accepted offer over loopback, its outcome counted, against
    /// the same hand-off routed by the roster. Ignored: it times,
    /// and asserts nothing. Run it in release:
    /// `cargo test --release -p gdl-runtime --lib cost_of -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "a timing, not a check"]
    async fn cost_of_a_cell_choice_and_an_offer() {
        const PICKS: u32 = 2_000_000;
        let random = std::hash::RandomState::new();
        let cx = Cx::new();
        for name in ["round_robin", "home_first"] {
            let layout = layout_under(gdl_model::CellPolicy {
                policy: name.into(),
            });
            let (crossing, _cells) = followed_under(&layout, &["m1", "m2", "m3", "m4"]);
            let mut best = Duration::MAX;
            for _ in 0..5 {
                let start = std::time::Instant::now();
                for _ in 0..PICKS {
                    let _ = std::hint::black_box(
                        crossing
                            .pick(&cx, &[], Policy::RoundRobin, &random, &[])
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
        // What an offer adds to count its outcome under its cell.
        let meters = super::super::CellMeters::unregistered();
        let figures = meters.figures("model_x", "m1");
        let mut best = Duration::MAX;
        for _ in 0..5 {
            let start = std::time::Instant::now();
            for _ in 0..PICKS {
                std::hint::black_box(figures).offered(super::super::meter::Outcome::Accepted);
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
        let cells = Cells::new(
            "home",
            [("model_x", "d")],
            super::super::damping::Damping::default(),
        );
        let id = entry.instance_id();
        cells.store(
            "d",
            EntryTable::new(Box::new([Arc::new(CellTable {
                class: "model_x".into(),
                cell: "m1".into(),
                id: cell_id("model_x", "m1"),
                generation: 1,
                region: "home".into(),
                state: CellState::Ready,
                load: super::super::tests::idle(),
                eligible: Box::new([id]),
                offered: Box::new([crate::dist::roster::selector_id(id)]),
                retired: Box::new([]),
                unread: false,
                turn: Arc::new(AtomicUsize::new(0)),
                offering: true,
                revision: 1,
                changes: 0,
                // Counted, as a chooser whose listener is on counts.
                leases: Arc::new(super::super::lease::Leases::counted(Some(figures))),
            })])),
        );
        let mut targets = targets(
            &layout,
            "a",
            &cells,
            &gdl::CellPolicies::new(),
            gdl::HomeFirstAt::DEFAULT,
        )
        .expect("targets");
        let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
        let policy = Within::Policy(Policy::RoundRobin);
        let routers = [
            ("roster", Router::Roster(policy.clone())),
            (
                "cells",
                Router::Cells(Arc::new(Crossing::new(target, policy, Default::default()))),
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

    /// Chooses as `home_first` or `round_robin` among every cell but `m2`,
    /// and builds a reason when none is left.
    struct KeptBut(bool);

    impl gdl::CellPolicy for KeptBut {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            request: gdl::CellRequest<'_, gdl::Encoded>,
            cells: &[gdl::Cell],
        ) -> std::result::Result<gdl::CellChoice, NoCell> {
            let keep = |cell: &gdl::Cell| cell.name() != "m2";
            let at = if self.0 {
                gdl::HomeFirst.choose_where(request, cells, keep)
            } else {
                gdl::RoundRobin.choose_where(request, cells, keep)
            };
            at.map(gdl::CellChoice::new)
                .ok_or_else(|| NoCell::because(format!("none of {} cells is kept", cells.len())))
        }
    }

    /// A pick allocates nothing under either policy the runtime provides,
    /// nor under a registered one that runs either over the cells it
    /// keeps, also while a cell's damping window lasts, which takes the
    /// pass that sets which damped cells are passed over, with a cell
    /// excluded, and with a cell gone, which is never chosen. The
    /// registered ones build a reason when they choose no cell, which
    /// allocates then and only then.
    #[test]
    fn a_pick_allocates_nothing_under_either_policy() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let mut policies = gdl::CellPolicies::new();
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
            let layout = layout_under(gdl_model::CellPolicy {
                policy: name.into(),
            });
            let (crossing, cells) =
                followed_with(&layout, &["m1", "m2", "m3", "m4", "m5"], &policies);
            let excluded = [cell_id("model_x", "m4")];
            crossing.declined(cell_id("model_x", "m1"), Decline::Full, None);
            crossing.declined(cell_id("model_x", "m2"), Decline::Full, None);
            cells
                .damping()
                .gone(&cells.table("d").expect("a table").views[4]);
            let damped = cells.followed("d");
            assert!(damped[0].damped.is_some() && damped[1].damped.is_some());
            assert!(damped[4].gone);
            let before = counted();
            // Made before counting, so that the record of the choices
            // allocates nothing while it counts.
            let mut chosen = Vec::with_capacity(1000);
            counting(true);
            for _ in 0..1000 {
                let (_, cell, _) = crossing
                    .pick(&cx, &[], Policy::RoundRobin, &random, &excluded)
                    .expect("a pick")
                    .expect("a cell");
                chosen.push(cell);
            }
            counting(false);
            let chosen: std::collections::BTreeSet<u64> = chosen.into_iter().collect();
            assert_eq!(counted() - before, 0, "a {name} pick allocated");
            // The undamped cell is chosen, and the excluded one never.
            assert!(chosen.contains(&cell_id("model_x", "m3")), "{name}");
            assert!(!chosen.contains(&excluded[0]), "{name}");
            assert!(!chosen.contains(&cell_id("model_x", "m5")), "{name}");
            if name.starts_with("kept") {
                assert!(!chosen.contains(&cell_id("model_x", "m2")), "{name}");
            }
            let every = ["m1", "m2", "m3", "m4"].map(|cell| cell_id("model_x", cell));
            let before = counted();
            counting(true);
            let no = crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, &every)
                .expect("a pick")
                .expect_err("no cell");
            counting(false);
            let built = counted() - before;
            if name.starts_with("kept") {
                assert_eq!(no.reason(), Some("none of 5 cells is kept"));
                assert!(built > 0, "a {name} reason was built");
            } else {
                assert_eq!((no, built), (NoCell::new(), 0), "{name}");
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

    impl gdl::CellPolicy for Leasing {
        type Head = gdl::Encoded;

        fn choose(
            &self,
            request: gdl::CellRequest<'_, gdl::Encoded>,
            cells: &[gdl::Cell],
        ) -> std::result::Result<gdl::CellChoice, NoCell> {
            let at = gdl::RoundRobin
                .choose_where(request, cells, |_| true)
                .ok_or_else(NoCell::new)?;
            Ok(gdl::CellChoice::new(at).with_lease(Idle(0)))
        }
    }

    /// A choice with a lease allocates twice per pick, the lease and the
    /// thin box it is held in, and the pick nothing more.
    #[test]
    fn a_choice_with_a_lease_allocates_twice() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        let mut policies = gdl::CellPolicies::new();
        policies.add("leasing", Leasing).expect("registered");
        let layout = layout_under(gdl_model::CellPolicy {
            policy: "leasing".into(),
        });
        let (crossing, _cells) = followed_with(&layout, &["m1", "m2"], &policies);
        let before = counted();
        counting(true);
        for _ in 0..1000 {
            let (_, cell, lease) = crossing
                .pick(&cx, &[], Policy::RoundRobin, &random, &[])
                .expect("a pick")
                .expect("a cell");
            crossing.accepted(cell);
            drop(lease);
        }
        counting(false);
        assert_eq!(counted() - before, 2000);
    }
}
