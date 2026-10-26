//! A chooser's hand-off into another class is an offer, as every routed
//! hand-off is ([`crate::dist::offers`]): the cell chosen for it may
//! decline it, and then another cell is chosen inside the same call. Only
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

use gdl::{Cx, Result};
use velo::InstanceId;

use super::Crossing;
use crate::dist::envelope::{Forward, WireCx};
use crate::dist::fabric::Offered;
use crate::dist::offers::{Answer, Missed, delivered};
use crate::dist::runtime::{Handed, Held, Runtime, Within};

/// What became of an offer, as `crossing` sent it to the cell `cell`.
/// A cell that declined it is damped, and a damped cell that accepted it
/// is no longer; a hand-off that never left says nothing of the cell, and
/// changes nothing. An unknown outcome names the class, since it is not
/// offered to another cell.
pub(crate) fn answer(offered: Offered, crossing: &Crossing, cell: u64) -> Answer {
    let answer = delivered(offered, Some(crossing.class()));
    match &answer {
        Answer::Accepted(_) => crossing.accepted(cell),
        Answer::Missed(Missed::Declined(_)) => crossing.declined(cell),
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
    /// follow, with the selector's meta added to `envelope`. `None` when
    /// no such cell may take it.
    pub(crate) async fn choose_cell(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        offered: &[u64],
    ) -> Result<Option<(InstanceId, u64, Held)>> {
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

    /// After the cell `cell` did not take `envelope` (`missed`), offer it
    /// to the cells not yet offered it, as the first was chosen, until one
    /// takes it, the request fails, or `cell_failovers` more were offered
    /// it. When no cell may take it any more, or no failover is left, the
    /// request fails with the last refusal, or, when no cell refused it,
    /// with the last hand-off that never left. Each choice starts from the
    /// request's own context, `cx`, which `envelope`'s was made from, so a
    /// declined selector's meta is not sent to the next cell; a declined
    /// choice's leases, its selector's and its cell's, end before the next
    /// choice. It borrows the envelope from `send_forward`'s future.
    ///
    /// A chooser never chooses itself: the entry segment is in another
    /// class, so the hand-off always crosses velo.
    pub(crate) async fn fail_over(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        timed: bool,
        cell: u64,
        missed: Missed,
    ) -> Result<Handed> {
        let mut offered = vec![cell];
        let mut missed = missed;
        while offered.len() <= self.config.cell_failovers {
            if let Within::Selector { .. } = crossing.within() {
                envelope.cx = WireCx::of(cx);
            }
            let Some((instance, cell, held)) =
                self.choose_cell(crossing, cx, envelope, &offered).await?
            else {
                break;
            };
            let (offering, sent) = self.deliver(instance, envelope, timed)?;
            match answer(offering.await, crossing, cell) {
                Answer::Accepted(accepted) => {
                    return Ok(Handed {
                        accepted,
                        sent,
                        held,
                    });
                }
                Answer::Failed(error) => return Err(error),
                Answer::Missed(then) => {
                    offered.push(cell);
                    missed = missed.then(then);
                }
            }
        }
        Err(missed.into_error())
    }
}

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
    fn layout() -> gdl_model::Layout {
        layout_under(gdl_model::CellPolicy::default())
    }

    /// [`layout`], choosing a cell of `model_x` by `policy`.
    fn layout_under(policy: gdl_model::CellPolicy) -> gdl_model::Layout {
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
                    payload: vec![1, 2, 3],
                };
                let send = Counted(Box::pin(rt.send_forward("d", router, &cx, envelope, false)));
                let before = counted();
                tokio::time::timeout(Duration::from_secs(5), send)
                    .await
                    .expect("answered in time")
                    .expect("accepted");
                let made = counted() - before;
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
    /// before a cell declines.
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

        let cells = Cells::new("home", [("model_x", "d")]);
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
                leases: Arc::default(),
            })])),
        );
        let crossing = |within: Within| {
            let mut targets =
                targets(&layout, "a", &cells, &gdl::CellPolicies::new()).expect("targets");
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
        let cells = Cells::new("home", [("model_x", "d")]);
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
                    leases: Arc::default(),
                })
            })
            .collect();
        cells.store("d", EntryTable::new(tables));
        let mut targets =
            targets(&layout, "a", &cells, &gdl::CellPolicies::new()).expect("targets");
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
            .expect("encoded"),
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

    /// Sends a prompt to the cell whose name is its text.
    struct ByText;

    impl gdl::CellPolicy for ByText {
        type Head = Prompt;

        fn choose(
            &self,
            request: gdl::CellRequest<'_, Prompt>,
            cells: &[gdl::Cell],
        ) -> Option<usize> {
            let text = &request.head().text;
            cells.iter().position(|cell| cell.name() == text)
        }
    }

    /// A registered cell policy reads the head, in the edge's encoding,
    /// and chooses the cell: two requests that differ only in the field
    /// it reads are offered to different cells, each to the one it names,
    /// over velo. A head it cannot read fails the request with `Codec`,
    /// and offers it to no cell.
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
        let offers: Vec<Arc<AtomicUsize>> = entries.iter().map(|_| Arc::default()).collect();
        for (entry, offers) in entries.iter().zip(&offers) {
            let (offers, accepted) = (Arc::clone(offers), accepted.clone());
            Fabric::new(entry.clone(), None, None, Duration::from_secs(30))
                .register(&rt.names().forward, move |_| {
                    offers.fetch_add(1, Ordering::SeqCst);
                    let accepted = accepted.clone();
                    Box::pin(async move { accepted })
                })
                .expect("a handler");
        }
        let cells = Cells::new("home", [("model_x", "d")]);
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
                    leases: Arc::default(),
                })
            })
            .collect();
        cells.store("d", EntryTable::new(tables));
        let mut targets = targets(&layout, "a", &cells, &policies).expect("targets");
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
                payload,
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
        let error = send(b"not json".to_vec())
            .await
            .expect_err("no head to read");
        assert_eq!(error.kind(), ErrorKind::Codec, "{error}");
        assert_eq!(offered(), [1, 2], "a request no cell may take was offered");
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
        let cells = Cells::new("home", [("model_x", "d")]);
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
                    leases: Arc::default(),
                })
            })
            .collect();
        cells.store("d", EntryTable::new(tables));
        let mut targets = targets(layout, "a", &cells, &gdl::CellPolicies::new()).expect("targets");
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
        lone.declined(second);
        assert!(damped());
        lone.accepted(first);
        assert!(
            !damped(),
            "an offer chosen before the decline left it damped"
        );

        lone.declined(pick());
        let (first, second) = (pick(), pick());
        let wrote = writes();
        lone.accepted(first);
        lone.accepted(second);
        assert!(!damped());
        assert_eq!(writes() - wrote, 1, "an undamped cell's acceptance wrote");
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
        lone.declined(chosen);
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
    /// provides, over four idle cells, and one accepted offer over loopback
    /// against the same hand-off routed by the roster. Ignored: it times,
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
                    std::hint::black_box(
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
        let cells = Cells::new("home", [("model_x", "d")]);
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
                leases: Arc::default(),
            })])),
        );
        let mut targets =
            targets(&layout, "a", &cells, &gdl::CellPolicies::new()).expect("targets");
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
                        payload: vec![1, 2, 3],
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

    /// A pick allocates nothing under either policy the runtime provides,
    /// also while a cell's damping window lasts, which takes the pass that
    /// sets which damped cells are passed over, and with a cell excluded.
    #[test]
    fn a_pick_allocates_nothing_under_either_policy() {
        let cx = Cx::new();
        let random = std::hash::RandomState::new();
        for name in ["home_first", "round_robin"] {
            let layout = layout_under(gdl_model::CellPolicy {
                policy: name.into(),
            });
            let (crossing, cells) = followed_under(&layout, &["m1", "m2", "m3", "m4"]);
            let excluded = [cell_id("model_x", "m4")];
            crossing.declined(cell_id("model_x", "m1"));
            crossing.declined(cell_id("model_x", "m2"));
            let damped = cells.followed("d");
            assert!(damped[0].damped.is_some() && damped[1].damped.is_some());
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
        }
    }
}
