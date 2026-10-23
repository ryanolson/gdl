//! A chooser's hand-off into another class is an offer: the cell chosen
//! for it may decline it, and then another cell is chosen inside the same
//! call. Only a proven refusal moves the request on. A cell declined it
//! when its entry answered, before taking anything, that it has no room
//! or is not of this deployment; it never left when velo refused to send
//! it. Anything else after the send may have run the request there, so
//! it is the request's error, and is not tried again.
//!
//! The first offer is made where any hand-off is
//! (`Runtime::send_forward`), and costs what a hand-off to an instance
//! does: the cells already offered the request, and the failover's own
//! state, are made only once a cell declined it ([`Runtime::fail_over`]).
//! They are locals of that call: never on the wire, never kept per
//! request, and gone when it returns.

use gdl::__runtime::Lease;
use gdl::{Cx, Error, ErrorKind, Result};
use velo::InstanceId;

use super::{Crossing, Within};
use crate::dist::envelope::{self, Accepted, Forward, WireCx};
use crate::dist::fabric::Offered;
use crate::dist::runtime::{Handed, Runtime};

/// What an offer's answer means.
pub(crate) enum Answer {
    /// The cell's entry took the request.
    Accepted(Accepted),
    /// The cell declined it, or it never left: another cell may be
    /// offered it.
    Declined(Error),
    /// The request's error: no other cell is offered it.
    Failed(Error),
}

/// What became of an offer, as `crossing` sent it.
pub(crate) fn answer(offered: Offered, crossing: &Crossing) -> Answer {
    match offered {
        Offered::Answered(reply) => match envelope::decode::<Result<Accepted, Error>>(&reply) {
            Ok(Ok(accepted)) => Answer::Accepted(accepted),
            Ok(Err(error)) if declines(&error) => Answer::Declined(error),
            Ok(Err(error)) | Err(error) => Answer::Failed(error),
        },
        Offered::Unsent(error) => Answer::Declined(error),
        Offered::Unknown(error) => Answer::Failed(Error::new(
            ErrorKind::OutcomeUnknown,
            format!(
                "the hand-off into class `{}` may have been taken, so it is not offered again: {}",
                crossing.class(),
                error.message()
            ),
        )),
    }
}

impl Runtime {
    /// Choose, for `envelope`, a cell of the class `crossing` enters that
    /// is not in `offered`, by the class's cell policy, then an instance
    /// of it by the hop's router: the instance, the cell's
    /// [`id`](super::CellTable::id), and a selector's lease, with the
    /// selector's meta added to `envelope`. `None` when no such cell may
    /// take it.
    pub(crate) async fn choose_cell(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        envelope: &mut Forward,
        offered: &[u64],
    ) -> Result<Option<(InstanceId, u64, Option<Lease>)>> {
        match crossing.within() {
            Within::Policy(policy) => Ok(crossing
                .pick(*policy, self.random(), offered)
                .map(|(instance, cell)| (instance, cell, None))),
            Within::Selector { selector, .. } => {
                let chosen = crossing
                    .choose(&**selector, cx, &envelope.payload, offered)
                    .await?;
                Ok(chosen.map(|(instance, cell, picked)| {
                    envelope.cx.add_meta(picked.meta);
                    (instance, cell, picked.lease)
                }))
            }
        }
    }

    /// After the cell `cell` declined `envelope` with `refusal`, offer it
    /// to the cells not yet offered it, as the first was chosen, until one
    /// takes it, the request fails, or `cell_failovers` more were offered
    /// it. When every cell that may take it declined, the request fails
    /// with the last refusal. Each choice starts from the request's own
    /// context, `cx`, which `envelope`'s was made from, so a declined
    /// selector's meta is not sent to the next cell; a declined choice's
    /// lease ends before the next choice.
    ///
    /// A chooser never chooses itself: the entry segment is in another
    /// class, so the hand-off always crosses velo.
    pub(crate) async fn fail_over(
        &self,
        crossing: &Crossing,
        cx: &Cx,
        mut envelope: Forward,
        timed: bool,
        cell: u64,
        refusal: Error,
    ) -> Result<Handed> {
        let mut offered = vec![cell];
        let mut refusal = refusal;
        while offered.len() <= self.config.cell_failovers {
            if let Within::Selector { .. } = crossing.within() {
                envelope.cx = WireCx::of(cx);
            }
            let Some((instance, cell, lease)) = self
                .choose_cell(crossing, cx, &mut envelope, &offered)
                .await?
            else {
                break;
            };
            let bytes = envelope::encode(&envelope)?;
            let sent = timed.then(tokio::time::Instant::now);
            let offer = self.fabric.offer(
                instance,
                &self.names().forward,
                bytes,
                self.config.setup_timeout,
            );
            match answer(offer.await, crossing) {
                Answer::Accepted(accepted) => {
                    return Ok(Handed {
                        accepted,
                        sent,
                        lease,
                    });
                }
                Answer::Failed(error) => return Err(error),
                Answer::Declined(declined) => {
                    offered.push(cell);
                    refusal = declined;
                }
            }
        }
        Err(refusal)
    }
}

/// Whether the entry's answer `error` declines the offer: no room
/// (admission or egress refused, or the instance retired), or the
/// hand-off is not of the entry's deployment. The entry gives either
/// only before it has admitted anything for the request: every refusal in
/// its forward entry returns before the request's task is spawned, and
/// after it only acceptance is answered. Any other answer (the request's
/// deadline, a codec error) is the request's own.
fn declines(error: &Error) -> bool {
    matches!(error.kind(), ErrorKind::Overloaded | ErrorKind::Placement)
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use gdl::__runtime::Typed;
    use gdl_cells::CellState;

    use super::super::{CellTable, Cells, EntryTable, cell_id, targets};
    use super::*;
    use crate::dist::envelope::{VERSION, WireCx};
    use crate::dist::fabric::{Fabric, ReplyTo, bind_node};
    use crate::dist::roster::{Policy, Roster};
    use crate::dist::runtime::{Router, RuntimeConfig};

    /// Counts the allocations a thread makes while it polls a [`Counted`]
    /// future, and no others: other tests, and other tasks, allocate
    /// uncounted.
    struct Counting;

    static COUNTED: AtomicU64 = AtomicU64::new(0);

    thread_local! {
        static COUNTING: Cell<bool> = const { Cell::new(false) };
    }

    fn count() {
        if COUNTING.with(Cell::get) {
            COUNTED.fetch_add(1, Ordering::Relaxed);
        }
    }

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            count();
            // SAFETY: forwarded unchanged to the system allocator.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: forwarded unchanged to the system allocator.
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            count();
            // SAFETY: forwarded unchanged to the system allocator.
            unsafe { System.realloc(ptr, layout, size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: Counting = Counting;

    /// Polls the future it wraps with counting on.
    struct Counted<F>(Pin<Box<F>>);

    impl<F: Future> Future for Counted<F> {
        type Output = F::Output;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
            COUNTING.with(|counting| counting.set(true));
            let polled = self.0.as_mut().poll(cx);
            COUNTING.with(|counting| counting.set(false));
            polled
        }
    }

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
                forward_types: vec![gdl_model::TypeInfo::new("T", true)],
                backward_types: vec![gdl_model::TypeInfo::new("T", true)],
            }],
        };
        let placement = gdl_model::Placement::new()
            .segment("a", ["S"])
            .segment("d", ["D"])
            .class("global", ["a"])
            .class("model_x", ["d"]);
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
                let before = COUNTED.load(Ordering::Relaxed);
                tokio::time::timeout(Duration::from_secs(5), send)
                    .await
                    .expect("answered in time")
                    .expect("accepted");
                let made = COUNTED.load(Ordering::Relaxed) - before;
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

        let cells = Cells::new("home", ["d"]);
        let id = entry.instance_id();
        cells.store(
            "d",
            EntryTable {
                cells: Box::new([Arc::new(CellTable {
                    class: "model_x".into(),
                    cell: "m1".into(),
                    id: cell_id("model_x", "m1"),
                    generation: 1,
                    region: "home".into(),
                    state: CellState::Ready,
                    load: None,
                    eligible: Box::new([id]),
                    offered: Box::new([gdl::InstanceId::from_u128(id.as_u128())]),
                    retired: Box::new([]),
                    turn: Arc::new(AtomicUsize::new(0)),
                })]),
            },
        );
        let crossing = |router: Router| {
            let mut targets = targets(&layout, "a", &cells).expect("targets");
            let target = targets.remove(&(0, 1)).expect("the boundary hand-off");
            Router::Cells(Arc::new(Crossing::new(target, router)))
        };
        let policy = Router::Policy(Policy::RoundRobin);
        let selector = Router::Selector {
            name: Arc::from("replacing"),
            selector: Arc::new(Typed(Arc::new(Replacing))),
        };
        let routers = [
            policy.clone(),
            crossing(policy),
            selector.clone(),
            crossing(selector),
        ];
        let made = allocations(&rt, &routers).await;
        assert_eq!(made[1], made[0], "with a policy: {made:?}");
        assert_eq!(made[3], made[2], "with a selector: {made:?}");
    }
}
