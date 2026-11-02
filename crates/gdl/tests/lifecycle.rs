//! The handle by which a node asks to hear that its instance is leaving,
//! and what it hears.

use std::time::Duration;

use gdl::__runtime::LeaveParts;
use gdl::{Cx, Fence, Handoff, InstanceId, Leave, LeaveCause, Lifecycle, Routing, Unconfirmed};
use tokio::sync::watch;

/// A leave as the runtime gives it, with its two signals in the test's hands.
fn leave() -> (Leave, watch::Sender<Routing>, watch::Sender<bool>) {
    let (routes, routing) = watch::channel(Routing::Pending);
    let (drains, drained) = watch::channel(false);
    let leave = Leave::new(LeaveParts {
        cause: LeaveCause::PoolDrain,
        external: true,
        routing,
        drained,
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
        cx: Cx::new(),
        fence: Fence::new(InstanceId::from_u128(7), Some(9)),
    });
    (leave, routes, drains)
}

#[tokio::test]
async fn a_build_reaches_the_runtimes_handle_through_current() {
    let runtime = Lifecycle::new();
    runtime
        .scope(async {
            Lifecycle::current()
                .on_leave("flush", |_| async { Handoff::Nothing })
                .expect("registered");
        })
        .await;
    let names: Vec<String> = runtime
        .close()
        .iter()
        .map(|hook| hook.name().to_string())
        .collect();
    assert_eq!(names, ["flush"]);
}

#[tokio::test]
async fn outside_a_build_the_handle_is_nobodys() {
    Lifecycle::current()
        .on_leave("flush", |_| async { Handoff::Nothing })
        .expect("a handle no runtime asks takes a hook, as `OfferGate::current` gives one");
    assert!(Lifecycle::current().close().is_empty());
}

#[test]
fn several_register_in_order_and_a_name_is_taken_once() {
    let lifecycle = Lifecycle::new();
    for name in ["kv", "flush"] {
        lifecycle
            .on_leave(name, |_| async { Handoff::Done })
            .expect("registered");
    }
    let refused = lifecycle
        .on_leave("kv", |_| async { Handoff::Done })
        .expect_err("the name is the metric's label, so it is one hook's");
    assert!(refused.to_string().contains("`kv`"), "{refused}");
    let names: Vec<String> = lifecycle
        .close()
        .iter()
        .map(|hook| hook.name().to_string())
        .collect();
    assert_eq!(names, ["kv", "flush"]);
}

#[test]
fn registration_closes_when_serving_starts_and_the_hooks_are_given_once() {
    let lifecycle = Lifecycle::new();
    lifecycle
        .on_leave("kv", |_| async { Handoff::Done })
        .expect("registered");
    assert_eq!(lifecycle.close().len(), 1);
    let refused = lifecycle
        .on_leave("late", |_| async { Handoff::Done })
        .expect_err("no registration races a leave");
    assert!(refused.to_string().contains("`late`"), "{refused}");
    assert!(lifecycle.close().is_empty(), "the hooks run once");
}

#[tokio::test]
async fn a_hook_is_called_with_the_leave() {
    let lifecycle = Lifecycle::new();
    lifecycle
        .on_leave("kv", |leave: Leave| async move {
            assert_eq!(leave.cause(), LeaveCause::PoolDrain);
            Handoff::Partial { moved: 3, left: 1 }
        })
        .expect("registered");
    let hook = lifecycle.close().pop().expect("the hook");
    let (leave, _routes, _drains) = leave();
    assert_eq!(
        hook.start(leave).await,
        Handoff::Partial { moved: 3, left: 1 }
    );
}

#[tokio::test]
async fn a_leave_says_what_the_runtime_gave_it() {
    let (leave, _routes, _drains) = leave();
    assert_eq!(leave.cause(), LeaveCause::PoolDrain);
    assert!(leave.external());
    assert!(!leave.cx().is_cancelled());
    assert!(leave.deadline() > tokio::time::Instant::now());
    assert_eq!(leave.fence().instance(), InstanceId::from_u128(7));
    assert_eq!(leave.fence().incarnation(), Some(9));
}

#[tokio::test]
async fn routing_starts_pending_and_a_hook_waits_for_its_move() {
    let (leave, routes, _drains) = leave();
    assert_eq!(*leave.routing().borrow(), Routing::Pending);
    let waited = tokio::spawn({
        let leave = leave.clone();
        async move { leave.routed().await }
    });
    routes.send_replace(Routing::Unconfirmed(Unconfirmed::ParentTimeout));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), waited)
            .await
            .expect("the move was seen")
            .expect("the task"),
        Routing::Unconfirmed(Unconfirmed::ParentTimeout)
    );
}

#[tokio::test]
async fn drained_waits_for_the_drains_own_point() {
    let (leave, _routes, drains) = leave();
    let waited = tokio::spawn({
        let leave = leave.clone();
        async move { leave.drained().await }
    });
    tokio::task::yield_now().await;
    assert!(!waited.is_finished());
    drains.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), waited)
        .await
        .expect("the drain's point was seen")
        .expect("the task");
}
