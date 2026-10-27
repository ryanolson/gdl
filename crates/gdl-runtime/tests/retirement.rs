//! A model cell's entry instance that retires is acknowledged by its
//! parent: each chooser of the global cell that follows the cell avoids it
//! once its tables hold it retired, and says so to its own hub; the global
//! cell's hub, once every chooser has, tells the model cell's hub, which
//! says so on the cell's watch. The retiree waits for that, up to a bound
//! of the setup timeout and twice the cell's summary period from its
//! retirement, then for the setup timeout, before it stops taking work.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed. A retiree waits out at least the setup timeout (1 s here), and
//! the test of its bound at least the bound (3 s), so these tests take
//! seconds each.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::boundary::{
    Model, ModelCell, Running, ask, model_fronted, model_summarized, placement, run_graph, spec_of,
};
use common::cells::{
    Instance, Resolving, cell_hub, cell_hub_of, followed, global_cell, global_hub, launch,
    model_hub, settings, sorted, until,
};
use common::proxy::{Mode, Proxy};
use common::{Chunk, Events, Plan, Prompt, WAIT, collect, expected, prompt, reference_graph};
use futures::StreamExt;
use gdl::{Cx, Next, Selectors};
use gdl_model::SegmentPhase;
use gdl_runtime::velo::InstanceId;
use gdl_runtime::{CellState, Cells, Deployment, DeploymentSpec, Register, RuntimeConfig};
use gdl_watch::CellWatchEvent;
use tokio::task::JoinHandle;
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// The setup timeout of every instance here.
const SETUP: Duration = Duration::from_secs(1);

/// A retiree's bound on its wait for its parent: the setup timeout and
/// twice the cells' summary period, the shortest, 1 s.
const BOUND: Duration = Duration::from_secs(3);

/// Every instance's `[runtime]` table: a drain long enough that its bound
/// never ends a wait here before the retiree's own bound does.
const RUNTIME: &str = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 10000\n";

/// The next event of `watch` about `instance`'s retirement.
async fn retirement(watch: &mut gdl_hub::CellWatch, instance: InstanceId) -> CellWatchEvent {
    let read = async {
        loop {
            match watch.next().await.expect("the watch goes on") {
                event @ CellWatchEvent::Retired { instance: of, .. } if of == instance => {
                    return event;
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the retirement in time")
}

/// A chooser that follows the model cell avoids its retired entry
/// instance once its tables hold it retired, and the model cell's hub
/// learns of it through the global cell's hub.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_choosers_acknowledgement_reaches_the_retirees_cell() {
    let directory = Resolving::new();
    let (global, _) = cell_hub(global_hub(), &directory).await;
    let (m1, model) = cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let (mut retiring, staying, _d) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &global_cell(),
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let cells = chooser
        .cells()
        .expect("a global instance in a cell is a chooser");
    until(&cells, "the model cell ready with its entries", |cells| {
        followed(cells, "f", M1).is_some_and(|m| {
            m.state == CellState::Ready && m.eligible == sorted(&[&retiring, &staying])
        })
    })
    .await;
    let mut watch = model.watch().await.expect("the cell's watch");
    retiring.announce(SegmentPhase::Retired).await;
    let id = retiring.id();
    assert_eq!(
        retirement(&mut watch, id).await,
        CellWatchEvent::Retired {
            segment: "f".into(),
            instance: id,
            acknowledged: false,
        },
        "the global cell follows the model cell"
    );
    assert_eq!(
        retirement(&mut watch, id).await,
        CellWatchEvent::Retired {
            segment: "f".into(),
            instance: id,
            acknowledged: true,
        }
    );
    assert!(
        followed(&cells, "f", M1).is_some_and(|m| m.retired == [id]),
        "acknowledged before the chooser's tables held it retired"
    );
}

/// A cell of `entries` entry instances, of which one makes its quorum, so
/// that it stays `ready` when one retires.
fn cell(name: &'static str, hub: &'static str, entries: usize) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries,
    }
}

/// A model cell of the reference graph whose nodes log to `events`.
async fn model(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    events: &Arc<Events>,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    model_summarized(
        spec,
        directory,
        cell,
        RUNTIME,
        &|runtime| runtime,
        &|deployment| reference_graph(deployment, events),
    )
    .await
}

/// A chooser, an instance of `a` in the global cell at `hub`, which reaches
/// the hub's public listener on `public`: the hub's own, or a proxy's.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
    public: u16,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_cell());
    let config = settings(public, hub.control_addr().port(), 5_000, &extra);
    let runtime = RuntimeConfig {
        selectors: Selectors::new(),
        ..config.runtime.clone()
    };
    let quiet = Events::quiet();
    run_graph(&config, "a", spec, runtime, &|deployment: &Deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

fn tables<H>(chooser: &Running<H>) -> Cells {
    chooser
        .cells
        .clone()
        .expect("a global instance in a cell chooses")
}

/// Wait until the chooser follows `cell` ready, with `entries` entry
/// instances to route to.
async fn following(cells: &Cells, cell: &str, entries: usize) {
    until(cells, "the model cell followed", |cells| {
        followed(cells, "f", cell).is_some_and(|m| {
            m.state == CellState::Ready && m.offered && m.eligible.len() == entries
        })
    })
    .await;
}

/// Stop `instance`, and return when it began to, and its end, with when
/// it ended.
fn stop<H: Send + Sync + 'static>(instance: Running<H>) -> (Instant, JoinHandle<Instant>) {
    let began = Instant::now();
    instance.process.stop();
    let ended = tokio::spawn(async move {
        instance
            .ended(WAIT * 4)
            .await
            .expect("the instance stopped");
        Instant::now()
    });
    (began, ended)
}

/// Wait until the cell's watch says that `instance`'s retirement is
/// acknowledged, and return when it did.
async fn acknowledged(watch: &mut gdl_hub::CellWatch, instance: InstanceId) -> Instant {
    loop {
        if let CellWatchEvent::Retired {
            acknowledged: true, ..
        } = retirement(watch, instance).await
        {
            return Instant::now();
        }
    }
}

/// A model cell with two entry instances, followed by a global cell with
/// two choosers: once one entry instance retires, each chooser avoids it,
/// the global's hub tells the model cell's, and the retiree, told by its
/// hub, waits out the setup timeout and leaves, long before its bound. No
/// offer reaches it once its parent has acknowledged it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acknowledged_retiree_waits_out_the_setup_timeout_and_leaves() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let second = chooser(&spec, &hub, hub.public_addr().port()).await;
    let mut m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 2), &quiet).await;
    following(&tables(&first), M1, 2).await;
    following(&tables(&second), M1, 2).await;
    let mut watch = m1.readiness.watch().await.expect("the cell's watch");
    let retiree = m1.entries.remove(0);
    let (id, offers) = (retiree.node.instance_id(), Arc::clone(&retiree.segment));
    let (began, ended) = stop(retiree);
    let told = acknowledged(&mut watch, id).await;
    let reached = offers.offered();
    for _ in 0..4 {
        ask(&first.segment).await.expect("answered");
        ask(&second.segment).await.expect("answered");
    }
    assert_eq!(offers.offered(), reached, "an offer reached the retiree");
    let left = ended.await.expect("the retiree's task");
    let (waited, took) = (left - told, left - began);
    assert!(
        waited + Duration::from_millis(200) >= SETUP,
        "left {waited:?} after its parent acknowledged it"
    );
    assert!(took < BOUND, "took {took:?} to leave");
}

/// With one chooser's view of the model cell held back, so that it has
/// not applied the retirement, the global's hub waits for it, and so does
/// the retiree, which takes the hand-offs that chooser still sends it and
/// does not leave. Once the chooser applies it, the retiree goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chooser_that_has_not_applied_the_retirement_holds_the_retiree() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, global) = cell_hub_of(&spec, global_hub(), &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let held = chooser(&spec, &hub, proxy.port).await;
    let mut m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 2), &quiet).await;
    following(&tables(&first), M1, 2).await;
    following(&tables(&held), M1, 2).await;
    proxy.set(Mode::Silent);
    let mut watch = m1.readiness.watch().await.expect("the cell's watch");
    let retiree = m1.entries.remove(0);
    let (id, taken) = (retiree.node.instance_id(), Arc::clone(&retiree.segment));
    let (_, ended) = stop(retiree);
    // The global's hub waits for the held chooser alone once the other
    // has avoided the retiree.
    let mut retirements = global.retirements();
    tokio::time::timeout(
        WAIT,
        retirements.wait_for(|waiting| waiting.iter().any(|(_, _, _, of)| *of == id)),
    )
    .await
    .expect("the global's hub waits for its choosers in time")
    .expect("the hub lives");
    until(&tables(&first), "the retirement applied", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.retired == [id])
    })
    .await;
    let before = taken.served().forwards;
    for _ in 0..4 {
        ask(&held.segment).await.expect("answered");
    }
    assert!(
        taken.served().forwards > before,
        "the retiree took no hand-off from the held chooser"
    );
    assert!(!ended.is_finished(), "the retiree left");
    assert!(
        global
            .retirements()
            .borrow()
            .iter()
            .any(|(_, _, _, of)| *of == id),
        "the global's hub no longer waits for the held chooser"
    );
    assert!(
        !matches!(
            retirement_now(&mut watch, id),
            Some(CellWatchEvent::Retired {
                acknowledged: true,
                ..
            })
        ),
        "acknowledged while a chooser has not applied it"
    );

    proxy.set(Mode::Forward);
    acknowledged(&mut watch, id).await;
    ended.await.expect("the retiree's task");
}

/// With a chooser whose view of the model cell is held back for good, the
/// retiree is never acknowledged: it goes on once its bound has passed,
/// then the setup timeout, and stops taking work, while a request it took
/// holds it. An offer that reaches it then is refused, and the chooser
/// offers the request to another cell, which answers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retiree_never_acknowledged_goes_on_at_its_bound() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let held = chooser(&spec, &hub, proxy.port).await;
    let m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 1), &events).await;
    let m2 = model(&spec, &directory, cell(M2, "m2-hub:1337", 1), &events).await;
    following(&tables(&held), M1, 1).await;
    following(&tables(&held), M2, 1).await;
    // A request the retiree holds, so that it is still there once it
    // stops taking work.
    let holding = gated(&held, &events, "held").await;
    let (mut holder, other) = if m1.forwards() == 1 {
        (m1, m2)
    } else {
        (m2, m1)
    };
    proxy.set(Mode::Silent);
    let retiree = holder.entries.remove(0);
    let taken = Arc::clone(&retiree.segment);
    let (began, ended) = stop(retiree);
    let refused = || taken.offered() - taken.served().forwards;
    let deadline = Instant::now() + BOUND + SETUP + WAIT;
    while refused() == 0 {
        assert!(Instant::now() < deadline, "the retiree never refused");
        ask(&held.segment).await.expect("answered by a cell");
        // Looked at again a poll later: the retiree's own drain polls too.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let took = began.elapsed();
    assert!(
        took >= BOUND + SETUP,
        "stopped taking work {took:?} after it retired"
    );
    assert!(other.forwards() > 0, "another cell answered");
    assert!(!ended.is_finished(), "the held request holds the retiree");
    events.record("test:release:held");
    holding
        .await
        .expect("the held request's task")
        .expect("the held request answered");
    ended.await.expect("the retiree's task");
    proxy.set(Mode::Forward);
}

/// A retiree whose cell starts to drain while it waits for its parent goes
/// by the drain's rules from then: with no acknowledgement and no follower
/// letting go of the cell, it keeps taking work until the drain's bound
/// has passed since it learned of the drain, and then declines offers from
/// other cells, as a draining entry does; the chooser offers the request to
/// another cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_while_the_retiree_waits_keeps_it_to_the_drains_bound() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = cell_hub_of(&spec, global_hub(), &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let held = chooser(&spec, &hub, proxy.port).await;
    let m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 1), &events).await;
    let m2 = model(&spec, &directory, cell(M2, "m2-hub:1337", 1), &events).await;
    following(&tables(&held), M1, 1).await;
    following(&tables(&held), M2, 1).await;
    let holding = gated(&held, &events, "held").await;
    let (mut holder, other) = if m1.forwards() == 1 {
        (m1, m2)
    } else {
        (m2, m1)
    };
    proxy.set(Mode::Silent);
    let retiree = holder.entries.remove(0);
    let (id, taken) = (retiree.node.instance_id(), Arc::clone(&retiree.segment));
    let (_, ended) = stop(retiree);
    let mut retirements = global.retirements();
    tokio::time::timeout(
        WAIT,
        retirements.wait_for(|waiting| waiting.iter().any(|(_, _, _, of)| *of == id)),
    )
    .await
    .expect("the global's hub waits for its chooser in time")
    .expect("the hub lives");
    // Its retirement's last step is to withdraw `loaded`: from then it
    // waits for its parent in its drain, which the cell's drain must find
    // it in.
    withdrawn(&holder.hub, id).await;
    let drained = Instant::now();
    holder.readiness.drain().await.expect("the cell drains");
    let refused = || taken.offered() - taken.served().forwards;
    while refused() == 0 {
        assert!(
            drained.elapsed() < BOUND + SETUP + WAIT,
            "the retiree never refused"
        );
        ask(&held.segment).await.expect("answered by a cell");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let took = drained.elapsed();
    assert!(
        took >= BOUND,
        "stopped taking work {took:?} after its cell began to drain"
    );
    // Declined at the bound, as a draining entry declines: within the
    // retiree's poll and this probe's, not a setup timeout later.
    assert!(
        took < BOUND + Duration::from_millis(400),
        "took offers {took:?} after its cell began to drain"
    );

    assert!(other.forwards() > 0, "another cell answered");
    events.record("test:release:held");
    holding
        .await
        .expect("the held request's task")
        .expect("the held request answered");
    ended.await.expect("the retiree's task");
    proxy.set(Mode::Forward);
}

/// A question the retiree asks its hub that stalls, as one to a hub that
/// takes the connection and does not answer, does not hold off the drain's
/// cutoff: the retiree still declines offers from other cells once the
/// drain's bound has passed, and takes none after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_question_to_the_hub_does_not_hold_off_the_drains_cutoff() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, global) = cell_hub_of(&spec, global_hub(), &directory).await;
    let proxy = Proxy::new(hub.public_addr()).await;
    let held = chooser(&spec, &hub, proxy.port).await;
    let assemble = |deployment: &Deployment| reference_graph(deployment, &events);
    let (m1, front1) = model_fronted(
        &spec,
        &directory,
        cell(M1, "m1-hub:1337", 1),
        RUNTIME,
        &assemble,
    )
    .await;
    let (m2, front2) = model_fronted(
        &spec,
        &directory,
        cell(M2, "m2-hub:1337", 1),
        RUNTIME,
        &assemble,
    )
    .await;
    following(&tables(&held), M1, 1).await;
    following(&tables(&held), M2, 1).await;
    let holding = gated(&held, &events, "held").await;
    let ((mut holder, front), other) = if m1.forwards() == 1 {
        ((m1, front1), m2)
    } else {
        ((m2, front2), m1)
    };
    proxy.set(Mode::Silent);
    let retiree = holder.entries.remove(0);
    let (id, taken) = (retiree.node.instance_id(), Arc::clone(&retiree.segment));
    let (_, ended) = stop(retiree);
    let mut retirements = global.retirements();
    tokio::time::timeout(
        WAIT,
        retirements.wait_for(|waiting| waiting.iter().any(|(_, _, _, of)| *of == id)),
    )
    .await
    .expect("the global's hub waits for its chooser in time")
    .expect("the hub lives");
    withdrawn(&holder.hub, id).await;
    // The retiree's questions about its hub's members go unanswered from
    // here; its cell watch, open already, goes on.
    front.hold(b"/v1/ext/gdl/members");
    let drained = Instant::now();
    holder.readiness.drain().await.expect("the cell drains");
    let refused = || taken.offered() - taken.served().forwards;
    while refused() == 0 {
        assert!(
            drained.elapsed() < BOUND + SETUP + WAIT,
            "the retiree never refused"
        );
        ask(&held.segment).await.expect("answered by a cell");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let took = drained.elapsed();
    assert!(
        took < BOUND + Duration::from_millis(400),
        "took offers {took:?} after its cell began to drain"
    );
    let taken_then = taken.served().forwards;
    for _ in 0..4 {
        ask(&held.segment).await.expect("answered by a cell");
    }
    assert_eq!(
        taken.served().forwards,
        taken_then,
        "took an offer after it declined"
    );
    assert!(other.forwards() > 0, "another cell answered");
    front.release();
    events.record("test:release:held");
    holding
        .await
        .expect("the held request's task")
        .expect("the held request answered");
    ended.await.expect("the retiree's task");
    proxy.set(Mode::Forward);
}

/// A model cell no global cell follows: its retiree is told at once that
/// it is acknowledged, and waits out the setup timeout alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retiree_no_cell_follows_waits_out_the_setup_timeout_alone() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let mut m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 2), &quiet).await;
    let retiree = m1.entries.remove(0);
    let (began, ended) = stop(retiree);
    let took = ended.await.expect("the retiree's task") - began;
    assert!(took >= SETUP, "left {took:?} after it retired");
    assert!(took < BOUND, "took {took:?} to leave");
}

/// An interior instance of a model cell retires inside its cell: it cannot
/// see the drain of its source in the global cell, so it waits for the
/// entry instances of its cell to avoid it or leave, then the setup
/// timeout, and for no cell outside it. Stopped together, the entry leaves
/// first, and the interior instance leaves well inside its drain's bound.
/// It takes the entry's setup timeout and then its own, so 2 to 3 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interior_instance_waits_for_its_entry_to_leave() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let mut m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 1), &quiet).await;
    following(&tables(&first), M1, 1).await;
    let (began, ended) = stop(m1.sink);
    let (_, entry) = stop(m1.entries.remove(0));
    let entry_left = entry.await.expect("the entry instance's task");
    let left = ended.await.expect("the interior instance's task");
    assert!(
        left >= entry_left,
        "the interior instance left {:?} before its entry",
        entry_left - left
    );
    // `drain_ms` in `RUNTIME`.
    let took = left - began;
    assert!(took < Duration::from_secs(10), "took {took:?} to leave");
}

/// A request held at the entry survives an interior instance's stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_interior_preserves_an_admitted_request() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 1), &events).await;
    following(&tables(&first), M1, 1).await;
    ask(&first.segment)
        .await
        .expect("the control request answered");

    let request = gated(&first, &events, "interior-stop").await;
    assert!(m1.entries[0].segment.admitted() > 0);
    assert_eq!(m1.sink.segment.admitted(), 0);
    m1.sink.process.stop();
    let closed_before_release = tokio::time::timeout(SETUP / 2, async {
        while !m1.sink.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    events.record("test:release:interior-stop");
    let answered = tokio::time::timeout(WAIT, request)
        .await
        .expect("the request ended within the wait")
        .expect("the request's task");
    assert!(
        answered.is_ok(),
        "F1: interior closed before release: {closed_before_release}; an admitted request failed: {answered:?}"
    );

    let ((), (), ended) = tokio::join!(
        first.stop(),
        async {
            for entry in m1.entries {
                entry.stop().await;
            }
        },
        m1.sink.ended(WAIT * 2),
    );
    ended.expect("the interior drained");
}

/// A later cell drain lets an empty interior leave while its entry keeps a response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_interior_observes_a_later_cell_drain() {
    use gdl_model::segment_service;
    use velo::discovery::ServiceDiscovery;
    use velo_hub::client::HubClient;

    let spec = spec_of(placement());
    let directory = Resolving::new();
    let events = Events::new();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let (m1, proxy) = model_fronted(
        &spec,
        &directory,
        cell(M1, "m1-hub:1337", 1),
        "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 30000\n",
        &|deployment| reference_graph(deployment, &events),
    )
    .await;
    following(&tables(&first), M1, 1).await;
    let front = Arc::clone(&first.segment);
    let pending = tokio::spawn(async move {
        let mut cx = Cx::new();
        cx.insert_meta("E", "hold:later-drain");
        let out = front
            .call(
                cx,
                prompt(
                    "later-drain",
                    Plan {
                        tokens: 3,
                        ..Plan::default()
                    },
                ),
            )
            .await
            .expect("the request admitted");
        tokio::time::timeout(WAIT * 5, out.collect::<Vec<_>>())
            .await
            .expect("the response ended in time")
    });
    events.wait_for("D:done:c(later-drain)#0", WAIT).await;

    // The held membership question proves that the stop entered its drain loop.
    proxy.hold(b"/v1/ext/gdl/members");
    m1.sink.process.stop();
    tokio::time::timeout(WAIT, proxy.holding())
        .await
        .expect("the stopped interior asked about its senders");
    assert!(!m1.sink.segment.is_closed());
    m1.readiness.drain().await.expect("the cell drains");
    proxy.release();

    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(m1.hub.public_addr().port())
        .control_port(m1.hub.control_addr().port())
        .build()
        .expect("the model hub's client");
    tokio::time::timeout(BOUND + WAIT, async {
        loop {
            let detached = client
                .services()
                .get_instances(&segment_service("f", SegmentPhase::Detached))
                .await
                .expect("the hub answers");
            if detached.contains(&m1.entries[0].node.instance_id()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the entry detached while holding only its response");
    assert_eq!(m1.entries[0].segment.admitted(), 1);
    assert_eq!(m1.sink.segment.admitted(), 0);
    let closed = tokio::time::timeout(WAIT, async {
        while !m1.sink.segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();

    common::let_go("later-drain");
    assert_eq!(pending.await.expect("the response task"), expected(0..3));
    for entry in m1.entries {
        entry.ended(WAIT).await.expect("the entry drained");
    }
    m1.sink.ended(WAIT).await.expect("the interior drained");
    first.stop().await;
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("the hub stopped");
    }
    assert!(
        closed,
        "F1: the empty interior ignored its cell's drain after its entry detached"
    );
}

/// [`placement`], with a forward path that leaves the entry's segment
/// for `d` and comes back: `B` and `D` in `f`, `C` in `d`.
fn returning() -> gdl_runtime::Placement {
    gdl_runtime::Placement::new()
        .segment("a", ["A", "G"])
        .segment("f", ["B", "D", "E", "F"])
        .segment("d", ["C"])
        .class("global", ["a"])
        .class("model_x", ["f", "d"])
}

/// A returning forward path drains without peers waiting for each other to leave.
/// A failure waits out both ten-second bounds so their duration can be reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "F2: a known limitation: the last instances of a returning path, stopped outside their cell's drain, each wait for the other to leave until their drain bounds"]
async fn a_deployment_stop_breaks_an_empty_cells_returning_path() {
    let spec = spec_of(returning());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let mut m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 1), &quiet).await;
    following(&tables(&first), M1, 1).await;
    ask(&first.segment).await.expect("the returning path works");
    tokio::time::timeout(WAIT, async {
        while first.segment.admitted() != 0
            || m1.entries[0].segment.admitted() != 0
            || m1.sink.segment.admitted() != 0
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the control request released every admission");

    let entry = m1.entries.remove(0);
    let entry_segment = Arc::clone(&entry.segment);
    let interior_segment = Arc::clone(&m1.sink.segment);
    let (entry_id, interior_id) = (entry.node.instance_id(), m1.sink.node.instance_id());
    let (_, source_end) = stop(first);
    let (began, entry_end) = stop(entry);
    let (_, interior_end) = stop(m1.sink);
    // Both senders are the last instances of their segments, so neither avoids the other.
    withdrawn(&m1.hub, entry_id).await;
    withdrawn(&m1.hub, interior_id).await;
    source_end.await.expect("the source stopped");
    let closed = tokio::time::timeout(WAIT, async {
        while !entry_segment.is_closed() || !interior_segment.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    let (entry_left, interior_left) = tokio::join!(entry_end, interior_end);
    let entry_took = entry_left.expect("the entry's task") - began;
    let interior_took = interior_left.expect("the interior's task") - began;
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("the hub stopped");
    }
    assert!(
        closed,
        "F2: empty peers waited for each other until their drain bounds: entry {entry_took:?}, interior {interior_took:?}"
    );
}

/// The control for the case above: a drain of the cell, rather than a stop
/// of each instance, takes the same empty returning path down well inside
/// its drain's bound, since the cell's entry instances are then the drain
/// sources. It waits out the entry's bound (3 s here), so it takes 3 to 4 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_drain_takes_down_an_empty_returning_path() {
    let spec = spec_of(returning());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let mut m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 1), &quiet).await;
    following(&tables(&first), M1, 1).await;
    ask(&first.segment).await.expect("the returning path works");

    let began = Instant::now();
    m1.readiness.drain().await.expect("the cell drains");
    let entry = m1.entries.remove(0);
    let (entry_left, interior_left) =
        tokio::join!(entry.ended(BOUND + WAIT), m1.sink.ended(BOUND + WAIT));
    entry_left.expect("the entry drained");
    interior_left.expect("the interior drained");
    // `drain_ms` in `RUNTIME`.
    let took = began.elapsed();
    assert!(took < Duration::from_secs(10), "took {took:?} to drain");
    first.stop().await;
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("the hub stopped");
    }
}

/// The next event of `watch` about `instance`'s retirement that has come
/// already, if any.
fn retirement_now(watch: &mut gdl_hub::CellWatch, instance: InstanceId) -> Option<CellWatchEvent> {
    use futures::FutureExt;
    while let Some(Some(event)) = watch.next().now_or_never() {
        if matches!(event, CellWatchEvent::Retired { instance: of, .. } if of == instance) {
            return Some(event);
        }
    }
    None
}

/// Send a request whose C waits, in the model cell, until the test
/// releases it, and return the request's answer once that C has it.
async fn gated<H: Next<Prompt, Chunk> + Send + Sync + 'static>(
    chooser: &Running<H>,
    events: &Arc<Events>,
    text: &str,
) -> JoinHandle<gdl::Result<()>> {
    let plan = Plan {
        tokens: 3,
        gated: true,
        ..Plan::default()
    };
    let segment = Arc::clone(&chooser.segment);
    let request = prompt(text, plan);
    let answer = tokio::spawn(async move {
        let out = segment.call(Cx::new(), request).await?;
        let answer = collect(out).await;
        if let Some(Err(error)) = answer.iter().find(|item| item.is_err()) {
            return Err(error.clone());
        }
        assert_eq!(answer, expected(0..3));
        Ok(())
    });
    events.wait_for(&format!("C:{text}"), WAIT).await;
    answer
}

/// Wait until `instance` no longer announces `loaded` at `hub`.
async fn withdrawn(hub: &Hub, instance: InstanceId) {
    use velo::discovery::ServiceDiscovery as _;
    let client = velo_hub::client::HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()
        .expect("valid test fixture");
    let loaded = gdl_model::segment_service("f", SegmentPhase::Loaded);
    let deadline = Instant::now() + WAIT;
    loop {
        let instances = client
            .services()
            .get_instances(&loaded)
            .await
            .expect("the hub answers");
        if !instances.contains(&instance) {
            return;
        }
        assert!(Instant::now() < deadline, "`loaded` not withdrawn in time");
        // The hub is asked again a poll later, as an instance asks it.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
