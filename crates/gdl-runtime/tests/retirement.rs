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
    Model, ModelCell, Running, ask, model_summarized, placement, run_graph, spec_of,
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
/// has passed since it learned of the drain, then the setup timeout, and
/// only then refuses an offer, which the chooser offers to another cell.
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

    assert!(other.forwards() > 0, "another cell answered");
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

/// An interior instance of a model cell retires inside its cell, as
/// before: it waits for no cell outside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interior_instance_retires_as_before() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let first = chooser(&spec, &hub, hub.public_addr().port()).await;
    let m1 = model(&spec, &directory, cell(M1, "m1-hub:1337", 1), &quiet).await;
    following(&tables(&first), M1, 1).await;
    let (began, ended) = stop(m1.sink);
    let took = ended.await.expect("the interior instance's task") - began;
    assert!(took < SETUP, "took {took:?} to leave");
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
