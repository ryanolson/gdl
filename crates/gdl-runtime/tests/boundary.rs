//! A chooser routes across the class boundary: each request its source
//! sends into `model_x` goes to a cell the class's cell policy chose from
//! the chooser's tables, then to an entry instance of that cell chosen by
//! the hop's own router. With no cell to choose, the request fails at once
//! with `Routing`.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::boundary::{
    Model, ModelCell, RUNTIME, Running, ask, model, model_unsummarized, placement, run, spec_of,
};
use common::cells::{Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until};
use common::{Events, WAIT};
use futures::StreamExt;
use gdl::{Choice, Encoded, ErrorKind, Instances, Request, Selector, Selectors};
use gdl_model::CellPolicy;
use gdl_runtime::{CellState, Cells, DeploymentSpec, RequestRouter, RuntimeConfig};
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// The global cell's hub and its chooser, an instance of `a`, whose
/// deployment registers `selectors`.
async fn global(
    spec: &DeploymentSpec,
    directory: &Resolving,
    selectors: Selectors,
    events: &Arc<Events>,
) -> (
    Hub,
    Running<impl gdl::Next<common::Prompt, common::Chunk> + gdl_runtime::Register + use<>>,
) {
    let (hub, _) = cell_hub_of(spec, global_hub(), directory).await;
    let extra = format!("{}{RUNTIME}", global_cell());
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let runtime = RuntimeConfig {
        selectors,
        ..config.runtime.clone()
    };
    let chooser = run(&config, "a", spec, runtime, events)
        .await
        .expect("the chooser serves");
    (hub, chooser)
}

/// The chooser's tables.
fn tables<H>(chooser: &Running<H>) -> Cells {
    chooser
        .cells
        .clone()
        .expect("a global instance in a cell chooses")
}

/// Whether `cell` is followed in `state` with `entries` eligible, and
/// with a load summary: `home_first` chooses no cell without one.
fn holds(cells: &Cells, cell: &str, state: CellState, entries: usize) -> bool {
    followed(cells, "f", cell)
        .is_some_and(|m| m.state == state && m.eligible.len() == entries && m.load.is_some())
}

/// The request failed with `Routing`, naming the class it could not
/// place.
fn unplaced(result: gdl::Result<()>) {
    let error = result.expect_err("no cell can take the request");
    assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
    assert!(error.to_string().contains("model_x"), "{error}");
}

/// Send `n` requests one after the other; each must be answered.
async fn asks<H: gdl::Next<common::Prompt, common::Chunk>>(chooser: &Running<H>, n: usize) {
    for i in 0..n {
        if let Err(error) = ask(&chooser.segment).await {
            panic!("request {i} failed: {error}");
        }
    }
}

/// The forward hand-offs each of `cells` took while `act` ran.
async fn counted<H, F: Future<Output = ()>>(cells: &[&ModelCell<H>], act: F) -> Vec<usize> {
    let before: Vec<usize> = cells.iter().map(|cell| cell.forwards()).collect();
    act.await;
    cells
        .iter()
        .zip(before)
        .map(|(cell, before)| cell.forwards() - before)
        .collect()
}

/// A global cell and two home model cells, each with its entry and its
/// sink: every request through the global's source is answered by one of
/// the model cells, and over many requests both are used.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_crosses_into_a_cell_the_chooser_follows() {
    let spec = spec_of(placement());
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, chooser) = global(&spec, &directory, Selectors::new(), &events).await;
    let m1 = model(&spec, &directory, M1, "m1-hub:1337", 1, 1, &events).await;
    let m2 = model(&spec, &directory, M2, "m2-hub:1337", 1, 1, &events).await;
    let cells = tables(&chooser);
    until(&cells, "both model cells ready", |cells| {
        holds(cells, M1, CellState::Ready, 1) && holds(cells, M2, CellState::Ready, 1)
    })
    .await;
    let used = counted(&[&m1, &m2], asks(&chooser, 40)).await;
    assert_eq!(used.iter().sum::<usize>(), 40, "{used:?}");
    assert!(
        used.iter().all(|n| *n > 0),
        "a cell was never chosen: {used:?}"
    );
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// With one `ready` and one `degraded` cell, every request goes to the
/// ready one; with only the degraded cell, to it; with only an
/// `unavailable` cell, or none, a request fails at once with `Routing`,
/// naming the class.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ready_cell_before_a_degraded_one_and_never_an_unavailable_one() {
    let spec = spec_of(placement());
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, chooser) = global(&spec, &directory, Selectors::new(), &events).await;
    let m1 = model(&spec, &directory, M1, "m1-hub:1337", 1, 1, &events).await;
    // Two entry instances make its quorum; it has one.
    let m2 = model(&spec, &directory, M2, "m2-hub:1337", 2, 1, &events).await;
    let cells = tables(&chooser);
    until(&cells, "one ready cell and one degraded", |cells| {
        holds(cells, M1, CellState::Ready, 1) && holds(cells, M2, CellState::Degraded, 1)
    })
    .await;
    let used = counted(&[&m1, &m2], asks(&chooser, 20)).await;
    assert_eq!(used, [20, 0], "a degraded cell was chosen over a ready one");

    m1.hub.shutdown().await.expect("valid test fixture");
    until(&cells, "only the degraded cell", |cells| {
        followed(cells, "f", M1).is_none() && holds(cells, M2, CellState::Degraded, 1)
    })
    .await;
    let used = counted(&[&m2], asks(&chooser, 5)).await;
    assert_eq!(used, [5]);

    // Without its `d`, the cell has a role with no instance.
    let ModelCell {
        hub: m2_hub,
        entries: _m2_entries,
        sink,
        ..
    } = m2;
    sink.stop().await;
    // The global's hub lets go of a cell that takes no work, once the
    // chooser acknowledges that it holds no offer to it.
    until(&cells, "the cell unavailable", |cells| {
        followed(cells, "f", M2).is_none_or(|m| m.state == CellState::Unavailable)
    })
    .await;
    unplaced(ask(&chooser.segment).await);

    m2_hub.shutdown().await.expect("valid test fixture");
    until(&cells, "no cell followed", |cells| {
        cells.followed("f").is_empty()
    })
    .await;
    unplaced(ask(&chooser.segment).await);
    hub.shutdown().await.expect("valid test fixture");
}

/// A ready home cell whose hub has made no load summary yet is followed,
/// with its entry instance, but not chosen: with no other cell, a request
/// fails at once with `Routing`. Its hub's summary period is 10 s, which
/// the test does not wait out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_without_a_load_summary_is_not_chosen() {
    let spec = spec_of(placement());
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, chooser) = global(&spec, &directory, Selectors::new(), &events).await;
    let cell = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries: 1,
    };
    let m1 = model_unsummarized(&spec, &directory, cell, &events).await;
    let cells = tables(&chooser);
    until(&cells, "the cell ready with its entry", |cells| {
        followed(cells, "f", M1)
            .is_some_and(|m| m.state == CellState::Ready && m.eligible.len() == 1)
    })
    .await;
    assert_eq!(followed(&cells, "f", M1).and_then(|m| m.load), None);
    unplaced(ask(&chooser.segment).await);
    assert_eq!(
        m1.entries[0].offered(),
        0,
        "the cell was offered the request"
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A model cell whose hub stops is dropped from the tables, and later
/// requests go to the other cell; with none left, `Routing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_that_goes_away_is_not_chosen() {
    let spec = spec_of(placement());
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, chooser) = global(&spec, &directory, Selectors::new(), &events).await;
    let m1 = model(&spec, &directory, M1, "m1-hub:1337", 1, 1, &events).await;
    let m2 = model(&spec, &directory, M2, "m2-hub:1337", 1, 1, &events).await;
    let cells = tables(&chooser);
    until(&cells, "both model cells ready", |cells| {
        holds(cells, M1, CellState::Ready, 1) && holds(cells, M2, CellState::Ready, 1)
    })
    .await;
    asks(&chooser, 4).await;

    m1.hub.shutdown().await.expect("valid test fixture");
    until(&cells, "the stopped cell dropped", |cells| {
        followed(cells, "f", M1).is_none()
    })
    .await;
    let used = counted(&[&m1, &m2], asks(&chooser, 10)).await;
    assert_eq!(used, [0, 10], "a dropped cell was chosen");

    m2.hub.shutdown().await.expect("valid test fixture");
    until(&cells, "no cell followed", |cells| {
        cells.followed("f").is_empty()
    })
    .await;
    unplaced(ask(&chooser.segment).await);
    hub.shutdown().await.expect("valid test fixture");
}

/// With `[routing] B = "round_robin"` and two entry instances in the one
/// cell, requests alternate between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_robin_takes_turns_within_the_chosen_cell() {
    let spec = spec_of(placement().route("B", RequestRouter::RoundRobin));
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, chooser) = global(&spec, &directory, Selectors::new(), &events).await;
    let m1 = model(&spec, &directory, M1, "m1-hub:1337", 2, 2, &events).await;
    let cells = tables(&chooser);
    until(&cells, "the cell ready with both entries", |cells| {
        holds(cells, M1, CellState::Ready, 2)
    })
    .await;
    let mut order = Vec::new();
    for _ in 0..8 {
        let before: Vec<usize> = m1.entries.iter().map(Running::forwards).collect();
        asks(&chooser, 1).await;
        let took = m1
            .entries
            .iter()
            .zip(&before)
            .position(|(entry, before)| entry.forwards() > *before)
            .expect("an entry took the request");
        order.push(took);
    }
    assert!(
        order.windows(2).all(|pair| pair[0] != pair[1]),
        "the entries did not alternate: {order:?}"
    );
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// Records the instances it is offered, takes the first, and holds a
/// lease per choice.
#[derive(Clone, Default)]
struct Recording {
    offered: Arc<Mutex<Vec<BTreeSet<u128>>>>,
    leases: Arc<AtomicUsize>,
}

struct Lease(Arc<AtomicUsize>);

impl Drop for Lease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Selector for Recording {
    type Head = Encoded;

    async fn select<'a>(
        &'a self,
        _: Request<'a, Encoded>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        let ids = instances.iter().map(|i| i.id().as_u128()).collect();
        self.offered.lock().expect("lock").push(ids);
        self.leases.fetch_add(1, Ordering::SeqCst);
        let chosen = instances.get(0).expect("never empty");
        Ok(Choice::new(chosen).with_lease(Lease(Arc::clone(&self.leases))))
    }
}

/// The ids of a cell's entry instances, as a selector sees them.
fn entry_ids<H>(cell: &ModelCell<H>) -> BTreeSet<u128> {
    cell.entries
        .iter()
        .map(|entry| entry.segment.peer_info().instance_id().as_u128())
        .collect()
}

/// With a selector on the boundary, the selector is offered only the
/// chosen cell's entry instances, and its lease is released when the
/// response ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selector_sees_only_the_chosen_cells_instances() {
    let spec = spec_of(placement().route("B", RequestRouter::Selector("recording".into())));
    let events = Events::new();
    let directory = Resolving::new();
    let recording = Recording::default();
    let mut selectors = Selectors::new();
    selectors
        .add("recording", recording.clone())
        .expect("a selector");
    let (hub, chooser) = global(&spec, &directory, selectors, &events).await;
    let m1 = model(&spec, &directory, M1, "m1-hub:1337", 2, 2, &events).await;
    let m2 = model(&spec, &directory, M2, "m2-hub:1337", 1, 1, &events).await;
    let cells = tables(&chooser);
    until(&cells, "both model cells ready", |cells| {
        holds(cells, M1, CellState::Ready, 2) && holds(cells, M2, CellState::Ready, 1)
    })
    .await;
    let (one, two) = (entry_ids(&m1), entry_ids(&m2));
    for i in 0..20 {
        asks(&chooser, 1).await;
        // The lease is dropped as the response ends, which the caller can
        // see a moment after its last item.
        let released = tokio::time::timeout(WAIT, async {
            while recording.leases.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(released.is_ok(), "request {i}'s lease was kept");
    }
    let offered = recording.offered.lock().expect("lock").clone();
    assert_eq!(offered.len(), 20);
    assert!(
        offered.iter().all(|ids| *ids == one || *ids == two),
        "offered instances of more than one cell: {offered:?}"
    );
    assert!(
        offered.contains(&one) && offered.contains(&two),
        "{offered:?}"
    );
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A boundary selector's lease is held while the response it chose for is
/// open, and released when the caller drops it (ruling 157).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boundary_selectors_lease_lives_as_long_as_the_response() {
    let spec = spec_of(placement().route("B", RequestRouter::Selector("recording".into())));
    let events = Events::new();
    let directory = Resolving::new();
    let recording = Recording::default();
    let mut selectors = Selectors::new();
    selectors
        .add("recording", recording.clone())
        .expect("a selector");
    let (hub, chooser) = global(&spec, &directory, selectors, &events).await;
    let m1 = model(&spec, &directory, M1, "m1-hub:1337", 1, 1, &events).await;
    let cells = tables(&chooser);
    until(&cells, "the cell ready", |cells| {
        holds(cells, M1, CellState::Ready, 1)
    })
    .await;
    // Three items, then the sink holds its stream open.
    let plan = common::Plan {
        tokens: 3,
        hold_at_end: true,
        ..common::Plan::default()
    };
    let mut out = tokio::time::timeout(
        WAIT,
        chooser
            .segment
            .call(gdl::Cx::new(), common::prompt("held", plan)),
    )
    .await
    .expect("handed on in time")
    .expect("handed on");
    for _ in 0..3 {
        let item = tokio::time::timeout(WAIT, out.next())
            .await
            .expect("an item in time");
        assert!(matches!(item, Some(Ok(_))), "{item:?}");
    }
    assert_eq!(
        recording.leases.load(Ordering::SeqCst),
        1,
        "the lease ended before the response did"
    );
    drop(out);
    let released = tokio::time::timeout(WAIT, async {
        while recording.leases.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(released.is_ok(), "the lease outlived the response");
    for hub in [hub, m1.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A placement whose `[cells.model_x] policy` names a policy the runtime
/// does not provide is refused when a chooser launches, naming it and the
/// class.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chooser_refuses_a_cell_policy_it_does_not_have() {
    let spec = spec_of(placement().cell_policy(
        "model_x",
        CellPolicy {
            policy: "nearest".into(),
            ..CellPolicy::default()
        },
    ));
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &global_cell(),
    );
    let Err(error) = run(&config, "a", &spec, config.runtime.clone(), &events).await else {
        panic!("a chooser launched with a cell policy it does not have");
    };
    let message = error.to_string();
    assert!(
        message.contains("nearest") && message.contains("model_x"),
        "{message}"
    );
    hub.shutdown().await.expect("valid test fixture");
}

/// A classed placement whose instances have no `[cell]` routes as a
/// placement without classes does: over a roster of every segment, the
/// hand-off into `model_x` goes to the roster's instance of `f`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_classed_placement_over_a_roster_routes_as_before() {
    let events = Events::new();
    let nodes = [
        common::velo_node().await,
        common::velo_node().await,
        common::velo_node().await,
    ];
    let instances: Vec<(String, gdl_runtime::velo::Velo)> = ["a", "f", "d"]
        .into_iter()
        .map(String::from)
        .zip(nodes.iter().cloned())
        .collect();
    let cluster =
        common::deploy_instances(&instances, &placement(), &events, RuntimeConfig::default());
    for _ in 0..4 {
        ask(cluster.segment("a")).await.expect("answered");
    }
    assert_eq!(cluster.segment("f").served().forwards, 4);
}

/// The same under a hub, with no instance in a cell: every instance meets
/// every segment, follows no cell, and routes by its roster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_classed_placement_without_cells_under_a_hub_routes_as_before() {
    let spec = spec_of(placement());
    let events = Events::new();
    let hub = Hub::builder(common::cells::hub_config())
        .extension(gdl_hub::GraphAdmission::new(spec.clone()))
        .serve()
        .await
        .expect("the hub serves");
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        RUNTIME,
    );
    let launched = futures::future::join_all(["a", "f", "d"].map(|segment| {
        let (config, spec, events) = (&config, &spec, &events);
        async move {
            run(config, segment, spec, config.runtime.clone(), events)
                .await
                .expect("an instance serves")
        }
    }))
    .await;
    assert!(
        launched.iter().all(|running| running.cells.is_none()),
        "an instance without a cell chose"
    );
    asks(&launched[0], 4).await;
    assert_eq!(launched[1].forwards(), 4);
    hub.shutdown().await.expect("valid test fixture");
}
