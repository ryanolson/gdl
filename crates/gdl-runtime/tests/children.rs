//! A global offers work only to its children, by round robin: its hub
//! follows the model cells whose record names the global's cell as their
//! parent (`home`), with its contract and placement hashes, whatever their
//! region, and its chooser gives each request the next of them that may
//! take it, `ready` before `degraded`, under the default cell policy,
//! `round_robin`, which reads no load summary.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory. The model cells' load summary period is the
//! default, 10 s, which no test here waits out, so no cell has a summary.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use common::boundary::{
    Model, ModelCell, RUNTIME, Running, ask, model_changed, placement, run, spec_of,
};
use common::cells::{Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until};
use common::{Chunk, Events, Plan, Prompt, WAIT, prompt};
use futures::StreamExt;
use gdl::{Cx, ErrorKind, Next, Selectors};
use gdl_cells::{CellDirectory, CellEvent};
use gdl_hub::CellReadiness;
use gdl_runtime::{CellState, Cells, DeploymentSpec, Register, RequestRouter, RuntimeConfig};
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";
const M3: &str = "model-x-03-us-east-1";

/// A child that is `ready` once its one entry instance is.
fn ready(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries: 1,
    }
}

/// A child that stays `degraded`: two entry instances make its quorum; it
/// has one.
fn degraded(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 2,
        entries: 1,
    }
}

/// The global cell's hub, its cell's extension, and its chooser, an
/// instance of `a` whose entry instances take `runtime`.
async fn global(
    spec: &DeploymentSpec,
    directory: &Resolving,
    events: &Arc<Events>,
) -> (
    Hub,
    Arc<CellReadiness<Resolving>>,
    Running<impl Next<Prompt, Chunk> + Register + use<>>,
) {
    let (hub, readiness) = cell_hub_of(spec, global_hub(), directory).await;
    let extra = format!("{}{RUNTIME}", global_cell());
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let runtime = RuntimeConfig {
        selectors: Selectors::new(),
        ..config.runtime.clone()
    };
    let chooser = run(&config, "a", spec, runtime, events)
        .await
        .expect("the chooser serves");
    (hub, readiness, chooser)
}

/// The child `cell` of the global, of `spec`, with no load summary.
async fn child(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    events: &Arc<Events>,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    model_changed(spec, directory, cell, &|hub| hub, events).await
}

/// The chooser's tables.
fn tables<H>(chooser: &Running<H>) -> Cells {
    chooser
        .cells
        .clone()
        .expect("a global instance in a cell chooses")
}

/// Wait until the chooser follows each of `expected` in its state, with
/// one eligible entry instance, and no load summary.
async fn following(cells: &Cells, expected: &[(&str, CellState)]) {
    until(cells, "the children followed", |cells| {
        expected.iter().all(|(cell, state)| {
            followed(cells, "f", cell)
                .is_some_and(|m| m.state == *state && m.eligible.len() == 1 && m.load.is_none())
        })
    })
    .await;
}

/// The request failed with `Routing`, naming the class.
fn unplaced(result: gdl::Result<()>) {
    let error = result.expect_err("no cell can take the request");
    assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
    assert!(error.to_string().contains("model_x"), "{error}");
}

/// Send `n` requests one after the other, and return, for each, the
/// index in `cells` of the cell that took it.
async fn order<H: Next<Prompt, Chunk>, M>(
    chooser: &Running<H>,
    cells: &[&ModelCell<M>],
    n: usize,
) -> Vec<usize> {
    let mut order = Vec::with_capacity(n);
    for i in 0..n {
        let before: Vec<usize> = cells.iter().map(|cell| cell.forwards()).collect();
        if let Err(error) = ask(&chooser.segment).await {
            panic!("request {i} failed: {error}");
        }
        let took: Vec<usize> = cells
            .iter()
            .zip(&before)
            .enumerate()
            .filter(|(_, (cell, before))| cell.forwards() > **before)
            .map(|(at, _)| at)
            .collect();
        assert_eq!(took.len(), 1, "request {i} was taken by {took:?}");
        order.push(took[0]);
    }
    order
}

/// How many requests each of `count` cells took in `order`.
fn tally(order: &[usize], count: usize) -> Vec<usize> {
    (0..count)
        .map(|at| order.iter().filter(|took| **took == at).count())
        .collect()
}

/// The forward hand-offs that reached each of `cells`, taken or refused.
fn offered<H>(cells: &[&ModelCell<H>]) -> Vec<usize> {
    cells.iter().map(|cell| offers(cell)).collect()
}

/// The forward hand-offs that reached `cell`, taken or refused.
fn offers<H>(cell: &ModelCell<H>) -> usize {
    cell.entries.iter().map(Running::offered).sum()
}

/// Wait until the directory holds a `ready` record of each of `cells`.
async fn published_ready(directory: &Resolving, cells: &[&str]) {
    let mut watch = directory.watch("model_x", None);
    let mut ready = BTreeSet::new();
    let waited = tokio::time::timeout(WAIT, async {
        while !cells.iter().all(|cell| ready.contains(*cell)) {
            match watch.next().await {
                Some(Ok(CellEvent::Current(record) | CellEvent::Put { record, .. })) => {
                    if record.state == CellState::Ready {
                        ready.insert(record.name);
                    } else {
                        ready.remove(&record.name);
                    }
                }
                Some(Ok(_)) => {}
                other => panic!("the directory's watch ended: {other:?}"),
            }
        }
    })
    .await;
    assert!(waited.is_ok(), "timed out waiting for {cells:?}: {ready:?}");
}

/// The model cells the global's hub follows, by name.
async fn hub_follows(
    global: &CellReadiness<Resolving>,
    done: impl Fn(&BTreeSet<String>) -> bool,
) -> BTreeSet<String> {
    let names = |cells: &BTreeMap<(String, String), u64>| -> BTreeSet<String> {
        cells
            .keys()
            .filter(|(class, _)| class == "model_x")
            .map(|(_, cell)| cell.clone())
            .collect()
    };
    let mut following = global.following();
    let waited = tokio::time::timeout(WAIT, async {
        following
            .wait_for(|cells| done(&names(cells)))
            .await
            .map(|cells| names(&cells))
    })
    .await;
    match waited {
        Ok(cells) => cells.expect("the extension runs"),
        Err(_) => panic!(
            "timed out waiting for the hub's follows: {:?}",
            *following.borrow()
        ),
    }
}

async fn shut(hubs: impl IntoIterator<Item = Hub>) {
    for hub in hubs {
        hub.shutdown().await.expect("the hub stops");
    }
}

/// Three ready children, none with a load summary: requests take them in
/// turn, in the order of the chooser's table, each a third of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_children_take_turns() {
    let spec = spec_of(placement());
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, _, chooser) = global(&spec, &directory, &events).await;
    let m1 = child(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
    let m2 = child(&spec, &directory, ready(M2, "m2-hub:1337"), &events).await;
    let m3 = child(&spec, &directory, ready(M3, "m3-hub:1337"), &events).await;
    let cells = tables(&chooser);
    following(
        &cells,
        &[
            (M1, CellState::Ready),
            (M2, CellState::Ready),
            (M3, CellState::Ready),
        ],
    )
    .await;

    let order = order(&chooser, &[&m1, &m2, &m3], 30).await;
    assert_eq!(tally(&order, 3), [10, 10, 10], "{order:?}");
    let first: BTreeSet<usize> = order[..3].iter().copied().collect();
    assert_eq!(first.len(), 3, "not each in turn: {order:?}");
    assert!(
        order
            .iter()
            .enumerate()
            .all(|(i, took)| *took == order[i % 3]),
        "not in turn: {order:?}"
    );
    assert!(
        cells.followed("f").iter().all(|m| m.load.is_none()),
        "a summary arrived: {:?}",
        cells.followed("f")
    );
    shut([hub, m1.hub, m2.hub, m3.hub]).await;
}

/// Only children are followed and chosen. A cell of the global's region
/// and deployment whose parent is another global, one with no parent, and
/// a child of another placement are never followed by the global's hub,
/// and never offered a request: with only them, a request fails with
/// `Routing`. A child in another region is followed and chosen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_children_are_followed_and_chosen() {
    let spec = spec_of(placement());
    let replaced = spec_of(placement().route("B", RequestRouter::Random));
    assert_eq!(replaced.graph.contract_hash(), spec.graph.contract_hash());
    assert_ne!(replaced.placement.hash(), spec.placement.hash());
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, readiness, chooser) = global(&spec, &directory, &events).await;

    let cousin = model_changed(
        &spec,
        &directory,
        ready("model-x-04-us-east-1", "m4-hub:1337"),
        &|hub| gdl_model::CellSettings {
            home: Some("global-02-us-east-1".into()),
            ..hub
        },
        &events,
    )
    .await;
    let orphan = model_changed(
        &spec,
        &directory,
        ready("model-x-05-us-east-1", "m5-hub:1337"),
        &|hub| gdl_model::CellSettings { home: None, ..hub },
        &events,
    )
    .await;
    let other = model_changed(
        &replaced,
        &directory,
        ready("model-x-06-us-east-1", "m6-hub:1337"),
        &|hub| hub,
        &events,
    )
    .await;
    published_ready(
        &directory,
        &[
            "model-x-04-us-east-1",
            "model-x-05-us-east-1",
            "model-x-06-us-east-1",
        ],
    )
    .await;
    unplaced(ask(&chooser.segment).await);

    // The children's records come after the others', and the global's
    // hub decides on records in the directory's order: once it follows
    // both children, it has decided on every record before theirs.
    // Made as `far` is, so that the two are of one type.
    let near = model_changed(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        &|hub| hub,
        &events,
    )
    .await;
    let far = model_changed(
        &spec,
        &directory,
        ready("model-x-01-us-west-2", "w1-hub:1337"),
        &|hub| gdl_model::CellSettings {
            region: "us-west-2".into(),
            ..hub
        },
        &events,
    )
    .await;
    let children: BTreeSet<String> = [M1, "model-x-01-us-west-2"]
        .into_iter()
        .map(String::from)
        .collect();
    let follows = hub_follows(&readiness, |cells| children.is_subset(cells)).await;
    assert_eq!(
        follows, children,
        "the hub followed a cell that is no child"
    );
    let cells = tables(&chooser);
    following(
        &cells,
        &[
            (M1, CellState::Ready),
            ("model-x-01-us-west-2", CellState::Ready),
        ],
    )
    .await;
    assert_eq!(
        followed(&cells, "f", "model-x-01-us-west-2").map(|m| m.region),
        Some("us-west-2".to_string())
    );

    let order = order(&chooser, &[&near, &far], 10).await;
    assert_eq!(tally(&order, 2), [5, 5], "{order:?}");
    assert_eq!(
        offered(&[&cousin, &orphan, &other]),
        [0, 0, 0],
        "a cell that is no child was offered a request"
    );
    assert!(
        cells.followed("f").len() == 2,
        "the chooser holds a cell that is no child: {:?}",
        cells.followed("f")
    );
    shut([hub, cousin.hub, orphan.hub, other.hub, near.hub, far.hub]).await;
}

/// A `degraded` child is chosen only while no child is `ready`, and never
/// one that is `draining`, or `unavailable`, which are not offered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_degraded_child_only_while_no_child_is_ready() {
    let spec = spec_of(placement());
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, _, chooser) = global(&spec, &directory, &events).await;
    let m1 = child(&spec, &directory, ready(M1, "m1-hub:1337"), &events).await;
    let m2 = child(&spec, &directory, degraded(M2, "m2-hub:1337"), &events).await;
    let m3 = child(&spec, &directory, ready(M3, "m3-hub:1337"), &events).await;
    let cells = tables(&chooser);
    following(
        &cells,
        &[
            (M1, CellState::Ready),
            (M2, CellState::Degraded),
            (M3, CellState::Ready),
        ],
    )
    .await;
    let order1 = order(&chooser, &[&m1, &m2, &m3], 10).await;
    assert_eq!(tally(&order1, 3), [5, 0, 5], "{order1:?}");

    m1.readiness.drain().await.expect("the cell drains");
    until(&cells, "M1 draining and not offered", |cells| {
        followed(cells, "f", M1).is_none_or(|m| !m.offered)
    })
    .await;
    let order2 = order(&chooser, &[&m1, &m2, &m3], 4).await;
    assert_eq!(tally(&order2, 3), [0, 0, 4], "{order2:?}");

    // Without its `d`, M3 has a role with no instance.
    let ModelCell {
        hub: m3_hub, sink, ..
    } = m3;
    sink.stop().await;
    until(&cells, "M3 unavailable and not offered", |cells| {
        followed(cells, "f", M3).is_none_or(|m| m.state == CellState::Unavailable && !m.offered)
    })
    .await;
    let before = m2.forwards();
    for _ in 0..4 {
        ask(&chooser.segment)
            .await
            .expect("the degraded child answered");
    }
    assert_eq!(
        m2.forwards() - before,
        4,
        "the degraded child was passed over"
    );
    shut([hub, m1.hub, m2.hub, m3_hub]).await;
}

/// The runtime settings of an entry that admits one request at a time.
fn one_at_a_time(runtime: RuntimeConfig) -> RuntimeConfig {
    RuntimeConfig {
        admission_limit: 1,
        ..runtime
    }
}

/// A child that declines is damped and skipped: the request it declined
/// fails over to the next child, and while its damping window lasts, the
/// next requests are not offered to it at all. `M1` takes the first turn
/// and holds a request that fills it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declining_child_is_damped_and_skipped() {
    let spec = spec_of(placement());
    let quiet = Events::quiet();
    let directory = Resolving::new();
    let (hub, _, chooser) = global(&spec, &directory, &quiet).await;
    let m1 = common::boundary::model_of(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        &one_at_a_time,
        &|deployment| common::reference_graph(deployment, &quiet),
    )
    .await;
    let m2 = child(&spec, &directory, ready(M2, "m2-hub:1337"), &quiet).await;
    let m3 = child(&spec, &directory, ready(M3, "m3-hub:1337"), &quiet).await;
    let cells = tables(&chooser);
    until(&cells, "three ready children", |cells| {
        [M1, M2, M3].iter().all(|cell| {
            followed(cells, "f", cell)
                .is_some_and(|m| m.state == CellState::Ready && m.eligible.len() == 1)
        })
    })
    .await;

    let plan = Plan {
        tokens: 1,
        hold_at_end: true,
        ..Plan::default()
    };
    let mut held =
        tokio::time::timeout(WAIT, chooser.segment.call(Cx::new(), prompt("held", plan)))
            .await
            .expect("handed on in time")
            .expect("handed on");
    let first = tokio::time::timeout(WAIT, held.next())
        .await
        .expect("an item in time");
    assert!(matches!(first, Some(Ok(_))), "{first:?}");
    assert_eq!(m1.forwards(), 1, "M1 took the first turn");

    // The turns of M2, M3, then M1, which declines: the request is
    // answered by another child, and M1 is damped.
    let order1 = order(&chooser, &[&m2, &m3], 3).await;
    assert_eq!(offers(&m1), 2, "M1 was not offered its turn");
    assert_eq!(m1.forwards(), 1, "M1 took a request it had no room for");
    assert_eq!(order1.len(), 3);
    let damped = followed(&cells, "f", M1).and_then(|m| m.damped);
    assert!(damped.is_some(), "M1 is not damped");

    let before = offers(&m1);
    order(&chooser, &[&m2, &m3], 4).await;
    let left = followed(&cells, "f", M1).and_then(|m| m.damped);
    assert!(
        left.is_some_and(|left| left > Duration::ZERO),
        "the window passed before the test was done: {left:?}"
    );
    assert_eq!(offers(&m1), before, "a damped child was offered a request");
    drop(held);
    shut([hub, m1.hub, m2.hub, m3.hub]).await;
}

/// With no child, a request fails at once with `Routing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_child_is_a_routing_error() {
    let spec = spec_of(placement());
    let events = Events::new();
    let directory = Resolving::new();
    let (hub, readiness, chooser) = global(&spec, &directory, &events).await;
    assert!(hub_follows(&readiness, |_| true).await.is_empty());
    unplaced(ask(&chooser.segment).await);
    shut([hub]).await;
}
