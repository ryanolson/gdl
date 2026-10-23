//! A cell's load reaches the choosers that may offer to it: each entry
//! instance of a model cell reports its admission to its own hub once a
//! summary period, the hub sums the reports into the cell's summary,
//! which its record and its cell watch carry, the global hub relays it,
//! and a chooser's table holds it with its age. Every hub and instance
//! runs in this process on OS-assigned ports, sharing one directory, with
//! the shortest summary period, 1 s; every wait is for something
//! observed.

mod common;

use std::sync::Arc;

use common::boundary::{
    Model, ModelCell, RUNTIME, Running, ask, model_summarized, placement, run_graph, spec_of,
};
use common::cells::{Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until};
use common::{Chunk, Events, Plan, Prompt, WAIT, prompt, reference_graph};
use futures::StreamExt;
use gdl::{Cx, ErrorKind, Next};
use gdl_cells::{CellDirectory, CellEvent, LoadSummary};
use gdl_hub::CellWatch;
use gdl_runtime::{CellState, Cells, DeploymentSpec, Register, RuntimeConfig};
use gdl_watch::{CellWatchEvent, FollowEvent};
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";

/// Each entry instance admits this many roots at once.
const LIMIT: usize = 4;

fn limited(runtime: RuntimeConfig) -> RuntimeConfig {
    RuntimeConfig {
        admission_limit: LIMIT,
        ..runtime
    }
}

fn summary(admitted: u64, entries: u32, refusals: u64) -> Option<LoadSummary> {
    Some(LoadSummary {
        admitted,
        limit: LIMIT as u64 * u64::from(entries),
        instances: entries,
        refusals,
    })
}

/// The global cell's hub and a chooser in it.
async fn chooser(
    spec: &DeploymentSpec,
    directory: &Resolving,
) -> (
    Hub,
    Arc<gdl_hub::CellReadiness<Resolving>>,
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
    let quiet = Events::quiet();
    let chooser = run_graph(&config, "a", spec, config.runtime.clone(), &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves");
    (hub, readiness, chooser)
}

/// The model cell `M1`, ready with `entries` entry instances, each
/// admitting [`LIMIT`] roots, with `runtime` as its instances' table.
async fn model(
    spec: &DeploymentSpec,
    directory: &Resolving,
    entries: usize,
    runtime: &str,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    let cell = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: u32::try_from(entries).expect("a few"),
        entries,
    };
    let quiet = Events::quiet();
    model_summarized(spec, directory, cell, runtime, &limited, &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
}

fn tables<H>(chooser: &Running<H>) -> Cells {
    chooser.cells.clone().expect("a global instance chooses")
}

/// A request through the chooser whose sink holds its stream open after
/// one item: it holds its entry's admission until the stream is dropped.
async fn hold<H: Next<Prompt, Chunk>>(chooser: &Running<H>, text: &str) -> gdl::Items<Chunk> {
    let plan = Plan {
        tokens: 1,
        hold_at_end: true,
        ..Plan::default()
    };
    let mut out = tokio::time::timeout(WAIT, chooser.segment.call(Cx::new(), prompt(text, plan)))
        .await
        .expect("handed on in time")
        .expect("handed on");
    let first = tokio::time::timeout(WAIT, out.next())
        .await
        .expect("an item in time");
    assert!(matches!(first, Some(Ok(_))), "{first:?}");
    out
}

/// A watch of the cell, past its snapshot.
async fn cell_watch<H>(cell: &ModelCell<H>) -> CellWatch {
    let mut watch = tokio::time::timeout(WAIT, cell.readiness.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    tokio::time::timeout(WAIT, async {
        while watch.next().await.expect("the watch goes on") != CellWatchEvent::Synced {}
    })
    .await
    .expect("the snapshot in time");
    watch
}

/// Read `watch` until it sends the load `load`.
async fn load(watch: &mut CellWatch, load: Option<LoadSummary>) {
    let mut seen = Vec::new();
    let waited = tokio::time::timeout(WAIT, async {
        loop {
            if let CellWatchEvent::Load { load: sent } =
                watch.next().await.expect("the watch goes on")
            {
                if sent == load {
                    return;
                }
                seen.push(sent);
            }
        }
    })
    .await;
    assert!(waited.is_ok(), "no load of {load:?} in time: {seen:?}");
}

/// Wait until the chooser's table of `M1` holds `load`.
async fn table_holds(cells: &Cells, load: Option<LoadSummary>) {
    until(cells, "the summary in the chooser's table", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.load == load)
    })
    .await;
    let age = followed(cells, "f", M1).expect("followed").load_age;
    assert!(
        age.is_some_and(|age| age < WAIT),
        "the summary's age: {age:?}"
    );
}

async fn shut(hubs: impl IntoIterator<Item = Hub>) {
    for hub in hubs {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// Two entry instances each hold a request: the cell's summary sums their
/// admitted roots and their limits, and counts them, on the cell watch,
/// on the cell's record, on the global hub's relay, and in the chooser's
/// table. When the requests finish, the summary falls, everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_entry_instances_load_reaches_the_choosers_table() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (global_hub, global, chooser) = chooser(&spec, &directory).await;
    let mut relay = global.follows();
    let m1 = model(&spec, &directory, 2, RUNTIME).await;
    let cells = tables(&chooser);
    until(&cells, "M1 followed", |cells| {
        followed(cells, "f", M1)
            .is_some_and(|m| m.state == CellState::Ready && m.eligible.len() == 2)
    })
    .await;
    let mut watch = cell_watch(&m1).await;
    let mut records = directory.watch("model_x", None);

    let held = [hold(&chooser, "one").await, hold(&chooser, "two").await];
    let admitted: Vec<usize> = m1.entries.iter().map(|e| e.segment.admitted()).collect();
    assert_eq!(admitted, [1, 1], "one request held at each entry");
    let busy = summary(2, 2, 0);
    load(&mut watch, busy).await;
    let recorded = tokio::time::timeout(WAIT, async {
        loop {
            match records.next().await {
                Some(Ok(CellEvent::Current(record) | CellEvent::Put { record, .. }))
                    if record.name == M1 && record.load == busy =>
                {
                    return;
                }
                Some(Ok(_)) => {}
                other => panic!("the directory's watch ended: {other:?}"),
            }
        }
    })
    .await;
    assert!(recorded.is_ok(), "no record carried {busy:?} in time");
    let relayed = tokio::time::timeout(WAIT, async {
        loop {
            match relay.next().await.expect("the relay goes on") {
                FollowEvent::Follow { cell, load, .. } | FollowEvent::Load { cell, load, .. }
                    if cell == M1 && load == busy =>
                {
                    return;
                }
                _ => {}
            }
        }
    })
    .await;
    assert!(
        relayed.is_ok(),
        "the global hub did not relay {busy:?} in time"
    );
    table_holds(&cells, busy).await;

    drop(held);
    let idle = summary(0, 2, 0);
    load(&mut watch, idle).await;
    table_holds(&cells, idle).await;
    drop(chooser);
    shut([global_hub, m1.hub]).await;
}

/// An entry instance that is stopped retires, and stops counting at once,
/// the request it still holds included, while it drains; the other goes
/// on counting. Its drain is long enough to see it hold the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retiring_entry_instance_stops_counting_at_once() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (global_hub, _, chooser) = chooser(&spec, &directory).await;
    let draining = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 10000\n";
    let mut m1 = model(&spec, &directory, 2, draining).await;
    let cells = tables(&chooser);
    until(&cells, "M1 followed", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.eligible.len() == 2)
    })
    .await;
    let mut watch = cell_watch(&m1).await;
    let held = [hold(&chooser, "one").await, hold(&chooser, "two").await];
    load(&mut watch, summary(2, 2, 0)).await;

    let retiring = m1.entries.remove(0);
    retiring.process.stop();
    load(&mut watch, summary(1, 1, 0)).await;
    assert_eq!(
        retiring.segment.admitted(),
        1,
        "the retiring instance's request finished before the summary left it out"
    );
    drop(held);
    retiring.stop().await;
    drop(chooser);
    shut([global_hub, m1.hub]).await;
}

/// An entry instance with no room declines an offer, and reports it: the
/// summary counts the refusal in the period it falls in, and not in the
/// next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_instances_refusals_are_in_the_summary() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (global_hub, _, chooser) = chooser(&spec, &directory).await;
    let m1 = model(&spec, &directory, 1, RUNTIME).await;
    let cells = tables(&chooser);
    until(&cells, "M1 followed", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.eligible.len() == 1)
    })
    .await;
    let mut watch = cell_watch(&m1).await;
    let mut held = Vec::new();
    for at in 0..LIMIT {
        held.push(hold(&chooser, &format!("held {at}")).await);
    }
    let refused = ask(&chooser.segment).await.expect_err("no room");
    assert_eq!(refused.kind(), ErrorKind::Overloaded, "{refused}");
    load(&mut watch, summary(LIMIT as u64, 1, 1)).await;
    load(&mut watch, summary(LIMIT as u64, 1, 0)).await;
    drop(held);
    drop(chooser);
    shut([global_hub, m1.hub]).await;
}
