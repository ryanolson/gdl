//! A model cell drains as a whole: its hub says it is `draining`, and the
//! choosers that follow it stop choosing it.
//!
//! Every hub and instance runs in this process on OS-assigned ports,
//! sharing one directory; every wait is bounded and for something
//! observed.

mod common;

use std::sync::Arc;

use common::boundary::{
    Model, ModelCell, RUNTIME, Running, ask, model_of, placement, run_graph, spec_of,
};
use common::cells::{Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until};
use common::{Chunk, Events, Prompt, WAIT, reference_graph};
use futures::StreamExt;
use gdl::{ErrorKind, Next, Selectors};
use gdl_cells::{CellDirectory, CellEvent, CellRecord};
use gdl_runtime::{CellState, Cells, Deployment, DeploymentSpec, Register, RuntimeConfig};
use gdl_watch::CellWatchEvent;
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// A `ready` cell: one entry instance makes its quorum.
fn ready(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries: 1,
    }
}

/// A `degraded` cell: two entry instances make its quorum; it has one.
/// A chooser offers to it only when no ready cell takes the request.
fn degraded(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 2,
        entries: 1,
    }
}

/// The global cell's hub.
async fn global_hub_of(spec: &DeploymentSpec, directory: &Resolving) -> Hub {
    cell_hub_of(spec, global_hub(), directory).await.0
}

/// A chooser, an instance of `a` in the global cell at `hub`.
async fn chooser(
    spec: &DeploymentSpec,
    hub: &Hub,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
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
    let quiet = Events::quiet();
    run_graph(&config, "a", spec, runtime, &|deployment: &Deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

/// A model cell of the reference graph whose nodes log to `events`.
async fn cell(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    events: &Arc<Events>,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    model_of(spec, directory, cell, &|runtime| runtime, &|deployment| {
        reference_graph(deployment, events)
    })
    .await
}

/// The chooser's tables.
fn tables<H>(chooser: &Running<H>) -> Cells {
    chooser
        .cells
        .clone()
        .expect("a global instance in a cell chooses")
}

/// Wait until the chooser follows each of `cells` in its state, with one
/// entry instance and a load summary.
async fn following(cells: &Cells, expected: &[(&str, CellState)]) {
    until(cells, "the model cells followed", |cells| {
        expected.iter().all(|(cell, state)| {
            followed(cells, "f", cell)
                .is_some_and(|m| m.state == *state && m.eligible.len() == 1 && m.load.is_some())
        })
    })
    .await;
}

/// Wait until the chooser no longer has `cell` to choose: it follows it
/// `draining`, or no longer follows it.
async fn unchosen(cells: &Cells, cell: &str) {
    until(cells, "the cell drained", |cells| {
        followed(cells, "f", cell).is_none_or(|m| m.state == CellState::Draining)
    })
    .await;
}

/// The forward hand-offs that reached each of `cells`, taken or refused.
fn offered<H>(cells: &[&ModelCell<H>]) -> Vec<usize> {
    cells
        .iter()
        .map(|cell| cell.entries.iter().map(Running::offered).sum())
        .collect()
}

/// The record `directory` holds for the model cell `cell`, if any.
async fn record(directory: &Resolving, cell: &str) -> Option<CellRecord> {
    let mut watch = std::pin::pin!(directory.watch("model_x", None));
    let mut held = None;
    let read = async {
        while let Some(Ok(event)) = watch.next().await {
            match event {
                CellEvent::Current(record) if record.name == cell => held = Some(record),
                CellEvent::Synced { .. } => return,
                _ => {}
            }
        }
    };
    tokio::time::timeout(WAIT, read)
        .await
        .expect("the directory's snapshot in time");
    held
}

/// A drained cell says it is `draining` on its health endpoint, its
/// record, and its watch by the time the drain is answered; a chooser
/// stops choosing it, and the other cell takes the requests. With every
/// cell draining, a request fails at once with `Routing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_cell_is_not_chosen() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub).await;
    let m1 = cell(&spec, &directory, ready(M1, "m1-hub:1337"), &quiet).await;
    let m2 = cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &quiet).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready), (M2, CellState::Degraded)]).await;
    ask(&chooser.segment).await.expect("answered");
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (1, 0),
        "the ready cell first"
    );

    let health = m1.readiness.drain().await.expect("the cell drains");
    assert_eq!(health.state, CellState::Draining);
    assert!(!health.healthy(), "a draining cell's health answers 503");
    let mut watch = m1.readiness.watch().await.expect("the cell's watch");
    let first = tokio::time::timeout(WAIT, watch.next())
        .await
        .expect("the snapshot in time");
    assert!(
        matches!(
            first,
            Some(CellWatchEvent::Cell {
                state: CellState::Draining,
                ..
            })
        ),
        "{first:?}"
    );
    let published = tokio::time::timeout(WAIT, async {
        loop {
            let held = record(&directory, M1).await;
            if held.as_ref().is_none_or(|r| r.state == CellState::Draining) {
                return held;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the record says draining in time");
    assert!(published.is_some(), "the record was withdrawn at once");

    unchosen(&cells, M1).await;
    let before = offered(&[&m1, &m2]);
    ask(&chooser.segment)
        .await
        .expect("the other cell answered");
    assert_eq!(
        offered(&[&m1, &m2])[0],
        before[0],
        "the draining cell was offered a request"
    );
    assert_eq!(m2.forwards(), 1);

    m2.readiness.drain().await.expect("the cell drains");
    unchosen(&cells, M2).await;
    let error = ask(&chooser.segment).await.expect_err("no cell takes work");
    assert_eq!(error.kind(), ErrorKind::Routing, "{error}");
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
