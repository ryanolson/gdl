//! A chooser fails over to another cell before acceptance: a hand-off
//! into a cell is an offer, and when the cell declines it (no room, or
//! retired), or the hand-off provably never left the chooser, another
//! cell is tried inside the same call, up to `cell_failovers` others.
//! A hand-off whose answer does not come is an unknown outcome: the
//! request fails with `OutcomeUnknown` and no other cell is tried.
//!
//! Each test makes `M1` the only `ready` cell, so `home_first` always
//! offers it first, and the other cells `degraded`, which take the
//! request only once `M1` declined it.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::boundary::{
    Model, ModelCell, RUNTIME, Running, SUMMARY, ask, model_of, placement, run_graph, spec_of,
};
use common::cells::{Resolving, cell_hub_of, followed, global_cell, global_hub, settings, until};
use common::streamed::{self, Script, replies, words};
use common::{Chunk, Events, Plan, Prompt, WAIT, collect, prompt, reference_graph};
use futures::StreamExt;
use gdl::{Choice, Cx, Encoded, ErrorKind, Instances, Next, Request, Selector, Selectors};
use gdl_runtime::{
    CellState, Cells, Deployment, DeploymentSpec, Register, RequestRouter, RuntimeConfig,
};
use velo_hub::Hub;

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";
const M3: &str = "model-x-03-us-east-1";

/// The one `ready` cell: one entry instance makes its quorum.
fn ready(name: &'static str, hub: &'static str) -> Model<'static> {
    Model {
        name,
        hub,
        quorum: 1,
        entries: 1,
    }
}

/// A `degraded` cell: two entry instances make its quorum; it has one.
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

/// A chooser, an instance of `a` in the global cell at `hub`, with
/// `runtime` added to its `[runtime]` table and `selectors` registered,
/// running the graph `assemble` builds.
async fn chooser<H: Register + Send + Sync + 'static>(
    spec: &DeploymentSpec,
    hub: &Hub,
    runtime: &str,
    selectors: Selectors,
    assemble: &(dyn Fn(&Deployment) -> gdl::Result<H> + Sync),
) -> Running<H> {
    let extra = format!("{}{RUNTIME}{runtime}", global_cell());
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
    run_graph(&config, "a", spec, runtime, assemble)
        .await
        .expect("the chooser serves")
}

/// A model cell of the reference graph whose entry instances log to
/// `events` and take `entry`'s runtime settings.
async fn reference_cell(
    spec: &DeploymentSpec,
    directory: &Resolving,
    cell: Model<'_>,
    entry: &(dyn Fn(RuntimeConfig) -> RuntimeConfig + Sync),
    events: &Arc<Events>,
) -> ModelCell<impl Next<Prompt, Chunk> + Register + use<>> {
    model_of(spec, directory, cell, entry, &|deployment| {
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

/// The forward hand-offs that reached each of `cells`, taken or refused.
fn offered<H>(cells: &[&ModelCell<H>]) -> Vec<usize> {
    cells
        .iter()
        .map(|cell| cell.entries.iter().map(Running::offered).sum())
        .collect()
}

/// How many times B started in a cell whose nodes log to `events`.
fn started(events: &Events) -> usize {
    events
        .snapshot()
        .iter()
        .filter(|event| event.starts_with("B:start:"))
        .count()
}

/// The runtime settings of an entry that admits one request at a time.
fn one_at_a_time(runtime: RuntimeConfig) -> RuntimeConfig {
    RuntimeConfig {
        admission_limit: 1,
        ..runtime
    }
}

/// Send a request whose sink holds its stream open after one item, and
/// return the stream once that item came: the request is admitted, and
/// holds its entry's admission until the stream is dropped.
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

/// The entry of the cell chosen first is full: it declines, and the
/// request is answered by the other cell. The full entry's B never runs
/// for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_with_no_room_declines_and_another_answers() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (e1, e2) = (Events::new(), Events::new());
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, "", Selectors::new(), &|deployment| {
        reference_graph(deployment, &Events::quiet())
    })
    .await;
    let m1 = reference_cell(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        &one_at_a_time,
        &e1,
    )
    .await;
    let m2 = reference_cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &|r| r, &e2).await;
    following(
        &tables(&chooser),
        &[(M1, CellState::Ready), (M2, CellState::Degraded)],
    )
    .await;
    let held = hold(&chooser, "held").await;
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (1, 0),
        "held where it should be"
    );

    ask(&chooser.segment).await.expect("another cell answered");
    assert_eq!(offered(&[&m1, &m2]), [2, 1]);
    assert_eq!((m1.forwards(), m2.forwards()), (1, 1));
    assert_eq!(started(&e1), 1, "the full cell ran the declined request");
    assert_eq!(started(&e2), 1);
    drop(held);
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// Records the instances it is offered and how many of its leases were
/// held when it was asked, takes the first instance, and holds a lease
/// per choice.
#[derive(Clone, Default)]
struct Recording {
    offered: Arc<Mutex<Vec<BTreeSet<u128>>>>,
    held: Arc<Mutex<Vec<usize>>>,
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
        let held = self.leases.fetch_add(1, Ordering::SeqCst);
        self.held.lock().expect("lock").push(held);
        let chosen = instances.get(0).expect("never empty");
        Ok(Choice::new(chosen)
            .with_lease(Lease(Arc::clone(&self.leases)))
            .with_meta("chosen", chosen.id().as_u128().to_string()))
    }
}

/// The chosen cell's only entry instance has retired and refuses new
/// work, though the chooser's table still holds it: the request is
/// answered by the other cell. With a selector on the boundary, the
/// declined choice's lease is released before the next cell is chosen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retired_entry_declines_and_another_cell_answers() {
    let spec = spec_of(placement().route("B", RequestRouter::Selector("recording".into())));
    let directory = Resolving::new();
    let (e1, e2) = (Events::new(), Events::new());
    let recording = Recording::default();
    let mut selectors = Selectors::new();
    selectors
        .add("recording", recording.clone())
        .expect("a selector");
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, "", selectors, &|deployment| {
        reference_graph(deployment, &Events::quiet())
    })
    .await;
    let m1 = reference_cell(&spec, &directory, ready(M1, "m1-hub:1337"), &|r| r, &e1).await;
    let m2 = reference_cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &|r| r, &e2).await;
    following(
        &tables(&chooser),
        &[(M1, CellState::Ready), (M2, CellState::Degraded)],
    )
    .await;
    m1.entries[0].segment.close();

    ask(&chooser.segment).await.expect("another cell answered");
    assert_eq!(offered(&[&m1, &m2]), [1, 1]);
    assert_eq!((m1.forwards(), m2.forwards()), (0, 1));
    assert_eq!((started(&e1), started(&e2)), (0, 1));
    let id = |cell: &ModelCell<_>| -> BTreeSet<u128> {
        cell.entries
            .iter()
            .map(|entry: &Running<_>| entry.segment.peer_info().instance_id().as_u128())
            .collect()
    };
    assert_eq!(
        *recording.offered.lock().expect("lock"),
        [id(&m1), id(&m2)],
        "each choice was not offered its own cell's instances alone"
    );
    assert_eq!(
        *recording.held.lock().expect("lock"),
        [0, 0],
        "the declined choice's lease was held when the next was made"
    );
    let released = tokio::time::timeout(WAIT, async {
        while recording.leases.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(released.is_ok(), "a lease outlived the request");
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// With every cell full, a request is offered to at most
/// `1 + cell_failovers` cells, each once, and fails with the last
/// refusal's `Overloaded`; with more failovers than cells, it runs out of
/// cells first, and fails the same way. The cells are filled through the
/// chooser itself, each held request taking the next cell with room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn when_every_cell_declines_the_request_is_overloaded() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let assemble = |deployment: &Deployment| reference_graph(deployment, &quiet);
    let default = chooser(&spec, &hub, "", Selectors::new(), &assemble).await;
    let one = chooser(
        &spec,
        &hub,
        "cell_failovers = 1\n",
        Selectors::new(),
        &assemble,
    )
    .await;
    let none = chooser(
        &spec,
        &hub,
        "cell_failovers = 0\n",
        Selectors::new(),
        &assemble,
    )
    .await;
    let many = chooser(
        &spec,
        &hub,
        "cell_failovers = 4\n",
        Selectors::new(),
        &assemble,
    )
    .await;
    let m1 = reference_cell(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        &one_at_a_time,
        &quiet,
    )
    .await;
    let m2 = reference_cell(
        &spec,
        &directory,
        degraded(M2, "m2-hub:1337"),
        &one_at_a_time,
        &quiet,
    )
    .await;
    let m3 = reference_cell(
        &spec,
        &directory,
        degraded(M3, "m3-hub:1337"),
        &one_at_a_time,
        &quiet,
    )
    .await;
    let cells = [&m1, &m2, &m3];
    let expected = [
        (M1, CellState::Ready),
        (M2, CellState::Degraded),
        (M3, CellState::Degraded),
    ];
    for chooser in [&default, &one, &none, &many] {
        following(&tables(chooser), &expected).await;
    }
    let mut held = Vec::new();
    for text in ["held-1", "held-2", "held-3"] {
        held.push(hold(&default, text).await);
    }
    assert_eq!(
        cells.map(|cell| cell.forwards()),
        [1, 1, 1],
        "each cell holds one"
    );

    for (chooser, tried) in [(&default, 3), (&one, 2), (&none, 1), (&many, 3)] {
        let before = offered(&cells);
        let error = ask(&chooser.segment).await.expect_err("every cell is full");
        assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
        let now: Vec<usize> = offered(&cells)
            .iter()
            .zip(&before)
            .map(|(now, before)| now - before)
            .collect();
        assert_eq!(now.iter().sum::<usize>(), tried, "offers {now:?}");
        assert!(
            now.iter().all(|n| *n <= 1),
            "a cell was offered twice: {now:?}"
        );
        assert_eq!(now[0], 1, "the ready cell was not offered first: {now:?}");
    }
    assert_eq!(cells.map(|cell| cell.forwards()), [1, 1, 1]);
    drop(held);
    for hub in [hub, m1.hub, m2.hub, m3.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A hand-off that never left is not a refusal: when a full cell has
/// declined the request and the next cell's hand-off never left, the
/// request fails with the full cell's `Overloaded`, whether its
/// failovers run out there or its cells do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hand_off_that_never_left_does_not_replace_the_last_refusal() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let assemble = |deployment: &Deployment| reference_graph(deployment, &quiet);
    let cells_out = chooser(&spec, &hub, "", Selectors::new(), &assemble).await;
    let failovers_out = chooser(
        &spec,
        &hub,
        "cell_failovers = 1\n",
        Selectors::new(),
        &assemble,
    )
    .await;
    let m1 = reference_cell(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        &one_at_a_time,
        &quiet,
    )
    .await;
    let m2 = reference_cell(
        &spec,
        &directory,
        degraded(M2, "m2-hub:1337"),
        &|r| r,
        &quiet,
    )
    .await;
    let expected = [(M1, CellState::Ready), (M2, CellState::Degraded)];
    for chooser in [&cells_out, &failovers_out] {
        following(&tables(chooser), &expected).await;
    }
    let held = hold(&cells_out, "held").await;
    assert_eq!(
        (m1.forwards(), m2.forwards()),
        (1, 0),
        "held where it should be"
    );
    let unsent = m2.entries[0].segment.peer_info().instance_id();
    for chooser in [&cells_out, &failovers_out] {
        chooser.node.unregister_peer(unsent);
    }

    for chooser in [&cells_out, &failovers_out] {
        let before = offered(&[&m1, &m2]);
        let error = ask(&chooser.segment)
            .await
            .expect_err("the full cell declined and the other was never reached");
        assert_eq!(error.kind(), ErrorKind::Overloaded, "{error}");
        let now = offered(&[&m1, &m2]);
        assert_eq!([now[0] - before[0], now[1] - before[1]], [1, 0]);
    }
    drop(held);
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The chosen cell's entry admits the request but its acknowledgement
/// comes after the chooser's setup timeout: the request fails with
/// `OutcomeUnknown`, the entry's B ran exactly once, and no other cell
/// was offered it. Runs for a setup timeout (1 s): that is the wait it
/// is about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unanswered_hand_off_is_an_unknown_outcome_and_not_retried() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (e1, e2) = (Events::new(), Events::new());
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, "", Selectors::new(), &|deployment| {
        reference_graph(deployment, &Events::quiet())
    })
    .await;
    let late = |runtime: RuntimeConfig| {
        let mut runtime = runtime;
        runtime.faults.delay_forward_ack = Some(runtime.setup_timeout * 3 / 2);
        runtime
    };
    let m1 = reference_cell(&spec, &directory, ready(M1, "m1-hub:1337"), &late, &e1).await;
    let m2 = reference_cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &|r| r, &e2).await;
    following(
        &tables(&chooser),
        &[(M1, CellState::Ready), (M2, CellState::Degraded)],
    )
    .await;

    let error = ask(&chooser.segment)
        .await
        .expect_err("the outcome is not known");
    assert_eq!(error.kind(), ErrorKind::OutcomeUnknown, "{error}");
    e1.wait_for("B:start:across", WAIT).await;
    assert_eq!(started(&e1), 1);
    assert_eq!(offered(&[&m1, &m2]), [1, 0], "another cell was offered it");
    assert_eq!(started(&e2), 0);
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A streamed request declined by one cell is answered by another, which
/// receives every input item; the declining cell receives none, and
/// every item is sent once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streamed_request_declined_by_one_cell_is_answered_whole_by_another() {
    let spec = spec_of(placement());
    let spec = DeploymentSpec::new(streamed::contract::manifest(), spec.placement)
        .expect("valid test fixture");
    let directory = Resolving::new();
    let (input, e1, e2) = (Events::new(), Events::new(), Events::new());
    let hub = global_hub_of(&spec, &directory).await;
    let quiet = Events::quiet();
    let chooser = chooser(&spec, &hub, "", Selectors::new(), &|deployment| {
        streamed::contract::assemble(deployment, streamed::bindings(&quiet))
    })
    .await;
    let cell = |model: Model<'static>, events: &Arc<Events>| {
        let events = Arc::clone(events);
        let directory = &directory;
        let spec = &spec;
        async move {
            model_of(spec, directory, model, &|r| r, &move |deployment| {
                streamed::contract::assemble(deployment, streamed::bindings(&events))
            })
            .await
        }
    };
    let m1 = cell(ready(M1, "m1-hub:1337"), &e1).await;
    let m2 = cell(degraded(M2, "m2-hub:1337"), &e2).await;
    following(
        &tables(&chooser),
        &[(M1, CellState::Ready), (M2, CellState::Degraded)],
    )
    .await;
    m1.entries[0].segment.close();

    let said = ["one", "two", "three"];
    let out = tokio::time::timeout(WAIT, async {
        let out = chooser
            .segment
            .call(
                Cx::new(),
                streamed::ask("across", Script::default(), words(&input, &said, false)),
            )
            .await?;
        Ok::<_, gdl::Error>(collect(out).await)
    })
    .await
    .expect("answered in time")
    .expect("handed on");
    assert_eq!(out, replies("across", &said));
    for word in said {
        let sent = input
            .snapshot()
            .iter()
            .filter(|event| **event == format!("input:sent:{word}"))
            .count();
        assert_eq!(sent, 1, "{word} was sent {sent} times");
    }
    let served = |cell: &ModelCell<_>| {
        cell.entries
            .iter()
            .map(|entry: &Running<_>| entry.segment.served())
            .fold((0, 0), |(f, i), s| (f + s.forwards, i + s.inputs))
    };
    assert_eq!(served(&m1), (0, 0), "the declining cell took input");
    assert_eq!(served(&m2), (1, 1));
    assert_eq!(offered(&[&m1, &m2]), [1, 1]);
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// A hand-off velo refuses before it writes anything is tried on another
/// cell: here the chosen entry instance is no longer registered with the
/// chooser's velo node, though its table still holds it, as when the
/// instance leaves between the choice and the send.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hand_off_that_never_left_is_tried_on_another_cell() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (e1, e2) = (Events::new(), Events::new());
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, "", Selectors::new(), &|deployment| {
        reference_graph(deployment, &Events::quiet())
    })
    .await;
    let m1 = reference_cell(&spec, &directory, ready(M1, "m1-hub:1337"), &|r| r, &e1).await;
    let m2 = reference_cell(&spec, &directory, degraded(M2, "m2-hub:1337"), &|r| r, &e2).await;
    let cells = tables(&chooser);
    following(&cells, &[(M1, CellState::Ready), (M2, CellState::Degraded)]).await;
    let gone = m1.entries[0].segment.peer_info().instance_id();
    chooser.node.unregister_peer(gone);
    assert!(
        followed(&cells, "f", M1).is_some_and(|m| m.eligible == [gone]),
        "the table no longer holds the instance"
    );

    ask(&chooser.segment).await.expect("another cell answered");
    assert_eq!(offered(&[&m1, &m2]), [0, 1]);
    assert_eq!((started(&e1), started(&e2)), (0, 1));
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The runtime settings of an entry that admits `limit` requests at once.
fn admitting(limit: usize) -> impl Fn(RuntimeConfig) -> RuntimeConfig + Sync {
    move |runtime| RuntimeConfig {
        admission_limit: limit,
        ..runtime
    }
}

/// Two ready home cells: `M1` holds 3 of the 4 roots it admits, `M2` none
/// of its 2. Requests go to the lighter, `M2`. Once `M2` is full, but
/// before its summary says so, the request offered to it is declined and
/// answered by `M1`, and the chooser damps `M2`: the next request is not
/// offered to it at all. The test fills `M2` just after a summary of it
/// arrived, so that the next, which would show it full, is a period
/// (1 s) away. It runs for about 4 s: it waits for each cell's summaries,
/// which come a period apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_that_declines_is_damped_and_the_lighter_cell_is_preferred() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let quiet = Events::quiet();
    let hub = global_hub_of(&spec, &directory).await;
    let chooser = chooser(&spec, &hub, "", Selectors::new(), &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await;
    let cells = tables(&chooser);
    let m1 = reference_cell(
        &spec,
        &directory,
        ready(M1, "m1-hub:1337"),
        &admitting(4),
        &quiet,
    )
    .await;
    following(&cells, &[(M1, CellState::Ready)]).await;
    let mut held = Vec::new();
    for text in ["m1-a", "m1-b", "m1-c"] {
        held.push(hold(&chooser, text).await);
    }
    let m2 = reference_cell(
        &spec,
        &directory,
        ready(M2, "m2-hub:1337"),
        &admitting(2),
        &quiet,
    )
    .await;
    let admitted = |cells: &Cells, cell: &str| {
        followed(cells, "f", cell).and_then(|m| m.load.map(|load| load.admitted))
    };
    until(
        &cells,
        "M1's summary of its held roots, and M2's",
        |cells| admitted(cells, M1) == Some(3) && admitted(cells, M2).is_some(),
    )
    .await;

    for _ in 0..10 {
        ask(&chooser.segment).await.expect("answered");
    }
    assert_eq!((m1.forwards(), m2.forwards()), (3, 10), "not the lighter");

    // A summary of M2 that has just arrived, with nothing admitted.
    until(&cells, "a fresh summary of an idle M2", |cells| {
        followed(cells, "f", M2).is_some_and(|m| {
            m.load.is_some_and(|load| load.admitted == 0)
                && m.load_age.is_some_and(|age| age < SUMMARY / 4)
        })
    })
    .await;
    for text in ["m2-a", "m2-b"] {
        held.push(hold(&chooser, text).await);
    }
    assert_eq!((m1.forwards(), m2.forwards()), (3, 12), "not the lighter");
    let before = offered(&[&m1, &m2]);
    ask(&chooser.segment).await.expect("M1 answered");
    assert_eq!((m1.forwards(), m2.forwards()), (4, 12));
    assert_eq!(
        offered(&[&m1, &m2]),
        [before[0] + 1, before[1] + 1],
        "M2 was not offered the request first"
    );
    let m2_now = followed(&cells, "f", M2).expect("followed");
    assert!(m2_now.damped.is_some(), "M2 was not damped: {m2_now:?}");

    ask(&chooser.segment).await.expect("M1 answered");
    assert_eq!((m1.forwards(), m2.forwards()), (5, 12));
    assert_eq!(
        offered(&[&m1, &m2]),
        [before[0] + 2, before[1] + 1],
        "the damped cell was offered the next request"
    );
    drop(held);
    for hub in [hub, m1.hub, m2.hub] {
        hub.shutdown().await.expect("valid test fixture");
    }
}
