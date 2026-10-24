//! A watch event of a kind a reader does not know fails closed for the
//! cell it concerns, and only that cell, until the reader has read it
//! again; the watch goes on. A proxy turns one event's kind into one this
//! build does not know, as a later build's hub would send it, and holds
//! the reading again until the test lets it through, so that the cell is
//! seen not chosen meanwhile while the other cell is chosen throughout.

mod common;

use common::boundary::{
    Model, RUNTIME, Running, ask, model_of, model_proxied, placement, run_graph, spec_of,
};
use common::cells::{
    Instance, Resolving, cell_hub, cell_hub_of, followed, global_cell, global_hub, launch,
    model_hub, settings, sorted, unresolved_cell_hub, until,
};
use common::proxy::Proxy;
use common::{Chunk, Events, Prompt, WAIT, reference_graph};
use gdl_model::SegmentPhase;
use gdl_runtime::{CellState, Cells, FollowedCell};

const M1: &str = "model-x-01-us-east-1";
const M2: &str = "model-x-02-us-east-1";

/// What a chooser's tables show of a cell that may be chosen: ready, with
/// `eligible` its eligible entries, and read.
fn takes_work(cell: Option<FollowedCell>, eligible: &[gdl_runtime::velo::InstanceId]) -> bool {
    cell.is_some_and(|cell| {
        cell.state == CellState::Ready && cell.eligible == eligible && !cell.unread
    })
}

/// Whether the tables keep `cell` from being chosen: none of its entries
/// is eligible.
fn not_chosen(cells: &Cells, cell: &str) -> bool {
    followed(cells, "f", cell).is_some_and(|cell| cell.eligible.is_empty())
}

/// M1's hub sends the global's hub an event of a kind it does not know
/// for M1, where its watch would say an entry retired. The global's hub
/// withholds M1 at once: its relay shows it `unavailable`, so the chooser
/// does not choose it, while M2 is chosen throughout. The hub reads M1
/// again from a second watch, held until the test lets it through; then M1
/// is chosen again, the retirement it could not read included. The
/// follow keeps its generation and its first watch, and the chooser's
/// relay watch is never cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hub_withholds_a_cell_whose_watch_sends_an_unknown_event_until_it_reads_it_again() {
    let directory = Resolving::new();
    let (global, relay) = cell_hub(global_hub(), &directory).await;
    // The global's hub reaches M1's hub through the proxy; M1's own
    // instances reach it directly.
    let (m1, m1_cell) = unresolved_cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let link = Proxy::new(m1.public_addr()).await;
    directory.resolve("m1-hub:1337", &format!("127.0.0.1:{}", link.port));
    let (m2, _) = cell_hub(model_hub(M2, "m2-hub:1337"), &directory).await;
    let (mut f1, f1b, _d1) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let (f2, _d2) = (
        Instance::ready(&m2, "f").await,
        Instance::ready(&m2, "d").await,
    );
    let config = settings(
        global.public_addr().port(),
        global.control_addr().port(),
        5_000,
        &global_cell(),
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let cells = chooser.cells().expect("a chooser");
    until(&cells, "both cells taking work", |cells| {
        takes_work(followed(cells, "f", M1), &sorted(&[&f1, &f1b]))
            && takes_work(followed(cells, "f", M2), &[f2.id()])
    })
    .await;
    let generation = followed(&cells, "f", M1).expect("followed").generation;
    let mut relay_watchers = relay.follows_watchers();
    relay_watchers.borrow_and_update();
    let mut m1_watchers = m1_cell.watchers();
    assert_eq!(*m1_watchers.borrow_and_update(), 1);

    link.rewrite_once(br#""event":"retired""#, br#""event":"xetired""#);
    link.hold(b"GET /v1/ext/gdl-cell/watch");
    f1.announce(SegmentPhase::Retired).await;
    until(&cells, "M1 withheld, M2 taking work", |cells| {
        not_chosen(cells, M1)
            && followed(cells, "f", M1).is_some_and(|m| m.state == CellState::Unavailable)
            && takes_work(followed(cells, "f", M2), &[f2.id()])
    })
    .await;
    tokio::time::timeout(WAIT, link.holding())
        .await
        .expect("the hub reads M1 again");
    assert!(not_chosen(&cells, M1), "not chosen until it is read again");
    assert!(takes_work(followed(&cells, "f", M2), &[f2.id()]));
    assert_eq!(*m1_watchers.borrow(), 1, "the first watch stays open");

    link.release();
    until(&cells, "M1 read again", |cells| {
        followed(cells, "f", M1).is_some_and(|m| {
            m.state == CellState::Ready && m.eligible == [f1b.id()] && m.retired == [f1.id()]
        })
    })
    .await;
    let m1_now = followed(&cells, "f", M1).expect("followed");
    assert_eq!(m1_now.generation, generation, "the same follow");
    assert!(takes_work(followed(&cells, "f", M2), &[f2.id()]));
    assert_eq!(relay.unknown_events(), 1);
    assert!(
        !relay_watchers.has_changed().expect("the hub lives"),
        "the chooser's relay watch was never cut"
    );
    assert_eq!(
        relay.following().borrow()[&("model_x".to_string(), M1.to_string())],
        generation
    );
    drop(chooser);
    for hub in [global, m1, m2] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The global's relay sends the chooser an event of a kind it does not
/// know for M1, where it would say an entry retired. At once M1 is unread
/// and not chosen, while M2 is chosen throughout. The chooser reads M1
/// again from the relay's follow of it alone, held until the test lets it
/// through; then M1 is chosen again with what the reading shows, the
/// retirement included. The relay watch is never cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chooser_leaves_a_cell_unread_for_an_unknown_relay_event_until_it_reads_it_again() {
    let directory = Resolving::new();
    let (global, relay) = cell_hub(global_hub(), &directory).await;
    let (m1, _) = cell_hub(model_hub(M1, "m1-hub:1337"), &directory).await;
    let (m2, _) = cell_hub(model_hub(M2, "m2-hub:1337"), &directory).await;
    let (mut f1, f1b, _d1) = (
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "f").await,
        Instance::ready(&m1, "d").await,
    );
    let (f2, _d2) = (
        Instance::ready(&m2, "f").await,
        Instance::ready(&m2, "d").await,
    );
    // The chooser reaches its hub's public listener through the proxy.
    let public = Proxy::new(global.public_addr()).await;
    let config = settings(
        public.port,
        global.control_addr().port(),
        5_000,
        &global_cell(),
    );
    let (_process, chooser) = launch(&config, "a").await.expect("the chooser launched");
    let cells = chooser.cells().expect("a chooser");
    until(&cells, "both cells taking work", |cells| {
        takes_work(followed(cells, "f", M1), &sorted(&[&f1, &f1b]))
            && takes_work(followed(cells, "f", M2), &[f2.id()])
    })
    .await;
    let mut relay_watchers = relay.follows_watchers();
    assert_eq!(*relay_watchers.borrow_and_update(), 1);

    let retired = format!(r#""event":"retired","class":"model_x","cell":"{M1}""#);
    let unknown = format!(r#""event":"xetired","class":"model_x","cell":"{M1}""#);
    public.rewrite_once(retired.as_bytes(), unknown.as_bytes());
    public.hold(format!("/follows/model_x/{M1}").as_bytes());
    f1.announce(SegmentPhase::Retired).await;
    until(&cells, "M1 unread, M2 taking work", |cells| {
        followed(cells, "f", M1).is_some_and(|m| m.unread)
            && not_chosen(cells, M1)
            && takes_work(followed(cells, "f", M2), &[f2.id()])
    })
    .await;
    tokio::time::timeout(WAIT, public.holding())
        .await
        .expect("the chooser reads M1 again");
    assert!(not_chosen(&cells, M1), "not chosen until it is read again");
    assert!(takes_work(followed(&cells, "f", M2), &[f2.id()]));
    assert_eq!(cells.unknown_events(), 1);

    public.release();
    until(&cells, "M1 read again", |cells| {
        followed(cells, "f", M1).is_some_and(|m| {
            !m.unread
                && m.state == CellState::Ready
                && m.eligible == [f1b.id()]
                && m.retired == [f1.id()]
        })
    })
    .await;
    assert!(takes_work(followed(&cells, "f", M2), &[f2.id()]));
    assert!(
        !relay_watchers.has_changed().expect("the hub lives"),
        "the relay watch was never cut"
    );
    drop(chooser);
    for hub in [global, m1, m2] {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// The chooser of the request-level tests, through `public`, a proxy in
/// front of its hub's public listener.
async fn chooser_through(
    spec: &gdl_runtime::DeploymentSpec,
    hub: &velo_hub::Hub,
    public: u16,
) -> Running<impl gdl::Next<Prompt, Chunk> + gdl_runtime::Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_cell());
    let config = settings(public, hub.control_addr().port(), 5_000, &extra);
    run_graph(&config, "a", spec, config.runtime.clone(), &|deployment| {
        reference_graph(deployment, &Events::quiet())
    })
    .await
    .expect("the chooser serves")
}

/// `M1`, ready with one entry, and `M2`, degraded with one: `home_first`
/// sends every request to `M1` while it may, and to `M2` only when it may
/// not.
fn cells_of(name: &'static str) -> Model<'static> {
    match name {
        M1 => Model {
            name,
            hub: "m1-hub:1337",
            quorum: 1,
            entries: 1,
        },
        _ => Model {
            name,
            hub: "m2-hub:1337",
            quorum: 2,
            entries: 1,
        },
    }
}

/// Wait until the chooser follows `M1` ready and `M2` degraded, each with
/// its entry and a load summary to choose it by.
async fn both_followed(cells: &Cells) {
    until(cells, "M1 ready and M2 degraded, with summaries", |cells| {
        let has = |cell, state| {
            followed(cells, "f", cell).is_some_and(|m| {
                m.state == state && m.eligible.len() == 1 && m.load.is_some() && !m.unread
            })
        };
        has(M1, CellState::Ready) && has(M2, CellState::Degraded)
    })
    .await;
}

/// Requests: `M1` takes them; the hub's follow of `M1` meets an event of
/// a kind it does not know, in place of one of `M1`'s load summaries, and
/// while the hub reads `M1` again the requests go to `M2`; once it has,
/// `M1` takes them again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_go_to_another_cell_while_a_hub_reads_one_again() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let chooser = chooser_through(&spec, &hub, hub.public_addr().port()).await;
    let cells = chooser.cells.clone().expect("a chooser");
    let assemble =
        |deployment: &gdl_runtime::Deployment| reference_graph(deployment, &Events::quiet());
    let (m1, link) = model_proxied(&spec, &directory, cells_of(M1), RUNTIME, &assemble).await;
    let m2 = model_of(&spec, &directory, cells_of(M2), &|r| r, &assemble).await;
    both_followed(&cells).await;
    let generation = followed(&cells, "f", M1).expect("followed").generation;
    ask(&chooser.segment).await.expect("answered");
    assert_eq!((m1.forwards(), m2.forwards()), (1, 0));

    link.rewrite_once(br#""event":"load""#, br#""event":"lxad""#);
    link.hold(b"GET /v1/ext/gdl-cell/watch");
    tokio::time::timeout(WAIT, link.holding())
        .await
        .expect("the hub reads M1 again");
    until(&cells, "M1 withheld", |cells| not_chosen(cells, M1)).await;
    ask(&chooser.segment).await.expect("answered");
    assert_eq!((m1.forwards(), m2.forwards()), (1, 1), "M2 took it");

    link.release();
    both_followed(&cells).await;
    ask(&chooser.segment).await.expect("answered");
    assert_eq!((m1.forwards(), m2.forwards()), (2, 1), "M1 took it again");
    assert_eq!(
        followed(&cells, "f", M1).expect("followed").generation,
        generation,
        "the same follow, not dropped and followed again"
    );
}

/// Requests: `M1` takes them; the chooser's relay watch meets an event of
/// a kind it does not know for `M1`, in place of one of its load
/// summaries, and while the chooser reads `M1` again the requests go to
/// `M2`; once it has, `M1` takes them again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_go_to_another_cell_while_a_chooser_reads_one_again() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (hub, _) = cell_hub_of(&spec, global_hub(), &directory).await;
    let public = Proxy::new(hub.public_addr()).await;
    let chooser = chooser_through(&spec, &hub, public.port).await;
    let cells = chooser.cells.clone().expect("a chooser");
    let assemble =
        |deployment: &gdl_runtime::Deployment| reference_graph(deployment, &Events::quiet());
    let m1 = model_of(&spec, &directory, cells_of(M1), &|r| r, &assemble).await;
    let m2 = model_of(&spec, &directory, cells_of(M2), &|r| r, &assemble).await;
    both_followed(&cells).await;
    ask(&chooser.segment).await.expect("answered");
    assert_eq!((m1.forwards(), m2.forwards()), (1, 0));

    let load = format!(r#""event":"load","class":"model_x","cell":"{M1}""#);
    let unknown = format!(r#""event":"lxad","class":"model_x","cell":"{M1}""#);
    public.rewrite_once(load.as_bytes(), unknown.as_bytes());
    public.hold(format!("/follows/model_x/{M1}").as_bytes());
    tokio::time::timeout(WAIT, public.holding())
        .await
        .expect("the chooser reads M1 again");
    assert!(followed(&cells, "f", M1).is_some_and(|m| m.unread));
    ask(&chooser.segment).await.expect("answered");
    assert_eq!((m1.forwards(), m2.forwards()), (1, 1), "M2 took it");

    public.release();
    both_followed(&cells).await;
    ask(&chooser.segment).await.expect("answered");
    assert_eq!((m1.forwards(), m2.forwards()), (2, 1), "M1 took it again");
    assert_eq!(cells.unknown_events(), 1);
}
