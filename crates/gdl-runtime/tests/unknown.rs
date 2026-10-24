//! A watch event of a kind a reader does not know fails closed for the
//! cell it concerns, and only that cell, until the reader has read it
//! again; the watch goes on. A proxy turns one event's kind into one this
//! build does not know, as a later build's hub would send it, and holds
//! the reading again until the test lets it through, so that the cell is
//! seen not chosen meanwhile while the other cell is chosen throughout.

mod common;

use common::WAIT;
use common::cells::{
    Instance, Resolving, cell_hub, followed, global_cell, global_hub, launch, model_hub, settings,
    sorted, unresolved_cell_hub, until,
};
use common::proxy::Proxy;
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
