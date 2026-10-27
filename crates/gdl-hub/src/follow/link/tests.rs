//! One follow link against a server in the test's own thread, on a paused
//! clock: what the link reports of a watch's health to its backoff.

use std::sync::Arc;
use std::time::Duration;

use gdl_cells::{CellRecord, CellState};
use gdl_watch::{Backoff, CellWatchEvent, CellWatcher, Follower};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::super::relay::Relay;
use super::run;
use crate::{CellHealth, Quorum};

const BASE: Duration = Duration::from_millis(250);
const CAP: Duration = Duration::from_secs(5);

/// A server that hands each connection to the test once its request's
/// head is read, with when it was asked.
async fn server() -> (String, mpsc::UnboundedReceiver<(TcpStream, Instant)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let address = listener.local_addr().expect("an address").to_string();
    let (accept, accepted) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                    break;
                }
                head.push(byte[0]);
            }
            let _ = accept.send((stream, Instant::now()));
        }
    });
    (address, accepted)
}

async fn send(stream: &mut TcpStream, data: &str) {
    stream
        .write_all(format!("data: {data}\n\n").as_bytes())
        .await
        .expect("the link reads");
}

fn relay() -> Arc<Relay> {
    let quorum = Quorum::new([("a", 1)], Duration::ZERO);
    let health = CellHealth::of(&quorum, CellState::Unavailable, &Default::default());
    Arc::new(Relay::new(
        16,
        tokio::sync::watch::Sender::new(health),
        super::super::Choosers::new([]),
        mpsc::unbounded_channel().0,
        gdl_model::HubTiming::DEFAULT_LOST_CHOOSER,
    ))
}

fn record(hub: String) -> CellRecord {
    CellRecord {
        name: "m".into(),
        class: "model_x".into(),
        region: "r".into(),
        home: Some("g".into()),
        contract: 1,
        placement: 1,
        state: CellState::Ready,
        epoch: 1,
        seq: 1,
        hub,
        lease: Duration::from_secs(30),
        load: None,
    }
}

/// A backoff at its cap.
fn capped() -> Backoff {
    let mut backoff = Backoff::new(BASE, CAP);
    for _ in 0..8 {
        backoff.wait(None, gdl_watch::WatchEnd::Failed, Instant::now());
    }
    backoff
}

/// Start a link of the cell at `address` with `backoff`, and answer its
/// first watch with the cell, `unknown` if it is to have an event of a
/// kind this build does not know, and the snapshot's end: the link, and
/// the watch, still open.
async fn linked(
    accepted: &mut mpsc::UnboundedReceiver<(TcpStream, Instant)>,
    address: String,
    backoff: Backoff,
    unknown: bool,
) -> (
    tokio::task::JoinHandle<(String, u64, bool, Backoff)>,
    TcpStream,
) {
    let relay = relay();
    let (generation, close) = relay.begin("model_x", "m");
    let me = Follower {
        class: "global".into(),
        cell: "g".into(),
        run: 1,
        generation,
    };
    let link = tokio::spawn(run(
        Arc::clone(&relay),
        CellWatcher::new().expect("a client"),
        me,
        record(address),
        generation,
        close,
        backoff,
    ));
    let (mut first, _) = accepted.recv().await.expect("the first watch");
    first
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n",
        )
        .await
        .expect("the link reads");
    let cell = CellWatchEvent::Cell {
        name: "m".into(),
        class: "model_x".into(),
        epoch: 1,
        state: CellState::Ready,
        load: None,
    };
    send(
        &mut first,
        &serde_json::to_string(&cell).expect("serializes"),
    )
    .await;
    if unknown {
        send(&mut first, r#"{"event":"paused"}"#).await;
    }
    send(
        &mut first,
        &serde_json::to_string(&CellWatchEvent::Synced).expect("serializes"),
    )
    .await;
    (link, first)
}

/// A link whose backoff is at its cap reads a snapshot with an event of a
/// kind this build does not know, and reads the cell again after a wait;
/// that reading never answers, and ends at its snapshot's 30 s bound. No
/// watch was healthy since the snapshot: the wait after the link ends is
/// drawn at the cap, not from the base, however long the wait and the
/// reading took.
#[tokio::test(start_paused = true)]
async fn a_failed_reading_does_not_count_as_a_healthy_watch() {
    let (address, mut accepted) = server().await;
    let (link, first) = linked(&mut accepted, address, capped(), true).await;
    // The reading is asked for and never answered.
    let (_second, asked) = accepted.recv().await.expect("the reading");
    let ended = asked + gdl_watch::WatchLimits::default().synced_within;
    let _ = link.await.expect("the link ends");
    let waited = Instant::now() - ended;
    assert!(
        waited >= CAP / 2 && waited <= CAP,
        "waited {waited:?} after the reading failed"
    );
    drop(first);
}

/// The control: a link whose backoff is at its cap reads a whole snapshot
/// and is healthy for longer than the healthy span, when an event of a
/// kind this build does not know makes it read the cell again at once;
/// that reading never answers. The healthy watch returned the step to the
/// base, and the failed reading doubles it: the wait after the link ends
/// is drawn from the base.
#[tokio::test(start_paused = true)]
async fn a_healthy_watch_let_go_of_for_a_reading_returns_the_step_to_the_base() {
    let (address, mut accepted) = server().await;
    let (link, mut first) = linked(&mut accepted, address, capped(), false).await;
    // Within the watch's idle bound, for longer than the healthy span.
    for _ in 0..7 {
        tokio::time::sleep(Duration::from_secs(5)).await;
        first.write_all(b":\n\n").await.expect("the link reads");
    }
    send(&mut first, r#"{"event":"paused"}"#).await;
    let (_second, asked) = accepted.recv().await.expect("the reading");
    let ended = asked + gdl_watch::WatchLimits::default().synced_within;
    let _ = link.await.expect("the link ends");
    let waited = Instant::now() - ended;
    assert!(
        waited >= BASE / 2 && waited <= BASE,
        "waited {waited:?} after the reading failed"
    );
    drop(first);
}
