//! A global's hub follows a model cell over its watch only once the
//! watch's snapshot is complete, within its bounds: a peer that sends the
//! snapshot's first event and then only keep-alives, or more snapshot
//! than the limit, ends the follow with nothing of the cell on the relay,
//! and the cell is followed again after the link's backoff, as for any
//! ended watch. `M` is served by a test server that speaks the cell
//! watch's protocol as the test chooses; `CELL` is a real cell's hub. The
//! global's bounds are set short for the test, so that it outlasts them.

mod common;

use std::time::Duration;

use common::{
    CELL, Instance, Records, Resolving, WAIT, global_default, meet_the_quorum, model, serve_cell,
    timing,
};
use futures::{FutureExt, StreamExt};
use gdl_cells::{CellDirectory, CellRecord, CellState};
use gdl_hub::CellTiming;
use gdl_model::SegmentPhase;
use gdl_watch::{CellWatchEvent, CellWatcher, FollowEvent, WatchLimits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::Instant;
use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

const M: &str = "model-x-07-us-east-1";

/// The global's bounds on a followed cell's snapshot: short, so that the
/// test outlasts them.
const LIMITS: WatchLimits = WatchLimits {
    event: 4096,
    snapshot: 64 << 10,
    synced_within: Duration::from_millis(500),
};

/// A server that answers every request as a cell's watch, and hands each
/// connection to the test.
struct Server {
    address: String,
    accepted: mpsc::UnboundedReceiver<Watch>,
}

impl Server {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let address = listener.local_addr().expect("an address").to_string();
        let (accept, accepted) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let accept = accept.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                            return;
                        }
                        head.push(byte[0]);
                    }
                    let query = String::from_utf8_lossy(&head)
                        .split_whitespace()
                        .nth(1)
                        .and_then(|path| path.split_once('?'))
                        .map(|(_, query)| query.to_string())
                        .unwrap_or_default();
                    let _ = accept.send(Watch {
                        stream,
                        query,
                        at: Instant::now(),
                    });
                });
            }
        });
        Self { address, accepted }
    }

    async fn next(&mut self) -> Watch {
        tokio::time::timeout(WAIT, self.accepted.recv())
            .await
            .expect("a watch in time")
            .expect("the server runs")
    }
}

/// One watch of the server.
struct Watch {
    stream: TcpStream,
    query: String,
    /// When it was asked for.
    at: Instant,
}

impl Watch {
    /// The follow's generation the watch names.
    fn generation(&self) -> u64 {
        self.query
            .split('&')
            .find_map(|pair| pair.strip_prefix("generation="))
            .and_then(|generation| generation.parse().ok())
            .unwrap_or_else(|| panic!("a generation in {:?}", self.query))
    }

    /// Answer, and send the snapshot's first event: the cell of `record`.
    async fn begin(&mut self, record: &CellRecord) {
        let head =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
        self.write(head.as_bytes()).await;
        self.send(&CellWatchEvent::Cell {
            name: M.into(),
            class: "model_x".into(),
            epoch: record.epoch,
            state: CellState::Ready,
            load: None,
        })
        .await;
    }

    async fn send(&mut self, event: &CellWatchEvent) {
        let sent = format!(
            "data: {}\n\n",
            serde_json::to_string(event).expect("an event serializes")
        );
        self.write(sent.as_bytes()).await;
    }

    async fn write(&mut self, bytes: &[u8]) {
        self.stream
            .write_all(bytes)
            .await
            .expect("the watcher reads");
    }

    /// Send keep-alives, every 50 ms, until the watcher hangs up.
    fn keep_alive(mut self) {
        tokio::spawn(async move {
            while self.stream.write_all(b":\n\n").await.is_ok() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    }
}

fn ready() -> CellWatchEvent {
    CellWatchEvent::Ready {
        segment: "f".into(),
        peer: PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(b"tcp://127.0.0.1:1".to_vec()),
        ),
    }
}

/// A global's hub with `timing`, following `M`, served by a test server,
/// beside the real model cell `CELL`.
struct Setup {
    server: Server,
    /// Before the global could ask for `M`'s watch.
    began: Instant,
    record: CellRecord,
    global: std::sync::Arc<gdl_hub::CellReadiness<Resolving>>,
    _hubs: (velo_hub::Hub, velo_hub::Hub),
    _instances: Vec<Instance>,
}

async fn setup(timing: CellTiming) -> Setup {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, _model) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        common::timing(),
    )
    .await;
    let (instances, _) = meet_the_quorum(&model_hub).await;
    let (like, _) = records.until(CellState::Ready).await;
    let (global_hub, global) = serve_cell(global_default(), &directory, timing).await;
    let server = Server::new().await;
    let record = CellRecord {
        name: M.into(),
        state: CellState::Ready,
        hub: server.address.clone(),
        ..like
    };
    let began = Instant::now();
    directory
        .inner
        .publish(record.clone())
        .await
        .expect("stored");
    Setup {
        server,
        began,
        record,
        global,
        _hubs: (model_hub, global_hub),
        _instances: instances,
    }
}

fn bounded() -> CellTiming {
    CellTiming {
        watch: LIMITS,
        ..timing()
    }
}

/// Whether `event` is of the cell `of`.
fn of(event: &FollowEvent, of: &str) -> bool {
    match event {
        FollowEvent::Follow { cell, .. }
        | FollowEvent::State { cell, .. }
        | FollowEvent::Load { cell, .. }
        | FollowEvent::Offering { cell, .. }
        | FollowEvent::Exposed { cell, .. }
        | FollowEvent::Retired { cell, .. }
        | FollowEvent::Left { cell, .. }
        | FollowEvent::Dropped { cell, .. }
        | FollowEvent::Withdrawn { cell, .. } => cell == of,
        FollowEvent::Synced => false,
    }
}

/// What the relay's watch `watch` has sent of `M` so far.
fn of_m(watch: &mut gdl_hub::FollowsWatch) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    while let Some(Some(event)) = watch.next().now_or_never() {
        if of(&event, M) {
            events.push(event);
        }
    }
    events
}

/// `M`'s watch sends the cell and then only keep-alives: the follow ends
/// at the snapshot's bound, nothing of `M` was installed in the relay,
/// and `M` is followed again in a new generation after the refollow wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_that_never_completes_ends_the_follow_at_its_bound() {
    let Setup {
        mut server,
        began,
        record,
        global,
        _hubs,
        _instances,
    } = setup(bounded()).await;
    let mut relay = global.follows();
    let mut first = server.next().await;
    let generation = first.generation();
    first.begin(&record).await;
    first.keep_alive();
    let second = server.next().await;
    let waited = second.at - began;
    assert!(
        waited >= LIMITS.synced_within + timing().refollow / 2,
        "followed again after {waited:?}"
    );
    assert_eq!(second.generation(), generation + 1, "a new follow");
    assert_eq!(
        global.following().borrow()[&("model_x".to_string(), M.to_string())],
        generation + 1
    );
    assert_eq!(of_m(&mut relay), [], "nothing of M on the relay");
}

/// `M`'s watch sends a snapshot of events each under the event limit but
/// together past the snapshot's: the follow ends, with nothing of `M` on
/// the relay, and `M` is followed again after the refollow wait. The
/// snapshot's bound is longer than the test waits for the second watch,
/// so that only the limit can have ended the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_past_its_limit_ends_the_follow() {
    let Setup {
        mut server,
        record,
        global,
        _hubs,
        _instances,
        ..
    } = setup(CellTiming {
        watch: WatchLimits {
            synced_within: WAIT * 2,
            ..LIMITS
        },
        ..timing()
    })
    .await;
    let mut relay = global.follows();
    let mut first = server.next().await;
    let generation = first.generation();
    first.begin(&record).await;
    // The link ends no sooner than the snapshot passes its limit, which
    // it does only in what is sent from here.
    let sending = Instant::now();
    let mut sent = 0;
    while sent <= LIMITS.snapshot {
        let event = ready();
        sent += serde_json::to_string(&event).expect("serializes").len() + 8;
        first.send(&event).await;
    }
    // The watcher may have hung up already.
    let synced = format!(
        "data: {}\n\n",
        serde_json::to_string(&CellWatchEvent::Synced).expect("serializes")
    );
    let _ = first.stream.write_all(synced.as_bytes()).await;
    first.keep_alive();
    let second = server.next().await;
    assert!(
        second.at - sending >= timing().refollow / 2,
        "followed again after {:?}",
        second.at - sending
    );
    assert_eq!(second.generation(), generation + 1);
    assert_eq!(of_m(&mut relay), [], "nothing of M on the relay");
}

/// A real cell's hub with many entry instances sends its whole snapshot
/// well inside the default bounds, both its cell watch and the global's
/// relay of it: the bounds do not refuse a healthy hub.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_hubs_snapshot_completes_well_inside_its_bounds() {
    // Each instance is a velo node of its own; 64 of them take about a
    // second to join.
    const ENTRIES: usize = 64;
    let directory = Resolving::new();
    let (model_hub, _model) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (mut instances, _) = meet_the_quorum(&model_hub).await;
    for _ in 1..ENTRIES {
        let mut instance = Instance::join(&model_hub, "f").await;
        instance.announce(SegmentPhase::Ready).await;
        instances.push(instance);
    }
    let (global_hub, _global) = serve_cell(global_default(), &directory, timing()).await;
    let watcher = CellWatcher::new().expect("a client");
    let model = format!("127.0.0.1:{}", model_hub.public_addr().port());
    let relay = format!("127.0.0.1:{}", global_hub.public_addr().port());

    // The cell's watch, once it shows every entry instance.
    let (bytes, took) = whole(|| watcher.watch(&model), ENTRIES, WAIT).await;
    println!("a cell's snapshot of {ENTRIES} entry instances: {bytes} bytes in {took:?}");
    assert!(took < WatchLimits::SYNCED_WITHIN / 30, "{took:?}");
    assert!(bytes < WatchLimits::SNAPSHOT / 100, "{bytes} bytes");

    // The global's relay, once it shows them too.
    let (bytes, took) = whole(|| watcher.follows(&relay), ENTRIES, WAIT).await;
    println!("a relay's snapshot of {ENTRIES} entry instances: {bytes} bytes in {took:?}");
    assert!(took < WatchLimits::SYNCED_WITHIN / 30, "{took:?}");
    assert!(bytes < WatchLimits::SNAPSHOT / 100, "{bytes} bytes");
    drop(instances);
}

/// A complete snapshot that never shows every entry instance expected
/// fails the test within its deadline, rather than being watched again
/// and again: here a cell of one entry instance, expected to show two.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a snapshot of every entry instance in time")]
async fn a_snapshot_short_of_its_entries_fails_in_time() {
    let directory = Resolving::new();
    let (model_hub, _model) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    let watcher = CellWatcher::new().expect("a client");
    let model = format!("127.0.0.1:{}", model_hub.public_addr().port());
    whole(|| watcher.watch(&model), 2, Duration::from_millis(500)).await;
}

/// An event of a watch that may show an entry instance.
trait Shown: serde::Serialize {
    fn synced(&self) -> bool;
    fn entry(&self) -> bool;
}

impl Shown for CellWatchEvent {
    fn synced(&self) -> bool {
        matches!(self, Self::Synced)
    }

    fn entry(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

impl Shown for FollowEvent {
    fn synced(&self) -> bool {
        matches!(self, Self::Synced)
    }

    fn entry(&self) -> bool {
        matches!(self, Self::Exposed { .. })
    }
}

/// Watch with `open` until a complete snapshot shows `entries` entry
/// instances, the hub having perhaps not shown them all yet, within
/// `within` in all: that snapshot's bytes, and how long it took.
async fn whole<E, U, F, O>(open: F, entries: usize, within: Duration) -> (usize, Duration)
where
    E: Shown,
    F: Fn() -> O,
    O: std::future::Future<
            Output = Result<gdl_watch::WatchStream<E, U>, gdl_watch::CellWatchError>,
        >,
{
    let watched = async {
        loop {
            let start = Instant::now();
            let mut events = open().await.expect("the watch begins");
            let (mut bytes, mut shown) = (0, 0);
            loop {
                let event = events
                    .next()
                    .await
                    .expect("the watch goes on")
                    .expect("the watch does not fail")
                    .event()
                    .expect("a known event");
                bytes += serde_json::to_string(&event).expect("serializes").len() + 8;
                if event.synced() {
                    break;
                }
                shown += usize::from(event.entry());
            }
            if shown == entries {
                return (bytes, start.elapsed());
            }
        }
    };
    tokio::time::timeout(within, watched)
        .await
        .expect("a snapshot of every entry instance in time")
}

/// A follow whose snapshot completed is never ended by the snapshot's
/// bound: it outlives it several times over, in its generation, and
/// carries the cell's changes throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follow_whose_snapshot_completed_outlives_the_bound() {
    let directory = Resolving::new();
    let (model_hub, _model) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    let (_global_hub, global) = serve_cell(global_default(), &directory, bounded()).await;
    let key = ("model_x".to_string(), CELL.to_string());
    let mut relay = global.follows();
    let generation = loop {
        let event = tokio::time::timeout(WAIT, relay.next())
            .await
            .expect("an event in time")
            .expect("the relay goes on");
        if let FollowEvent::Follow { generation, .. } = event {
            break generation;
        }
    };
    // Outliving the bound is the point: there is nothing to observe but
    // that nothing happens.
    tokio::time::sleep(LIMITS.synced_within * 4).await;
    let mut spare = Instance::join(&model_hub, "f").await;
    spare.announce(SegmentPhase::Ready).await;
    loop {
        let event = tokio::time::timeout(WAIT, relay.next())
            .await
            .expect("an event in time")
            .expect("the relay goes on");
        assert!(
            !matches!(
                event,
                FollowEvent::Dropped { .. } | FollowEvent::Follow { .. }
            ),
            "{event:?}"
        );
        if let FollowEvent::Exposed {
            generation: of,
            peer,
            ..
        } = event
            && peer.instance_id() == spare.id()
        {
            assert_eq!(of, generation);
            break;
        }
    }
    assert_eq!(global.following().borrow()[&key], generation);
}
