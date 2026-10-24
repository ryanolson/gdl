//! A global's hub that follows a model cell whose watch sends an event of
//! a kind it does not know withholds that cell alone, shown `unavailable`
//! on the relay, until it has read the cell again from a second watch of
//! it; the follow stays open throughout, in its generation. The cell `M`
//! is served by a test server that speaks the cell watch's protocol, so
//! that the test chooses each event it sends and when it answers the
//! second watch; `N` is a cell's hub, beside it.

mod common;

use common::{
    CELL, Instance, Records, Resolving, WAIT, global_requiring, health, meet_the_quorum, model,
    serve_cell, timing,
};
use futures::{Stream, StreamExt};
use gdl_cells::{CellDirectory, CellRecord, CellState};
use gdl_model::SegmentPhase;
use gdl_watch::{CellWatchEvent, FollowEvent};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

const M: &str = "model-x-07-us-east-1";

/// A server that answers every request as a cell's watch, and hands each
/// connection to the test, which sends its events.
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
                    // The request's head, then a response whose body runs
                    // until the test drops the connection.
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                            return;
                        }
                        head.push(byte[0]);
                    }
                    let path = String::from_utf8_lossy(&head)
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();
                    let _ = accept.send(Watch { stream, path });
                });
            }
        });
        Self { address, accepted }
    }

    /// The next connection, as it asks for a watch: unanswered until the
    /// test sends on it.
    async fn next(&mut self) -> Watch {
        let watch = tokio::time::timeout(WAIT, self.accepted.recv())
            .await
            .expect("a watch in time")
            .expect("the server runs");
        assert_eq!(watch.path, "/v1/ext/gdl-cell/watch");
        watch
    }

    /// No connection is waiting.
    fn quiet(&mut self) -> bool {
        self.accepted.try_recv().is_err()
    }
}

/// One watch of the server.
struct Watch {
    stream: TcpStream,
    path: String,
}

impl Watch {
    async fn answer(&mut self) {
        let head =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
        self.stream
            .write_all(head.as_bytes())
            .await
            .expect("the watcher reads");
    }

    async fn raw(&mut self, data: &str) {
        self.stream
            .write_all(format!("data: {data}\n\n").as_bytes())
            .await
            .expect("the watcher reads");
    }

    async fn send(&mut self, event: &CellWatchEvent) {
        self.raw(&serde_json::to_string(event).expect("an event serializes"))
            .await;
    }

    /// Answer with the snapshot of the cell of `record`: its state, the
    /// retired instances, the ready ones, then the marker.
    async fn snapshot(
        &mut self,
        record: &CellRecord,
        state: CellState,
        retired: &[InstanceId],
        ready: &[&PeerInfo],
    ) {
        self.answer().await;
        self.send(&CellWatchEvent::Cell {
            name: M.into(),
            class: "model_x".into(),
            epoch: record.epoch,
            state,
            load: None,
        })
        .await;
        for instance in retired {
            self.send(&CellWatchEvent::Retired {
                segment: "f".into(),
                instance: *instance,
            })
            .await;
        }
        for peer in ready {
            self.send(&CellWatchEvent::Ready {
                segment: "f".into(),
                peer: (*peer).clone(),
            })
            .await;
        }
        self.send(&CellWatchEvent::Synced).await;
    }
}

fn peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"tcp://test".to_vec()),
    )
}

/// The record of `M`, served by the server, as the directory names it.
fn record_of(like: &CellRecord, server: &Server) -> CellRecord {
    CellRecord {
        name: M.into(),
        state: CellState::Ready,
        hub: server.address.clone(),
        ..like.clone()
    }
}

/// The next event of the relay that concerns `cell`, the others gathered
/// in `others`.
async fn next_of(
    watch: &mut (impl Stream<Item = FollowEvent> + Unpin),
    cell: &str,
    others: &mut Vec<FollowEvent>,
) -> FollowEvent {
    loop {
        let event = tokio::time::timeout(WAIT, watch.next())
            .await
            .unwrap_or_else(|_| panic!("an event of {cell} in time; others: {others:?}"))
            .expect("the relay's watch goes on");
        if concerns(&event, cell) {
            return event;
        }
        others.push(event);
    }
}

fn concerns(event: &FollowEvent, of: &str) -> bool {
    match event {
        FollowEvent::Follow { cell, .. }
        | FollowEvent::State { cell, .. }
        | FollowEvent::Load { cell, .. }
        | FollowEvent::Exposed { cell, .. }
        | FollowEvent::Retired { cell, .. }
        | FollowEvent::Left { cell, .. }
        | FollowEvent::Dropped { cell, .. } => cell == of,
        FollowEvent::Synced => false,
    }
}

fn state(generation: u64, state: CellState) -> FollowEvent {
    FollowEvent::State {
        class: "model_x".into(),
        cell: M.into(),
        generation,
        state,
    }
}

fn exposed(generation: u64, peer: &PeerInfo) -> FollowEvent {
    FollowEvent::Exposed {
        class: "model_x".into(),
        cell: M.into(),
        generation,
        segment: "f".into(),
        peer: peer.clone(),
    }
}

/// How many ready cells of `model_x` the global's hub counts, once it is
/// `met`.
async fn met(global: &gdl_hub::CellReadiness<Resolving>, met: u32) {
    health(global, &format!("{met} ready model cells"), |health| {
        health.requires["model_x"].met == met
    })
    .await;
}

/// `M`'s watch sends an event of a kind the global's hub does not know:
/// at once the relay shows `M` `unavailable`, its instances hidden, and
/// the requirement counts it no more, while `N`'s events go on as they
/// would. The hub reads `M` again from a second watch, holding what the
/// first sends meanwhile; once it has, the relay shows `M` as the reading
/// has it, a retirement that came only in the reading included, then
/// applies what it held. The first watch stays open, and the follow keeps
/// its generation: no `dropped`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_followed_cell_is_withheld_for_an_unknown_event_until_it_is_read_again() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, _model_cell) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (_quorum, _) = meet_the_quorum(&model_hub).await;
    let mut spare = Instance::join(&model_hub, "f").await;
    spare.announce(SegmentPhase::Ready).await;
    let (like, _) = records.until(CellState::Ready).await;
    let (global_hub, global) = serve_cell(global_requiring(2), &directory, timing()).await;
    let mut server = Server::new().await;
    directory
        .inner
        .publish(record_of(&like, &server))
        .await
        .expect("stored");
    let mut first = server.next().await;
    let (a, b) = (peer(), peer());
    first.snapshot(&like, CellState::Ready, &[], &[&a]).await;
    met(&global, 2).await;
    let generation = global.following().borrow()[&("model_x".to_string(), M.to_string())];
    let mut watch = global.follows();
    let mut others = Vec::new();
    loop {
        if next_of(&mut watch, M, &mut others).await == exposed(generation, &a) {
            break;
        }
    }

    first.raw(r#"{"event":"paused","for_ms":50}"#).await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        state(generation, CellState::Unavailable)
    );
    met(&global, 1).await;
    // Read alone over its route, the follow is withheld too.
    let watcher = gdl_watch::CellWatcher::new().expect("a client");
    let relay = format!("127.0.0.1:{}", global_hub.public_addr().port());
    assert_eq!(
        watcher
            .followed(&relay, "model_x", M)
            .await
            .expect("the hub serves the follow"),
        [FollowEvent::Follow {
            class: "model_x".into(),
            cell: M.into(),
            generation,
            region: like.region.clone(),
            state: CellState::Unavailable,
            load: None,
        }]
    );
    assert_eq!(
        watcher
            .followed(&relay, "model_x", "model-x-99-us-east-1")
            .await
            .expect("the hub answers"),
        [],
        "a cell the hub does not follow"
    );
    let mut second = server.next().await;
    // While the reading waits, the first watch's events are held, and
    // `N`'s apply.
    first
        .send(&CellWatchEvent::State {
            state: CellState::Degraded,
        })
        .await;
    others.clear();
    spare.announce(SegmentPhase::Retired).await;
    let retired = loop {
        let event = next_of(&mut watch, CELL, &mut others).await;
        if matches!(event, FollowEvent::Retired { .. }) {
            break event;
        }
    };
    assert_eq!(
        retired,
        FollowEvent::Retired {
            class: "model_x".into(),
            cell: CELL.into(),
            generation: global.following().borrow()[&("model_x".to_string(), CELL.to_string())],
            segment: "f".into(),
            instance: spare.id(),
        }
    );
    assert!(
        !others.iter().any(|event| concerns(event, M)),
        "nothing of M while it is read: {others:?}"
    );

    second
        .snapshot(&like, CellState::Ready, &[a.instance_id()], &[&b])
        .await;
    drop(second);
    let mut read = Vec::new();
    for _ in 0..4 {
        read.push(next_of(&mut watch, M, &mut others).await);
    }
    assert_eq!(
        read,
        [
            FollowEvent::Retired {
                class: "model_x".into(),
                cell: M.into(),
                generation,
                segment: "f".into(),
                instance: a.instance_id(),
            },
            state(generation, CellState::Ready),
            exposed(generation, &b),
            state(generation, CellState::Degraded),
        ]
    );
    // The first watch still carries the cell.
    first
        .send(&CellWatchEvent::State {
            state: CellState::Ready,
        })
        .await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        state(generation, CellState::Ready)
    );
    met(&global, 2).await;
    assert_eq!(
        global.following().borrow()[&("model_x".to_string(), M.to_string())],
        generation,
        "the same follow"
    );
    assert!(server.quiet(), "no other watch of M");
    assert_eq!(global.unknown_events(), 1);
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}

/// An event of a kind the hub does not know in the snapshot that begins
/// `M`'s watch installs `M` withheld, `unavailable` with nothing exposed,
/// and the hub reads it again at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_event_in_the_first_snapshot_installs_the_cell_withheld() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let (model_hub, _model_cell) = serve_cell(
        model(CELL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        timing(),
    )
    .await;
    let (_quorum, _) = meet_the_quorum(&model_hub).await;
    let (like, _) = records.until(CellState::Ready).await;
    let (global_hub, global) = serve_cell(global_requiring(0), &directory, timing()).await;
    let mut watch = global.follows();
    let mut server = Server::new().await;
    directory
        .inner
        .publish(record_of(&like, &server))
        .await
        .expect("stored");
    let mut first = server.next().await;
    let a = peer();
    first.answer().await;
    first
        .send(&CellWatchEvent::Cell {
            name: M.into(),
            class: "model_x".into(),
            epoch: like.epoch,
            state: CellState::Ready,
            load: None,
        })
        .await;
    first.raw(r#"{"event":"paused"}"#).await;
    first
        .send(&CellWatchEvent::Ready {
            segment: "f".into(),
            peer: a.clone(),
        })
        .await;
    first.send(&CellWatchEvent::Synced).await;
    let mut others = Vec::new();
    let installed = next_of(&mut watch, M, &mut others).await;
    let FollowEvent::Follow {
        generation, state, ..
    } = installed
    else {
        panic!("{installed:?}");
    };
    assert_eq!(state, CellState::Unavailable);
    let mut second = server.next().await;
    second.snapshot(&like, CellState::Ready, &[], &[&a]).await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        self::state(generation, CellState::Ready)
    );
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        exposed(generation, &a)
    );
    // The first watch goes on, and nothing came of M before what it sends.
    first
        .send(&CellWatchEvent::State {
            state: CellState::Degraded,
        })
        .await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        self::state(generation, CellState::Degraded)
    );
    assert!(server.quiet(), "no other watch of M");
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}
