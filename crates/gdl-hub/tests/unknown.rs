//! A global's hub that follows a model cell whose watch sends an event of
//! a kind it does not know withholds that cell alone, shown `unavailable`
//! on the relay, until it has read the cell again from a second watch of
//! it; the follow stays open throughout, in its generation. The cell `M`
//! is served by a test server that speaks the cell watch's protocol, so
//! that the test chooses each event it sends and when it answers the
//! second watch; `N` is a cell's hub, beside it.

mod common;

use common::{
    CELL, Instance, Records, Resolving, WAIT, global_default, global_requiring, health,
    meet_the_quorum, model, serve_cell, timing,
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
                    let (path, query) = match path.split_once('?') {
                        Some((path, query)) => (path.to_string(), query.to_string()),
                        None => (path, String::new()),
                    };
                    let _ = accept.send(Watch {
                        stream,
                        path,
                        query,
                    });
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
    /// The watch's query: the follow it names, for a hub that follows the
    /// cell.
    query: String,
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
                acknowledged: false,
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

/// Whether `event` is one of what the relay shows of the cell `of`: its
/// follow, state, load, and instances. Whether the follow is offered, which
/// a withheld cell is not, is shown apart and asserted apart
/// ([`a_withheld_follow_is_not_offered_and_never_closes_while_read_again`]).
fn concerns(event: &FollowEvent, of: &str) -> bool {
    match event {
        FollowEvent::Follow { cell, .. }
        | FollowEvent::State { cell, .. }
        | FollowEvent::Load { cell, .. }
        | FollowEvent::Exposed { cell, .. }
        | FollowEvent::Retired { cell, .. }
        | FollowEvent::Left { cell, .. }
        | FollowEvent::Dropped { cell, .. }
        | FollowEvent::Withdrawn { cell, .. } => cell == of,
        FollowEvent::Offering { .. } | FollowEvent::Synced => false,
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
/// would. The hub reads `M` again from a second watch, which replaces the
/// first: nothing the first sends after the event is applied. Once the
/// second's snapshot is complete, the relay shows `M` as it has it, a
/// retirement that came only in the reading included, and the second
/// watch carries the cell from then. The follow keeps its generation: no
/// `dropped`.
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
    // A chooser that never acknowledges, so that no follow the hub stops
    // offering closes.
    let _chooser = Instance::chooser(&global_hub).await;
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
    // Watched alone over its route, the follow is withheld too.
    let watcher = gdl_watch::CellWatcher::new().expect("a client");
    let relay = format!("127.0.0.1:{}", global_hub.public_addr().port());
    let mut alone = tokio::time::timeout(WAIT, watcher.follows_of(&relay, "model_x", M))
        .await
        .expect("the hub answers in time")
        .expect("the hub serves the follow");
    let mut snapshot = Vec::new();
    loop {
        let item = tokio::time::timeout(WAIT, alone.next())
            .await
            .expect("an event in time")
            .expect("the watch goes on")
            .expect("the watch does not fail");
        match item.event().expect("a known event") {
            FollowEvent::Synced => break,
            event => snapshot.push(event),
        }
    }
    assert_eq!(
        snapshot,
        [
            FollowEvent::Follow {
                class: "model_x".into(),
                cell: M.into(),
                generation,
                region: like.region.clone(),
                state: CellState::Unavailable,
                load: None,
            },
            FollowEvent::Offering {
                class: "model_x".into(),
                cell: M.into(),
                generation,
                revision: 2,
                offered: false,
            }
        ]
    );
    let mut none = tokio::time::timeout(
        WAIT,
        watcher.follows_of(&relay, "model_x", "model-x-99-us-east-1"),
    )
    .await
    .expect("the hub answers in time")
    .expect("the hub serves the follow");
    assert_eq!(
        tokio::time::timeout(WAIT, none.next())
            .await
            .expect("an event in time")
            .and_then(|item| item.expect("the watch does not fail").event()),
        Some(FollowEvent::Synced),
        "a cell the hub does not follow"
    );
    let mut second = server.next().await;
    // The watch that replaces the first names the same follow, so that
    // the model cell's hub counts the follower on through the
    // replacement, rather than one watch closed without a release.
    assert!(
        first.query.starts_with("class=global&cell="),
        "{}",
        first.query
    );
    assert_eq!(second.query, first.query, "the reading names its follow");
    // While the reading waits, nothing more of the first watch is read,
    // and `N`'s events apply.
    first
        .offer(
            &serde_json::to_string(&CellWatchEvent::State {
                state: CellState::Degraded,
                unfollowed: false,
            })
            .expect("an event serializes"),
        )
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
    let mut read = Vec::new();
    for _ in 0..3 {
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
        ]
    );
    met(&global, 2).await;
    // The second watch carries the cell now.
    second
        .send(&CellWatchEvent::State {
            state: CellState::Degraded,
            unfollowed: false,
        })
        .await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        state(generation, CellState::Degraded)
    );
    met(&global, 1).await;
    // The watch of M alone had the same changes, and none of N's.
    let mut alone_seen = Vec::new();
    while alone_seen.last() != Some(&state(generation, CellState::Degraded)) {
        let item = tokio::time::timeout(WAIT, alone.next())
            .await
            .expect("an event in time")
            .expect("the watch goes on")
            .expect("the watch does not fail");
        let event = item.event().expect("a known event");
        if concerns(&event, M) {
            alone_seen.push(event);
        }
    }
    assert_eq!(
        alone_seen,
        read.iter()
            .cloned()
            .chain([state(generation, CellState::Degraded)])
            .collect::<Vec<_>>()
    );
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
/// and the hub reads it again, from a watch that replaces the first.
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
    let (global_hub, global) = serve_cell(global_default(), &directory, timing()).await;
    // A chooser that never acknowledges, so that no follow the hub stops
    // offering closes.
    let _chooser = Instance::chooser(&global_hub).await;
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
    // The second watch carries the cell, and nothing came of M before
    // what it sends.
    drop(first);
    second
        .send(&CellWatchEvent::State {
            state: CellState::Degraded,
            unfollowed: false,
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

impl Watch {
    /// Send `data` if the watcher still reads this watch.
    async fn offer(&mut self, data: &str) {
        let _ = self
            .stream
            .write_all(format!("data: {data}\n\n").as_bytes())
            .await;
    }
}

fn load_of(admitted: u64) -> String {
    serde_json::to_string(&CellWatchEvent::Load {
        load: Some(gdl_cells::LoadSummary {
            admitted,
            limit: 8,
            instances: 1,
            refusals: 0,
            period_ms: 1000,
        }),
    })
    .expect("an event serializes")
}

/// The hubs of these tests: a model cell's, beside the server's `M`, so
/// that the global's hub has a record of the deployment to copy for `M`;
/// the global's; and `M`'s server, whose record the global follows.
struct Setup {
    model_hub: velo_hub::Hub,
    global_hub: velo_hub::Hub,
    global: std::sync::Arc<gdl_hub::CellReadiness<Resolving>>,
    server: Server,
    like: CellRecord,
    _quorum: Vec<Instance>,
    /// A chooser, which acknowledges only where a test says, so that no
    /// follow the hub stops offering closes otherwise.
    chooser: Instance,
}

impl Setup {
    async fn new() -> Self {
        let directory = Resolving::new();
        let mut records = Records::new(&directory.inner);
        let (model_hub, _) = serve_cell(
            model(CELL, "us-east-1", "model-x-01-hub:1337"),
            &directory,
            timing(),
        )
        .await;
        let (quorum, _) = meet_the_quorum(&model_hub).await;
        let (like, _) = records.until(CellState::Ready).await;
        let (global_hub, global) = serve_cell(global_default(), &directory, timing()).await;
        let chooser = Instance::chooser(&global_hub).await;
        let server = Server::new().await;
        directory
            .inner
            .publish(record_of(&like, &server))
            .await
            .expect("stored");
        Self {
            model_hub,
            global_hub,
            global,
            server,
            like,
            _quorum: quorum,
            chooser,
        }
    }

    async fn stop(self) {
        self.global_hub.shutdown().await.expect("the hub stops");
        self.model_hub.shutdown().await.expect("the hub stops");
    }
}

/// `M` followed with `a` exposed, and the relay's watch after it.
async fn exposing(
    setup: &mut Setup,
    a: &PeerInfo,
) -> (Watch, gdl_hub::FollowsWatch, Vec<FollowEvent>) {
    let mut first = setup.server.next().await;
    first
        .snapshot(&setup.like, CellState::Ready, &[], &[a])
        .await;
    let mut watch = setup.global.follows();
    let mut others = Vec::new();
    loop {
        if matches!(
            next_of(&mut watch, M, &mut others).await,
            FollowEvent::Exposed { .. }
        ) {
            break;
        }
    }
    (first, watch, others)
}

/// The events of the relay's watch now that concern `M`.
fn pending_of(watch: &mut gdl_hub::FollowsWatch) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    while let Some(Some(event)) = futures::FutureExt::now_or_never(watch.next()) {
        if concerns(&event, M) {
            events.push(event);
        }
    }
    events
}

/// What the replaced watch sends after the unknown event is never applied
/// over the reading: `M`'s reading says it is draining, and nothing the
/// first watch sent meanwhile, a state that exposes and a new instance,
/// brings it back. The reading's watch carries the cell after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_the_replaced_watch_sends_is_not_applied_over_the_reading() {
    let mut setup = Setup::new().await;
    let (a, b) = (peer(), peer());
    let (mut first, mut watch, mut others) = exposing(&mut setup, &a).await;
    first.raw(r#"{"event":"paused"}"#).await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        state(1, CellState::Unavailable)
    );
    let mut second = setup.server.next().await;
    let degraded = serde_json::to_string(&CellWatchEvent::State {
        state: CellState::Degraded,
        unfollowed: false,
    })
    .expect("an event serializes");
    let ready = serde_json::to_string(&CellWatchEvent::Ready {
        segment: "f".into(),
        peer: b.clone(),
    })
    .expect("an event serializes");
    first.offer(&degraded).await;
    first.offer(&ready).await;
    second
        .snapshot(&setup.like, CellState::Draining, &[], &[&a])
        .await;
    first.offer(&load_of(1)).await;
    second.raw(&load_of(2)).await;
    let mut read = Vec::new();
    loop {
        let event = next_of(&mut watch, M, &mut others).await;
        let load = matches!(event, FollowEvent::Load { .. });
        read.push(event);
        if load {
            break;
        }
    }
    assert_eq!(
        read,
        [
            state(1, CellState::Draining),
            FollowEvent::Load {
                class: "model_x".into(),
                cell: M.into(),
                generation: 1,
                load: Some(gdl_cells::LoadSummary {
                    admitted: 2,
                    limit: 8,
                    instances: 1,
                    refusals: 0,
                    period_ms: 1000,
                }),
            },
        ]
    );
    setup.stop().await;
}

/// An event of a kind the hub does not know in the reading's own snapshot
/// keeps the cell withheld: nothing of it is shown, not even for a moment,
/// until a later reading's snapshot is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_event_in_the_reading_keeps_the_cell_withheld() {
    let mut setup = Setup::new().await;
    let a = peer();
    let (mut first, mut watch, mut others) = exposing(&mut setup, &a).await;
    first.raw(r#"{"event":"paused"}"#).await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        state(1, CellState::Unavailable)
    );
    let mut second = setup.server.next().await;
    second.answer().await;
    second
        .send(&CellWatchEvent::Cell {
            name: M.into(),
            class: "model_x".into(),
            epoch: setup.like.epoch,
            state: CellState::Ready,
            load: None,
        })
        .await;
    second.raw(r#"{"event":"paused"}"#).await;
    second
        .send(&CellWatchEvent::Ready {
            segment: "f".into(),
            peer: a.clone(),
        })
        .await;
    second.send(&CellWatchEvent::Synced).await;
    // The hub asks for a third reading only once it has read the second.
    let mut third = setup.server.next().await;
    assert_eq!(pending_of(&mut watch), [], "nothing of M shown");
    third
        .snapshot(&setup.like, CellState::Ready, &[], &[&a])
        .await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        state(1, CellState::Ready)
    );
    assert_eq!(next_of(&mut watch, M, &mut others).await, exposed(1, &a));
    assert_eq!(setup.global.unknown_events(), 2);
    setup.stop().await;
}

/// One that the reading's watch sends after its snapshot withholds the
/// cell again at once, and the cell is read again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_event_after_the_reading_withholds_the_cell_again() {
    let mut setup = Setup::new().await;
    let a = peer();
    let (mut first, mut watch, mut others) = exposing(&mut setup, &a).await;
    first.raw(r#"{"event":"paused"}"#).await;
    let _ = next_of(&mut watch, M, &mut others).await;
    let mut second = setup.server.next().await;
    second
        .snapshot(&setup.like, CellState::Ready, &[], &[&a])
        .await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        state(1, CellState::Ready)
    );
    assert_eq!(next_of(&mut watch, M, &mut others).await, exposed(1, &a));
    second.raw(r#"{"event":"paused"}"#).await;
    assert_eq!(
        next_of(&mut watch, M, &mut others).await,
        state(1, CellState::Unavailable)
    );
    let _third = setup.server.next().await;
    assert_eq!(
        setup.global.following().borrow()[&("model_x".to_string(), M.to_string())],
        1,
        "the same follow"
    );
    setup.stop().await;
}

/// Every event of the relay's watch that names `M`, its offering included,
/// up to and including the first that `last` takes.
async fn through_of(
    watch: &mut gdl_hub::FollowsWatch,
    last: impl Fn(&FollowEvent) -> bool,
) -> Vec<FollowEvent> {
    let mut events = Vec::new();
    loop {
        let event = tokio::time::timeout(WAIT, watch.next())
            .await
            .unwrap_or_else(|_| panic!("an event of {M} in time; seen: {events:?}"))
            .expect("the relay's watch goes on");
        if event.cell().is_some_and(|(_, cell)| cell == M) {
            let done = last(&event);
            events.push(event);
            if done {
                return events;
            }
        }
    }
}

/// A withheld cell is not offered, at the follow's next revision, so its
/// chooser stops offering to it and acknowledges; but the follow does not
/// close while its link reads the cell again, however every chooser has
/// acknowledged, and once the cell is read again and takes work it is
/// offered again, in the same generation, never dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withheld_follow_is_not_offered_and_never_closes_while_read_again() {
    let mut setup = Setup::new().await;
    let a = peer();
    let (mut first, mut watch, _) = exposing(&mut setup, &a).await;
    first.raw(r#"{"event":"paused"}"#).await;
    let marked = through_of(&mut watch, |event| {
        matches!(event, FollowEvent::Offering { .. })
    })
    .await;
    assert_eq!(
        marked.last(),
        Some(&FollowEvent::Offering {
            class: "model_x".into(),
            cell: M.into(),
            generation: 1,
            revision: 2,
            offered: false,
        })
    );
    setup
        .chooser
        .acknowledge(&gdl_watch::Unoffered {
            class: "model_x".into(),
            cell: M.into(),
            generation: 1,
            revision: 2,
        })
        .await
        .expect("the hub takes the acknowledgement");
    let mut second = setup.server.next().await;
    second
        .snapshot(&setup.like, CellState::Ready, &[], &[&a])
        .await;
    let read = through_of(&mut watch, |event| {
        matches!(event, FollowEvent::Offering { offered: true, .. })
    })
    .await;
    assert!(
        !read
            .iter()
            .any(|event| matches!(event, FollowEvent::Dropped { .. })),
        "a withheld follow closed while its link read it again: {read:?}"
    );
    assert_eq!(
        read.last(),
        Some(&FollowEvent::Offering {
            class: "model_x".into(),
            cell: M.into(),
            generation: 1,
            revision: 3,
            offered: true,
        })
    );
    assert_eq!(
        setup.global.following().borrow()[&("model_x".to_string(), M.to_string())],
        1
    );
    setup.stop().await;
}
