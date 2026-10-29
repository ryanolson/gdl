//! The bounds a watch's reader keeps on its snapshot: its time from when
//! the watch was asked for, whatever bytes come meanwhile, and its bytes.
//! The clock is paused, so that a bound of seconds passes at once.

use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::time::Instant;
use velo::{InstanceId, PeerInfo, WorkerAddress};

use super::{PoolWatcher, events};
use crate::{PoolWatchEvent, UnknownEvent, WatchFailure, WatchItem, WatchLimits, Watched};

const WITHIN: Duration = Duration::from_secs(1);

fn limits() -> WatchLimits {
    WatchLimits {
        event: 1024,
        snapshot: 4096,
        synced_within: WITHIN,
    }
}

fn sent(event: &PoolWatchEvent) -> Vec<u8> {
    format!(
        "data: {}\n\n",
        serde_json::to_string(event).expect("an event serializes")
    )
    .into_bytes()
}

fn pool() -> PoolWatchEvent {
    PoolWatchEvent::Pool {
        properties: None,
        name: "m".into(),
        class: "model_x".into(),
        epoch: 1,
        state: gdl_pools::PoolState::Ready,
        load: None,
    }
}

fn ready() -> PoolWatchEvent {
    PoolWatchEvent::Ready {
        segment: "f".into(),
        peer: PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(b"tcp://127.0.0.1:1".to_vec()),
        ),
    }
}

/// `first`, then forever a keep-alive every 100 ms and, every third, the
/// next of `then`, while it has one.
fn trickle(
    first: Vec<Vec<u8>>,
    then: Vec<Vec<u8>>,
) -> impl Stream<Item = Result<Vec<u8>, String>> + Send + 'static {
    let first = futures::stream::iter(first.into_iter().map(Ok));
    let then = futures::stream::unfold((0usize, then.into_iter()), |(n, mut then)| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let chunk = match n % 3 {
            2 => then.next().unwrap_or_else(|| b":\n\n".to_vec()),
            _ => b":\n\n".to_vec(),
        };
        Some((Ok(chunk), (n + 1, then)))
    });
    first.chain(then)
}

/// The items of `bytes` within `limits`, from now, and when the watch
/// ended; a watch that has not ended after `wait` fails the test.
async fn read(
    bytes: impl Stream<Item = Result<Vec<u8>, String>> + Send + 'static,
    limits: WatchLimits,
    wait: Duration,
) -> (Vec<WatchItem<PoolWatchEvent, UnknownEvent>>, Duration) {
    let start = Instant::now();
    let items = tokio::time::timeout(
        wait,
        events(bytes, Arc::default(), limits, start + limits.synced_within).collect(),
    )
    .await
    .expect("the watch ends in time");
    (items, start.elapsed())
}

/// A peer that sends a snapshot's first event and then only keep-alives,
/// and now and then an event of the snapshot, never completes it: the
/// watch ends at the bound, whatever came meanwhile.
#[tokio::test(start_paused = true)]
async fn a_snapshot_not_complete_by_the_bound_ends_the_watch() {
    let readies = vec![ready(), ready()];
    let bytes = trickle(vec![sent(&pool())], readies.iter().map(sent).collect());
    let (items, ended) = read(bytes, limits(), WITHIN * 10).await;
    let mut expected: Vec<WatchItem<PoolWatchEvent, UnknownEvent>> =
        vec![Ok(Watched::Event(pool()))];
    expected.extend(readies.into_iter().map(|ready| Ok(Watched::Event(ready))));
    expected.push(Err(WatchFailure::SnapshotLate { within: WITHIN }));
    assert_eq!(items, expected);
    assert!(
        ended >= WITHIN && ended < WITHIN + Duration::from_millis(200),
        "ended after {ended:?}"
    );
}

/// A snapshot whose events are each under the event limit but together
/// past the snapshot's ends the watch at the event that passes it, before
/// its `synced`.
#[tokio::test]
async fn a_snapshot_past_its_limit_ends_the_watch() {
    let limits = limits();
    let mut chunks = vec![sent(&pool())];
    let one = sent(&ready()).len();
    assert!(one < limits.event);
    chunks.extend((0..2 * limits.snapshot / one).map(|_| sent(&ready())));
    chunks.push(sent(&PoolWatchEvent::Synced));
    let bytes = futures::stream::iter(chunks.into_iter().map(Ok));
    let (items, _) = read(bytes, limits, WITHIN).await;
    let (last, read) = items.split_last().expect("items");
    assert_eq!(
        last,
        &Err(WatchFailure::SnapshotTooLarge {
            limit: limits.snapshot
        })
    );
    assert!(read.iter().all(Result::is_ok));
    assert!(
        !read.contains(&Ok(Watched::Event(PoolWatchEvent::Synced))),
        "the snapshot completed"
    );
    let bytes = sent(&pool()).len() + (read.len() - 1) * one;
    assert!(bytes <= limits.snapshot, "{bytes} bytes read");
    assert!(bytes + one > limits.snapshot, "{bytes} bytes read");
}

/// A watch whose snapshot is complete is not ended by the bound, however
/// long it lives, nor by the snapshot's limit, however much it sends.
#[tokio::test(start_paused = true)]
async fn a_complete_snapshot_is_never_ended_by_its_bounds() {
    let limits = limits();
    let after: Vec<PoolWatchEvent> = (0..limits.snapshot / sent(&ready()).len() * 2)
        .map(|_| ready())
        .collect();
    let bytes = trickle(
        vec![sent(&pool()), sent(&PoolWatchEvent::Synced)],
        after.iter().map(sent).collect(),
    )
    .take(after.len() * 3 + 10);
    let (items, ended) = read(bytes, limits, WITHIN * 1000).await;
    assert!(ended > WITHIN * 10, "lived {ended:?}");
    let mut expected: Vec<WatchItem<PoolWatchEvent, UnknownEvent>> = vec![
        Ok(Watched::Event(pool())),
        Ok(Watched::Event(PoolWatchEvent::Synced)),
    ];
    expected.extend(after.into_iter().map(|ready| Ok(Watched::Event(ready))));
    assert_eq!(items, expected);
}

/// A peer that takes the connection and never answers is bounded by the
/// same bound: the watch does not begin.
#[tokio::test]
async fn a_hub_that_never_answers_is_bounded_too() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let address = listener.local_addr().expect("an address").to_string();
    let within = Duration::from_millis(200);
    let watcher = PoolWatcher::with_limits(WatchLimits {
        synced_within: within,
        ..WatchLimits::default()
    })
    .expect("a client");
    let start = Instant::now();
    let Err(error) = tokio::time::timeout(Duration::from_secs(5), watcher.watch(&address))
        .await
        .expect("the watch is refused in time")
    else {
        panic!("a watch began");
    };
    assert!(start.elapsed() >= within, "{:?}", start.elapsed());
    assert!(
        error.to_string().contains("not complete within 200 ms"),
        "{error}"
    );
    drop(listener);
}

/// An event that arrived in time but is read after the bound, as a slow
/// reader reads one it has buffered, is read only once the bound has
/// passed: the watch ends there, and a `synced` buffered with the
/// snapshot's first event does not complete it.
#[tokio::test(start_paused = true)]
async fn a_buffered_event_read_after_the_bound_ends_the_watch() {
    for then in [PoolWatchEvent::Synced, ready()] {
        let chunk = [sent(&pool()), sent(&then)].concat();
        let bytes =
            futures::stream::iter([Ok::<_, String>(chunk)]).chain(futures::stream::pending());
        let start = Instant::now();
        let mut items = std::pin::pin!(events::<PoolWatchEvent, _, _, _>(
            bytes,
            Arc::default(),
            limits(),
            start + WITHIN,
        ));
        assert_eq!(items.next().await, Some(Ok(Watched::Event(pool()))));
        tokio::time::advance(WITHIN * 2).await;
        assert_eq!(
            items.next().await,
            Some(Err(WatchFailure::SnapshotLate { within: WITHIN })),
            "{then:?} read after the bound"
        );
        assert_eq!(items.next().await, None);
    }
}

/// The bytes of an event not yet ended count toward the snapshot's limit:
/// complete events and an unfinished one past it end the watch at once,
/// not at the bound.
#[tokio::test(start_paused = true)]
async fn an_unfinished_event_counts_toward_the_snapshot() {
    let limits = limits();
    let mut chunk = sent(&pool());
    let one = sent(&ready()).len();
    while chunk.len() + one <= limits.snapshot {
        chunk.extend(sent(&ready()));
    }
    let tail = limits.snapshot + 1 - chunk.len();
    assert!(tail < limits.event, "{tail}");
    chunk.extend(b"data: ");
    chunk.resize(limits.snapshot + 1, b'x');
    let bytes = futures::stream::iter([Ok::<_, String>(chunk)]).chain(futures::stream::pending());
    let (items, ended) = read(bytes, limits, WITHIN * 10).await;
    assert_eq!(
        items.last(),
        Some(&Err(WatchFailure::SnapshotTooLarge {
            limit: limits.snapshot
        }))
    );
    assert!(ended < WITHIN, "ended after {ended:?}");
}
