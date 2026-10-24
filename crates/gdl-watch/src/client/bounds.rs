//! The limits a watch's reader keeps: on one event's bytes, however many
//! chunks they come in, and on its snapshot's bytes and time.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;
use velo::{InstanceId, PeerInfo, WorkerAddress};

use super::events;
use super::tests::{read_within, runtime};
use crate::{CellWatchEvent, UnknownEvent, WatchFailure, WatchItem, WatchLimits, Watched};

/// A small limit, so that a test sends a few kilobytes, not megabytes.
const LIMIT: usize = 1024;

fn limits() -> WatchLimits {
    WatchLimits {
        event: LIMIT,
        ..WatchLimits::default()
    }
}

/// A `left` event whose bytes, as sent, are `len` long.
fn left_of_len(len: usize) -> (CellWatchEvent, String) {
    let instance = InstanceId::new_v4();
    let event = |segment: String| CellWatchEvent::Left { segment, instance };
    let sent = |event: &CellWatchEvent| {
        format!(
            "data: {}\n\n",
            serde_json::to_string(event).expect("an event serializes")
        )
    };
    let bare = sent(&event(String::new())).len();
    let event = event("s".repeat(len - bare));
    let sent = sent(&event);
    assert_eq!(sent.len(), len);
    (event, sent)
}

fn chunked(bytes: &str, size: usize) -> Vec<Result<Vec<u8>, String>> {
    bytes
        .as_bytes()
        .chunks(size)
        .map(|chunk| Ok(chunk.to_vec()))
        .collect()
}

/// Twice the limit of bytes that never end an event, in lines, as a peer
/// that misbehaves might send them: the watch ends with the size failure
/// once the limit is reached, and reads no chunk past the one that reached
/// it, so its buffer never holds more than the limit and one chunk.
#[test]
fn bytes_past_the_limit_without_an_end_end_the_watch() {
    const CHUNK: usize = 100;
    let line = format!("data: {}\n", "x".repeat(CHUNK - 7));
    let chunks: Vec<Result<Vec<u8>, String>> = (0..2 * LIMIT / CHUNK)
        .map(|_| Ok(line.clone().into_bytes()))
        .collect();
    let pulled = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&pulled);
    let bytes = futures::stream::iter(chunks).inspect(move |_| {
        counted.fetch_add(1, Ordering::Relaxed);
    });
    let items: Vec<WatchItem<CellWatchEvent, UnknownEvent>> = runtime().block_on(async {
        let synced_by = tokio::time::Instant::now() + WatchLimits::SYNCED_WITHIN;
        events(bytes, Arc::default(), limits(), synced_by)
            .collect()
            .await
    });
    assert_eq!(
        items,
        [Err(WatchFailure::EventTooLarge { limit: LIMIT })],
        "the watch ends at the limit"
    );
    let pulled = pulled.load(Ordering::Relaxed);
    assert!(
        pulled * CHUNK < LIMIT + CHUNK,
        "{pulled} chunks of {CHUNK} bytes read for a limit of {LIMIT}"
    );
}

/// An event of exactly the limit parses, split over chunks of a few bytes,
/// so that its length is counted across them; one a byte past it ends the
/// watch, and nothing after it is read.
#[test]
fn an_event_at_the_limit_parses_and_one_past_it_does_not() {
    let (at, at_sent) = left_of_len(LIMIT);
    let (_, past_sent) = left_of_len(LIMIT + 1);
    let (after, after_sent) = left_of_len(100);
    let (got, failed, _) =
        read_within::<CellWatchEvent>(chunked(&format!("{at_sent}{after_sent}"), 7), limits());
    assert_eq!(got, [Watched::Event(at.clone()), Watched::Event(after)]);
    assert_eq!(failed, None);

    let (got, failed, _) = read_within::<CellWatchEvent>(
        chunked(&format!("{at_sent}{past_sent}{after_sent}"), 7),
        limits(),
    );
    assert_eq!(got, [Watched::Event(at.clone())]);
    assert_eq!(failed, Some(WatchFailure::EventTooLarge { limit: LIMIT }));

    // Whole in one chunk, too: the limit is the event's, not the chunk's.
    let (got, failed, _) =
        read_within::<CellWatchEvent>(vec![Ok(past_sent.into_bytes())], limits());
    assert_eq!(got, []);
    assert_eq!(failed, Some(WatchFailure::EventTooLarge { limit: LIMIT }));
}

/// Many small events in one chunk far longer than the limit all parse:
/// the limit is per event.
#[test]
fn many_small_events_in_one_chunk_all_parse() {
    let events: Vec<(CellWatchEvent, String)> = (0..500).map(|_| left_of_len(120)).collect();
    let chunk: String = events.iter().map(|(_, sent)| sent.as_str()).collect();
    assert!(chunk.len() > 50 * LIMIT);
    let (got, failed, _) = read_within::<CellWatchEvent>(vec![Ok(chunk.into_bytes())], limits());
    assert_eq!(failed, None);
    assert_eq!(
        got,
        events
            .into_iter()
            .map(|(event, _)| Watched::Event(event))
            .collect::<Vec<_>>()
    );
}

/// The ways a watch ends are told apart: a clean end yields nothing more,
/// and each failure is its own last item.
#[test]
fn the_endings_are_told_apart() {
    let (event, sent) = left_of_len(100);
    let read = |chunks| read_within::<CellWatchEvent>(chunks, limits());
    assert_eq!(
        read(chunked(&sent, 9)),
        (vec![Watched::Event(event)], None, 0)
    );
    assert_eq!(
        read(vec![Ok(b"data: [1]\n\n".to_vec())]).1,
        Some(WatchFailure::Unparsed)
    );
    assert_eq!(
        read(vec![Err("reset".into())]).1,
        Some(WatchFailure::Connection("reset".into()))
    );
    assert_eq!(
        read(vec![Ok(vec![b'x'; LIMIT])]).1,
        Some(WatchFailure::EventTooLarge { limit: LIMIT })
    );
}

/// The default limit takes the largest event a hub admits: a `ready` of an
/// instance whose registration was as large as the hub takes, 2 MiB, which
/// encodes the peer as the event does.
#[test]
fn the_default_limit_takes_the_largest_peer_a_hub_admits() {
    const BODY: usize = 2 << 20;
    // Each byte of the worker address is up to four characters of JSON.
    let address = vec![255u8; BODY / 4 - 64];
    let peer = PeerInfo::new(InstanceId::new_v4(), WorkerAddress::from_encoded(address));
    let registration = serde_json::to_string(&serde_json::json!({ "peer": peer }))
        .expect("a registration serializes");
    assert!(registration.len() <= BODY, "{}", registration.len());
    let ready = CellWatchEvent::Ready {
        segment: "s".repeat(64),
        peer,
    };
    let sent = format!(
        "data: {}\n\n",
        serde_json::to_string(&ready).expect("an event serializes")
    );
    assert!(sent.len() > BODY - 1024, "{}", sent.len());
    let (got, failed, _) =
        read_within::<CellWatchEvent>(chunked(&sent, 16 << 10), WatchLimits::default());
    assert_eq!(failed, None);
    assert_eq!(got, [Watched::Event(ready)]);
}
