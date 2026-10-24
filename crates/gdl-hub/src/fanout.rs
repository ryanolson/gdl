//! A stream of changes fanned out to many watchers: each watcher holds its
//! snapshot and its place in a buffer that all share, which is all it
//! holds. The owner sends each change and takes each snapshot under its
//! own lock, subscribing under the same lock, so that no change between a
//! snapshot and the changes after it is lost or seen twice.
//!
//! A route that serves the stream holds a capped number of places, one per
//! watch it serves: one beyond them is refused before anything is built
//! for it, so that a storm of watches costs the hub a refusal each, not a
//! snapshot and a place in the buffer each.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use futures::stream::BoxStream;
use serde::Serialize;
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use velo_hub::axum::http::{StatusCode, header};
use velo_hub::axum::response::sse::{Event, KeepAlive, Sse};
use velo_hub::axum::response::{IntoResponse, Response};

/// How often a watch with nothing to say sends a keep-alive comment, as
/// velo-hub's service watch does, so that a watcher's idle timeout tells a
/// stalled connection from a quiet stream.
const KEEP_ALIVE: Duration = Duration::from_secs(10);

/// The seconds a refused watcher is asked to wait before it asks again.
/// The hub's clients do not read it yet: each asks again after its own
/// delay, as after any watch that does not begin.
const RETRY_AFTER: &str = "1";

/// The watchers of one stream: how many there are, and the signal that
/// ends every watch when the hub stops.
pub(crate) struct Watchers {
    /// Cancelled when the hub stops: every watch ends, so that the
    /// listener's drain never waits one out.
    closed: CancellationToken,
    /// How many watches are open.
    watching: watch::Sender<usize>,
}

impl Watchers {
    pub(crate) fn new() -> Self {
        Self {
            closed: CancellationToken::new(),
            watching: watch::Sender::new(0),
        }
    }

    /// Count one more watch, until the returned guard is dropped.
    pub(crate) fn count(&self) -> Counted {
        self.watching.send_modify(|watching| *watching += 1);
        Counted(self.watching.clone())
    }

    /// A watch: `snapshot`, then each change `events` receives, until the
    /// hub stops or the watch falls behind the buffer.
    pub(crate) fn stream<E: Clone + Send + 'static>(
        &self,
        counted: Counted,
        snapshot: Vec<E>,
        events: broadcast::Receiver<E>,
    ) -> BoxStream<'static, E> {
        let watch = Watch {
            snapshot: snapshot.into_iter(),
            events,
            closed: self.closed.clone(),
            _counted: counted,
        };
        Box::pin(futures::stream::unfold(watch, |mut watch| async move {
            let event = watch.next().await?;
            Some((event, watch))
        }))
    }

    /// How many watches are open, as it changes.
    pub(crate) fn watchers(&self) -> watch::Receiver<usize> {
        self.watching.subscribe()
    }

    /// End every watch: the hub stops.
    pub(crate) fn close(&self) {
        self.closed.cancel();
    }

    /// Whether the hub stopped.
    pub(crate) fn closed(&self) -> &CancellationToken {
        &self.closed
    }
}

/// One watcher's place in a stream: its snapshot, then its cursor in the
/// changes.
struct Watch<E> {
    snapshot: std::vec::IntoIter<E>,
    events: broadcast::Receiver<E>,
    closed: CancellationToken,
    _counted: Counted,
}

impl<E: Clone> Watch<E> {
    async fn next(&mut self) -> Option<E> {
        // A stopped hub sends nothing more, its snapshot included: what it
        // held is no longer so.
        if self.closed.is_cancelled() {
            return None;
        }
        if let Some(event) = self.snapshot.next() {
            return Some(event);
        }
        tokio::select! {
            biased;
            () = self.closed.cancelled() => None,
            received = self.events.recv() => match received {
                Ok(event) => {
                    #[cfg(test)]
                    RECEIVED.with(|received| received.set(received.get() + 1));
                    Some(event)
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    // What it holds is stale: it watches again from a
                    // snapshot.
                    tracing::info!(missed, "a watcher fell behind; its watch ends");
                    None
                }
                Err(broadcast::error::RecvError::Closed) => None,
            },
        }
    }
}

#[cfg(test)]
thread_local! {
    /// The changes the watches polled on this thread have received, each
    /// a copy of the change: so that a test counts a watch's work rather
    /// than timing it.
    pub(crate) static RECEIVED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The snapshots built and the subscriptions taken for watches, counted
/// where each is built and taken: so that a test counts the work a refused
/// watch did not cost.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Work {
    snapshots: AtomicUsize,
    subscriptions: AtomicUsize,
}

#[cfg(test)]
impl Work {
    pub(crate) fn snapshot(&self) {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn subscription(&self) {
        self.subscriptions.fetch_add(1, Ordering::Relaxed);
    }

    /// The snapshots built and the subscriptions taken.
    pub(crate) fn counted(&self) -> (usize, usize) {
        (
            self.snapshots.load(Ordering::Relaxed),
            self.subscriptions.load(Ordering::Relaxed),
        )
    }
}

/// One watch in the count of watchers, until it is dropped.
pub(crate) struct Counted(watch::Sender<usize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.send_modify(|watching| *watching -= 1);
    }
}

/// The places of one route's watches: apart from [`Watchers`], whose
/// count is behind a lock and counts the watches taken in process too.
pub(crate) struct Places {
    /// How many are held.
    open: AtomicUsize,
    /// How many there are.
    cap: usize,
    /// How many watches were refused, every place being held.
    refused: AtomicU64,
}

impl Places {
    pub(crate) fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            open: AtomicUsize::new(0),
            cap,
            refused: AtomicU64::new(0),
        })
    }

    /// A place for one more watch served on the route, held until the
    /// returned place is dropped; refused while every place is held. Taken
    /// before anything is built for the watch, its snapshot and its
    /// subscription included, with one atomic count and no lock.
    pub(crate) fn take(self: &Arc<Self>) -> Result<Place, Full> {
        self.open
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                (open < self.cap).then_some(open + 1)
            })
            .map(|_| Place(Arc::clone(self)))
            .map_err(|_| {
                self.refused.fetch_add(1, Ordering::Relaxed);
                Full
            })
    }

    /// How many watches the route has refused, every place being held.
    pub(crate) fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }
}

/// One watch's place on its route, until it is dropped.
pub(crate) struct Place(Arc<Places>);

impl Drop for Place {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A watch served in its place.
struct Placed<E> {
    /// Declared first, so that it is dropped first: a watcher that sees the
    /// count of watches fall finds the place free.
    _place: Place,
    watch: BoxStream<'static, E>,
}

impl<E> futures::Stream for Placed<E> {
    type Item = E;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<E>> {
        self.watch.as_mut().poll_next(cx)
    }
}

/// A watch refused because its route holds no place for it.
pub(crate) struct Full;

impl IntoResponse for Full {
    fn into_response(self) -> Response {
        // One text for every refusal, so that a client that logs a refusal
        // once until its reason changes logs a storm once.
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, RETRY_AFTER)],
            "the hub serves as many watches of this route as it holds places for; ask again later",
        )
            .into_response()
    }
}

/// A watch as server-sent events, each one JSON value, with a keep-alive
/// every 10 s, holding its `place` until the connection closes or the watch
/// ends.
pub(crate) fn serve<E: Serialize + Send + 'static>(
    watch: BoxStream<'static, E>,
    place: Place,
) -> Response {
    let placed = Placed {
        _place: place,
        watch,
    };
    let events = futures::StreamExt::map(placed, |event| Event::default().json_data(event));
    Sse::new(events)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response()
}
