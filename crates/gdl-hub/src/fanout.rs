//! A stream of changes fanned out to many watchers: each watcher holds its
//! snapshot and its place in a buffer that all share, which is all it
//! holds. The owner sends each change and takes each snapshot under its
//! own lock, subscribing under the same lock, so that no change between a
//! snapshot and the changes after it is lost or seen twice.

use std::time::Duration;

use futures::stream::BoxStream;
use serde::Serialize;
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use velo_hub::axum::response::sse::{Event, KeepAlive, Sse};
use velo_hub::axum::response::{IntoResponse, Response};

/// How often a watch with nothing to say sends a keep-alive comment, as
/// velo-hub's service watch does, so that a watcher's idle timeout tells a
/// stalled connection from a quiet stream.
const KEEP_ALIVE: Duration = Duration::from_secs(10);

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
        if let Some(event) = self.snapshot.next() {
            return Some(event);
        }
        tokio::select! {
            biased;
            () = self.closed.cancelled() => None,
            received = self.events.recv() => match received {
                Ok(event) => Some(event),
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

/// One watch in the count of watchers, until it is dropped.
pub(crate) struct Counted(watch::Sender<usize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.send_modify(|watching| *watching -= 1);
    }
}

/// A watch as server-sent events, each one JSON value, with a keep-alive
/// every 10 s.
pub(crate) fn serve<E: Serialize + Send + 'static>(watch: BoxStream<'static, E>) -> Response {
    let events = futures::StreamExt::map(watch, |event| Event::default().json_data(event));
    Sse::new(events)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response()
}
