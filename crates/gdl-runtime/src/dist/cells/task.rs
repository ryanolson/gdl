//! The chooser's follower as a task: one watch of its own hub's relay, for
//! the life of the instance, read into [`Follows`], and beside it a reading
//! of each cell left unread by an event of a kind this build does not
//! know.

use std::sync::Arc;
use std::time::Duration;

use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use gdl::{Error, ErrorKind, Result};
use gdl_model::Layout;
use gdl_watch::{CellWatcher, FollowEvent, UnknownFollowEvent, Watched};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use velo::Velo;

use super::Cells;
use super::follow::{Follows, Key};
use crate::dist::fabric::Registry;
use crate::dist::roster::Live;

/// How long to wait before watching the relay again after a watch ended
/// or could not begin, as the membership follower waits.
const REWATCH: Duration = Duration::from_millis(100);

/// Start following the cells of the classes `segment` sends to, for an
/// instance in a cell of `region` at the hub `host`, whose public
/// listener is on `public_port`: one task, which reads the hub's relay
/// into the tables it returns and registers their instances with `node`,
/// until the handle is aborted. `None`, and nothing started, for a
/// segment that holds no hop into another class. Only an instance in a
/// cell may call it: one without a cell follows nothing. `live` is the
/// instance's own membership, `drain` how long an instance in no table
/// stays registered.
#[expect(
    clippy::too_many_arguments,
    reason = "the chooser's settings, its hub, and its membership, as the launch holds them"
)]
pub(crate) fn follow(
    layout: &Layout,
    segment: &str,
    region: &str,
    host: &str,
    public_port: u16,
    node: &Velo,
    live: &Arc<Live>,
    drain: Duration,
) -> Result<Option<(Cells, JoinHandle<()>)>> {
    let wanted = super::entries(layout, segment);
    if wanted.is_empty() {
        return Ok(None);
    }
    let watcher =
        CellWatcher::new().map_err(|e| Error::new(ErrorKind::Transport, e.to_string()))?;
    let address = hub_address(host, public_port);
    let cells = Cells::new(region, wanted.values().flatten().map(String::as_str));
    let follows = Follows::new(cells.clone(), node.clone(), Arc::clone(live), drain, wanted);
    let task = tokio::spawn(run(Hub { watcher, address }, follows));
    Ok(Some((cells, task)))
}

/// The address of the hub at `host` whose public listener is on `port`,
/// `host:port` as a cell's watches take it, with an IPv6 host bracketed.
pub(crate) fn hub_address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// One item of the relay.
pub(super) type Relayed = Watched<FollowEvent, UnknownFollowEvent>;

/// Where the follower reads the relay from.
pub(super) trait Source: Send + Sync + 'static {
    /// One watch of the relay: its snapshot, then each change, until it
    /// ends.
    fn watch(
        &self,
    ) -> impl Future<Output = std::result::Result<BoxStream<'static, Relayed>, String>> + Send;

    /// The relay's follow of the cell `cell` of `class` alone, as a watch
    /// begins with it, without the marker.
    fn followed(
        &self,
        class: &str,
        cell: &str,
    ) -> impl Future<Output = std::result::Result<Vec<FollowEvent>, String>> + Send;
}

/// The instance's own hub.
struct Hub {
    watcher: CellWatcher,
    address: String,
}

impl Source for Hub {
    async fn watch(&self) -> std::result::Result<BoxStream<'static, Relayed>, String> {
        self.watcher
            .follows(&self.address)
            .await
            .map_err(|e| e.to_string())
    }

    async fn followed(
        &self,
        class: &str,
        cell: &str,
    ) -> std::result::Result<Vec<FollowEvent>, String> {
        self.watcher
            .followed(&self.address, class, cell)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Follow `source` into `follows` until aborted: watch, read the snapshot
/// and then each change, and when the watch ends or cannot begin, watch
/// again after [`REWATCH`]. What the tables hold stays until a new
/// snapshot is complete. A watch refused again for the same reason is
/// not logged again, since a hub without the cell's extension refuses
/// every one.
pub(super) async fn run<S: Source, R: Registry>(source: S, mut follows: Follows<R>) {
    let mut refused: Option<String> = None;
    loop {
        // A hub can take the connection and not answer: the deadlines are
        // kept while the watch is asked for.
        let opening = source.watch();
        tokio::pin!(opening);
        let opened = loop {
            let due = follows.next_due();
            tokio::select! {
                opened = &mut opening => break opened,
                () = until(due) => follows.forget(Instant::now()),
            }
        };
        match opened {
            Ok(events) => {
                if refused.take().is_some() {
                    tracing::info!("the hub's relay of the cells it follows is watched again");
                }
                read(&source, &mut follows, events).await;
            }
            Err(error) => {
                if refused.as_deref() != Some(error.as_str()) {
                    tracing::warn!(%error, "the hub's relay of the cells it follows could not be watched");
                    refused = Some(error);
                }
            }
        }
        let pause = tokio::time::sleep(REWATCH);
        tokio::pin!(pause);
        loop {
            let due = follows.next_due();
            tokio::select! {
                () = &mut pause => break,
                () = until(due) => follows.forget(Instant::now()),
            }
        }
    }
}

/// Read one watch of the relay into `follows` until it ends: its snapshot
/// apart, applied whole at its `synced`, then each change. A second
/// `synced` in one watch is not the protocol, and ends it. A cell an
/// event of a kind this build does not know names is read again from
/// `source` beside the watch, which goes on; a reading that fails is tried
/// again after [`REWATCH`].
async fn read<S: Source, R: Registry>(
    source: &S,
    follows: &mut Follows<R>,
    mut events: BoxStream<'static, Relayed>,
) {
    let mut snapshot = Some(follows.snapshot());
    let mut reading = FuturesUnordered::new();
    loop {
        for key in follows.asked() {
            reading.push(reread(source, key).boxed());
        }
        let due = follows.next_due();
        tokio::select! {
            item = events.next() => {
                let Some(item) = item else {
                    return;
                };
                let event = match item {
                    Watched::Event(event) => event,
                    Watched::Unknown(event) => {
                        match snapshot.as_mut() {
                            Some(filling) => filling.unknown(event),
                            None => follows.unknown(event),
                        }
                        continue;
                    }
                };
                let synced = matches!(event, FollowEvent::Synced);
                match snapshot.take() {
                    Some(complete) if synced => follows.resnapshot(complete, Instant::now()),
                    Some(mut filling) => {
                        filling.apply(event, Instant::now());
                        snapshot = Some(filling);
                    }
                    None if synced => return,
                    None => {
                        if !follows.take(event, Instant::now()) {
                            tracing::info!("the relay sent too much while a cell was read again; it is watched again");
                            return;
                        }
                    }
                }
            }
            Some((key, read)) = reading.next() => {
                if !follows.reread(key, read, Instant::now()) {
                    tracing::info!("the relay sent too much while a cell was read again; it is watched again");
                    return;
                }
            }
            () = until(due) => follows.forget(Instant::now()),
        }
    }
}

/// Read the cell `key` again from `source`, until a reading succeeds.
async fn reread<S: Source>(source: &S, key: Key) -> (Key, Vec<FollowEvent>) {
    let mut refused: Option<String> = None;
    loop {
        match source.followed(&key.0, &key.1).await {
            Ok(events) => return (key, events),
            Err(error) => {
                if refused.as_deref() != Some(error.as_str()) {
                    tracing::info!(class = key.0, cell = key.1, %error, "a cell could not be read again");
                    refused = Some(error);
                }
            }
        }
        tokio::time::sleep(REWATCH).await;
    }
}

/// Resolves at `at`, or never.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
