//! The chooser's follower as a task: one watch of its own hub's relay, for
//! the life of the instance, read into [`Follows`].

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use gdl::{Error, ErrorKind, Result};
use gdl_model::Layout;
use gdl_watch::{CellWatcher, FollowEvent};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use velo::Velo;

use super::Cells;
use super::follow::Follows;
use crate::dist::fabric::Registry;
use crate::dist::roster::Live;

/// How long to wait before watching the relay again after a watch ended
/// or could not begin, as the membership follower waits.
const REWATCH: Duration = Duration::from_millis(100);

/// Start following the cells of the classes `segment` sends to, for an
/// instance in a cell at the hub `host`, whose public listener is on
/// `public_port`: one task, which reads the hub's relay into the tables
/// it returns and registers their instances with `node`, until the handle
/// is aborted. `None`, and nothing started, for a segment that holds no
/// hop into another class. Only an instance in a cell may call it: one
/// without a cell follows nothing. `live` is the instance's own
/// membership, `drain` how long an instance in no table stays registered.
pub(crate) fn follow(
    layout: &Layout,
    segment: &str,
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
    let address = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{public_port}")
    } else {
        format!("{host}:{public_port}")
    };
    let cells = Cells::new(wanted.values().flatten().map(String::as_str));
    let follows = Follows::new(cells.clone(), node.clone(), Arc::clone(live), drain, wanted);
    let task = tokio::spawn(run(Hub { watcher, address }, follows));
    Ok(Some((cells, task)))
}

/// Where the follower reads the relay from.
pub(super) trait Source: Send + Sync + 'static {
    /// One watch of the relay: its snapshot, then each change, until it
    /// ends.
    fn watch(
        &self,
    ) -> impl Future<Output = std::result::Result<BoxStream<'static, FollowEvent>, String>> + Send;
}

/// The instance's own hub.
struct Hub {
    watcher: CellWatcher,
    address: String,
}

impl Source for Hub {
    async fn watch(&self) -> std::result::Result<BoxStream<'static, FollowEvent>, String> {
        self.watcher
            .follows(&self.address)
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
        match source.watch().await {
            Ok(events) => {
                if refused.take().is_some() {
                    tracing::info!("the hub's relay of the cells it follows is watched again");
                }
                read(&mut follows, events).await;
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
/// `synced` in one watch is not the protocol, and ends it.
async fn read<R: Registry>(follows: &mut Follows<R>, mut events: BoxStream<'static, FollowEvent>) {
    let mut snapshot = Some(follows.snapshot());
    loop {
        let due = follows.next_due();
        tokio::select! {
            event = events.next() => {
                let Some(event) = event else {
                    return;
                };
                let synced = matches!(event, FollowEvent::Synced);
                match snapshot.take() {
                    Some(complete) if synced => follows.resnapshot(complete, Instant::now()),
                    Some(mut filling) => {
                        filling.apply(event);
                        snapshot = Some(filling);
                    }
                    None if synced => return,
                    None => follows.apply(event, Instant::now()),
                }
            }
            () = until(due) => follows.forget(Instant::now()),
        }
    }
}

/// Resolves at `at`, or never.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
