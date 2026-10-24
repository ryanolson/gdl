//! One follow of one cell: its cell watch, read into the relay.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use gdl_cells::CellRecord;

use super::relay::{Installed, Relay};
use gdl_watch::{CellWatchEvent, CellWatcher, UnknownEvent, Watched};

/// Follow the cell of `record` in `generation` until its watch ends or
/// fails, then drop it from the relay at once; after `refollow`, return
/// the cell and the generation, for the follower to decide whether to
/// follow it again. The wait is in the link's own task, which already
/// exists per followed cell, so a follower holds no timer per cell.
pub(super) async fn run(
    relay: Arc<Relay>,
    watcher: CellWatcher,
    record: CellRecord,
    generation: u64,
    refollow: Duration,
) -> (String, u64) {
    follow(&relay, &watcher, &record, generation, refollow).await;
    relay.end(&record.class, &record.name, generation);
    tokio::time::sleep(refollow).await;
    (record.name, generation)
}

/// One item of a cell's watch.
type Item = Watched<CellWatchEvent, UnknownEvent>;

/// Read the cell's watch: its snapshot, installed in the relay once it is
/// complete and only if it is the cell of `record` in the record's epoch,
/// then each change. An event of a kind this build does not know withholds
/// the cell in the relay until the link has read it again, from a new
/// watch of it, which replaces this one: the new watch's snapshot and the
/// events after it are ordered by the cell's hub, so nothing sent before
/// the snapshot is applied over it, and nothing after it is lost. A watch
/// that does not begin or ends before its snapshot is complete ends the
/// follow, as any watch that ends does; one whose snapshot has an event of
/// a kind this build does not know leaves the cell withheld, and the cell
/// is read again after `retry`.
async fn follow(
    relay: &Relay,
    watcher: &CellWatcher,
    record: &CellRecord,
    generation: u64,
    retry: Duration,
) {
    let (class, cell) = (record.class.as_str(), record.name.as_str());
    let mut events = match watcher.watch(&record.hub).await {
        Ok(events) => events,
        Err(error) => {
            tracing::info!(cell, %error, "a followed cell's watch did not begin");
            return;
        }
    };
    let Some(Snapshot {
        mut installed,
        unknown,
    }) = snapshot(&mut events, record).await
    else {
        return;
    };
    // Withheld before it is installed, so that no watcher of the relay is
    // shown it otherwise.
    if unknown {
        installed.withhold(generation, |_| {});
    }
    relay.install(generation, installed);
    let mut pause = unknown.then_some(retry);
    loop {
        if let Some(wait) = pause.take() {
            // The watch that showed the cell cannot be read on: the cell is
            // read again from a watch of its own, which replaces it.
            drop(events);
            tokio::time::sleep(wait).await;
            let Some((fresh, read)) = reread(watcher, record).await else {
                return;
            };
            events = read;
            match fresh {
                Some(fresh) => relay.restore(class, cell, generation, fresh),
                None => {
                    pause = Some(retry);
                    continue;
                }
            }
        }
        match events.next().await {
            None => return,
            // A second snapshot in one watch is not the protocol.
            Some(Watched::Event(CellWatchEvent::Cell { .. } | CellWatchEvent::Synced)) => return,
            Some(Watched::Event(event)) => relay.apply(class, cell, generation, event),
            Some(Watched::Unknown(event)) => {
                tracing::info!(
                    cell,
                    kind = event.kind,
                    "a followed cell's watch sent an event of a kind this build does not know; the cell is withheld until it is read again"
                );
                relay.withhold(class, cell, generation);
                pause = Some(Duration::ZERO);
            }
        }
    }
}

/// A cell's watch's snapshot, read into the cell it shows.
struct Snapshot {
    installed: Installed,
    /// Whether it had an event of a kind this build does not know.
    unknown: bool,
}

/// Read the snapshot that begins a watch of the cell of `record`: `None`
/// if the watch ends first, or its hub serves another cell, or this cell
/// in another epoch.
async fn snapshot(events: &mut BoxStream<'static, Item>, record: &CellRecord) -> Option<Snapshot> {
    let cell = record.name.as_str();
    // A hub at the record's address that serves another cell, or this
    // cell in another epoch (a restart the directory has not shown yet,
    // or one it has and this record predates), is not the cell the record
    // names.
    let mut installed = match events.next().await {
        Some(Watched::Event(CellWatchEvent::Cell {
            name,
            class,
            epoch,
            state,
            load,
        })) if name == record.name && class == record.class && epoch == record.epoch => {
            Installed::new(name, class, record.region.clone(), state, load)
        }
        Some(Watched::Event(CellWatchEvent::Cell { name, epoch, .. })) => {
            tracing::info!(
                cell,
                served = name,
                epoch,
                expected = record.epoch,
                "a followed cell's hub serves another cell or epoch"
            );
            return None;
        }
        // A watch begins with its cell.
        _ => return None,
    };
    let mut unknown = false;
    loop {
        match events.next().await? {
            Watched::Event(CellWatchEvent::Synced) => break,
            Watched::Event(CellWatchEvent::Cell { .. }) => return None,
            // The snapshot's own events are its cell's, before any watcher
            // of the relay sees it.
            Watched::Event(event) => installed.apply(0, event, |_| {}),
            Watched::Unknown(_) => unknown = true,
        }
    }
    Some(Snapshot { installed, unknown })
}

/// Read the cell of `record` again, from a watch of its own: the cell as
/// its snapshot shows it, `None` if the snapshot had an event of a kind
/// this build does not know, and the watch, to read on from. `None` if the
/// watch does not begin, ends before its snapshot is complete, or its hub
/// serves another cell or epoch, which ends the follow.
async fn reread(
    watcher: &CellWatcher,
    record: &CellRecord,
) -> Option<(Option<Installed>, BoxStream<'static, Item>)> {
    let mut events = watcher.watch(&record.hub).await.ok()?;
    let Snapshot { installed, unknown } = snapshot(&mut events, record).await?;
    Some(((!unknown).then_some(installed), events))
}
