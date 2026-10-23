//! One follow of one cell: its cell watch, read into the relay.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use gdl_cells::CellRecord;
use tokio_util::sync::CancellationToken;

use super::relay::{Installed, Relay};
use gdl_watch::{CellWatchEvent, CellWatcher};

/// Follow the cell of `record` in `generation` until its watch ends or
/// fails, or until the relay closes the follow (`close`), then end it in
/// the relay at once, which marks it not offered if the relay has not
/// closed it; after `refollow`, return the cell, the generation, and
/// whether the relay closed it, for the follower to decide whether to
/// follow it again. The wait is in the link's own task, which already
/// exists per followed cell, so a follower holds no timer per cell.
pub(super) async fn run(
    relay: Arc<Relay>,
    watcher: CellWatcher,
    record: CellRecord,
    generation: u64,
    close: CancellationToken,
    refollow: Duration,
) -> (String, u64, bool) {
    let closed = tokio::select! {
        () = follow(&relay, &watcher, &record, generation) => false,
        () = close.cancelled() => true,
    };
    relay.end(&record.class, &record.name, generation);
    tokio::time::sleep(refollow).await;
    (record.name, generation, closed)
}

/// Read the cell's watch: its snapshot, installed in the relay once it is
/// complete and only if it is the cell of `record` in the record's epoch,
/// then each change.
async fn follow(relay: &Relay, watcher: &CellWatcher, record: &CellRecord, generation: u64) {
    let cell = record.name.as_str();
    let mut events = match watcher.watch(&record.hub).await {
        Ok(events) => events,
        Err(error) => {
            tracing::info!(cell, %error, "a followed cell's watch did not begin");
            return;
        }
    };
    // A hub at the record's address that serves another cell, or this
    // cell in another epoch (a restart the directory has not shown yet,
    // or one it has and this record predates), is not the cell the record
    // names.
    let mut installed = match events.next().await {
        Some(CellWatchEvent::Cell {
            name,
            class,
            epoch,
            state,
            load,
        }) if name == record.name && class == record.class && epoch == record.epoch => {
            Installed::new(name, class, record.region.clone(), state, load)
        }
        Some(CellWatchEvent::Cell { name, epoch, .. }) => {
            tracing::info!(
                cell,
                served = name,
                epoch,
                expected = record.epoch,
                "a followed cell's hub serves another cell or epoch"
            );
            return;
        }
        _ => return,
    };
    loop {
        match events.next().await {
            Some(CellWatchEvent::Synced) => break,
            Some(CellWatchEvent::Cell { .. }) | None => return,
            Some(event) => installed.apply(generation, event, |_| {}),
        }
    }
    relay.install(generation, installed);
    while let Some(event) = events.next().await {
        if matches!(event, CellWatchEvent::Cell { .. } | CellWatchEvent::Synced) {
            // A second snapshot in one watch is not the protocol.
            return;
        }
        relay.apply(&record.class, cell, generation, event);
    }
}
