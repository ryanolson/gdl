//! One follow of one cell: its cell watch, read into the relay.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use gdl_cells::CellRecord;

use super::relay::{Installed, Relay};
use gdl_watch::{CellWatchEvent, CellWatcher, UnknownEvent, Watched};

/// The most events a link holds while it reads its cell again: past it,
/// the link ends, and the cell is followed again from a snapshot, as a
/// watcher that falls behind its hub's buffer is.
const HELD: usize = 1024;

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
/// the cell in the relay until the link has read it again, from a second
/// watch's snapshot, while this watch goes on: what it sends meanwhile is
/// held, and applied over what was read, so that nothing sent after the
/// snapshot was taken is lost. What was sent before it and is applied
/// again only repeats what the snapshot shows.
async fn follow(
    relay: &Relay,
    watcher: &CellWatcher,
    record: &CellRecord,
    generation: u64,
    retry: Duration,
) {
    let mut events = match watcher.watch(&record.hub).await {
        Ok(events) => events,
        Err(error) => {
            tracing::info!(cell = record.name, %error, "a followed cell's watch did not begin");
            return;
        }
    };
    let Ok(snapshot) = snapshot(&mut events, record).await else {
        return;
    };
    let mut link = Link {
        relay,
        watcher,
        record,
        generation,
        retry,
        reread: None,
        held: VecDeque::new(),
    };
    let Snapshot {
        mut installed,
        unknown,
    } = snapshot;
    // Withheld before it is installed, so that no watcher of the relay is
    // shown it otherwise.
    if unknown {
        installed.withhold(generation, |_| {});
    }
    relay.install(generation, installed);
    if unknown {
        link.read_again();
    }
    loop {
        let reading = async {
            match link.reread.as_mut() {
                Some(reread) => reread.await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            item = events.next() => {
                let Some(item) = item else {
                    return;
                };
                if !link.take(item) {
                    return;
                }
            }
            fresh = reading => {
                link.reread = None;
                let Some(fresh) = fresh else {
                    return;
                };
                relay.restore(&record.class, &record.name, generation, fresh);
                for item in std::mem::take(&mut link.held) {
                    if !link.take(item) {
                        return;
                    }
                }
            }
        }
    }
}

/// A link after its cell is installed: what it holds while it reads the
/// cell again.
struct Link<'a> {
    relay: &'a Relay,
    watcher: &'a CellWatcher,
    record: &'a CellRecord,
    generation: u64,
    /// How long a reading that failed waits before the next.
    retry: Duration,
    /// The cell read again, under way: `None` at its end if the hub serves
    /// another cell or epoch.
    reread: Option<BoxFuture<'a, Option<Installed>>>,
    /// The events the watch sent since the reading began.
    held: VecDeque<Item>,
}

impl Link<'_> {
    /// Take one item of the watch: apply it, hold it while the cell is read
    /// again, or withhold the cell for it. `false` when the link ends.
    fn take(&mut self, item: Item) -> bool {
        let (class, cell) = (&self.record.class, self.record.name.as_str());
        if self.reread.is_some() {
            if self.held.len() == HELD {
                tracing::info!(
                    cell,
                    "a followed cell's watch sent too much while it was read again"
                );
                return false;
            }
            self.held.push_back(item);
            return true;
        }
        match item {
            // A second snapshot in one watch is not the protocol.
            Watched::Event(CellWatchEvent::Cell { .. } | CellWatchEvent::Synced) => false,
            Watched::Event(event) => {
                self.relay.apply(class, cell, self.generation, event);
                true
            }
            Watched::Unknown(event) => {
                tracing::info!(
                    cell,
                    kind = event.kind,
                    "a followed cell's watch sent an event of a kind this build does not know; the cell is withheld until it is read again"
                );
                self.withhold();
                true
            }
        }
    }

    /// Withhold the cell in the relay, and read it again.
    fn withhold(&mut self) {
        self.relay
            .withhold(&self.record.class, &self.record.name, self.generation);
        self.read_again();
    }

    fn read_again(&mut self) {
        self.reread = Some(reread(self.watcher, self.record, self.retry).boxed());
    }
}

/// A cell's watch's snapshot, read into the cell it shows.
struct Snapshot {
    installed: Installed,
    /// Whether it had an event of a kind this build does not know.
    unknown: bool,
}

/// Why a watch's snapshot was not read.
enum Missed {
    /// The watch ended first, or sent a second cell.
    Ended,
    /// Its hub serves another cell, or this cell in another epoch.
    Other,
}

/// Read the snapshot that begins a watch of the cell of `record`.
async fn snapshot(
    events: &mut BoxStream<'static, Item>,
    record: &CellRecord,
) -> Result<Snapshot, Missed> {
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
            return Err(Missed::Other);
        }
        // A watch begins with its cell.
        _ => return Err(Missed::Ended),
    };
    let mut unknown = false;
    loop {
        match events.next().await.ok_or(Missed::Ended)? {
            Watched::Event(CellWatchEvent::Synced) => break,
            Watched::Event(CellWatchEvent::Cell { .. }) => return Err(Missed::Ended),
            // The snapshot's own events are its cell's, before any watcher
            // of the relay sees it.
            Watched::Event(event) => installed.apply(0, event, |_| {}),
            Watched::Unknown(_) => unknown = true,
        }
    }
    Ok(Snapshot { installed, unknown })
}

/// Read the cell of `record` again, from a watch of its own that ends with
/// its snapshot: `None` if its hub serves another cell or epoch, which
/// ends the follow. A reading whose watch does not begin, ends early, or
/// has an event of a kind this build does not know is tried again after
/// `retry`, the cell withheld meanwhile.
async fn reread(watcher: &CellWatcher, record: &CellRecord, retry: Duration) -> Option<Installed> {
    loop {
        if let Ok(mut events) = watcher.watch(&record.hub).await {
            match snapshot(&mut events, record).await {
                Ok(Snapshot {
                    installed,
                    unknown: false,
                }) => return Some(installed),
                Err(Missed::Other) => return None,
                Ok(_) | Err(Missed::Ended) => {}
            }
        }
        tokio::time::sleep(retry).await;
    }
}
