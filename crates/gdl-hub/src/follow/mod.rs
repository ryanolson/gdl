//! How a cell's hub follows the cells of a class its class sends to: it
//! finds them in the cell directory, follows each over its cell watch,
//! and relays what it follows to its own instances.
//!
//! One task per class watches the directory and decides, record by
//! record, which cells to follow; each follow is a task of its own that
//! reads one cell's watch into the [`Relay`]. The directory finds cells
//! and the link carries them: a record that expires or is withdrawn never
//! ends a follow, and a link that ends drops its follow at once, whatever
//! the directory says. A newer epoch in the directory ends a follow too,
//! since the hub it was following is gone.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use gdl_cells::{CellDirectory, CellEvent, CellRecord, CellState, CellView};
use tokio::task::{AbortHandle, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::CellWatcher;

mod link;
mod relay;

pub use relay::FollowsWatch;
pub(crate) use relay::Relay;

/// How long to wait before watching the directory again after its watch
/// ended or failed.
const REWATCH: Duration = Duration::from_millis(100);

/// The follower of one class: which of its cells this hub follows.
pub(crate) struct Follower<D> {
    pub(crate) directory: Arc<D>,
    pub(crate) relay: Arc<Relay>,
    pub(crate) watcher: CellWatcher,
    /// How long a follow that ended waits before the cell may be followed
    /// again.
    pub(crate) refollow: Duration,
    /// The class followed.
    pub(crate) class: String,
    /// The follower's own region: its home cells are those of this region.
    pub(crate) region: String,
    /// The follower's contract and placement hashes: a cell with others
    /// serves another deployment, and is ignored.
    pub(crate) contract: u64,
    pub(crate) placement: u64,
}

impl<D: CellDirectory> Follower<D> {
    /// Follow until `cancel`. Every follow's task ends with it.
    pub(crate) async fn run(self, cancel: CancellationToken) {
        tokio::select! {
            () = self.follow() => {}
            () = cancel.cancelled() => {}
        }
    }

    async fn follow(self) {
        let mut follows = Follows {
            view: CellView::new(),
            current: HashMap::new(),
            links: JoinSet::new(),
            follower: &self,
        };
        loop {
            let mut watch = std::pin::pin!(self.directory.watch(&self.class, None));
            loop {
                tokio::select! {
                    event = watch.next() => match event {
                        Some(Ok(event)) => follows.directory(event),
                        Some(Err(error)) => {
                            tracing::warn!(class = self.class, %error, "the cell directory's watch failed");
                            break;
                        }
                        None => break,
                    },
                    Some(ended) = follows.links.join_next() => follows.ended(ended),
                }
            }
            let pause = tokio::time::sleep(REWATCH);
            tokio::pin!(pause);
            loop {
                tokio::select! {
                    () = &mut pause => break,
                    Some(ended) = follows.links.join_next() => follows.ended(ended),
                }
            }
        }
    }

    /// Whether `record` is a home cell that takes work: of this region and
    /// deployment, and `ready` or `degraded`.
    fn home(&self, record: &CellRecord) -> bool {
        record.region == self.region
            && record.contract == self.contract
            && record.placement == self.placement
            && matches!(record.state, CellState::Ready | CellState::Degraded)
    }
}

/// The follower's state, owned by its task.
struct Follows<'a, D> {
    /// The class's records, as the directory shows them.
    view: CellView,
    /// Each cell followed: its link under way, or waiting to follow again.
    current: HashMap<String, Following>,
    /// The links' tasks, each returning its cell and generation once it
    /// has ended and waited to follow again. Dropping the set ends them.
    links: JoinSet<(String, u64)>,
    follower: &'a Follower<D>,
}

/// One cell followed.
struct Following {
    generation: u64,
    /// The epoch of the record the follow began from.
    epoch: u64,
    link: AbortHandle,
}

impl<D: CellDirectory> Follows<'_, D> {
    /// Apply one event of the directory's watch, and decide on the cell it
    /// names, if any: a record is considered when it arrives, never on a
    /// pass over the others.
    fn directory(&mut self, event: CellEvent) {
        let cell = match &event {
            CellEvent::Current(record) | CellEvent::Put { record, .. } => Some(record.name.clone()),
            _ => None,
        };
        let now = Instant::now();
        self.view.apply(event, now);
        if let Some(cell) = cell {
            self.decide(&cell, now);
        }
    }

    /// Follow `cell` if its record makes it a home cell that takes work
    /// and it is not followed yet; end its follow if the record has a newer
    /// epoch than the one it began from, and follow it anew if it can.
    fn decide(&mut self, cell: &str, now: Instant) {
        let record = self.view.get(cell, now).cloned();
        if let Some(following) = self.current.get(cell) {
            if record
                .as_ref()
                .is_none_or(|record| record.epoch <= following.epoch)
            {
                return;
            }
            let following = self
                .current
                .remove(cell)
                .expect("the follow was just found");
            following.link.abort();
            self.follower.relay.end(cell, following.generation);
        }
        match record.filter(|record| self.follower.home(record)) {
            Some(record) => self.start(record),
            None => self.follower.relay.forget(cell),
        }
    }

    /// A link's task ended: if it is the cell's current follow, follow the
    /// cell again if its record still makes it a home cell that takes work.
    fn ended(&mut self, ended: Result<(String, u64), JoinError>) {
        // A task aborted for a newer epoch was replaced already.
        let Ok((cell, generation)) = ended else {
            return;
        };
        if self
            .current
            .get(&cell)
            .is_none_or(|following| following.generation != generation)
        {
            return;
        }
        self.current.remove(&cell);
        self.decide(&cell, Instant::now());
    }

    fn start(&mut self, record: CellRecord) {
        let relay = &self.follower.relay;
        let generation = relay.begin(&record.name);
        let name = record.name.clone();
        let epoch = record.epoch;
        let link = self.links.spawn(link::run(
            Arc::clone(relay),
            self.follower.watcher.clone(),
            record,
            generation,
            self.follower.refollow,
        ));
        self.current.insert(
            name,
            Following {
                generation,
                epoch,
                link,
            },
        );
    }
}
