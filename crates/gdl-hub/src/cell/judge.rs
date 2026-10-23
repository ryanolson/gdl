//! The cell's state, judged as the counts change and as the hold runs
//! out, and shown on the health endpoint. Nothing here waits for the
//! directory: the writer takes the latest state from a watch.

use std::sync::Arc;
use std::time::Instant;

use gdl_cells::CellState;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::CellHealth;
use super::count::Count;
use super::feed::Feed;
use crate::Quorum;

pub(super) struct Judge {
    pub(super) quorum: Quorum,
    pub(super) health: watch::Sender<CellHealth>,
    /// The state, for the writer: a change it has not written yet is
    /// replaced by the next, so a slow write never delays a judgment.
    pub(super) states: watch::Sender<CellState>,
    /// The cell's watchers, sent each change of state.
    pub(super) feed: Arc<Feed>,
    /// Whether the cell drains: from then its state is `draining`,
    /// whatever its quorum.
    pub(super) draining: watch::Receiver<bool>,
}

impl Judge {
    /// Judge until `cancel`, or until the counting ends.
    pub(super) async fn run(
        mut self,
        mut counts: mpsc::Receiver<Count>,
        cancel: CancellationToken,
    ) {
        tokio::select! {
            () = self.judge_until(&mut counts) => {}
            () = cancel.cancelled() => {}
        }
    }

    async fn judge_until(&mut self, counts: &mut mpsc::Receiver<Count>) {
        self.judge();
        loop {
            let held = self.quorum.next_change();
            let hold = async {
                match held {
                    Some(at) => tokio::time::sleep_until(at.into()).await,
                    None => std::future::pending().await,
                }
            };
            let draining = async {
                if self.draining.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                count = counts.recv() => {
                    let Some(count) = count else { return };
                    self.count(count);
                }
                () = hold => {}
                () = draining => {}
            }
            self.judge();
        }
    }

    fn count(&mut self, count: Count) {
        match count {
            Count::Synced(usable) => {
                for (index, usable) in usable.into_iter().enumerate() {
                    self.quorum.set_nth(index, usable);
                }
            }
            Count::Changed(index, usable) => self.quorum.set_nth(index, usable),
            Count::Lost => {
                for index in 0..self.quorum.segments().count() {
                    self.quorum.set_nth(index, 0);
                }
            }
        }
    }

    /// Judge the state now, and show it: to the writer and the watchers
    /// when it changed, and on the health endpoint, whose body is updated
    /// in place so that a count costs no allocation. The endpoint is told
    /// last, so that a drain, which is answered once the endpoint shows
    /// it, is answered after the watchers were sent it.
    fn judge(&mut self) {
        let state = match *self.draining.borrow_and_update() {
            true => CellState::Draining,
            false => self.quorum.state(Instant::now()),
        };
        let changed = self.states.send_if_modified(|current| {
            let changed = *current != state;
            *current = state;
            changed
        });
        if changed {
            self.feed.set_state(state);
        }
        let quorum = &self.quorum;
        self.health.send_if_modified(|health| {
            let mut modified = health.state != state;
            health.state = state;
            for (name, usable, _) in quorum.segments() {
                if let Some(segment) = health.segments.get_mut(name)
                    && segment.usable != usable
                {
                    segment.usable = usable;
                    modified = true;
                }
            }
            modified
        });
    }
}
