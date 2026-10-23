//! An instance's part in its cell's drain. The instance learns of the
//! drain from its own hub's cell watch, which carries the cell's state;
//! nothing about it is on a request's path.
//!
//! The cell's entry instances are the drain sources of its interior ones
//! (`agent-docs/cells.md`, section 5): a chooser in another class reaches
//! only the entry instances, so an interior instance's own sources are
//! not members of its hub, and without this rule it would count them as
//! drained and close at once, while an entry instance still holds a
//! request that will call it.

use std::time::Duration;

use futures::StreamExt;
use gdl::{Error, ErrorKind, Result};
use gdl_cells::CellState;
use gdl_model::{CellSettings, Layout};
use gdl_watch::{CellWatchEvent, CellWatcher};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::HubSession;

/// How long to wait before watching the cell again after a watch ended.
const REWATCH: Duration = Duration::from_millis(100);

/// How long to wait before asking again a hub that refused the watch: one
/// without the cell's extension refuses every one.
const REFUSED: Duration = Duration::from_secs(1);

/// An instance's view of its cell's drain, for an instance in a cell under
/// a hub whose class a hop enters. The watch ends when this is dropped.
pub(crate) struct CellDrain {
    /// When the instance learned that its cell drains, on its own clock.
    learned: watch::Receiver<Option<Instant>>,
    role: Role,
    /// The cell's entry segments, whose instances are the drain sources of
    /// every instance of the cell while it drains.
    entries: Vec<String>,
    watch: JoinHandle<()>,
}

/// What the instance is in its cell.
enum Role {
    /// An instance of an entry segment, which takes new work for `bound`
    /// after it learns of the drain.
    Entry { bound: Duration },
    /// Any other instance.
    Interior,
}

/// How long an entry instance keeps taking new work after it learns that
/// its cell drains, so that an offer from a chooser whose view of the cell
/// is stale lands: the runtime's setup timeout, the longest a hand-off sent
/// before its chooser stopped offering to the cell takes to arrive, and
/// twice the cell's load summary period, the follow link's lag bound. A
/// chooser whose view lags further than three periods has a stale summary
/// of the cell, and its cell policy does not choose the cell at all.
pub(crate) fn bound(setup: Duration, summary: Duration) -> Duration {
    setup + summary * 2
}

impl CellDrain {
    /// Watch the cell of `cell` for its drain, for an instance of
    /// `segment` at the hub whose public listener is `host`'s
    /// `public_port`. `None`, and nothing watched, when no hop enters the
    /// cell's class: such a cell is not drained as a cell.
    pub(crate) fn watch(
        layout: &Layout,
        cell: &CellSettings,
        segment: &str,
        host: &str,
        public_port: u16,
        setup: Duration,
    ) -> Result<Option<Self>> {
        let entries = layout.entry_segments(&cell.class);
        if entries.is_empty() {
            return Ok(None);
        }
        let role = match entries.contains(segment) {
            true => Role::Entry {
                bound: bound(setup, cell.summary),
            },
            false => Role::Interior,
        };
        let entries = entries.into_iter().map(ToString::to_string).collect();
        let watcher =
            CellWatcher::new().map_err(|e| Error::new(ErrorKind::Transport, e.to_string()))?;
        let address = crate::dist::cells::hub_address(host, public_port);
        let (learns, learned) = watch::channel(None);
        let watch = tokio::spawn(run(watcher, address, learns));
        Ok(Some(Self {
            learned,
            role,
            entries,
            watch,
        }))
    }

    /// Resolves when the cell's drain begins this instance's stop: an entry
    /// instance's once the bound has passed since it learned of the drain,
    /// an interior instance's once every entry instance of the cell has
    /// drained or left `session`'s hub, which it asks every
    /// [`HubSession::poll`] while the cell drains.
    pub(crate) async fn stops(&self, session: &HubSession) {
        let learned = self.learned().await;
        match &self.role {
            Role::Entry { bound } => tokio::time::sleep_until(learned + *bound).await,
            Role::Interior => {
                let entries: Vec<&str> = self.entries.iter().map(String::as_str).collect();
                loop {
                    // A hub can stop answering without closing its
                    // connections; the lease's own end stops the instance.
                    let asked = tokio::time::timeout(
                        HubSession::poll() * 10,
                        session.all_drained(&entries),
                    );
                    if matches!(asked.await, Ok(Ok(true))) {
                        return;
                    }
                    tokio::time::sleep(HubSession::poll()).await;
                }
            }
        }
    }

    /// Whether the stop the drain begins retires the instance. An entry
    /// instance does not: a chooser stops offering to a draining cell by
    /// its state, and one whose view is stale enough to offer still is
    /// declined once the entry declines offers; a retirement sent ahead of
    /// that would only take the entry from that chooser's view while it
    /// may still take the offer.
    pub(crate) fn retires(&self) -> bool {
        matches!(self.role, Role::Interior)
    }

    /// While the cell drains, the segments whose instances are this
    /// instance's drain sources: the cell's entry segments, for an entry
    /// instance too, since a request another entry instance took may still
    /// hand work to it inside the cell. `None` while the instance has not
    /// learned of a drain.
    pub(crate) fn sources(&self) -> Option<&[String]> {
        self.learned.borrow().as_ref()?;
        Some(&self.entries)
    }

    /// For an entry instance that has learned of its cell's drain, when its
    /// bound passes: when it stops taking offers.
    pub(crate) fn declines(&self) -> Option<Instant> {
        let learned = (*self.learned.borrow())?;
        match &self.role {
            Role::Entry { bound } => Some(learned + *bound),
            Role::Interior => None,
        }
    }

    /// Wait, for an entry instance of a draining cell, until the bound has
    /// passed since it learned of the drain, or until `deadline`; any
    /// other instance goes on at once.
    pub(crate) async fn accepting(&self, deadline: Instant) {
        if let Some(at) = self.declines() {
            tokio::time::sleep_until(at.min(deadline)).await;
        }
    }

    /// Whether the instance announces that it has drained once it holds
    /// no request: an entry instance of a draining cell does, since every
    /// instance of the cell waits for it.
    pub(crate) fn announces(&self) -> bool {
        matches!(self.role, Role::Entry { .. }) && self.learned.borrow().is_some()
    }

    /// When the instance learned of the drain, once it has.
    async fn learned(&self) -> Instant {
        let mut learned = self.learned.clone();
        let at = learned
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|at| *at);
        match at {
            Some(at) => at,
            // The watch ends only with this.
            None => std::future::pending().await,
        }
    }
}

impl Drop for CellDrain {
    fn drop(&mut self) {
        self.watch.abort();
    }
}

/// Watch the cell at the hub at `address` until it says it is
/// `draining`, then say when on `learns`. A watch that ends or is refused
/// is asked for again; a refusal is logged once, until a watch begins.
async fn run(watcher: CellWatcher, address: String, learns: watch::Sender<Option<Instant>>) {
    let mut refused: Option<String> = None;
    loop {
        let pause = match watcher.watch(&address).await {
            Ok(mut events) => {
                refused = None;
                while let Some(event) = events.next().await {
                    let state = match event {
                        CellWatchEvent::Cell { state, .. } | CellWatchEvent::State { state } => {
                            state
                        }
                        _ => continue,
                    };
                    if state == CellState::Draining {
                        learns.send_replace(Some(Instant::now()));
                        return;
                    }
                }
                REWATCH
            }
            Err(error) => {
                let error = error.to_string();
                if refused.as_deref() != Some(error.as_str()) {
                    tracing::info!(%error, "this instance's cell could not be watched for a drain");
                    refused = Some(error);
                }
                REFUSED
            }
        };
        tokio::time::sleep(pause).await;
    }
}
