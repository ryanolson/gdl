//! The cell's drain, as its hub keeps it: once started, the cell is
//! `draining` whatever its quorum, for the life of the hub process. A
//! drain cannot be cancelled: the instances that learned of it are already
//! on their way out, and nothing would bring them back.

use tokio::sync::watch;

pub(super) struct Drain {
    started: watch::Sender<bool>,
}

impl Drain {
    pub(super) fn new() -> Self {
        Self {
            started: watch::Sender::new(false),
        }
    }

    /// Start the drain; a drain already started is left as it is.
    pub(super) fn start(&self) {
        self.started
            .send_if_modified(|started| !std::mem::replace(started, true));
    }

    /// Whether the drain has started, as it changes.
    pub(super) fn started(&self) -> watch::Receiver<bool> {
        self.started.subscribe()
    }
}
