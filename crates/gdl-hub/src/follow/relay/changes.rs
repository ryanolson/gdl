//! The relay's changes, sent to its watchers.

use std::collections::HashMap;

use gdl_watch::FollowEvent;
use tokio::sync::broadcast;

/// Where the relay's changes are sent: to the watches of the whole relay,
/// and to the watches of the one follow each names, on a channel of the
/// cell's own, so that such a watch is sent its cell's changes alone
/// rather than every change, to drop all but its own: with a watch of
/// each cell, that would cost each change a copy per cell.
pub(super) struct Changes {
    all: broadcast::Sender<FollowEvent>,
    /// By class, then by cell, so that finding a change's channel by the
    /// names it carries allocates nothing; a channel no watch reads any
    /// more is dropped when its last watch closes.
    one: HashMap<String, HashMap<String, broadcast::Sender<FollowEvent>>>,
    buffer: usize,
    #[cfg(test)]
    pub(super) work: crate::fanout::Work,
}

impl Changes {
    pub(super) fn new(buffer: usize) -> Self {
        Self {
            all: broadcast::channel(buffer).0,
            one: HashMap::new(),
            buffer,
            #[cfg(test)]
            work: Default::default(),
        }
    }

    pub(super) fn send(&mut self, event: FollowEvent) {
        // A watch of one follow is how a reader of an earlier build reads
        // a cell again after a kind it does not know, so it carries none.
        if !self.one.is_empty()
            && !matches!(event, FollowEvent::Listed { .. })
            && let Some((class, cell)) = event.cell()
            && let Some(cells) = self.one.get_mut(class)
            && let Some(channel) = cells.get(cell)
        {
            let _ = channel.send(event.clone());
        }
        let _ = self.all.send(event);
    }

    /// Drop the channel of the cell `cell` of `class` if no watch reads it.
    pub(super) fn release(&mut self, class: &str, cell: &str) {
        if let Some(cells) = self.one.get_mut(class)
            && cells
                .get(cell)
                .is_some_and(|channel| channel.receiver_count() == 0)
        {
            cells.remove(cell);
            if cells.is_empty() {
                self.one.remove(class);
            }
        }
    }

    /// Subscribe to the changes of the cell `cell` of `class` alone.
    pub(super) fn subscribe_one(
        &mut self,
        class: &str,
        cell: &str,
    ) -> broadcast::Receiver<FollowEvent> {
        #[cfg(test)]
        self.work.subscription();
        let buffer = self.buffer;
        self.one
            .entry(class.to_string())
            .or_default()
            .entry(cell.to_string())
            .or_insert_with(|| broadcast::channel(buffer).0)
            .subscribe()
    }

    /// Subscribe to every change.
    pub(super) fn subscribe_all(&self) -> broadcast::Receiver<FollowEvent> {
        #[cfg(test)]
        self.work.subscription();
        self.all.subscribe()
    }

    /// How many channels of cells watched alone there are.
    #[cfg(test)]
    pub(super) fn channels(&self) -> usize {
        self.one.values().map(HashMap::len).sum()
    }
}
