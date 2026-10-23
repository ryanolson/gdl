//! Where a launch's stop meets its cell's drain: when the drain begins the
//! stop, what it changes about the drain, and what the instance announces
//! at its end. An instance outside a cell, under a roster directory, or
//! in a binary without hub discovery, takes no part in a cell's drain, and
//! every answer here is the one its stop has without a cell.

use super::Launch;
#[cfg(feature = "hub")]
use super::Peers;

impl Launch {
    /// Resolves when the cell's drain begins this instance's stop; never
    /// for an instance that takes no part in one.
    pub(super) async fn cell_drain_stops(&self) {
        #[cfg(feature = "hub")]
        if let (Peers::Hub(session), Some(cell)) = (&self.peers, &self.cell_drain) {
            return cell.stops(session).await;
        }
        std::future::pending().await
    }

    /// Whether the stop the cell's drain begins retires the instance.
    pub(super) fn cell_drain_retires(&self) -> bool {
        #[cfg(feature = "hub")]
        if let Some(cell) = &self.cell_drain {
            return cell.retires();
        }
        true
    }

    /// The segments whose instances are this one's drain sources: while
    /// its cell drains, the cell's entry segments, or none for an entry
    /// instance; otherwise those hosting a source that can reach it.
    pub(super) fn drain_sources(&self) -> Vec<String> {
        #[cfg(feature = "hub")]
        if let Some(sources) = self.cell_drain.as_ref().and_then(|cell| cell.sources()) {
            return sources.to_vec();
        }
        self.reaching.clone()
    }

    /// For an entry instance of a draining cell, wait until it may stop
    /// taking new work, or until `deadline`.
    pub(super) async fn cell_drain_accepting(&self, deadline: tokio::time::Instant) {
        #[cfg(feature = "hub")]
        if let Some(cell) = &self.cell_drain {
            cell.accepting(deadline).await;
        }
        #[cfg(not(feature = "hub"))]
        let _ = deadline;
    }

    /// For an entry instance of a draining cell that holds no request,
    /// announce that it has drained, retried until `deadline`: the cell's
    /// interior instances wait for it.
    pub(super) async fn cell_drained(&mut self, deadline: tokio::time::Instant) {
        #[cfg(feature = "hub")]
        if let (Peers::Hub(session), Some(cell)) = (&mut self.peers, &self.cell_drain)
            && cell.announces()
            && let Err(error) = session.drained(deadline).await
        {
            eprintln!(
                "{}: could not announce that it drained, so the cell's interior instances wait for it to leave: {error}",
                self.segment
            );
        }
        #[cfg(not(feature = "hub"))]
        let _ = deadline;
    }
}
