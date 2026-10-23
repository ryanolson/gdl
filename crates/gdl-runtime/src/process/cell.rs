//! Where a launch's stop meets its cell's drain: when the drain begins the
//! stop, what it changes about the drain, and what the instance announces
//! at its end. An instance outside a cell, under a roster directory, or
//! in a binary without hub discovery, takes no part in a cell's drain, and
//! every answer here is the one its stop has without a cell.

#[cfg(feature = "hub")]
use super::{DRAIN_POLL, Peers};
use super::{Drains, Launch};

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

    /// For an entry instance of a draining cell, decline offers once its
    /// bound has passed, or at `deadline`; for any other, nothing. What it
    /// reads is taken now, so that the wait borrows nothing of the launch
    /// while the stop goes on beside it.
    pub(super) fn cell_drain_declines<'a>(
        &self,
        segment: &'a dyn Drains,
        deadline: tokio::time::Instant,
    ) -> impl Future<Output = ()> + use<'a> {
        #[cfg(feature = "hub")]
        let declines = self.cell_drain.as_ref().and_then(|cell| cell.declines());
        #[cfg(not(feature = "hub"))]
        let declines: Option<tokio::time::Instant> = None;
        async move {
            if let Some(at) = declines {
                tokio::time::sleep_until(at.min(deadline)).await;
                segment.decline_offers();
            }
        }
    }

    /// For an entry instance of a draining cell: wait until its bound has
    /// passed, or until `deadline`, decline offers, wait until it holds no
    /// request, so that every request it took has been answered, and
    /// announce that it has drained, retried until `deadline`; with `hub`
    /// false (its lease is lost), nothing is announced. Any other instance
    /// goes on at once.
    pub(super) async fn cell_drain_settles(
        &mut self,
        segment: &dyn Drains,
        deadline: tokio::time::Instant,
        hub: bool,
    ) {
        #[cfg(feature = "hub")]
        if let Some(cell) = &self.cell_drain
            && cell.announces()
        {
            cell.accepting(deadline).await;
            segment.decline_offers();
            while segment.admitted() > 0 && tokio::time::Instant::now() + DRAIN_POLL <= deadline {
                tokio::time::sleep(DRAIN_POLL).await;
            }
            if let (Peers::Hub(session), true) = (&mut self.peers, hub)
                && let Err(error) = session.drained(deadline).await
            {
                eprintln!(
                    "{}: could not announce that it drained, so the cell's other instances wait for it to leave: {error}",
                    self.segment
                );
            }
        }
        #[cfg(not(feature = "hub"))]
        let _ = (segment, deadline, hub);
    }
}
