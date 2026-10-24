//! Where a launch's stop meets its cell's drain: when the drain begins the
//! stop, what it changes about the drain, and what the instance announces
//! at its end; and what an entry instance's retirement waits for across
//! its cell's boundary. An instance outside a cell, under a roster
//! directory, or in a binary without hub discovery, takes no part in
//! either, and every answer here is the one its stop has without a cell.

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
    /// its cell drains, the cell's entry segments, for an entry instance
    /// too; otherwise those hosting a source that can reach it. `None` when
    /// their drain cannot be seen from here, for an entry instance that a
    /// source of another class reaches: the drain of the sources' requests
    /// (B) then never holds, and the instance waits for its senders and its
    /// parent (A).
    pub(super) fn drain_sources(&self) -> Option<Vec<String>> {
        #[cfg(feature = "hub")]
        if let Some(cell) = &self.cell_drain {
            if let Some(sources) = cell.sources() {
                return Some(sources.to_vec());
            }
            if cell.blind() {
                return None;
            }
        }
        Some(self.reaching.clone())
    }

    /// The instance retires now: from here an entry instance in a cell
    /// counts its wait for the cells that follow its cell.
    pub(super) fn cell_retires(&mut self) {
        #[cfg(feature = "hub")]
        if let Some(cell) = &mut self.cell_drain {
            cell.retire();
        }
    }

    /// Whether the cells that follow this instance's cell may be taken to
    /// avoid it, asked by (A) at each poll: for an entry instance that
    /// retired, once its hub says they do, or, while its cell drains, that
    /// none follows it, or the bound has passed since it retired and since
    /// it learned of a drain; for any other, always. What it reads
    /// is taken now, so that the question borrows nothing of the launch.
    #[cfg(feature = "hub")]
    pub(super) fn cell_parent(&self) -> impl Fn() -> bool + use<> {
        let parent = self.cell_drain.as_ref().and_then(|cell| cell.parent());
        move || parent.as_ref().is_none_or(crate::dist::Parent::avoids)
    }

    /// For a retired entry instance, decline offers from `segment` from
    /// other cells once a drain of its cell it learned of while it waits
    /// reaches its bound, or no cell follows the cell; then, and for any
    /// other instance, whose stop decided that as it began, never resolve.
    /// It runs beside the drain's waiting rather than between its
    /// questions to the hub, which may stall. What it reads is taken now,
    /// so that it borrows nothing of the launch.
    pub(super) fn cell_declining<'a>(
        &self,
        segment: &'a dyn Drains,
    ) -> impl Future<Output = ()> + use<'a> {
        #[cfg(feature = "hub")]
        let parent = self.cell_drain.as_ref().and_then(|cell| cell.parent());
        #[cfg(not(feature = "hub"))]
        let _ = segment;
        async move {
            #[cfg(feature = "hub")]
            if let Some(parent) = parent {
                parent.declining().await;
                segment.decline_offers();
            }
            std::future::pending::<()>().await;
        }
    }

    /// For an entry instance of a draining cell, decline offers once its
    /// bound has passed or no other cell follows the cell, or at
    /// `deadline`; for any other, nothing. What it reads is taken now, so
    /// that the wait borrows nothing of the launch while the stop goes on
    /// beside it.
    pub(super) fn cell_drain_declines<'a>(
        &self,
        segment: &'a dyn Drains,
        deadline: tokio::time::Instant,
    ) -> impl Future<Output = ()> + use<'a> {
        #[cfg(feature = "hub")]
        let declining = self.cell_drain.as_ref().and_then(|cell| cell.declining());
        #[cfg(not(feature = "hub"))]
        let declining: Option<std::future::Ready<()>> = None;
        async move {
            if let Some(declining) = declining {
                tokio::select! {
                    () = declining => {}
                    () = tokio::time::sleep_until(deadline) => {}
                }
                segment.decline_offers();
            }
        }
    }

    /// For an entry instance of a draining cell: wait until its bound has
    /// passed or no other cell follows the cell, or until `deadline`,
    /// decline offers, wait until it holds no
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
